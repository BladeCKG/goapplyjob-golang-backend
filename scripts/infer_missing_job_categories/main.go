package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/parsedaiclassifier"
	"log"
	"time"
)

func jsonStringOrNil(values []string) any {
	if len(values) == 0 {
		return "[]"
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "[]"
	}
	return string(encoded)
}

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT id, role_title, role_description, role_requirements, tech_stack
		   FROM parsed_jobs
		  WHERE categorized_job_title IS NULL
		  ORDER BY id ASC`,
	)
	if err != nil {
		log.Fatalf("select missing categories: %v", err)
	}
	defer rows.Close()

	svc := parsedaiclassifier.New(parsedaiclassifier.Config{}, db)
	updatedRows := 0
	skippedRows := 0
	for rows.Next() {
		var (
			id               int64
			roleTitle        sql.NullString
			roleDesc         sql.NullString
			roleRequirements sql.NullString
			techStackRaw     sql.NullString
			techStackValue   any
		)
		if err := rows.Scan(&id, &roleTitle, &roleDesc, &roleRequirements, &techStackRaw); err != nil {
			log.Fatalf("scan row: %v", err)
		}

		if techStackRaw.Valid && techStackRaw.String != "" {
			var parsedStack []string
			if err := json.Unmarshal([]byte(techStackRaw.String), &parsedStack); err == nil {
				techStackValue = parsedStack
			}
		}

		category, function, techStack, err := svc.SuggestCategoryWithTechStack(
			ctx,
			roleRequirements.String,
			roleTitle.String,
			roleDesc.String,
			techStackValue,
			false, // overrideTechStack
		)
		if err != nil {
			log.Printf("infer failed id=%d error=%v", id, err)
			skippedRows++
			continue
		}
		if category == "" {
			log.Printf("infer skipped id=%d reason=empty_category", id)
			skippedRows++
			continue
		}

		if _, err := db.SQL.ExecContext(
			ctx,
			`UPDATE parsed_jobs
			    SET categorized_job_title = ?,
			        categorized_job_function = ?,
					tech_stack = ?,
			        updated_at = ?
			  WHERE id = ?`,
			category,
			function,
			jsonStringOrNil(techStack),
			time.Now().UTC().Format(time.RFC3339Nano),
			id,
		); err != nil {
			log.Printf("update failed id=%d category=%s function=%s err=%v", id, category, function, err)
			skippedRows++
			continue
		}
		updatedRows++
		log.Printf("updated id=%d category=%s function=%s", id, category, function)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows error: %v", err)
	}

	log.Printf("done updated_rows=%d skipped_rows=%d", updatedRows, skippedRows)
}
