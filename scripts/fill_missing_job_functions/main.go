package main

import (
	"context"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"log"
	"strings"
	"time"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	rows, err := db.SQL.QueryContext(ctx,
		`SELECT categorized_job_title, categorized_job_function
		   FROM parsed_jobs
		  WHERE categorized_job_title IS NOT NULL
		    AND categorized_job_title != ''
		    AND categorized_job_function IS NOT NULL
		    AND categorized_job_function != ''
		  GROUP BY categorized_job_title, categorized_job_function
		  ORDER BY categorized_job_title ASC, COUNT(id) DESC, categorized_job_function ASC`,
	)
	if err != nil {
		log.Fatalf("select category function pairs: %v", err)
	}
	categoryFunction := map[string]string{}
	for rows.Next() {
		var category, function string
		if err := rows.Scan(&category, &function); err != nil {
			rows.Close()
			log.Fatalf("scan pair: %v", err)
		}
		if category == "" || function == "" {
			continue
		}
		if _, exists := categoryFunction[category]; !exists {
			categoryFunction[category] = function
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		log.Fatalf("pair rows error: %v", err)
	}
	rows.Close()

	missingRows, err := db.SQL.QueryContext(ctx,
		`SELECT id, categorized_job_title
		   FROM parsed_jobs
		  WHERE categorized_job_title IS NOT NULL
		    AND categorized_job_title != ''
		    AND (categorized_job_function IS NULL OR categorized_job_function = '')
		  ORDER BY id ASC`,
	)
	if err != nil {
		log.Fatalf("select missing rows: %v", err)
	}
	defer missingRows.Close()

	updatedRows := 0
	skippedRows := 0
	for missingRows.Next() {
		var id int64
		var category string
		if err := missingRows.Scan(&id, &category); err != nil {
			log.Fatalf("scan missing row: %v", err)
		}
		fn := strings.TrimSpace(categoryFunction[category])
		if fn == "" {
			skippedRows++
			continue
		}
		if _, err := db.SQL.ExecContext(
			ctx,
			`UPDATE parsed_jobs
			    SET categorized_job_function = ?, updated_at = ?
			  WHERE id = ?`,
			fn,
			time.Now().UTC().Format(time.RFC3339Nano),
			id,
		); err != nil {
			log.Printf("update failed id=%d category=%s function=%s err=%v", id, category, fn, err)
			skippedRows++
			continue
		}
		updatedRows++
		log.Printf("updated id=%d category=%s function=%s", id, category, fn)
	}
	if err := missingRows.Err(); err != nil {
		log.Fatalf("missing rows error: %v", err)
	}

	log.Printf("done updated_rows=%d skipped_rows=%d categories=%d", updatedRows, skippedRows, len(categoryFunction))
}
