package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/extract/techstack"
	"goapplyjob-golang-backend/internal/normalize/techstacknorm"
	"goapplyjob-golang-backend/internal/sources/plugins"
)

func main() {
	fromParsedJobID := flag.Int64("from-parsed-job-id", 0, "required inclusive parsed_jobs.id start")
	toParsedJobID := flag.Int64("to-parsed-job-id", 0, "required inclusive parsed_jobs.id end")
	dryRun := flag.Bool("dry-run", false, "log changes without writing updates")
	batchSize := flag.Int("batch-size", 500, "select/update batch size")
	flag.Parse()

	if *fromParsedJobID < 1 || *toParsedJobID < 1 {
		log.Fatal("--from-parsed-job-id and --to-parsed-job-id are required and must be >= 1")
	}
	if *fromParsedJobID > *toParsedJobID {
		log.Fatal("--from-parsed-job-id must be <= --to-parsed-job-id")
	}
	if *batchSize < 1 {
		*batchSize = 1
	}

	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	scanned, updated, err := run(
		context.Background(),
		db,
		*fromParsedJobID,
		*toParsedJobID,
		*dryRun,
		*batchSize,
	)
	if err != nil {
		log.Fatal(err)
	}

	mode := "APPLIED"
	if *dryRun {
		mode = "DRY-RUN"
	}
	log.Printf(
		"done mode=%s from_parsed_job_id=%d to_parsed_job_id=%d scanned=%d updated=%d",
		mode,
		*fromParsedJobID,
		*toParsedJobID,
		scanned,
		updated,
	)
}

type parsedJobRow struct {
	ParsedJobID         int64
	RawJobID            int64
	Source              string
	RoleTitle           sql.NullString
	RoleDescription     sql.NullString
	RoleRequirements    sql.NullString
	CategorizedTitle    sql.NullString
	CategorizedFunction sql.NullString
	TechStackJSON       sql.NullString
}

func run(ctx context.Context, db *database.DB, fromParsedJobID, toParsedJobID int64, dryRun bool, batchSize int) (int, int, error) {
	scanned := 0
	updated := 0
	lastParsedJobID := fromParsedJobID - 1

	for {
		rows, err := selectBatch(ctx, db, lastParsedJobID, toParsedJobID, batchSize)
		if err != nil {
			return scanned, updated, err
		}
		if len(rows) == 0 {
			break
		}

		var tx *database.Tx
		if !dryRun {
			tx, err = db.SQL.BeginTx(ctx, nil)
			if err != nil {
				return scanned, updated, fmt.Errorf("begin tx: %w", err)
			}
		}

		commitBatch := false
		for _, row := range rows {
			lastParsedJobID = row.ParsedJobID
			scanned++

			nextTechStack, ok := extractForRow(row)
			if !ok {
				continue
			}

			updated++
			log.Printf(
				"parsed_job_update parsed_job_id=%d raw_job_id=%d source=%s category=%q function=%q tech_stack=%v",
				row.ParsedJobID,
				row.RawJobID,
				row.Source,
				row.CategorizedTitle.String,
				row.CategorizedFunction.String,
				nextTechStack,
			)
			if dryRun {
				continue
			}

			payload, err := json.Marshal(nextTechStack)
			if err != nil {
				_ = tx.Rollback()
				return scanned, updated, fmt.Errorf("marshal parsed_job_id=%d: %w", row.ParsedJobID, err)
			}
			if _, err := tx.ExecContext(
				ctx,
				`UPDATE parsed_jobs SET tech_stack = ?::jsonb, updated_at = ? WHERE id = ?`,
				string(payload),
				time.Now().UTC().Format(time.RFC3339Nano),
				row.ParsedJobID,
			); err != nil {
				_ = tx.Rollback()
				return scanned, updated, fmt.Errorf("update parsed_job_id=%d: %w", row.ParsedJobID, err)
			}
			commitBatch = true
		}

		if dryRun {
			continue
		}
		if commitBatch {
			if err := tx.Commit(); err != nil {
				_ = tx.Rollback()
				return scanned, updated, fmt.Errorf("commit batch: %w", err)
			}
			continue
		}
		if err := tx.Rollback(); err != nil {
			return scanned, updated, fmt.Errorf("rollback empty batch: %w", err)
		}
	}

	return scanned, updated, nil
}

func selectBatch(ctx context.Context, db *database.DB, lastParsedJobID, toParsedJobID int64, batchSize int) ([]parsedJobRow, error) {
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT p.id,
		        p.raw_us_job_id,
		        r.source,
		        p.role_title,
		        p.role_description,
		        p.role_requirements,
		        p.categorized_job_title,
		        p.categorized_job_function,
		        p.tech_stack::text
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE p.id > ?
		    AND p.id <= ?
		  ORDER BY p.id ASC
		  LIMIT ?`,
		lastParsedJobID,
		toParsedJobID,
		batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("select parsed_jobs batch: %w", err)
	}
	defer rows.Close()

	out := make([]parsedJobRow, 0, batchSize)
	for rows.Next() {
		var row parsedJobRow
		if err := rows.Scan(
			&row.ParsedJobID,
			&row.RawJobID,
			&row.Source,
			&row.RoleTitle,
			&row.RoleDescription,
			&row.RoleRequirements,
			&row.CategorizedTitle,
			&row.CategorizedFunction,
			&row.TechStackJSON,
		); err != nil {
			return nil, fmt.Errorf("scan parsed_jobs batch: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("parsed_jobs batch rows error: %w", err)
	}
	return out, nil
}

func extractForRow(row parsedJobRow) ([]string, bool) {
	plugin, ok := plugins.Get(row.Source)
	if !ok || !plugin.UseManualTechStackExtraction {
		return nil, false
	}
	if !techstack.IsAllowedInference(row.CategorizedTitle.String, row.CategorizedFunction.String) {
		return nil, false
	}
	if hasExistingTechStack(row.TechStackJSON) {
		return nil, false
	}

	extracted := techstack.ExtractDescriptionRequirements(row.RoleDescription.String, row.RoleRequirements.String)
	normalized := techstacknorm.Normalize(extracted)
	if len(normalized) == 0 {
		return nil, false
	}
	return normalized, true
}

func hasExistingTechStack(raw sql.NullString) bool {
	if !raw.Valid || raw.String == "" || raw.String == "null" {
		return false
	}

	var list []string
	if err := json.Unmarshal([]byte(raw.String), &list); err == nil {
		return len(list) > 0
	}

	var anyList []any
	if err := json.Unmarshal([]byte(raw.String), &anyList); err != nil {
		return false
	}
	for _, item := range anyList {
		text, ok := item.(string)
		if ok && strings.TrimSpace(text) != "" {
			return true
		}
	}
	return false
}

