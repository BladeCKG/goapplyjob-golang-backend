package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/normalize/locationnorm"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "log changes without writing updates")
	batchSize := flag.Int("batch-size", 500, "commit every N updates")
	sourcesCSV := flag.String("sources", "", "optional comma-separated sources")
	flag.Parse()

	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	if *batchSize < 1 {
		*batchSize = 1
	}

	scanned, updated, err := normalizeExistingLocationCountries(context.Background(), db, *dryRun, *batchSize, splitSources(*sourcesCSV))
	if err != nil {
		log.Fatal(err)
	}

	mode := "APPLIED"
	if *dryRun {
		mode = "DRY-RUN"
	}
	log.Printf("done mode=%s scanned=%d updated=%d", mode, scanned, updated)
}

func normalizeExistingLocationCountries(ctx context.Context, db *database.DB, dryRun bool, batchSize int, sources []string) (int, int, error) {
	scanned := 0
	updated := 0
	lastParsedJobID := int64(0)
	for {
		rows, err := selectLocationCountryBatch(ctx, db, batchSize, lastParsedJobID, sources)
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

			before := parseJSONArrayStrings(row.CountriesJSON)
			after := normalizeCountries(before)
			if equalStringSlices(before, after) {
				continue
			}

			updated++
			log.Printf("parsed_job_update parsed_job_id=%d raw_job_id=%d before=%v after=%v", row.ParsedJobID, row.RawJobID, before, after)
			if dryRun {
				continue
			}

			now := time.Now().UTC().Format(time.RFC3339Nano)
			if len(after) == 0 {
				if _, err := tx.ExecContext(ctx, `UPDATE parsed_jobs SET location_countries = NULL, updated_at = ? WHERE id = ?`, now, row.ParsedJobID); err != nil {
					_ = tx.Rollback()
					return scanned, updated, fmt.Errorf("update parsed_job_id=%d: %w", row.ParsedJobID, err)
				}
				commitBatch = true
				continue
			}

			payload, err := json.Marshal(after)
			if err != nil {
				_ = tx.Rollback()
				return scanned, updated, fmt.Errorf("marshal parsed_job_id=%d: %w", row.ParsedJobID, err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE parsed_jobs SET location_countries = ?::jsonb, updated_at = ? WHERE id = ?`, string(payload), now, row.ParsedJobID); err != nil {
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

type locationCountryRow struct {
	ParsedJobID   int64
	RawJobID      int64
	CountriesJSON string
}

func selectLocationCountryBatch(ctx context.Context, db *database.DB, batchSize int, lastParsedJobID int64, sources []string) ([]locationCountryRow, error) {
	query := `SELECT p.id, p.raw_us_job_id, COALESCE(p.location_countries::text, '')
		FROM parsed_jobs p
		JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		WHERE p.location_countries IS NOT NULL
		  AND p.id > ?`
	args := make([]any, 0, len(sources)+2)
	args = append(args, lastParsedJobID)
	if len(sources) > 0 {
		placeholders := make([]string, 0, len(sources))
		for _, source := range sources {
			placeholders = append(placeholders, "?")
			args = append(args, source)
		}
		query += ` AND r.source IN (` + strings.Join(placeholders, ", ") + `)`
	}
	query += ` ORDER BY p.id ASC LIMIT ?`
	args = append(args, batchSize)

	rows, err := db.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select parsed_jobs batch: %w", err)
	}
	defer rows.Close()

	out := make([]locationCountryRow, 0, batchSize)
	for rows.Next() {
		var row locationCountryRow
		if err := rows.Scan(&row.ParsedJobID, &row.RawJobID, &row.CountriesJSON); err != nil {
			return nil, fmt.Errorf("scan parsed_jobs batch: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("parsed_jobs batch rows error: %w", err)
	}
	return out, nil
}

func parseJSONArrayStrings(value string) []string {
	if value == "" || value == "null" {
		return []string{}
	}
	out := []string{}
	if err := json.Unmarshal([]byte(value), &out); err == nil {
		return out
	}
	anyItems := []any{}
	if err := json.Unmarshal([]byte(value), &anyItems); err != nil {
		return []string{}
	}
	out = make([]string, 0, len(anyItems))
	for _, item := range anyItems {
		text, ok := item.(string)
		if !ok || text == "" {
			continue
		}
		out = append(out, text)
	}
	return out
}

func normalizeCountries(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		normalized := locationnorm.NormalizeCountryName(value)
		if normalized == "" {
			continue
		}
		key := strings.ToLower(normalized)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func splitSources(csv string) []string {
	if csv == "" {
		return nil
	}
	out := []string{}
	for _, part := range strings.Split(csv, ",") {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}
