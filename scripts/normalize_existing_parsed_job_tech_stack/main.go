package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/extract/techstack"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "log changes without writing updates")
	batchSize := flag.Int("batch-size", 500, "select/update batch size")
	flag.Parse()

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

	scanned, updated, err := run(context.Background(), db, *dryRun, *batchSize)
	if err != nil {
		log.Fatal(err)
	}

	mode := "APPLIED"
	if *dryRun {
		mode = "DRY-RUN"
	}
	log.Printf("done mode=%s scanned=%d updated=%d", mode, scanned, updated)
}

type parsedJobRow struct {
	ParsedJobID   int64
	TechStackJSON string
}

type techstackCatalogEntry struct {
	Canonical string `json:"canonical"`
}

var (
	loadCanonicalsOnce sync.Once
	exactCanonicals    map[string]string
)

func run(ctx context.Context, db *database.DB, dryRun bool, batchSize int) (int, int, error) {
	scanned := 0
	updated := 0
	lastParsedJobID := int64(0)

	for {
		rows, err := selectBatch(ctx, db, lastParsedJobID, batchSize)
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

			before := parseJSONArrayStrings(row.TechStackJSON)
			after := normalizeTechStack(before)
			if equalStringSlices(before, after) {
				continue
			}

			updated++
			log.Printf("parsed_job_update parsed_job_id=%d before=%v after=%v", row.ParsedJobID, before, after)
			if dryRun {
				continue
			}

			now := time.Now().UTC().Format(time.RFC3339Nano)
			if len(after) == 0 {
				if _, err := tx.ExecContext(ctx, `UPDATE parsed_jobs SET tech_stack = NULL, updated_at = ? WHERE id = ?`, now, row.ParsedJobID); err != nil {
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
			if _, err := tx.ExecContext(ctx, `UPDATE parsed_jobs SET tech_stack = ?::jsonb, updated_at = ? WHERE id = ?`, string(payload), now, row.ParsedJobID); err != nil {
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

func selectBatch(ctx context.Context, db *database.DB, lastParsedJobID int64, batchSize int) ([]parsedJobRow, error) {
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT p.id, p.tech_stack::text
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE tech_stack IS NOT NULL
		    AND tech_stack::text != '[]'
		    AND p.id > ?
		  ORDER BY p.id ASC
		  LIMIT ?`,
		lastParsedJobID,
		batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("select parsed_jobs batch: %w", err)
	}
	defer rows.Close()

	out := make([]parsedJobRow, 0, batchSize)
	for rows.Next() {
		var row parsedJobRow
		if err := rows.Scan(&row.ParsedJobID, &row.TechStackJSON); err != nil {
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

	var list []string
	if err := json.Unmarshal([]byte(value), &list); err == nil {
		return list
	}

	var anyList []any
	if err := json.Unmarshal([]byte(value), &anyList); err != nil {
		return []string{}
	}

	out := make([]string, 0, len(anyList))
	for _, item := range anyList {
		text, ok := item.(string)
		if !ok || text == "" {
			continue
		}
		out = append(out, text)
	}
	return out
}

func normalizeTechStack(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if canonical, ok := normalizeExactCanonical(value); ok {
			if _, ok := seen[canonical]; ok {
				continue
			}
			normalized = append(normalized, canonical)
			seen[canonical] = struct{}{}
			continue
		}
		extracted := techstack.Extract(value)
		if len(extracted) == 0 {
			continue
		}
		canonical := extracted[0]
		if _, ok := seen[canonical]; ok {
			continue
		}
		normalized = append(normalized, canonical)
		seen[canonical] = struct{}{}
	}
	return normalized
}

func normalizeExactCanonical(value string) (string, bool) {
	if exactCanonicals != nil {
		canonical, ok := exactCanonicals[strings.ToLower(value)]
		return canonical, ok
	}
	loadCanonicalsOnce.Do(loadExactCanonicals)
	if value == "" {
		return "", false
	}
	canonical, ok := exactCanonicals[strings.ToLower(value)]
	return canonical, ok
}

func loadExactCanonicals() {
	path := filepath.Join("internal", "extract", "techstack", "catalog.json")
	content, err := os.ReadFile(path)
	if err != nil {
		exactCanonicals = map[string]string{}
		return
	}
	var entries []techstackCatalogEntry
	if err := json.Unmarshal(content, &entries); err != nil {
		exactCanonicals = map[string]string{}
		return
	}
	loaded := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.Canonical == "" {
			continue
		}
		loaded[strings.ToLower(entry.Canonical)] = entry.Canonical
	}
	exactCanonicals = loaded
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
