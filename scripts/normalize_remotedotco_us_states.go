package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"reflect"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/locationnorm"
)

const remotedotcoSource = "remotedotco"

func main() {
	dryRun := flag.Bool("dry-run", false, "log changes without writing updates")
	flag.Parse()

	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	rawUpdated, rawSkipped := normalizeRemoteDotCoRawJobs(ctx, db, *dryRun)
	parsedUpdated, parsedSkipped := normalizeRemoteDotCoParsedJobs(ctx, db, *dryRun)

	log.Printf(
		"done dry_run=%t raw_updated=%d raw_skipped=%d parsed_updated=%d parsed_skipped=%d",
		*dryRun,
		rawUpdated,
		rawSkipped,
		parsedUpdated,
		parsedSkipped,
	)
}

func normalizeRemoteDotCoRawJobs(ctx context.Context, db *database.DB, dryRun bool) (int, int) {
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT id, COALESCE(raw_json, '')
		   FROM raw_us_jobs
		  WHERE source = ?
		    AND raw_json IS NOT NULL
		    AND btrim(raw_json) != ''
		  ORDER BY id ASC`,
		remotedotcoSource,
	)
	if err != nil {
		log.Fatalf("select raw_us_jobs: %v", err)
	}
	defer rows.Close()

	updated := 0
	skipped := 0
	for rows.Next() {
		var rawID int64
		var rawJSONText string
		if err := rows.Scan(&rawID, &rawJSONText); err != nil {
			log.Fatalf("scan raw_us_jobs: %v", err)
		}

		payload := map[string]any{}
		if err := json.Unmarshal([]byte(rawJSONText), &payload); err != nil {
			log.Printf("skip raw_job_id=%d reason=invalid_raw_json err=%v", rawID, err)
			skipped++
			continue
		}

		beforeStates, exists := extractStateStrings(payload["locationUSStates"])
		if !exists {
			skipped++
			continue
		}
		afterStates := normalizeUSStates(beforeStates)
		if reflect.DeepEqual(beforeStates, afterStates) {
			skipped++
			continue
		}

		payload["locationUSStates"] = afterStates
		rawJSONBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("skip raw_job_id=%d reason=marshal_failed err=%v", rawID, err)
			skipped++
			continue
		}

		log.Printf("raw_job_update raw_job_id=%d before=%v after=%v", rawID, beforeStates, afterStates)
		if dryRun {
			updated++
			continue
		}
		if _, err := db.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET raw_json = ? WHERE id = ?`, string(rawJSONBytes), rawID); err != nil {
			log.Printf("skip raw_job_id=%d reason=update_failed err=%v", rawID, err)
			skipped++
			continue
		}
		updated++
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("raw_us_jobs rows error: %v", err)
	}
	return updated, skipped
}

func normalizeRemoteDotCoParsedJobs(ctx context.Context, db *database.DB, dryRun bool) (int, int) {
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT p.id, p.raw_us_job_id, COALESCE(p.location_us_states::text, '')
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE r.source = ?
		  ORDER BY p.id ASC`,
		remotedotcoSource,
	)
	if err != nil {
		log.Fatalf("select parsed_jobs: %v", err)
	}
	defer rows.Close()

	updated := 0
	skipped := 0
	for rows.Next() {
		var parsedJobID int64
		var rawJobID int64
		var statesJSON string
		if err := rows.Scan(&parsedJobID, &rawJobID, &statesJSON); err != nil {
			log.Fatalf("scan parsed_jobs: %v", err)
		}

		beforeStates := parseJSONArrayStrings(statesJSON)
		afterStates := normalizeUSStates(beforeStates)
		if reflect.DeepEqual(beforeStates, afterStates) {
			skipped++
			continue
		}

		log.Printf("parsed_job_update parsed_job_id=%d raw_job_id=%d before=%v after=%v", parsedJobID, rawJobID, beforeStates, afterStates)
		if dryRun {
			updated++
			continue
		}

		now := time.Now().UTC().Format(time.RFC3339Nano)
		if len(afterStates) == 0 {
			if _, err := db.SQL.ExecContext(
				ctx,
				`UPDATE parsed_jobs SET location_us_states = NULL, updated_at = ? WHERE id = ?`,
				now,
				parsedJobID,
			); err != nil {
				log.Printf("skip parsed_job_id=%d reason=update_failed err=%v", parsedJobID, err)
				skipped++
				continue
			}
		} else {
			statesJSONBytes, err := json.Marshal(afterStates)
			if err != nil {
				log.Printf("skip parsed_job_id=%d reason=marshal_failed err=%v", parsedJobID, err)
				skipped++
				continue
			}
			if _, err := db.SQL.ExecContext(
				ctx,
				`UPDATE parsed_jobs SET location_us_states = ?::jsonb, updated_at = ? WHERE id = ?`,
				string(statesJSONBytes),
				now,
				parsedJobID,
			); err != nil {
				log.Printf("skip parsed_job_id=%d reason=update_failed err=%v", parsedJobID, err)
				skipped++
				continue
			}
		}
		updated++
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("parsed_jobs rows error: %v", err)
	}
	return updated, skipped
}

func extractStateStrings(value any) ([]string, bool) {
	switch typed := value.(type) {
	case nil:
		return nil, false
	case []string:
		return append([]string{}, typed...), true
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				continue
			}
			out = append(out, text)
		}
		return out, true
	case string:
		if strings.TrimSpace(typed) == "" {
			return []string{}, true
		}
		return parseJSONArrayStrings(typed), true
	default:
		return nil, false
	}
}

func parseJSONArrayStrings(value string) []string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "null" {
		return []string{}
	}
	out := []string{}
	if err := json.Unmarshal([]byte(trimmed), &out); err == nil {
		return out
	}
	anyItems := []any{}
	if err := json.Unmarshal([]byte(trimmed), &anyItems); err == nil {
		out = make([]string, 0, len(anyItems))
		for _, item := range anyItems {
			text, ok := item.(string)
			if !ok {
				continue
			}
			out = append(out, text)
		}
		return out
	}
	return []string{}
}

func normalizeUSStates(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	allowed := map[string]struct{}{}
	for _, state := range locationnorm.USStateNames() {
		allowed[state] = struct{}{}
	}
	normalized := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		state := locationnorm.NormalizeUSStateName(value)
		if state == "" {
			continue
		}
		if _, ok := allowed[state]; !ok {
			continue
		}
		if _, ok := seen[state]; ok {
			continue
		}
		seen[state] = struct{}{}
		normalized = append(normalized, state)
	}
	return normalized
}
