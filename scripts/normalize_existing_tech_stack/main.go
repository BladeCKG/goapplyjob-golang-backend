package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
)

var techStackDropValues = map[string]struct{}{
	"n/a": {}, "na": {}, "none": {}, "null": {}, "unknown": {}, "tbd": {},
}

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	mappingPath := flag.String("mapping-json", "", "required path to JSON object mapping original tech stack to normalized value")
	sourcesCSV := flag.String("sources", "builtin,workable", "optional comma-separated sources (example: builtin,workable,hiringcafe)")
	dryRun := flag.Bool("dry-run", false, "preview only; do not write updates")
	batchSize := flag.Int("batch-size", 500, "commit every N updates")
	flag.Parse()
	if strings.TrimSpace(*mappingPath) == "" {
		log.Fatal("--mapping-json is required")
	}

	techStackAliases, err := loadTechStackAliases(*mappingPath)
	if err != nil {
		log.Fatal(err)
	}

	db, err := database.Open(config.Getenv("DATABASE_URL", "file:page_extract.db?_foreign_keys=on"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sources := splitSources(*sourcesCSV)
	scanned, updated, err := run(context.Background(), db.SQL, sources, techStackAliases, *dryRun, max(*batchSize, 1))
	if err != nil {
		log.Fatal(err)
	}
	mode := "APPLIED"
	if *dryRun {
		mode = "DRY-RUN"
	}
	label := sources
	if len(label) == 0 {
		label = []string{"<all>"}
	}
	fmt.Printf("[%s] scanned=%d updated=%d sources=%v\n", mode, scanned, updated, label)
}

func run(ctx context.Context, db *database.SQLConn, sources []string, techStackAliases map[string]string, dryRun bool, batchSize int) (int, int, error) {
	query := `SELECT p.id, p.tech_stack
		FROM parsed_jobs p
		JOIN raw_us_jobs r ON r.id = p.raw_us_job_id`
	args := make([]any, 0, len(sources))
	if len(sources) > 0 {
		placeholders := make([]string, 0, len(sources))
		for _, source := range sources {
			placeholders = append(placeholders, "?")
			args = append(args, source)
		}
		query += ` WHERE r.source IN (` + strings.Join(placeholders, ", ") + `)`
	}
	query += ` ORDER BY p.id ASC`

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	scanned := 0
	updated := 0
	for rows.Next() {
		var id int64
		var rawTech sql.NullString
		if err := rows.Scan(&id, &rawTech); err != nil {
			return scanned, updated, err
		}
		scanned++
		current := parseTechStack(rawTech)
		next := normalizeTechStack(current, techStackAliases)
		currentJSON, _ := json.Marshal(current)
		nextJSON, _ := json.Marshal(next)
		if string(currentJSON) == string(nextJSON) {
			continue
		}
		updated++
		if dryRun {
			continue
		}
		var nextValue any
		if len(next) > 0 {
			nextValue = string(nextJSON)
		}
		if _, err := db.ExecContext(ctx, `UPDATE parsed_jobs SET tech_stack = ?, updated_at = ? WHERE id = ?`, nextValue, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
			return scanned, updated, err
		}
		if updated%batchSize == 0 {
			// no-op checkpoint for parity with batched scripts
		}
	}
	if err := rows.Err(); err != nil {
		return scanned, updated, err
	}
	return scanned, updated, nil
}

func parseTechStack(value sql.NullString) []string {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	var list []string
	if err := json.Unmarshal([]byte(value.String), &list); err == nil {
		return list
	}
	var anyList []any
	if err := json.Unmarshal([]byte(value.String), &anyList); err != nil {
		return nil
	}
	out := make([]string, 0, len(anyList))
	for _, item := range anyList {
		text, _ := item.(string)
		if strings.TrimSpace(text) != "" {
			out = append(out, strings.TrimSpace(text))
		}
	}
	return out
}

func normalizeTechStackValue(value string, techStackAliases map[string]string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.Trim(normalized, "\"'")
	normalized = regexpReplace(`\([^)]*\)`, normalized, "")
	if strings.Contains(normalized, "(") && !strings.Contains(normalized, ")") {
		normalized = strings.SplitN(normalized, "(", 2)[0]
	}
	normalized = strings.ReplaceAll(normalized, ")", "")
	normalized = strings.ReplaceAll(normalized, "]", "")
	normalized = strings.ReplaceAll(normalized, "}", "")
	normalized = regexpReplace(`\s*/\s*`, normalized, "/")
	normalized = regexpReplace(`\s*-\s*`, normalized, "-")
	normalized = regexpReplace(`[;,:]+$`, normalized, "")
	normalized = regexpReplace(`\s+`, normalized, " ")
	normalized = strings.Trim(normalized, " .-_/")
	if normalized == "" {
		return ""
	}
	lowered := strings.ToLower(normalized)
	if _, ok := techStackDropValues[lowered]; ok {
		return ""
	}
	if alias, ok := techStackAliases[lowered]; ok {
		return strings.TrimSpace(alias)
	}
	return normalized
}

func normalizeTechStack(values []string, techStackAliases map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		next := normalizeTechStackValue(value, techStackAliases)
		if next == "" {
			continue
		}
		key := strings.ToLower(next)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, next)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func loadTechStackAliases(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mapping json: %w", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("parse mapping json: %w", err)
	}
	out := make(map[string]string, len(payload))
	for key, value := range payload {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		if normalizedKey == "" {
			continue
		}
		switch typed := value.(type) {
		case string:
			out[normalizedKey] = strings.TrimSpace(typed)
		case nil:
			out[normalizedKey] = ""
		default:
			out[normalizedKey] = strings.TrimSpace(fmt.Sprint(typed))
		}
	}
	return out, nil
}

func splitSources(csv string) []string {
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

func regexpReplace(pattern, value, replacement string) string {
	re := regexpCache(pattern)
	return re.ReplaceAllString(value, replacement)
}

var reCache = map[string]*regexp.Regexp{}

func regexpCache(pattern string) *regexp.Regexp {
	if existing, ok := reCache[pattern]; ok {
		return existing
	}
	compiled := regexp.MustCompile(pattern)
	reCache[pattern] = compiled
	return compiled
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
