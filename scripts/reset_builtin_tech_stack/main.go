package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/scraper"
	"goapplyjob-golang-backend/internal/sources/builtin"
)

type rowData struct {
	ParsedID int64
	RawID    int64
	URL      string
	RawJSON  string
}

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	fetcher, err := scraper.NewCloudscraperFetcher(scraper.CloudscraperConfig{
		Timeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("cloudscraper init: %v", err)
	}

	const minParsedID = 140232
	rows, err := db.SQL.QueryContext(
		context.Background(),
		`SELECT p.id, p.raw_us_job_id, r.url, COALESCE(r.raw_json::text, '')
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE p.id > ? AND r.source = 'builtin'
		  ORDER BY p.id ASC`,
		minParsedID,
	)
	if err != nil {
		log.Fatalf("select rows: %v", err)
	}
	defer rows.Close()

	updated := 0
	skipped := 0
	for rows.Next() {
		var row rowData
		if err := rows.Scan(&row.ParsedID, &row.RawID, &row.URL, &row.RawJSON); err != nil {
			log.Fatalf("scan row: %v", err)
		}
		if strings.TrimSpace(row.URL) == "" {
			skipped++
			continue
		}

		htmlText, status, err := fetcher.ReadHTML(context.Background(), row.URL)
		if err != nil || status < 200 || status >= 300 || strings.TrimSpace(htmlText) == "" {
			log.Printf("skip parsed_id=%d raw_id=%d url=%s status=%d err=%v", row.ParsedID, row.RawID, row.URL, status, err)
			skipped++
			continue
		}

		payload, parseErr := builtin.ExtractJobFromHTML(htmlText, row.URL)
		if parseErr != nil {
			log.Printf("skip parsed_id=%d raw_id=%d url=%s parse_error=%v", row.ParsedID, row.RawID, row.URL, parseErr)
			skipped++
			continue
		}
		techStack := extractTechStack(payload)
		techStackJSON, _ := json.Marshal(techStack)

		rawPayload := map[string]any{}
		if strings.TrimSpace(row.RawJSON) != "" {
			_ = json.Unmarshal([]byte(row.RawJSON), &rawPayload)
		}
		if len(rawPayload) == 0 {
			rawPayload = payload
		} else {
			rawPayload["techStack"] = techStack
		}
		rawJSONBytes, err := json.Marshal(rawPayload)
		if err != nil {
			log.Printf("skip parsed_id=%d raw_id=%d url=%s marshal_raw_json_error=%v", row.ParsedID, row.RawID, row.URL, err)
			skipped++
			continue
		}

		if _, err := db.SQL.ExecContext(
			context.Background(),
			`UPDATE raw_us_jobs SET raw_json = ? WHERE id = ?`,
			string(rawJSONBytes),
			row.RawID,
		); err != nil {
			log.Printf("update raw_us_jobs failed parsed_id=%d raw_id=%d err=%v", row.ParsedID, row.RawID, err)
			skipped++
			continue
		}

		if _, err := db.SQL.ExecContext(
			context.Background(),
			`UPDATE parsed_jobs SET tech_stack = ?, updated_at = ? WHERE id = ?`,
			string(techStackJSON),
			time.Now().UTC().Format(time.RFC3339Nano),
			row.ParsedID,
		); err != nil {
			log.Printf("update parsed_jobs failed parsed_id=%d raw_id=%d err=%v", row.ParsedID, row.RawID, err)
			skipped++
			continue
		}

		updated++
		log.Printf("updated parsed_id=%d raw_id=%d tech_stack_count=%d tech_stack=%v", row.ParsedID, row.RawID, len(techStack), techStack)
		time.Sleep(300 * time.Millisecond)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows error: %v", err)
	}
	log.Printf("done updated=%d skipped=%d", updated, skipped)
}

func extractTechStack(payload map[string]any) []string {
	raw := payload["techStack"]
	out := []string{}
	seen := map[string]struct{}{}
	switch items := raw.(type) {
	case []string:
		for _, item := range items {
			out = addTechStackItem(out, seen, item)
		}
	case []any:
		for _, item := range items {
			if text, ok := item.(string); ok {
				out = addTechStackItem(out, seen, text)
			}
		}
	}
	return out
}

func addTechStackItem(out []string, seen map[string]struct{}, value string) []string {
	cleaned := strings.TrimSpace(value)
	if cleaned == "" {
		return out
	}
	key := strings.ToLower(cleaned)
	if _, ok := seen[key]; ok {
		return out
	}
	seen[key] = struct{}{}
	return append(out, cleaned)
}
