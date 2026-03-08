package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"regexp"
	"strings"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
)

var countryAliases = map[string]string{
	"united states": "United States",
	"usa":           "United States",
	"us":            "United States",
	"u.s.":          "United States",
	"u.s.a.":        "United States",
	"uk":            "United Kingdom",
	"gbr":           "United Kingdom",
	"england":       "United Kingdom",
}

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	dryRun := flag.Bool("dry-run", false, "preview updates without writing")
	batchSize := flag.Int("batch-size", 500, "commit every N updates")
	sourceCSV := flag.String("sources", "builtin,workable", "comma-separated sources")
	flag.Parse()

	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sources := splitSources(*sourceCSV)
	if len(sources) == 0 {
		log.Fatal("no sources provided")
	}
	if *batchSize < 1 {
		*batchSize = 1
	}

	scanned, updated, err := normalizeRows(context.Background(), db.SQL, sources, *dryRun, *batchSize)
	if err != nil {
		log.Fatal(err)
	}
	mode := "APPLIED"
	if *dryRun {
		mode = "DRY-RUN"
	}
	log.Printf("[%s] scanned=%d updated=%d sources=%v", mode, scanned, updated, sources)
}

func normalizeRows(ctx context.Context, db *sql.DB, sources []string, dryRun bool, batchSize int) (int, int, error) {
	placeholders := make([]string, 0, len(sources))
	args := make([]any, 0, len(sources))
	for _, source := range sources {
		placeholders = append(placeholders, "?")
		args = append(args, source)
	}
	query := `SELECT p.id, p.location, p.location_city, p.location_us_states, p.employment_type
		FROM parsed_jobs p
		JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		WHERE r.source IN (` + strings.Join(placeholders, ", ") + `)
		ORDER BY p.id ASC`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	scanned := 0
	updated := 0
	for rows.Next() {
		scanned++
		var id int64
		var location, city, statesJSON, employment sql.NullString
		if err := rows.Scan(&id, &location, &city, &statesJSON, &employment); err != nil {
			return scanned, updated, err
		}
		states := []string{}
		if statesJSON.Valid && strings.TrimSpace(statesJSON.String) != "" {
			_ = json.Unmarshal([]byte(statesJSON.String), &states)
		}

		newLocation, newCity, newStatesJSON := normalizeLocationFields(location.String, city.String, states)
		newEmployment := normalizeEmploymentType(employment.String)
		if toNullString(location.String) == toNullString(newLocation) &&
			toNullString(city.String) == toNullString(newCity) &&
			toNullString(statesJSON.String) == toNullString(newStatesJSON) &&
			toNullString(employment.String) == toNullString(newEmployment) {
			continue
		}

		updated++
		if !dryRun {
			if _, err := tx.ExecContext(ctx, `UPDATE parsed_jobs SET location = ?, location_city = ?, location_us_states = ?, employment_type = ? WHERE id = ?`,
				nilIfEmpty(newLocation), nilIfEmpty(newCity), nilIfEmpty(newStatesJSON), nilIfEmpty(newEmployment), id); err != nil {
				return scanned, updated, err
			}
			if updated%batchSize == 0 {
				if err := tx.Commit(); err != nil {
					return scanned, updated, err
				}
				tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					return scanned, updated, err
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return scanned, updated, err
	}
	if !dryRun {
		if err := tx.Commit(); err != nil {
			return scanned, updated, err
		}
	}
	return scanned, updated, nil
}

func splitSources(raw string) []string {
	out := []string{}
	for _, item := range strings.Split(raw, ",") {
		value := strings.ToLower(strings.TrimSpace(item))
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func normalizeEmploymentType(value string) string {
	normalized := strings.TrimSpace(strings.ToLower(value))
	normalized = regexp.MustCompile(`[\s_]+`).ReplaceAllString(normalized, "-")
	normalized = regexp.MustCompile(`-{2,}`).ReplaceAllString(normalized, "-")
	normalized = strings.Trim(normalized, "-")
	switch normalized {
	case "fulltime", "full-time", "full time":
		return "full-time"
	case "parttime", "part-time", "part time":
		return "part-time"
	case "contract", "contractor":
		return "contract"
	case "intern", "internship":
		return "internship"
	case "temp", "temporary":
		return "temporary"
	default:
		return normalized
	}
}

func normalizeLocationFields(location, city string, states []string) (string, string, string) {
	normalizedStates := []string{}
	for _, state := range states {
		if value := normalizeStateName(state); value != "" && !containsString(normalizedStates, value) {
			normalizedStates = append(normalizedStates, value)
		}
	}
	normalizedCity := normalizeStateName(city)

	segments := strings.Split(location, "|")
	bestCountry := ""
	bestState := ""
	bestCity := normalizedCity
	for _, segment := range segments {
		parts := []string{}
		for _, part := range strings.Split(segment, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				parts = append(parts, part)
			}
		}
		if len(parts) == 0 {
			continue
		}
		country := ""
		for idx := len(parts) - 1; idx >= 0; idx-- {
			country = normalizeCountry(parts[idx])
			if country != "" {
				break
			}
		}
		if country == "" {
			continue
		}
		state := ""
		candidateCity := ""
		if len(parts) >= 2 {
			state = normalizeStateName(parts[len(parts)-2])
		}
		if len(parts) >= 3 {
			candidateCity = normalizeStateName(parts[0])
		}
		if bestCountry == "" || country == "United States" {
			bestCountry = country
			bestState = state
			if bestCity == "" {
				bestCity = candidateCity
			}
			if country == "United States" {
				break
			}
		}
	}
	if bestCountry == "United States" && bestState != "" && !containsString(normalizedStates, bestState) {
		normalizedStates = append(normalizedStates, bestState)
	}
	statesJSON := ""
	if len(normalizedStates) > 0 {
		body, _ := json.Marshal(normalizedStates)
		statesJSON = string(body)
	}
	return bestCountry, bestCity, statesJSON
}

func normalizeCountry(value string) string {
	cleaned := regexp.MustCompile(`[^a-zA-Z.\s]`).ReplaceAllString(value, "")
	cleaned = strings.TrimSpace(strings.ToLower(regexp.MustCompile(`\s+`).ReplaceAllString(cleaned, " ")))
	return countryAliases[cleaned]
}

func normalizeStateName(value string) string {
	normalized := strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(value, " "))
	if normalized == "" {
		return ""
	}
	if regexp.MustCompile(`^[A-Za-z]{2,3}$`).MatchString(normalized) {
		return strings.ToUpper(normalized)
	}
	parts := strings.Fields(strings.ToLower(normalized))
	for idx, part := range parts {
		parts[idx] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func nilIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func toNullString(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return value
}
