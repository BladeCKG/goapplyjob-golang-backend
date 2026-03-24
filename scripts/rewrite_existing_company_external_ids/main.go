package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
)

const (
	externalCompanyIDPrefix = "gaj("
	externalCompanyIDSuffix = ")gaj"
)

var supportedSources = []string{
	"builtin",
	"dailyremote",
	"remotedotco",
	"remoterocketship",
	"remotive",
	"workable",
}

type companyRow struct {
	ID                int64
	Name              string
	ExternalCompanyID sql.NullString
}

type updateRow struct {
	CompanyID         int64
	CompanyName       string
	CurrentExternalID string
	NextExternalID    string
}

func main() {
	dryRun := flag.Bool("dry-run", false, "preview changes without committing")
	limit := flag.Int("limit", 0, "limit companies to update")
	flag.Parse()

	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	companies, err := loadCompanies(ctx, db)
	if err != nil {
		log.Fatalf("load companies: %v", err)
	}

	updates, unchanged := buildUpdates(ctx, db, companies, *limit)
	if len(updates) == 0 {
		log.Printf("done dry_run=%t updated=0 unchanged=%d", *dryRun, unchanged)
		return
	}

	for _, row := range updates {
		log.Printf(
			"rewrite company_id=%d company=%q from=%q to=%q",
			row.CompanyID,
			row.CompanyName,
			row.CurrentExternalID,
			row.NextExternalID,
		)
	}

	if *dryRun {
		log.Printf("done dry_run=true updated=%d unchanged=%d", len(updates), unchanged)
		return
	}

	applied, err := applyUpdates(ctx, db, updates)
	if err != nil {
		log.Fatalf("apply updates: %v", err)
	}

	log.Printf("done dry_run=false updated=%d unchanged=%d", applied, unchanged)
}

func loadCompanies(ctx context.Context, db *database.DB) ([]companyRow, error) {
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT id, COALESCE(name, ''), external_company_id
		   FROM parsed_companies
		  WHERE external_company_id IS NOT NULL
		    AND btrim(external_company_id) != ''
		  ORDER BY id ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]companyRow, 0, 1024)
	for rows.Next() {
		var item companyRow
		if err := rows.Scan(&item.ID, &item.Name, &item.ExternalCompanyID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func buildUpdates(ctx context.Context, db *database.DB, companies []companyRow, limit int) ([]updateRow, int) {
	updates := make([]updateRow, 0, 256)
	unchanged := 0

	for _, company := range companies {
		current := normalizeExternalCompanyIDs(company.ExternalCompanyID.String)
		next := rewriteExternalCompanyIDsForCompany(ctx, db, company.ID, current)
		if next == "" || next == current {
			unchanged++
			continue
		}

		updates = append(updates, updateRow{
			CompanyID:         company.ID,
			CompanyName:       company.Name,
			CurrentExternalID: current,
			NextExternalID:    next,
		})
		if limit > 0 && len(updates) >= limit {
			break
		}
	}

	return updates, unchanged
}

func rewriteExternalCompanyIDsForCompany(ctx context.Context, db *database.DB, companyID int64, current string) string {
	ordered := make([]string, 0, 4)
	seen := map[string]struct{}{}

	for _, part := range strings.Split(current, ",") {
		bareID := bareExternalCompanyID(part)
		if bareID == "" {
			continue
		}
		if isSourcePrefixedExternalCompanyID(bareID) {
			appendExternalCompanyIDToken(externalCompanyIDToken(bareID), seen, &ordered)
			continue
		}

		source, err := findSourceForCompanyExternalID(ctx, db, companyID, bareID)
		if err != nil {
			log.Printf("company_id=%d bare_external_company_id=%q find_source_error=%v", companyID, bareID, err)
			appendExternalCompanyIDToken(externalCompanyIDToken(bareID), seen, &ordered)
			continue
		}
		if source == "" {
			appendExternalCompanyIDToken(externalCompanyIDToken(bareID), seen, &ordered)
			continue
		}

		appendExternalCompanyIDToken(externalCompanyIDToken(source+"_"+bareID), seen, &ordered)
	}

	return strings.Join(ordered, ",")
}

func findSourceForCompanyExternalID(ctx context.Context, db *database.DB, companyID int64, bareID string) (string, error) {
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT COALESCE(r.source, ''), COALESCE(r.raw_json::text, '')
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE p.company_id = ?
		    AND r.raw_json IS NOT NULL
		    AND btrim(r.raw_json::text) != ''
		    AND r.raw_json::text ILIKE ?
		  ORDER BY p.id ASC, r.id ASC
		  LIMIT 50`,
		companyID,
		"%"+bareID+"%",
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var source sql.NullString
		var rawJSON string
		if err := rows.Scan(&source, &rawJSON); err != nil {
			return "", err
		}
		if rawJSONCompanyIDEquals(rawJSON, bareID) {
			return source.String, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return "", nil
}

func applyUpdates(ctx context.Context, db *database.DB, updates []updateRow) (int, error) {
	applied := 0
	for _, row := range updates {
		tx, err := db.SQL.BeginTx(ctx, nil)
		if err != nil {
			return applied, err
		}

		result, err := tx.ExecContext(
			ctx,
			`UPDATE parsed_companies
			    SET external_company_id = ?,
			        updated_at = NOW()
			  WHERE id = ?`,
			row.NextExternalID,
			row.CompanyID,
		)
		if err != nil {
			_ = tx.Rollback()
			return applied, fmt.Errorf("update company_id=%d: %w", row.CompanyID, err)
		}

		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return applied, fmt.Errorf("commit company_id=%d: %w", row.CompanyID, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return applied, fmt.Errorf("rows affected company_id=%d: %w", row.CompanyID, err)
		}
		if rowsAffected > 0 {
			applied++
		}
	}
	return applied, nil
}

func normalizeExternalCompanyIDs(raw string) string {
	ordered := make([]string, 0, 4)
	seen := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		appendExternalCompanyIDToken(externalCompanyIDToken(part), seen, &ordered)
	}
	return strings.Join(ordered, ",")
}

func bareExternalCompanyID(value string) string {
	normalized := value
	normalized = strings.TrimPrefix(normalized, externalCompanyIDPrefix)
	normalized = strings.TrimSuffix(normalized, externalCompanyIDSuffix)
	return normalized
}

func externalCompanyIDToken(value string) string {
	normalized := bareExternalCompanyID(value)
	if normalized == "" {
		return ""
	}
	return externalCompanyIDPrefix + normalized + externalCompanyIDSuffix
}

func appendExternalCompanyIDToken(token string, seen map[string]struct{}, ordered *[]string) {
	if token == "" {
		return
	}
	if _, ok := seen[token]; ok {
		return
	}
	seen[token] = struct{}{}
	*ordered = append(*ordered, token)
}

func isSourcePrefixedExternalCompanyID(value string) bool {
	for _, source := range supportedSources {
		if strings.HasPrefix(value, source+"_") {
			return true
		}
	}
	return false
}

func rawJSONCompanyIDEquals(rawJSON, bareID string) bool {
	if rawJSON == "" || bareID == "" {
		return false
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &payload); err != nil {
		return false
	}

	company, _ := payload["company"].(map[string]any)
	if len(company) == 0 {
		return false
	}

	return bareExternalCompanyID(scalarString(company["id"])) == bareID
}

func scalarString(value any) string {
	switch item := value.(type) {
	case string:
		return item
	case float64:
		return fmt.Sprintf("%.0f", item)
	case int:
		return fmt.Sprintf("%d", item)
	case int64:
		return fmt.Sprintf("%d", item)
	case json.Number:
		return item.String()
	default:
		return ""
	}
}
