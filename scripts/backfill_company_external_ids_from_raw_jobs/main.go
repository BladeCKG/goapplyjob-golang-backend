package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"log"
	"strconv"
	"strings"
)

const (
	externalCompanyIDPrefix = "gaj("
	externalCompanyIDSuffix = ")gaj"
)

type candidateRow struct {
	CompanyID   int64
	CompanyName string
	ParsedJobID int64
	RawJobID    int64
	Source      string
	URL         string
	RawJSON     string
}

type updateRow struct {
	CompanyID         int64
	CompanyName       string
	ParsedJobID       int64
	RawJobID          int64
	Source            string
	URL               string
	ExternalCompanyID string
}

type companyRecord struct {
	ID                int64
	ExternalCompanyID sql.NullString
	Name              sql.NullString
	Slug              sql.NullString
	Tagline           sql.NullString
	FoundedYear       sql.NullString
	HomePageURL       sql.NullString
	LinkedInURL       sql.NullString
	EmployeeRange     sql.NullString
	ProfilePicURL     sql.NullString
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
	candidates, err := loadCandidates(ctx, db)
	if err != nil {
		log.Fatalf("load candidates: %v", err)
	}

	updates, skippedNoID, skippedInvalidJSON := buildUpdates(candidates, *limit)
	if len(updates) == 0 {
		log.Printf(
			"done dry_run=%t updated=0 skipped_no_id=%d skipped_invalid_json=%d",
			*dryRun,
			skippedNoID,
			skippedInvalidJSON,
		)
		return
	}

	for _, row := range updates {
		log.Printf(
			"backfill company_id=%d company=%q parsed_job_id=%d raw_job_id=%d source=%s external_company_id=%q url=%s",
			row.CompanyID,
			row.CompanyName,
			row.ParsedJobID,
			row.RawJobID,
			row.Source,
			row.ExternalCompanyID,
			row.URL,
		)
	}

	if *dryRun {
		log.Printf(
			"done dry_run=true updated=%d skipped_no_id=%d skipped_invalid_json=%d",
			len(updates),
			skippedNoID,
			skippedInvalidJSON,
		)
		return
	}

	applied, err := applyUpdates(ctx, db, updates)
	if err != nil {
		log.Fatalf("apply updates: %v", err)
	}

	log.Printf(
		"done dry_run=false updated=%d skipped_no_id=%d skipped_invalid_json=%d",
		applied,
		skippedNoID,
		skippedInvalidJSON,
	)
}

func loadCandidates(ctx context.Context, db *database.DB) ([]candidateRow, error) {
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT c.id,
		        COALESCE(c.name, ''),
		        p.id,
		        r.id,
		        COALESCE(r.source, ''),
		        COALESCE(r.url, ''),
		        COALESCE(r.raw_json::text, '')
		   FROM parsed_companies c
		   JOIN parsed_jobs p ON p.company_id = c.id
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE (c.external_company_id IS NULL OR btrim(c.external_company_id) = '')
		    AND r.raw_json IS NOT NULL
		    AND btrim(r.raw_json::text) != ''
		  ORDER BY c.id ASC, p.updated_at DESC NULLS LAST, p.id DESC, r.id DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]candidateRow, 0, 1024)
	for rows.Next() {
		var item candidateRow
		if err := rows.Scan(
			&item.CompanyID,
			&item.CompanyName,
			&item.ParsedJobID,
			&item.RawJobID,
			&item.Source,
			&item.URL,
			&item.RawJSON,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func buildUpdates(candidates []candidateRow, limit int) ([]updateRow, int, int) {
	updates := make([]updateRow, 0, 256)
	visitedCompanies := map[int64]struct{}{}
	skippedNoID := 0
	skippedInvalidJSON := 0

	for _, item := range candidates {
		if _, seen := visitedCompanies[item.CompanyID]; seen {
			continue
		}

		externalCompanyID, ok, invalidJSON := externalCompanyIDFromRawJSON(item.Source, item.RawJSON)
		if invalidJSON {
			skippedInvalidJSON++
			continue
		}
		if !ok {
			skippedNoID++
			continue
		}

		visitedCompanies[item.CompanyID] = struct{}{}
		updates = append(updates, updateRow{
			CompanyID:         item.CompanyID,
			CompanyName:       item.CompanyName,
			ParsedJobID:       item.ParsedJobID,
			RawJobID:          item.RawJobID,
			Source:            item.Source,
			URL:               item.URL,
			ExternalCompanyID: externalCompanyIDToken(externalCompanyID),
		})
		if limit > 0 && len(updates) >= limit {
			break
		}
	}

	return updates, skippedNoID, skippedInvalidJSON
}

func externalCompanyIDFromRawJSON(source, raw string) (string, bool, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false, false
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return "", false, true
	}

	company, _ := payload["company"].(map[string]any)
	if len(company) == 0 {
		return "", false, false
	}

	value := scalarString(company["id"])
	if value == "" {
		return "", false, false
	}
	return namespaceCompanyID(source, value), true, false
}

func namespaceCompanyID(source, raw string) string {
	if strings.HasPrefix(raw, source+"_") {
		return raw
	}
	return source + "_" + raw
}

func applyUpdates(ctx context.Context, db *database.DB, updates []updateRow) (int, error) {
	applied := 0
	for _, row := range updates {
		appliedRow, err := applyUpdate(ctx, db, row)
		if err != nil {
			return applied, err
		}
		if appliedRow {
			applied++
		}
	}
	return applied, nil
}

func applyUpdate(ctx context.Context, db *database.DB, row updateRow) (bool, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	existingCompanyID, err := findExistingCompanyByExternalID(ctx, tx, row.CompanyID, row.ExternalCompanyID)
	if err != nil {
		return false, err
	}

	if existingCompanyID.Valid {
		merged, err := mergeCompanyIntoCanonical(ctx, tx, existingCompanyID.Int64, row.CompanyID, row.ExternalCompanyID)
		if err != nil {
			return false, err
		}
		if !merged {
			return false, fmt.Errorf("company_id=%d external_company_id=%q merge target disappeared", row.CompanyID, row.ExternalCompanyID)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit merged company_id=%d: %w", row.CompanyID, err)
		}
		return true, nil
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE parsed_companies
		    SET external_company_id = ?,
		        updated_at = NOW()
		  WHERE id = ?`,
		row.ExternalCompanyID,
		row.CompanyID,
	)
	if err != nil {
		return false, fmt.Errorf("update company_id=%d: %w", row.CompanyID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected company_id=%d: %w", row.CompanyID, err)
	}
	if rowsAffected == 0 {
		return false, fmt.Errorf("company_id=%d not updated", row.CompanyID)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit company_id=%d: %w", row.CompanyID, err)
	}
	return true, nil
}

func findExistingCompanyByExternalID(ctx context.Context, tx *database.Tx, companyID int64, externalCompanyID string) (sql.NullInt64, error) {
	var existingCompanyID sql.NullInt64
	err := tx.QueryRowContext(
		ctx,
		`SELECT id
		   FROM parsed_companies
		  WHERE id <> ?
		    AND external_company_id ILIKE ?
		  ORDER BY id ASC
		  LIMIT 1`,
		companyID,
		"%"+externalCompanyID+"%",
	).Scan(&existingCompanyID)
	if err != nil {
		if err == sql.ErrNoRows {
			return sql.NullInt64{}, nil
		}
		return sql.NullInt64{}, err
	}
	return existingCompanyID, nil
}

func mergeCompanyIntoCanonical(ctx context.Context, tx *database.Tx, canonicalID, duplicateCompanyID int64, externalCompanyID string) (bool, error) {
	canonical, err := loadCompanyRecord(ctx, tx, canonicalID)
	if err != nil {
		return false, err
	}
	duplicate, err := loadCompanyRecord(ctx, tx, duplicateCompanyID)
	if err != nil {
		return false, err
	}

	mergeMissingCompanyFields(&canonical, duplicate)
	if err := updateCompanyRecord(ctx, tx, canonical); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE parsed_jobs SET company_id = ? WHERE company_id = ?`, canonicalID, duplicateCompanyID); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM parsed_companies WHERE id = ?`, duplicateCompanyID); err != nil {
		return false, err
	}
	log.Printf("merged duplicate company_id=%d into canonical_company_id=%d by external_company_id=%q", duplicateCompanyID, canonicalID, externalCompanyID)
	return true, nil
}

func loadCompanyRecord(ctx context.Context, tx *database.Tx, companyID int64) (companyRecord, error) {
	var row companyRecord
	err := tx.QueryRowContext(
		ctx,
		`SELECT id, external_company_id, name, slug, tagline, founded_year, home_page_url, linkedin_url, employee_range, profile_pic_url
		   FROM parsed_companies
		  WHERE id = ?
		  LIMIT 1`,
		companyID,
	).Scan(
		&row.ID,
		&row.ExternalCompanyID,
		&row.Name,
		&row.Slug,
		&row.Tagline,
		&row.FoundedYear,
		&row.HomePageURL,
		&row.LinkedInURL,
		&row.EmployeeRange,
		&row.ProfilePicURL,
	)
	return row, err
}

func mergeMissingCompanyFields(canonical *companyRecord, duplicate companyRecord) {
	if !populatedString(canonical.Name) && populatedString(duplicate.Name) {
		canonical.Name = duplicate.Name
	}
	if !populatedString(canonical.Slug) && populatedString(duplicate.Slug) {
		canonical.Slug = duplicate.Slug
	}
	if !populatedString(canonical.Tagline) && populatedString(duplicate.Tagline) {
		canonical.Tagline = duplicate.Tagline
	}
	if !populatedString(canonical.FoundedYear) && populatedString(duplicate.FoundedYear) {
		canonical.FoundedYear = duplicate.FoundedYear
	}
	if !populatedString(canonical.HomePageURL) && populatedString(duplicate.HomePageURL) {
		canonical.HomePageURL = duplicate.HomePageURL
	}
	if !populatedString(canonical.LinkedInURL) && populatedString(duplicate.LinkedInURL) {
		canonical.LinkedInURL = duplicate.LinkedInURL
	}
	if !populatedString(canonical.EmployeeRange) && populatedString(duplicate.EmployeeRange) {
		canonical.EmployeeRange = duplicate.EmployeeRange
	}
	if !populatedString(canonical.ProfilePicURL) && populatedString(duplicate.ProfilePicURL) {
		canonical.ProfilePicURL = duplicate.ProfilePicURL
	}
}

func updateCompanyRecord(ctx context.Context, tx *database.Tx, row companyRecord) error {
	_, err := tx.ExecContext(
		ctx,
		`UPDATE parsed_companies
		    SET external_company_id = ?,
		        name = ?,
		        slug = ?,
		        tagline = ?,
		        founded_year = ?,
		        home_page_url = ?,
		        linkedin_url = ?,
		        employee_range = ?,
		        profile_pic_url = ?,
		        updated_at = NOW()
		  WHERE id = ?`,
		nullStringValue(row.ExternalCompanyID),
		nullStringValue(row.Name),
		nullStringValue(row.Slug),
		nullStringValue(row.Tagline),
		nullStringValue(row.FoundedYear),
		nullStringValue(row.HomePageURL),
		nullStringValue(row.LinkedInURL),
		nullStringValue(row.EmployeeRange),
		nullStringValue(row.ProfilePicURL),
		row.ID,
	)
	return err
}

func scalarString(value any) string {
	switch item := value.(type) {
	case string:
		return strings.TrimSpace(item)
	case float64:
		return strconv.FormatInt(int64(item), 10)
	case int:
		return strconv.Itoa(item)
	case int64:
		return strconv.FormatInt(item, 10)
	case json.Number:
		return strings.TrimSpace(item.String())
	default:
		return ""
	}
}

func externalCompanyIDToken(value string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.TrimPrefix(normalized, externalCompanyIDPrefix)
	normalized = strings.TrimSuffix(normalized, externalCompanyIDSuffix)
	if normalized == "" {
		return ""
	}
	return externalCompanyIDPrefix + normalized + externalCompanyIDSuffix
}

func populatedString(value sql.NullString) bool {
	return value.Valid && value.String != ""
}

func nullStringValue(value sql.NullString) any {
	if !populatedString(value) {
		return nil
	}
	return value.String
}
