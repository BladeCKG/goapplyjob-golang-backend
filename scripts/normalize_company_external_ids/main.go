package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"log"
	"strings"
)

const (
	externalCompanyIDPrefix = "gaj("
	externalCompanyIDSuffix = ")gaj"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "preview changes without committing")
	limit := flag.Int("limit", 0, "limit rows to update")
	flag.Parse()

	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	query := `SELECT id, external_company_id
	            FROM parsed_companies
	           WHERE external_company_id IS NOT NULL
	           ORDER BY id ASC`
	if *limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", *limit)
	}

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("query companies: %v", err)
	}

	type companyRow struct {
		id                int64
		externalCompanyID sql.NullString
	}
	companies := make([]companyRow, 0, 1024)

	for rows.Next() {
		var id int64
		var externalCompanyID sql.NullString
		if err := rows.Scan(&id, &externalCompanyID); err != nil {
			log.Fatalf("scan company: %v", err)
		}
		companies = append(companies, companyRow{id: id, externalCompanyID: externalCompanyID})
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows error: %v", err)
	}
	if err := rows.Close(); err != nil {
		log.Fatalf("close rows: %v", err)
	}

	updated := 0
	unchanged := 0
	for _, company := range companies {
		normalized := normalizeExternalCompanyIDs(company.externalCompanyID.String)
		if normalized == company.externalCompanyID.String || normalized == "" {
			unchanged++
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE parsed_companies SET external_company_id = ?, updated_at = NOW() WHERE id = ?`, normalized, company.id); err != nil {
			log.Fatalf("update company_id=%d: %v", company.id, err)
		}
		updated++
		log.Printf("normalized company_id=%d from=%q to=%q", company.id, company.externalCompanyID.String, normalized)
	}

	mode := "APPLIED"
	if *dryRun {
		if err := tx.Rollback(); err != nil {
			log.Fatalf("rollback: %v", err)
		}
		mode = "DRY-RUN"
	} else {
		if err := tx.Commit(); err != nil {
			log.Fatalf("commit: %v", err)
		}
	}

	fmt.Printf("[%s] updated=%d unchanged=%d\n", mode, updated, unchanged)
}

func normalizeExternalCompanyIDs(raw string) string {
	ordered := make([]string, 0, 4)
	seen := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		token := externalCompanyIDToken(part)
		if token == "" {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		ordered = append(ordered, token)
	}
	return strings.Join(ordered, ",")
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
