package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"log"
)

type companyRow struct {
	ID                   int64
	ExternalCompanyID    sql.NullString
	IndustriesJSON       sql.NullString
	IndustrySpecialJSON  string
}

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

	updated, err := run(context.Background(), db, *dryRun, *limit)
	if err != nil {
		log.Fatal(err)
	}

	mode := "APPLIED"
	if *dryRun {
		mode = "DRY-RUN"
	}
	log.Printf("done mode=%s updated=%d", mode, updated)
}

func run(ctx context.Context, db *database.DB, dryRun bool, limit int) (int, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	rows, err := selectRows(ctx, tx, limit)
	if err != nil {
		return 0, err
	}

	updated := 0
	for _, row := range rows {
		log.Printf(
			"parsed_company_update id=%d external_company_id=%q industries_before=%q industry_specialities_before=%q",
			row.ID,
			row.ExternalCompanyID.String,
			row.IndustriesJSON.String,
			row.IndustrySpecialJSON,
		)
		if dryRun {
			updated++
			continue
		}

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE parsed_companies
			    SET industries = ?::jsonb,
			        industry_specialities = '[]'::jsonb,
			        updated_at = NOW()
			  WHERE id = ?`,
			row.IndustrySpecialJSON,
			row.ID,
		); err != nil {
			return updated, fmt.Errorf("update parsed_company id=%d: %w", row.ID, err)
		}
		updated++
	}

	if dryRun {
		if err := tx.Rollback(); err != nil {
			return updated, fmt.Errorf("rollback: %w", err)
		}
		return updated, nil
	}

	if err := tx.Commit(); err != nil {
		return updated, fmt.Errorf("commit: %w", err)
	}
	return updated, nil
}

func selectRows(ctx context.Context, tx *database.Tx, limit int) ([]companyRow, error) {
	query := `SELECT id,
	                 external_company_id,
	                 industries::text,
	                 industry_specialities::text
	            FROM parsed_companies
	           WHERE industry_specialities IS NOT NULL
	             AND industry_specialities::text != '[]'
	             AND (external_company_id IS NULL OR external_company_id NOT LIKE '%remoterocketship%')
	           ORDER BY id ASC`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query parsed_companies: %w", err)
	}
	defer rows.Close()

	out := make([]companyRow, 0, 256)
	for rows.Next() {
		var row companyRow
		if err := rows.Scan(&row.ID, &row.ExternalCompanyID, &row.IndustriesJSON, &row.IndustrySpecialJSON); err != nil {
			return nil, fmt.Errorf("scan parsed_companies: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("parsed_companies rows error: %w", err)
	}
	return out, nil
}
