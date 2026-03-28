package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/currency"
)

func main() {
	fromParsedJobID := flag.Int64("from-parsed-job-id", 0, "optional inclusive lower parsed job id bound")
	toParsedJobID := flag.Int64("to-parsed-job-id", 0, "optional inclusive upper parsed job id bound")
	batchSize := flag.Int("batch-size", 500, "select/update batch size")
	dryRun := flag.Bool("dry-run", false, "log changes without writing updates")
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

	scanned, updated, err := backfillSalaryCurrencySymbols(context.Background(), db, *fromParsedJobID, *toParsedJobID, *batchSize, *dryRun)
	if err != nil {
		log.Fatal(err)
	}

	mode := "APPLIED"
	if *dryRun {
		mode = "DRY-RUN"
	}
	log.Printf("done mode=%s scanned=%d updated=%d", mode, scanned, updated)
}

type salaryCurrencyRow struct {
	ParsedJobID         int64
	SalaryCurrencyCode  string
	SalaryCurrencySymbol string
}

func backfillSalaryCurrencySymbols(ctx context.Context, db *database.DB, fromParsedJobID, toParsedJobID int64, batchSize int, dryRun bool) (int, int, error) {
	scanned := 0
	updated := 0
	lastParsedJobID := int64(-1)
	if fromParsedJobID > 0 {
		lastParsedJobID = fromParsedJobID - 1
	}

	for {
		rows, err := selectSalaryCurrencyBatch(ctx, db, lastParsedJobID, toParsedJobID, batchSize)
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

			inferredSymbol := currency.SymbolForCode(row.SalaryCurrencyCode)
			if inferredSymbol == "" || inferredSymbol == row.SalaryCurrencySymbol {
				continue
			}

			updated++
			log.Printf("parsed_job_update parsed_job_id=%d salary_currency_code=%q before_symbol=%q after_symbol=%q", row.ParsedJobID, row.SalaryCurrencyCode, row.SalaryCurrencySymbol, inferredSymbol)
			if dryRun {
				continue
			}

			if _, err := tx.ExecContext(
				ctx,
				`UPDATE parsed_jobs
				    SET salary_currency_symbol = ?, updated_at = ?
				  WHERE id = ?
				    AND salary_currency_code IS NOT NULL
				    AND salary_currency_code <> ''
				    AND (salary_currency_symbol IS NULL OR salary_currency_symbol = '')`,
				inferredSymbol,
				time.Now().UTC().Format(time.RFC3339Nano),
				row.ParsedJobID,
			); err != nil {
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

func selectSalaryCurrencyBatch(ctx context.Context, db *database.DB, lastParsedJobID, toParsedJobID int64, batchSize int) ([]salaryCurrencyRow, error) {
	query := `SELECT id, salary_currency_code, COALESCE(salary_currency_symbol, '')
		FROM parsed_jobs
		WHERE id > ?
		  AND salary_currency_code IS NOT NULL
		  AND salary_currency_code <> ''
		  AND (salary_currency_symbol IS NULL OR salary_currency_symbol = '')`
	args := []any{lastParsedJobID}
	if toParsedJobID > 0 {
		query += ` AND id <= ?`
		args = append(args, toParsedJobID)
	}
	query += ` ORDER BY id ASC LIMIT ?`
	args = append(args, batchSize)

	rows, err := db.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select parsed_jobs batch: %w", err)
	}
	defer rows.Close()

	out := make([]salaryCurrencyRow, 0, batchSize)
	for rows.Next() {
		var row salaryCurrencyRow
		if err := rows.Scan(&row.ParsedJobID, &row.SalaryCurrencyCode, &row.SalaryCurrencySymbol); err != nil {
			return nil, fmt.Errorf("scan parsed_jobs batch: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("parsed_jobs batch rows error: %w", err)
	}
	return out, nil
}
