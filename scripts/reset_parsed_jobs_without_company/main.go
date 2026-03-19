package main

import (
	"context"
	"flag"
	"log"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
)

type orphanParsedJobRow struct {
	ParsedJobID int64
	RawJobID    int64
	Source      string
	URL         string
}

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
	rows, err := loadOrphanParsedJobs(ctx, db)
	if err != nil {
		log.Fatalf("load orphan parsed jobs: %v", err)
	}
	if len(rows) == 0 {
		log.Printf("done dry_run=%t orphan_parsed_jobs=0 raw_jobs_reset=0", *dryRun)
		return
	}

	for _, row := range rows {
		log.Printf(
			"orphan_parsed_job parsed_job_id=%d raw_job_id=%d source=%s url=%s",
			row.ParsedJobID,
			row.RawJobID,
			row.Source,
			row.URL,
		)
	}

	if *dryRun {
		log.Printf("done dry_run=true orphan_parsed_jobs=%d raw_jobs_reset=%d", len(rows), len(rows))
		return
	}

	if err := resetOrphanParsedJobs(ctx, db, rows); err != nil {
		log.Fatalf("reset orphan parsed jobs: %v", err)
	}

	log.Printf("done dry_run=false orphan_parsed_jobs=%d raw_jobs_reset=%d", len(rows), len(rows))
}

func loadOrphanParsedJobs(ctx context.Context, db *database.DB) ([]orphanParsedJobRow, error) {
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT p.id, p.raw_us_job_id, r.source, r.url
           FROM parsed_jobs p
           JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
          WHERE p.company_id IS NULL
          ORDER BY p.id ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []orphanParsedJobRow{}
	for rows.Next() {
		var item orphanParsedJobRow
		if err := rows.Scan(&item.ParsedJobID, &item.RawJobID, &item.Source, &item.URL); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func resetOrphanParsedJobs(ctx context.Context, db *database.DB, rows []orphanParsedJobRow) error {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, row := range rows {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE raw_us_jobs
                SET is_ready = false,
                    is_parsed = false,
                    raw_json = NULL
              WHERE id = ?`,
			row.RawJobID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM parsed_jobs WHERE id = ?`,
			row.ParsedJobID,
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
