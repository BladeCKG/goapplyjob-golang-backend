package main

import (
	"context"
	"log"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/parsed"
	"goapplyjob-golang-backend/internal/workerlog"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	closeLogFile, err := workerlog.Setup("PARSED_FRESHNESS_WORKER_LOG_FILE", "parsed_freshness_worker.log")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = closeLogFile() }()
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	svc := parsed.New(db)
	checkedCount, staleCount, err := svc.ResetStaleParsed(context.Background(), 100)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("parsed-freshness-worker completed checked_rows=%d stale_rows_reset=%d", checkedCount, staleCount)
}
