package main

import (
	"context"
	"log"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/raw"
	"goapplyjob-golang-backend/internal/workerlog"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	closeLogFile, err := workerlog.Setup("SKIPPABLE_RECHECK_LOG_FILE", "recheck_skippable_jobs.log")
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

	svc := raw.New(raw.Config{}, db)
	checkedCount, clearedCount, err := svc.RecheckSkippable(context.Background(), cfg.SkippableRecheckBatchSize)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("recheck-skippable completed checked=%d cleared=%d", checkedCount, clearedCount)
}
