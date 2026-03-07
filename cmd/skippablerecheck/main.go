package main

import (
	"context"
	"log"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/raw"
)

func main() {
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	svc := raw.New(db)
	checkedCount, clearedCount, err := svc.RecheckSkippable(context.Background(), cfg.SkippableRecheckBatchSize)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("recheck-skippable completed checked=%d cleared=%d", checkedCount, clearedCount)
}
