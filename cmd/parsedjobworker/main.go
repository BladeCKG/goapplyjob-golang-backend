package main

import (
	"context"
	"log"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/parsed"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	batchSize := config.GetenvInt("PARSED_JOB_WORKER_BATCH_SIZE", 260)
	if batchSize < 1 {
		batchSize = 1
	}
	pollSeconds := config.GetenvInt("PARSED_JOB_WORKER_POLL_SECONDS", 5)
	if pollSeconds < 1 {
		pollSeconds = 5
	}
	runOnce := config.GetenvBool("PARSED_JOB_RUN_ONCE", false)

	svc := parsed.New(db)
	for {
		processed, err := svc.ProcessPending(context.Background(), batchSize)
		if err != nil {
			log.Fatal(err)
		}
		if runOnce {
			log.Printf("parsed-job-worker run-once completed processed=%d", processed)
			return
		}
		time.Sleep(time.Duration(pollSeconds) * time.Second)
	}
}
