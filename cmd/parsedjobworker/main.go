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
	pollSeconds := config.GetenvFloat("PARSED_JOB_WORKER_POLL_SECONDS", 5)
	if pollSeconds < 1 {
		pollSeconds = 1
	}
	runOnce := config.GetenvBool("PARSED_JOB_RUN_ONCE", false)
	errorBackoffSeconds := config.GetenvInt("WORKER_ERROR_BACKOFF_SECONDS", 10)
	if errorBackoffSeconds < 1 {
		errorBackoffSeconds = 1
	}

	svc := parsed.New(db)
	svc.EnabledSources = config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship")
	for {
		processed, err := svc.ProcessPending(context.Background(), batchSize)
		if err != nil {
			log.Printf("parsed-job-worker cycle_failed error=%v", err)
			if runOnce {
				return
			}
			time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
			continue
		}
		if runOnce {
			if processed == 0 {
				log.Printf("parsed-job-worker run-once completed: no pending parsed rows")
			} else {
				log.Printf("parsed-job-worker run-once completed processed=%d", processed)
			}
			return
		}
		time.Sleep(time.Duration(pollSeconds * float64(time.Second)))
	}
}
