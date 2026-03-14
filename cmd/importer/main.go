package main

import (
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/importer"
	"goapplyjob-golang-backend/internal/workerlog"
	"log"
	"time"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	closeLogFile, err := workerlog.Setup("RAW_IMPORT_LOG_FILE", "raw_import_worker.log")
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
	intervalMinutes := config.GetenvFloat("RAW_IMPORT_INTERVAL_MINUTES", 1)
	if intervalMinutes < 0 {
		intervalMinutes = 1
	}
	sleepDuration := time.Duration(intervalMinutes * float64(time.Minute))
	if sleepDuration < time.Second {
		sleepDuration = time.Second
	}
	batchSize := config.GetenvInt("RAW_IMPORT_BATCH_SIZE", 1000)
	if batchSize < 1 {
		batchSize = 1
	}
	payloadsPerCycle := config.GetenvInt("RAW_IMPORT_PAYLOADS_PER_CYCLE", 40)
	if payloadsPerCycle < 1 {
		payloadsPerCycle = 1
	}
	enabledSources := config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship")
	runOnce := config.GetenvBool("RAW_IMPORT_RUN_ONCE", false)
	errorBackoffSeconds := config.GetenvInt("WORKER_ERROR_BACKOFF_SECONDS", 10)
	if errorBackoffSeconds < 1 {
		errorBackoffSeconds = 1
	}
	workerCount := config.GetenvInt("RAW_IMPORT_WORKER_COUNT", 2)
	if workerCount < 1 {
		workerCount = 1
	}

	svc := importer.New(importer.Config{
		IntervalMinutes:     intervalMinutes,
		SleepDuration:       sleepDuration,
		BatchSize:           batchSize,
		PayloadsPerCycle:    payloadsPerCycle,
		EnabledSources:      enabledSources,
		RunOnce:             runOnce,
		ErrorBackoffSeconds: errorBackoffSeconds,
		WorkerCount:         workerCount,
	}, db)

	if err := svc.RunForever(); err != nil {
		log.Fatal(err)
	}
}
