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
	intervalMinutes := cfg.RawImportIntervalMinutes
	if intervalMinutes < 0 {
		intervalMinutes = 1
	}
	sleepDuration := time.Duration(intervalMinutes * float64(time.Minute))
	if sleepDuration < time.Second {
		sleepDuration = time.Second
	}
	batchSize := cfg.RawImportBatchSize
	if batchSize < 1 {
		batchSize = 1
	}
	payloadsPerCycle := cfg.RawImportPayloadsPerCycle
	if payloadsPerCycle < 1 {
		payloadsPerCycle = 1
	}
	enabledSources := cfg.EnabledSources
	runOnce := cfg.RawImportRunOnce
	errorBackoffSeconds := cfg.WorkerErrorBackoffSeconds
	if errorBackoffSeconds < 1 {
		errorBackoffSeconds = 1
	}
	workerCount := cfg.RawImportWorkerCount
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
