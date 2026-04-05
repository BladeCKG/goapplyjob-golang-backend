package main

import (
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/parsed"
	"goapplyjob-golang-backend/internal/workerlog"
	"log"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	closeLogFile, err := workerlog.Setup("PARSED_JOB_WORKER_LOG_FILE", "parsed_job_worker.log")
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

	svc := parsed.New(parsed.Config{
		BatchSize:               config.GetenvInt("PARSED_JOB_WORKER_BATCH_SIZE", 260),
		PollSeconds:             config.GetenvFloat("PARSED_JOB_WORKER_POLL_SECONDS", 5),
		RunOnce:                 config.GetenvBool("PARSED_JOB_RUN_ONCE", false),
		ErrorBackoffSeconds:     config.GetenvInt("WORKER_ERROR_BACKOFF_SECONDS", 10),
		WorkerCount:             config.GetenvInt("PARSED_JOB_WORKER_COUNT", 1),
		CategorySignalTokensURL: cfg.CategorySignalTokensURL,
		DuplicateJobURLRulesURL: cfg.DuplicateJobURLRulesURL,
		TechStackCatalogURL:     cfg.TechStackCatalogURL,
	}, db)
	svc.EnabledSources = config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship")
	if err := svc.RunForever(); err != nil {
		log.Fatal(err)
	}
}
