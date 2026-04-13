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
		BatchSize:               cfg.ParsedJobWorkerBatchSize,
		PollSeconds:             cfg.ParsedJobWorkerPollSeconds,
		RunOnce:                 cfg.ParsedJobRunOnce,
		ErrorBackoffSeconds:     cfg.WorkerErrorBackoffSeconds,
		WorkerCount:             cfg.ParsedJobWorkerCount,
		CategorySignalTokensURL: cfg.CategorySignalTokensURL,
		DuplicateJobURLRulesURL: cfg.DuplicateJobURLRulesURL,
		TechStackCatalogURL:     cfg.TechStackCatalogURL,
	}, db)
	svc.EnabledSources = cfg.EnabledSources
	if err := svc.RunForever(); err != nil {
		log.Fatal(err)
	}
}
