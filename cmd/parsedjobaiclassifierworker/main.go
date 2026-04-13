package main

import (
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/parsedaiclassifier"
	"goapplyjob-golang-backend/internal/workerlog"
	"log"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	closeLogFile, err := workerlog.Setup("PARSED_JOB_AI_CLASSIFIER_LOG_FILE", "parsed_job_ai_classifier_worker.log")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = closeLogFile() }()
	cfg := config.Load()
	if !cfg.ParsedJobAIClassifierEnabled {
		log.Printf("parsed-job-ai-classifier-worker disabled")
		return
	}
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	svc := parsedaiclassifier.New(parsedaiclassifier.Config{
		BatchSize:            cfg.ParsedJobAIClassifierBatchSize,
		PollSeconds:          cfg.ParsedJobAIClassifierPollSeconds,
		RunOnce:              cfg.ParsedJobAIClassifierRunOnce,
		ErrorBackoffSeconds:  cfg.WorkerErrorBackoffSeconds,
		Provider:             cfg.AIClassifierProvider,
		Providers:            cfg.AIClassifierProviders,
		GroqAPIKey:           cfg.GroqAPIKey,
		GroqAPIKeys:          cfg.GroqAPIKeys,
		GroqModel:            cfg.GroqModel,
		GroqModels:           cfg.GroqModels,
		GroqBaseURL:          cfg.GroqBaseURL,
		GroqPromptSource:     cfg.GroqClassifierPromptSource,
		OllamaConfigured:     cfg.OllamaConfigured,
		OllamaBaseURL:        cfg.OllamaBaseURL,
		OllamaModel:          cfg.OllamaModel,
		OllamaModels:         cfg.OllamaModels,
		OllamaAPIKey:         cfg.OllamaAPIKey,
		OllamaAPIKeys:        cfg.OllamaAPIKeys,
		OllamaPromptSource:   cfg.OllamaClassifierPromptSource,
		CerebrasAPIKey:       cfg.CerebrasAPIKey,
		CerebrasAPIKeys:      cfg.CerebrasAPIKeys,
		CerebrasModel:        cfg.CerebrasModel,
		CerebrasModels:       cfg.CerebrasModels,
		CerebrasBaseURL:      cfg.CerebrasBaseURL,
		CerebrasPromptSource: cfg.CerebrasClassifierPromptSource,
		OpenAIAPIKey:         cfg.OpenAIAPIKey,
		OpenAIAPIKeys:        cfg.OpenAIAPIKeys,
		OpenAIModel:          cfg.OpenAIModel,
		OpenAIModels:         cfg.OpenAIModels,
		OpenAIBaseURL:        cfg.OpenAIBaseURL,
		OpenAIPromptSource:   cfg.OpenAIClassifierPromptSource,
	}, db)
	svc.EnabledSources = cfg.EnabledSources

	if err := svc.RunForever(); err != nil {
		log.Fatal(err)
	}
}
