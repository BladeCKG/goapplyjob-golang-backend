package main

import (
	"context"
	"errors"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/raw"
	"goapplyjob-golang-backend/internal/scraper"
	"goapplyjob-golang-backend/internal/sources/flexjobs"
	"goapplyjob-golang-backend/internal/sources/remotedotco"
	"goapplyjob-golang-backend/internal/workerlog"
	"log"
	"time"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	closeLogFile, err := workerlog.Setup("RAW_JOB_WORKER_LOG_FILE", "raw_job_worker.log")
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

	retries429 := cfg.RawJobHTTP429Retries
	if retries429 < 0 {
		retries429 = 0
	}
	retryDelaySeconds := cfg.RawJobHTTP429RetryDelaySeconds
	if retryDelaySeconds < 0 {
		retryDelaySeconds = 0
	}

	svc := raw.New(raw.Config{
		BatchSize:             cfg.RawJobWorkerBatchSize,
		PollSeconds:           cfg.RawJobWorkerPollSeconds,
		RunOnce:               cfg.RawJobRunOnce,
		ErrorBackoffSeconds:   cfg.WorkerErrorBackoffSeconds,
		FetchTimeoutSeconds:   cfg.RawJobFetchTimeoutSeconds,
		RetentionDays:         cfg.RawJobRetentionDays,
		RetentionCleanupBatch: cfg.RawJobRetentionCleanupBatch,
		WorkerCount:           cfg.RawJobWorkerCount,
	}, db)
	svc.EnabledSources = cfg.EnabledSources
	svc.ReadHTMLForSource = makeReadHTMLForSourceWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)
	if err := svc.RunForever(); err != nil {
		log.Fatal(err)
	}
}

func makeReadHTMLForSourceWith429Retry(max429Retries int, retryDelay time.Duration) raw.ReadHTMLForSourceFunc {
	fetcher, err := scraper.NewCloudscraperFetcher(scraper.CloudscraperConfig{
		Timeout: 30 * time.Second,
	})
	if err != nil {
		log.Printf("rawjobworker cloudscraper init failed: %v", err)
		return func(ctx context.Context, _ string, targetURL string) (string, int, error) {
			if ctx == nil {
				ctx = context.Background()
			}
			return "", -1, errors.New("cloudscraper unavailable")
		}
	}
	tlsFetcher, tlsErr := scraper.NewTLSClientFetcher(scraper.TLSClientConfig{Timeout: 30 * time.Second})
	if tlsErr != nil {
		log.Printf("rawjobworker tls-client init failed: %v", tlsErr)
	}

	return func(ctx context.Context, source, targetURL string) (string, int, error) {
		if tlsErr == nil && source == flexjobs.Source {
			return tlsFetcher.ReadHTML(ctx, targetURL)
		}
		if tlsErr == nil && source == remotedotco.Source {
			return tlsFetcher.ReadHTMLWithHeaders(ctx, targetURL, map[string]string{
				"Cookie": remotedotco.Cookie,
			})
		}
		return fetcher.ReadHTMLWith429Retry(ctx, targetURL, max429Retries, retryDelay)
	}
}
