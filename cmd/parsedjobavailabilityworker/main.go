package main

import (
	"context"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/parsedjobavailability"
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
	closeLogFile, err := workerlog.Setup("PARSED_JOB_AVAILABILITY_LOG_FILE", "parsed_job_availability_worker.log")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = closeLogFile() }()

	cfg := config.Load()
	if !cfg.ParsedJobAvailabilityEnabled {
		log.Printf("parsed-job-availability-worker disabled")
		return
	}
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

	svc := parsedjobavailability.New(parsedjobavailability.Config{
		BatchSize:           cfg.ParsedJobAvailabilityBatchSize,
		PollSeconds:         cfg.ParsedJobAvailabilityPollSeconds,
		RunOnce:             cfg.ParsedJobAvailabilityRunOnce,
		ErrorBackoffSeconds: cfg.WorkerErrorBackoffSeconds,
		WorkerCount:         cfg.ParsedJobAvailabilityWorkerCount,
		FetchTimeoutSeconds: cfg.ParsedJobAvailabilityFetchTimeoutSeconds,
	}, db)
	svc.EnabledSources = cfg.EnabledSources
	readHTMLForSource := makeReadHTMLForSourceWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)
	svc.ReadHTMLForSource = func(ctx context.Context, source, targetURL string) (string, int, error) {
		return readHTMLForSource(ctx, source, targetURL)
	}

	if err := svc.RunForever(); err != nil {
		log.Fatal(err)
	}
}

func makeReadHTMLForSourceWith429Retry(max429Retries int, retryDelay time.Duration) raw.ReadHTMLForSourceFunc {
	fetcher, err := scraper.NewCloudscraperFetcher(scraper.CloudscraperConfig{
		Timeout: 30 * time.Second,
	})
	if err != nil {
		log.Printf("parsed-job-availability-worker cloudscraper init failed: %v", err)
		return func(ctx context.Context, _ string, targetURL string) (string, int, error) {
			if ctx == nil {
				ctx = context.Background()
			}
			return "", -1, err
		}
	}
	tlsFetcher, tlsErr := scraper.NewTLSClientFetcher(scraper.TLSClientConfig{Timeout: 30 * time.Second})
	if tlsErr != nil {
		log.Printf("parsed-job-availability-worker tls-client init failed: %v", tlsErr)
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
