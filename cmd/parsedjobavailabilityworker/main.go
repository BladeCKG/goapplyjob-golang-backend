package main

import (
	"context"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/parsedjobavailability"
	"goapplyjob-golang-backend/internal/raw"
	"goapplyjob-golang-backend/internal/scraper"
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

	retries429 := config.GetenvInt("RAW_JOB_HTTP_429_RETRIES", 3)
	if retries429 < 0 {
		retries429 = 0
	}
	retryDelaySeconds := config.GetenvInt("RAW_JOB_HTTP_429_RETRY_DELAY_SECONDS", 10)
	if retryDelaySeconds < 0 {
		retryDelaySeconds = 0
	}

	svc := parsedjobavailability.New(parsedjobavailability.Config{
		BatchSize:           config.GetenvInt("PARSED_JOB_AVAILABILITY_BATCH_SIZE", 200),
		PollSeconds:         config.GetenvFloat("PARSED_JOB_AVAILABILITY_POLL_SECONDS", 5),
		RunOnce:             config.GetenvBool("PARSED_JOB_AVAILABILITY_RUN_ONCE", false),
		ErrorBackoffSeconds: config.GetenvInt("WORKER_ERROR_BACKOFF_SECONDS", 10),
		WorkerCount:         config.GetenvInt("PARSED_JOB_AVAILABILITY_WORKER_COUNT", 4),
		FetchTimeoutSeconds: cfg.ParsedJobAvailabilityFetchTimeoutSeconds,
	}, db)
	svc.EnabledSources = config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship")
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
		if tlsErr == nil && source == remotedotco.Source {
			return tlsFetcher.ReadHTMLWithHeaders(ctx, targetURL, map[string]string{
				"Cookie": remotedotco.Cookie,
			})
		}
		return fetcher.ReadHTMLWith429Retry(ctx, targetURL, max429Retries, retryDelay)
	}
}
