package main

import (
	"io"
	"log"
	"net/http"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/raw"
	"goapplyjob-golang-backend/internal/workerlog"
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

	retries429 := config.GetenvInt("RAW_JOB_HTTP_429_RETRIES", 3)
	if retries429 < 0 {
		retries429 = 0
	}
	retryDelaySeconds := config.GetenvInt("RAW_JOB_HTTP_429_RETRY_DELAY_SECONDS", 10)
	if retryDelaySeconds < 0 {
		retryDelaySeconds = 0
	}

	svc := raw.New(raw.Config{
		BatchSize:             config.GetenvInt("RAW_JOB_WORKER_BATCH_SIZE", 320),
		PollSeconds:           config.GetenvInt("RAW_JOB_WORKER_POLL_SECONDS", 5),
		RunOnce:               config.GetenvBool("RAW_JOB_RUN_ONCE", false),
		ErrorBackoffSeconds:   config.GetenvInt("WORKER_ERROR_BACKOFF_SECONDS", 10),
		RetentionDays:         config.GetenvInt("RAW_JOB_RETENTION_DAYS", 365),
		RetentionCleanupBatch: config.GetenvInt("RAW_JOB_RETENTION_CLEANUP_BATCH", 5000),
	}, db)
	svc.EnabledSources = config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship")
	svc.ReadHTML = makeReadHTMLWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)
	if err := svc.RunForever(); err != nil {
		log.Fatal(err)
	}
}

func makeReadHTMLWith429Retry(max429Retries int, retryDelay time.Duration) raw.ReadHTMLFunc {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	return func(targetURL string) (string, int, error) {
		attempt := 0
		for {
			req, err := http.NewRequest(http.MethodGet, targetURL, nil)
			if err != nil {
				return "", 0, err
			}
			req.Header.Set("User-Agent", "goapplyjob-backend/raw-job-worker")
			resp, err := httpClient.Do(req)
			if err != nil {
				return "", -1, nil
			}
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
			_ = resp.Body.Close()
			if readErr != nil {
				return "", -1, nil
			}
			if resp.StatusCode != 429 || attempt >= max429Retries {
				return string(body), resp.StatusCode, nil
			}
			attempt++
			if retryDelay > 0 {
				time.Sleep(retryDelay)
			}
		}
	}
}
