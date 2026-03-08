package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/raw"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	batchSize := config.GetenvInt("RAW_JOB_WORKER_BATCH_SIZE", 320)
	if batchSize < 1 {
		batchSize = 1
	}
	pollSeconds := config.GetenvInt("RAW_JOB_WORKER_POLL_SECONDS", 5)
	if pollSeconds < 1 {
		pollSeconds = 5
	}
	runOnce := config.GetenvBool("RAW_JOB_RUN_ONCE", false)
	retries429 := config.GetenvInt("RAW_JOB_HTTP_429_RETRIES", 3)
	if retries429 < 0 {
		retries429 = 0
	}
	retryDelaySeconds := config.GetenvInt("RAW_JOB_HTTP_429_RETRY_DELAY_SECONDS", 10)
	if retryDelaySeconds < 0 {
		retryDelaySeconds = 0
	}

	svc := raw.New(db)
	svc.ReadHTML = makeReadHTMLWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)

	for {
		processed, err := svc.ProcessPending(context.Background(), batchSize)
		if err != nil {
			log.Fatal(err)
		}
		if runOnce {
			log.Printf("raw-us-job-worker run-once completed processed=%d", processed)
			return
		}
		time.Sleep(time.Duration(pollSeconds) * time.Second)
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
				return "", 0, err
			}
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
			_ = resp.Body.Close()
			if readErr != nil {
				return "", resp.StatusCode, readErr
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
