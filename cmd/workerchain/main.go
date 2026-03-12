package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"

	"goapplyjob-golang-backend/internal/app"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/importer"
	"goapplyjob-golang-backend/internal/parsed"
	"goapplyjob-golang-backend/internal/raw"
	"goapplyjob-golang-backend/internal/scraper"
	"goapplyjob-golang-backend/internal/watcher"
	"goapplyjob-golang-backend/internal/workerlog"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	closeLogFile, err := workerlog.Setup("WORKER_CHAIN_LOG_FILE", "worker_chain.log")
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

	go func() {
		const (
			defaultRemoteRocketshipURL        = "https://www.remoterocketship.com/us/sitemap_job_openings_us_1.xml"
			defaultBuiltinBaseURL             = "https://builtin.com/jobs/remote?country=USA&allLocations=true&page={page}"
			defaultWorkableAPIURL             = "https://jobs.workable.com/api/v1/jobs?location=United States&workplace=remote&day_range=1"
			defaultRemotiveSitemapURLTemplate = "https://remotive.com/sitemap-job-postings-{partition}.xml"
			defaultRemotiveSitemapMaxIndex    = 10
			defaultRemotiveSitemapMinIndex    = 1
			defaultDailyRemoteBaseURL         = "https://dailyremote.com/?location_country=United+States&sort_by=time&page={page}"
			defaultHiringCafeSearchURL        = "https://hiring.cafe/api/search-jobs?s="
			defaultHiringCafeCountURL         = "https://hiring.cafe/api/search-jobs/get-total-count?s="
		)

		enabledSources := config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship")
		errorBackoffSeconds := config.GetenvInt("WORKER_ERROR_BACKOFF_SECONDS", 10)
		if errorBackoffSeconds < 1 {
			errorBackoffSeconds = 1
		}
		chainSleepSeconds := config.GetenvInt("WORKER_CHAIN_SLEEP_SECONDS", 5)
		if chainSleepSeconds < 0 {
			chainSleepSeconds = 0
		}
		runOnce := config.GetenvBool("WORKER_CHAIN_RUN_ONCE", false)

		watcherSvc := watcher.New(watcher.Config{
			Enabled:                         config.GetenvBool("WATCH_ENABLED", true),
			RemoteRocketshipUSJobSitemapURL: defaultRemoteRocketshipURL,
			IntervalMinutes:                 config.GetenvFloat("WATCH_INTERVAL_MINUTES", 1),
			SampleKB:                        config.GetenvInt("WATCH_SAMPLE_KB", 40),
			TimeoutSeconds:                  config.GetenvFloat("WATCH_TIMEOUT_SECONDS", 30),
			BuiltinBaseURL:                  defaultBuiltinBaseURL,
			BuiltinMaxPage:                  config.GetenvInt("WATCH_BUILTIN_MAX_PAGE", 1000),
			BuiltinPagesPerCycle:            config.GetenvInt("WATCH_BUILTIN_PAGES_PER_CYCLE", 25),
			BuiltinCheckpointPages:          config.GetenvInt("WATCH_BUILTIN_STATE_CHECKPOINT_PAGES", 5),
			BuiltinFetchIntervalSeconds:     config.GetenvFloat("WATCH_BUILTIN_FETCH_INTERVAL_SECONDS", 0),
			Builtin429RetryCount:            config.GetenvInt("WATCH_BUILTIN_429_RETRY_COUNT", 3),
			Builtin429BackoffSeconds:        config.GetenvFloat("WATCH_BUILTIN_429_BACKOFF_SECONDS", 10),
			WorkableAPIURL:                  defaultWorkableAPIURL,
			WorkablePageLimit:               config.GetenvInt("WATCH_WORKABLE_PAGE_LIMIT", 100),
			RemotiveSitemapURLTemplate:      defaultRemotiveSitemapURLTemplate,
			RemotiveSitemapMaxIndex:         config.GetenvInt("WATCH_REMOTIVE_SITEMAP_MAX_INDEX", defaultRemotiveSitemapMaxIndex),
			RemotiveSitemapMinIndex:         config.GetenvInt("WATCH_REMOTIVE_SITEMAP_MIN_INDEX", defaultRemotiveSitemapMinIndex),
			DailyRemoteBaseURL:              defaultDailyRemoteBaseURL,
			DailyRemoteMaxPage:              config.GetenvInt("WATCH_DAILYREMOTE_MAX_PAGE", 5000),
			DailyRemotePagesPerCycle:        config.GetenvInt("WATCH_DAILYREMOTE_PAGES_PER_CYCLE", 300),
			HiringCafeSearchAPIURL:          defaultHiringCafeSearchURL,
			HiringCafeTotalCountURL:         defaultHiringCafeCountURL,
			HiringCafePageSize:              config.GetenvInt("WATCH_HIRINGCAFE_PAGE_SIZE", 200),
			EnabledSources:                  enabledSources,
		}, db)

		importerInterval := config.GetenvFloat("RAW_IMPORT_INTERVAL_MINUTES", 1)
		if importerInterval < 0 {
			importerInterval = 1
		}
		importerSleep := time.Duration(importerInterval * float64(time.Minute))
		if importerSleep < time.Second {
			importerSleep = time.Second
		}
		importerSvc := importer.New(importer.Config{
			IntervalMinutes:     importerInterval,
			SleepDuration:       importerSleep,
			BatchSize:           config.GetenvInt("RAW_IMPORT_BATCH_SIZE", 1000),
			PayloadsPerCycle:    config.GetenvInt("RAW_IMPORT_PAYLOADS_PER_CYCLE", 40),
			EnabledSources:      enabledSources,
			RunOnce:             true,
			ErrorBackoffSeconds: errorBackoffSeconds,
		}, db)

		rawSvc := raw.New(raw.Config{
			BatchSize:             config.GetenvInt("RAW_JOB_WORKER_BATCH_SIZE", 320),
			PollSeconds:           config.GetenvInt("RAW_JOB_WORKER_POLL_SECONDS", 5),
			RunOnce:               true,
			ErrorBackoffSeconds:   errorBackoffSeconds,
			RetentionDays:         config.GetenvInt("RAW_JOB_RETENTION_DAYS", 365),
			RetentionCleanupBatch: config.GetenvInt("RAW_JOB_RETENTION_CLEANUP_BATCH", 5000),
		}, db)
		rawSvc.EnabledSources = enabledSources
		retries429 := config.GetenvInt("RAW_JOB_HTTP_429_RETRIES", 3)
		if retries429 < 0 {
			retries429 = 0
		}
		retryDelaySeconds := config.GetenvInt("RAW_JOB_HTTP_429_RETRY_DELAY_SECONDS", 10)
		if retryDelaySeconds < 0 {
			retryDelaySeconds = 0
		}
		rawSvc.ReadHTML = makeReadHTMLWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)

		parsedSvc := parsed.New(parsed.Config{
			BatchSize:           config.GetenvInt("PARSED_JOB_WORKER_BATCH_SIZE", 260),
			PollSeconds:         config.GetenvFloat("PARSED_JOB_WORKER_POLL_SECONDS", 5),
			RunOnce:             true,
			ErrorBackoffSeconds: errorBackoffSeconds,
		}, db)
		parsedSvc.EnabledSources = enabledSources

		for {
			if err := watcherSvc.RunOnce(); err != nil {
				log.Printf("worker-chain watcher_failed error=%v", err)
				if runOnce {
					return
				}
				time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
				continue
			}
			if err := importerSvc.RunOnce(); err != nil {
				log.Printf("worker-chain importer_failed error=%v", err)
				if runOnce {
					return
				}
				time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
				continue
			}
			if _, err := rawSvc.RunOnce(context.Background()); err != nil {
				log.Printf("worker-chain raw_failed error=%v", err)
				if runOnce {
					return
				}
				time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
				continue
			}
			if _, err := parsedSvc.RunOnce(context.Background()); err != nil {
				log.Printf("worker-chain parsed_failed error=%v", err)
				if runOnce {
					return
				}
				time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
				continue
			}
			if runOnce {
				return
			}
			if chainSleepSeconds > 0 {
				time.Sleep(time.Duration(chainSleepSeconds) * time.Second)
			}
		}
	}()

	router := app.NewHealthRouter(db)
	if err := router.Run(cfg.HTTPHost + ":" + cfg.HTTPPort); err != nil {
		log.Fatalf("run server: %v", err)
	}
}

func makeReadHTMLWith429Retry(max429Retries int, retryDelay time.Duration) raw.ReadHTMLFunc {
	fetcher, err := scraper.NewTLSClientFetcher(scraper.TLSClientConfig{
		Timeout: 30 * time.Second,
	})
	if err != nil {
		log.Printf("worker-chain tls-client init failed, fallback to net/http: %v", err)
		return makeReadHTMLWithHTTP429Retry(max429Retries, retryDelay)
	}
	return func(targetURL string) (string, int, error) {
		return fetcher.ReadHTMLWith429Retry(targetURL, max429Retries, retryDelay)
	}
}

func makeReadHTMLWithHTTP429Retry(max429Retries int, retryDelay time.Duration) raw.ReadHTMLFunc {
	userAgent := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
	httpClient := &http.Client{Timeout: 30 * time.Second}
	return func(targetURL string) (string, int, error) {
		attempt := 0
		for {
			req, err := http.NewRequest(http.MethodGet, targetURL, nil)
			if err != nil {
				return "", 0, err
			}
			req.Header.Set("User-Agent", userAgent)
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
