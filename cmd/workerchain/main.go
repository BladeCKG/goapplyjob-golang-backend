package main

import (
	"context"
	"errors"
	"goapplyjob-golang-backend/internal/app"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/importer"
	"goapplyjob-golang-backend/internal/parsed"
	"goapplyjob-golang-backend/internal/raw"
	"goapplyjob-golang-backend/internal/scraper"
	"goapplyjob-golang-backend/internal/watcher"
	"goapplyjob-golang-backend/internal/workerlog"
	"log"
	"strings"
	"time"
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
			defaultRemoteDotCoSitemapURL      = "https://remote.co/latest-jobs-sitemap.xml"
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

		importerInterval := config.GetenvFloat("RAW_IMPORT_INTERVAL_MINUTES", 1)
		if importerInterval < 0 {
			importerInterval = 1
		}
		importerSleep := time.Duration(importerInterval * float64(time.Minute))
		if importerSleep < time.Second {
			importerSleep = time.Second
		}
		retries429 := config.GetenvInt("RAW_JOB_HTTP_429_RETRIES", 3)
		if retries429 < 0 {
			retries429 = 0
		}
		retryDelaySeconds := config.GetenvInt("RAW_JOB_HTTP_429_RETRY_DELAY_SECONDS", 10)
		if retryDelaySeconds < 0 {
			retryDelaySeconds = 0
		}
		stepTimeoutSeconds := config.GetenvInt("WORKER_CHAIN_STEP_TIMEOUT_SECONDS", 900)
		if stepTimeoutSeconds < 0 {
			stepTimeoutSeconds = 0
		}

		runChain := func() {
			for {
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
					RemoteDotCoSitemapURL:           config.Getenv("WATCH_REMOTEDOTCO_SITEMAP_URL", defaultRemoteDotCoSitemapURL),
					HiringCafeSearchAPIURL:          defaultHiringCafeSearchURL,
					HiringCafeTotalCountURL:         defaultHiringCafeCountURL,
					HiringCafePageSize:              config.GetenvInt("WATCH_HIRINGCAFE_PAGE_SIZE", 200),
					EnabledSources:                  enabledSources,
				}, db)

				importerSvc := importer.New(importer.Config{
					IntervalMinutes:     importerInterval,
					SleepDuration:       importerSleep,
					BatchSize:           config.GetenvInt("RAW_IMPORT_BATCH_SIZE", 1000),
					PayloadsPerCycle:    config.GetenvInt("RAW_IMPORT_PAYLOADS_PER_CYCLE", 40),
					EnabledSources:      enabledSources,
					RunOnce:             true,
					ErrorBackoffSeconds: errorBackoffSeconds,
					WorkerCount:         config.GetenvInt("RAW_IMPORT_WORKER_COUNT", 2),
				}, db)

				rawSvc := raw.New(raw.Config{
					BatchSize:             config.GetenvInt("RAW_JOB_WORKER_BATCH_SIZE", 320),
					PollSeconds:           config.GetenvInt("RAW_JOB_WORKER_POLL_SECONDS", 5),
					RunOnce:               true,
					ErrorBackoffSeconds:   errorBackoffSeconds,
					RetentionDays:         config.GetenvInt("RAW_JOB_RETENTION_DAYS", 365),
					RetentionCleanupBatch: config.GetenvInt("RAW_JOB_RETENTION_CLEANUP_BATCH", 5000),
					WorkerCount:           config.GetenvInt("RAW_JOB_WORKER_COUNT", 4),
				}, db)
				rawSvc.EnabledSources = enabledSources
				rawSvc.ReadHTML = makeReadHTMLWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)

				parsedSvc := parsed.New(parsed.Config{
					BatchSize:           config.GetenvInt("PARSED_JOB_WORKER_BATCH_SIZE", 260),
					PollSeconds:         config.GetenvFloat("PARSED_JOB_WORKER_POLL_SECONDS", 5),
					RunOnce:             true,
					ErrorBackoffSeconds: errorBackoffSeconds,
					WorkerCount:         config.GetenvInt("PARSED_JOB_WORKER_COUNT", 1),
				}, db)
				parsedSvc.EnabledSources = enabledSources

				hadError := false
				errorDetails := make([]string, 0, 4)
				log.Printf("worker-chain cycle_start")
				stepTimeout := time.Duration(stepTimeoutSeconds) * time.Second
				type stepResult struct {
					name  string
					count int
					err   error
				}
				results := make(chan stepResult, 4)
				go func() {
					err := runStepWithTimeout("watcher", stepTimeout, func(ctx context.Context) error {
						return watcherSvc.RunOnceWithContext(ctx)
					})
					results <- stepResult{name: "watcher", err: err}
				}()
				go func() {
					err := runStepWithTimeout("importer", stepTimeout, func(ctx context.Context) error {
						return importerSvc.RunOnceWithContext(ctx)
					})
					results <- stepResult{name: "importer", err: err}
				}()
				go func() {
					count, err := runCountStepWithTimeout(stepTimeout, func(ctx context.Context) (int, error) {
						return rawSvc.RunOnce(ctx)
					})
					results <- stepResult{name: "raw", count: count, err: err}
				}()
				go func() {
					count, err := runCountStepWithTimeout(stepTimeout, func(ctx context.Context) (int, error) {
						return parsedSvc.RunOnce(ctx)
					})
					results <- stepResult{name: "parsed", count: count, err: err}
				}()
				for i := 0; i < 4; i++ {
					res := <-results
					switch res.name {
					case "watcher":
						if res.err != nil {
							log.Printf("worker-chain watcher_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "watcher="+res.err.Error())
						} else {
							log.Printf("worker-chain watcher_done")
						}
					case "importer":
						if res.err != nil {
							log.Printf("worker-chain importer_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "importer="+res.err.Error())
						} else {
							log.Printf("worker-chain importer_done")
						}
					case "raw":
						if res.err != nil {
							log.Printf("worker-chain raw_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "raw="+res.err.Error())
						} else {
							log.Printf("worker-chain raw_done processed=%d", res.count)
						}
					case "parsed":
						if res.err != nil {
							log.Printf("worker-chain parsed_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "parsed="+res.err.Error())
						} else {
							log.Printf("worker-chain parsed_done processed=%d", res.count)
						}
					}
				}
				if hadError {
					log.Printf("worker-chain cycle_done had_error=true details=%s", strings.Join(errorDetails, " | "))
				} else {
					log.Printf("worker-chain cycle_done had_error=false")
				}
				if runOnce {
					return
				}
				if chainSleepSeconds > 0 {
					time.Sleep(time.Duration(chainSleepSeconds) * time.Second)
				}
			}
		}

		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("worker-chain panic_recovered error=%v", r)
					}
				}()
				runChain()
			}()
			if runOnce {
				return
			}
			time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
		}
	}()

	router := app.NewHealthRouter(db)
	if err := router.Run(cfg.HTTPHost + ":" + cfg.HTTPPort); err != nil {
		log.Fatalf("run server: %v", err)
	}
}

func runStepWithTimeout(name string, timeout time.Duration, fn func(context.Context) error) error {
	if timeout <= 0 {
		return fn(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- fn(ctx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Printf("worker-chain step_timeout step=%s timeout=%s", name, timeout)
		return ctx.Err()
	}
}

func runCountStepWithTimeout(timeout time.Duration, fn func(context.Context) (int, error)) (int, error) {
	if timeout <= 0 {
		return fn(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	type result struct {
		count int
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		count, err := fn(ctx)
		ch <- result{count: count, err: err}
	}()
	select {
	case res := <-ch:
		return res.count, res.err
	case <-ctx.Done():
		log.Printf("worker-chain step_timeout timeout=%s", timeout)
		return 0, ctx.Err()
	}
}

func makeReadHTMLWith429Retry(max429Retries int, retryDelay time.Duration) raw.ReadHTMLFunc {
	fetcher, err := scraper.NewCloudscraperFetcher(scraper.CloudscraperConfig{
		Timeout: 30 * time.Second,
	})
	if err != nil {
		log.Printf("worker-chain cloudscraper init failed: %v", err)
		return func(ctx context.Context, targetURL string) (string, int, error) {
			if ctx == nil {
				ctx = context.Background()
			}
			return "", -1, errors.New("cloudscraper unavailable")
		}
	}
	return func(ctx context.Context, targetURL string) (string, int, error) {
		return fetcher.ReadHTMLWith429Retry(ctx, targetURL, max429Retries, retryDelay)
	}
}
