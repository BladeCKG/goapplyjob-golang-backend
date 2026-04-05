package main

import (
	"context"
	"errors"
	"goapplyjob-golang-backend/internal/app"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/constants"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/importer"
	"goapplyjob-golang-backend/internal/parsed"
	"goapplyjob-golang-backend/internal/parsedaiclassifier"
	"goapplyjob-golang-backend/internal/parsedjobavailability"
	"goapplyjob-golang-backend/internal/raw"
	"goapplyjob-golang-backend/internal/scraper"
	"goapplyjob-golang-backend/internal/sources/remotedotco"
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
			defaultBuiltinBaseURL             = "https://builtin.com/jobs/remote?allLocations=true&page={page}"
			defaultWorkableAPIURL             = "https://jobs.workable.com/api/v1/jobs?workplace=remote&day_range=1"
			defaultRemotiveSitemapURLTemplate = "https://remotive.com/sitemap-job-postings-{partition}.xml"
			defaultRemotiveSitemapMaxIndex    = 10
			defaultRemotiveSitemapMinIndex    = 1
			defaultDailyRemoteBaseURL         = "https://dailyremote.com/?sort_by=time&page={page}"
			defaultRemoteDotCoSitemapURL      = "https://remote.co/latest-jobs-sitemap.xml"
			defaultHiringCafeSearchURL        = "https://hiring.cafe/api/search-jobs?s="
			defaultHiringCafeCountURL         = "https://hiring.cafe/api/search-jobs/get-total-count?s="
		)
		defaultRemoteRocketshipURLs := []string{
			"https://www.remoterocketship.com/us/sitemap_job_openings_us_1.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_worldwide.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_rest_of_world.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_mx.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_es.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_pl.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_nl.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_it.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_pt.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_in.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_au.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_ie.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_ch.xml",
			"https://www.remoterocketship.com/sitemap_job_openings_at.xml",
		}

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
				parsedAIClassifierEnabled := config.GetenvBool("PARSED_JOB_AI_CLASSIFIER_ENABLED", false)
				watcherSvc := watcher.New(watcher.Config{
					Enabled:                          config.GetenvBool("WATCH_ENABLED", true),
					RemoteRocketshipUSJobSitemapURLs: defaultRemoteRocketshipURLs,
					IntervalMinutes:                  config.GetenvFloat("WATCH_INTERVAL_MINUTES", 1),
					SampleKB:                         config.GetenvInt("WATCH_SAMPLE_KB", 40),
					TimeoutSeconds:                   config.GetenvFloat("WATCH_TIMEOUT_SECONDS", 30),
					BuiltinBaseURL:                   defaultBuiltinBaseURL,
					BuiltinMaxPage:                   config.GetenvInt("WATCH_BUILTIN_MAX_PAGE", 1000),
					BuiltinPagesPerCycle:             config.GetenvInt("WATCH_BUILTIN_PAGES_PER_CYCLE", 200),
					BuiltinCheckpointPages:           config.GetenvInt("WATCH_BUILTIN_STATE_CHECKPOINT_PAGES", 5),
					BuiltinFetchIntervalSeconds:      config.GetenvFloat("WATCH_BUILTIN_FETCH_INTERVAL_SECONDS", 0),
					Builtin429RetryCount:             config.GetenvInt("WATCH_BUILTIN_429_RETRY_COUNT", 3),
					Builtin429BackoffSeconds:         config.GetenvFloat("WATCH_BUILTIN_429_BACKOFF_SECONDS", 10),
					WorkableAPIURL:                   defaultWorkableAPIURL,
					WorkablePageLimit:                config.GetenvInt("WATCH_WORKABLE_PAGE_LIMIT", 100),
					RemotiveSitemapURLTemplate:       defaultRemotiveSitemapURLTemplate,
					RemotiveSitemapMaxIndex:          config.GetenvInt("WATCH_REMOTIVE_SITEMAP_MAX_INDEX", defaultRemotiveSitemapMaxIndex),
					RemotiveSitemapMinIndex:          config.GetenvInt("WATCH_REMOTIVE_SITEMAP_MIN_INDEX", defaultRemotiveSitemapMinIndex),
					DailyRemoteBaseURL:               defaultDailyRemoteBaseURL,
					DailyRemoteMaxPage:               config.GetenvInt("WATCH_DAILYREMOTE_MAX_PAGE", 5000),
					DailyRemotePagesPerCycle:         config.GetenvInt("WATCH_DAILYREMOTE_PAGES_PER_CYCLE", 300),
					RemoteDotCoSitemapURL:            config.Getenv("WATCH_REMOTEDOTCO_SITEMAP_URL", defaultRemoteDotCoSitemapURL),
					HiringCafeSearchAPIURL:           defaultHiringCafeSearchURL,
					HiringCafeTotalCountURL:          defaultHiringCafeCountURL,
					HiringCafePageSize:               config.GetenvInt("WATCH_HIRINGCAFE_PAGE_SIZE", 200),
					EnabledSources:                   enabledSources,
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
					BatchSize:             config.GetenvInt("RAW_JOB_WORKER_BATCH_SIZE", 500),
					PollSeconds:           config.GetenvInt("RAW_JOB_WORKER_POLL_SECONDS", 5),
					RunOnce:               true,
					ErrorBackoffSeconds:   errorBackoffSeconds,
					FetchTimeoutSeconds:   config.GetenvInt("RAW_JOB_FETCH_TIMEOUT_SECONDS", 45),
					RetentionDays:         config.GetenvInt("RAW_JOB_RETENTION_DAYS", 365),
					RetentionCleanupBatch: config.GetenvInt("RAW_JOB_RETENTION_CLEANUP_BATCH", 5000),
					WorkerCount:           config.GetenvInt("RAW_JOB_WORKER_COUNT", 4),
				}, db)
				rawSvc.EnabledSources = enabledSources
				rawSvc.ReadHTMLForSource = makeReadHTMLForSourceWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)

				parsedSvc := parsed.New(parsed.Config{
					BatchSize:               config.GetenvInt("PARSED_JOB_WORKER_BATCH_SIZE", 200),
					PollSeconds:             config.GetenvFloat("PARSED_JOB_WORKER_POLL_SECONDS", 5),
					RunOnce:                 true,
					ErrorBackoffSeconds:     errorBackoffSeconds,
					WorkerCount:             config.GetenvInt("PARSED_JOB_WORKER_COUNT", 1),
					CategorySignalTokensURL: cfg.CategorySignalTokensURL,
					TechStackCatalogURL:     cfg.TechStackCatalogURL,
				}, db)
				parsedSvc.EnabledSources = enabledSources
				var parsedAIClassifierSvc *parsedaiclassifier.Service
				if parsedAIClassifierEnabled {
					parsedAIClassifierSvc = parsedaiclassifier.New(parsedaiclassifier.Config{
						BatchSize:            config.GetenvInt("PARSED_JOB_AI_CLASSIFIER_BATCH_SIZE", 200),
						PollSeconds:          config.GetenvFloat("PARSED_JOB_AI_CLASSIFIER_POLL_SECONDS", 5),
						RunOnce:              true,
						ErrorBackoffSeconds:  errorBackoffSeconds,
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
					parsedAIClassifierSvc.EnabledSources = enabledSources
				}
				parsedAvailabilitySvc := parsedjobavailability.New(parsedjobavailability.Config{
					BatchSize:           config.GetenvInt("PARSED_JOB_AVAILABILITY_BATCH_SIZE", 200),
					PollSeconds:         config.GetenvFloat("PARSED_JOB_AVAILABILITY_POLL_SECONDS", 5),
					RunOnce:             true,
					ErrorBackoffSeconds: errorBackoffSeconds,
					WorkerCount:         config.GetenvInt("PARSED_JOB_AVAILABILITY_WORKER_COUNT", 4),
				}, db)
				parsedAvailabilitySvc.EnabledSources = enabledSources
				readHTMLForSource := makeReadHTMLForSourceWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)
				parsedAvailabilitySvc.ReadHTMLForSource = func(ctx context.Context, source, targetURL string) (string, int, error) {
					return readHTMLForSource(ctx, source, targetURL)
				}

				hadError := false
				errorDetails := make([]string, 0, 6)
				log.Printf("worker-chain cycle_start")
				stepTimeout := time.Duration(stepTimeoutSeconds) * time.Second
				type stepResult struct {
					name  string
					count int
					err   error
				}
				results := make(chan stepResult, 6)
				go func() {
					err := runStepWithTimeout(constants.WorkerNameWatcher, stepTimeout, func(ctx context.Context) error {
						return watcherSvc.RunOnceWithContext(ctx)
					})
					results <- stepResult{name: constants.WorkerNameWatcher, err: err}
				}()
				go func() {
					err := runStepWithTimeout(constants.WorkerNameImporter, stepTimeout, func(ctx context.Context) error {
						return importerSvc.RunOnceWithContext(ctx)
					})
					results <- stepResult{name: constants.WorkerNameImporter, err: err}
				}()
				go func() {
					count, err := runCountStepWithTimeout(stepTimeout, func(ctx context.Context) (int, error) {
						return rawSvc.RunOnce(ctx)
					})
					results <- stepResult{name: constants.WorkerNameRaw, count: count, err: err}
				}()
				go func() {
					count, err := runCountStepWithTimeout(stepTimeout, func(ctx context.Context) (int, error) {
						return parsedSvc.RunOnce(ctx)
					})
					results <- stepResult{name: constants.WorkerNameParsed, count: count, err: err}
				}()
				go func() {
					if !parsedAIClassifierEnabled || parsedAIClassifierSvc == nil {
						log.Printf("worker-chain parsed_ai_classifier_disabled")
						results <- stepResult{name: constants.WorkerNameParsedAIClassifier, count: 0, err: nil}
						return
					}
					count, err := runCountStepWithTimeout(stepTimeout, func(ctx context.Context) (int, error) {
						return parsedAIClassifierSvc.RunOnce(ctx)
					})
					results <- stepResult{name: constants.WorkerNameParsedAIClassifier, count: count, err: err}
				}()
				go func() {
					count, err := runCountStepWithTimeout(stepTimeout, func(ctx context.Context) (int, error) {
						return parsedAvailabilitySvc.RunOnce(ctx)
					})
					results <- stepResult{name: constants.WorkerNameParsedAvailability, count: count, err: err}
				}()
				for i := 0; i < 6; i++ {
					res := <-results
					switch res.name {
					case constants.WorkerNameWatcher:
						if res.err != nil {
							log.Printf("worker-chain watcher_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "watcher="+res.err.Error())
						} else {
							log.Printf("worker-chain watcher_done")
						}
					case constants.WorkerNameImporter:
						if res.err != nil {
							log.Printf("worker-chain importer_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "importer="+res.err.Error())
						} else {
							log.Printf("worker-chain importer_done")
						}
					case constants.WorkerNameRaw:
						if res.err != nil {
							log.Printf("worker-chain raw_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "raw="+res.err.Error())
						} else {
							log.Printf("worker-chain raw_done processed=%d", res.count)
						}
					case constants.WorkerNameParsed:
						if res.err != nil {
							log.Printf("worker-chain parsed_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "parsed="+res.err.Error())
						} else {
							log.Printf("worker-chain parsed_done processed=%d", res.count)
						}
					case constants.WorkerNameParsedAIClassifier:
						if res.err != nil {
							log.Printf("worker-chain parsed_ai_classifier_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "parsed_ai_classifier="+res.err.Error())
						} else {
							log.Printf("worker-chain parsed_ai_classifier_done processed=%d", res.count)
						}
					case constants.WorkerNameParsedAvailability:
						if res.err != nil {
							log.Printf("worker-chain parsed_job_availability_failed error=%v", res.err)
							hadError = true
							errorDetails = append(errorDetails, "parsed_job_availability="+res.err.Error())
						} else {
							log.Printf("worker-chain parsed_job_availability_done processed=%d", res.count)
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

func makeReadHTMLForSourceWith429Retry(max429Retries int, retryDelay time.Duration) raw.ReadHTMLForSourceFunc {
	fetcher, err := scraper.NewCloudscraperFetcher(scraper.CloudscraperConfig{
		Timeout: 30 * time.Second,
	})
	if err != nil {
		log.Printf("worker-chain cloudscraper init failed: %v", err)
		return func(ctx context.Context, _ string, targetURL string) (string, int, error) {
			if ctx == nil {
				ctx = context.Background()
			}
			return "", -1, errors.New("cloudscraper unavailable")
		}
	}
	tlsFetcher, tlsErr := scraper.NewTLSClientFetcher(scraper.TLSClientConfig{Timeout: 30 * time.Second})
	if tlsErr != nil {
		log.Printf("worker-chain tls-client init failed: %v", tlsErr)
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
