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
	"goapplyjob-golang-backend/internal/sources/flexjobs"
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

		enabledSources := cfg.EnabledSources
		errorBackoffSeconds := cfg.WorkerErrorBackoffSeconds
		if errorBackoffSeconds < 1 {
			errorBackoffSeconds = 1
		}
		chainSleepSeconds := cfg.WorkerChainSleepSeconds
		if chainSleepSeconds < 0 {
			chainSleepSeconds = 0
		}
		runOnce := cfg.WorkerChainRunOnce

		importerInterval := cfg.RawImportIntervalMinutes
		if importerInterval < 0 {
			importerInterval = 1
		}
		importerSleep := time.Duration(importerInterval * float64(time.Minute))
		if importerSleep < time.Second {
			importerSleep = time.Second
		}
		retries429 := cfg.RawJobHTTP429Retries
		if retries429 < 0 {
			retries429 = 0
		}
		retryDelaySeconds := cfg.RawJobHTTP429RetryDelaySeconds
		if retryDelaySeconds < 0 {
			retryDelaySeconds = 0
		}
		stepTimeoutSeconds := cfg.WorkerChainStepTimeoutSeconds
		if stepTimeoutSeconds < 0 {
			stepTimeoutSeconds = 0
		}

		runChain := func() {
			for {
				parsedAIClassifierEnabled := cfg.ParsedJobAIClassifierEnabled
				parsedJobAvailabilityEnabled := cfg.ParsedJobAvailabilityEnabled
				watcherSvc := watcher.New(watcher.Config{
					Enabled:                          cfg.WatchEnabled,
					RemoteRocketshipUSJobSitemapURLs: defaultRemoteRocketshipURLs,
					IntervalMinutes:                  cfg.WatchIntervalMinutes,
					SampleKB:                         cfg.WatchSampleKB,
					TimeoutSeconds:                   cfg.WatchTimeoutSeconds,
					BuiltinBaseURL:                   defaultBuiltinBaseURL,
					BuiltinMaxPage:                   cfg.WatchBuiltinMaxPage,
					BuiltinPagesPerCycle:             cfg.WatchBuiltinPagesPerCycle,
					BuiltinCheckpointPages:           cfg.WatchBuiltinCheckpointPages,
					BuiltinFetchIntervalSeconds:      cfg.WatchBuiltinFetchIntervalSeconds,
					Builtin429RetryCount:             cfg.WatchBuiltin429RetryCount,
					Builtin429BackoffSeconds:         cfg.WatchBuiltin429BackoffSeconds,
					WorkableAPIURL:                   defaultWorkableAPIURL,
					WorkablePageLimit:                cfg.WatchWorkablePageLimit,
					RemotiveSitemapURLTemplate:       defaultRemotiveSitemapURLTemplate,
					RemotiveSitemapMaxIndex:          cfg.WatchRemotiveSitemapMaxIndex,
					RemotiveSitemapMinIndex:          cfg.WatchRemotiveSitemapMinIndex,
					DailyRemoteBaseURL:               defaultDailyRemoteBaseURL,
					DailyRemoteMaxPage:               cfg.WatchDailyRemoteMaxPage,
					DailyRemotePagesPerCycle:         cfg.WatchDailyRemotePagesPerCycle,
					RemoteDotCoSitemapURL:            cfg.WatchRemoteDotCoSitemapURL,
					FlexJobsSitemapURL:               cfg.FlexJobsSitemapURL,
					HiringCafeSearchAPIURL:           defaultHiringCafeSearchURL,
					HiringCafeTotalCountURL:          defaultHiringCafeCountURL,
					HiringCafePageSize:               cfg.WatchHiringCafePageSize,
					EnabledSources:                   enabledSources,
				}, db)

				importerSvc := importer.New(importer.Config{
					IntervalMinutes:     importerInterval,
					SleepDuration:       importerSleep,
					BatchSize:           cfg.RawImportBatchSize,
					PayloadsPerCycle:    cfg.RawImportPayloadsPerCycle,
					EnabledSources:      enabledSources,
					RunOnce:             true,
					ErrorBackoffSeconds: errorBackoffSeconds,
					WorkerCount:         cfg.RawImportWorkerCount,
				}, db)

				rawSvc := raw.New(raw.Config{
					BatchSize:             cfg.RawJobWorkerBatchSize,
					PollSeconds:           cfg.RawJobWorkerPollSeconds,
					RunOnce:               true,
					ErrorBackoffSeconds:   errorBackoffSeconds,
					FetchTimeoutSeconds:   cfg.RawJobFetchTimeoutSeconds,
					RetentionDays:         cfg.RawJobRetentionDays,
					RetentionCleanupBatch: cfg.RawJobRetentionCleanupBatch,
					WorkerCount:           cfg.RawJobWorkerCount,
				}, db)
				rawSvc.EnabledSources = enabledSources
				rawSvc.ReadHTMLForSource = makeReadHTMLForSourceWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)

				parsedSvc := parsed.New(parsed.Config{
					BatchSize:               cfg.ParsedJobWorkerBatchSize,
					PollSeconds:             cfg.ParsedJobWorkerPollSeconds,
					RunOnce:                 true,
					ErrorBackoffSeconds:     errorBackoffSeconds,
					WorkerCount:             cfg.ParsedJobWorkerCount,
					CategorySignalTokensURL: cfg.CategorySignalTokensURL,
					DuplicateJobURLRulesURL: cfg.DuplicateJobURLRulesURL,
					TechStackCatalogURL:     cfg.TechStackCatalogURL,
				}, db)
				parsedSvc.EnabledSources = enabledSources
				var parsedAIClassifierSvc *parsedaiclassifier.Service
				if parsedAIClassifierEnabled {
					parsedAIClassifierSvc = parsedaiclassifier.New(parsedaiclassifier.Config{
						BatchSize:            cfg.ParsedJobAIClassifierBatchSize,
						PollSeconds:          cfg.ParsedJobAIClassifierPollSeconds,
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
				var parsedAvailabilitySvc *parsedjobavailability.Service
				if parsedJobAvailabilityEnabled {
					parsedAvailabilitySvc = parsedjobavailability.New(parsedjobavailability.Config{
						BatchSize:           cfg.ParsedJobAvailabilityBatchSize,
						PollSeconds:         cfg.ParsedJobAvailabilityPollSeconds,
						RunOnce:             true,
						ErrorBackoffSeconds: errorBackoffSeconds,
						WorkerCount:         cfg.ParsedJobAvailabilityWorkerCount,
						FetchTimeoutSeconds: cfg.ParsedJobAvailabilityFetchTimeoutSeconds,
					}, db)
					parsedAvailabilitySvc.EnabledSources = enabledSources
					readHTMLForSource := makeReadHTMLForSourceWith429Retry(retries429, time.Duration(retryDelaySeconds)*time.Second)
					parsedAvailabilitySvc.ReadHTMLForSource = func(ctx context.Context, source, targetURL string) (string, int, error) {
						return readHTMLForSource(ctx, source, targetURL)
					}
				}

				hadError := false
				errorDetails := make([]string, 0, 6)
				log.Printf("worker-chain cycle_start")
				stepTimeout := time.Duration(stepTimeoutSeconds) * time.Second
				cycleCtx := context.Background()
				cancelCycle := func() {}
				if stepTimeout > 0 {
					cycleCtx, cancelCycle = context.WithTimeout(context.Background(), stepTimeout)
				}
				type stepResult struct {
					name  string
					count int
					err   error
				}
				results := make(chan stepResult, 6)
				launchStep := func(name string, fn func(context.Context) error) {
					go func() {
						defer func() {
							if r := recover(); r != nil {
								results <- stepResult{name: name, err: errors.New("panic")}
							}
						}()
						err := fn(cycleCtx)
						results <- stepResult{name: name, err: err}
					}()
				}
				launchCountStep := func(name string, fn func(context.Context) (int, error)) {
					go func() {
						defer func() {
							if r := recover(); r != nil {
								results <- stepResult{name: name, err: errors.New("panic")}
							}
						}()
						count, err := fn(cycleCtx)
						results <- stepResult{name: name, count: count, err: err}
					}()
				}
				launchOptionalCountStep := func(name string, enabled bool, disabledLog string, fn func(context.Context) (int, error)) {
					if !enabled {
						log.Printf("%s", disabledLog)
						results <- stepResult{name: name, count: 0, err: nil}
						return
					}
					launchCountStep(name, fn)
				}
				launchStep(constants.WorkerNameWatcher, func(ctx context.Context) error {
					return watcherSvc.RunOnceWithContext(ctx)
				})
				launchStep(constants.WorkerNameImporter, func(ctx context.Context) error {
					return importerSvc.RunOnceWithContext(ctx)
				})
				launchCountStep(constants.WorkerNameRaw, func(ctx context.Context) (int, error) {
					return rawSvc.RunOnce(ctx)
				})
				launchCountStep(constants.WorkerNameParsed, func(ctx context.Context) (int, error) {
					return parsedSvc.RunOnce(ctx)
				})
				launchOptionalCountStep(
					constants.WorkerNameParsedAIClassifier,
					parsedAIClassifierEnabled && parsedAIClassifierSvc != nil,
					"worker-chain parsed_ai_classifier_disabled",
					func(ctx context.Context) (int, error) {
						return parsedAIClassifierSvc.RunOnce(ctx)
					},
				)
				launchOptionalCountStep(
					constants.WorkerNameParsedAvailability,
					parsedJobAvailabilityEnabled && parsedAvailabilitySvc != nil,
					"worker-chain parsed_job_availability_disabled",
					func(ctx context.Context) (int, error) {
						return parsedAvailabilitySvc.RunOnce(ctx)
					},
				)
				pendingSteps := map[string]struct{}{
					constants.WorkerNameWatcher:            {},
					constants.WorkerNameImporter:           {},
					constants.WorkerNameRaw:                {},
					constants.WorkerNameParsed:             {},
					constants.WorkerNameParsedAIClassifier: {},
					constants.WorkerNameParsedAvailability: {},
				}
				handleResult := func(res stepResult) {
					delete(pendingSteps, res.name)
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
				for len(pendingSteps) > 0 {
					select {
					case res := <-results:
						handleResult(res)
					case <-cycleCtx.Done():
						for name := range pendingSteps {
							log.Printf("worker-chain %s_failed error=%v", strings.ToLower(name), cycleCtx.Err())
							hadError = true
							errorDetails = append(errorDetails, name+"="+cycleCtx.Err().Error())
						}
						pendingSteps = map[string]struct{}{}
					}
				}
				cancelCycle()
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
	err := fn(ctx)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		log.Printf("worker-chain step_timeout step=%s timeout=%s", name, timeout)
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func runCountStepWithTimeout(timeout time.Duration, fn func(context.Context) (int, error)) (int, error) {
	if timeout <= 0 {
		return fn(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	count, err := fn(ctx)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		log.Printf("worker-chain step_timeout timeout=%s", timeout)
	}
	if err != nil {
		return count, err
	}
	return count, ctx.Err()
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
