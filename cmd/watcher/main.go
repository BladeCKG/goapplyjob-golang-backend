package main

import (
	"log"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/watcher"
	"goapplyjob-golang-backend/internal/workerlog"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	closeLogFile, err := workerlog.Setup("WATCHER_WORKER_LOG_FILE", "watcher_worker.log")
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

	svc := watcher.New(watcher.Config{
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
		EnabledSources:                  config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship"),
	}, db)
	runOnce := config.GetenvBool("WATCH_RUN_ONCE", false)
	if err := svc.RunForever(runOnce); err != nil {
		log.Fatal(err)
	}
}
