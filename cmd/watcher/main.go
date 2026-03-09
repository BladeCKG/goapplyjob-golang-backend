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
		defaultRemoteRocketshipURL = "https://www.remoterocketship.com/us/sitemap_job_openings_us_1.xml"
		defaultBuiltinBaseURL      = "https://builtin.com/jobs/remote?country=USA&allLocations=true&page={page}"
		defaultWorkableAPIURL      = "https://jobs.workable.com/api/v1/jobs?location=United States&workplace=remote&day_range=1"
		defaultRemotiveSitemapURL  = "https://remotive.com/sitemap-job-postings-8.xml"
		defaultDailyRemoteBaseURL  = "https://dailyremote.com/?location_country=United+States&sort_by=time&page={page}"
		defaultHiringCafeSearchURL = "https://hiring.cafe/api/search-jobs?s="
		defaultHiringCafeCountURL  = "https://hiring.cafe/api/search-jobs/get-total-count?s="
	)

	svc := watcher.New(watcher.Config{
		Enabled:                  config.GetenvBool("WATCH_ENABLED", true),
		URL:                      defaultRemoteRocketshipURL,
		IntervalMinutes:          config.GetenvFloat("WATCH_INTERVAL_MINUTES", 1),
		SampleKB:                 config.GetenvInt("WATCH_SAMPLE_KB", 8),
		TimeoutSeconds:           config.GetenvFloat("WATCH_TIMEOUT_SECONDS", 30),
		BuiltinBaseURL:           defaultBuiltinBaseURL,
		BuiltinMaxPage:           config.GetenvInt("WATCH_BUILTIN_MAX_PAGE", 1000),
		BuiltinPagesPerCycle:     config.GetenvInt("WATCH_BUILTIN_PAGES_PER_CYCLE", 25),
		BuiltinCheckpointPages:   config.GetenvInt("WATCH_BUILTIN_STATE_CHECKPOINT_PAGES", 5),
		WorkableAPIURL:           defaultWorkableAPIURL,
		WorkablePageLimit:        config.GetenvInt("WATCH_WORKABLE_PAGE_LIMIT", 100),
		RemotiveSitemapURL:       defaultRemotiveSitemapURL,
		DailyRemoteBaseURL:       config.Getenv("WATCH_DAILYREMOTE_BASE_URL", defaultDailyRemoteBaseURL),
		DailyRemoteMaxPage:       config.GetenvInt("WATCH_DAILYREMOTE_MAX_PAGE", 5000),
		DailyRemotePagesPerCycle: config.GetenvInt("WATCH_DAILYREMOTE_PAGES_PER_CYCLE", 300),
		HiringCafeSearchAPIURL:   defaultHiringCafeSearchURL,
		HiringCafeTotalCountURL:  defaultHiringCafeCountURL,
		HiringCafePageSize:       config.GetenvInt("WATCH_HIRINGCAFE_PAGE_SIZE", 200),
		EnabledSources:           config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship"),
	}, db)
	runOnce := config.GetenvBool("WATCH_RUN_ONCE", false)
	if err := svc.RunForever(runOnce); err != nil {
		log.Fatal(err)
	}
}
