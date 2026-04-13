package main

import (
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/watcher"
	"goapplyjob-golang-backend/internal/workerlog"
	"log"
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

	svc := watcher.New(watcher.Config{
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
		EnabledSources:                   cfg.EnabledSources,
	}, db)
	runOnce := cfg.WatchRunOnce
	if err := svc.RunForever(runOnce); err != nil {
		log.Fatal(err)
	}
}
