package main

import (
	"log"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/watcher"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	svc := watcher.New(watcher.Config{
		Enabled:              config.GetenvBool("WATCH_ENABLED", true),
		URL:                  config.Getenv("WATCH_URL", ""),
		IntervalMinutes:      config.GetenvFloat("WATCH_INTERVAL_MINUTES", 1),
		SampleKB:             config.GetenvInt("WATCH_SAMPLE_KB", 8),
		TimeoutSeconds:       config.GetenvFloat("WATCH_TIMEOUT_SECONDS", 30),
		BuiltinBaseURL:       config.Getenv("WATCH_BUILTIN_BASE_URL", "https://builtin.com/jobs/remote?country=USA&allLocations=true&page={page}"),
		BuiltinMaxPage:       config.GetenvInt("WATCH_BUILTIN_MAX_PAGE", 1000),
		BuiltinPagesPerCycle: config.GetenvInt("WATCH_BUILTIN_PAGES_PER_CYCLE", 25),
	}, db)
	runOnce := config.GetenvBool("WATCH_RUN_ONCE", false)
	if err := svc.RunForever(runOnce); err != nil {
		log.Fatal(err)
	}
}
