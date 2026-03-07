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

	_ = watcher.New(watcher.Config{
		Enabled:         config.GetenvBool("WATCH_ENABLED", true),
		URL:             config.Getenv("WATCH_URL", ""),
		IntervalMinutes: config.GetenvFloat("WATCH_INTERVAL_MINUTES", 1),
		SampleKB:        config.GetenvInt("WATCH_SAMPLE_KB", 8),
		TimeoutSeconds:  config.GetenvFloat("WATCH_TIMEOUT_SECONDS", 30),
	}, db)
}
