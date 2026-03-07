package main

import (
	"log"
	"os"
	"strconv"
	"strings"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/watcher"
)

func main() {
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_ = watcher.New(watcher.Config{
		Enabled:         envBool("WATCH_ENABLED", true),
		URL:             strings.TrimSpace(os.Getenv("WATCH_URL")),
		IntervalMinutes: envFloat("WATCH_INTERVAL_MINUTES", 1),
		SampleKB:        envInt("WATCH_SAMPLE_KB", 8),
		TimeoutSeconds:  envFloat("WATCH_TIMEOUT_SECONDS", 30),
	}, db)
}

func envInt(key string, fallback int) int {
	if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			return parsed
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
