package main

import (
	"fmt"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/sources/workable"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	apiURL := config.Getenv("WATCH_WORKABLE_API_URL", "https://jobs.workable.com/api/v1/jobs?location=United States&workplace=remote")
	pageLimit := config.GetenvInt("WATCH_WORKABLE_PAGE_LIMIT", 100)
	fmt.Printf("[workable] one-time fetch scaffold api_url=%s next=%s\n", apiURL, workable.BuildAPIURL(apiURL, "", pageLimit))
}
