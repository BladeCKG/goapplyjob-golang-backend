package main

import (
	"fmt"

	"goapplyjob-golang-backend/internal/config"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	baseURL := config.Getenv("WATCH_BUILTIN_BASE_URL", "https://builtin.com/jobs/remote?country=USA&allLocations=true&page={page}")
	maxPage := config.GetenvInt("WATCH_BUILTIN_MAX_PAGE", 1000)
	fmt.Printf("[builtin] one-time fetch scaffold base_url=%s max_page=%d\n", baseURL, maxPage)
}
