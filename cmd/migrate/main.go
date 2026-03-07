package main

import (
	"log"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
)

func main() {
	if err := config.LoadDotEnvIfExists(".env"); err != nil {
		log.Fatalf("load .env: %v", err)
	}
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("migrate database: %v", err)
	}
	defer db.Close()
}
