package main

import (
	"log"

	"goapplyjob-golang-backend/internal/app"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	router := app.NewRouter(cfg, db)
	if err := router.Run(cfg.HTTPHost + ":" + cfg.HTTPPort); err != nil {
		log.Fatalf("run server: %v", err)
	}
}
