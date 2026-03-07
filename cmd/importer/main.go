package main

import (
	"log"
	"os"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/importer"
)

func main() {
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if len(os.Args) < 2 {
		return
	}
	svc := importer.New(db)
	_, _, err = svc.ProcessImportFile(os.Args[1], ".", "imported", 100)
	if err != nil {
		log.Fatal(err)
	}
}
