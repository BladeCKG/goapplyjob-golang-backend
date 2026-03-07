package main

import (
	"log"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/importer"
)

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	svc := importer.New(db)
	intervalMinutes := config.GetenvFloat("RAW_IMPORT_INTERVAL_MINUTES", 1)
	if intervalMinutes < 0 {
		intervalMinutes = 1
	}
	sleepDuration := time.Duration(intervalMinutes * float64(time.Minute))
	if sleepDuration < time.Second {
		sleepDuration = time.Second
	}
	batchSize := config.GetenvInt("RAW_IMPORT_BATCH_SIZE", 1000)
	if batchSize < 1 {
		batchSize = 1
	}
	payloadsPerCycle := config.GetenvInt("RAW_IMPORT_PAYLOADS_PER_CYCLE", 5)
	if payloadsPerCycle < 1 {
		payloadsPerCycle = 1
	}
	runOnce := config.GetenvBool("RAW_IMPORT_RUN_ONCE", false)

	for {
		payloads, err := svc.PickUnconsumedPayloads(payloadsPerCycle)
		if err != nil {
			log.Fatal(err)
		}
		remainingRowsBudget := batchSize
		for _, payload := range payloads {
			if remainingRowsBudget <= 0 {
				break
			}
			payloadRows, skippedInvalid := importer.ParseRowsForImport(payload.BodyText)
			if len(payloadRows) == 0 {
				if err := svc.MarkPayloadConsumed(payload.ID); err != nil {
					log.Fatal(err)
				}
				log.Printf("importer consumed empty payload_id=%d skipped_invalid=%d", payload.ID, skippedInvalid)
				continue
			}

			toProcessCount := len(payloadRows)
			if toProcessCount > remainingRowsBudget {
				toProcessCount = remainingRowsBudget
			}
			rowsToProcess := payloadRows[:toProcessCount]
			unprocessedRows := payloadRows[toProcessCount:]

			stats, failedRows, _, err := svc.ImportRawUSJobsRows(rowsToProcess, batchSize)
			if err != nil {
				log.Fatal(err)
			}
			stats.SkippedInvalid = skippedInvalid
			failedRowsList := importer.FailedRowsToList(failedRows)
			remainingRows := append(failedRowsList, unprocessedRows...)
			remainingRowsBudget -= toProcessCount

			if len(remainingRows) > 0 {
				if err := svc.ReplacePayloadRows(payload.ID, remainingRows); err != nil {
					log.Fatal(err)
				}
				log.Printf("importer partial payload_id=%d seen=%d inserted=%d updated=%d skipped_invalid=%d failed_db=%d remaining_rows=%d remaining_budget=%d", payload.ID, stats.Seen, stats.Inserted, stats.Updated, stats.SkippedInvalid, stats.FailedDB, len(remainingRows), remainingRowsBudget)
				continue
			}
			if err := svc.MarkPayloadConsumed(payload.ID); err != nil {
				log.Fatal(err)
			}
			log.Printf("importer imported payload_id=%d seen=%d inserted=%d updated=%d skipped_invalid=%d failed_db=%d", payload.ID, stats.Seen, stats.Inserted, stats.Updated, stats.SkippedInvalid, stats.FailedDB)
		}

		if runOnce {
			return
		}
		time.Sleep(sleepDuration)
	}
}
