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
	if deleted, err := svc.DeleteConsumedPayloads(); err != nil {
		log.Fatal(err)
	} else if deleted > 0 {
		log.Printf("importer removed legacy consumed payloads=%d", deleted)
	}
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
	payloadsPerCycle := config.GetenvInt("RAW_IMPORT_PAYLOADS_PER_CYCLE", 50)
	if payloadsPerCycle < 1 {
		payloadsPerCycle = 1
	}
	enabledSources := config.GetenvCSVSet("ENABLED_SOURCES", "remoterocketship")
	runOnce := config.GetenvBool("RAW_IMPORT_RUN_ONCE", false)

	for {
		payloads, err := svc.PickUnconsumedPayloads(payloadsPerCycle, enabledSources)
		if err != nil {
			log.Fatal(err)
		}
		remainingRowsBudget := batchSize
		for _, payload := range payloads {
			if remainingRowsBudget <= 0 {
				break
			}
			var payloadRows []importer.SitemapRow
			var skippedInvalid int
			switch {
			case payload.PayloadType == "delta_xml":
				payloadRows, skippedInvalid = importer.ParseRowsForImport(payload.BodyText)
			case payload.PayloadType == "delta" && payload.Source == "builtin":
				payloadRows, skippedInvalid = importer.ParseRowsForBuiltinPayload(payload.BodyText)
			case payload.PayloadType == "delta" && payload.Source == "workable":
				payloadRows, skippedInvalid = importer.ParseRowsForWorkablePayload(payload.BodyText)
			default:
				log.Printf("importer skipping unsupported payload_id=%d source=%s payload_type=%s", payload.ID, payload.Source, payload.PayloadType)
				continue
			}
			if len(payloadRows) == 0 {
				log.Printf("importer kept empty payload_id=%d skipped_invalid=%d", payload.ID, skippedInvalid)
				continue
			}

			toProcessCount := len(payloadRows)
			if toProcessCount > remainingRowsBudget {
				toProcessCount = remainingRowsBudget
			}
			rowsToProcess := payloadRows[:toProcessCount]
			unprocessedRows := payloadRows[toProcessCount:]

			stats, failedRows, _, err := svc.ImportRawUSJobsRows(rowsToProcess, batchSize, payload.Source)
			if err != nil {
				log.Fatal(err)
			}
			stats.SkippedInvalid = skippedInvalid
			failedRowsList := importer.FailedImportRowsToList(failedRows)
			remainingRows := append(failedRowsList, unprocessedRows...)
			remainingRowsBudget -= toProcessCount

			if len(remainingRows) > 0 {
				var err error
				if payload.PayloadType == "delta" && payload.Source == "builtin" {
					err = svc.ReplaceBuiltinPayloadRows(payload.ID, remainingRows)
				} else if payload.PayloadType == "delta" && payload.Source == "workable" {
					serializedRows := make([]map[string]any, 0, len(remainingRows))
					for _, row := range remainingRows {
						serializedRows = append(serializedRows, map[string]any{"url": row.URL, "post_date": row.PostDate})
					}
					err = svc.ReplaceSourcePayloadRows(payload.ID, payload.Source, serializedRows)
				} else {
					err = svc.ReplacePayloadRows(payload.ID, remainingRows)
				}
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("importer partial payload_id=%d seen=%d inserted=%d updated=%d skipped_invalid=%d failed_db=%d remaining_rows=%d remaining_budget=%d", payload.ID, stats.Seen, stats.Inserted, stats.Updated, stats.SkippedInvalid, stats.FailedDB, len(remainingRows), remainingRowsBudget)
				continue
			}
			if err := svc.DeletePayload(payload.ID); err != nil {
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
