package importer

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"goapplyjob-golang-backend/internal/database"
	rr "goapplyjob-golang-backend/internal/sources/remoterocketship"
)

type Stats struct {
	Seen           int
	Inserted       int
	Updated        int
	SkippedInvalid int
	FailedDB       int
}

type SitemapRow struct {
	URL      string
	PostDate time.Time
}

type Service struct {
	DB *database.DB
}

func New(db *database.DB) *Service { return &Service{DB: db} }

func extractCompleteURLBlocks(xmlText string) []string {
	return rr.ExtractCompleteURLBlocks(xmlText)
}

func extractRowFromURLBlock(block string) (string, string, bool) {
	return rr.ExtractRowFromURLBlock(block)
}

func iterSitemapRowsText(xmlText string) [][2]string {
	return rr.IterSitemapRowsText(xmlText)
}

func parseRowsFromXMLText(xmlText string) ([]SitemapRow, int) {
	rows, skippedInvalid := rr.ParseRowsFromXMLText(xmlText)
	parsed := make([]SitemapRow, 0, len(rows))
	for _, row := range rows {
		parsed = append(parsed, SitemapRow{URL: row.URL, PostDate: row.PostDate})
	}
	return parsed, skippedInvalid
}

func ParseRowsForImport(xmlText string) ([]SitemapRow, int) {
	return parseRowsFromXMLText(xmlText)
}

func iterSitemapRows(xmlPath string) ([][2]string, error) {
	raw, err := os.ReadFile(xmlPath)
	if err != nil {
		return nil, err
	}
	rows := iterSitemapRowsText(string(raw))
	return rows, nil
}

func normalizeDBDatetime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New("empty")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.Parse(time.RFC3339, value)
}

func mergeRows(target map[string]time.Time, incoming map[string]time.Time) {
	for url, postDate := range incoming {
		current, ok := target[url]
		if !ok || postDate.After(current) {
			target[url] = postDate
		}
	}
}

func rowsToXML(rows map[string]time.Time) string {
	return rr.RowsMapToXML(rows)
}

func rowsListToXML(rows []SitemapRow) string {
	sourceRows := make([]rr.SitemapRow, 0, len(rows))
	for _, row := range rows {
		sourceRows = append(sourceRows, rr.SitemapRow{URL: row.URL, PostDate: row.PostDate})
	}
	return rr.RowsListToXML(sourceRows)
}

func atomicWriteText(path, content string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeRowsFile(path string, rows map[string]time.Time) error {
	return atomicWriteText(path, rowsToXML(rows))
}

func newImportedPath(importDir, importedPrefix string) string {
	ts := time.Now().UTC().Format("20060102T150405Z")
	target := filepath.Join(importDir, importedPrefix+"_"+ts+".xml")
	counter := 1
	for {
		if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
			return target
		}
		target = filepath.Join(importDir, fmt.Sprintf("%s_%s_%d.xml", importedPrefix, ts, counter))
		counter++
	}
}

func (s *Service) flushBuffer(buffer map[string]time.Time) (int, int, int, map[string]time.Time) {
	if len(buffer) == 0 {
		return 0, 0, 0, map[string]time.Time{}
	}
	inserted, updated, failedDB := 0, 0, 0
	failedRows := map[string]time.Time{}

	tx, err := s.DB.SQL.Begin()
	if err != nil {
		for url, postDate := range buffer {
			failedRows[url] = postDate
		}
		return 0, 0, len(buffer), failedRows
	}
	defer tx.Rollback()

	for url, postDate := range buffer {
		var existingID int64
		var existingPostDate string
		err := tx.QueryRow(`SELECT id, post_date FROM raw_us_jobs WHERE url = ? LIMIT 1`, url).Scan(&existingID, &existingPostDate)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			failedDB++
			failedRows[url] = postDate
			continue
		}
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := tx.Exec(`INSERT INTO raw_us_jobs (url, post_date, is_ready, is_skippable, retry_count, raw_json) VALUES (?, ?, 0, 0, 0, NULL)`, url, postDate.Format(time.RFC3339)); err != nil {
				failedDB++
				failedRows[url] = postDate
				continue
			}
			inserted++
			continue
		}

		existingDT, err := normalizeDBDatetime(existingPostDate)
		if err == nil && !postDate.After(existingDT) {
			continue
		}
		if _, err := tx.Exec(`DELETE FROM parsed_jobs WHERE raw_us_job_id = ?`, existingID); err != nil {
			failedDB++
			failedRows[url] = postDate
			continue
		}
		if _, err := tx.Exec(`DELETE FROM raw_us_jobs WHERE id = ?`, existingID); err != nil {
			failedDB++
			failedRows[url] = postDate
			continue
		}
		if _, err := tx.Exec(`INSERT INTO raw_us_jobs (url, post_date, is_ready, is_skippable, retry_count, raw_json) VALUES (?, ?, 0, 0, 0, NULL)`, url, postDate.Format(time.RFC3339)); err != nil {
			failedDB++
			failedRows[url] = postDate
			continue
		}
		updated++
	}

	if err := tx.Commit(); err != nil {
		for url, postDate := range buffer {
			failedRows[url] = postDate
		}
		return 0, 0, len(buffer), failedRows
	}
	return inserted, updated, failedDB, failedRows
}

func (s *Service) ImportRawUSJobs(xmlPath string, batchSize int) (Stats, map[string]time.Time, map[string]time.Time, error) {
	if _, err := os.Stat(xmlPath); err != nil {
		return Stats{}, nil, nil, err
	}
	raw, err := os.ReadFile(xmlPath)
	if err != nil {
		return Stats{}, nil, nil, err
	}
	return s.ImportRawUSJobsText(string(raw), batchSize)
}

func (s *Service) ImportRawUSJobsText(xmlText string, batchSize int) (Stats, map[string]time.Time, map[string]time.Time, error) {
	rows, skippedInvalid := parseRowsFromXMLText(xmlText)
	stats, failedRows, succeededRows, err := s.ImportRawUSJobsRows(rows, batchSize)
	if err != nil {
		return stats, failedRows, succeededRows, err
	}
	stats.SkippedInvalid = skippedInvalid
	return stats, failedRows, succeededRows, nil
}

func (s *Service) ImportRawUSJobsRows(rows []SitemapRow, batchSize int) (Stats, map[string]time.Time, map[string]time.Time, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	stats := Stats{}
	buffer := map[string]time.Time{}
	allRows := map[string]time.Time{}
	failedRows := map[string]time.Time{}

	for _, row := range rows {
		stats.Seen++
		if current, ok := allRows[row.URL]; !ok || row.PostDate.After(current) {
			allRows[row.URL] = row.PostDate
		}
		if current, ok := buffer[row.URL]; !ok || row.PostDate.After(current) {
			buffer[row.URL] = row.PostDate
		}
		if len(buffer) >= batchSize {
			inserted, updated, failedDB, failedBatch := s.flushBuffer(buffer)
			stats.Inserted += inserted
			stats.Updated += updated
			stats.FailedDB += failedDB
			mergeRows(failedRows, failedBatch)
			buffer = map[string]time.Time{}
		}
	}
	inserted, updated, failedDB, failedBatch := s.flushBuffer(buffer)
	stats.Inserted += inserted
	stats.Updated += updated
	stats.FailedDB += failedDB
	mergeRows(failedRows, failedBatch)

	succeededRows := map[string]time.Time{}
	for url, postDate := range allRows {
		if _, failed := failedRows[url]; !failed {
			succeededRows[url] = postDate
		}
	}
	return stats, failedRows, succeededRows, nil
}

func (s *Service) PickUnconsumedPayloads(limit int) ([]struct {
	ID       int64
	BodyText string
}, error) {
	if limit <= 0 {
		limit = 1
	}
	rows, err := s.DB.SQL.Query(`SELECT id, body_text FROM watcher_payloads WHERE payload_type = 'delta_xml' AND consumed_at IS NULL ORDER BY created_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []struct {
		ID       int64
		BodyText string
	}{}
	for rows.Next() {
		var item struct {
			ID       int64
			BodyText string
		}
		if err := rows.Scan(&item.ID, &item.BodyText); err == nil {
			result = append(result, item)
		}
	}
	return result, rows.Err()
}

func (s *Service) DeletePayload(payloadID int64) error {
	_, err := s.DB.SQL.Exec(`DELETE FROM watcher_payloads WHERE id = ?`, payloadID)
	return err
}

func (s *Service) DeleteConsumedPayloads() (int64, error) {
	result, err := s.DB.SQL.Exec(`DELETE FROM watcher_payloads WHERE consumed_at IS NOT NULL`)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func (s *Service) MarkPayloadConsumed(payloadID int64) error {
	_, err := s.DB.SQL.Exec(`UPDATE watcher_payloads SET consumed_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339Nano), payloadID)
	return err
}

func (s *Service) ReplacePayloadBody(payloadID int64, failedRows map[string]time.Time) error {
	_, err := s.DB.SQL.Exec(`UPDATE watcher_payloads SET body_text = ? WHERE id = ?`, rowsToXML(failedRows), payloadID)
	return err
}

func (s *Service) ReplacePayloadRows(payloadID int64, rows []SitemapRow) error {
	_, err := s.DB.SQL.Exec(`UPDATE watcher_payloads SET body_text = ? WHERE id = ?`, rowsListToXML(rows), payloadID)
	return err
}

func FailedRowsToList(rows map[string]time.Time) []SitemapRow {
	out := make([]SitemapRow, 0, len(rows))
	for url, postDate := range rows {
		out = append(out, SitemapRow{URL: url, PostDate: postDate})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PostDate.After(out[j].PostDate) })
	return out
}

func (s *Service) ProcessImportFile(xmlPath, importDir, importedPrefix string, batchSize int) (Stats, string, error) {
	stats, failedRows, succeededRows, err := s.ImportRawUSJobs(xmlPath, batchSize)
	if err != nil {
		return stats, "", err
	}
	if len(failedRows) > 0 {
		var importedPath string
		if len(succeededRows) > 0 {
			importedPath = newImportedPath(importDir, importedPrefix)
			if err := writeRowsFile(importedPath, succeededRows); err != nil {
				return stats, importedPath, err
			}
		}
		if err := writeRowsFile(xmlPath, failedRows); err != nil {
			return stats, importedPath, err
		}
		return stats, importedPath, nil
	}
	renamed := newImportedPath(importDir, importedPrefix)
	if err := os.Rename(xmlPath, renamed); err != nil {
		return stats, "", err
	}
	return stats, renamed, nil
}

func MarshalRows(rows map[string]time.Time) string {
	payload, _ := json.Marshal(rows)
	return string(payload)
}
