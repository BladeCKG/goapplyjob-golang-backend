package importer

import (
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/dailyremote"
	"goapplyjob-golang-backend/internal/sources/plugins"
	"goapplyjob-golang-backend/internal/sources/remotive"
	"goapplyjob-golang-backend/internal/sources/workable"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	xmlDecl             = `<?xml version="1.0" encoding="UTF-8"?>`
	namespaceURLSetOpen = `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`
	rootCloseTag        = `</urlset>`
)

var urlBlockPattern = regexp.MustCompile(`(?is)<url(?:\s[^>]*)?>.*?</url>`)

type xmlURL struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

type Stats struct {
	Seen           int
	Inserted       int
	Updated        int
	SkippedInvalid int
	FailedDB       int
}

type SitemapRow struct {
	URL       string
	PostDate  time.Time
	RawJSON   map[string]any
	ExtraJSON map[string]any
	Payload   map[string]any
}

type Config struct {
	IntervalMinutes     float64
	SleepDuration       time.Duration
	BatchSize           int
	PayloadsPerCycle    int
	EnabledSources      map[string]struct{}
	RunOnce             bool
	ErrorBackoffSeconds int
}

type Service struct {
	DB     *database.DB
	Config Config
}

const (
	sourceRemoterocketship  = "remoterocketship"
	sourceBuiltin           = "builtin"
	sourceRemotive          = "remotive"
	sourceDailyremote       = "dailyremote"
	sourceWorkable          = "workable"
	payloadTypeXML          = "delta_xml"
	payloadTypeJSON         = "delta"
	defaultPayloadsPerCycle = 40
)

func New(config Config, db *database.DB) *Service {
	return &Service{DB: db, Config: config}
}

func extractCompleteURLBlocks(xmlText string) []string {
	return urlBlockPattern.FindAllString(xmlText, -1)
}

func extractRowFromURLBlock(block string) (string, string, bool) {
	var row xmlURL
	if err := xml.Unmarshal([]byte(block), &row); err != nil {
		return "", "", false
	}
	loc := strings.TrimSpace(row.Loc)
	lastmod := strings.TrimSpace(row.LastMod)
	if loc == "" || lastmod == "" {
		return "", "", false
	}
	return loc, lastmod, true
}

func iterSitemapRowsText(xmlText string) [][2]string {
	blocks := extractCompleteURLBlocks(xmlText)
	rows := make([][2]string, 0, len(blocks))
	for _, block := range blocks {
		loc, lastmod, ok := extractRowFromURLBlock(block)
		if ok {
			rows = append(rows, [2]string{loc, lastmod})
		}
	}
	return rows
}

func ParseRowsForWorkablePayload(payloadText string) ([]SitemapRow, int) {
	rows, skipped := workable.ParseImportRows(payloadText)
	out := make([]SitemapRow, 0, len(rows))
	for _, row := range rows {
		sitemapRow, ok := parseGenericImportRow(row, true)
		if !ok {
			skipped++
			continue
		}
		out = append(out, sitemapRow)
	}
	return out, skipped
}

func ParseRowsForRemotivePayload(payloadText string) ([]SitemapRow, int) {
	rows, skipped := remotive.ParseImportRows(payloadText)
	out := make([]SitemapRow, 0, len(rows))
	for _, row := range rows {
		sitemapRow, ok := parseGenericImportRow(row, false)
		if !ok {
			skipped++
			continue
		}
		out = append(out, sitemapRow)
	}
	return out, skipped
}

func ParseRowsForDailyremotePayload(payloadText string) ([]SitemapRow, int) {
	rows, skipped := dailyremote.ParseImportRows(payloadText)
	out := make([]SitemapRow, 0, len(rows))
	for _, row := range rows {
		sitemapRow, ok := parseGenericImportRow(row, false)
		if !ok {
			skipped++
			continue
		}
		out = append(out, sitemapRow)
	}
	return out, skipped
}

func ParseRowsForSourcePayload(source, payloadType, payloadText string) ([]SitemapRow, int, bool) {
	plugin, ok := plugins.Get(source)
	if !ok || plugin.ParseImportRows == nil {
		return nil, 0, false
	}
	parsedRows, skipped := plugin.ParseImportRows(payloadText)
	rows := make([]SitemapRow, 0, len(parsedRows))
	for _, row := range parsedRows {
		sitemapRow, ok := parseGenericImportRow(row, plugin.Source == sourceWorkable)
		if !ok {
			skipped++
			continue
		}
		rows = append(rows, sitemapRow)
	}
	return rows, skipped, true
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
	type pair struct {
		url      string
		postDate time.Time
	}
	ordered := make([]pair, 0, len(rows))
	for url, postDate := range rows {
		ordered = append(ordered, pair{url: url, postDate: postDate})
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].postDate.After(ordered[j].postDate) })
	parts := []string{xmlDecl, namespaceURLSetOpen}
	for _, row := range ordered {
		parts = append(parts,
			"  <url>",
			"    <loc>"+escapeXML(row.url)+"</loc>",
			"    <lastmod>"+row.postDate.Format(time.RFC3339)+"</lastmod>",
			"  </url>",
		)
	}
	parts = append(parts, rootCloseTag)
	return strings.Join(parts, "\n") + "\n"
}

func rowsListToXML(rows []SitemapRow) string {
	parts := []string{xmlDecl, namespaceURLSetOpen}
	for _, row := range rows {
		parts = append(parts,
			"  <url>",
			"    <loc>"+escapeXML(row.URL)+"</loc>",
			"    <lastmod>"+row.PostDate.Format(time.RFC3339)+"</lastmod>",
			"  </url>",
		)
	}
	parts = append(parts, rootCloseTag)
	return strings.Join(parts, "\n") + "\n"
}

func escapeXML(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", `'`, "&apos;")
	return replacer.Replace(value)
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

func (s *Service) flushBuffer(buffer map[string]SitemapRow, source string) (int, int, int, map[string]SitemapRow) {
	if len(buffer) == 0 {
		return 0, 0, 0, map[string]SitemapRow{}
	}
	log.Printf("raw-import-worker flush_buffer_start source=%s rows=%d", source, len(buffer))
	inserted, updated, failedDB := 0, 0, 0
	failedRows := map[string]SitemapRow{}
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		tx, err := s.DB.SQL.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		type pendingUpdate struct {
			url           string
			row           SitemapRow
			existingID    int64
			isReady       bool
			rawJSONText   any
			extraJSONText any
		}
		pendingUpdates := make([]pendingUpdate, 0, len(buffer))

		for url, row := range buffer {
			postDate := row.PostDate
			var existingID int64
			var existingPostDate string
			var existingRawJSON sql.NullString
			err := tx.QueryRow(`SELECT id, post_date, raw_json FROM raw_us_jobs WHERE url = ? LIMIT 1`, url).Scan(&existingID, &existingPostDate, &existingRawJSON)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				failedDB++
				failedRows[url] = row
				continue
			}
			rawJSONText := any(nil)
			isReady := false
			if row.RawJSON != nil {
				body, _ := json.Marshal(row.RawJSON)
				rawJSONText = string(body)
				isReady = true
			}
			extraJSONText := any(nil)
			if row.ExtraJSON != nil {
				body, _ := json.Marshal(row.ExtraJSON)
				extraJSONText = string(body)
			}
			if errors.Is(err, sql.ErrNoRows) {
				if _, err := tx.Exec(`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, retry_count, extra_json, raw_json) VALUES (?, ?, ?, ?, false, 0, ?, ?)`, source, url, postDate.Format(time.RFC3339), isReady, extraJSONText, rawJSONText); err != nil {
					failedDB++
					failedRows[url] = row
					continue
				}
				inserted++
				continue
			}

			existingDT, err := normalizeDBDatetime(existingPostDate)
			hasExistingRawJSON := existingRawJSON.Valid && strings.TrimSpace(existingRawJSON.String) != ""
			if err == nil && !postDate.After(existingDT) && hasExistingRawJSON {
				continue
			}
			pendingUpdates = append(pendingUpdates, pendingUpdate{
				url:           url,
				row:           row,
				existingID:    existingID,
				isReady:       isReady,
				rawJSONText:   rawJSONText,
				extraJSONText: extraJSONText,
			})
		}

		parsedByRawID := map[int64]struct{}{}
		if len(pendingUpdates) > 0 {
			rawIDs := make([]int64, 0, len(pendingUpdates))
			for _, candidate := range pendingUpdates {
				rawIDs = append(rawIDs, candidate.existingID)
			}
			parsedRows, err := tx.Query(`SELECT raw_us_job_id FROM parsed_jobs WHERE raw_us_job_id = ANY(?::bigint[])`, rawIDs)
			if err != nil {
				return err
			}
			for parsedRows.Next() {
				var rawID int64
				if err := parsedRows.Scan(&rawID); err == nil {
					parsedByRawID[rawID] = struct{}{}
				}
			}
			parsedRows.Close()
		}

		for _, candidate := range pendingUpdates {
			if _, hasParsed := parsedByRawID[candidate.existingID]; hasParsed {
				var parsedJobID int64
				err := tx.QueryRow(`SELECT id FROM parsed_jobs WHERE raw_us_job_id = ? LIMIT 1`, candidate.existingID).Scan(&parsedJobID)
				if err == nil {
					if _, err := tx.Exec(`DELETE FROM user_job_actions WHERE parsed_job_id = ?`, parsedJobID); err != nil {
						failedDB++
						failedRows[candidate.url] = candidate.row
						continue
					}
				}
				if _, err := tx.Exec(`DELETE FROM parsed_jobs WHERE raw_us_job_id = ?`, candidate.existingID); err != nil {
					failedDB++
					failedRows[candidate.url] = candidate.row
					continue
				}
			}
			if _, err := tx.Exec(
				`UPDATE raw_us_jobs
				 SET source = ?,
				     post_date = ?,
				     is_ready = ?,
				     is_skippable = false,
				     is_parsed = false,
				     retry_count = 0,
				     extra_json = ?,
				     raw_json = ?
				 WHERE id = ?`,
				source,
				candidate.row.PostDate.Format(time.RFC3339),
				candidate.isReady,
				candidate.extraJSONText,
				candidate.rawJSONText,
				candidate.existingID,
			); err != nil {
				failedDB++
				failedRows[candidate.url] = candidate.row
				continue
			}
			updated++
		}

		return tx.Commit()
	})
	if err != nil && database.IsLockedError(err) {
		log.Printf("raw-import-worker flush_buffer_failed source=%s rows=%d reason=locked", source, len(buffer))
		for url, row := range buffer {
			failedRows[url] = row
		}
		return 0, 0, len(buffer), failedRows
	}
	if err != nil {
		log.Printf("raw-import-worker flush_buffer_failed source=%s rows=%d error=%v", source, len(buffer), err)
		for url, row := range buffer {
			failedRows[url] = row
		}
		return 0, 0, len(buffer), failedRows
	}
	log.Printf("raw-import-worker flush_buffer_done source=%s inserted=%d updated=%d failed_db=%d", source, inserted, updated, failedDB)
	return inserted, updated, failedDB, failedRows
}

func (s *Service) ImportRawUSJobsRows(rows []SitemapRow, batchSize int, source string) (Stats, map[string]SitemapRow, map[string]SitemapRow, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	rowSource := source

	log.Printf("raw-import-worker import_rows_start source=%s rows=%d batch_size=%d", rowSource, len(rows), batchSize)

	stats := Stats{}
	buffer := map[string]SitemapRow{}
	allRows := map[string]SitemapRow{}
	failedRows := map[string]SitemapRow{}

	for _, row := range rows {
		stats.Seen++
		if current, ok := allRows[row.URL]; !ok || row.PostDate.After(current.PostDate) {
			allRows[row.URL] = row
		}
		if current, ok := buffer[row.URL]; !ok || row.PostDate.After(current.PostDate) {
			buffer[row.URL] = row
		}
		if len(buffer) >= batchSize {
			inserted, updated, failedDB, failedBatch := s.flushBuffer(buffer, rowSource)
			stats.Inserted += inserted
			stats.Updated += updated
			stats.FailedDB += failedDB
			mergeImportRows(failedRows, failedBatch)
			buffer = map[string]SitemapRow{}
		}
	}
	inserted, updated, failedDB, failedBatch := s.flushBuffer(buffer, rowSource)
	stats.Inserted += inserted
	stats.Updated += updated
	stats.FailedDB += failedDB
	mergeImportRows(failedRows, failedBatch)

	succeededRows := map[string]SitemapRow{}
	for url, row := range allRows {
		if _, failed := failedRows[url]; !failed {
			succeededRows[url] = row
		}
	}
	log.Printf(
		"raw-import-worker import_rows_done source=%s seen=%d inserted=%d updated=%d failed_db=%d failed_rows=%d",
		rowSource,
		stats.Seen,
		stats.Inserted,
		stats.Updated,
		stats.FailedDB,
		len(failedRows),
	)
	return stats, failedRows, succeededRows, nil
}

func (s *Service) PickUnconsumedPayloads(ctx context.Context, limit int, enabledSources map[string]struct{}) ([]struct {
	ID          int64
	Source      string
	PayloadType string
	BodyText    string
}, error,
) {
	if limit <= 0 {
		limit = 1
	}
	query := `SELECT id, source, payload_type, body_text FROM watcher_payloads WHERE consumed_at IS NULL`
	args := make([]any, 0, 2)
	if len(enabledSources) > 0 {
		sources := sortedSourceNames(enabledSources)
		query += ` AND source = ANY(?::text[])`
		args = append(args, sources)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	var rows *sql.Rows
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		var queryErr error
		rows, queryErr = s.DB.SQL.QueryContext(ctx, query, args...)
		return queryErr
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []struct {
		ID          int64
		Source      string
		PayloadType string
		BodyText    string
	}{}
	for rows.Next() {
		var item struct {
			ID          int64
			Source      string
			PayloadType string
			BodyText    string
		}
		if err := rows.Scan(&item.ID, &item.Source, &item.PayloadType, &item.BodyText); err == nil {
			result = append(result, item)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	log.Printf("raw-import-worker picked_payloads=%d limit=%d", len(result), limit)
	return result, nil
}

func sortedSourceNames(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func (s *Service) DeletePayload(payloadID int64) error {
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		_, execErr := s.DB.SQL.Exec(`DELETE FROM watcher_payloads WHERE id = ?`, payloadID)
		return execErr
	})
	if err == nil {
		log.Printf("raw-import-worker payload_deleted payload_id=%d", payloadID)
	}
	return err
}

func (s *Service) DeleteConsumedPayloads() (int64, error) {
	var result sql.Result
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		var execErr error
		result, execErr = s.DB.SQL.Exec(`DELETE FROM watcher_payloads WHERE consumed_at IS NOT NULL`)
		return execErr
	})
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
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		_, execErr := s.DB.SQL.Exec(`UPDATE watcher_payloads SET consumed_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339Nano), payloadID)
		return execErr
	})
	return err
}

func (s *Service) ReplacePayloadBody(payloadID int64, failedRows map[string]time.Time) error {
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		_, execErr := s.DB.SQL.Exec(`UPDATE watcher_payloads SET body_text = ? WHERE id = ?`, rowsToXML(failedRows), payloadID)
		return execErr
	})
	return err
}

func (s *Service) ReplacePayloadRows(payloadID int64, rows []SitemapRow) error {
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		_, execErr := s.DB.SQL.Exec(`UPDATE watcher_payloads SET body_text = ? WHERE id = ?`, rowsListToXML(rows), payloadID)
		return execErr
	})
	if err == nil {
		log.Printf("raw-import-worker payload_updated payload_id=%d remaining_rows=%d", payloadID, len(rows))
	}
	return err
}

func (s *Service) ReplaceBuiltinPayloadRows(payloadID int64, rows []SitemapRow) error {
	payload := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		payload = append(payload, map[string]any{
			"url":          row.URL,
			"post_date":    row.PostDate.Format(time.RFC3339Nano),
			"is_ready":     false,
			"is_skippable": false,
			"is_parsed":    false,
			"retry_count":  0,
			"raw_json":     nil,
		})
	}
	body, _ := json.Marshal(payload)
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		_, execErr := s.DB.SQL.Exec(`UPDATE watcher_payloads SET body_text = ? WHERE id = ?`, string(body), payloadID)
		return execErr
	})
	return err
}

func (s *Service) ReplaceSourcePayloadRows(payloadID int64, source string, rows []map[string]any) error {
	plugin, ok := plugins.Get(source)
	if !ok || plugin.SerializeImportRows == nil {
		return errors.New("unsupported source payload serializer")
	}
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		_, execErr := s.DB.SQL.Exec(`UPDATE watcher_payloads SET body_text = ? WHERE id = ?`, plugin.SerializeImportRows(rows), payloadID)
		return execErr
	})
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

func FailedImportRowsToList(rows map[string]SitemapRow) []SitemapRow {
	out := make([]SitemapRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PostDate.After(out[j].PostDate) })
	return out
}

func mergeImportRows(target map[string]SitemapRow, incoming map[string]SitemapRow) {
	for url, row := range incoming {
		current, ok := target[url]
		if !ok || row.PostDate.After(current.PostDate) {
			target[url] = row
		}
	}
}

func flattenImportRowDates(rows map[string]SitemapRow) map[string]time.Time {
	out := make(map[string]time.Time, len(rows))
	for url, row := range rows {
		out[url] = row.PostDate
	}
	return out
}

func MarshalRows(rows map[string]time.Time) string {
	payload, _ := json.Marshal(rows)
	return string(payload)
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func parseGenericImportRow(row map[string]any, requireRawPayload bool) (SitemapRow, bool) {
	rowURL := stringValue(row["url"])
	if rowURL == "" {
		return SitemapRow{}, false
	}
	postDate, ok := row["post_date"].(time.Time)
	if !ok || postDate.IsZero() {
		return SitemapRow{}, false
	}
	extraPayload, _ := row["extra_payload"].(map[string]any)
	if extraPayload == nil {
		extraPayload, _ = row["extra_json"].(map[string]any)
	}
	if !requireRawPayload {
		return SitemapRow{
			URL:       rowURL,
			PostDate:  postDate,
			ExtraJSON: extraPayload,
			Payload:   row,
		}, true
	}
	rawPayload, ok := row["raw_payload"].(map[string]any)
	if !ok || rawPayload == nil {
		return SitemapRow{}, false
	}
	return SitemapRow{
		URL:       rowURL,
		PostDate:  postDate,
		RawJSON:   rawPayload,
		ExtraJSON: extraPayload,
		Payload:   row,
	}, true
}

func serializeRowForSource(row SitemapRow) map[string]any {
	if row.Payload != nil {
		return row.Payload
	}
	serialized := map[string]any{
		"url":       row.URL,
		"post_date": row.PostDate,
	}
	if row.RawJSON != nil {
		serialized["raw_payload"] = row.RawJSON
	}
	if row.ExtraJSON != nil {
		serialized["extra_payload"] = row.ExtraJSON
	}
	return serialized
}

func (s *Service) RunOnce() error {
	return s.RunOnceWithContext(context.Background())
}

func (s *Service) RunOnceWithContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batchSize := s.Config.BatchSize
	payloadsPerCycle := s.Config.PayloadsPerCycle
	enabledSources := s.Config.EnabledSources
	errorBackoffSeconds := s.Config.ErrorBackoffSeconds

	payloads, err := s.PickUnconsumedPayloads(ctx, payloadsPerCycle, enabledSources)
	if err != nil {
		log.Printf("raw-import-worker cycle_failed error=%v", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(errorBackoffSeconds) * time.Second):
		}
		return nil
	}
	remainingRowsBudget := batchSize
	for _, payload := range payloads {
		if err := ctx.Err(); err != nil {
			return err
		}
		if remainingRowsBudget <= 0 {
			break
		}
		payloadRows, skippedInvalid, supported := ParseRowsForSourcePayload(payload.Source, payload.PayloadType, payload.BodyText)
		if !supported {
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

		stats, failedRows, _, err := s.ImportRawUSJobsRows(rowsToProcess, batchSize, payload.Source)
		if err != nil {
			log.Printf("raw-import-worker payload_failed payload_id=%d source=%s error=%v", payload.ID, payload.Source, err)
			continue
		}
		stats.SkippedInvalid = skippedInvalid
		failedRowsList := FailedImportRowsToList(failedRows)
		remainingRows := append(failedRowsList, unprocessedRows...)
		remainingRowsBudget -= toProcessCount

		if len(remainingRows) > 0 {
			var err error
			serializedRows := make([]map[string]any, 0, len(remainingRows))
			for _, row := range remainingRows {
				serializedRows = append(serializedRows, serializeRowForSource(row))
			}
			err = s.ReplaceSourcePayloadRows(payload.ID, payload.Source, serializedRows)
			if err != nil {
				log.Printf("raw-import-worker payload_update_failed payload_id=%d source=%s error=%v", payload.ID, payload.Source, err)
				continue
			}
			log.Printf("importer partial payload_id=%d seen=%d inserted=%d updated=%d skipped_invalid=%d failed_db=%d remaining_rows=%d remaining_budget=%d", payload.ID, stats.Seen, stats.Inserted, stats.Updated, stats.SkippedInvalid, stats.FailedDB, len(remainingRows), remainingRowsBudget)
			continue
		}
		if err := s.DeletePayload(payload.ID); err != nil {
			log.Printf("raw-import-worker payload_delete_failed payload_id=%d source=%s error=%v", payload.ID, payload.Source, err)
			continue
		}
		log.Printf("importer imported payload_id=%d seen=%d inserted=%d updated=%d skipped_invalid=%d failed_db=%d", payload.ID, stats.Seen, stats.Inserted, stats.Updated, stats.SkippedInvalid, stats.FailedDB)
	}
	log.Printf("raw-import-worker cycle_complete processed_payloads=%d remaining_rows_budget=%d", len(payloads), remainingRowsBudget)
	return nil
}

func (s *Service) RunForever() error {
	return s.RunForeverWithContext(context.Background())
}

func (s *Service) RunForeverWithContext(ctx context.Context) error {
	sleepDuration := s.Config.SleepDuration
	runOnce := s.Config.RunOnce

	if deleted, err := s.DeleteConsumedPayloads(); err != nil {
		log.Fatal(err)
	} else if deleted > 0 {
		log.Printf("importer removed legacy consumed payloads=%d", deleted)
	}

	for {
		if err := s.RunOnceWithContext(ctx); err != nil {
			return err
		}
		if runOnce {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
		}
	}
}
