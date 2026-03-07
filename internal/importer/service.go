package importer

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
	"goapplyjob-golang-backend/internal/sources/workable"
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
	URL      string
	PostDate time.Time
	RawJSON  map[string]any
}

type Service struct {
	DB *database.DB
}

const sourceName = "remoterocketship"
const (
	sourceBuiltin           = "builtin"
	sourceWorkable          = "workable"
	payloadTypeXML          = "delta_xml"
	payloadTypeJSON         = "delta"
	defaultPayloadsPerCycle = 50
)

func New(db *database.DB) *Service { return &Service{DB: db} }

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

func parseRowsFromXMLText(xmlText string) ([]SitemapRow, int) {
	rows := iterSitemapRowsText(xmlText)
	parsed := make([]SitemapRow, 0, len(rows))
	skippedInvalid := 0
	for _, row := range rows {
		postDate, err := normalizeDBDatetime(row[1])
		if err != nil {
			skippedInvalid++
			continue
		}
		parsed = append(parsed, SitemapRow{URL: row[0], PostDate: postDate})
	}
	return parsed, skippedInvalid
}

func ParseRowsForImport(xmlText string) ([]SitemapRow, int) {
	return parseRowsFromXMLText(xmlText)
}

func ParseRowsForBuiltinPayload(payloadText string) ([]SitemapRow, int) {
	var payload []map[string]any
	if err := json.Unmarshal([]byte(payloadText), &payload); err != nil {
		return nil, 1
	}
	rows := make([]SitemapRow, 0, len(payload))
	skipped := 0
	for _, item := range payload {
		rowURL, _ := item["url"].(string)
		postDateRaw, _ := item["post_date"].(string)
		if strings.TrimSpace(rowURL) == "" || strings.TrimSpace(postDateRaw) == "" {
			skipped++
			continue
		}
		postDate, err := normalizeDBDatetime(postDateRaw)
		if err != nil {
			skipped++
			continue
		}
		rows = append(rows, SitemapRow{URL: rowURL, PostDate: postDate})
	}
	return rows, skipped
}

func ParseRowsForWorkablePayload(payloadText string) ([]SitemapRow, int) {
	rows, skipped := workable.ParseImportRows(payloadText)
	out := make([]SitemapRow, 0, len(rows))
	for _, row := range rows {
		postDate, _ := row["post_date"].(time.Time)
		rawPayload, _ := row["raw_payload"].(map[string]any)
		out = append(out, SitemapRow{URL: stringValue(row["url"]), PostDate: postDate, RawJSON: rawPayload})
	}
	return out, skipped
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
	inserted, updated, failedDB := 0, 0, 0
	failedRows := map[string]SitemapRow{}

	tx, err := s.DB.SQL.Begin()
	if err != nil {
		for url, row := range buffer {
			failedRows[url] = row
		}
		return 0, 0, len(buffer), failedRows
	}
	defer tx.Rollback()

	for url, row := range buffer {
		postDate := row.PostDate
		var existingID int64
		var existingPostDate string
		err := tx.QueryRow(`SELECT id, post_date FROM raw_us_jobs WHERE source = ? AND url = ? LIMIT 1`, source, url).Scan(&existingID, &existingPostDate)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			failedDB++
			failedRows[url] = row
			continue
		}
		rawJSONText := any(nil)
		isReady := 0
		if row.RawJSON != nil {
			body, _ := json.Marshal(row.RawJSON)
			rawJSONText = string(body)
			isReady = 1
		}
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := tx.Exec(`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, retry_count, raw_json) VALUES (?, ?, ?, ?, 0, 0, ?)`, source, url, postDate.Format(time.RFC3339), isReady, rawJSONText); err != nil {
				failedDB++
				failedRows[url] = row
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
			failedRows[url] = row
			continue
		}
		if _, err := tx.Exec(`DELETE FROM raw_us_jobs WHERE id = ?`, existingID); err != nil {
			failedDB++
			failedRows[url] = row
			continue
		}
		if _, err := tx.Exec(`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, retry_count, raw_json) VALUES (?, ?, ?, ?, 0, 0, ?)`, source, url, postDate.Format(time.RFC3339), isReady, rawJSONText); err != nil {
			failedDB++
			failedRows[url] = row
			continue
		}
		updated++
	}

	if err := tx.Commit(); err != nil {
		for url, row := range buffer {
			failedRows[url] = row
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
		return stats, flattenImportRowDates(failedRows), flattenImportRowDates(succeededRows), err
	}
	stats.SkippedInvalid = skippedInvalid
	return stats, flattenImportRowDates(failedRows), flattenImportRowDates(succeededRows), nil
}

func (s *Service) ImportRawUSJobsRows(rows []SitemapRow, batchSize int, source ...string) (Stats, map[string]SitemapRow, map[string]SitemapRow, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	rowSource := sourceName
	if len(source) > 0 && strings.TrimSpace(source[0]) != "" {
		rowSource = strings.TrimSpace(source[0])
	}

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
	return stats, failedRows, succeededRows, nil
}

func (s *Service) PickUnconsumedPayloads(limit int, enabledSources map[string]struct{}) ([]struct {
	ID          int64
	Source      string
	PayloadType string
	BodyText    string
}, error) {
	if limit <= 0 {
		limit = 1
	}
	query := `SELECT id, source, payload_type, body_text FROM watcher_payloads WHERE consumed_at IS NULL`
	args := make([]any, 0, len(enabledSources)+1)
	if len(enabledSources) > 0 {
		placeholders := make([]string, 0, len(enabledSources))
		sources := sortedSourceNames(enabledSources)
		for _, source := range sources {
			placeholders = append(placeholders, "?")
			args = append(args, source)
		}
		query += ` AND source IN (` + strings.Join(placeholders, ", ") + `)`
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.DB.SQL.Query(query, args...)
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
	return result, rows.Err()
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
	_, err := s.DB.SQL.Exec(`UPDATE watcher_payloads SET body_text = ? WHERE id = ?`, string(body), payloadID)
	return err
}

func (s *Service) ReplaceSourcePayloadRows(payloadID int64, source string, rows []map[string]any) error {
	plugin, ok := plugins.Get(source)
	if !ok || plugin.SerializeImportRows == nil {
		return errors.New("unsupported source payload serializer")
	}
	_, err := s.DB.SQL.Exec(`UPDATE watcher_payloads SET body_text = ? WHERE id = ?`, plugin.SerializeImportRows(rows), payloadID)
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

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
