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
)

const (
	xmlDecl             = `<?xml version="1.0" encoding="UTF-8"?>`
	namespaceURLSetOpen = `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`
	rootCloseTag        = `</urlset>`
)

var urlBlockPattern = regexp.MustCompile(`(?is)<url(?:\s[^>]*)?>.*?</url>`)

type Stats struct {
	Seen           int
	Inserted       int
	Updated        int
	SkippedInvalid int
	FailedDB       int
}

type Service struct {
	DB *database.DB
}

func New(db *database.DB) *Service { return &Service{DB: db} }

func extractCompleteURLBlocks(xmlText string) []string {
	return urlBlockPattern.FindAllString(xmlText, -1)
}

type xmlURL struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
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

func iterSitemapRows(xmlPath string) ([][2]string, error) {
	raw, err := os.ReadFile(xmlPath)
	if err != nil {
		return nil, err
	}
	blocks := extractCompleteURLBlocks(string(raw))
	rows := make([][2]string, 0, len(blocks))
	for _, block := range blocks {
		loc, lastmod, ok := extractRowFromURLBlock(block)
		if ok {
			rows = append(rows, [2]string{loc, lastmod})
		}
	}
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
	if batchSize <= 0 {
		batchSize = 100
	}

	stats := Stats{}
	buffer := map[string]time.Time{}
	allRows := map[string]time.Time{}
	failedRows := map[string]time.Time{}

	rows, err := iterSitemapRows(xmlPath)
	if err != nil {
		return stats, nil, nil, err
	}
	for _, row := range rows {
		stats.Seen++
		postDate, err := normalizeDBDatetime(row[1])
		if err != nil {
			stats.SkippedInvalid++
			continue
		}
		if current, ok := allRows[row[0]]; !ok || postDate.After(current) {
			allRows[row[0]] = postDate
		}
		if current, ok := buffer[row[0]]; !ok || postDate.After(current) {
			buffer[row[0]] = postDate
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
