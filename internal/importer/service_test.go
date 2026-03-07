package importer

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/database"
)

func TestExtractCompleteURLBlocksFromIncompleteXML(t *testing.T) {
	xmlText := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/jobs/1</loc>
    <lastmod>2026-02-12T16:00:00+00:00</lastmod>
  </url>
  <url>
    <loc>https://example.com/jobs/2</loc>
    <lastmod>2026-02-12T15:00:00+00:00</lastmod>
  </url>
  <url>
    <loc>https://example.com/jobs/3</loc>
`
	blocks := extractCompleteURLBlocks(xmlText)
	if len(blocks) != 2 || !strings.Contains(blocks[0], "jobs/1") || !strings.Contains(blocks[1], "jobs/2") {
		t.Fatalf("unexpected blocks %#v", blocks)
	}
}

func TestExtractRowFromURLBlockHandlesNamespace(t *testing.T) {
	block := `<url>
  <loc>https://example.com/jobs/42</loc>
  <lastmod>2026-02-12T17:30:00+00:00</lastmod>
</url>`
	loc, lastmod, ok := extractRowFromURLBlock(block)
	if !ok || loc != "https://example.com/jobs/42" || lastmod != "2026-02-12T17:30:00+00:00" {
		t.Fatalf("unexpected row %v %v %v", loc, lastmod, ok)
	}
}

func TestIterSitemapRowsReturnsOnlyCompleteURLTags(t *testing.T) {
	dir := t.TempDir()
	xmlPath := filepath.Join(dir, "latest_delta_broken.xml")
	if err := os.WriteFile(xmlPath, []byte(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://example.com/a</loc><lastmod>2026-02-12T18:00:00+00:00</lastmod></url>
<url><loc>https://example.com/b</loc><lastmod>2026-02-12T17:00:00+00:00</lastmod></url>
<url><loc>https://example.com/c</loc>`), 0o644); err != nil {
		t.Fatal(err)
	}
	rows, err := iterSitemapRows(xmlPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][0] != "https://example.com/a" || rows[1][0] != "https://example.com/b" {
		t.Fatalf("unexpected rows %#v", rows)
	}
}

func TestProcessImportFileKeepsFailedRowsAndExportsSuccesses(t *testing.T) {
	dir := t.TempDir()
	db, err := database.Open("file:test_importer_success?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(db)

	importFile := filepath.Join(dir, "latest_delta.xml")
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc><lastmod>2026-02-12T18:00:00+00:00</lastmod></url>
  <url><loc>https://example.com/b</loc><lastmod>2026-02-12T17:00:00+00:00</lastmod></url>
</urlset>`
	if err := os.WriteFile(importFile, []byte(xml), 0o644); err != nil {
		t.Fatal(err)
	}

	stats, importedPath, err := svc.ProcessImportFile(importFile, dir, "imported", 100)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Inserted != 2 || stats.FailedDB != 0 {
		t.Fatalf("unexpected success stats %#v", stats)
	}
	if _, err := os.Stat(importFile); !os.IsNotExist(err) {
		t.Fatalf("expected original file to be renamed")
	}
	importedRaw, err := os.ReadFile(importedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(importedRaw), "https://example.com/a") || !strings.Contains(string(importedRaw), "https://example.com/b") {
		t.Fatalf("unexpected imported file %s", string(importedRaw))
	}
}

func TestImportRawUSJobsReturnsFailedRowsAndSuccessesSeparately(t *testing.T) {
	dir := t.TempDir()
	db, err := database.Open("file:test_importer_fail?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(db)

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (1, 'https://example.com/b', ?, 1, 0, 1, 0, '{}')`, time.Date(2026, 2, 12, 16, 0, 0, 0, time.UTC).Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, created_at_source, url) VALUES (1, 'Old Role', ?, 'https://example.com/b')`, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.Exec(`ALTER TABLE raw_us_jobs RENAME TO raw_us_jobs_disabled`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = db.SQL.Exec(`ALTER TABLE raw_us_jobs_disabled RENAME TO raw_us_jobs`) }()

	xmlPath := filepath.Join(dir, "latest_delta.xml")
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc><lastmod>2026-02-12T18:00:00+00:00</lastmod></url>
  <url><loc>https://example.com/b</loc><lastmod>2026-02-12T17:00:00+00:00</lastmod></url>
</urlset>`
	if err := os.WriteFile(xmlPath, []byte(xml), 0o644); err != nil {
		t.Fatal(err)
	}

	stats, failedRows, succeededRows, err := svc.ImportRawUSJobs(xmlPath, 100)
	if err != nil {
		t.Fatal(err)
	}
	if stats.FailedDB != 2 || len(failedRows) != 2 || len(succeededRows) != 0 {
		t.Fatalf("unexpected failure split stats=%#v failed=%#v succeeded=%#v", stats, failedRows, succeededRows)
	}
}

var _ = sql.ErrNoRows
