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
	rows := iterSitemapRowsText(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://example.com/a</loc><lastmod>2026-02-12T18:00:00+00:00</lastmod></url>
<url><loc>https://example.com/b</loc><lastmod>2026-02-12T17:00:00+00:00</lastmod></url>
<url><loc>https://example.com/c</loc>`)
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
	importedFiles, err := filepath.Glob(filepath.Join(dir, "imported*.xml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(importedFiles) != 1 {
		t.Fatalf("expected one imported file, got %d: %#v", len(importedFiles), importedFiles)
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

func TestImportRawUSJobsTextProcessesWatcherPayloadBody(t *testing.T) {
	db, err := database.Open("file:test_importer_payloads?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(db)

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO watcher_payloads (source_url, payload_type, body_text, created_at) VALUES (?, 'delta_xml', ?, ?)`,
		"https://example.com/jobs.xml",
		`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc><lastmod>2026-02-12T18:00:00+00:00</lastmod></url>
</urlset>`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	payloads, err := svc.PickUnconsumedPayloads(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected one payload, got %d", len(payloads))
	}
	stats, failedRows, succeededRows, err := svc.ImportRawUSJobsText(payloads[0].BodyText, 100)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Inserted != 1 || len(failedRows) != 0 || len(succeededRows) != 1 {
		t.Fatalf("unexpected payload import results stats=%#v failed=%#v succeeded=%#v", stats, failedRows, succeededRows)
	}
	if err := svc.DeletePayload(payloads[0].ID); err != nil {
		t.Fatal(err)
	}
}

func TestPickUnconsumedPayloadsReturnsNewestFirst(t *testing.T) {
	db, err := database.Open("file:test_importer_order?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(db)

	now := time.Now().UTC()
	for idx, createdAt := range []time.Time{now.Add(-2 * time.Minute), now.Add(-1 * time.Minute), now} {
		_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO watcher_payloads (source_url, payload_type, body_text, created_at) VALUES (?, 'delta_xml', ?, ?)`,
			"https://example.com/jobs.xml",
			`<urlset></urlset>`,
			createdAt.Format(time.RFC3339Nano),
		)
		if err != nil {
			t.Fatalf("insert payload %d: %v", idx, err)
		}
	}

	payloads, err := svc.PickUnconsumedPayloads(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 2 {
		t.Fatalf("expected two payloads, got %d", len(payloads))
	}
	if payloads[0].ID <= payloads[1].ID {
		t.Fatalf("expected newest payload first, got ids %d then %d", payloads[0].ID, payloads[1].ID)
	}
}

func TestReplacePayloadRowsKeepsRemainingRowsInOrder(t *testing.T) {
	db, err := database.Open("file:test_importer_replace_rows?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(db)

	result, err := db.SQL.ExecContext(context.Background(), `INSERT INTO watcher_payloads (source_url, payload_type, body_text, created_at) VALUES (?, 'delta_xml', ?, ?)`,
		"https://example.com/jobs.xml",
		`<urlset></urlset>`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	payloadID, _ := result.LastInsertId()

	rows := []SitemapRow{
		{URL: "https://example.com/new", PostDate: time.Date(2026, 2, 12, 18, 0, 0, 0, time.UTC)},
		{URL: "https://example.com/old", PostDate: time.Date(2026, 2, 12, 17, 0, 0, 0, time.UTC)},
	}
	if err := svc.ReplacePayloadRows(payloadID, rows); err != nil {
		t.Fatal(err)
	}

	var bodyText string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT body_text FROM watcher_payloads WHERE id = ?`, payloadID).Scan(&bodyText); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(bodyText, "https://example.com/new") || !strings.Contains(bodyText, "https://example.com/old") {
		t.Fatalf("unexpected payload body %q", bodyText)
	}
	if strings.Index(bodyText, "https://example.com/new") > strings.Index(bodyText, "https://example.com/old") {
		t.Fatalf("expected newest row first in payload body %q", bodyText)
	}
}

func TestDeleteConsumedPayloadsRemovesLegacyRows(t *testing.T) {
	db, err := database.Open("file:test_importer_delete_consumed?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(db)

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO watcher_payloads (source_url, payload_type, body_text, consumed_at, created_at) VALUES (?, 'delta_xml', ?, ?, ?)`,
		"https://example.com/jobs.xml",
		`<urlset></urlset>`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO watcher_payloads (source_url, payload_type, body_text, created_at) VALUES (?, 'delta_xml', ?, ?)`,
		"https://example.com/jobs.xml",
		`<urlset></urlset>`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	deleted, err := svc.DeleteConsumedPayloads()
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("expected one deleted payload, got %d", deleted)
	}
	var remaining int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM watcher_payloads`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("expected one remaining payload, got %d", remaining)
	}
}

var _ = sql.ErrNoRows
