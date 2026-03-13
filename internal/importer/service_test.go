package importer

import (
	"context"
	"database/sql"
	"goapplyjob-golang-backend/internal/database"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func requirePostgresTestDB(t *testing.T) {
	t.Helper()
	if !database.HasTestDatabaseURL() {
		t.Skip("TEST_DATABASE_URL is required for DB-backed tests")
	}
}

func testDatabaseURL(t *testing.T, schemaName string) string {
	t.Helper()
	requirePostgresTestDB(t)
	baseURL := database.TestDatabaseBaseURL()
	adminDB, err := sql.Open("pgx", baseURL)
	if err != nil {
		t.Fatalf("open test postgres connection: %v", err)
	}
	defer adminDB.Close()
	schema := "test_" + strings.ReplaceAll(strings.ToLower(schemaName), "-", "_") + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := adminDB.ExecContext(context.Background(), `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`); err != nil {
		t.Fatalf("create test schema %q: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanupDB, openErr := sql.Open("pgx", baseURL)
		if openErr != nil {
			return
		}
		defer cleanupDB.Close()
		_, _ = cleanupDB.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
	})
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	q := parsed.Query()
	q.Set("search_path", schema)
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

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

func TestPickUnconsumedPayloadsReturnsNewestFirst(t *testing.T) {
	requirePostgresTestDB(t)
	db, err := database.Open(testDatabaseURL(t, "test_importer_order"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(Config{}, db)

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

	payloads, err := svc.PickUnconsumedPayloads(context.Background(), 2, map[string]struct{}{sourceRemoterocketship: {}, sourceBuiltin: {}})
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
	requirePostgresTestDB(t)
	db, err := database.Open(testDatabaseURL(t, "test_importer_replace_rows"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(Config{}, db)

	var payloadID int64
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`INSERT INTO watcher_payloads (source_url, payload_type, body_text, created_at)
		 VALUES (?, 'delta_xml', ?, ?)
		 RETURNING id`,
		"https://example.com/jobs.xml",
		`<urlset></urlset>`,
		time.Now().UTC().Format(time.RFC3339Nano),
	).Scan(&payloadID); err != nil {
		t.Fatal(err)
	}

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
	requirePostgresTestDB(t)
	db, err := database.Open(testDatabaseURL(t, "test_importer_delete_consumed"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc := New(Config{}, db)

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
