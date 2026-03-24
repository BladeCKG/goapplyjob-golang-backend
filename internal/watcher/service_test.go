package watcher

import (
	"context"
	"database/sql"
	"errors"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/remotive"
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
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	q := parsedURL.Query()
	q.Set("search_path", schema)
	parsedURL.RawQuery = q.Encode()
	return parsedURL.String()
}

func buildService(t *testing.T) *Service {
	t.Helper()
	requirePostgresTestDB(t)
	db, err := database.Open(testDatabaseURL(t, "watcher_test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return New(Config{
		Enabled:                          true,
		RemoteRocketshipUSJobSitemapURLs: []string{"https://example.com/jobs.xml"},
		IntervalMinutes:                  1,
		SampleKB:                         8,
		TimeoutSeconds:                   30,
		BuiltinBaseURL:                   "",
		BuiltinMaxPage:                   1000,
		BuiltinPagesPerCycle:             25,
		HiringCafeSearchAPIURL:           "",
		HiringCafeTotalCountURL:          "",
		HiringCafePageSize:               200,
		EnabledSources: map[string]struct{}{
			sourceRemoterocketship: {},
		},
	}, db)
}

type stubFetcher struct {
	fn func(context.Context, string) (string, int, error)
}

func (s stubFetcher) ReadHTML(ctx context.Context, rawURL string) (string, int, error) {
	return s.fn(ctx, rawURL)
}

func (s stubFetcher) ReadHTMLWithLimit(ctx context.Context, rawURL string, maxBytes int64) (string, int, error) {
	body, status, err := s.ReadHTML(ctx, rawURL)
	if err != nil || maxBytes <= 0 {
		return body, status, err
	}
	if int64(len(body)) <= maxBytes {
		return body, status, err
	}
	return body[:maxBytes], status, err
}

func TestDeltaNewerThanLastmodReturnsOnlyNewerURLBlocks(t *testing.T) {
	service := buildService(t)
	fullData := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://example.com/new</loc><lastmod>2026-02-12T17:00:00+00:00</lastmod></url>
<url><loc>https://example.com/mid</loc><lastmod>2026-02-12T16:00:00+00:00</lastmod></url>
<url><loc>https://example.com/old</loc><lastmod>2026-02-12T15:00:00+00:00</lastmod></url>
</urlset>`)
	delta := service.DeltaNewerThanLastmod(fullData, "2026-02-12T15:30:00+00:00")
	output := string(delta)
	if !strings.Contains(output, "https://example.com/new") || !strings.Contains(output, "https://example.com/mid") || strings.Contains(output, "https://example.com/old") || !strings.Contains(output, "</urlset>") {
		t.Fatalf("unexpected delta: %s", output)
	}
}

func TestDeltaNewerThanLastmodReturnsEmptyWhenNoNewRows(t *testing.T) {
	service := buildService(t)
	fullData := []byte(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://example.com/a</loc><lastmod>2026-02-12T10:00:00+00:00</lastmod></url>
<url><loc>https://example.com/b</loc><lastmod>2026-02-12T09:00:00+00:00</lastmod></url>
</urlset>`)
	delta := service.DeltaNewerThanLastmod(fullData, "2026-02-12T10:00:00+00:00")
	if len(delta) != 0 {
		t.Fatalf("expected empty delta, got %s", string(delta))
	}
}

func TestExtractFirstAndLastLastmodFromPartialSample(t *testing.T) {
	service := buildService(t)
	sample := []byte(`<urlset>
<url><loc>https://example.com/a</loc><lastmod>2026-02-12T19:00:00+00:00</lastmod></url>
<url><loc>https://example.com/b</loc><lastmod>2026-02-12T18:00:00+00:00</lastmod></url>
<url><loc>https://example.com/c</loc>`)
	if got := service.ExtractFirstLastmod(sample); got != "2026-02-12T19:00:00+00:00" {
		t.Fatalf("first lastmod=%s", got)
	}
	if got := service.ExtractLastLastmod(sample); got != "2026-02-12T18:00:00+00:00" {
		t.Fatalf("last lastmod=%s", got)
	}
}

func TestRunOnceSkipsDeltaFileWhenDeltaIsEmpty(t *testing.T) {
	service := buildService(t)
	sample := []byte(`<urlset>
<url><loc>https://example.com/a</loc><lastmod>2026-02-12T10:00:00+00:00</lastmod></url>
</urlset>`)
	fullData := []byte(`<urlset>
<url><loc>https://example.com/a</loc><lastmod>2026-02-12T10:00:00+00:00</lastmod></url>
</urlset>`)

	service.RemoteRocketShipUSJobsSitemapFetchSample = func(context.Context, string) ([]byte, error) { return sample, nil }
	service.RemoteRocketShipUSJobsSitemapFetchFull = func(context.Context, string) ([]byte, error) { return fullData, nil }
	if _, err := service.DB.SQL.ExecContext(context.Background(), `INSERT INTO watcher_states (source, state_json, updated_at) VALUES (?, ?, ?)`, "remoterocketship", `{"sitemaps":{"https://example.com/jobs.xml":{"source_url":"https://example.com/jobs.xml","sample_hash":"old-hash","first_lastmod":"2026-02-12T10:00:00+00:00"}}}`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	if err := service.RunOnce(); err != nil {
		t.Fatal(err)
	}
	status := service.Status()
	if status["remoterocketship_last_delta_payload_id"] != nil {
		t.Fatalf("expected nil delta payload id, got %#v", status["remoterocketship_last_delta_payload_id"])
	}
	if status["remoterocketship_last_delta_size"].(int) != 0 {
		t.Fatalf("expected zero delta size, got %#v", status["remoterocketship_last_delta_size"])
	}
}

func TestRunOnceUsesSampleDeltaWithoutFullFetch(t *testing.T) {
	service := buildService(t)
	sample := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://example.com/new</loc><lastmod>2026-02-12T11:00:00+00:00</lastmod></url>
<url><loc>https://example.com/old</loc><lastmod>2026-02-12T10:00:00+00:00</lastmod></url>
</urlset>`)

	service.RemoteRocketShipUSJobsSitemapFetchSample = func(context.Context, string) ([]byte, error) { return sample, nil }
	service.RemoteRocketShipUSJobsSitemapFetchFull = func(context.Context, string) ([]byte, error) { return nil, errors.New("full fetch should not be called") }
	if _, err := service.DB.SQL.ExecContext(context.Background(), `INSERT INTO watcher_states (source, state_json, updated_at) VALUES (?, ?, ?)`, "remoterocketship", `{"sitemaps":{"https://example.com/jobs.xml":{"source_url":"https://example.com/jobs.xml","sample_hash":"old-hash","first_lastmod":"2026-02-12T10:00:00+00:00"}}}`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	if err := service.RunOnce(); err != nil {
		t.Fatal(err)
	}

	status := service.Status()
	if status["remoterocketship_last_delta_source"] != "sample_lastmod_window" {
		t.Fatalf("expected sample_lastmod_window source, got %#v", status["remoterocketship_last_delta_source"])
	}
	if status["remoterocketship_last_delta_payload_id"] == nil {
		t.Fatalf("expected delta payload id, got %#v", status["remoterocketship_last_delta_payload_id"])
	}
	if status["remoterocketship_last_delta_size"].(int) == 0 {
		t.Fatalf("expected non-zero delta size")
	}
}

func TestRunOnceRemoteRocketshipPersistsStateInDB(t *testing.T) {
	service := buildService(t)
	sample := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://example.com/new</loc><lastmod>2026-02-12T11:00:00+00:00</lastmod></url>
</urlset>`)

	service.RemoteRocketShipUSJobsSitemapFetchSample = func(context.Context, string) ([]byte, error) { return sample, nil }
	service.RemoteRocketShipUSJobsSitemapFetchFull = func(context.Context, string) ([]byte, error) { return sample, nil }

	if err := service.RunOnce(); err != nil {
		t.Fatal(err)
	}

	state, err := service.loadStatePayload(context.Background(), sourceRemoterocketship)
	if err != nil {
		t.Fatal(err)
	}
	lastScanAt, _ := state["last_scan_at"].(string)
	if lastScanAt == "" {
		t.Fatalf("expected last_scan_at to be stored, got %#v", state["last_scan_at"])
	}
	sitemaps, _ := state["sitemaps"].(map[string]any)
	if sitemaps == nil {
		t.Fatalf("expected sitemaps state, got %#v", state["sitemaps"])
	}
	sitemapState, _ := sitemaps["https://example.com/jobs.xml"].(map[string]any)
	if sitemapState == nil {
		t.Fatalf("expected stored sitemap state, got %#v", sitemaps)
	}
	if sitemapState["source_url"] != "https://example.com/jobs.xml" {
		t.Fatalf("unexpected source_url %#v", sitemapState["source_url"])
	}
	if sitemapState["sample_hash"] == "" {
		t.Fatalf("expected sample_hash to be stored, got %#v", sitemapState["sample_hash"])
	}
	if sitemapState["first_lastmod"] != "2026-02-12T11:00:00+00:00" {
		t.Fatalf("unexpected first_lastmod %#v", sitemapState["first_lastmod"])
	}
}

func TestRunForeverRunOnceExecutesSingleCycle(t *testing.T) {
	service := buildService(t)
	sample := []byte(`<urlset><url><loc>https://example.com/a</loc><lastmod>2026-02-12T10:00:00+00:00</lastmod></url></urlset>`)
	sampleCalls := 0
	fullCalls := 0
	service.RemoteRocketShipUSJobsSitemapFetchSample = func(context.Context, string) ([]byte, error) {
		sampleCalls++
		return sample, nil
	}
	service.RemoteRocketShipUSJobsSitemapFetchFull = func(context.Context, string) ([]byte, error) {
		fullCalls++
		return sample, nil
	}

	if err := service.RunForever(true); err != nil {
		t.Fatal(err)
	}
	if sampleCalls != 1 {
		t.Fatalf("expected one sample fetch, got %d", sampleCalls)
	}
	if fullCalls != 1 {
		t.Fatalf("expected one full fetch, got %d", fullCalls)
	}
	if service.Status()["running"].(bool) {
		t.Fatalf("expected watcher not running after run-once")
	}
}

func TestBuiltinScansNextPagesThenUpperPages(t *testing.T) {
	service := buildService(t)
	service.Config.BuiltinBaseURL = "https://builtin.com/jobs?page={page}"
	service.Config.BuiltinMaxPage = 1000
	service.Config.BuiltinPagesPerCycle = 4
	service.Config.EnabledSources = map[string]struct{}{sourceBuiltin: {}}
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		page := rawURL[strings.LastIndex(rawURL, "=")+1:]
		switch page {
		case "11":
			return builtinPageHTML("https://builtin.com/job/new-a/11111", 11111, "2026-02-18T00:00:00+00:00"), 200, nil
		case "12":
			return builtinPageHTML("https://builtin.com/job/marker/12121", 12121, "2026-02-17T00:00:00+00:00"), 200, nil
		case "10":
			return builtinPageHTML("https://builtin.com/job/up-10/10101", 10101, "2026-02-16T00:00:00+00:00"), 200, nil
		case "9":
			return builtinPageHTML("https://builtin.com/job/up-9/9090", 9090, "2026-02-15T00:00:00+00:00"), 200, nil
		default:
			return "No job results", 200, nil
		}
	}}
	if err := service.saveStatePayload(context.Background(), sourceBuiltin, map[string]any{
		"next_page":      10,
		"last_job_url":   "https://builtin.com/job/marker/12121",
		"last_post_date": "2026-02-17T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.runOnceBuiltin(context.Background()); err != nil {
		t.Fatal(err)
	}

	rows, err := service.DB.SQL.QueryContext(context.Background(), `SELECT source_url FROM watcher_payloads WHERE source = ? ORDER BY id ASC`, sourceBuiltin)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := []string{}
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			t.Fatal(err)
		}
		got = append(got, url)
	}
	expected := []string{
		"https://builtin.com/jobs?page=11",
		"https://builtin.com/jobs?page=12",
		"https://builtin.com/jobs?page=10",
		"https://builtin.com/jobs?page=9",
	}
	if strings.Join(got, ",") != strings.Join(expected, ",") {
		t.Fatalf("payload pages=%v expected=%v", got, expected)
	}
	state, err := service.loadStatePayload(context.Background(), sourceBuiltin)
	if err != nil {
		t.Fatal(err)
	}
	if intFromAny(state["next_page"], 0) != 8 {
		t.Fatalf("expected next_page=8, got %#v", state["next_page"])
	}
}

func TestBuiltinKeepsNextPageAtOneAfterFullScan(t *testing.T) {
	service := buildService(t)
	service.Config.BuiltinBaseURL = "https://builtin.com/jobs?page={page}"
	service.Config.BuiltinMaxPage = 2
	service.Config.BuiltinPagesPerCycle = 5
	service.Config.EnabledSources = map[string]struct{}{sourceBuiltin: {}}
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		page := rawURL[strings.LastIndex(rawURL, "=")+1:]
		switch page {
		case "2":
			return builtinPageHTML("https://builtin.com/job/two/2", 2, "2026-02-18T00:00:00+00:00"), 200, nil
		case "1":
			return builtinPageHTML("https://builtin.com/job/one/1", 1, "2026-02-17T00:00:00+00:00"), 200, nil
		default:
			return "No job results", 200, nil
		}
	}}

	if err := service.runOnceBuiltin(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := service.loadStatePayload(context.Background(), sourceBuiltin)
	if err != nil {
		t.Fatal(err)
	}
	if intFromAny(state["next_page"], 0) != 1 {
		t.Fatalf("expected next_page=1 after full scan, got %#v", state["next_page"])
	}
}

func TestRemotiveWatcherUsesJobIDCutoffAndNewestFirst(t *testing.T) {
	service := buildService(t)
	service.Config.RemotiveSitemapURLTemplate = "https://remotive.com/sitemap-job-postings-{partition}.xml"
	service.Config.RemotiveSitemapMaxIndex = 8
	service.Config.RemotiveSitemapMinIndex = 1
	service.Config.EnabledSources = map[string]struct{}{sourceRemotive: {}}
	sitemap := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-3803900</loc><lastmod>2026-02-01T00:00:00+00:00</lastmod></url>
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-3803910</loc><lastmod>2026-02-01T00:00:00+00:00</lastmod></url>
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-3803920</loc><lastmod>2026-02-01T00:00:00+00:00</lastmod></url>
</urlset>`
	if err := service.saveStatePayload(context.Background(), sourceRemotive, map[string]any{"latest_job_id": 3803900}); err != nil {
		t.Fatal(err)
	}
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		if rawURL != "https://remotive.com/sitemap-job-postings-8.xml" {
			return "", 500, errors.New("unexpected URL: " + rawURL)
		}
		return sitemap, 200, nil
	}}

	if err := service.runOnceRemotive(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := service.loadStatePayload(context.Background(), sourceRemotive)
	if err != nil {
		t.Fatal(err)
	}
	if intFromAny(state["latest_job_id"], 0) != 3803920 {
		t.Fatalf("expected latest_job_id=3803920 got %#v", state["latest_job_id"])
	}
	var body string
	if err := service.DB.SQL.QueryRowContext(context.Background(), `SELECT body_text FROM watcher_payloads WHERE source = ? ORDER BY id DESC LIMIT 1`, sourceRemotive).Scan(&body); err != nil {
		t.Fatal(err)
	}
	rows, skipped := remotive.ParseImportRows(body)
	if skipped != 0 {
		t.Fatalf("unexpected skipped rows: %d", skipped)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows got %d", len(rows))
	}
	got := []int{
		extractRemotiveJobIDFromURL(strings.TrimSpace(anyString(rows[0]["url"]))),
		extractRemotiveJobIDFromURL(strings.TrimSpace(anyString(rows[1]["url"]))),
	}
	if got[0] != 3803920 || got[1] != 3803910 {
		t.Fatalf("unexpected id order %#v", got)
	}
}

func TestExtractRemotiveJobIDFromCurrentSlugFormat(t *testing.T) {
	got := extractRemotiveJobIDFromURL("https://remotive.com/remote/jobs/all-others/video-editor-3888494")
	if got != 3888494 {
		t.Fatalf("expected id 3888494, got %d", got)
	}
}

func TestRemotiveWatcherScansBackwardPartitionsUntilCrossingWatermark(t *testing.T) {
	service := buildService(t)
	service.Config.RemotiveSitemapURLTemplate = "https://remotive.com/sitemap-job-postings-{partition}.xml"
	service.Config.RemotiveSitemapMaxIndex = 9
	service.Config.RemotiveSitemapMinIndex = 1
	service.Config.EnabledSources = map[string]struct{}{sourceRemotive: {}}
	if err := service.saveStatePayload(context.Background(), sourceRemotive, map[string]any{"latest_job_id": 105}); err != nil {
		t.Fatal(err)
	}
	sitemap9 := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-109</loc><lastmod>2026-02-01T00:00:00+00:00</lastmod></url>
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-110</loc><lastmod>2026-02-01T00:00:00+00:00</lastmod></url>
</urlset>`
	sitemap8 := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-104</loc><lastmod>2026-01-31T00:00:00+00:00</lastmod></url>
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-108</loc><lastmod>2026-01-31T00:00:00+00:00</lastmod></url>
</urlset>`
	fetchedPartitions := []int{}
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		if strings.HasSuffix(rawURL, "-9.xml") {
			return sitemap9, 200, nil
		}
		if strings.HasSuffix(rawURL, "-8.xml") {
			fetchedPartitions = append(fetchedPartitions, 8)
			return sitemap8, 200, nil
		}
		return "", 500, errors.New("unexpected URL: " + rawURL)
	}}

	if err := service.runOnceRemotive(context.Background()); err != nil {
		t.Fatal(err)
	}
	var body string
	if err := service.DB.SQL.QueryRowContext(context.Background(), `SELECT body_text FROM watcher_payloads WHERE source = ? ORDER BY id DESC LIMIT 1`, sourceRemotive).Scan(&body); err != nil {
		t.Fatal(err)
	}
	rows, skipped := remotive.ParseImportRows(body)
	if skipped != 0 {
		t.Fatalf("unexpected skipped rows: %d", skipped)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows got %d", len(rows))
	}
	gotIDs := []int{
		extractRemotiveJobIDFromURL(strings.TrimSpace(anyString(rows[0]["url"]))),
		extractRemotiveJobIDFromURL(strings.TrimSpace(anyString(rows[1]["url"]))),
		extractRemotiveJobIDFromURL(strings.TrimSpace(anyString(rows[2]["url"]))),
	}
	if strings.Join([]string{strconv.Itoa(gotIDs[0]), strconv.Itoa(gotIDs[1]), strconv.Itoa(gotIDs[2])}, ",") != "110,109,108" {
		t.Fatalf("unexpected ids %#v", gotIDs)
	}
	if len(fetchedPartitions) != 1 || fetchedPartitions[0] != 8 {
		t.Fatalf("unexpected fetched partitions %#v", fetchedPartitions)
	}
	state, err := service.loadStatePayload(context.Background(), sourceRemotive)
	if err != nil {
		t.Fatal(err)
	}
	rawIndexes, _ := state["latest_scanned_sitemap_indexes"].([]any)
	if len(rawIndexes) != 2 || intFromAny(rawIndexes[0], 0) != 9 || intFromAny(rawIndexes[1], 0) != 8 {
		t.Fatalf("unexpected latest_scanned_sitemap_indexes %#v", state["latest_scanned_sitemap_indexes"])
	}
}

func TestRemotiveWatcherUsesNowForDateOnlyLastmodWhenToday(t *testing.T) {
	service := buildService(t)
	service.Config.RemotiveSitemapURLTemplate = "https://remotive.com/sitemap-job-postings-{partition}.xml"
	service.Config.RemotiveSitemapMaxIndex = 10
	service.Config.RemotiveSitemapMinIndex = 1
	service.Config.EnabledSources = map[string]struct{}{sourceRemotive: {}}
	if err := service.saveStatePayload(context.Background(), sourceRemotive, map[string]any{"latest_job_id": 4_999_999}); err != nil {
		t.Fatal(err)
	}
	today := time.Now().UTC().Format("2006-01-02")
	yesterdayDate := time.Now().UTC().Add(-24 * time.Hour)
	yesterday := yesterdayDate.Format("2006-01-02")
	sitemap := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-5000001</loc><lastmod>` + today + `</lastmod></url>
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-5000000</loc><lastmod>` + yesterday + `</lastmod></url>
</urlset>`
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		if rawURL != "https://remotive.com/sitemap-job-postings-10.xml" {
			return "", 500, errors.New("unexpected URL: " + rawURL)
		}
		return sitemap, 200, nil
	}}

	beforeRun := time.Now().UTC()
	if err := service.runOnceRemotive(context.Background()); err != nil {
		t.Fatal(err)
	}
	afterRun := time.Now().UTC()

	var body string
	if err := service.DB.SQL.QueryRowContext(context.Background(), `SELECT body_text FROM watcher_payloads WHERE source = ? ORDER BY id DESC LIMIT 1`, sourceRemotive).Scan(&body); err != nil {
		t.Fatal(err)
	}
	rows, skipped := remotive.ParseImportRows(body)
	if skipped != 0 || len(rows) != 2 {
		t.Fatalf("unexpected rows len=%d skipped=%d", len(rows), skipped)
	}
	rowByURL := map[string]time.Time{}
	for _, row := range rows {
		rowURL, _ := row["url"].(string)
		postDate, _ := row["post_date"].(time.Time)
		rowByURL[rowURL] = postDate.UTC()
	}
	todayDT := rowByURL["https://remotive.com/remote-jobs/software-dev/job-5000001"]
	yesterdayDT := rowByURL["https://remotive.com/remote-jobs/software-dev/job-5000000"]

	if todayDT.Before(beforeRun) || todayDT.After(afterRun) {
		t.Fatalf("expected today date-only lastmod to map to now, got %s", todayDT.Format(time.RFC3339Nano))
	}
	expectedYesterday := time.Date(yesterdayDate.Year(), yesterdayDate.Month(), yesterdayDate.Day(), 0, 0, 0, 0, time.UTC)
	if !yesterdayDT.Equal(expectedYesterday) {
		t.Fatalf("expected yesterday midnight UTC %s got %s", expectedYesterday.Format(time.RFC3339Nano), yesterdayDT.Format(time.RFC3339Nano))
	}
}

func TestRemotiveWatcherUsesNowFallbackWhenLastmodInvalid(t *testing.T) {
	service := buildService(t)
	service.Config.RemotiveSitemapURLTemplate = "https://remotive.com/sitemap-job-postings-{partition}.xml"
	service.Config.RemotiveSitemapMaxIndex = 10
	service.Config.RemotiveSitemapMinIndex = 1
	service.Config.EnabledSources = map[string]struct{}{sourceRemotive: {}}
	if err := service.saveStatePayload(context.Background(), sourceRemotive, map[string]any{"latest_job_id": 5_999_999}); err != nil {
		t.Fatal(err)
	}
	sitemap := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://remotive.com/remote-jobs/software-dev/job-6000001</loc><lastmod>not-a-date</lastmod></url>
</urlset>`
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		if rawURL != "https://remotive.com/sitemap-job-postings-10.xml" {
			return "", 500, errors.New("unexpected URL: " + rawURL)
		}
		return sitemap, 200, nil
	}}

	beforeRun := time.Now().UTC()
	if err := service.runOnceRemotive(context.Background()); err != nil {
		t.Fatal(err)
	}
	afterRun := time.Now().UTC()

	var body string
	if err := service.DB.SQL.QueryRowContext(context.Background(), `SELECT body_text FROM watcher_payloads WHERE source = ? ORDER BY id DESC LIMIT 1`, sourceRemotive).Scan(&body); err != nil {
		t.Fatal(err)
	}
	rows, skipped := remotive.ParseImportRows(body)
	if skipped != 0 || len(rows) != 1 {
		t.Fatalf("unexpected rows len=%d skipped=%d", len(rows), skipped)
	}
	postDate, _ := rows[0]["post_date"].(time.Time)
	if postDate.Before(beforeRun) || postDate.After(afterRun) {
		t.Fatalf("expected invalid lastmod to fallback to now, got %s", postDate.Format(time.RFC3339Nano))
	}
}

func builtinPageHTML(jobURL string, jobID int, publishedDate string) string {
	return `<html><head><script type="application/ld+json">{"@graph":[{"@type":"ItemList","itemListElement":[{"@type":"ListItem","position":1,"url":"` + jobURL + `","name":"Role","description":"Desc"}]}]}</script></head><body><script>logBuiltinTrackEvent('job_board_view', {'jobs':[{'id':` + strconv.Itoa(jobID) + `,'published_date':'` + publishedDate + `'}],'filters':{}});</script></body></html>`
}

func anyString(value any) string {
	text, _ := value.(string)
	return text
}

func TestHiringCafeWatcherUpsertsJobsWithoutImporter(t *testing.T) {
	service := buildService(t)
	service.Config.HiringCafeSearchAPIURL = "https://hiring.cafe/api/search-jobs?s=abc"
	service.Config.HiringCafeTotalCountURL = "https://hiring.cafe/api/search-jobs/get-total-count?s=abc"
	service.Config.HiringCafePageSize = 1
	service.Config.EnabledSources = map[string]struct{}{sourceHiringCafe: {}}
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		switch rawURL {
		case "https://hiring.cafe/api/search-jobs/get-total-count?s=abc":
			return `{"total":1}`, 200, nil
		case "https://hiring.cafe/api/search-jobs?page=0&s=abc&size=1":
			return `{"results":[{"requisition_id":"abc123","v5_processed_job_data":{"estimated_publish_date":"2026-02-20T20:14:34Z","job_title_raw":"Software Engineer","commitment":["Full Time"]},"apply_url":"https://hiring.cafe/viewjob/abc123"}]}`, 200, nil
		default:
			return "", 500, errors.New("unexpected URL: " + rawURL)
		}
	}}

	if err := service.RunOnce(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := service.DB.SQL.QueryRowContext(context.Background(), `SELECT COUNT(1) FROM raw_us_jobs WHERE source = ?`, sourceHiringCafe).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 hiringcafe raw row, got %d", count)
	}
}

func TestWorkableWatcherUpsertsJobsWithoutImporter(t *testing.T) {
	service := buildService(t)
	service.Config.WorkableAPIURL = "https://jobs.workable.com/api/v1/jobs?location=United%20States&workplace=remote&day_range=1"
	service.Config.WorkablePageLimit = 100
	service.Config.EnabledSources = map[string]struct{}{sourceWorkable: {}}
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		if !strings.Contains(rawURL, "jobs.workable.com/api/v1/jobs") {
			return "", 500, errors.New("unexpected URL: " + rawURL)
		}
		return `{"jobs":[{"id":"w1","url":"https://jobs.workable.com/view/abc123","title":"Software Engineer","created":"2026-02-20T20:14:34Z","updated":"2026-02-20T20:14:34Z","employmentType":"full-time","workplace":"remote","language":"en","description":"Role","requirementsSection":"Req","benefitsSection":"Ben","socialSharingDescription":"Summary","company":{"id":"c1","title":"Acme","website":"https://acme.example","image":"https://acme.example/logo.png"},"location":{"city":"New York","subregion":"New York","countryName":"United States"},"locations":["New York, United States"]}]}`, 200, nil
	}}

	if err := service.RunOnce(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := service.DB.SQL.QueryRowContext(context.Background(), `SELECT COUNT(1) FROM raw_us_jobs WHERE source = ?`, sourceWorkable).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 workable raw row, got %d", count)
	}

	var payloadCount int
	if err := service.DB.SQL.QueryRowContext(context.Background(), `SELECT COUNT(1) FROM watcher_payloads WHERE source = ?`, sourceWorkable).Scan(&payloadCount); err != nil {
		t.Fatal(err)
	}
	if payloadCount != 0 {
		t.Fatalf("expected 0 workable watcher payload rows, got %d", payloadCount)
	}
}

func TestDailyremoteWatcherCreatesDeltaPayload(t *testing.T) {
	service := buildService(t)
	service.Config.DailyRemoteBaseURL = "https://dailyremote.com/?page={page}"
	service.Config.DailyRemoteMaxPage = 2
	service.Config.DailyRemotePagesPerCycle = 2
	service.Config.EnabledSources = map[string]struct{}{sourceDailyremote: {}}
	service.Fetcher = stubFetcher{fn: func(ctx context.Context, rawURL string) (string, int, error) {
		if strings.Contains(rawURL, "page=1") {
			return `<article class="card js-card"><h2 class="job-position"><a href="/remote-job/backend-engineer-1001">Backend Engineer</a></h2><span>1 hour ago</span></article>`, 200, nil
		}
		return "", 200, nil
	}}
	if err := service.saveStatePayload(context.Background(), sourceDailyremote, map[string]any{"latest_external_id": 1000}); err != nil {
		t.Fatal(err)
	}

	if err := service.RunOnce(); err != nil {
		t.Fatal(err)
	}

	var payloadType string
	if err := service.DB.SQL.QueryRowContext(context.Background(), `SELECT payload_type FROM watcher_payloads WHERE source = ? ORDER BY id DESC LIMIT 1`, sourceDailyremote).Scan(&payloadType); err != nil {
		t.Fatal(err)
	}
	if payloadType != "delta_dailyremote_json" {
		t.Fatalf("expected delta_dailyremote_json payload type, got %s", payloadType)
	}
}
