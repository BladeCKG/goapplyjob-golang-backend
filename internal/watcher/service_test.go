package watcher

import (
	"context"
	"strings"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/database"
)

func buildService(t *testing.T) *Service {
	t.Helper()
	db, err := database.Open("file:watcher_test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return New(Config{
		Enabled:         true,
		URL:             "https://example.com/jobs.xml",
		IntervalMinutes: 1,
		SampleKB:        8,
		TimeoutSeconds:  30,
	}, db)
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

	service.FetchSample = func() ([]byte, error) { return sample, nil }
	service.FetchFull = func() ([]byte, error) { return fullData, nil }
	if _, err := service.DB.SQL.ExecContext(context.Background(), `INSERT INTO watcher_states (source_url, sample_hash, first_lastmod, updated_at) VALUES (?, ?, ?, ?)`, service.Config.URL, "old-hash", "2026-02-12T10:00:00+00:00", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	if err := service.RunOnce(); err != nil {
		t.Fatal(err)
	}
	status := service.Status()
	if status["last_delta_payload_id"] != nil {
		t.Fatalf("expected nil delta payload id, got %#v", status["last_delta_payload_id"])
	}
	if status["last_delta_size"].(int) != 0 {
		t.Fatalf("expected zero delta size, got %#v", status["last_delta_size"])
	}
}
