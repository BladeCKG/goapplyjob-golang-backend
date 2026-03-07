package watcher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func buildService(t *testing.T) *Service {
	t.Helper()
	dir := t.TempDir()
	return New(Config{
		Enabled:         true,
		URL:             "https://example.com/jobs.xml",
		IntervalMinutes: 1,
		SampleKB:        8,
		TimeoutSeconds:  30,
		StateFile:       filepath.Join(dir, "watcher_state.json"),
		OutputDir:       filepath.Join(dir, "watcher_output"),
	})
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
	if err := os.WriteFile(service.Config.StateFile, []byte(`{"sample_hash":"old-hash","first_lastmod":"2026-02-12T10:00:00+00:00"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := service.RunOnce(); err != nil {
		t.Fatal(err)
	}
	status := service.Status()
	if status["last_delta_file"] != nil {
		t.Fatalf("expected nil delta file, got %#v", status["last_delta_file"])
	}
	if status["last_delta_size"].(int) != 0 {
		t.Fatalf("expected zero delta size, got %#v", status["last_delta_size"])
	}
}
