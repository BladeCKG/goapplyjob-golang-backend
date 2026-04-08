package scraper

import (
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCloudscraperFetcherReadHTMLDecodesGzip(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		_, _ = gz.Write([]byte(`{"jobs":[{"id":"w1","url":"https://jobs.workable.com/view/abc123"}]}`))
	}))
	defer server.Close()

	fetcher, err := NewCloudscraperFetcher(CloudscraperConfig{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("new fetcher: %v", err)
	}

	body, status, err := fetcher.ReadHTML(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("read html: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	expected := `{"jobs":[{"id":"w1","url":"https://jobs.workable.com/view/abc123"}]}`
	if body != expected {
		t.Fatalf("expected decoded body %q, got %q", expected, body)
	}
}
