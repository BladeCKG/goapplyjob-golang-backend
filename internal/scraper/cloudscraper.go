package scraper

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	cloudscraper "github.com/Advik-B/cloudscraper/lib"
	"github.com/Advik-B/cloudscraper/lib/stealth"
	useragent "github.com/Advik-B/cloudscraper/lib/user_agent"
)

type CloudscraperFetcher struct {
	client  *cloudscraper.Scraper
	timeout time.Duration
}

type CloudscraperConfig struct {
	Timeout time.Duration
}

func NewCloudscraperFetcher(cfg CloudscraperConfig) (*CloudscraperFetcher, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}

	client, err := cloudscraper.New(
		cloudscraper.WithBrowser(useragent.Config{
			Browser: "chrome",
			Desktop: true,
			Mobile:  false,
		}),
		// Keep challenge handling lightweight for worker fetches. The upstream
		// library can otherwise spend multiple 4s sleeps and 403 refresh retries
		// inside a single Get() call, which makes raw-job workers look stuck for
		// minutes even though our outer fetch context has already expired.
		cloudscraper.WithStealth(stealth.Options{
			Enabled:          true,
			HumanLikeDelays:  false,
			RandomizeHeaders: true,
			BrowserQuirks:    true,
		}),
		cloudscraper.WithSessionConfig(false, time.Hour, 0),
		func(o *cloudscraper.Options) {
			o.MaxRetries = 1
		},
	)
	if err != nil {
		return nil, err
	}

	return &CloudscraperFetcher{
		client:  client,
		timeout: cfg.Timeout,
	}, nil
}

func (f *CloudscraperFetcher) Close() error {
	return nil
}

func (f *CloudscraperFetcher) ReadHTML(ctx context.Context, targetURL string) (string, int, error) {
	return f.ReadHTMLWithLimit(ctx, targetURL, 0)
}

func (f *CloudscraperFetcher) ReadHTMLWithLimit(ctx context.Context, targetURL string, maxBytes int64) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("cloudscraper fetcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if maxBytes <= 0 {
		maxBytes = 5 * 1024 * 1024
	}
	if err := ctx.Err(); err != nil {
		return "", -1, err
	}
	resp, err := f.client.Get(targetURL)
	if err != nil {
		return "", -1, err
	}
	if resp.Body == nil {
		return "", -1, nil
	}
	defer resp.Body.Close()
	rawBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if readErr != nil {
		return "", -1, readErr
	}
	decoded, decodeErr := decodeCompressedBody(resp.Header.Get("Content-Encoding"), rawBody, maxBytes)
	if decodeErr != nil {
		return "", -1, decodeErr
	}
	if err := ctx.Err(); err != nil {
		return "", -1, err
	}
	return string(decoded), resp.StatusCode, nil
}

func decodeCompressedBody(encoding string, raw []byte, maxBytes int64) ([]byte, error) {
	normalized := strings.ToLower(strings.TrimSpace(encoding))
	switch {
	case strings.Contains(normalized, "gzip"):
		return decodeGzip(raw, maxBytes)
	case strings.Contains(normalized, "deflate"):
		return decodeZlib(raw, maxBytes)
	}
	if len(raw) >= 2 && raw[0] == 0x1f && raw[1] == 0x8b {
		return decodeGzip(raw, maxBytes)
	}
	if len(raw) >= 2 && raw[0] == 0x78 {
		return decodeZlib(raw, maxBytes)
	}
	return raw, nil
}

func decodeGzip(raw []byte, maxBytes int64) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	if maxBytes <= 0 {
		return io.ReadAll(reader)
	}
	return io.ReadAll(io.LimitReader(reader, maxBytes))
}

func decodeZlib(raw []byte, maxBytes int64) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	if maxBytes <= 0 {
		return io.ReadAll(reader)
	}
	return io.ReadAll(io.LimitReader(reader, maxBytes))
}

func (f *CloudscraperFetcher) ResolveFinalURL(ctx context.Context, targetURL string) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("cloudscraper fetcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", -1, err
	}
	resp, err := f.client.Get(targetURL)
	if err != nil {
		return "", -1, err
	}
	if resp.Body == nil {
		return "", -1, nil
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	finalURL := targetURL
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}
	if err := ctx.Err(); err != nil {
		return "", -1, err
	}
	return finalURL, resp.StatusCode, nil
}

func (f *CloudscraperFetcher) ReadHTMLWith429Retry(ctx context.Context, targetURL string, max429Retries int, retryDelay time.Duration) (string, int, error) {
	attempt := 0
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return "", -1, err
			}
		}
		html, status, err := f.ReadHTML(ctx, targetURL)
		if err != nil {
			return "", 0, err
		}
		if status != 429 || attempt >= max429Retries {
			return html, status, nil
		}
		attempt++
		if retryDelay > 0 {
			select {
			case <-ctx.Done():
				return "", -1, ctx.Err()
			case <-time.After(retryDelay):
			}
		}
	}
}
