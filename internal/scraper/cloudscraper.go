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

	type result struct {
		body   string
		status int
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := f.client.Get(targetURL)
		if err != nil {
			ch <- result{body: "", status: -1, err: err}
			return
		}
		if resp.Body == nil {
			ch <- result{body: "", status: -1, err: nil}
			return
		}
		defer resp.Body.Close()
		rawBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
		if readErr != nil {
			ch <- result{body: "", status: -1, err: readErr}
			return
		}
		decoded, decodeErr := decodeCompressedBody(resp.Header.Get("Content-Encoding"), rawBody, maxBytes)
		if decodeErr != nil {
			ch <- result{body: "", status: -1, err: decodeErr}
			return
		}
		ch <- result{body: string(decoded), status: resp.StatusCode, err: nil}
	}()

	select {
	case <-ctx.Done():
		return "", -1, ctx.Err()
	case res := <-ch:
		return res.body, res.status, res.err
	}
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

	type result struct {
		url    string
		status int
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := f.client.Get(targetURL)
		if err != nil {
			ch <- result{url: "", status: -1, err: err}
			return
		}
		if resp.Body == nil {
			ch <- result{url: "", status: -1, err: nil}
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		finalURL := targetURL
		if resp.Request != nil && resp.Request.URL != nil {
			finalURL = resp.Request.URL.String()
		}
		ch <- result{url: finalURL, status: resp.StatusCode, err: nil}
	}()

	select {
	case <-ctx.Done():
		return "", -1, ctx.Err()
	case res := <-ch:
		return res.url, res.status, res.err
	}
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
