package scraper

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const defaultSimpleHTTPMaxHTMLBytes int64 = 3 * 1024 * 1024

type SimpleHTTPFetcher struct {
	client  *http.Client
	timeout time.Duration
}

type SimpleHTTPConfig struct {
	Timeout time.Duration
}

func NewSimpleHTTPFetcher(cfg SimpleHTTPConfig) *SimpleHTTPFetcher {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 20 * time.Second
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          50,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: cfg.Timeout,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &SimpleHTTPFetcher{
		client: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: transport,
		},
		timeout: cfg.Timeout,
	}
}

func (f *SimpleHTTPFetcher) ReadHTML(ctx context.Context, targetURL string) (string, int, error) {
	return f.ReadHTMLWithHeaders(ctx, targetURL, nil)
}

func (f *SimpleHTTPFetcher) ReadHTMLWithHeaders(ctx context.Context, targetURL string, headers map[string]string) (string, int, error) {
	if f == nil || f.client == nil {
		return "", 0, errors.New("simple http fetcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("User-Agent", UserAgents[rand.Intn(len(UserAgents))])
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Referer", "https://www.google.com/")
	for key, value := range headers {
		if strings.TrimSpace(key) == "" {
			continue
		}
		req.Header.Set(key, value)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return "", -1, err
	}
	if resp.Body == nil {
		return "", -1, nil
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, defaultSimpleHTTPMaxHTMLBytes))
	if readErr != nil {
		return "", -1, readErr
	}
	return string(body), resp.StatusCode, nil
}

func (f *SimpleHTTPFetcher) ReadHTMLWith429Retry(ctx context.Context, targetURL string, max429Retries int, retryDelay time.Duration) (string, int, error) {
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
