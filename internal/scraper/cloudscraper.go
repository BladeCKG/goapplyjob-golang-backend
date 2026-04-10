package scraper

import (
	"context"
	"errors"
	"goapplyjob-golang-backend/internal/thirdparty/cloudscraper/lib/stealth"
	"io"
	"time"

	cloudscraper "goapplyjob-golang-backend/internal/thirdparty/cloudscraper/lib"

	useragent "goapplyjob-golang-backend/internal/thirdparty/cloudscraper/lib/user_agent"
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
		cloudscraper.WithStealth(stealth.Options{
			Enabled:          true,
			HumanLikeDelays:  true,
			RandomizeHeaders: true,
			BrowserQuirks:    true,
		}),
		// Keep retries bounded while allowing more recovery attempts on
		// throttled/blocked sessions.
		cloudscraper.WithSessionConfig(true, time.Hour, 4),
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
	if f == nil || f.client == nil {
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

	resp, err := f.client.GetWithContext(ctx, targetURL)
	if err != nil {
		return "", -1, err
	}
	if resp.Body == nil {
		return "", -1, nil
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if readErr != nil {
		return "", -1, readErr
	}
	if err := ctx.Err(); err != nil {
		return "", -1, err
	}
	return string(body), resp.StatusCode, nil
}

func (f *CloudscraperFetcher) ResolveFinalURL(ctx context.Context, targetURL string) (string, int, error) {
	if f == nil || f.client == nil {
		return "", 0, errors.New("cloudscraper fetcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", -1, err
	}

	resp, err := f.client.GetWithContext(ctx, targetURL)
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
