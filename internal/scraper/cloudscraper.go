package scraper

import (
	"errors"
	"io"
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

func (f *CloudscraperFetcher) ReadHTML(targetURL string) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("cloudscraper fetcher is nil")
	}
	resp, err := f.client.Get(targetURL)
	if err != nil {
		return "", -1, nil
	}
	if resp.Body == nil {
		return "", -1, nil
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if readErr != nil {
		return "", -1, nil
	}
	return string(body), resp.StatusCode, nil
}

func (f *CloudscraperFetcher) ResolveFinalURL(targetURL string) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("cloudscraper fetcher is nil")
	}
	resp, err := f.client.Get(targetURL)
	if err != nil {
		return "", -1, nil
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
	return finalURL, resp.StatusCode, nil
}

func (f *CloudscraperFetcher) ReadHTMLWith429Retry(targetURL string, max429Retries int, retryDelay time.Duration) (string, int, error) {
	attempt := 0
	for {
		html, status, err := f.ReadHTML(targetURL)
		if err != nil {
			return "", 0, err
		}
		if status != 429 || attempt >= max429Retries {
			return html, status, nil
		}
		attempt++
		if retryDelay > 0 {
			time.Sleep(retryDelay)
		}
	}
}
