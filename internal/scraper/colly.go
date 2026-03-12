package scraper

import (
	"errors"
	"time"

	"github.com/gocolly/colly/v2"
)

type CollyFetcher struct {
	userAgent string
	timeout   time.Duration
}

type CollyConfig struct {
	Timeout   time.Duration
	UserAgent string
}

func NewCollyFetcher(cfg CollyConfig) (*CollyFetcher, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
	}
	return &CollyFetcher{
		userAgent: cfg.UserAgent,
		timeout:   cfg.Timeout,
	}, nil
}

func (f *CollyFetcher) Close() error {
	return nil
}

func (f *CollyFetcher) ReadHTML(targetURL string) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("colly fetcher is nil")
	}
	collector := colly.NewCollector(
		colly.UserAgent(f.userAgent),
		colly.MaxDepth(1),
	)
	collector.SetRequestTimeout(f.timeout)

	var (
		body       string
		statusCode int
		reqErr     error
	)

	collector.OnResponse(func(resp *colly.Response) {
		statusCode = resp.StatusCode
		body = string(resp.Body)
	})
	collector.OnError(func(resp *colly.Response, err error) {
		reqErr = err
		if resp != nil {
			statusCode = resp.StatusCode
			body = string(resp.Body)
		}
	})

	if err := collector.Visit(targetURL); err != nil {
		return "", -1, nil
	}
	if reqErr != nil {
		return "", -1, nil
	}
	return body, statusCode, nil
}

func (f *CollyFetcher) ReadHTMLWith429Retry(targetURL string, max429Retries int, retryDelay time.Duration) (string, int, error) {
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
