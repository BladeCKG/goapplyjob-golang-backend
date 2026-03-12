package scraper

import (
	"errors"
	"io"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

type TLSClientFetcher struct {
	client    tls_client.HttpClient
	userAgent string
	timeout   time.Duration
}

type TLSClientConfig struct {
	Timeout   time.Duration
	UserAgent string
}

func NewTLSClientFetcher(cfg TLSClientConfig) (*TLSClientFetcher, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
	}

	options := []tls_client.HttpClientOption{
		tls_client.WithTimeout(int(cfg.Timeout.Seconds())),
		tls_client.WithClientProfile(profiles.Chrome_120),
		tls_client.WithNotFollowRedirects(),
		tls_client.WithCookieJar(tls_client.NewCookieJar()),
	}
	client, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
	if err != nil {
		return nil, err
	}
	return &TLSClientFetcher{
		client:    client,
		userAgent: cfg.UserAgent,
		timeout:   cfg.Timeout,
	}, nil
}

func (f *TLSClientFetcher) Close() error {
	return nil
}

func (f *TLSClientFetcher) ReadHTML(targetURL string) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("tls-client fetcher is nil")
	}
	req, err := fhttp.NewRequest("GET", targetURL, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("User-Agent", f.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")

	resp, err := f.client.Do(req)
	if err != nil {
		return "", -1, nil
	}
	if resp.Body == nil {
		return "", -1, nil
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", -1, nil
	}
	return string(body), resp.StatusCode, nil
}

func (f *TLSClientFetcher) ReadHTMLWith429Retry(targetURL string, max429Retries int, retryDelay time.Duration) (string, int, error) {
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
