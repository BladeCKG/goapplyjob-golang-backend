package scraper

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

var UserAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}

var ProfilesPool = []profiles.ClientProfile{
	profiles.Chrome_120,
	profiles.Firefox_117,
	profiles.Safari_16_0,
}

type TLSClientFetcher struct {
	client  tls_client.HttpClient
	timeout time.Duration
}

type TLSClientConfig struct {
	Timeout time.Duration
}

func NewTLSClientFetcher(cfg TLSClientConfig) (*TLSClientFetcher, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}

	options := []tls_client.HttpClientOption{
		tls_client.WithTimeout(int(cfg.Timeout.Seconds())),
		tls_client.WithClientProfile(ProfilesPool[rand.Intn(len(ProfilesPool))]),
		tls_client.WithCookieJar(tls_client.NewCookieJar()),
	}
	client, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
	if err != nil {
		return nil, err
	}
	return &TLSClientFetcher{
		client:  client,
		timeout: cfg.Timeout,
	}, nil
}

func (f *TLSClientFetcher) Close() error {
	return nil
}

func (f *TLSClientFetcher) ReadHTML(ctx context.Context, targetURL string) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("tls-client fetcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		body   string
		status int
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		req, err := fhttp.NewRequest("GET", targetURL, nil)
		if err != nil {
			ch <- result{body: "", status: 0, err: err}
			return
		}
		req.Header = fhttp.Header{
			"User-Agent":                {UserAgents[rand.Intn(len(UserAgents))]},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"},
			"Accept-Language":           {"en-US,en;q=0.9"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Upgrade-Insecure-Requests": {"1"},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Referer":                   {"https://www.google.com/"},
		}

		resp, err := f.client.Do(req)
		if err != nil {
			ch <- result{body: "", status: -1, err: err}
			return
		}
		if resp.Body == nil {
			ch <- result{body: "", status: -1, err: nil}
			return
		}
		defer resp.Body.Close()
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			ch <- result{body: "", status: -1, err: readErr}
			return
		}
		ch <- result{body: string(body), status: resp.StatusCode, err: nil}
	}()
	select {
	case <-ctx.Done():
		return "", -1, ctx.Err()
	case res := <-ch:
		return res.body, res.status, res.err
	}
}

func (f *TLSClientFetcher) ReadHTMLWithHeaders(ctx context.Context, targetURL string, headers map[string]string) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("tls-client fetcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		body   string
		status int
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		req, err := fhttp.NewRequest("GET", targetURL, nil)
		if err != nil {
			ch <- result{body: "", status: 0, err: err}
			return
		}
		req.Header = fhttp.Header{
			"User-Agent":                {UserAgents[rand.Intn(len(UserAgents))]},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"},
			"Accept-Language":           {"en-US,en;q=0.9"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Upgrade-Insecure-Requests": {"1"},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Referer":                   {"https://www.google.com/"},
		}
		for key, value := range headers {
			if strings.TrimSpace(key) == "" {
				continue
			}
			req.Header.Set(key, value)
		}

		resp, err := f.client.Do(req)
		if err != nil {
			ch <- result{body: "", status: -1, err: err}
			return
		}
		if resp.Body == nil {
			ch <- result{body: "", status: -1, err: nil}
			return
		}
		defer resp.Body.Close()
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			ch <- result{body: "", status: -1, err: readErr}
			return
		}
		ch <- result{body: string(body), status: resp.StatusCode, err: nil}
	}()
	select {
	case <-ctx.Done():
		return "", -1, ctx.Err()
	case res := <-ch:
		return res.body, res.status, res.err
	}
}

func (f *TLSClientFetcher) ResolveFinalURL(ctx context.Context, targetURL string) (string, int, error) {
	if f == nil {
		return "", 0, errors.New("tls-client fetcher is nil")
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
		req, err := fhttp.NewRequest("GET", targetURL, nil)
		if err != nil {
			ch <- result{url: "", status: 0, err: err}
			return
		}
		req.Header = fhttp.Header{
			"User-Agent":                {UserAgents[rand.Intn(len(UserAgents))]},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"},
			"Accept-Language":           {"en-US,en;q=0.9"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Upgrade-Insecure-Requests": {"1"},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Referer":                   {"https://www.google.com/"},
		}

		resp, err := f.client.Do(req)
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

func (f *TLSClientFetcher) ReadHTMLWith429Retry(ctx context.Context, targetURL string, max429Retries int, retryDelay time.Duration) (string, int, error) {
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
