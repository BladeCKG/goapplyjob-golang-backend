package cloudscraper

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/thirdparty/cloudscraper/lib/errors"
)

const (
	// modernChallengeSubmitPath is the standard submission URL path for modern Cloudflare challenges
	// when the form element is not present in the HTML
	modernChallengeSubmitPath = "/cdn-cgi/challenge-platform/h/b/orchestrate/jsch/v1"
)

var (
	jsV1DetectRegex    = regexp.MustCompile(`(?i)cdn-cgi/images/trace/jsch/`)
	jsV2DetectRegex    = regexp.MustCompile(`(?i)/cdn-cgi/challenge-platform/`)
	captchaDetectRegex = regexp.MustCompile(`data-sitekey="([^\"]+)"`)
	challengeFormRegex = regexp.MustCompile(`<form class="challenge-form" id="challenge-form" action="(.+?)" method="POST">`)
	jschlVcRegex       = regexp.MustCompile(`name="jschl_vc" value="(\w+)"`)
	passRegex          = regexp.MustCompile(`name="pass" value="(.+?)"`)
	rValueOldRegex     = regexp.MustCompile(`name="r" value="([^"]+)"`)
	rValueModernRegex  = regexp.MustCompile(`r:\s*'([^']+)'`)
)

func (s *Scraper) handleChallenge(resp *http.Response, body []byte) (*http.Response, error) {
	bodyStr := string(body)

	// Check for modern v2/v3 JS VM challenge first
	if jsV2DetectRegex.MatchString(bodyStr) {
		s.logger.Printf("Modern (v2/v3) JavaScript challenge detected. Solving with '%s'...\n", s.opts.JSRuntime)
		return s.solveModernJSChallenge(resp, bodyStr)
	}

	// Check for classic lib JS challenge
	if jsV1DetectRegex.MatchString(bodyStr) {
		s.logger.Printf("Classic (v1) JavaScript challenge detected. Solving with '%s'...\n", s.opts.JSRuntime)
		return s.solveClassicJSChallenge(resp.Request.Context(), resp.Request.URL, bodyStr)
	}

	// Check for Captcha/Turnstile
	if siteKeyMatch := captchaDetectRegex.FindStringSubmatch(bodyStr); len(siteKeyMatch) > 1 {
		s.logger.Println("Captcha/Turnstile challenge detected...")
		return s.solveCaptchaChallenge(resp, bodyStr, siteKeyMatch[1])
	}

	return nil, errors.ErrUnknownChallenge
}

func (s *Scraper) solveClassicJSChallenge(ctx context.Context, originalURL *url.URL, body string) (*http.Response, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(4 * time.Second):
	}

	answer, err := solveV1Logic(ctx, body, originalURL.Host, s.jsEngine)
	if err != nil {
		return nil, fmt.Errorf("v1 challenge solver failed: %w", err)
	}

	formMatch := challengeFormRegex.FindStringSubmatch(body)
	if len(formMatch) < 2 {
		return nil, fmt.Errorf("v1: could not find challenge form")
	}
	vcMatch := jschlVcRegex.FindStringSubmatch(body)
	if len(vcMatch) < 2 {
		return nil, fmt.Errorf("v1: could not find jschl_vc")
	}
	passMatch := passRegex.FindStringSubmatch(body)
	if len(passMatch) < 2 {
		return nil, fmt.Errorf("v1: could not find pass")
	}

	fullSubmitURL, _ := originalURL.Parse(formMatch[1])
	formData := url.Values{
		"r":            {s.extractRValue(body)},
		"jschl_vc":     {vcMatch[1]},
		"pass":         {passMatch[1]},
		"jschl_answer": {answer},
	}

	return s.submitChallengeForm(ctx, fullSubmitURL.String(), originalURL.String(), formData)
}

func (s *Scraper) solveModernJSChallenge(resp *http.Response, body string) (*http.Response, error) {
	s.logger.Printf("cloudscraper: solveModernJSChallenge start url=%q host=%q body_len=%d", resp.Request.URL.String(), resp.Request.URL.Host, len(body))
	answer, err := solveV2Logic(resp.Request.Context(), body, resp.Request.URL.Host, s.jsEngine, s.logger)
	if err != nil {
		s.logger.Printf("cloudscraper: solveModernJSChallenge solveV2Logic failed url=%q host=%q err=%v", resp.Request.URL.String(), resp.Request.URL.Host, err)
		return nil, fmt.Errorf("v2 challenge solver failed: %w", err)
	}
	s.logger.Printf("cloudscraper: solveModernJSChallenge answer_ready url=%q host=%q answer_len=%d", resp.Request.URL.String(), resp.Request.URL.Host, len(answer))

	// Try to find the challenge form (old style)
	formMatch := challengeFormRegex.FindStringSubmatch(body)
	var submitURL string

	if len(formMatch) >= 2 {
		// Old style: form exists in HTML
		fullSubmitURL, err := resp.Request.URL.Parse(formMatch[1])
		if err != nil {
			// Log the error but continue - we'll try the fallback URL construction
			s.logger.Printf("Warning: failed to parse form action URL %q: %v", formMatch[1], err)
			submitURL = s.buildModernSubmitURL(resp.Request.URL)
		} else {
			submitURL = fullSubmitURL.String()
		}
	} else {
		// New style: form is created dynamically by JavaScript
		// Use the standard modern challenge submission URL pattern
		submitURL = s.buildModernSubmitURL(resp.Request.URL)
	}
	s.logger.Printf("cloudscraper: solveModernJSChallenge submit_prepared url=%q submit_url=%q has_form=%t jschl_vc_present=%t pass_present=%t", resp.Request.URL.String(), submitURL, len(formMatch) >= 2, len(jschlVcRegex.FindStringSubmatch(body)) >= 2, len(passRegex.FindStringSubmatch(body)) >= 2)

	// Extract optional fields that may not be present in modern challenges
	var jschlVc, pass string

	vcMatch := jschlVcRegex.FindStringSubmatch(body)
	if len(vcMatch) >= 2 {
		jschlVc = vcMatch[1]
	}

	passMatch := passRegex.FindStringSubmatch(body)
	if len(passMatch) >= 2 {
		pass = passMatch[1]
	}

	formData := url.Values{
		"r":            {s.extractRValue(body)},
		"jschl_vc":     {jschlVc},
		"pass":         {pass},
		"jschl_answer": {answer},
	}

	return s.submitChallengeForm(resp.Request.Context(), submitURL, resp.Request.URL.String(), formData)
}

func (s *Scraper) solveCaptchaChallenge(resp *http.Response, body, siteKey string) (*http.Response, error) {
	if s.CaptchaSolver == nil {
		return nil, errors.ErrNoCaptchaSolver
	}

	token, err := s.CaptchaSolver.Solve(resp.Request.Context(), "turnstile", resp.Request.URL.String(), siteKey)
	if err != nil {
		return nil, fmt.Errorf("captcha solver failed: %w", err)
	}

	// Note: Captcha challenges currently require a form element in the HTML.
	// If modern captcha challenges also use dynamic forms, this will need the same
	// flexible detection logic as solveModernJSChallenge (see issue #2 fix).
	formMatch := challengeFormRegex.FindStringSubmatch(body)
	if len(formMatch) < 2 {
		return nil, fmt.Errorf("captcha: could not find challenge form")
	}
	submitURL, err := resp.Request.URL.Parse(formMatch[1])
	if err != nil {
		s.logger.Printf("Warning: failed to parse captcha form action URL %q: %v", formMatch[1], err)
		return nil, fmt.Errorf("captcha: invalid form action URL: %w", err)
	}

	formData := url.Values{
		"r":                     {s.extractRValue(body)},
		"cf-turnstile-response": {token},
		"g-recaptcha-response":  {token},
	}

	return s.submitChallengeForm(resp.Request.Context(), submitURL.String(), resp.Request.URL.String(), formData)
}

func (s *Scraper) submitChallengeForm(ctx context.Context, submitURL, refererURL string, formData url.Values) (*http.Response, error) {
	req, _ := http.NewRequestWithContext(ctx, "POST", submitURL, strings.NewReader(formData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", refererURL)

	// Use the main `do` method to ensure all headers and logic are applied
	return s.do(req)
}

func (s *Scraper) extractRValue(body string) string {
	// First try the old format: name="r" value="..."
	rValMatch := rValueOldRegex.FindStringSubmatch(body)
	if len(rValMatch) > 1 {
		return rValMatch[1]
	}

	// Try the modern format: r:'...' in __CF$cv$params
	rParamsMatch := rValueModernRegex.FindStringSubmatch(body)
	if len(rParamsMatch) > 1 {
		return rParamsMatch[1]
	}

	return ""
}

func (s *Scraper) buildModernSubmitURL(originalURL *url.URL) string {
	// Modern Cloudflare challenges use a standard submission URL pattern
	// when the form is not present in the HTML.
	submitURL, err := originalURL.Parse(modernChallengeSubmitPath)
	if err != nil {
		// This should rarely happen with a relative path, but log it for debugging
		s.logger.Printf("Warning: failed to parse modern challenge submit URL path %q: %v", modernChallengeSubmitPath, err)
		// Fall back to constructing the URL manually
		return originalURL.Scheme + "://" + originalURL.Host + modernChallengeSubmitPath
	}
	return submitURL.String()
}

func isChallengeResponse(resp *http.Response, body []byte) bool {
	if !strings.HasPrefix(resp.Header.Get("Server"), "cloudflare") {
		return false
	}
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusForbidden {
		return false
	}

	bodyStr := string(body)
	return jsV1DetectRegex.MatchString(bodyStr) || jsV2DetectRegex.MatchString(bodyStr) || captchaDetectRegex.MatchString(bodyStr)
}
