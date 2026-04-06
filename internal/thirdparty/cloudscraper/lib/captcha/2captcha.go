package captcha

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// TwoCaptchaSolver implements the Solver interface for 2captcha.com.
type TwoCaptchaSolver struct {
	APIKey string
	Client *http.Client
}

type twoCaptchaRequest struct {
	Status  int    `json:"status"`
	Request string `json:"request"`
}

// NewTwoCaptchaSolver creates a new 2captcha solver.
func NewTwoCaptchaSolver(apiKey string) *TwoCaptchaSolver {
	return &TwoCaptchaSolver{
		APIKey: apiKey,
		Client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Solve sends a captcha to 2captcha and polls for the result.
func (s *TwoCaptchaSolver) Solve(captchaType, pageURL, siteKey string) (string, error) {
	// Map cloudscraper types to 2captcha method names
	method := ""
	switch captchaType {
	case "reCaptcha":
		method = "userrecaptcha"
	case "hCaptcha":
		method = "hcaptcha"
	case "turnstile":
		method = "turnstile"
	default:
		return "", fmt.Errorf("2captcha: unsupported captcha type %s", captchaType)
	}

	// 1. Submit the captcha solving job
	form := url.Values{}
	form.Add("key", s.APIKey)
	form.Add("method", method)
	form.Add("googlekey", siteKey) // sitekey for hcaptcha/turnstile also uses this param
	form.Add("pageurl", pageURL)
	form.Add("json", "1")

	resp, err := s.Client.PostForm("https://2captcha.com/in.php", form)
	if err != nil {
		return "", fmt.Errorf("2captcha: failed to submit job: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var req twoCaptchaRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", fmt.Errorf("2captcha: failed to parse submission response: %s", string(body))
	}

	if req.Status != 1 {
		return "", fmt.Errorf("2captcha: submission failed: %s", req.Request)
	}

	jobID := req.Request

	// 2. Poll for the result
	return s.pollForResult(jobID)
}

func (s *TwoCaptchaSolver) pollForResult(jobID string) (string, error) {
	u, _ := url.Parse("https://2captcha.com/res.php")
	q := u.Query()
	q.Set("key", s.APIKey)
	q.Set("action", "get")
	q.Set("id", jobID)
	q.Set("json", "1")
	u.RawQuery = q.Encode()

	// Poll for 180 seconds with 5-second intervals
	for i := 0; i < 36; i++ {
		time.Sleep(5 * time.Second)

		resp, err := s.Client.Get(u.String())
		if err != nil {
			continue // Retry on network error
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var res twoCaptchaRequest
		if err := json.Unmarshal(body, &res); err != nil {
			continue // Retry on parsing error
		}

		if res.Status == 1 {
			return res.Request, nil // Success
		}
		if res.Request != "CAPCHA_NOT_READY" {
			return "", fmt.Errorf("2captcha: polling failed: %s", res.Request)
		}
	}

	return "", fmt.Errorf("2captcha: timeout waiting for solve")
}
