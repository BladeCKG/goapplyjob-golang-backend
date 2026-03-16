package email

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"goapplyjob-golang-backend/internal/config"
)

const defaultBrevoURL = "https://api.brevo.com/v3/smtp/email"

var brevoKeyRing = struct {
	mu   sync.Mutex
	keys []string
	next int
}{}

func (s *Service) sendViaBrevo(toEmail, siteName, code, textContent, htmlContent string) error {
	keys := collectBrevoKeys(s.cfg)
	if len(keys) == 0 || strings.TrimSpace(s.cfg.BrevoFromEmail) == "" {
		return fmt.Errorf("Brevo email API is not configured")
	}
	apiURL := strings.TrimSpace(s.cfg.BrevoAPIURL)
	if apiURL == "" {
		apiURL = defaultBrevoURL
	}
	body := map[string]any{
		"sender": map[string]any{
			"name":  firstNonEmpty(s.cfg.BrevoFromName, siteName),
			"email": s.cfg.BrevoFromEmail,
		},
		"to":          []map[string]any{{"email": toEmail}},
		"subject":     siteName + " verification code: " + code,
		"htmlContent": htmlContent,
		"textContent": textContent,
	}
	rawBody, _ := json.Marshal(body)

	start := brevoKeyRingStart(keys)
	var lastErr error
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewReader(rawBody))
		if err != nil {
			return err
		}
		req.Header.Set("accept", "application/json")
		req.Header.Set("api-key", apiKey)
		req.Header.Set("content-type", "application/json")
		resp, err := s.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("Brevo API email send failed: %T", err)
			brevoKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		bodyText, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			lastErr = fmt.Errorf("Brevo API email send failed: status=%d body=%s", resp.StatusCode, string(bodyText))
			brevoKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		brevoKeyRingSetNext(keys, keyIndex)
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("Brevo API email send failed")
}

func collectBrevoKeys(cfg config.Config) []string {
	keys := make([]string, 0, 8)
	seen := map[string]struct{}{}
	if raw := strings.TrimSpace(cfg.BrevoAPIKeys); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			key := strings.TrimSpace(part)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	if single := strings.TrimSpace(cfg.BrevoAPIKey); single != "" {
		if _, exists := seen[single]; !exists {
			keys = append(keys, single)
		}
	}
	return keys
}

func brevoKeyRingStart(keys []string) int {
	brevoKeyRing.mu.Lock()
	defer brevoKeyRing.mu.Unlock()
	if !equalStringSlices(brevoKeyRing.keys, keys) {
		brevoKeyRing.keys = append([]string(nil), keys...)
		brevoKeyRing.next = 0
	}
	if len(keys) == 0 {
		brevoKeyRing.next = 0
		return 0
	}
	if brevoKeyRing.next < 0 || brevoKeyRing.next >= len(keys) {
		brevoKeyRing.next = 0
	}
	return brevoKeyRing.next
}

func brevoKeyRingSetNext(keys []string, next int) {
	brevoKeyRing.mu.Lock()
	defer brevoKeyRing.mu.Unlock()
	if !equalStringSlices(brevoKeyRing.keys, keys) {
		return
	}
	if len(keys) == 0 {
		brevoKeyRing.next = 0
		return
	}
	if next < 0 {
		next = 0
	}
	brevoKeyRing.next = next % len(keys)
}
