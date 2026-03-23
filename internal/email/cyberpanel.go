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

const defaultCyberPanelURL = "https://platform.cyberpersons.com/email/v1/send"

var cyberPanelKeyRing = struct {
	mu   sync.Mutex
	keys []string
	next int
}{}

func (s *Service) sendViaCyberPanel(toEmail, subject, textContent, htmlContent string) error {
	keys := collectCyberPanelKeys(s.cfg)
	fromEmail := strings.TrimSpace(s.cfg.CyberPanelFromEmail)
	if len(keys) == 0 || fromEmail == "" {
		return fmt.Errorf("CyberPanel email API is not configured")
	}
	apiURL := strings.TrimSpace(s.cfg.CyberPanelAPIURL)
	if apiURL == "" {
		apiURL = defaultCyberPanelURL
	}

	body := map[string]any{
		"from":    fromEmail,
		"to":      toEmail,
		"subject": subject,
		"html":    htmlContent,
		"text":    textContent,
	}
	rawBody, _ := json.Marshal(body)

	start := cyberPanelKeyRingStart(keys)
	var lastErr error
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewReader(rawBody))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("CyberPanel API email send failed: %T", err)
			cyberPanelKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		bodyText, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			lastErr = fmt.Errorf("CyberPanel API email send failed: status=%d body=%s", resp.StatusCode, string(bodyText))
			cyberPanelKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		cyberPanelKeyRingSetNext(keys, keyIndex)
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("CyberPanel API email send failed")
}

func collectCyberPanelKeys(cfg config.Config) []string {
	keys := make([]string, 0, 8)
	seen := map[string]struct{}{}
	if raw := strings.TrimSpace(cfg.CyberPanelAPIKeys); raw != "" {
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
	if single := strings.TrimSpace(cfg.CyberPanelAPIKey); single != "" {
		if _, exists := seen[single]; !exists {
			keys = append(keys, single)
		}
	}
	return keys
}

func cyberPanelKeyRingStart(keys []string) int {
	cyberPanelKeyRing.mu.Lock()
	defer cyberPanelKeyRing.mu.Unlock()
	if !equalStringSlices(cyberPanelKeyRing.keys, keys) {
		cyberPanelKeyRing.keys = append([]string(nil), keys...)
		cyberPanelKeyRing.next = 0
	}
	if len(keys) == 0 {
		cyberPanelKeyRing.next = 0
		return 0
	}
	if cyberPanelKeyRing.next < 0 || cyberPanelKeyRing.next >= len(keys) {
		cyberPanelKeyRing.next = 0
	}
	return cyberPanelKeyRing.next
}

func cyberPanelKeyRingSetNext(keys []string, next int) {
	cyberPanelKeyRing.mu.Lock()
	defer cyberPanelKeyRing.mu.Unlock()
	if !equalStringSlices(cyberPanelKeyRing.keys, keys) {
		return
	}
	if len(keys) == 0 {
		cyberPanelKeyRing.next = 0
		return
	}
	if next < 0 {
		next = 0
	}
	cyberPanelKeyRing.next = next % len(keys)
}
