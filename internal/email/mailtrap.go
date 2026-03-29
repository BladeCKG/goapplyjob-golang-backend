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

const (
	defaultMailtrap      = "https://send.api.mailtrap.io/api/send"
	defaultMailtrapBatch = "https://send.api.mailtrap.io/api/batch"
)

var mailtrapKeyRing = struct {
	mu   sync.Mutex
	keys []string
	next int
}{}

func (s *Service) sendViaMailtrap(toEmail, subject, textContent, htmlContent string) error {
	keys := collectMailtrapKeys(s.cfg)
	if len(keys) == 0 || strings.TrimSpace(s.cfg.MailtrapFromEmail) == "" {
		return fmt.Errorf("Mailtrap API is not configured")
	}
	endpoint := defaultMailtrap
	if s.cfg.MailtrapUseSandbox && strings.TrimSpace(s.cfg.MailtrapInboxID) != "" {
		endpoint = "https://sandbox.api.mailtrap.io/api/send/" + strings.TrimSpace(s.cfg.MailtrapInboxID)
	}
	body := map[string]any{
		"from": map[string]any{
			"email": s.cfg.MailtrapFromEmail,
			"name":  s.cfg.MailtrapFromName,
		},
		"to":      []map[string]any{{"email": toEmail}},
		"subject": subject,
		"text":    textContent,
		"html":    htmlContent,
	}
	rawBody, _ := json.Marshal(body)

	start := mailtrapKeyRingStart(keys)
	var lastErr error
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(rawBody))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("Mailtrap API email send failed: %T", err)
			mailtrapKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		bodyText, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			lastErr = fmt.Errorf("Mailtrap API email send failed: status=%d body=%s", resp.StatusCode, string(bodyText))
			mailtrapKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		mailtrapKeyRingSetNext(keys, keyIndex)
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("Mailtrap API email send failed")
}

func (s *Service) sendViaMailtrapBatchDetailed(toEmails []string, subject, textContent, htmlContent string) (BatchDeliveryResult, error) {
	keys := collectMailtrapKeys(s.cfg)
	if len(keys) == 0 || strings.TrimSpace(s.cfg.MailtrapFromEmail) == "" {
		return BatchDeliveryResult{}, fmt.Errorf("Mailtrap API is not configured")
	}
	if s.cfg.MailtrapUseSandbox {
		items := make([]RecipientDeliveryResult, 0, len(toEmails))
		for _, toEmail := range toEmails {
			if err := s.sendViaMailtrap(toEmail, subject, textContent, htmlContent); err != nil {
				items = append(items, RecipientDeliveryResult{
					Email:   toEmail,
					Status:  "error",
					Message: err.Error(),
				})
				return BatchDeliveryResult{
					Provider: "mailtrap",
					Status:   "error",
					Items:    items,
				}, err
			}
			items = append(items, RecipientDeliveryResult{
				Email:  toEmail,
				Status: "sent",
			})
		}
		return BatchDeliveryResult{
			Provider: "mailtrap",
			Status:   "sent",
			Items:    items,
		}, nil
	}
	requests := make([]map[string]any, 0, len(toEmails))
	for _, toEmail := range toEmails {
		requests = append(requests, map[string]any{
			"to": []map[string]any{{"email": toEmail}},
		})
	}
	body := map[string]any{
		"base": map[string]any{
			"from": map[string]any{
				"email": s.cfg.MailtrapFromEmail,
				"name":  s.cfg.MailtrapFromName,
			},
			"subject": subject,
			"text":    textContent,
			"html":    htmlContent,
		},
		"requests": requests,
	}
	rawBody, _ := json.Marshal(body)

	start := mailtrapKeyRingStart(keys)
	var lastErr error
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		req, err := http.NewRequest(http.MethodPost, defaultMailtrapBatch, bytes.NewReader(rawBody))
		if err != nil {
			return BatchDeliveryResult{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("Mailtrap API batch email send failed: %T", err)
			mailtrapKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		bodyText, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			lastErr = fmt.Errorf("Mailtrap API batch email send failed: status=%d body=%s", resp.StatusCode, string(bodyText))
			mailtrapKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		var responseBody struct {
			Success   bool `json:"success"`
			Responses []struct {
				Success    bool     `json:"success"`
				MessageIDs []string `json:"message_ids"`
				Errors     []string `json:"errors"`
			} `json:"responses"`
			Errors []string `json:"errors"`
		}
		if err := json.Unmarshal(bodyText, &responseBody); err != nil {
			return BatchDeliveryResult{}, fmt.Errorf("Mailtrap API batch email send failed: invalid response body")
		}
		if len(responseBody.Responses) != len(toEmails) {
			return BatchDeliveryResult{}, fmt.Errorf("Mailtrap API batch email send failed: unexpected responses count")
		}
		items := make([]RecipientDeliveryResult, 0, len(toEmails))
		hasErrors := len(responseBody.Errors) > 0
		for i, toEmail := range toEmails {
			item := RecipientDeliveryResult{
				Email:  toEmail,
				Status: "accepted",
			}
			if len(responseBody.Responses[i].MessageIDs) > 0 {
				item.MessageID = responseBody.Responses[i].MessageIDs[0]
			}
			if !responseBody.Responses[i].Success {
				item.Status = "error"
				item.Message = strings.Join(responseBody.Responses[i].Errors, " | ")
				hasErrors = true
			}
			items = append(items, item)
		}
		mailtrapKeyRingSetNext(keys, keyIndex)
		status := "accepted"
		if hasErrors {
			status = "partial"
		}
		return BatchDeliveryResult{
			Provider: "mailtrap",
			Status:   status,
			Items:    items,
		}, nil
	}
	if lastErr != nil {
		return BatchDeliveryResult{}, lastErr
	}
	return BatchDeliveryResult{}, fmt.Errorf("Mailtrap API batch email send failed")
}

func collectMailtrapKeys(cfg config.Config) []string {
	keys := make([]string, 0, 8)
	seen := map[string]struct{}{}
	if raw := strings.TrimSpace(cfg.MailtrapAPITokens); raw != "" {
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
	if single := strings.TrimSpace(cfg.MailtrapAPIToken); single != "" {
		if _, exists := seen[single]; !exists {
			keys = append(keys, single)
		}
	}
	return keys
}

func mailtrapKeyRingStart(keys []string) int {
	mailtrapKeyRing.mu.Lock()
	defer mailtrapKeyRing.mu.Unlock()
	if !equalStringSlices(mailtrapKeyRing.keys, keys) {
		mailtrapKeyRing.keys = append([]string(nil), keys...)
		mailtrapKeyRing.next = 0
	}
	if len(keys) == 0 {
		mailtrapKeyRing.next = 0
		return 0
	}
	if mailtrapKeyRing.next < 0 || mailtrapKeyRing.next >= len(keys) {
		mailtrapKeyRing.next = 0
	}
	return mailtrapKeyRing.next
}

func mailtrapKeyRingSetNext(keys []string, next int) {
	mailtrapKeyRing.mu.Lock()
	defer mailtrapKeyRing.mu.Unlock()
	if !equalStringSlices(mailtrapKeyRing.keys, keys) {
		return
	}
	if len(keys) == 0 {
		mailtrapKeyRing.next = 0
		return
	}
	if next < 0 {
		next = 0
	}
	mailtrapKeyRing.next = next % len(keys)
}
