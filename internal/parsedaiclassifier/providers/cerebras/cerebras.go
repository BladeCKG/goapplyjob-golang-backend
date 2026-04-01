package cerebras

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	providercommon "goapplyjob-golang-backend/internal/parsedaiclassifier/providers/common"
)

const (
	defaultBaseURL = "https://api.cerebras.ai"
	defaultModel   = "gpt-oss-120b"
)

var ErrNotConfigured = errors.New("cerebras api key not configured")

type Config struct {
	APIKey                string
	APIKeys               string
	Model                 string
	Models                string
	BaseURL               string
	PromptContent         string
	CategoryPromptContent string
}

type Classifier struct {
	HTTPClient *http.Client
	Config     Config
}

func (c *Classifier) IsConfigured() bool {
	return len(providercommon.CollectProviderKeys(c.Config.APIKey, c.Config.APIKeys, false)) > 0
}

func (c *Classifier) baseURL() string {
	baseURL := strings.TrimSpace(c.Config.BaseURL)
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return strings.TrimRight(baseURL, "/")
}

func (c *Classifier) apiKeys() []string {
	return providercommon.CollectProviderKeys(c.Config.APIKey, c.Config.APIKeys, false)
}

func (c *Classifier) models() []string {
	return providercommon.CollectProviderModels(c.Config.Model, c.Config.Models, defaultModel)
}

func (c *Classifier) ClassifySync(ctx context.Context, jobTitle, jobDescription string, allowedCategories []string) (string, []string, error) {
	normalizedTitle := strings.TrimSpace(jobTitle)
	if normalizedTitle == "" {
		return "", nil, nil
	}
	if !c.IsConfigured() {
		return "", nil, ErrNotConfigured
	}
	if len(allowedCategories) == 0 {
		return "", nil, nil
	}

	reqPayload := map[string]any{
		"model": "",
		"messages": []map[string]string{
			{"role": "system", "content": c.Config.PromptContent},
			{"role": "user", "content": "Job title:\n" + normalizedTitle + "\n\nJob description:\n" + providercommon.CleanDescription(jobDescription)},
		},
		"temperature": 0,
		"response_format": map[string]any{
			"type":        "json_schema",
			"json_schema": providercommon.BuildJobClassifierSchema(allowedCategories),
		},
	}

	content, err := c.chat(ctx, reqPayload)
	if err != nil {
		return "", nil, err
	}
	category, skills := providercommon.ExtractClassification(content, allowedCategories)
	if category == "" && providercommon.ExtractJSONPayload(content) == nil {
		return "", nil, errors.New("cerebras response did not contain valid classification payload")
	}
	return category, skills, nil
}

func (c *Classifier) ClassifyCategoryOnlySync(ctx context.Context, jobTitle string, allowedCategories []string) (string, error) {
	normalizedTitle := strings.TrimSpace(jobTitle)
	if normalizedTitle == "" {
		return "", nil
	}
	if !c.IsConfigured() {
		return "", ErrNotConfigured
	}
	if len(allowedCategories) == 0 {
		return "", nil
	}

	reqPayload := map[string]any{
		"model": "",
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": c.Config.CategoryPromptContent,
			},
			{"role": "user", "content": "Job title:\n" + normalizedTitle},
		},
		"temperature": 0,
		"response_format": map[string]any{
			"type":        "json_schema",
			"json_schema": providercommon.BuildJobCategoryOnlySchema(allowedCategories),
		},
	}

	content, err := c.chat(ctx, reqPayload)
	if err != nil {
		return "", err
	}
	category := providercommon.ExtractCategoryOnly(content, allowedCategories)
	if category == "" && providercommon.ExtractJSONPayload(content) == nil {
		return "", errors.New("cerebras response did not contain valid classification payload")
	}
	return category, nil
}

func (c *Classifier) chat(ctx context.Context, payload map[string]any) (string, error) {
	if !c.IsConfigured() {
		return "", ErrNotConfigured
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	keys := c.apiKeys()
	models := c.models()
	start := providercommon.KeyRingStart("cerebras", keys)
	var lastErr error
keyAttempts:
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		for _, model := range models {
			reqPayload := map[string]any{}
			for key, value := range payload {
				reqPayload[key] = value
			}
			reqPayload["model"] = model
			body, _ := json.Marshal(reqPayload)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL()+"/v1/chat/completions", bytes.NewReader(body))
			if err != nil {
				return "", err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+apiKey)

			resp, err := client.Do(req)
			if err != nil {
				log.Printf(providercommon.WorkerLogPrefix+" cerebras_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
				lastErr = err
				providercommon.KeyRingSetNext("cerebras", keys, keyIndex+1)
				continue keyAttempts
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf(providercommon.WorkerLogPrefix+" cerebras_classify_failed key_index=%d model=%s status=%d", keyIndex, model, resp.StatusCode)
				lastErr = fmt.Errorf("cerebras returned status %d", resp.StatusCode)
				resp.Body.Close()
				if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
					continue
				}
				providercommon.KeyRingSetNext("cerebras", keys, keyIndex+1)
				continue keyAttempts
			}

			rawResp, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
			resp.Body.Close()
			if err != nil {
				lastErr = err
				providercommon.KeyRingSetNext("cerebras", keys, keyIndex+1)
				continue keyAttempts
			}
			var envelope map[string]any
			if err := json.Unmarshal(rawResp, &envelope); err != nil {
				lastErr = err
				providercommon.KeyRingSetNext("cerebras", keys, keyIndex+1)
				continue keyAttempts
			}
			choices, _ := envelope["choices"].([]any)
			if len(choices) == 0 {
				lastErr = errors.New("cerebras response missing choices")
				providercommon.KeyRingSetNext("cerebras", keys, keyIndex+1)
				continue keyAttempts
			}
			firstChoice, _ := choices[0].(map[string]any)
			message, _ := firstChoice["message"].(map[string]any)
			content, _ := message["content"].(string)
			if strings.TrimSpace(content) == "" {
				lastErr = errors.New("cerebras response content empty")
				providercommon.KeyRingSetNext("cerebras", keys, keyIndex+1)
				continue keyAttempts
			}
			providercommon.KeyRingSetNext("cerebras", keys, keyIndex)
			return content, nil
		}
		providercommon.KeyRingSetNext("cerebras", keys, keyIndex+1)
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", errors.New("cerebras classification failed")
}
