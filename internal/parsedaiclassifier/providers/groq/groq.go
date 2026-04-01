package groq

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
	DefaultModel   = "openai/gpt-oss-120b"
	defaultBaseURL = "https://api.groq.com/openai/v1"
)

var FallbackModels = []string{
	"openai/gpt-oss-120b",
	"openai/gpt-oss-20b",
}

var ErrAPIKeysNotConfigured = errors.New("groq api keys not configured")

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

func CollectAPIKeys(cfg Config) []string {
	return providercommon.CollectProviderKeys(cfg.APIKey, cfg.APIKeys, false)
}

func CollectModels(cfg Config) []string {
	primary := strings.TrimSpace(cfg.Model)
	if primary == "" {
		primary = DefaultModel
	}
	return providercommon.CollectProviderModels(primary, cfg.Models, FallbackModels...)
}

func baseURL(cfg Config) string {
	value := strings.TrimSpace(cfg.BaseURL)
	if value == "" {
		value = defaultBaseURL
	}
	return strings.TrimRight(value, "/")
}

func shouldIncludeReasoningEffort(model string) bool {
	return model != "moonshotai/kimi-k2-instruct-0905"
}

func (c *Classifier) ClassifySync(ctx context.Context, jobTitle, jobDescription string, allowedCategories []string) (string, []string, error) {
	normalizedTitle := strings.TrimSpace(jobTitle)
	if normalizedTitle == "" {
		return "", nil, nil
	}

	keys := CollectAPIKeys(c.Config)
	if len(keys) == 0 {
		return "", nil, ErrAPIKeysNotConfigured
	}
	if len(allowedCategories) == 0 {
		return "", nil, nil
	}

	models := CollectModels(c.Config)
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}

	start := providercommon.KeyRingStart("groq", keys)
	var lastErr error
keyAttempts:
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		for modelIndex, model := range models {
			reqPayload := map[string]any{
				"model": model,
				"messages": []map[string]string{
					{
						"role":    "system",
						"content": c.Config.PromptContent,
					},
					{
						"role":    "user",
						"content": "Job title:\n" + normalizedTitle + "\n\nJob description:\n" + providercommon.CleanDescription(jobDescription),
					},
				},
				"temperature":           1,
				"max_completion_tokens": 8192,
				"top_p":                 1,
				"stream":                false,
				"stop":                  nil,
				"response_format": map[string]any{
					"type":        "json_schema",
					"json_schema": providercommon.BuildJobClassifierSchema(allowedCategories),
				},
			}
			if shouldIncludeReasoningEffort(model) {
				reqPayload["reasoning_effort"] = "low"
			}
			body, _ := json.Marshal(reqPayload)

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL(c.Config)+"/chat/completions", bytes.NewReader(body))
			if err != nil {
				return "", nil, err
			}
			req.Header.Set("Authorization", "Bearer "+apiKey)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
				lastErr = err
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s status=%d model_index=%d", keyIndex, model, resp.StatusCode, modelIndex)
				lastErr = fmt.Errorf("groq returned status %d", resp.StatusCode)
				resp.Body.Close()
				continue
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s status=%d", keyIndex, model, resp.StatusCode)
				lastErr = fmt.Errorf("groq returned status %d", resp.StatusCode)
				resp.Body.Close()
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			rawResp, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
			resp.Body.Close()
			if err != nil {
				log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
				lastErr = err
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			var envelope map[string]any
			if err := json.Unmarshal(rawResp, &envelope); err != nil {
				log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
				lastErr = err
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			choices, _ := envelope["choices"].([]any)
			if len(choices) == 0 {
				lastErr = errors.New("groq response missing choices")
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			firstChoice, _ := choices[0].(map[string]any)
			message, _ := firstChoice["message"].(map[string]any)
			content, _ := message["content"].(string)
			if strings.TrimSpace(content) == "" {
				lastErr = errors.New("groq response content empty")
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			category, skills := providercommon.ExtractClassification(content, allowedCategories)
			if category == "" && providercommon.ExtractJSONPayload(content) == nil {
				lastErr = errors.New("groq response did not contain valid classification payload")
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			providercommon.KeyRingSetNext("groq", keys, keyIndex)
			return category, skills, nil
		}
		providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
	}

	if lastErr != nil {
		return "", nil, lastErr
	}
	return "", nil, errors.New("groq classification failed")
}

func (c *Classifier) ClassifyCategoryOnlySync(ctx context.Context, jobTitle string, allowedCategories []string) (string, error) {
	normalizedTitle := strings.TrimSpace(jobTitle)
	if normalizedTitle == "" {
		return "", nil
	}

	keys := CollectAPIKeys(c.Config)
	if len(keys) == 0 {
		return "", ErrAPIKeysNotConfigured
	}
	if len(allowedCategories) == 0 {
		return "", nil
	}

	models := CollectModels(c.Config)
	systemPrompt := strings.TrimSpace(c.Config.CategoryPromptContent)
	if systemPrompt == "" {
		systemPrompt = "You are a strict job classifier. Classify the given job title into exactly one job_category from the schema enum. Use only the job title. Return only schema-compliant JSON."
	}

	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}

	start := providercommon.KeyRingStart("groq", keys)
	var lastErr error
keyAttempts:
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		for modelIndex, model := range models {
			reqPayload := map[string]any{
				"model": model,
				"messages": []map[string]string{
					{
						"role":    "system",
						"content": systemPrompt,
					},
					{
						"role":    "user",
						"content": "Job title:\n" + normalizedTitle,
					},
				},
				"temperature":           0,
				"max_completion_tokens": 128,
				"top_p":                 1,
				"stream":                false,
				"stop":                  nil,
				"response_format": map[string]any{
					"type":        "json_schema",
					"json_schema": providercommon.BuildJobCategoryOnlySchema(allowedCategories),
				},
			}
			if shouldIncludeReasoningEffort(model) {
				reqPayload["reasoning_effort"] = "low"
			}
			body, _ := json.Marshal(reqPayload)

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL(c.Config)+"/chat/completions", bytes.NewReader(body))
			if err != nil {
				return "", err
			}
			req.Header.Set("Authorization", "Bearer "+apiKey)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
				lastErr = err
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s status=%d model_index=%d", keyIndex, model, resp.StatusCode, modelIndex)
				lastErr = fmt.Errorf("groq returned status %d", resp.StatusCode)
				resp.Body.Close()
				continue
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s status=%d", keyIndex, model, resp.StatusCode)
				lastErr = fmt.Errorf("groq returned status %d", resp.StatusCode)
				resp.Body.Close()
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			rawResp, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
			resp.Body.Close()
			if err != nil {
				log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
				lastErr = err
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			var envelope map[string]any
			if err := json.Unmarshal(rawResp, &envelope); err != nil {
				log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
				lastErr = err
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			choices, _ := envelope["choices"].([]any)
			if len(choices) == 0 {
				lastErr = errors.New("groq response missing choices")
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			firstChoice, _ := choices[0].(map[string]any)
			message, _ := firstChoice["message"].(map[string]any)
			content, _ := message["content"].(string)
			if strings.TrimSpace(content) == "" {
				lastErr = errors.New("groq response content empty")
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			category := providercommon.ExtractCategoryOnly(content, allowedCategories)
			if category == "" && providercommon.ExtractJSONPayload(content) == nil {
				lastErr = errors.New("groq response did not contain valid classification payload")
				providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
				continue keyAttempts
			}
			providercommon.KeyRingSetNext("groq", keys, keyIndex)
			return category, nil
		}
		providercommon.KeyRingSetNext("groq", keys, keyIndex+1)
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", errors.New("groq category-only classification failed")
}
