package parsed

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"goapplyjob-golang-backend/internal/config"
	"html"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	envGroqAPIKey                 = "GROQ_API_KEY"
	envGroqAPIKeys                = "GROQ_API_KEYS"
	envGroqModel                  = "GROQ_MODEL"
	envGroqClassifierPromptSource = "GROQ_CLASSIFIER_PROMPT_SOURCE"

	defaultGroqModel                = "moonshotai/kimi-k2-instruct-0905"
	defaultGroqClassifierPromptPath = "internal/parsed/prompts/job_title_classification.txt"
)

type GroqJobClassifier struct {
	HTTPClient *http.Client
}

var defaultGroqClassifier = &GroqJobClassifier{}

var groqCategoryCache = struct {
	mu        sync.RWMutex
	items     []string
	functions map[string]string
}{}

var groqAPIKeyRing = struct {
	mu   sync.Mutex
	keys []string
	next int
}{}

var groqPromptCache = struct {
	mu      sync.RWMutex
	source  string
	content string
}{}

var groqHTMLTagPattern = regexp.MustCompile(`(?is)<[^>]+>`)

//go:embed prompts\job_title_classification.txt
var groqPromptFS embed.FS

func SetCachedGroqCategorizedJobTitles(titles []string, functions map[string]string) {
	groqCategoryCache.mu.Lock()
	groqCategoryCache.items = titles
	groqCategoryCache.functions = functions
	groqCategoryCache.mu.Unlock()
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func groqKeyRingStart(keys []string) int {
	groqAPIKeyRing.mu.Lock()
	defer groqAPIKeyRing.mu.Unlock()

	if !equalStringSlices(groqAPIKeyRing.keys, keys) {
		groqAPIKeyRing.keys = append([]string(nil), keys...)
		groqAPIKeyRing.next = 0
	}
	if len(keys) == 0 {
		groqAPIKeyRing.next = 0
		return 0
	}
	if groqAPIKeyRing.next < 0 || groqAPIKeyRing.next >= len(keys) {
		groqAPIKeyRing.next = 0
	}
	return groqAPIKeyRing.next
}

func groqKeyRingSetNext(keys []string, next int) {
	groqAPIKeyRing.mu.Lock()
	defer groqAPIKeyRing.mu.Unlock()

	// Only update when we're still talking about the same key set.
	if !equalStringSlices(groqAPIKeyRing.keys, keys) {
		return
	}
	if len(keys) == 0 {
		groqAPIKeyRing.next = 0
		return
	}
	if next < 0 {
		next = 0
	}
	groqAPIKeyRing.next = next % len(keys)
}

func classifyJobTitleWithGroqSync(jobTitle, jobDescription string, allowedCategories []string) (string, []string) {
	return defaultGroqClassifier.classifySync(jobTitle, jobDescription, allowedCategories)
}

func classifyJobCategoryWithGroqSync(jobTitle string, allowedCategories []string) string {
	return defaultGroqClassifier.classifyCategoryOnlySync(jobTitle, allowedCategories)
}

func extractJSONPayload(rawContent string) map[string]any {
	content := strings.TrimSpace(rawContent)
	if content == "" {
		return nil
	}
	if strings.Contains(content, "```") {
		re := regexp.MustCompile("(?is)```(?:json)?\\s*(\\{.*\\})\\s*```")
		if match := re.FindStringSubmatch(content); len(match) == 2 {
			content = strings.TrimSpace(match[1])
		}
	}
	if !(strings.HasPrefix(content, "{") && strings.HasSuffix(content, "}")) {
		start := strings.Index(content, "{")
		end := strings.LastIndex(content, "}")
		if start < 0 || end <= start {
			return nil
		}
		content = content[start : end+1]
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return nil
	}
	return payload
}

func dedupeCleanSkills(raw any) []string {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(list))
	for _, item := range list {
		value, ok := item.(string)
		if !ok {
			continue
		}
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func normalizeGroqCategory(category string, allowedCategories []string) string {
	normalizedCategory := strings.TrimSpace(category)
	if normalizedCategory == "" {
		return ""
	}

	if !containsCaseSensitive(allowedCategories, normalizedCategory) {
		switch {
		case containsCaseSensitive(allowedCategories, "Blank"):
			normalizedCategory = "Blank"
		case len(allowedCategories) > 0:
			normalizedCategory = allowedCategories[0]
		default:
			normalizedCategory = ""
		}
	}

	if strings.EqualFold(normalizedCategory, "blank") {
		return ""
	}

	return normalizedCategory
}

func cleanGroqDescription(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	unescaped := html.UnescapeString(trimmed)
	withoutTags := groqHTMLTagPattern.ReplaceAllString(unescaped, " ")
	return strings.Join(strings.Fields(withoutTags), " ")
}

func extractGroqClassification(rawContent string, allowedCategories []string) (string, []string) {
	payload := extractJSONPayload(rawContent)
	if len(payload) == 0 {
		return "", nil
	}

	category, _ := payload["job_category"].(string)
	skills := dedupeCleanSkills(payload["required_skills"])

	return normalizeGroqCategory(category, allowedCategories), skills
}

func extractGroqCategoryOnly(rawContent string, allowedCategories []string) string {
	payload := extractJSONPayload(rawContent)
	if len(payload) == 0 {
		return ""
	}

	category, _ := payload["job_category"].(string)
	return normalizeGroqCategory(category, allowedCategories)
}

func collectGroqAPIKeys() []string {
	keys := make([]string, 0, 8)
	seen := map[string]struct{}{}

	keysRaw := strings.TrimSpace(os.Getenv(envGroqAPIKeys))
	if keysRaw != "" {
		for _, part := range strings.Split(keysRaw, ",") {
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

	single := strings.TrimSpace(os.Getenv(envGroqAPIKey))
	if single != "" {
		if _, exists := seen[single]; !exists {
			keys = append(keys, single)
		}
	}
	return keys
}

func buildGroqJobClassifierSchema(allowedCategories []string) map[string]any {
	return map[string]any{
		"name":   "job_classifier",
		"strict": true,
		"schema": map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"properties": map[string]any{
				"job_category": map[string]any{
					"type": "string",
					"enum": allowedCategories,
				},
				"required_skills": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "string",
					},
				},
			},
			"required": []string{"job_category", "required_skills"},
		},
	}
}

func buildGroqJobCategoryOnlySchema(allowedCategories []string) map[string]any {
	return map[string]any{
		"name":   "job_category_classifier",
		"strict": true,
		"schema": map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"properties": map[string]any{
				"job_category": map[string]any{
					"type": "string",
					"enum": allowedCategories,
				},
			},
			"required": []string{"job_category"},
		},
	}
}

func (g *GroqJobClassifier) loadClassifierPromptContent() string {
	source := config.Getenv(envGroqClassifierPromptSource, defaultGroqClassifierPromptPath)

	groqPromptCache.mu.RLock()
	if groqPromptCache.source == source && groqPromptCache.content != "" {
		cached := groqPromptCache.content
		groqPromptCache.mu.RUnlock()
		return cached
	}
	groqPromptCache.mu.RUnlock()

	content, err := g.readPromptContentSource(source)
	if err != nil {
		log.Printf("parsed-job-worker groq_prompt_load_failed source=%q error=%v", source, err)
		content = loadEmbeddedDefaultGroqPrompt()
		if content == "" {
			return ""
		}
		source = defaultGroqClassifierPromptPath
	}
	content = strings.TrimSpace(content)
	if content == "" {
		content = loadEmbeddedDefaultGroqPrompt()
		source = defaultGroqClassifierPromptPath
		if content == "" {
			return ""
		}
	}

	groqPromptCache.mu.Lock()
	groqPromptCache.source = source
	groqPromptCache.content = content
	groqPromptCache.mu.Unlock()
	return content
}

func loadEmbeddedDefaultGroqPrompt() string {
	body, err := groqPromptFS.ReadFile("prompts/job_title_classification.txt")
	if err != nil {
		log.Printf("parsed-job-worker groq_prompt_embedded_load_failed error=%v", err)
		return ""
	}
	return strings.TrimSpace(string(body))
}

func (g *GroqJobClassifier) readPromptContentSource(source string) (string, error) {
	if !strings.Contains(source, "://") {
		source = filepath.Clean(source)
	}
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		client := g.HTTPClient
		if client == nil {
			client = &http.Client{Timeout: 20 * time.Second}
		}
		req, err := http.NewRequest(http.MethodGet, source, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("User-Agent", "goapplyjob-groq-classifier/1.0")
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", &url.Error{Op: "GET", URL: source, Err: io.ErrUnexpectedEOF}
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
		if err != nil {
			return "", err
		}
		return string(body), nil
	}

	if strings.HasPrefix(source, "file://") {
		parsed, err := url.Parse(source)
		if err != nil {
			return "", err
		}
		body, err := os.ReadFile(parsed.Path)
		if err != nil {
			return "", err
		}
		return string(body), nil
	}

	body, err := os.ReadFile(source)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (g *GroqJobClassifier) classifySync(jobTitle, jobDescription string, allowedCategories []string) (string, []string) {
	normalizedTitle := strings.TrimSpace(jobTitle)
	if normalizedTitle == "" {
		return "", nil
	}

	keys := collectGroqAPIKeys()
	if len(keys) == 0 || len(allowedCategories) == 0 {
		return "", nil
	}

	model := strings.TrimSpace(os.Getenv(envGroqModel))
	if model == "" {
		model = defaultGroqModel
	}
	description := cleanGroqDescription(jobDescription)
	schema := buildGroqJobClassifierSchema(allowedCategories)
	systemPrompt := g.loadClassifierPromptContent()

	reqPayload := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": systemPrompt,
			},
			{
				"role":    "user",
				"content": "Job title:\n" + normalizedTitle + "\n\nJob description:\n" + description,
			},
		},
		"temperature":           0,
		"max_completion_tokens": 512,
		"top_p":                 1,
		"stream":                false,
		"stop":                  nil,
		"response_format": map[string]any{
			"type":        "json_schema",
			"json_schema": schema,
		},
	}
	body, _ := json.Marshal(reqPayload)

	client := g.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}

	start := groqKeyRingStart(keys)
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		req, err := http.NewRequest(http.MethodPost, "https://api.groq.com/openai/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return "", nil
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s status=%d", keyIndex, model, resp.StatusCode)
			resp.Body.Close()
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		rawResp, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		resp.Body.Close()
		if err != nil {
			log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		var envelope map[string]any
		if err := json.Unmarshal(rawResp, &envelope); err != nil {
			log.Printf("parsed-job-worker groq_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		choices, _ := envelope["choices"].([]any)
		if len(choices) == 0 {
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		firstChoice, _ := choices[0].(map[string]any)
		message, _ := firstChoice["message"].(map[string]any)
		content, _ := message["content"].(string)
		if strings.TrimSpace(content) == "" {
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		category, skills := extractGroqClassification(content, allowedCategories)
		groqKeyRingSetNext(keys, keyIndex)
		return category, skills
	}

	return "", nil
}

func (g *GroqJobClassifier) classifyCategoryOnlySync(jobTitle string, allowedCategories []string) string {
	normalizedTitle := strings.TrimSpace(jobTitle)
	if normalizedTitle == "" {
		return ""
	}

	keys := collectGroqAPIKeys()
	if len(keys) == 0 || len(allowedCategories) == 0 {
		return ""
	}

	model := strings.TrimSpace(os.Getenv(envGroqModel))
	if model == "" {
		model = defaultGroqModel
	}
	schema := buildGroqJobCategoryOnlySchema(allowedCategories)

	reqPayload := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{
				"role": "system",
				"content": "You are a strict job classifier. " +
					"Classify the given job title into exactly one job_category from the schema enum. " +
					"Use only the job title. " +
					"Return only schema-compliant JSON.",
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
			"json_schema": schema,
		},
	}
	body, _ := json.Marshal(reqPayload)

	client := g.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}

	start := groqKeyRingStart(keys)
	for attempt := 0; attempt < len(keys); attempt++ {
		keyIndex := (start + attempt) % len(keys)
		apiKey := keys[keyIndex]
		req, err := http.NewRequest(http.MethodPost, "https://api.groq.com/openai/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return ""
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s status=%d", keyIndex, model, resp.StatusCode)
			resp.Body.Close()
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		rawResp, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		resp.Body.Close()
		if err != nil {
			log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		var envelope map[string]any
		if err := json.Unmarshal(rawResp, &envelope); err != nil {
			log.Printf("parsed-job-worker groq_category_only_classify_failed key_index=%d model=%s error=%v", keyIndex, model, err)
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		choices, _ := envelope["choices"].([]any)
		if len(choices) == 0 {
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		firstChoice, _ := choices[0].(map[string]any)
		message, _ := firstChoice["message"].(map[string]any)
		content, _ := message["content"].(string)
		if strings.TrimSpace(content) == "" {
			groqKeyRingSetNext(keys, keyIndex+1)
			continue
		}
		groqKeyRingSetNext(keys, keyIndex)
		return extractGroqCategoryOnly(content, allowedCategories)
	}

	return ""
}

func containsCaseSensitive(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (s *Service) loadAllowedJobCategoriesForGroq(ctx context.Context) ([]string, error) {
	categories, _, err := s.loadAllowedJobCategoriesAndFunctionsForGroq(ctx)
	return categories, err
}

func (s *Service) loadAllowedJobCategoriesAndFunctionsForGroq(ctx context.Context) ([]string, map[string]string, error) {
	groqCategoryCache.mu.RLock()
	cached := append([]string(nil), groqCategoryCache.items...)
	cachedFunctions := groqCategoryCache.functions
	groqCategoryCache.mu.RUnlock()
	if len(cached) > 0 {
		if !containsCaseSensitive(cached, "Blank") {
			cached = append(cached, "Blank")
		}
		return cached, cachedFunctions, nil
	}
	log.Printf("parsed-job-worker groq_category_cache_empty_fallback=db")

	rows, err := s.DB.SQL.QueryContext(
		ctx,
		`SELECT categorized_job_title, categorized_job_function
		   FROM parsed_jobs
		  WHERE categorized_job_title IS NOT NULL
		    AND categorized_job_function IS NOT NULL
		  GROUP BY categorized_job_title, categorized_job_function`,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 128)
	functions := map[string]string{}
	for rows.Next() {
		var title sql.NullString
		var function sql.NullString
		if scanErr := rows.Scan(&title, &function); scanErr != nil {
			return nil, nil, scanErr
		}
		titleString := title.String
		out = append(out, titleString)

		functionValue := function.String
		functions[titleString] = functionValue
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if !containsCaseSensitive(out, "Blank") {
		out = append(out, "Blank")
	}

	groqCategoryCache.mu.Lock()
	groqCategoryCache.items = append([]string(nil), out...)
	groqCategoryCache.functions = functions
	groqCategoryCache.mu.Unlock()

	return out, functions, nil
}
