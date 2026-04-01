package common

import (
	"encoding/json"
	"html"
	"regexp"
	"strings"
)

const WorkerLogPrefix = "ai-classifier-worker"

var htmlTagPattern = regexp.MustCompile(`(?is)<[^>]+>`)

func ExtractJSONPayload(rawContent string) map[string]any {
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

func DedupeCleanSkills(raw any) []string {
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

func NormalizeCategory(category string, allowedCategories []string) string {
	normalizedCategory := strings.TrimSpace(category)
	if normalizedCategory == "" {
		return ""
	}

	if !ContainsCaseSensitive(allowedCategories, normalizedCategory) {
		switch {
		case ContainsCaseSensitive(allowedCategories, "Blank"):
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

func CleanDescription(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	unescaped := html.UnescapeString(trimmed)
	withoutTags := htmlTagPattern.ReplaceAllString(unescaped, " ")
	return strings.Join(strings.Fields(withoutTags), " ")
}

func ExtractClassification(rawContent string, allowedCategories []string) (string, []string) {
	payload := ExtractJSONPayload(rawContent)
	if len(payload) == 0 {
		return "", nil
	}

	category, _ := payload["job_category"].(string)
	skills := DedupeCleanSkills(payload["required_skills"])

	return NormalizeCategory(category, allowedCategories), skills
}

func ExtractCategoryOnly(rawContent string, allowedCategories []string) string {
	payload := ExtractJSONPayload(rawContent)
	if len(payload) == 0 {
		return ""
	}

	category, _ := payload["job_category"].(string)
	return NormalizeCategory(category, allowedCategories)
}

func BuildJobClassifierSchema(allowedCategories []string) map[string]any {
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

func BuildJobCategoryOnlySchema(allowedCategories []string) map[string]any {
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

func ContainsCaseSensitive(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
