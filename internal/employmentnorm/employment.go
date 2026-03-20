package employmentnorm

import (
	"regexp"
	"strings"
)

var (
	separatorPattern = regexp.MustCompile(`[\s_-]+`)
	splitPattern     = regexp.MustCompile(`\s*[;,/|]+\s*`)
)

func normalizeToken(value string) string {
	normalized := strings.TrimSpace(strings.ToLower(value))
	if normalized == "" {
		return ""
	}
	normalized = separatorPattern.ReplaceAllString(normalized, " ")
	normalized = strings.Join(strings.Fields(normalized), " ")
	return normalized
}

func mapTokenToGoogleEmploymentType(token string) string {
	switch token {
	case "full time", "fulltime":
		return "full_time"
	case "part time", "parttime":
		return "part_time"
	case "contract", "contractor", "freelance":
		return "contractor"
	case "temp", "temporary":
		return "temporary"
	case "intern", "internship":
		return "intern"
	case "volunteer":
		return "volunteer"
	case "per diem":
		return "per_diem"
	case "other", "alternative schedule":
		return "other"
	default:
		return ""
	}
}

func NormalizeEmploymentTypeString(value string) string {
	normalized := normalizeToken(value)
	if normalized == "" {
		return "other"
	}

	parts := splitPattern.Split(normalized, -1)
	for _, part := range parts {
		mapped := mapTokenToGoogleEmploymentType(part)
		if mapped != "" {
			return mapped
		}
	}

	if mapped := mapTokenToGoogleEmploymentType(normalized); mapped != "" {
		return mapped
	}
	return "other"
}

func NormalizeEmploymentTypeAny(value any) any {
	switch item := value.(type) {
	case string:
		if normalized := NormalizeEmploymentTypeString(item); normalized != "" {
			return normalized
		}
	case []any:
		for _, entry := range item {
			if normalized := NormalizeEmploymentTypeAny(entry); normalized != nil {
				return normalized
			}
		}
	}
	return nil
}
