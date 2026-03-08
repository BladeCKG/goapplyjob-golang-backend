package hiringcafe

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/locationnorm"
	"goapplyjob-golang-backend/internal/sources/remoterocketship"
)

const (
	Source      = "hiringcafe"
	PayloadType = "delta"
)

type NormalizedJob struct {
	URL        string
	PostDate   time.Time
	RawPayload map[string]any
}

func ToTargetJobURL(rawURL string) string {
	return rawURL
}

func ParseRawHTML(_ string, _ string) map[string]any {
	return map[string]any{}
}

func ParseImportRows(bodyText string) ([]map[string]any, int) {
	return remoterocketship.ParseImportRows(bodyText)
}

func SerializeImportRows(rows []map[string]any) string {
	return remoterocketship.SerializeImportRows(rows)
}

func BuildSearchAPIURL(baseURL string, page, pageSize int) string {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return baseURL
	}
	query := parsed.Query()
	if page < 0 {
		page = 0
	}
	if pageSize < 1 {
		pageSize = 1
	}
	query.Set("page", strconv.Itoa(page))
	query.Set("size", strconv.Itoa(pageSize))
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func ParseTotalCount(payload map[string]any) int {
	if payload == nil {
		return 0
	}
	switch value := payload["total"].(type) {
	case float64:
		if value < 0 {
			return 0
		}
		return int(value)
	case int:
		if value < 0 {
			return 0
		}
		return value
	default:
		return 0
	}
}

func NormalizeJobs(results []map[string]any) []NormalizedJob {
	normalized := make([]NormalizedJob, 0, len(results))
	for _, item := range results {
		v5Data, _ := item["v5_processed_job_data"].(map[string]any)
		if v5Data == nil {
			v5Data = map[string]any{}
		}
		postDate := parseTime(valueString(v5Data["estimated_publish_date"]))
		if postDate == nil {
			postDate = parseTime(valueString(item["updated_at"]))
		}
		if postDate == nil {
			postDate = parseTime(valueString(item["created_at"]))
		}
		if postDate == nil {
			continue
		}
		requisitionID := valueString(item["requisition_id"])
		if requisitionID == "" {
			requisitionID = valueString(item["id"])
		}
		if requisitionID == "" {
			requisitionID = valueString(item["objectID"])
		}
		if requisitionID == "" {
			continue
		}
		rawURL := "https://hiring.cafe/viewjob/" + requisitionID
		roleTitle := firstNonEmpty(
			valueString(v5Data["job_title_raw"]),
			valueString(v5Data["core_job_title"]),
		)
		senioritySource := strings.TrimSpace(strings.ToLower(firstNonEmpty(valueString(v5Data["seniority_level"]), roleTitle)))
		seniorityTokens := map[string]struct{}{}
		if senioritySource != "" {
			normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(senioritySource, " ")
			for _, token := range strings.Fields(strings.TrimSpace(normalized)) {
				seniorityTokens[token] = struct{}{}
			}
		}
		_, isEntry := seniorityTokens["entry"]
		_, hasIntern := seniorityTokens["intern"]
		isEntry = isEntry || hasIntern
		_, hasJunior := seniorityTokens["junior"]
		_, hasJr := seniorityTokens["jr"]
		isJunior := hasJunior || hasJr
		_, hasMid := seniorityTokens["mid"]
		_, hasSenior := seniorityTokens["senior"]
		_, hasSr := seniorityTokens["sr"]
		isSenior := hasSenior || hasSr
		_, hasLead := seniorityTokens["lead"]
		_, hasPrincipal := seniorityTokens["principal"]
		_, hasStaff := seniorityTokens["staff"]
		isLead := hasLead || hasPrincipal || hasStaff
		isMid := hasMid
		if !isEntry && !isJunior && !isMid && !isSenior && !isLead {
			isMid = true
		}
		locationCountry := locationnorm.NormalizeCountryName("United States", true)
		if locationCountry == "" {
			locationCountry = "United States"
		}
		normalized = append(normalized, NormalizedJob{
			URL:      rawURL,
			PostDate: *postDate,
			RawPayload: map[string]any{
				"id":               requisitionID,
				"created_at":       postDate.UTC().Format(time.RFC3339Nano),
				"url":              valueString(item["apply_url"]),
				"roleTitle":        roleTitle,
				"employmentType":   normalizeEmploymentType(valueStringSlice(v5Data["commitment"])),
				"location":         locationCountry,
				"locationCity":     nil,
				"locationUSStates": []string{},
				"locationCountries": []string{
					locationCountry,
				},
				"isEntryLevel":                             isEntry,
				"isJunior":                                 isJunior,
				"isMidLevel":                               isMid,
				"isSenior":                                 isSenior,
				"isLead":                                   isLead,
				"educationRequirementsCredentialCategory": nil,
			},
		})
	}
	return normalized
}

func normalizeEmploymentType(values []string) any {
	for _, value := range values {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "full time":
			return "full-time"
		case "part time":
			return "part-time"
		case "contract":
			return "contract"
		case "internship":
			return "internship"
		case "temporary":
			return "temporary"
		}
	}
	return nil
}

func parseTime(value string) *time.Time {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		utc := parsed.UTC()
		return &utc
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		utc := parsed.UTC()
		return &utc
	}
	return nil
}

func valueString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func valueStringSlice(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, _ := item.(string)
		text = strings.TrimSpace(text)
		if text != "" {
			out = append(out, text)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
