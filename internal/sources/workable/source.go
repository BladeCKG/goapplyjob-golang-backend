package workable

import (
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	Source      = "workable"
	PayloadType = "delta"
)

func BuildAPIURL(baseURL, pageToken string, pageLimit int) string {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return baseURL
	}
	query := parsed.Query()
	if strings.TrimSpace(pageToken) == "" {
		query.Del("pageToken")
	} else {
		query.Set("pageToken", pageToken)
	}
	query.Set("limit", strconvItoa(pageLimit))
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func ToTargetJobURL(rawURL string) string { return rawURL }

func ParseRawHTML(_ string, sourceURL string) map[string]any {
	return map[string]any{"url": sourceURL}
}

func NormalizeJobs(payloadText string) ([]map[string]any, int) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadText), &payload); err != nil {
		return nil, 1
	}
	jobs, _ := payload["jobs"].([]any)
	rows := make([]map[string]any, 0, len(jobs))
	skipped := 0
	for _, job := range jobs {
		item, _ := job.(map[string]any)
		if item == nil {
			skipped++
			continue
		}
		row := normalizeJob(item)
		if row == nil {
			skipped++
			continue
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		left, _ := rows[i]["post_date"].(time.Time)
		right, _ := rows[j]["post_date"].(time.Time)
		if left.Equal(right) {
			return stringValue(rows[i]["url"]) > stringValue(rows[j]["url"])
		}
		return left.After(right)
	})
	return rows, skipped
}

func ParseImportRows(bodyText string) ([]map[string]any, int) {
	var payload any
	if err := json.Unmarshal([]byte(bodyText), &payload); err != nil {
		return nil, 1
	}
	rows := []map[string]any{}
	skipped := 0
	switch item := payload.(type) {
	case map[string]any:
		return NormalizeJobs(bodyText)
	case []any:
		for _, entry := range item {
			row, _ := entry.(map[string]any)
			if row == nil {
				skipped++
				continue
			}
			urlValue := stringValue(row["url"])
			postDate, err := parseISO(stringValue(row["post_date"]))
			rawPayload, _ := row["raw_payload"].(map[string]any)
			if urlValue == "" || err != nil || rawPayload == nil {
				skipped++
				continue
			}
			rows = append(rows, map[string]any{
				"url":         urlValue,
				"post_date":   postDate,
				"raw_payload": rawPayload,
			})
		}
	default:
		return nil, 1
	}
	return rows, skipped
}

func SerializeImportRows(rows []map[string]any) string {
	payload := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := map[string]any{
			"url":         row["url"],
			"post_date":   timeValue(row["post_date"]).UTC().Format(time.RFC3339Nano),
			"raw_payload": row["raw_payload"],
		}
		payload = append(payload, item)
	}
	data, _ := json.Marshal(payload)
	return string(data)
}

func normalizeJob(item map[string]any) map[string]any {
	urlValue := stringValue(item["url"])
	createdAt, createdErr := parseISO(stringValue(item["created"]))
	updatedAt, updatedErr := parseISO(stringValue(item["updated"]))
	postDate := time.Time{}
	switch {
	case createdErr == nil && updatedErr == nil:
		if updatedAt.After(createdAt) {
			postDate = updatedAt
		} else {
			postDate = createdAt
		}
	case createdErr == nil:
		postDate = createdAt
	case updatedErr == nil:
		postDate = updatedAt
	default:
		return nil
	}
	if strings.TrimSpace(urlValue) == "" {
		return nil
	}
	return map[string]any{
		"url":         urlValue,
		"post_date":   postDate,
		"raw_payload": buildRawPayload(item, urlValue, postDate),
	}
}

func buildRawPayload(item map[string]any, urlValue string, postDate time.Time) map[string]any {
	title := stringValue(item["title"])
	company, _ := item["company"].(map[string]any)
	location, _ := item["location"].(map[string]any)
	locationItems, _ := item["locations"].([]any)
	locationParts := []string{}
	for _, entry := range locationItems {
		value := strings.TrimSpace(stringValue(entry))
		if value == "" || strings.EqualFold(value, "TELECOMMUTE") {
			continue
		}
		locationParts = append(locationParts, value)
	}
	if len(locationParts) == 0 {
		for _, value := range []string{stringValue(location["city"]), stringValue(location["subregion"]), stringValue(location["countryName"])} {
			if strings.TrimSpace(value) != "" {
				locationParts = append(locationParts, value)
			}
		}
	}
	isEntry, isJunior, isMid, isSenior, isLead := inferSeniority(title)
	companySlug := firstNonEmpty(
		slugify(stringValue(company["title"])),
		slugFromCompanyURL(stringValue(company["website"])),
		slugFromCompanyURL(stringValue(company["url"])),
	)
	if companySlug == "" {
		companySlug = "workable-company"
	}
	jobSlug := firstNonEmpty(
		slugFromURLPath(urlValue),
		slugify(title),
	)
	if jobSlug == "" {
		jobSlug = "workable-job"
	}
	return map[string]any{
		"id":                           stringOrNil(item["id"]),
		"created_at":                   postDate.UTC().Format(time.RFC3339Nano),
		"validUntilDate":               nil,
		"dateDeleted":                  nil,
		"descriptionLanguage":          stringOrNil(item["language"]),
		"roleTitle":                    stringOrNil(title),
		"roleDescription":              stringOrNil(item["description"]),
		"roleRequirements":             stringOrNil(item["requirementsSection"]),
		"benefits":                     stringOrNil(item["benefitsSection"]),
		"jobDescriptionSummary":        stringOrNil(item["socialSharingDescription"]),
		"twoLineJobDescriptionSummary": stringOrNil(item["socialSharingDescription"]),
		"url":                          urlValue,
		"slug":                         jobSlug,
		"employmentType":               stringOrNil(item["employmentType"]),
		"location":                     strings.Join(locationParts, ", "),
		"locationType":                 stringOrNil(item["workplace"]),
		"locationCity":                 stringOrNil(location["city"]),
		"categorizedJobTitle":          nil,
		"categorizedJobFunction":       nil,
		"educationRequirementsCredentialCategory":  nil,
		"experienceInPlaceOfEducation":             nil,
		"experienceRequirementsMonthsOfExperience": nil,
		"isOnLinkedIn":      false,
		"isPromoted":        boolValue(item["isFeatured"]),
		"isEntryLevel":      isEntry,
		"isJunior":          isJunior,
		"isMidLevel":        isMid,
		"isSenior":          isSenior,
		"isLead":            isLead,
		"requiredLanguages": []string{stringValue(item["language"])},
		"locationUSStates":  sliceIfValue(location["subregion"]),
		"locationCountries": sliceIfValue(location["countryName"]),
		"techStack":         []string{},
		"salaryRange":       map[string]any{},
		"company": map[string]any{
			"id":                   stringOrNil(company["id"]),
			"name":                 stringOrNil(company["title"]),
			"slug":                 companySlug,
			"tagline":              nil,
			"foundedYear":          nil,
			"homePageURL":          stringOrNil(company["website"]),
			"linkedInURL":          nil,
			"employeeRange":        nil,
			"profilePicURL":        stringOrNil(company["image"]),
			"fundingData":          []any{},
			"industrySpecialities": nil,
			"companyMatchKey":      buildCompanyMatchKeys(stringValue(company["website"]), stringValue(company["url"]), stringValue(company["title"])),
		},
	}
}

func inferSeniority(title string) (bool, bool, bool, bool, bool) {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(title), " ")
	tokens := map[string]struct{}{}
	for _, token := range strings.Fields(strings.TrimSpace(normalized)) {
		tokens[token] = struct{}{}
	}
	_, hasEntry := tokens["entry"]
	_, hasIntern := tokens["intern"]
	_, hasJunior := tokens["junior"]
	_, hasJr := tokens["jr"]
	_, hasSenior := tokens["senior"]
	_, hasSr := tokens["sr"]
	_, hasLead := tokens["lead"]
	_, hasPrincipal := tokens["principal"]
	_, hasStaff := tokens["staff"]
	_, hasHead := tokens["head"]
	isEntry := hasEntry || hasIntern
	isJunior := hasJunior || hasJr
	isSenior := hasSenior || hasSr
	isLead := hasLead || hasPrincipal || hasStaff || hasHead
	isMid := !(isEntry || isJunior || isSenior || isLead)
	return isEntry, isJunior, isMid, isSenior, isLead
}

func buildCompanyMatchKeys(websiteURL, companyURL, companyName string) []string {
	keys := []string{}
	for _, rawURL := range []string{websiteURL, companyURL} {
		if strings.TrimSpace(rawURL) == "" {
			continue
		}
		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Hostname() == "" {
			continue
		}
		host := strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
		keys = append(keys, "domain:"+host)
		parts := strings.Split(host, ".")
		if len(parts) > 2 {
			keys = append(keys, "subdomain:"+host)
		}
	}
	if len(keys) == 0 && strings.TrimSpace(companyName) != "" {
		normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(companyName), "-")
		normalized = strings.Trim(normalized, "-")
		if normalized != "" {
			keys = append(keys, "name:"+normalized)
		}
	}
	return keys
}

func slugify(value string) string {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), "-")
	return strings.Trim(normalized, "-")
}

func slugFromURLPath(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	parts := []string{}
	for _, part := range strings.Split(parsed.Path, "/") {
		if strings.TrimSpace(part) != "" {
			parts = append(parts, strings.TrimSpace(part))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return slugify(parts[len(parts)-1])
}

func slugFromCompanyURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	host := strings.Trim(strings.ToLower(parsed.Hostname()), ".")
	host = strings.TrimPrefix(host, "www.")
	if host == "" {
		return ""
	}
	return slugify(strings.Split(host, ".")[0])
}

func parseISO(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02T15:04:05.999Z", value); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, errors.New("invalid time format")
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func stringOrNil(value any) any {
	text := stringValue(value)
	if text == "" {
		return nil
	}
	return text
}

func boolValue(value any) bool {
	flag, _ := value.(bool)
	return flag
}

func sliceIfValue(value any) []string {
	text := stringValue(value)
	if text == "" {
		return []string{}
	}
	return []string{text}
}

func timeValue(value any) time.Time {
	parsed, _ := value.(time.Time)
	return parsed
}

func strconvItoa(value int) string {
	return strconv.Itoa(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
