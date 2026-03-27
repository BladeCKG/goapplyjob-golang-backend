package workable

import (
	"encoding/json"
	"errors"
	"goapplyjob-golang-backend/internal/normalize/employmentnorm"
	"goapplyjob-golang-backend/internal/normalize/locationnorm"
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

func ParseRawHTML(_ string, _ string) map[string]any {
	return map[string]any{}
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
	title, _ := item["title"].(string)
	company, _ := item["company"].(map[string]any)
	location, _ := item["location"].(map[string]any)
	locationItems, _ := item["locations"].([]any)
	language, _ := item["language"].(string)
	workplace, _ := item["workplace"].(string)
	employmentType, _ := item["employmentType"].(string)

	locationCity := stripRemotePrefix(stringValue(location["city"]))
	locationState := stripRemotePrefix(stringValue(location["subregion"]))
	if isCountryLike(locationCity) {
		locationCity = ""
	}
	if isCountryLike(locationState) {
		locationState = ""
	}
	locationCountry := stringValue(location["countryName"])
	normalizedLocationState := normalizeKnownUSState(locationState)
	normalizedLocationCountry := locationnorm.NormalizeCountryName(locationCountry)

	locationParts := []string{}
	for _, entry := range locationItems {
		value := normalizeLocationLabel(stringValue(entry))
		if value == "" || strings.EqualFold(value, "TELECOMMUTE") || isRemoteToken(value) {
			continue
		}
		locationParts = append(locationParts, value)
	}
	if len(locationParts) == 0 {
		for _, value := range []string{locationCity, normalizedLocationState, normalizedLocationCountry} {
			if value != "" {
				locationParts = append(locationParts, value)
			}
		}
	}
	isEntry, isJunior, isMid, isSenior, isLead := inferSeniority(title)
	companyTitle := stringValue(company["title"])
	companySlug := slugify(companyTitle)
	jobSlug := firstNonEmpty(
		slugify(title),
		slugFromURLPath(urlValue),
	)
	if jobSlug == "" {
		jobSlug = "workable-job"
	}
	requiredLanguages := []string{}
	if language != "" && !isNullLikeToken(language) {
		requiredLanguages = []string{language}
	}
	locationUSStates := []string{}
	if normalizedLocationState != "" && !isNullLikeToken(locationState) {
		locationUSStates = []string{normalizedLocationState}
	}
	locationCountries := []string{}
	if normalizedLocationCountry != "" {
		locationCountries = []string{normalizedLocationCountry}
	}
	return map[string]any{
		"id":                           intOrStringOrNil(item["id"]),
		"created_at":                   postDate.UTC().Format(time.RFC3339Nano),
		"validUntilDate":               nil,
		"dateDeleted":                  nil,
		"descriptionLanguage":          stringOrNil(language),
		"roleTitle":                    stringOrNil(title),
		"roleDescription":              item["description"],
		"roleRequirements":             item["requirementsSection"],
		"benefits":                     item["benefitsSection"],
		"jobDescriptionSummary":        item["socialSharingDescription"],
		"twoLineJobDescriptionSummary": item["socialSharingDescription"],
		"url":                          urlValue,
		"slug":                         jobSlug,
		"employmentType":               employmentnorm.NormalizeEmploymentTypeString(employmentType),
		"location":                     stringOrNil(strings.Join(locationParts, ", ")),
		"locationType":                 stringOrNil(workplace),
		"locationCity":                 stringOrNil(locationCity),
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
		"requiredLanguages": requiredLanguages,
		"locationUSStates":  locationUSStates,
		"locationCountries": locationCountries,
		"techStack":         []string{},
		"salaryRange":       map[string]any{},
		"company": map[string]any{
			"id":                   namespacedCompanyID(company["id"]),
			"name":                 stringOrNil(companyTitle),
			"slug":                 stringOrNil(companySlug),
			"tagline":              nil,
			"foundedYear":          nil,
			"homePageURL":          stringOrNil(company["website"]),
			"linkedInURL":          nil,
			"employeeRange":        nil,
			"profilePicURL":        stringOrNil(company["image"]),
			"fundingData":          []any{},
			"industrySpecialities": nil,
		},
	}
}

func isRemoteToken(value string) bool {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), " ")
	normalized = strings.TrimSpace(normalized)
	return normalized == "remote" || strings.HasPrefix(normalized, "remote ")
}

func isNullLikeToken(value string) bool {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), " ")
	normalized = strings.TrimSpace(normalized)
	switch normalized {
	case "null", "none", "na", "n a", "unknown":
		return true
	default:
		return false
	}
}

func stripRemotePrefix(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	parts := strings.Split(value, ",")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || isRemoteToken(part) || isNullLikeToken(part) {
			continue
		}
		filtered = append(filtered, part)
	}
	if len(filtered) == 0 {
		return ""
	}
	return filtered[0]
}

func isCountryLike(value string) bool {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), " ")
	normalized = strings.TrimSpace(normalized)
	switch normalized {
	case "us", "usa", "united states", "united states of america":
		return true
	default:
		return false
	}
}

func normalizeLocationLabel(value string) string {
	if value == "" {
		return ""
	}
	parts := strings.Split(value, ",")
	normalizedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if normalizedState := normalizeKnownUSState(part); normalizedState != "" {
			normalizedParts = append(normalizedParts, normalizedState)
			continue
		}
		if normalizedCountry := locationnorm.NormalizeCountryName(part); normalizedCountry != "" {
			normalizedParts = append(normalizedParts, normalizedCountry)
			continue
		}
		normalizedParts = append(normalizedParts, part)
	}
	return strings.Join(normalizedParts, ", ")
}

func normalizeKnownUSState(value string) string {
	normalized := locationnorm.NormalizeUSStateName(value)
	if normalized == "" {
		return ""
	}
	for _, state := range locationnorm.USStateNames() {
		if state == normalized {
			return normalized
		}
	}
	return ""
}

func intOrStringOrNil(value any) any {
	switch item := value.(type) {
	case int:
		return item
	case int32:
		return int(item)
	case int64:
		return int(item)
	case float64:
		return int(item)
	case string:
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			return nil
		}
		return trimmed
	default:
		return nil
	}
}

func stringIDOrNil(value any) any {
	switch item := value.(type) {
	case float64:
		return strconv.FormatInt(int64(item), 10)
	case int:
		return strconv.Itoa(item)
	case int64:
		return strconv.FormatInt(item, 10)
	default:
		return item
	}
}

func namespacedCompanyID(value any) any {
	raw, _ := stringIDOrNil(value).(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if strings.HasPrefix(raw, Source+"_") {
		return raw
	}
	return Source + "_" + raw
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
	_, hasDirector := tokens["director"]
	_, hasChief := tokens["chief"]
	_, hasManager := tokens["manager"]
	isEntry := hasEntry || hasIntern
	isJunior := hasJunior || hasJr
	isSenior := hasSenior || hasSr
	isLead := hasLead || hasPrincipal || hasStaff || hasHead || hasDirector || hasChief || hasManager
	isMid := !(isEntry || isJunior || isSenior || isLead)
	return isEntry, isJunior, isMid, isSenior, isLead
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
	candidate := parts[len(parts)-1]
	if regexp.MustCompile(`^\d+$`).MatchString(candidate) && len(parts) >= 2 {
		candidate = parts[len(parts)-2]
	}
	candidate = regexp.MustCompile(`-\d+$`).ReplaceAllString(candidate, "")
	slug := slugify(candidate)
	if slug == "" || !regexp.MustCompile(`[a-z]`).MatchString(slug) {
		return ""
	}
	return slug
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
