package remotive

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/locationnorm"
)

const (
	Source      = "remotive"
	PayloadType = "delta"
)

var (
	scriptLDPattern = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/ld\+json['"][^>]*>(.*?)</script>`)
	tagPattern      = regexp.MustCompile(`(?is)<[^>]+>`)
	descItemPattern = regexp.MustCompile(`(?is)<(p|li)([^>]*)>(.*?)</(?:p|li)>`)
)

type sitemapURL struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

func ToTargetJobURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return rawURL
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func ParseRawHTML(htmlText, sourceURL string) map[string]any {
	jobPosting := extractJobPostingLD(htmlText)
	if len(jobPosting) == 0 {
		return map[string]any{}
	}
	locationCountries := extractLocationCountries(jobPosting["applicantLocationRequirements"])
	if len(locationCountries) > 0 && !containsIgnoreCase(locationCountries, "United States") {
		return map[string]any{"_skip_for_non_us": true, "locationCountries": locationCountries}
	}
	postedAt := extractPublicationDate(htmlText)
	if postedAt == "" {
		postedAt = parseISO(stringValue(jobPosting["datePosted"]))
	}
	descriptionHTML := stringValue(jobPosting["description"])
	descriptionSections := extractDescriptionSections(descriptionHTML)
	roleDescription := descriptionHTML
	roleDescriptionText := firstNonEmpty(descriptionSections["role_description"], stringValue(toPlainText(descriptionHTML)))
	roleRequirements := nilIfEmpty(descriptionSections["requirements"])
	benefitsText := nilIfEmpty(descriptionSections["benefits"])
	companyTagline := nilIfEmpty(descriptionSections["company_description"])
	externalID := stringValue(extractExternalID(stringValue(jobPosting["url"]), sourceURL, jobPosting["identifier"]))
	return map[string]any{
		"id":                           nilIfEmpty(externalID),
		"url":                          firstNonEmpty(stringValue(jobPosting["url"]), sourceURL),
		"created_at":                   postedAt,
		"validUntilDate":               nilIfEmpty(parseISO(stringValue(jobPosting["validThrough"]))),
		"roleTitle":                    normalizeTitle(stringValue(jobPosting["title"])),
		"roleDescription":              nilIfEmpty(roleDescription),
		"roleRequirements":             roleRequirements,
		"benefits":                     benefitsText,
		"jobDescriptionSummary":        nilIfEmpty(trimDescriptionSummary(roleDescriptionText)),
		"twoLineJobDescriptionSummary": nilIfEmpty(trimDescriptionSummary(roleDescriptionText)),
		"descriptionLanguage":          "en",
		"employmentType":               normalizeEmploymentType(stringValue(jobPosting["employmentType"])),
		"locationType":                 "remote",
		"locationCountries":            locationCountries,
		"salaryRange":                  parseSalaryRange(jobPosting["baseSalary"]),
		"company":                      parseCompany(jobPosting["hiringOrganization"], externalID, companyTagline),
	}
}

func ParseImportRows(payloadText string) ([]map[string]any, int) {
	var payload []map[string]any
	if err := json.Unmarshal([]byte(payloadText), &payload); err != nil {
		return nil, 1
	}
	rows := make([]map[string]any, 0, len(payload))
	skipped := 0
	for _, item := range payload {
		rowURL := strings.TrimSpace(stringValue(item["url"]))
		if rowURL == "" {
			skipped++
			continue
		}
		rawDate := firstNonEmpty(stringValue(item["scrapt_Date"]), stringValue(item["scraped_at"]))
		postDate, err := normalizeTime(rawDate)
		if err != nil {
			skipped++
			continue
		}
		rows = append(rows, map[string]any{"url": rowURL, "post_date": postDate})
	}
	return rows, skipped
}

func SerializeImportRows(rows []map[string]any) string {
	payload := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		rowURL := strings.TrimSpace(stringValue(row["url"]))
		postDate, _ := row["post_date"].(time.Time)
		if rowURL == "" || postDate.IsZero() {
			continue
		}
		payload = append(payload, map[string]any{
			"url":         rowURL,
			"scrapt_Date": postDate.UTC().Format(time.RFC3339Nano),
		})
	}
	raw, _ := json.Marshal(payload)
	return string(raw)
}

func ParseSitemapRows(xmlText string) ([]map[string]any, int) {
	type xmlURLSet struct {
		URLs []sitemapURL `xml:"url"`
	}
	var doc xmlURLSet
	if err := xml.Unmarshal([]byte(xmlText), &doc); err != nil {
		return nil, 1
	}
	rows := make([]map[string]any, 0, len(doc.URLs))
	skipped := 0
	for _, item := range doc.URLs {
		rowURL := strings.TrimSpace(item.Loc)
		if rowURL == "" {
			skipped++
			continue
		}
		postDate, err := normalizeTime(strings.TrimSpace(item.LastMod))
		if err != nil {
			skipped++
			continue
		}
		rows = append(rows, map[string]any{"url": rowURL, "post_date": postDate})
	}
	return rows, skipped
}

func extractJobPostingLD(htmlText string) map[string]any {
	for _, match := range scriptLDPattern.FindAllStringSubmatch(htmlText, -1) {
		raw := strings.TrimSpace(match[1])
		if raw == "" {
			continue
		}
		var payload any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue
		}
		if item, ok := payload.(map[string]any); ok {
			if strings.EqualFold(stringValue(item["@type"]), "JobPosting") {
				return item
			}
			if graph, ok := item["@graph"].([]any); ok {
				for _, node := range graph {
					candidate, _ := node.(map[string]any)
					if strings.EqualFold(stringValue(candidate["@type"]), "JobPosting") {
						return candidate
					}
				}
			}
		}
	}
	return map[string]any{}
}

func extractLocationCountries(value any) []string {
	out := []string{}
	seen := map[string]struct{}{}
	appendCountry := func(raw string) {
		country := locationnorm.NormalizeCountryName(raw, true)
		if country == "" {
			return
		}
		key := strings.ToLower(country)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, country)
	}
	switch item := value.(type) {
	case map[string]any:
		appendCountry(stringValue(item["name"]))
	case []any:
		for _, entry := range item {
			obj, _ := entry.(map[string]any)
			appendCountry(stringValue(obj["name"]))
		}
	}
	return out
}

func parseCompany(value any, remotiveJobID string, tagline any) map[string]any {
	item, _ := value.(map[string]any)
	name := stringValue(item["name"])
	slug := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(name), "-")
	slug = strings.Trim(slug, "-")
	if slug == "" {
		slug = "unknown"
	}
	profilePicURL := ""
	if strings.TrimSpace(remotiveJobID) != "" {
		profilePicURL = "https://remotive.com/job_board/job/logo/" + strings.TrimSpace(remotiveJobID)
	}
	return map[string]any{
		"id":            "remotive_company_" + slug,
		"name":          stringOrNil(item["name"]),
		"slug":          slug,
		"tagline":       tagline,
		"homePageURL":   nil,
		"linkedInURL":   nil,
		"employeeRange": nil,
		"sponsorsH1B":   nil,
		"profilePicURL": nilIfEmpty(profilePicURL),
	}
}

func parseSalaryRange(value any) any {
	out := map[string]any{
		"min":        nil,
		"max":        nil,
		"salaryType": nil,
	}
	base, _ := value.(map[string]any)
	valMap, _ := base["value"].(map[string]any)
	minValue, minOK := parseFloat(valMap["minValue"])
	maxValue, maxOK := parseFloat(valMap["maxValue"])
	if minOK {
		out["min"] = minValue
	}
	if maxOK {
		out["max"] = maxValue
	}
	if minValue == 0 {
		out["min"] = nil
	}
	if maxValue == 0 {
		out["max"] = nil
	}
	if strings.TrimSpace(stringValue(valMap["unitText"])) != "" {
		out["salaryType"] = strings.TrimSpace(stringValue(valMap["unitText"]))
	}
	if out["min"] == nil && out["max"] == nil {
		return nil
	}
	return out
}

func normalizeEmploymentType(value string) any {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "-")
	if normalized == "" {
		return nil
	}
	return normalized
}

func toPlainText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	text := html.UnescapeString(tagPattern.ReplaceAllString(value, " "))
	text = strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(text, " "))
	if text == "" {
		return nil
	}
	return text
}

func normalizeTitle(value string) any {
	title := strings.TrimSpace(value)
	if title == "" {
		return nil
	}
	title = regexp.MustCompile(`(?i)^\[hiring\]\s*`).ReplaceAllString(title, "")
	title = regexp.MustCompile(`\s+@[^@]+$`).ReplaceAllString(title, "")
	title = strings.TrimSpace(title)
	if title == "" {
		return nil
	}
	return title
}

func extractDescriptionSections(value string) map[string]string {
	sections := map[string][]string{
		"role_description":    {},
		"requirements":        {},
		"benefits":            {},
		"company_description": {},
	}
	active := "role_description"
	for _, match := range descItemPattern.FindAllStringSubmatch(value, -1) {
		tag := strings.ToLower(strings.TrimSpace(match[1]))
		attrs := strings.ToLower(match[2])
		text := normalizeDescText(match[3])
		if text == "" {
			continue
		}
		if strings.Contains(strings.ToLower(text), "this description is a summary of our understanding") {
			continue
		}
		if tag == "p" && strings.Contains(attrs, "h2") && strings.Contains(attrs, "tw-mt-4") && strings.Contains(attrs, "remotive-text-bigger") {
			if mapped := mapDescriptionHeading(text); mapped != "" {
				active = mapped
				continue
			}
		}
		sections[active] = append(sections[active], text)
	}
	out := map[string]string{}
	for key, lines := range sections {
		out[key] = joinUniqueLines(lines)
	}
	return out
}

func normalizeDescText(value string) string {
	plain := html.UnescapeString(tagPattern.ReplaceAllString(value, " "))
	return strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(plain, " "))
}

func mapDescriptionHeading(value string) string {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(value), " ")
	normalized = strings.TrimSpace(normalized)
	switch normalized {
	case "role description", "job description", "role responsibilities", "responsibilities":
		return "role_description"
	case "qualifications", "qualification", "requirements", "requirement":
		return "requirements"
	case "benefits", "benefit", "perks":
		return "benefits"
	case "company description", "about company", "about us", "about the company":
		return "company_description"
	default:
		return ""
	}
}

func joinUniqueLines(lines []string) string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, line := range lines {
		key := strings.ToLower(strings.TrimSpace(line))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, strings.TrimSpace(line))
	}
	return strings.Join(out, "\n")
}

func trimDescriptionSummary(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if len(trimmed) <= 280 {
		return trimmed
	}
	return trimmed[:280] + "..."
}

func extractExternalID(jobURL, sourceURL string, identifier any) any {
	identifierMap, _ := identifier.(map[string]any)
	value := strings.TrimSpace(stringValue(identifierMap["value"]))
	if value != "" {
		if match := regexp.MustCompile(`(\d+)$`).FindStringSubmatch(value); len(match) == 2 {
			return match[1]
		}
		return value
	}
	target := firstNonEmpty(jobURL, sourceURL)
	if match := regexp.MustCompile(`-(\d+)(?:[/?#].*)?$`).FindStringSubmatch(target); len(match) == 2 {
		return match[1]
	}
	return nil
}

func extractPublicationDate(htmlText string) string {
	match := regexp.MustCompile(`(?is)data-publication-date\s*=\s*['"]([^'"]+)['"]`).FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return ""
	}
	return parseISO(match[1])
}

func parseISO(value string) string {
	if parsed, err := normalizeTime(value); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

func normalizeTime(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value)); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(value)); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, errors.New("invalid time format")
}

func parseFloat(value any) (float64, bool) {
	switch item := value.(type) {
	case float64:
		return item, true
	case int:
		return float64(item), true
	default:
		return 0, false
	}
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

func containsIgnoreCase(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func nilIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}
