package flexjobs

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"goapplyjob-golang-backend/internal/normalize/employmentnorm"
	"goapplyjob-golang-backend/internal/normalize/locationnorm"
	"goapplyjob-golang-backend/internal/sources/parseerr"
	"html"
	"math"
	"regexp"
	"strings"
	"time"
)

const (
	Source      = "flexjobs"
	PayloadType = "delta_xml"
)

var (
	nextDataPattern = regexp.MustCompile(`(?is)<script[^>]*id=['"]__NEXT_DATA__['"][^>]*type=['"]application/json['"][^>]*>(.*?)</script>`)
)

type sitemapURL struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

func ToTargetJobURL(rawURL string) string { return rawURL }

func ParseImportRows(bodyText string) ([]map[string]any, int) {
	type xmlURLSet struct {
		URLs []sitemapURL `xml:"url"`
	}
	var doc xmlURLSet
	if err := xml.Unmarshal([]byte(bodyText), &doc); err != nil {
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
		rows = append(rows, map[string]any{
			"url":       rowURL,
			"post_date": postDate,
		})
	}
	return rows, skipped
}

func SerializeImportRows(rows []map[string]any) string {
	type pair struct {
		url      string
		postDate time.Time
	}
	ordered := make([]pair, 0, len(rows))
	for _, row := range rows {
		rowURL, _ := row["url"].(string)
		postDate, _ := row["post_date"].(time.Time)
		if strings.TrimSpace(rowURL) == "" || postDate.IsZero() {
			continue
		}
		ordered = append(ordered, pair{url: strings.TrimSpace(rowURL), postDate: postDate.UTC()})
	}
	parts := []string{`<?xml version="1.0" encoding="UTF-8"?>`, `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`}
	for _, row := range ordered {
		parts = append(parts,
			"  <url>",
			"    <loc>"+escapeXML(row.url)+"</loc>",
			"    <lastmod>"+row.postDate.Format(time.RFC3339Nano)+"</lastmod>",
			"  </url>",
		)
	}
	parts = append(parts, `</urlset>`)
	return strings.Join(parts, "\n") + "\n"
}

func ParseSitemapRows(xmlText string, now time.Time) ([]map[string]any, int) {
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
			postDate = now.UTC()
		}
		rows = append(rows, map[string]any{
			"url":       rowURL,
			"post_date": postDate,
		})
	}
	return rows, skipped
}

func ParseRawHTML(htmlText, sourceURL string) (map[string]any, error) {
	match := nextDataPattern.FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return nil, parseerr.Retry("missing_next_data")
	}
	raw := strings.TrimSpace(match[1])
	if raw == "" {
		return nil, parseerr.Retry("empty_next_data")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, parseerr.Retry("invalid_next_data")
	}

	props, _ := payload["props"].(map[string]any)
	pageProps, _ := props["pageProps"].(map[string]any)
	jobList, _ := pageProps["jobList"].(map[string]any)
	if jobList == nil {
		return nil, parseerr.Retry("missing_job_list")
	}

	applyURL := strings.TrimSpace(stringValue(jobList["applyURL"]))
	if applyURL == "" {
		return nil, parseerr.Skip("missing_apply_url")
	}

	companyPayload := parseCompany(jobList["company"])
	if companyPayload == nil {
		return nil, parseerr.Skip("missing_company")
	}

	roleTitle := strings.TrimSpace(stringValue(jobList["title"]))
	roleDescription := strings.TrimSpace(stringValue(jobList["description"]))
	jobSummary := strings.TrimSpace(stringValue(jobList["jobSummary"]))
	createdAt := normalizeISO(firstNonEmptyAny(jobList["postedDate"], jobList["createdOn"]))
	validUntil := normalizeISO(jobList["expireOn"])
	slug := stripExternalIDFromSlug(strings.TrimSpace(stringValue(jobList["slug"])))

	locationCities := normalizeCities(jobList["cities"])
	locationStates := normalizeStates(jobList["states"])
	locationCountries := normalizeCountries(jobList["countries"])
	if len(locationCountries) == 0 {
		locationCountries = normalizeCountriesFromLocations(jobList["locations"])
	}
	if len(locationCountries) == 0 {
		locationCountries = []string{locationnorm.NormalizeRegionName("Worldwide")}
	}
	locationType := normalizeRemoteOption(jobList["remoteOptions"])
	if locationType == "" {
		locationType = inferLocationTypeFromDisplayLocations(jobList["displayLocations"])
	}
	employmentType := normalizeEmployment(jobList["jobSchedules"], jobList["jobTypes"])
	isEntry, isJunior, isMid, isSenior, isLead := inferSeniority(roleTitle, jobList["careerLevel"])

	out := map[string]any{
		"id":                           nilIfEmpty(stringValue(jobList["id"])),
		"url":                          nilIfEmpty(applyURL),
		"slug":                         nilIfEmpty(slug),
		"created_at":                   nilIfEmpty(createdAt),
		"validUntilDate":               nilIfEmpty(validUntil),
		"roleTitle":                    nilIfEmpty(roleTitle),
		"roleDescription":              nilIfEmpty(roleDescription),
		"jobDescriptionSummary":        nilIfEmpty(jobSummary),
		"twoLineJobDescriptionSummary": nilIfEmpty(jobSummary),
		"employmentType":               nilIfEmpty(employmentType),
		"locationType":                 nilIfEmpty(locationType),
		"locationCity":                 nilIfEmpty(firstString(locationCities)),
		"locationUSStates":             locationStates,
		"locationCountries":            locationCountries,
		"isEntryLevel":                 isEntry,
		"isJunior":                     isJunior,
		"isMidLevel":                   isMid,
		"isSenior":                     isSenior,
		"isLead":                       isLead,
		"company":                      companyPayload,
		"salaryRange":                  buildSalaryRange(jobList["salaryRanges"]),
		"educationRequirementsCredentialCategory": normalizeEducationLevel(firstString(anyStringSlice(jobList["educationLevels"]))),
		"descriptionLanguage":                     "en",
	}
	if travelRequired := nilIfEmpty(stringValue(jobList["travelRequired"])); travelRequired != nil {
		out["travelRequired"] = travelRequired
	}
	if locationPostalCode := nilIfEmpty(stringValue(jobList["postalCode"])); locationPostalCode != nil {
		out["locationPostalCode"] = locationPostalCode
	}
	_ = sourceURL
	return out, nil
}

func parseCompany(value any) map[string]any {
	item, _ := value.(map[string]any)
	if item == nil {
		return nil
	}
	name := strings.TrimSpace(stringValue(item["name"]))
	slug := strings.TrimSpace(stringValue(item["slug"]))
	companyID := strings.TrimSpace(stringValue(item["companyId"]))
	if name == "" && slug == "" && companyID == "" {
		return nil
	}
	if slug == "" {
		slug = slugify(name)
	}
	logo := normalizeFlexJobsURL(firstNonEmptyAny(item["logo"], item["squareLogo"]))
	descriptionHTML := stringValue(item["description"])
	tagline := summarizeHTML(descriptionHTML, 180)
	return map[string]any{
		"id":            nilIfEmpty(namespacedCompanyID(companyID, slug, name)),
		"name":          nilIfEmpty(name),
		"slug":          nilIfEmpty(slug),
		"tagline":       nilIfEmpty(tagline),
		"homePageURL":   nilIfEmpty(stringValue(item["website"])),
		"linkedInURL":   nilIfEmpty(stringValue(item["linkedinURL"])),
		"employeeRange": nil,
		"sponsorsH1B":   nil,
		"profilePicURL": nilIfEmpty(logo),
	}
}

func namespacedCompanyID(companyID, slug, name string) string {
	raw := strings.TrimSpace(companyID)
	if raw == "" {
		raw = strings.TrimSpace(slug)
	}
	if raw == "" {
		raw = slugify(name)
	}
	if raw == "" {
		return ""
	}
	return Source + "_" + raw
}

func normalizeFlexJobsURL(value any) string {
	raw := strings.TrimSpace(stringValue(value))
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if strings.HasPrefix(raw, "/") {
		return "https://www.flexjobs.com" + raw
	}
	return "https://www.flexjobs.com/" + raw
}

func normalizeCountries(value any) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, raw := range anyStringSlice(value) {
		normalized := locationnorm.NormalizeCountryName(raw)
		if normalized == "" {
			continue
		}
		key := strings.ToLower(normalized)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func normalizeCountriesFromLocations(value any) []string {
	items, _ := value.([]any)
	out := []string{}
	seen := map[string]struct{}{}
	for _, item := range items {
		location, _ := item.(map[string]any)
		if location == nil {
			continue
		}
		normalized := locationnorm.NormalizeCountryName(stringValue(location["country"]))
		if normalized == "" {
			continue
		}
		key := strings.ToLower(normalized)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func normalizeStates(value any) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, raw := range anyStringSlice(value) {
		normalized := locationnorm.NormalizeUSStateName(raw)
		if normalized == "" {
			continue
		}
		key := strings.ToLower(normalized)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func normalizeCities(value any) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, raw := range anyStringSlice(value) {
		clean := strings.TrimSpace(raw)
		if clean == "" {
			continue
		}
		key := strings.ToLower(clean)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, clean)
	}
	return out
}

func normalizeRemoteOption(value any) string {
	for _, raw := range anyStringSlice(value) {
		token := strings.ToLower(strings.TrimSpace(raw))
		switch token {
		case "100% remote work", "remote":
			return "remote"
		case "hybrid remote work", "hybrid":
			return "hybrid"
		case "partial remote work":
			return "partial_remote"
		}
	}
	return ""
}

func inferLocationTypeFromDisplayLocations(value any) string {
	for _, raw := range anyStringSlice(value) {
		lowered := strings.ToLower(raw)
		switch {
		case strings.Contains(lowered, "remote"):
			return "remote"
		case strings.Contains(lowered, "hybrid"):
			return "hybrid"
		}
	}
	return ""
}

func normalizeEmployment(schedules any, jobTypes any) string {
	if employmentType := employmentnorm.NormalizeEmploymentTypeAny(schedules); employmentType != nil {
		if value, ok := employmentType.(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	if employmentType := employmentnorm.NormalizeEmploymentTypeAny(jobTypes); employmentType != nil {
		if value, ok := employmentType.(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func buildSalaryRange(value any) any {
	items, _ := value.([]any)
	if len(items) == 0 {
		return nil
	}
	first, _ := items[0].(map[string]any)
	if first == nil {
		return nil
	}
	minValue := floatValue(first["salaryMin"])
	maxValue := floatValue(first["salaryMax"])
	currencyCode := strings.ToUpper(strings.TrimSpace(stringValue(first["salaryCurrency"])))
	salaryType := salaryTypeFromUnit(stringValue(first["salaryUnit"]))
	if minValue == 0 && maxValue == 0 && currencyCode == "" && salaryType == "" {
		return nil
	}
	payload := map[string]any{}
	if minValue > 0 {
		payload["min"] = normalizeSalaryValue(minValue)
	}
	if maxValue > 0 {
		payload["max"] = normalizeSalaryValue(maxValue)
	}
	if salaryType != "" {
		payload["salaryType"] = salaryType
	}
	if currencyCode != "" {
		payload["currencyCode"] = currencyCode
	}
	if currencyCode == "USD" {
		if minValue > 0 {
			payload["minSalaryAsUSD"] = normalizeSalaryValue(minValue)
		}
		if maxValue > 0 {
			payload["maxSalaryAsUSD"] = normalizeSalaryValue(maxValue)
		}
		payload["currencySymbol"] = "$"
	}
	return payload
}

func salaryTypeFromUnit(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "annually", "annual", "year", "yearly":
		return "per year"
	case "monthly", "month":
		return "per month"
	case "weekly", "week":
		return "per week"
	case "daily", "day":
		return "per day"
	case "hourly", "hour":
		return "per hour"
	default:
		return ""
	}
}

func normalizeEducationLevel(value string) any {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "bachelor's/undergraduate degree", "bachelors/undergraduate degree", "bachelor degree":
		return "bachelor degree"
	case "master's degree", "masters degree":
		return "master degree"
	case "high school diploma", "high school degree":
		return "high school degree"
	case "professional certificate":
		return "professional certificate"
	case "no education specified", "":
		return nil
	default:
		return nilIfEmpty(value)
	}
}

func inferSeniority(title string, careerLevels any) (bool, bool, bool, bool, bool) {
	text := strings.ToLower(strings.TrimSpace(title))
	levels := strings.ToLower(strings.Join(anyStringSlice(careerLevels), " "))
	isEntry := strings.Contains(text, "entry") || strings.Contains(levels, "entry")
	isJunior := strings.Contains(text, "junior") || strings.Contains(levels, "junior")
	isSenior := strings.Contains(text, "senior") || strings.Contains(levels, "experienced") || strings.Contains(levels, "senior")
	isLead := strings.Contains(text, "lead") || strings.Contains(text, "manager") || strings.Contains(text, "director") || strings.Contains(levels, "manager")
	isMid := !(isEntry || isJunior || isSenior || isLead)
	return isEntry, isJunior, isMid, isSenior, isLead
}

func stripExternalIDFromSlug(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	uuidPattern := regexp.MustCompile(`-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	return uuidPattern.ReplaceAllString(trimmed, "")
}

func slugify(value string) string {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), "-")
	return strings.Trim(normalized, "-")
}

func summarizeHTML(value string, maxLen int) string {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return ""
	}
	text := html.UnescapeString(raw)
	text = regexp.MustCompile(`(?is)<[^>]+>`).ReplaceAllString(text, " ")
	text = strings.Join(strings.Fields(text), " ")
	if maxLen > 0 && len(text) > maxLen {
		text = strings.TrimSpace(text[:maxLen])
	}
	return text
}

func normalizeISO(value any) string {
	raw := strings.TrimSpace(stringValue(value))
	if raw == "" {
		return ""
	}
	if parsed, err := normalizeTime(raw); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

func normalizeTime(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, errors.New("invalid time format")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", trimmed); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02", trimmed); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, errors.New("invalid time format")
}

func anyStringSlice(value any) []string {
	switch v := value.(type) {
	case []string:
		return append([]string(nil), v...)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text := strings.TrimSpace(stringValue(item))
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		text := strings.TrimSpace(stringValue(value))
		if text == "" {
			return nil
		}
		return []string{text}
	}
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func firstNonEmptyAny(values ...any) any {
	for _, value := range values {
		if strings.TrimSpace(stringValue(value)) != "" {
			return value
		}
	}
	return nil
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func floatValue(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}

func normalizeSalaryValue(value float64) any {
	if value == 0 {
		return 0
	}
	if value == float64(int64(value)) {
		return int64(value)
	}
	return math.Round(value*100) / 100
}

func nilIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func escapeXML(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", `'`, "&apos;")
	return replacer.Replace(html.UnescapeString(value))
}
