package remotive

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"goapplyjob-golang-backend/internal/normalize/employmentnorm"
	"goapplyjob-golang-backend/internal/normalize/locationnorm"
	"goapplyjob-golang-backend/internal/sources/currency"
	"goapplyjob-golang-backend/internal/sources/parseerr"
	"html"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	nethtml "golang.org/x/net/html"
)

const (
	Source      = "remotive"
	PayloadType = "delta_json"
)

var (
	scriptLDPattern = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/ld\+json['"][^>]*>(.*?)</script>`)
	tagPattern      = regexp.MustCompile(`(?is)<[^>]+>`)
	descItemPattern = regexp.MustCompile(`(?is)<(p|li)([^>]*)>(.*?)</(?:p|li)>`)
	anchorHrefRE    = regexp.MustCompile(`(?is)<a\b[^>]*\bhref\s*=\s*['"]([^'"]+)['"][^>]*>(.*?)</a>`)
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

func ParseRawHTML(htmlText, sourceURL string) (map[string]any, error) {
	jobPosting := extractJobPostingLD(htmlText)
	if len(jobPosting) == 0 {
		return nil, parseerr.Retry("missing_job_posting_ld")
	}
	locationCountries := extractLocationCountriesFromLocationComponent(htmlText)
	postedAt := extractPublicationDate(htmlText)
	if postedAt == "" {
		postedAt = parseISO(stringValue(jobPosting["datePosted"]))
	}
	applyURL := extractApplyURL(htmlText, sourceURL)
	descriptionHTML := stringValue(jobPosting["description"])
	descriptionSections := extractDescriptionSections(descriptionHTML)
	roleDescriptionText := stringValue(toPlainText(descriptionHTML))
	companyTagline := nilIfEmpty(descriptionSections["company_description"])
	externalID := stringValue(extractExternalID(stringValue(jobPosting["url"]), sourceURL, jobPosting["identifier"]))
	jobSlug := buildJobSlug(stringValue(jobPosting["url"]))
	roleTitle := stringValue(normalizeTitle(stringValue(jobPosting["title"])))
	isEntry, isJunior, isMid, isSenior, isLead := inferSeniority(roleTitle)
	salaryRange := extractSalaryRangeFromSummaryHTML(htmlText)
	return map[string]any{
		"id":                           nilIfEmpty(externalID),
		"url":                          firstNonEmpty(applyURL, stringValue(jobPosting["url"]), sourceURL),
		"slug":                         jobSlug,
		"created_at":                   postedAt,
		"validUntilDate":               nilIfEmpty(parseISO(stringValue(jobPosting["validThrough"]))),
		"roleTitle":                    nilIfEmpty(roleTitle),
		"occupationalCategory":         nilIfEmpty(normalizeOccupationalCategory(stringValue(jobPosting["occupationalCategory"]))),
		"roleDescription":              nilIfEmpty(descriptionHTML),
		"roleRequirements":             nil,
		"benefits":                     nil,
		"jobDescriptionSummary":        nilIfEmpty(trimDescriptionSummary(roleDescriptionText)),
		"twoLineJobDescriptionSummary": nilIfEmpty(trimDescriptionSummary(roleDescriptionText)),
		"descriptionLanguage":          "en",
		"employmentType":               normalizeEmploymentType(jobPosting["employmentType"]),
		"locationType":                 "remote",
		"locationCountries":            locationCountries,
		"isEntryLevel":                 isEntry,
		"isJunior":                     isJunior,
		"isMidLevel":                   isMid,
		"isSenior":                     isSenior,
		"isLead":                       isLead,
		"salaryRange":                  salaryRange,
		"company":                      parseCompany(jobPosting["hiringOrganization"], externalID, companyTagline),
	}, nil
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
		rawDate := stringValue(item["scrapt_Date"])
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
			// page-extract watcher keeps sitemap rows even when lastmod parsing fails
			// and applies "now" fallback at watcher level.
			postDate = time.Time{}
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
		country := locationnorm.NormalizeCountryName(raw)
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
			if raw, ok := entry.(string); ok {
				appendCountry(raw)
				continue
			}
			obj, _ := entry.(map[string]any)
			appendCountry(stringValue(obj["name"]))
		}
	case string:
		appendCountry(item)
	}
	return out
}

func extractLocationCountriesFromLocationComponent(htmlText string) []string {
	text := extractSummaryTextFromHTML(htmlText)
	if text == "" {
		return nil
	}
	locationIndex := strings.Index(strings.ToLower(text), "location:")
	if locationIndex < 0 {
		return nil
	}
	locationText := strings.TrimSpace(text[locationIndex+len("location:"):])
	if locationText == "" {
		return nil
	}
	return normalizeLocationCountries(locationText)
}

func extractSummaryTextFromHTML(htmlText string) string {
	if strings.TrimSpace(htmlText) == "" {
		return ""
	}
	doc, err := nethtml.Parse(strings.NewReader(htmlText))
	if err != nil {
		return ""
	}
	return extractSummaryTextAfterH1(doc)
}

func normalizeSummaryText(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return normalizeText(strings.Join(strings.Fields(value), " "))
}

func extractSummaryTextAfterH1(root *nethtml.Node) string {
	foundH1 := false
	summaryText := ""
	var walk func(*nethtml.Node)
	walk = func(node *nethtml.Node) {
		if summaryText != "" {
			return
		}
		if node.Type == nethtml.ElementNode && strings.EqualFold(node.Data, "h1") {
			foundH1 = true
		} else if foundH1 && node.Type == nethtml.ElementNode && strings.EqualFold(node.Data, "p") {
			text := normalizeSummaryText(textContent(node))
			if text != "" {
				summaryText = text
				return
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return summaryText
}

func normalizeLocationCountries(locationText string) []string {
	segments := strings.FieldsFunc(locationText, func(r rune) bool {
		return r == ',' || r == ';' || r == '/' || r == '|'
	})
	out := []string{}
	seen := map[string]struct{}{}
	appendValue := func(raw string) {
		candidate := strings.TrimSpace(raw)
		if candidate == "" {
			return
		}
		candidate = strings.Trim(candidate, " .:-")
		if candidate == "" {
			return
		}
		country := locationnorm.NormalizeCountryName(candidate)
		value := candidate
		if country != "" {
			value = country
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	for _, segment := range segments {
		appendValue(segment)
	}
	return out
}

func parseCompany(value any, _ string, tagline any) map[string]any {
	item, _ := value.(map[string]any)
	name := stringValue(item["name"])
	slug := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(name), "-")
	slug = strings.Trim(slug, "-")
	if slug == "" {
		slug = "unknown"
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
		"profilePicURL": nil,
	}
}

func parseSalaryRange(value any) any {
	out := map[string]any{"min": nil, "max": nil, "salaryType": nil, "currencyCode": nil}
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
	salaryType := "per year"
	if unitText := strings.ToLower(strings.TrimSpace(stringValue(valMap["unitText"]))); unitText != "" {
		switch unitText {
		case "month":
			salaryType = "per month"
		case "hour":
			salaryType = "per hour"
		case "week":
			salaryType = "per week"
		case "day":
			salaryType = "per day"
		default:
			salaryType = "per " + unitText
		}
	}
	out["salaryType"] = salaryType
	if currency := strings.TrimSpace(stringValue(base["currency"])); currency != "" {
		out["currencyCode"] = currency
	}
	if out["min"] == nil && out["max"] == nil {
		return nil
	}
	return out
}

func normalizeText(value string) string {
	value = strings.ReplaceAll(value, "\u00a0", " ")
	return strings.TrimSpace(value)
}

func extractSalaryRangeFromSummaryHTML(htmlText string) any {
	summaryText := extractSummaryTextFromHTML(htmlText)
	if summaryText == "" {
		return nil
	}
	lower := strings.ToLower(summaryText)
	idx := strings.Index(lower, "salary:")
	if idx < 0 {
		return nil
	}
	salaryText := strings.TrimSpace(summaryText[idx+len("salary:"):])
	if locIdx := strings.Index(strings.ToLower(salaryText), "location:"); locIdx >= 0 {
		salaryText = strings.TrimSpace(salaryText[:locIdx])
	}
	salaryText = strings.TrimSpace(strings.TrimRight(salaryText, ".!?,;:- "))
	salaryText = strings.TrimSpace(strings.TrimRight(salaryText, "📍"))
	return parseSalaryRangeFromText(salaryText)
}

func parseSalaryRangeFromText(value string) any {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return nil
	}
	normalizedText := normalizeSalaryText(raw)
	normalized := strings.ToLower(normalizedText)
	matches := currency.SalaryNumberPattern.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 {
		matches = currency.SalaryNumberPattern.FindAllStringSubmatch(normalizedText, -1)
	}
	if len(matches) == 0 {
		return nil
	}
	amounts := make([]float64, 0, len(matches))
	for _, match := range matches {
		if len(match) < 4 {
			continue
		}
		amount := parseAmount(match[2], match[3])
		if amount > 0 {
			amounts = append(amounts, amount)
		}
	}
	if len(amounts) == 0 {
		return nil
	}
	salaryType := "per year"
	if strings.Contains(normalized, "per hour") || strings.Contains(normalized, "/hour") || strings.Contains(normalized, "/hr") || strings.Contains(normalized, " hourly") {
		salaryType = "per hour"
	} else if strings.Contains(normalized, "per month") || strings.Contains(normalized, "/month") || strings.Contains(normalized, "/mo") || strings.Contains(normalized, " monthly") {
		salaryType = "per month"
	} else if strings.Contains(normalized, "per week") || strings.Contains(normalized, "/week") || strings.Contains(normalized, "/wk") || strings.Contains(normalized, " weekly") {
		salaryType = "per week"
	} else if strings.Contains(normalized, "per day") || strings.Contains(normalized, "/day") || strings.Contains(normalized, " daily") {
		salaryType = "per day"
	} else if strings.Contains(normalized, "per year") || strings.Contains(normalized, "/year") || strings.Contains(normalized, "/yr") || strings.Contains(normalized, " yearly") || strings.Contains(normalized, " annual") {
		salaryType = "per year"
	}
	currencyCode, currencySymbol, _ := currency.DetectCurrency(normalized)

	minValue := amounts[0]
	maxValue := amounts[0]
	if len(amounts) > 1 {
		maxValue = amounts[1]
	}
	payload := map[string]any{
		"min":                     normalizeSalaryValue(minValue),
		"max":                     normalizeSalaryValue(maxValue),
		"salaryType":              salaryType,
		"currencyCode":            currencyCode,
		"currencySymbol":          currencySymbol,
		"salaryHumanReadableText": raw,
	}
	if currencyCode == "USD" {
		payload["minSalaryAsUSD"] = normalizeSalaryValue(minValue)
		payload["maxSalaryAsUSD"] = normalizeSalaryValue(maxValue)
	}
	return payload
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

func normalizeSalaryText(value string) string {
	text := html.UnescapeString(value)
	replacer := strings.NewReplacer(
		"â‚¬", "€",
		"в‚¬", "€",
		"Â£", "£",
		"Ã‚Â£", "£",
		"Г‚ВЈ", "£",
		"ВЈ", "£",
		"Ј", "£",
		"â‚¹", "₹",
	)
	return replacer.Replace(text)
}

func parseAmount(numberText, suffix string) float64 {
	clean := strings.TrimSpace(strings.ReplaceAll(numberText, ",", ""))
	if clean == "" {
		return 0
	}
	value, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(suffix)) {
	case "k":
		value *= 1000
	case "m":
		value *= 1000000
	}
	return value
}

func textContent(node *nethtml.Node) string {
	var b strings.Builder
	var walk func(*nethtml.Node)
	walk = func(n *nethtml.Node) {
		if n.Type == nethtml.TextNode {
			b.WriteString(n.Data)
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(node)
	return b.String()
}

func normalizeEmploymentType(value any) any {
	return employmentnorm.NormalizeEmploymentTypeAny(value)
}

func normalizeOccupationalCategory(value string) string {
	return strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(html.UnescapeString(strings.TrimSpace(value)), " "))
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
	case "role description", "job description", "role responsibilities", "responsibilities", "key responsibilities":
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

func buildJobSlug(canonicalURL string) string {
	if strings.TrimSpace(canonicalURL) != "" {
		parsed, err := url.Parse(strings.TrimSpace(canonicalURL))
		if err == nil {
			segments := []string{}
			for _, part := range strings.Split(parsed.Path, "/") {
				if strings.TrimSpace(part) != "" {
					segments = append(segments, strings.TrimSpace(part))
				}
			}
			if len(segments) > 0 {
				last := regexp.MustCompile(`-\d+$`).ReplaceAllString(segments[len(segments)-1], "")
				if value := slugFromText(last); value != "" && value != "unknown" {
					return value
				}
			}
		}
	}
	return "remotive-job"
}

func slugFromText(value string) string {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), "-")
	return strings.Trim(normalized, "-")
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

func extractApplyURL(htmlText, sourceURL string) string {
	for _, match := range anchorHrefRE.FindAllStringSubmatch(htmlText, -1) {
		if len(match) < 3 {
			continue
		}
		href := strings.TrimSpace(html.UnescapeString(match[1]))
		innerText := strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(tagPattern.ReplaceAllString(html.UnescapeString(match[2]), " "), " "))
		if !strings.EqualFold(innerText, "Apply for this position") || href == "" {
			continue
		}
		if strings.TrimSpace(sourceURL) == "" {
			return href
		}
		base, err := url.Parse(sourceURL)
		if err != nil {
			return href
		}
		target, err := url.Parse(href)
		if err != nil {
			return href
		}
		return base.ResolveReference(target).String()
	}
	return ""
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
	if parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value)); err == nil {
		dateOnly := parsed.UTC()
		now := time.Now().UTC()
		if dateOnly.Year() == now.Year() && dateOnly.YearDay() == now.YearDay() {
			return now, nil
		}
		return dateOnly, nil
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
