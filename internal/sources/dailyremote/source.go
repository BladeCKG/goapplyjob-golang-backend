package dailyremote

import (
	"encoding/json"
	"errors"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/locationnorm"
)

const (
	Source      = "dailyremote"
	PayloadType = "delta_dailyremote_json"
)

var (
	externalIDPattern     = regexp.MustCompile(`(\d+)(?:[/?#].*)?$`)
	jsonLDPattern         = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/ld\+json['"][^>]*>(.*?)</script>`)
	articlePattern        = regexp.MustCompile(`(?is)<article[^>]*class=['"][^'"]*\bcard\b[^'"]*\bjs-card\b[^'"]*['"][^>]*>(.*?)</article>`)
	hrefPattern           = regexp.MustCompile(`(?is)href=['"]([^'"]+)['"]`)
	tagPattern            = regexp.MustCompile(`(?is)<[^>]+>`)
	relTimePattern        = regexp.MustCompile(`(?i)(\d+)\s*(min|mins|minute|minutes|hour|hours|day|days|week|weeks)\s*ago`)
	salaryNumberPattern   = regexp.MustCompile(`([$€£])?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?`)
	salaryHintPattern     = regexp.MustCompile(`(?i)(?:[$€£]|\b(?:usd|eur|gbp|salary|compensation|hour|hr|day|week|month|year|annual|annum|monthly|weekly|daily)\b|/[a-z]+)`)
	experienceHintPattern = regexp.MustCompile(`(?i)\b(?:exp|experience|yr|yrs|year|years)\b`)
	headContainerPattern  = regexp.MustCompile(`(?is)<div[^>]*class=['"][^'"]*\bjob_head_info_container\b[^'"]*['"][^>]*>(.*?)</div>`)
	inlineHeadItemPattern = regexp.MustCompile(`(?is)<div[^>]*class=['"][^'"]*\binline-flex\b[^'"]*\bitems-center\b[^'"]*['"][^>]*>(.*?)</div>`)
	aiSummaryPattern      = regexp.MustCompile(`(?is)<h3[^>]*>\s*AI\s*Summary\s*</h3>.*?<div[^>]*class=['"][^'"]*\bpx-3\b[^'"]*\bpy-3\b[^'"]*['"][^>]*>(.*?)</div>`)
	companyProfilePattern = regexp.MustCompile(`(?is)<div[^>]*class=['"][^'"]*\bdetailed-job-company-profile\b[^'"]*['"][^>]*>(.*?)</div>`)
	companyTagPattern     = regexp.MustCompile(`(?is)<span[^>]*class=['"][^'"]*\btag\b[^'"]*['"][^>]*>(.*?)</span>`)
)

var resolveRedirectURLDailyRemoteFunc = resolveRedirectURLDailyRemote

func ExtractExternalIDFromURL(rawURL string) int {
	match := externalIDPattern.FindStringSubmatch(strings.TrimSpace(rawURL))
	if len(match) < 2 {
		return 0
	}
	id, _ := strconv.Atoi(strings.TrimSpace(match[1]))
	return id
}

func ToTargetJobURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return rawURL
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	normalized := parsed.String()
	externalID := ExtractExternalIDFromURL(normalized)
	if externalID > 0 {
		return "https://dailyremote.com/apply/" + strconv.Itoa(externalID)
	}
	return normalized
}

func ExtractJobListings(htmlText, pageURL string, scrapedAt time.Time) []map[string]any {
	matches := articlePattern.FindAllStringSubmatch(htmlText, -1)
	if len(matches) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(matches))
	for _, match := range matches {
		body := match[1]
		link := ""
		if href := hrefPattern.FindStringSubmatch(body); len(href) >= 2 {
			link = strings.TrimSpace(href[1])
		}
		if link == "" {
			continue
		}
		listingURL := resolveURL(pageURL, link)
		postDate := extractRelativePostDate(body, scrapedAt)
		if postDate.IsZero() {
			continue
		}
		externalID := ExtractExternalIDFromURL(listingURL)
		out = append(out, map[string]any{
			"url":         listingURL,
			"post_date":   postDate,
			"external_id": externalID,
		})
	}
	return out
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
		postDateRaw := strings.TrimSpace(stringValue(item["post_date"]))
		if rowURL == "" || postDateRaw == "" {
			skipped++
			continue
		}
		postDate, err := parseISO(postDateRaw)
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
			"url":       rowURL,
			"post_date": postDate.UTC().Format(time.RFC3339Nano),
		})
	}
	data, _ := json.Marshal(payload)
	return string(data)
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
	externalID := extractExternalID(jobPosting, sourceURL)
	roleTitle := normalizeText(jobPosting["title"])
	roleDescription := decodeHTMLText(stringValue(jobPosting["description"]))
	isEntry, isJunior, isMid, isSenior, isLead := inferSeniority(roleTitle)
	createdAt := normalizeISO(jobPosting["datePosted"])
	validUntil := normalizeISO(jobPosting["validThrough"])
	locationType := normalizeLocationType(jobPosting["jobLocationType"])
	company := parseCompany(jobPosting["hiringOrganization"])
	headSalaryText, aiSummary := extractHeadInfoFromHTML(htmlText)
	companyEnrichment := extractCompanyEnrichmentFromHTML(htmlText)
	if employeeRange := stringValue(companyEnrichment["employeeRange"]); employeeRange != "" {
		company["employeeRange"] = employeeRange
	}
	if industry, ok := companyEnrichment["industrySpecialities"]; ok {
		company["industrySpecialities"] = industry
	}
	targetURL := resolveRedirectURLDailyRemoteFunc(ToTargetJobURL(firstNonEmpty(stringValue(jobPosting["url"]), sourceURL)))
	salaryPayload := parseSalaryRangeFromText(headSalaryText)
	payload := map[string]any{
		"id":                   nilIfEmpty(strconv.Itoa(externalID)),
		"url":                  targetURL,
		"slug":                 buildJobSlug(sourceURL, roleTitle),
		"created_at":           createdAt,
		"validUntilDate":       nilIfEmpty(validUntil),
		"roleTitle":            nilIfEmpty(roleTitle),
		"occupationalCategory": nilIfEmpty(normalizeText(jobPosting["occupationalCategory"])),
		"roleDescription":      nilIfEmpty(roleDescription),
		"roleRequirements":     nil,
		"benefits":             nilIfEmpty(decodeHTMLText(stringValue(jobPosting["jobBenefits"]))),
		"descriptionLanguage":  "en",
		"employmentType":       normalizeEmploymentType(jobPosting["employmentType"]),
		"locationType":         locationType,
		"locationCountries":    locationCountries,
		"isEntryLevel":         isEntry,
		"isJunior":             isJunior,
		"isMidLevel":           isMid,
		"isSenior":             isSenior,
		"isLead":               isLead,
		"company":              company,
	}
	if strings.Contains(targetURL, "dailyremote.com/apply/") {
		payload["_skip_for_retry"] = true
		payload["_skip_reason"] = "dailyremote_unresolved_url"
	}
	if strings.TrimSpace(aiSummary) != "" {
		payload["jobDescriptionSummary"] = aiSummary
		payload["twoLineJobDescriptionSummary"] = aiSummary
	}
	if salaryPayload != nil {
		payload["salaryRange"] = salaryPayload
	}
	return payload
}

func extractJobPostingLD(htmlText string) map[string]any {
	matches := jsonLDPattern.FindAllStringSubmatch(htmlText, -1)
	for _, match := range matches {
		raw := strings.TrimSpace(match[1])
		if raw == "" {
			continue
		}
		var payload any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue
		}
		for _, node := range flattenLD(payload) {
			item, _ := node.(map[string]any)
			if strings.EqualFold(stringValue(item["@type"]), "JobPosting") {
				return item
			}
		}
	}
	return map[string]any{}
}

func flattenLD(payload any) []any {
	switch item := payload.(type) {
	case []any:
		return item
	case map[string]any:
		if graph, ok := item["@graph"].([]any); ok {
			return graph
		}
		return []any{item}
	default:
		return nil
	}
}

func extractLocationCountries(value any) []string {
	values := []any{value}
	if list, ok := value.([]any); ok {
		values = list
	}
	out := []string{}
	seen := map[string]struct{}{}
	for _, entry := range values {
		obj, _ := entry.(map[string]any)
		countryName := normalizeText(obj["name"])
		if countryName == "" {
			continue
		}
		normalized := locationnorm.NormalizeCountryName(countryName, true)
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
	if len(out) == 0 {
		out = append(out, "United States")
	}
	return out
}

func parseCompany(value any) map[string]any {
	item, _ := value.(map[string]any)
	name := normalizeText(item["name"])
	homeURL := normalizeText(firstNonEmpty(stringValue(item["sameAs"]), stringValue(item["url"])))
	profilePic := normalizeText(item["logo"])
	slug := slugify(name)
	if slug == "" {
		slug = "dailyremote-company"
	}
	return map[string]any{
		"id":            "dailyremote_company_" + slug,
		"name":          nilIfEmpty(name),
		"slug":          slug,
		"tagline":       nil,
		"homePageURL":   nilIfEmpty(homeURL),
		"linkedInURL":   nil,
		"employeeRange": nil,
		"sponsorsH1B":   nil,
		"profilePicURL": nilIfEmpty(profilePic),
	}
}

func resolveRedirectURLDailyRemote(rawURL string) string {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return trimmed
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return trimmed
	}
	req, err := http.NewRequest(http.MethodGet, trimmed, nil)
	if err != nil {
		return trimmed
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return trimmed
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	_ = resp.Body.Close()
	if resp.Request != nil && resp.Request.URL != nil {
		if finalURL := strings.TrimSpace(resp.Request.URL.String()); finalURL != "" {
			return finalURL
		}
	}
	return trimmed
}

func extractExternalID(jobPosting map[string]any, sourceURL string) int {
	if id := ExtractExternalIDFromURL(sourceURL); id > 0 {
		return id
	}
	if id := ExtractExternalIDFromURL(stringValue(jobPosting["url"])); id > 0 {
		return id
	}
	identifier, _ := jobPosting["identifier"].(map[string]any)
	value := stringValue(identifier["value"])
	if id := ExtractExternalIDFromURL(value); id > 0 {
		return id
	}
	return 0
}

func buildJobSlug(canonicalURL, roleTitle string) string {
	if strings.TrimSpace(canonicalURL) != "" {
		parsed, err := url.Parse(strings.TrimSpace(canonicalURL))
		if err == nil {
			parts := []string{}
			for _, part := range strings.Split(parsed.Path, "/") {
				if strings.TrimSpace(part) != "" {
					parts = append(parts, strings.TrimSpace(part))
				}
			}
			if len(parts) > 0 {
				tail := parts[len(parts)-1]
				tail = regexp.MustCompile(`-\d+$`).ReplaceAllString(tail, "")
				if slug := slugify(tail); slug != "" {
					return slug
				}
			}
		}
	}
	return slugify(roleTitle)
}

func normalizeLocationType(value any) string {
	token := strings.ToLower(strings.TrimSpace(stringValue(value)))
	if token == "" || token == "telecommute" {
		return "remote"
	}
	return token
}

func extractHeadInfoFromHTML(htmlText string) (string, string) {
	salaryText := ""
	containerMatch := headContainerPattern.FindStringSubmatch(htmlText)
	if len(containerMatch) >= 2 {
		for _, item := range inlineHeadItemPattern.FindAllStringSubmatch(containerMatch[1], -1) {
			if len(item) < 2 {
				continue
			}
			text := normalizeText(tagPattern.ReplaceAllString(item[1], " "))
			if looksLikeSalaryText(text) && salaryNumberPattern.MatchString(normalizeSalaryText(text)) {
				salaryText = text
				break
			}
		}
	}
	aiSummary := ""
	if match := aiSummaryPattern.FindStringSubmatch(htmlText); len(match) >= 2 {
		aiSummary = normalizeText(tagPattern.ReplaceAllString(match[1], " "))
	}
	return salaryText, aiSummary
}

func extractCompanyEnrichmentFromHTML(htmlText string) map[string]any {
	match := companyProfilePattern.FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return map[string]any{}
	}
	employeeRange := ""
	industries := make([]string, 0)
	seen := map[string]struct{}{}
	for _, tagMatch := range companyTagPattern.FindAllStringSubmatch(match[1], -1) {
		if len(tagMatch) < 2 {
			continue
		}
		text := normalizeText(tagPattern.ReplaceAllString(tagMatch[1], " "))
		if text == "" {
			continue
		}
		lowered := strings.ToLower(text)
		if strings.Contains(lowered, "employees") && employeeRange == "" {
			employeeRange = normalizeEmployeeRangeText(text)
			continue
		}
		if strings.Contains(lowered, "industry") {
			industry := strings.TrimSpace(regexp.MustCompile(`(?i)^\W*industry\s*`).ReplaceAllString(text, ""))
			key := strings.ToLower(industry)
			if industry != "" {
				if _, ok := seen[key]; !ok {
					seen[key] = struct{}{}
					industries = append(industries, industry)
				}
			}
		}
	}
	payload := map[string]any{}
	if employeeRange != "" {
		payload["employeeRange"] = employeeRange
	}
	if len(industries) > 0 {
		payload["industrySpecialities"] = industries
	}
	return payload
}

func normalizeEmployeeRangeText(value string) string {
	rangeRe := regexp.MustCompile(`([0-9][0-9,]*)\s*-\s*([0-9][0-9,]*)`)
	if match := rangeRe.FindStringSubmatch(value); len(match) >= 3 {
		return strings.ReplaceAll(match[1], ",", "") + "-" + strings.ReplaceAll(match[2], ",", "")
	}
	plusRe := regexp.MustCompile(`([0-9][0-9,]*)\s*\+`)
	if match := plusRe.FindStringSubmatch(value); len(match) >= 2 {
		return strings.ReplaceAll(match[1], ",", "") + "+"
	}
	numberRe := regexp.MustCompile(`([0-9][0-9,]*)`)
	if match := numberRe.FindStringSubmatch(value); len(match) >= 2 {
		return strings.ReplaceAll(match[1], ",", "")
	}
	return ""
}

func parseSalaryRangeFromText(value string) any {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return nil
	}
	normalized := strings.ToLower(normalizeSalaryText(raw))
	matches := salaryNumberPattern.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 {
		matches = salaryNumberPattern.FindAllStringSubmatch(normalizeSalaryText(raw), -1)
	}
	if len(matches) == 0 {
		return nil
	}
	amounts := make([]float64, 0, len(matches))
	currencySymbol := "$"
	for _, match := range matches {
		if len(match) < 4 {
			continue
		}
		if strings.TrimSpace(currencySymbol) == "$" && strings.TrimSpace(match[1]) != "" {
			currencySymbol = strings.TrimSpace(match[1])
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
	}
	minValue := amounts[0]
	maxValue := amounts[0]
	if len(amounts) > 1 {
		maxValue = amounts[1]
	}
	currencyCode := "USD"
	if currencySymbol == "€" {
		currencyCode = "EUR"
	} else if currencySymbol == "£" {
		currencyCode = "GBP"
	}
	payload := map[string]any{
		"min":                     minValue,
		"max":                     maxValue,
		"salaryType":              salaryType,
		"currencyCode":            currencyCode,
		"currencySymbol":          currencySymbol,
		"salaryHumanReadableText": raw,
	}
	if currencyCode == "USD" {
		payload["minSalaryAsUSD"] = minValue
		payload["maxSalaryAsUSD"] = maxValue
	}
	return payload
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

func looksLikeSalaryText(value string) bool {
	if salaryHintPattern.MatchString(value) {
		return true
	}
	if experienceHintPattern.MatchString(value) {
		return false
	}
	return false
}

func decodeHTMLText(value string) string {
	text := strings.TrimSpace(html.UnescapeString(value))
	if text == "" {
		return ""
	}
	return text
}

func normalizeEmploymentType(value any) any {
	switch item := value.(type) {
	case string:
		token := strings.ToLower(strings.TrimSpace(item))
		if token == "" {
			return nil
		}
		return token
	case []any:
		for _, raw := range item {
			token := strings.ToLower(strings.TrimSpace(stringValue(raw)))
			if token == "full_time" || token == "full-time" {
				return "full_time"
			}
			if token == "part_time" || token == "part-time" {
				return "part_time"
			}
			if token != "" {
				return token
			}
		}
	}
	return nil
}

func extractRelativePostDate(articleHTML string, now time.Time) time.Time {
	text := normalizeText(tagPattern.ReplaceAllString(articleHTML, " "))
	if text == "" {
		return time.Time{}
	}
	lowered := strings.ToLower(strings.TrimSpace(text))
	switch {
	case strings.Contains(lowered, "just now"), strings.Contains(lowered, " now "), strings.Contains(lowered, " today "):
		return now.UTC()
	case strings.Contains(lowered, "yesterday"):
		return now.UTC().Add(-24 * time.Hour)
	}
	match := relTimePattern.FindStringSubmatch(lowered)
	if len(match) < 3 {
		return time.Time{}
	}
	count, _ := strconv.Atoi(match[1])
	unit := strings.ToLower(match[2])
	switch {
	case strings.HasPrefix(unit, "min"):
		return now.UTC().Add(-time.Duration(count) * time.Minute)
	case strings.HasPrefix(unit, "hour"):
		return now.UTC().Add(-time.Duration(count) * time.Hour)
	case strings.HasPrefix(unit, "day"):
		return now.UTC().Add(-time.Duration(count) * 24 * time.Hour)
	case strings.HasPrefix(unit, "week"):
		return now.UTC().Add(-time.Duration(count) * 7 * 24 * time.Hour)
	default:
		return time.Time{}
	}
}

func normalizeISO(value any) string {
	raw := strings.TrimSpace(stringValue(value))
	if raw == "" {
		return ""
	}
	if parsed, err := parseISO(raw); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

func parseISO(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC(), nil
	}
	if strings.HasSuffix(value, "Z") {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSuffix(value, "Z")+"+00:00"); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, errors.New("invalid time format")
}

func resolveURL(baseURL, href string) string {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return strings.TrimSpace(href)
	}
	ref, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return strings.TrimSpace(href)
	}
	return base.ResolveReference(ref).String()
}

func normalizeText(value any) string {
	text := html.UnescapeString(strings.TrimSpace(stringValue(value)))
	text = strings.Join(strings.Fields(text), " ")
	return text
}

func slugify(value string) string {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), "-")
	return strings.Trim(normalized, "-")
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func nilIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func containsIgnoreCase(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
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
