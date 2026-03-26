package remotedotco

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"goapplyjob-golang-backend/internal/employmentnorm"
	"goapplyjob-golang-backend/internal/locationnorm"
	"goapplyjob-golang-backend/internal/sources/currency"
	"html"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	Source      = "remotedotco"
	PayloadType = "delta_xml"
	Cookie      = "bm_sv=5A8C7F1CB92FE5C2C4A41ABDED31BD15~YAAQLg7SF68pw8ycAQAApUNB6x+woxlOaUD8U+4uGiITnowuSTjYUQ1mV48EVhnpSJKdhVDfz+RkzEGchTu1dyTPhK3AYn94J3usk8zaQPys5KGNsS/FLk51rjwPXTZOJDWBjA6mczsxHknaSndu3k8YrkKk4bCGrmayS3dQdkYA1cHMAwmWVJ/MkAC3wReE0MudAARwDhHiGHUwz503y00dUtIldmi1sy52YdeoDmsAQ2dncj78q3B7QSCWgac=~1;"
)

var (
	urlBlockPattern    = regexp.MustCompile(`(?is)<url(?:\s[^>]*)?>.*?</url>`)
	urlOpenPattern     = regexp.MustCompile(`(?is)<url(?:\s|>)`)
	urlSetClosePattern = regexp.MustCompile(`(?is)</urlset\s*>`)
	lastmodPattern     = regexp.MustCompile(`(?is)<lastmod>\s*([^<]+?)\s*</lastmod>`)
	jsonAppPattern     = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/json['"][^>]*>(.*?)</script>`)
	jsonLDPattern      = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/ld\+json['"][^>]*>(.*?)</script>`)
)

type xmlURL struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

func ToTargetJobURL(rawURL string) string { return rawURL }

func ParseImportRows(bodyText string) ([]map[string]any, int) {
	blocks := urlBlockPattern.FindAllString(bodyText, -1)
	rows := make([]map[string]any, 0, len(blocks))
	skipped := 0
	for _, block := range blocks {
		var row xmlURL
		if err := xml.Unmarshal([]byte(block), &row); err != nil {
			skipped++
			continue
		}
		postDate, err := normalizeTime(row.LastMod)
		if err != nil || strings.TrimSpace(row.Loc) == "" {
			skipped++
			continue
		}
		rows = append(rows, map[string]any{
			"url":       strings.TrimSpace(row.Loc),
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
		urlValue, _ := row["url"].(string)
		postDate, _ := row["post_date"].(time.Time)
		if urlValue == "" || postDate.IsZero() {
			continue
		}
		ordered = append(ordered, pair{url: urlValue, postDate: postDate})
	}
	parts := []string{`<?xml version="1.0" encoding="UTF-8"?>`, `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`}
	for _, row := range ordered {
		parts = append(parts, "  <url>")
		parts = append(parts, "    <loc>"+escapeXML(row.url)+"</loc>")
		parts = append(parts, "    <lastmod>"+row.postDate.UTC().Format(time.RFC3339Nano)+"</lastmod>")
		parts = append(parts, "  </url>")
	}
	parts = append(parts, `</urlset>`)
	return strings.Join(parts, "\n") + "\n"
}

func ParseRawHTML(htmlText, _ string) map[string]any {
	matches := jsonAppPattern.FindAllStringSubmatch(htmlText, -1)
	if len(matches) == 0 {
		return map[string]any{}
	}
	payloadRaw := strings.TrimSpace(matches[len(matches)-1][1])
	if payloadRaw == "" {
		return map[string]any{}
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(payloadRaw), &data); err != nil {
		return map[string]any{}
	}
	props, _ := data["props"].(map[string]any)
	pageProps, _ := props["pageProps"].(map[string]any)
	jobDetails, _ := pageProps["jobDetails"].(map[string]any)
	if jobDetails == nil {
		return map[string]any{}
	}

	applyURL := stringValue(jobDetails["applyURL"])
	roleTitle := stringValue(jobDetails["title"])
	roleDescription := stringValue(jobDetails["description"])
	jobSummary := stringValue(jobDetails["jobSummary"])
	postedDate := normalizeISO(jobDetails["postedDate"])
	validUntil := normalizeISO(jobDetails["expireOn"])
	locationType := normalizeRemoteOption(jobDetails["remoteOptions"])
	employmentType := normalizeJobSchedule(jobDetails["jobSchedules"])
	locationCities := stringSlice(jobDetails["cities"])
	locationStates := stringSlice(jobDetails["states"])
	locationCountries := stringSlice(jobDetails["countries"])
	locationStates = filterUSStates(locationStates)
	isEntry, isJunior, isMid, isSenior, isLead := inferSeniorityFromCareerLevel(jobDetails["careerLevel"])

	company := map[string]any{}
	if rawCompany, ok := jobDetails["company"].(map[string]any); ok {
		logo := normalizeRemoteCoURL(rawCompany["logo"])
		companyDescription := stringOrNil(rawCompany["description"])
		tagline := normalizeHTMLToPlainText(companyDescription)
		company = map[string]any{
			"id":                          namespacedCompanyID(rawCompany["companyId"]),
			"name":                        stringOrNil(rawCompany["name"]),
			"slug":                        stringOrNil(rawCompany["slug"]),
			"tagline":                     stringOrNil(tagline),
			"foundedYear":                 nil,
			"homePageURL":                 stringOrNil(rawCompany["website"]),
			"linkedInURL":                 stringOrNil(rawCompany["linkedinURL"]),
			"employeeRange":               nil,
			"sponsorsH1B":                 nil,
			"sponsorsUKSkilledWorkerVisa": nil,
			"profilePicURL":               stringOrNil(logo),
			"linkedInDescription":         companyDescription,
			"fundingData":                 []any{},
			"industrySpecialities":        nil,
		}
	}

	salaryRange := extractSalaryRangeFromJobPostingLDJSON(htmlText)
	salaryRangeText := stringValue(jobDetails["salaryRange"])
	if salaryRangeMap, ok := salaryRange.(map[string]any); ok && salaryRangeText != "" {
		salaryRangeMap["salaryHumanReadableText"] = salaryRangeText
	}
	slug := stripExternalIDFromSlug(stringValue(jobDetails["slug"]))

	payload := map[string]any{
		"id":                           stringOrNil(jobDetails["id"]),
		"url":                          stringOrNil(applyURL),
		"slug":                         stringOrNil(slug),
		"created_at":                   stringOrNil(postedDate),
		"validUntilDate":               stringOrNil(validUntil),
		"roleTitle":                    stringOrNil(roleTitle),
		"roleDescription":              stringOrNil(roleDescription),
		"jobDescriptionSummary":        stringOrNil(jobSummary),
		"twoLineJobDescriptionSummary": stringOrNil(jobSummary),
		"employmentType":               employmentType,
		"locationType":                 stringOrNil(locationType),
		"locationCity":                 stringOrNil(firstString(locationCities)),
		"locationUSStates":             locationStates,
		"locationCountries":            locationCountries,
		"isEntryLevel":                 isEntry,
		"isJunior":                     isJunior,
		"isMidLevel":                   isMid,
		"isSenior":                     isSenior,
		"isLead":                       isLead,
		"company":                      company,
		"salaryRange":                  salaryRange,
		"descriptionLanguage":          "en",
	}
	return payload
}

func ExtractFirstLastmod(data []byte) string {
	match := lastmodPattern.FindSubmatch(data)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(string(match[1]))
}

func ExtractLastLastmod(data []byte) string {
	matches := lastmodPattern.FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		return ""
	}
	return strings.TrimSpace(string(matches[len(matches)-1][1]))
}

func DeltaNewerThanLastmod(fullData []byte, previousFirstLastmod string) []byte {
	previousDT, err := normalizeTime(previousFirstLastmod)
	if err != nil {
		return fullData
	}
	blocks := make([][]byte, 0)
	for _, match := range urlBlockPattern.FindAll(fullData, -1) {
		blockLastmod := ExtractFirstLastmod(match)
		blockDT, err := normalizeTime(blockLastmod)
		if err != nil {
			continue
		}
		if blockDT.After(previousDT) {
			blocks = append(blocks, []byte(match))
		} else {
			break
		}
	}
	if len(blocks) == 0 {
		return []byte{}
	}
	firstURL := urlOpenPattern.FindIndex(fullData)
	if firstURL == nil {
		return []byte(strings.Join(byteBlocksToStrings(blocks), ""))
	}
	suffix := []byte{}
	if match := urlSetClosePattern.Find(fullData); len(match) > 0 {
		suffix = match
	}
	output := make([]byte, 0, len(fullData))
	output = append(output, fullData[:firstURL[0]]...)
	for _, block := range blocks {
		output = append(output, block...)
	}
	output = append(output, suffix...)
	return output
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
		dateOnly := parsed.UTC()
		now := time.Now().UTC()
		if dateOnly.Year() == now.Year() && dateOnly.YearDay() == now.YearDay() {
			return now, nil
		}
		return dateOnly, nil
	}
	return time.Time{}, errors.New("invalid time format")
}

func normalizeISO(value any) string {
	raw := stringValue(value)
	if raw == "" {
		return ""
	}
	if parsed, err := normalizeTime(raw); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

func normalizeToken(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	lowered := strings.ToLower(trimmed)
	lowered = strings.ReplaceAll(lowered, "-", "_")
	fields := strings.FieldsFunc(lowered, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '_' })
	if len(fields) == 0 {
		return ""
	}
	return strings.Join(fields, "_")
}

func normalizeRemoteOption(value any) string {
	normalized := map[string]string{
		"100% remote work":    "remote",
		"hybrid remote work":  "hybrid",
		"partial remote work": "partial_remote",
	}
	for _, raw := range stringSlice(value) {
		key := strings.ToLower(strings.TrimSpace(raw))
		if mapped, ok := normalized[key]; ok {
			return mapped
		}
	}
	return ""
}

func normalizeJobSchedule(value any) string {
	values := stringSlice(value)
	if len(values) == 0 {
		return employmentnorm.NormalizeEmploymentTypeString("")
	}
	return employmentnorm.NormalizeEmploymentTypeString(values[0])
}

func inferSeniorityFromCareerLevel(value any) (bool, bool, bool, bool, bool) {
	levels := stringSlice(value)
	if len(levels) == 0 {
		return false, false, false, false, false
	}
	hasManager := false
	hasSenior := false
	hasJunior := false
	hasEntry := false
	for _, level := range levels {
		token := strings.ToLower(strings.TrimSpace(level))
		token = strings.ReplaceAll(token, "-", " ")
		token = strings.Join(strings.Fields(token), " ")
		switch token {
		case "manager", "lead", "director", "executive":
			hasManager = true
		case "senior level manager (director, dept head, vp, general manager, c level)":
			hasManager = true
			hasSenior = true
		case "senior":
			hasSenior = true
		case "senior level":
			hasSenior = true
		case "experienced":
			hasSenior = true
		case "junior":
			hasJunior = true
		case "entry", "entry level":
			hasEntry = true
		}
	}
	isEntry := hasEntry
	isJunior := hasJunior
	isSenior := hasSenior
	isLead := hasManager
	isMid := !(isEntry || isJunior || isSenior || isLead)
	return isEntry, isJunior, isMid, isSenior, isLead
}

func stringSlice(value any) []string {
	switch v := value.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, entry := range v {
			if entry == "" {
				continue
			}
			out = append(out, entry)
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, entry := range v {
			text := stringValue(entry)
			if text == "" {
				continue
			}
			out = append(out, text)
		}
		return out
	default:
		text := stringValue(value)
		if text == "" {
			return []string{}
		}
		return []string{text}
	}
}

func firstString(value any) string {
	items := stringSlice(value)
	if len(items) == 0 {
		return ""
	}
	return items[0]
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func stringOrNil(value any) any {
	text := stringValue(value)
	if text == "" {
		return nil
	}
	return text
}

func namespacedCompanyID(value any) any {
	raw := stringValue(value)
	if raw == "" {
		return nil
	}
	if strings.HasPrefix(raw, Source+"_") {
		return raw
	}
	return Source + "_" + raw
}

func filterUSStates(values []string) []string {
	if len(values) == 0 {
		return values
	}
	allowed := map[string]struct{}{}
	for _, state := range locationnorm.USStateNames() {
		allowed[state] = struct{}{}
	}
	filtered := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		normalized := locationnorm.NormalizeUSStateName(value)
		if normalized == "" {
			continue
		}
		if _, ok := allowed[normalized]; !ok {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		filtered = append(filtered, normalized)
	}
	return filtered
}

func stripExternalIDFromSlug(value string) string {
	if value == "" {
		return value
	}
	uuidPattern := regexp.MustCompile(`-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	return uuidPattern.ReplaceAllString(value, "")
}

func normalizeRemoteCoURL(value any) string {
	raw := stringValue(value)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if strings.HasPrefix(raw, "/") {
		return "https://remote.co" + raw
	}
	return "https://remote.co/" + raw
}

func normalizeHTMLToPlainText(value any) string {
	raw := strings.TrimSpace(stringValue(value))
	if raw == "" {
		return ""
	}
	text := html.UnescapeString(raw)
	text = regexp.MustCompile(`(?is)<[^>]+>`).ReplaceAllString(text, " ")
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 160 {
		text = strings.TrimSpace(text[:160])
	}
	return text
}

func containsIgnoreCase(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}

func escapeXML(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", `'`, "&apos;")
	return replacer.Replace(html.UnescapeString(value))
}

func byteBlocksToStrings(blocks [][]byte) []string {
	out := make([]string, 0, len(blocks))
	for _, block := range blocks {
		out = append(out, string(block))
	}
	return out
}

func buildSalaryRange(jobDetails map[string]any) any {
	minVal, _ := jobDetails["salaryMin"].(float64)
	maxVal, _ := jobDetails["salaryMax"].(float64)
	currencyCode := stringValue(jobDetails["salaryCurrency"])
	unitText := normalizeToken(stringValue(jobDetails["salaryUnit"]))
	text := stringValue(jobDetails["salaryRange"])

	if minVal == 0 && maxVal == 0 && currencyCode == "" && unitText == "" && text == "" {
		return nil
	}
	if minVal == 0 && maxVal == 0 && currencyCode == "" && unitText == "" && text != "" {
		return parseSalaryRangeFromText(text)
	}

	payload := map[string]any{}
	if minVal > 0 {
		payload["min"] = normalizeSalaryValue(minVal)
	}
	if maxVal > 0 {
		payload["max"] = normalizeSalaryValue(maxVal)
	}
	if unitText != "" {
		payload["salaryType"] = unitText
	}
	if currencyCode != "" {
		payload["currencyCode"] = currencyCode
	}
	if text != "" {
		payload["salaryHumanReadableText"] = text
	}
	if currencyCode == "USD" {
		if minVal > 0 {
			payload["minSalaryAsUSD"] = normalizeSalaryValue(minVal)
		}
		if maxVal > 0 {
			payload["maxSalaryAsUSD"] = normalizeSalaryValue(maxVal)
		}
	}
	return payload
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
		return map[string]any{
			"salaryHumanReadableText": raw,
		}
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
		return map[string]any{
			"salaryHumanReadableText": raw,
		}
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
		"Ã¢â€šÂ¬", "â‚¬",
		"Ð²â€šÂ¬", "â‚¬",
		"Ã‚Â£", "Â£",
		"Ãƒâ€šÃ‚Â£", "Â£",
		"Ð“â€šÐ’Ðˆ", "Â£",
		"Ð’Ðˆ", "Â£",
		"Ðˆ", "Â£",
		"Ã¢â€šÂ¹", "â‚¹",
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

func extractSalaryRangeFromJobPostingLDJSON(htmlText string) any {
	matches := jsonLDPattern.FindAllStringSubmatch(htmlText, -1)
	for _, match := range matches {
		payloadRaw := strings.TrimSpace(match[1])
		if payloadRaw == "" {
			continue
		}
		var decoded any
		if err := json.Unmarshal([]byte(payloadRaw), &decoded); err != nil {
			continue
		}
		if salaryRange := extractSalaryRangeFromLDNode(decoded); salaryRange != nil {
			return salaryRange
		}
	}
	return nil
}

func extractSalaryRangeFromLDNode(node any) any {
	switch value := node.(type) {
	case []any:
		for _, item := range value {
			if salaryRange := extractSalaryRangeFromLDNode(item); salaryRange != nil {
				return salaryRange
			}
		}
	case map[string]any:
		if strings.EqualFold(stringValue(value["@type"]), "JobPosting") {
			return buildSalaryRangeFromBaseSalary(value["baseSalary"])
		}
		for _, child := range value {
			if salaryRange := extractSalaryRangeFromLDNode(child); salaryRange != nil {
				return salaryRange
			}
		}
	}
	return nil
}

func buildSalaryRangeFromBaseSalary(value any) any {
	baseSalary, _ := value.(map[string]any)
	if baseSalary == nil {
		return nil
	}
	quantitativeValue, _ := baseSalary["value"].(map[string]any)
	if quantitativeValue == nil {
		return nil
	}
	minValue, _ := quantitativeValue["minValue"].(float64)
	maxValue, _ := quantitativeValue["maxValue"].(float64)
	currencyCode := stringValue(baseSalary["currency"])
	salaryType := salaryTypeFromUnitText(stringValue(quantitativeValue["unitText"]))
	if minValue == 0 && maxValue == 0 && currencyCode == "" && salaryType == "" {
		return nil
	}
	payload := map[string]any{
		"min":          minValue,
		"max":          maxValue,
		"salaryType":   salaryType,
		"currencyCode": currencyCode,
	}
	if currencyCode == "USD" {
		payload["minSalaryAsUSD"] = minValue
		payload["maxSalaryAsUSD"] = maxValue
	}
	return payload
}

func salaryTypeFromUnitText(value string) string {
	switch normalizeToken(value) {
	case "annually", "annual", "yearly", "year":
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
