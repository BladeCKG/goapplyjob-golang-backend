package builtin

import (
	"encoding/json"
	"html"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	scriptLDPattern         = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/ld\+json['"][^>]*>(.*?)</script>`)
	canonicalPattern        = regexp.MustCompile(`(?is)<link[^>]*rel=['"]canonical['"][^>]*href=['"]([^'"]+)['"]`)
	metaDescriptionPattern  = regexp.MustCompile(`(?is)<meta[^>]*name=['"]description['"][^>]*content=['"]([^'"]+)['"]`)
	companyProfileInitRegex = regexp.MustCompile(`(?is)Builtin\.companyProfileInit\((\{.*?\})\);`)
	jobPostInitRegex        = regexp.MustCompile(`(?is)Builtin\.jobPostInit\((\{.*\})\)`)
	topSkillsRegex          = regexp.MustCompile(`(?is)Top Skills\s*</h[1-6]>.*?<div[^>]*>\s*([^<]*,[^<]*)\s*</div>`)
	salaryChipRegex         = regexp.MustCompile(`(?is)(\d[\d,]*)\s*K?\s*-\s*(\d[\d,]*)\s*K?\s*(Annually|Yearly|Hourly|Monthly)?`)
	seniorityRegex          = regexp.MustCompile(`(?is)\b(Entry level|Junior level|Mid level|Senior level|Expert\s*/\s*Leader)\b`)
	tagPattern              = regexp.MustCompile(`(?is)<[^>]+>`)
	spacePattern            = regexp.MustCompile(`\s+`)
)

func ExtractJob(htmlText, companyHTML string) map[string]any {
	jobPosting := findJobPostingLD(htmlText)
	if len(jobPosting) == 0 {
		return map[string]any{}
	}

	jobPostInit := extractJobPostInit(htmlText)
	jobURL := stringValueFromMap(jobPostInit, "job", "howToApply")
	if jobURL == "" {
		jobURL = extractCanonicalURL(htmlText)
	}
	identifierValue := ""
	if identifier, ok := jobPosting["identifier"].(map[string]any); ok {
		identifierValue = stringValue(identifier["value"])
	}

	locationLabels, firstLocality, applicantCountry := extractLocationParts(jobPosting)
	roleDescription := toPlainText(stringValue(jobPosting["description"]))
	builtinSummaryText := extractBuiltinSummaryText(htmlText)
	roleTitle := stringValue(jobPosting["title"])
	jobSummary, twoLineSummary := summariesFromDescription(roleDescription)
	if builtinSummaryText != "" {
		jobSummary = builtinSummaryText
		twoLineSummary = builtinSummaryText
	}
	rawCompany := toRawCompanyShape(extractCompanyInfo(companyHTML, stringValueFromMap(jobPosting, "hiringOrganization", "sameAs")))
	rawCompanyMap, _ := rawCompany.(map[string]any)
	techStack := extractJSONLDSkills(jobPosting)
	if len(techStack) == 0 {
		techStack = extractTopSkills(htmlText)
	}
	seniorityLabel := extractSeniorityLabel(htmlText)
	levelFlags := inferLevelFlags(roleTitle, "")
	if seniorityLabel != "" {
		levelFlags = inferLevelFlags(roleTitle, seniorityLabel)
	}
	salaryRange := parseSalaryRange(jobPosting, htmlText)

	return map[string]any{
		"id":                           extractExternalJobID(jobURL, identifierValue),
		"created_at":                   parseISO(stringValue(jobPosting["datePosted"])),
		"validUntilDate":               parseISO(stringValue(jobPosting["validThrough"])),
		"dateDeleted":                  nil,
		"requiredLanguages":            []string{"en"},
		"descriptionLanguage":          "en",
		"roleTitle":                    roleTitle,
		"isOnLinkedIn":                 rawCompanyMap != nil && stringValue(rawCompanyMap["linkedInURL"]) != "",
		"roleDescription":              roleDescription,
		"roleRequirements":             nil,
		"benefits":                     nil,
		"jobDescriptionSummary":        jobSummary,
		"twoLineJobDescriptionSummary": twoLineSummary,
		"educationRequirementsCredentialCategory":  stringValueFromMap(jobPosting, "EducationRequirements", "credentialCategory"),
		"experienceInPlaceOfEducation":             false,
		"experienceRequirementsMonthsOfExperience": valueFromMap(jobPosting, "experienceRequirements", "monthsOfExperience"),
		"roleTitleBrazil":                          nil,
		"roleDescriptionBrazil":                    nil,
		"roleRequirementsBrazil":                   nil,
		"benefitsBrazil":                           nil,
		"slugBrazil":                               nil,
		"jobDescriptionSummaryBrazil":              nil,
		"twoLineJobDescriptionSummaryBrazil":       nil,
		"roleTitleFrance":                          nil,
		"roleDescriptionFrance":                    nil,
		"roleRequirementsFrance":                   nil,
		"benefitsFrance":                           nil,
		"slugFrance":                               nil,
		"jobDescriptionSummaryFrance":              nil,
		"twoLineJobDescriptionSummaryFrance":       nil,
		"roleTitleGermany":                         nil,
		"roleDescriptionGermany":                   nil,
		"roleRequirementsGermany":                  nil,
		"benefitsGermany":                          nil,
		"slugGermany":                              nil,
		"jobDescriptionSummaryGermany":             nil,
		"twoLineJobDescriptionSummaryGermany":      nil,
		"url":                                      jobURL,
		"isEntryLevel":                             levelFlags["isEntryLevel"],
		"isJunior":                                 levelFlags["isJunior"],
		"isMidLevel":                               levelFlags["isMidLevel"],
		"isSenior":                                 levelFlags["isSenior"],
		"isLead":                                   levelFlags["isLead"],
		"salaryRange":                              salaryRange,
		"techStack":                                techStack,
		"slug":                                     slugFromURL(firstNonEmpty(extractCanonicalURL(htmlText), jobURL)),
		"isPromoted":                               false,
		"employmentType":                           normalizeEmploymentType(stringValue(jobPosting["employmentType"])),
		"location":                                 firstNonEmpty(strings.Join(locationLabels, " | "), applicantCountry),
		"locationType":                             normalizeLocationType(stringValue(jobPosting["jobLocationType"])),
		"locationCity":                             firstLocality,
		"locationUSStates":                         extractUSStates(jobPosting),
		"categorizedJobTitle":                      nil,
		"categorizedJobFunction":                   nil,
		"company":                                  rawCompany,
	}
}

func ExtractJobFromHTML(htmlText string, fallbackJobURL string) map[string]any {
	payload := ExtractJob(htmlText, "")
	if len(payload) == 0 {
		return payload
	}
	if strings.TrimSpace(stringValue(payload["url"])) == "" && strings.TrimSpace(fallbackJobURL) != "" {
		payload["url"] = fallbackJobURL
	}
	requirements, cleanedDescription := extractRoleRequirementsAndCleanDescription(stringValue(payload["roleDescription"]))
	if requirements != nil {
		payload["roleRequirements"] = requirements
	}
	if cleanedDescription != nil {
		payload["roleDescription"] = cleanedDescription
	}
	if company, _ := payload["company"].(map[string]any); company == nil || len(company) == 0 {
		jobPosting := findJobPostingLD(htmlText)
		companyPayload := fallbackCompanyFromJobPosting(jobPosting, htmlText)
		if companyPayload != nil {
			payload["company"] = toRawCompanyShape(companyPayload)
		}
	}
	return payload
}

func findJobPostingLD(htmlText string) map[string]any {
	for _, match := range scriptLDPattern.FindAllStringSubmatch(htmlText, -1) {
		raw := strings.TrimSpace(match[1])
		if raw == "" {
			continue
		}
		var payload any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue
		}
		if jobPosting := unwrapJobPosting(payload); len(jobPosting) > 0 {
			return jobPosting
		}
	}
	return map[string]any{}
}

func unwrapJobPosting(payload any) map[string]any {
	item, ok := payload.(map[string]any)
	if !ok {
		return nil
	}
	if strings.EqualFold(stringValue(item["@type"]), "JobPosting") {
		return item
	}
	graph, _ := item["@graph"].([]any)
	for _, node := range graph {
		candidate, _ := node.(map[string]any)
		if strings.EqualFold(stringValue(candidate["@type"]), "JobPosting") {
			return candidate
		}
	}
	return nil
}

func extractCanonicalURL(htmlText string) string {
	match := canonicalPattern.FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return ""
	}
	return html.UnescapeString(strings.TrimSpace(match[1]))
}

func extractJobPostInit(htmlText string) map[string]any {
	match := jobPostInitRegex.FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return map[string]any{}
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(match[1])), &payload); err != nil {
		return map[string]any{}
	}
	return payload
}

func parseISO(value string) any {
	if value == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	return value
}

func toPlainText(htmlText string) any {
	if strings.TrimSpace(htmlText) == "" {
		return nil
	}
	text := tagPattern.ReplaceAllString(htmlText, "\n")
	text = html.UnescapeString(text)
	lines := strings.Split(text, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(spacePattern.ReplaceAllString(line, " "))
		if line != "" {
			cleaned = append(cleaned, line)
		}
	}
	if len(cleaned) == 0 {
		return nil
	}
	return strings.Join(cleaned, "\n")
}

func summariesFromDescription(description any) (any, any) {
	text := stringValue(description)
	if text == "" {
		return nil, nil
	}
	summary := text
	if len(summary) > 280 {
		summary = summary[:280] + "..."
	}
	lines := strings.Split(text, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			filtered = append(filtered, strings.TrimSpace(line))
		}
	}
	twoLine := text
	if len(filtered) > 0 {
		if len(filtered) > 2 {
			filtered = filtered[:2]
		}
		twoLine = strings.Join(filtered, " ")
	}
	return summary, twoLine
}

func slugFromURL(rawURL string) any {
	if rawURL == "" {
		return nil
	}
	re := regexp.MustCompile(`/job/([^/?#]+)/(\d+)`)
	if match := re.FindStringSubmatch(rawURL); len(match) == 3 {
		return match[1] + "-" + match[2]
	}
	parts := strings.FieldsFunc(rawURL, func(r rune) bool { return r == '/' })
	if len(parts) == 0 {
		return nil
	}
	return parts[len(parts)-1]
}

func extractExternalJobID(rawURL, identifier string) any {
	if strings.TrimSpace(identifier) != "" {
		return strings.TrimSpace(identifier)
	}
	re := regexp.MustCompile(`/(\d+)(?:[/?#]|$)`)
	if match := re.FindStringSubmatch(rawURL); len(match) == 2 {
		return match[1]
	}
	return nil
}

func extractTopSkills(htmlText string) []string {
	match := topSkillsRegex.FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return []string{}
	}
	parts := strings.Split(match[1], ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		skill := strings.TrimSpace(spacePattern.ReplaceAllString(html.UnescapeString(part), " "))
		if skill == "" {
			continue
		}
		key := strings.ToLower(skill)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, skill)
	}
	return out
}

func extractBuiltinSummaryText(htmlText string) string {
	lower := strings.ToLower(htmlText)
	marker := "summary generated by built in"
	index := strings.Index(lower, marker)
	if index < 0 {
		return ""
	}
	before := htmlText[:index]
	divMatches := regexp.MustCompile(`(?is)<div[^>]*>(.*?)</div>`).FindAllStringSubmatch(before, -1)
	for i := len(divMatches) - 1; i >= 0; i-- {
		text := stringValue(toPlainText(divMatches[i][1]))
		if text != "" {
			return text
		}
	}
	return ""
}

func normalizeHeadingText(value string) string {
	replacer := strings.NewReplacer("\u2019", "'", "\u2018", "'", "\u2032", "'")
	normalized := replacer.Replace(value)
	normalized = strings.ToLower(strings.TrimSpace(spacePattern.ReplaceAllString(normalized, " ")))
	return normalized
}

func extractRoleRequirementsAndCleanDescription(descriptionText string) (any, any) {
	if strings.TrimSpace(descriptionText) == "" {
		return nil, nil
	}
	lines := strings.Split(descriptionText, "\n")
	cleanedLines := make([]string, 0, len(lines))
	requirementLines := []string{}
	capturing := false
	requirementHeadingPattern := regexp.MustCompile(`(?i)(requirement|qualification|what you'll bring|what you bring|must have|who you are|experience you have|skills and qualifications)`)
	stopHeadingPattern := regexp.MustCompile(`(?i)(about|responsibilit|what you'll do|what you will do|benefit|perks|compensation|salary|about the role|company)`)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		normalized := normalizeHeadingText(trimmed)
		if requirementHeadingPattern.MatchString(normalized) && len(trimmed) <= 80 {
			capturing = true
			continue
		}
		if capturing && stopHeadingPattern.MatchString(normalized) && len(trimmed) <= 80 {
			capturing = false
		}
		if capturing {
			requirementLines = append(requirementLines, trimmed)
			continue
		}
		cleanedLines = append(cleanedLines, trimmed)
	}
	if len(requirementLines) == 0 {
		return nil, strings.Join(cleanedLines, "\n")
	}
	return strings.Join(dedupeLines(requirementLines), "\n"), strings.Join(cleanedLines, "\n")
}

func dedupeLines(lines []string) []string {
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
	return out
}

func extractJSONLDSkills(jobPosting map[string]any) []string {
	out := []string{}
	seen := map[string]struct{}{}
	var add func(any)
	add = func(value any) {
		switch item := value.(type) {
		case string:
			parts := []string{item}
			if strings.Contains(item, ",") {
				parts = strings.Split(item, ",")
			}
			for _, part := range parts {
				skill := strings.TrimSpace(part)
				if skill == "" {
					continue
				}
				key := strings.ToLower(skill)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, skill)
			}
		case []any:
			for _, entry := range item {
				add(entry)
			}
		}
	}
	add(jobPosting["skills"])
	add(jobPosting["keywords"])
	return out
}

func extractCompanyInfo(companyHTML, companySameAs string) map[string]any {
	if strings.TrimSpace(companyHTML) == "" {
		return nil
	}
	initPayload := map[string]any{}
	if match := companyProfileInitRegex.FindStringSubmatch(companyHTML); len(match) == 2 {
		_ = json.Unmarshal([]byte(match[1]), &initPayload)
	}
	canonicalURL := extractCanonicalURL(companyHTML)
	if canonicalURL == "" {
		canonicalURL = companySameAs
	}
	tagline := ""
	if match := metaDescriptionPattern.FindStringSubmatch(companyHTML); len(match) == 2 {
		tagline = html.UnescapeString(strings.TrimSpace(match[1]))
	}
	websiteURL := valueOrNil(matchAnchorHref(companyHTML, `(?i)View Website`))
	companyMatchKey := buildCompanyMatchKey(firstNonEmpty(stringValue(initPayload["companyName"]), extractTitle(companyHTML)), "", stringValue(websiteURL))
	return map[string]any{
		"external_company_id":         valueOrNil(initPayload["companyId"]),
		"name":                        firstNonEmpty(stringValue(initPayload["companyName"]), extractTitle(companyHTML)),
		"slug":                        firstNonEmpty(stringValue(initPayload["companyAlias"]), companySlugFromURL(canonicalURL)),
		"tagline":                     valueOrNil(tagline),
		"founded_year":                valueOrNil(matchOne(companyHTML, `(?is)Year Founded:\s*([0-9]{4})`)),
		"home_page_url":               websiteURL,
		"linkedin_url":                nil,
		"employee_range":              valueOrNil(matchOne(companyHTML, `(?is)([0-9][0-9,]*)\s+Total Employees`)),
		"profile_pic_url":             valueOrNil(matchOne(companyHTML, `(?is)<img[^>]*src=['"]([^'"]+)['"]`)),
		"industry_specialities":       nil,
		"source_name":                 "builtin",
		"source_company_slug":         valueOrNil(firstNonEmpty(stringValue(initPayload["companyAlias"]), companySlugFromURL(canonicalURL))),
		"company_match_key":           companyMatchKey,
		"source_company_profile_url":  valueOrNil(canonicalURL),
		"source_company_profile_init": mapOrNil(initPayload),
		"updated_at":                  time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func extractIndustrySpecialitiesFromJobPosting(jobPosting map[string]any) any {
	values := []string{}
	var add func(any)
	add = func(value any) {
		switch item := value.(type) {
		case string:
			parts := []string{item}
			if strings.Contains(item, ",") {
				parts = strings.Split(item, ",")
			}
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part != "" {
					values = append(values, part)
				}
			}
		case []any:
			for _, entry := range item {
				add(entry)
			}
		}
	}
	add(jobPosting["industry"])
	deduped := dedupeLines(values)
	if len(deduped) == 0 {
		return nil
	}
	return deduped
}

func normalizeEmployeeRange(value string) any {
	digits := regexp.MustCompile(`[^0-9]`).ReplaceAllString(value, "")
	if digits == "" {
		return nil
	}
	return digits
}

func fallbackCompanyFromJobPosting(jobPosting map[string]any, jobPageHTML string) map[string]any {
	hiringOrg, _ := jobPosting["hiringOrganization"].(map[string]any)
	companyName := stringValue(hiringOrg["name"])
	companySameAs := stringValue(hiringOrg["sameAs"])
	companySlug := companySlugFromURL(companySameAs)
	var foundedYear any
	if value := matchOne(jobPageHTML, `(?is)Year Founded:\s*([0-9]{4})`); value != "" {
		foundedYear = value
	}
	employeeRange := normalizeEmployeeRange(matchOne(jobPageHTML, `(?is)([0-9][0-9,]*)\s+employees`))
	return map[string]any{
		"external_company_id":         nil,
		"name":                        valueOrNil(companyName),
		"slug":                        valueOrNil(companySlug),
		"tagline":                     nil,
		"founded_year":                foundedYear,
		"home_page_url":               nil,
		"linkedin_url":                nil,
		"employee_range":              employeeRange,
		"profile_pic_url":             valueOrNil(stringValue(hiringOrg["logo"])),
		"industry_specialities":       extractIndustrySpecialitiesFromJobPosting(jobPosting),
		"source_name":                 "builtin",
		"source_company_slug":         valueOrNil(companySlug),
		"company_match_key":           buildCompanyMatchKey(companyName, "", ""),
		"source_company_profile_url":  valueOrNil(companySameAs),
		"source_company_profile_init": nil,
		"updated_at":                  time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func toRawCompanyShape(parsedCompany map[string]any) any {
	if len(parsedCompany) == 0 {
		return nil
	}
	return map[string]any{
		"id":                          parsedCompany["external_company_id"],
		"name":                        parsedCompany["name"],
		"slug":                        parsedCompany["slug"],
		"tagline":                     parsedCompany["tagline"],
		"foundedYear":                 parsedCompany["founded_year"],
		"fundingData":                 []any{},
		"homePageURL":                 parsedCompany["home_page_url"],
		"linkedInURL":                 parsedCompany["linkedin_url"],
		"sponsorsH1B":                 nil,
		"employeeRange":               parsedCompany["employee_range"],
		"profilePicURL":               parsedCompany["profile_pic_url"],
		"taglineBrazil":               nil,
		"taglineFrance":               nil,
		"taglineGermany":              nil,
		"chatGPTIndustries":           nil,
		"chatGPTDescription":          nil,
		"linkedInDescription":         nil,
		"industrySpecialities":        parsedCompany["industry_specialities"],
		"chatGPTDescriptionBrazil":    nil,
		"chatGPTDescriptionFrance":    nil,
		"chatGPTDescriptionGermany":   nil,
		"linkedInDescriptionBrazil":   nil,
		"linkedInDescriptionFrance":   nil,
		"industrySpecialitiesBrazil":  nil,
		"industrySpecialitiesFrance":  nil,
		"linkedInDescriptionGermany":  nil,
		"industrySpecialitiesGermany": nil,
		"sponsorsUKSkilledWorkerVisa": nil,
		"sourceCompanySlug":           parsedCompany["source_company_slug"],
		"companyMatchKey":             parsedCompany["company_match_key"],
	}
}

func normalizeEmploymentType(value string) any {
	normalized := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(value, "_", "-")))
	if normalized == "" {
		return nil
	}
	return normalized
}

func normalizeLocationType(value string) any {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return nil
	}
	if normalized == "telecommute" {
		return "remote"
	}
	return normalized
}

func inferLevelFlags(roleTitle, seniorityLabel string) map[string]bool {
	switch strings.ToLower(strings.TrimSpace(seniorityLabel)) {
	case "entry level":
		return map[string]bool{"isEntryLevel": true, "isJunior": false, "isMidLevel": false, "isSenior": false, "isLead": false}
	case "junior level":
		return map[string]bool{"isEntryLevel": false, "isJunior": true, "isMidLevel": false, "isSenior": false, "isLead": false}
	case "mid level":
		return map[string]bool{"isEntryLevel": false, "isJunior": false, "isMidLevel": true, "isSenior": false, "isLead": false}
	case "senior level":
		return map[string]bool{"isEntryLevel": false, "isJunior": false, "isMidLevel": false, "isSenior": true, "isLead": false}
	case "expert / leader":
		return map[string]bool{"isEntryLevel": false, "isJunior": false, "isMidLevel": false, "isSenior": false, "isLead": true}
	}
	title := strings.ToLower(roleTitle)
	return map[string]bool{
		"isEntryLevel": strings.Contains(title, "entry") || strings.Contains(title, "intern"),
		"isJunior":     strings.Contains(title, "junior") || strings.Contains(title, " jr"),
		"isMidLevel":   strings.Contains(title, "mid"),
		"isSenior":     strings.Contains(title, "senior") || strings.Contains(title, " sr"),
		"isLead":       strings.Contains(title, "lead") || strings.Contains(title, "principal") || strings.Contains(title, "staff"),
	}
}

func extractSeniorityLabel(htmlText string) string {
	headerPart := htmlText
	if parts := strings.SplitN(htmlText, "container py-lg", 2); len(parts) > 0 {
		headerPart = parts[0]
	}
	match := seniorityRegex.FindStringSubmatch(html.UnescapeString(tagPattern.ReplaceAllString(headerPart, " ")))
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(spacePattern.ReplaceAllString(match[1], " "))
}

func parseSalaryRange(jobPosting map[string]any, htmlText string) map[string]any {
	out := map[string]any{
		"max":                     nil,
		"min":                     nil,
		"salaryType":              nil,
		"currencyCode":            "USD",
		"currencySymbol":          "$",
		"maxSalaryAsUSD":          nil,
		"minSalaryAsUSD":          nil,
		"salaryHumanReadableText": nil,
	}
	baseSalary, _ := jobPosting["baseSalary"].(map[string]any)
	if currency := stringValue(baseSalary["currency"]); currency != "" {
		out["currencyCode"] = strings.ToUpper(currency)
		if out["currencyCode"] != "USD" {
			out["currencySymbol"] = nil
		}
	}
	valueMap, _ := baseSalary["value"].(map[string]any)
	if minValue, ok := parseInt(valueMap["minValue"]); ok {
		out["min"] = minValue
	}
	if maxValue, ok := parseInt(valueMap["maxValue"]); ok {
		out["max"] = maxValue
	}
	if salaryType := salaryTypeFromUnit(stringValue(valueMap["unitText"])); salaryType != nil {
		out["salaryType"] = salaryType
	}
	if out["min"] == nil && out["max"] == nil {
		if match := salaryChipRegex.FindStringSubmatch(htmlText); len(match) >= 3 {
			left, _ := strconv.Atoi(strings.ReplaceAll(match[1], ",", ""))
			right, _ := strconv.Atoi(strings.ReplaceAll(match[2], ",", ""))
			if strings.Contains(strings.ToLower(match[0]), "k") && len(match[1]) <= 3 {
				left *= 1000
				right *= 1000
			}
			out["min"] = left
			out["max"] = right
			out["salaryType"] = salaryTypeFromUnit(match[3])
		}
	}
	if out["currencyCode"] == "USD" {
		out["minSalaryAsUSD"] = out["min"]
		out["maxSalaryAsUSD"] = out["max"]
	}
	if out["min"] != nil && out["max"] != nil {
		typeLabel := stringValue(out["salaryType"])
		if typeLabel == "" {
			typeLabel = "salary"
		}
		out["salaryHumanReadableText"] = "$" + humanizeInt(out["min"]) + "-$" + humanizeInt(out["max"]) + " " + typeLabel
	}
	return out
}

func salaryTypeFromUnit(value string) any {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "year", "yearly", "annually", "annual":
		return "per year"
	case "hour", "hourly":
		return "per hour"
	case "month", "monthly":
		return "per month"
	case "week", "weekly":
		return "per week"
	case "day", "daily":
		return "per day"
	default:
		return nil
	}
}

func parseInt(value any) (int, bool) {
	switch v := value.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, false
		}
		n, err := strconv.Atoi(v)
		return n, err == nil
	default:
		return 0, false
	}
}

func humanizeInt(value any) string {
	number, ok := parseInt(value)
	if !ok {
		return ""
	}
	text := strconv.Itoa(number)
	if len(text) <= 3 {
		return text
	}
	parts := []string{}
	for len(text) > 3 {
		parts = append([]string{text[len(text)-3:]}, parts...)
		text = text[:len(text)-3]
	}
	parts = append([]string{text}, parts...)
	return strings.Join(parts, ",")
}

func extractLocationParts(jobPosting map[string]any) ([]string, any, string) {
	locations, _ := jobPosting["jobLocation"].([]any)
	if locations == nil {
		if one, ok := jobPosting["jobLocation"].(map[string]any); ok {
			locations = []any{one}
		}
	}
	labels := []string{}
	firstLocality := ""
	for idx, location := range locations {
		entry, _ := location.(map[string]any)
		address, _ := entry["address"].(map[string]any)
		locality := stringValue(address["addressLocality"])
		region := stringValue(address["addressRegion"])
		country := stringValue(address["addressCountry"])
		if idx == 0 {
			firstLocality = locality
		}
		parts := []string{}
		for _, part := range []string{locality, region, country} {
			if part != "" {
				parts = append(parts, part)
			}
		}
		if len(parts) > 0 {
			labels = append(labels, strings.Join(parts, ", "))
		}
	}
	applicantCountry := stringValueFromMap(jobPosting, "applicantLocationRequirements", "name")
	return labels, valueOrNil(firstLocality), applicantCountry
}

func extractUSStates(jobPosting map[string]any) any {
	locations, _ := jobPosting["jobLocation"].([]any)
	if locations == nil {
		if one, ok := jobPosting["jobLocation"].(map[string]any); ok {
			locations = []any{one}
		}
	}
	states := []string{}
	seen := map[string]struct{}{}
	for _, location := range locations {
		entry, _ := location.(map[string]any)
		address, _ := entry["address"].(map[string]any)
		region := stringValue(address["addressRegion"])
		country := strings.ToUpper(stringValue(address["addressCountry"]))
		if region == "" || (country != "USA" && country != "US" && country != "UNITED STATES") {
			continue
		}
		if _, ok := seen[region]; ok {
			continue
		}
		seen[region] = struct{}{}
		states = append(states, region)
	}
	return states
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func valueFromMap(item map[string]any, key, child string) any {
	nested, _ := item[key].(map[string]any)
	if nested == nil {
		return nil
	}
	return valueOrNil(nested[child])
}

func stringValueFromMap(item map[string]any, key, child string) string {
	nested, _ := item[key].(map[string]any)
	if nested == nil {
		return ""
	}
	return stringValue(nested[child])
}

func sliceIfSet(value string) any {
	if value == "" {
		return []string{}
	}
	return []string{value}
}

func valueOrNil(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return strings.TrimSpace(v)
	default:
		return value
	}
}

func mapOrNil(value map[string]any) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func extractTitle(htmlText string) string {
	match := regexp.MustCompile(`(?is)<title>\s*([^|<]+)`).FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return ""
	}
	return html.UnescapeString(strings.TrimSpace(match[1]))
}

func companySlugFromURL(value string) string {
	match := regexp.MustCompile(`/company/([^/?#]+)`).FindStringSubmatch(value)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(match[1])
}

func normalizeNameForKey(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "&", " and ")
	normalized = regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(normalized, "-")
	normalized = regexp.MustCompile(`-{2,}`).ReplaceAllString(normalized, "-")
	return strings.Trim(normalized, "-")
}

func hostFromURL(rawURL string) string {
	if strings.TrimSpace(rawURL) == "" {
		return ""
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.Trim(parsed.Hostname(), "."))
	host = strings.TrimPrefix(host, "www.")
	return host
}

func domainFromURL(rawURL string) string {
	host := hostFromURL(rawURL)
	if host == "" {
		return ""
	}
	parts := strings.Split(host, ".")
	if len(parts) <= 2 {
		return host
	}
	secondLevelCC := map[string]struct{}{"co": {}, "com": {}, "org": {}, "net": {}, "gov": {}, "edu": {}, "ac": {}}
	if len(parts[len(parts)-1]) == 2 {
		if _, ok := secondLevelCC[parts[len(parts)-2]]; ok && len(parts) >= 3 {
			return strings.Join(parts[len(parts)-3:], ".")
		}
	}
	return strings.Join(parts[len(parts)-2:], ".")
}

func buildCompanyMatchKey(name, linkedinURL, homePageURL string) any {
	linkedinDomain := domainFromURL(linkedinURL)
	if linkedinDomain != "" {
		parsed, _ := url.Parse(linkedinURL)
		path := strings.Trim(strings.ToLower(parsed.Path), "/")
		if path != "" {
			return []string{"linkedin:" + linkedinDomain + "/" + path}
		}
		return []string{"linkedin:" + linkedinDomain}
	}
	keys := []string{}
	homeDomain := domainFromURL(homePageURL)
	fullHost := hostFromURL(homePageURL)
	if homeDomain != "" {
		keys = append(keys, "domain:"+homeDomain)
	}
	if fullHost != "" && fullHost != homeDomain {
		keys = append(keys, "subdomain:"+fullHost)
	}
	if len(keys) > 0 {
		return keys
	}
	normalizedName := normalizeNameForKey(name)
	if normalizedName != "" {
		return []string{"name:" + normalizedName}
	}
	return nil
}

func matchOne(text, pattern string) string {
	match := regexp.MustCompile(pattern).FindStringSubmatch(text)
	if len(match) < 2 {
		return ""
	}
	return html.UnescapeString(strings.TrimSpace(match[1]))
}

func matchHref(text, pattern string) string {
	re := regexp.MustCompile(`(?is)<a[^>]*href=['"]([^'"]*` + pattern + `[^'"]*)['"]`)
	match := re.FindStringSubmatch(text)
	if len(match) < 2 {
		return ""
	}
	return html.UnescapeString(strings.TrimSpace(match[1]))
}

func matchAnchorHref(text, anchorPattern string) string {
	re := regexp.MustCompile(`(?is)<a[^>]*href=['"]([^'"]+)['"][^>]*>[^<]*` + anchorPattern + `[^<]*</a>`)
	match := re.FindStringSubmatch(text)
	if len(match) < 2 {
		return ""
	}
	return html.UnescapeString(strings.TrimSpace(match[1]))
}
