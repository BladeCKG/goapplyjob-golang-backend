package builtin

import (
	"encoding/json"
	"errors"
	"goapplyjob-golang-backend/internal/locationnorm"
	"html"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	nethtml "golang.org/x/net/html"
)

var (
	scriptLDPattern         = regexp.MustCompile(`(?is)<script[^>]*type=['"][^'"]*ld(?:\+|&#x2B;|&#43;)json[^'"]*['"][^>]*>(.*?)</script>`)
	canonicalPattern        = regexp.MustCompile(`(?is)<link[^>]*rel=['"]canonical['"][^>]*href=['"]([^'"]+)['"]`)
	companyProfileInitRegex = regexp.MustCompile(`(?is)Builtin\.companyProfileInit\((\{.*?\})\);`)
	jobPostInitRegex        = regexp.MustCompile(`(?is)Builtin\.jobPostInit\((.*)\)`)
	howToApplyRegex         = regexp.MustCompile(`(?is)howToApply\s*:\s*"([^"]+)"`)
	topSkillsRegex          = regexp.MustCompile(`(?is)Top Skills\s*</h[1-6]>.*?<div[^>]*>\s*([^<]*,[^<]*)\s*</div>`)
	salaryChipRegex         = regexp.MustCompile(`(?is)(\d[\d,]*)\s*K?\s*-\s*(\d[\d,]*)\s*K?\s*(Annually|Yearly|Hourly|Monthly)?`)
	seniorityRegex          = regexp.MustCompile(`(?is)\b(Entry level|Junior level|Mid level|Senior level|Junior|Expert\s*/\s*Leader)\b`)
	senioritySpanRegex      = regexp.MustCompile(`(?is)<span[^>]*>(.*?)</span>`)
	tagPattern              = regexp.MustCompile(`(?is)<[^>]+>`)
	spacePattern            = regexp.MustCompile(`\s+`)
	nullLikeTokenPattern    = regexp.MustCompile(`[^a-z0-9]+`)
)

func ExtractJob(htmlText, companyHTML string) map[string]any {
	jobPosting := findJobPostingLD(htmlText)
	if len(jobPosting) == 0 {
		return map[string]any{}
	}

	jobPostInit := extractJobPostInit(htmlText)
	jobURL := stringValueFromMap(jobPostInit, "job", "howToApply")
	identifierValue := ""
	if identifier, ok := jobPosting["identifier"].(map[string]any); ok {
		identifierValue = stringValue(identifier["value"])
	}

	_, locationCity, locationStates, locationCountries := extractLocationFields(jobPosting)
	descriptionHTML := stringValue(jobPosting["description"])
	var roleDescription any
	if strings.TrimSpace(descriptionHTML) != "" {
		roleDescription = descriptionHTML
	}
	roleDescriptionText := stringValue(toPlainText(descriptionHTML))
	builtinSummaryText := extractBuiltinSummaryText(htmlText)
	roleTitle := stringValue(jobPosting["title"])
	jobSummary, twoLineSummary := summariesFromDescription(roleDescriptionText)
	if builtinSummaryText != "" {
		jobSummary = builtinSummaryText
		twoLineSummary = builtinSummaryText
	}
	rawCompany := toRawCompanyShape(extractCompanyInfo(companyHTML, stringValueFromMap(jobPosting, "hiringOrganization", "sameAs")))
	rawCompanyMap, _ := rawCompany.(map[string]any)
	techStack := []string{}
	if topSkills := extractTopSkills(htmlText); len(topSkills) > 0 {
		techStack = topSkills
	}
	seniorityLabel := extractSeniorityLabel(htmlText)
	levelFlags := inferLevelFlags(roleTitle, "")
	if seniorityLabel != "" {
		levelFlags = inferLevelFlags(roleTitle, seniorityLabel)
	}
	salaryRange := parseSalaryRange(jobPosting, htmlText)
	benefits := valueOrNil(jobPosting["jobBenefits"])

	payload := map[string]any{
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
		"benefits":                     benefits,
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
		"slug":                                     slugFromURL(extractCanonicalURL(htmlText)),
		"isPromoted":                               false,
		"employmentType":                           normalizeEmploymentType(stringValue(jobPosting["employmentType"])),
		"locationType":                             normalizeLocationType(stringValue(jobPosting["jobLocationType"])),
		"locationCity":                             locationCity,
		"locationUSStates":                         locationStates,
		"locationCountries":                        locationCountries,
		"categorizedJobTitle":                      nil,
		"categorizedJobFunction":                   nil,
		"company":                                  rawCompany,
	}
	return payload
}

func ExtractJobFromHTML(htmlText string, fallbackJobURL string) map[string]any {
	payload := ExtractJob(htmlText, "")
	if len(payload) == 0 {
		return payload
	}
	if company, _ := payload["company"].(map[string]any); company == nil || len(company) == 0 {
		jobPosting := findJobPostingLD(htmlText)
		payload["company"] = toRawCompanyShape(fallbackCompanyFromJobPosting(jobPosting, htmlText))
	}
	if jobPosting := findJobPostingLD(htmlText); len(jobPosting) > 0 {
		companySameAs := stringValueFromMap(jobPosting, "hiringOrganization", "sameAs")
		if companySlug := extractBuiltinCompanySlugFromURL(companySameAs); companySlug != "" {
			if company, _ := payload["company"].(map[string]any); company != nil {
				company["slug"] = companySlug
				company["sourceCompanySlug"] = companySlug
			}
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
	marker := "Builtin.jobPostInit("
	start := strings.Index(htmlText, marker)
	if start < 0 {
		return map[string]any{}
	}

	payloadStart := strings.Index(htmlText[start+len(marker):], "{")
	if payloadStart < 0 {
		return map[string]any{}
	}
	i := start + len(marker) + payloadStart
	depth := 0
	inString := false
	stringChar := byte(0)
	escaped := false
	payloadChars := make([]byte, 0, 4096)

	for i < len(htmlText) {
		ch := htmlText[i]
		payloadChars = append(payloadChars, ch)

		if inString {
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
			} else if ch == stringChar {
				inString = false
			}
			i++
			continue
		}

		if ch == '"' || ch == '\'' {
			inString = true
			stringChar = ch
		} else if ch == '{' {
			depth++
		} else if ch == '}' {
			depth--
			if depth == 0 {
				break
			}
		}
		i++
	}

	payloadRaw := strings.TrimSpace(string(payloadChars))
	if payloadRaw == "" {
		return map[string]any{}
	}

	payload := map[string]any{}
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err == nil {
		return payload
	}

	cleaned := coerceJSObjectToJSON(payloadRaw)
	if cleaned == "" {
		return map[string]any{}
	}
	if err := json.Unmarshal([]byte(cleaned), &payload); err != nil {
		return map[string]any{}
	}
	return payload
}

func coerceJSObjectToJSON(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	// Quote unquoted keys.
	value = regexp.MustCompile(`([,{]\s*)([A-Za-z_][A-Za-z0-9_$]*)\s*:`).ReplaceAllString(value, `$1"$2":`)
	// Convert single-quoted strings to double-quoted strings.
	value = regexp.MustCompile(`'([^'\\]*(?:\\.[^'\\]*)*)'`).ReplaceAllStringFunc(value, func(match string) string {
		if len(match) < 2 {
			return match
		}
		content := match[1 : len(match)-1]
		content = strings.ReplaceAll(content, `"`, `\"`)
		return `"` + content + `"`
	})
	// Remove trailing commas.
	value = regexp.MustCompile(`,(\s*[}\]])`).ReplaceAllString(value, `$1`)
	return value
}

func extractHowToApplyURL(htmlText string) string {
	match := howToApplyRegex.FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return ""
	}
	value := strings.TrimSpace(match[1])
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, `\u0026`, "&")
	value = strings.ReplaceAll(value, `\\u0026`, "&")
	return html.UnescapeString(value)
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
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}
	segments := []string{}
	for _, segment := range strings.Split(parsed.Path, "/") {
		segment = strings.TrimSpace(segment)
		if segment != "" {
			segments = append(segments, segment)
		}
	}
	if len(segments) == 0 {
		return nil
	}
	candidate := segments[len(segments)-1]
	if len(segments) >= 3 && regexp.MustCompile(`^\d+$`).MatchString(candidate) && strings.EqualFold(segments[len(segments)-3], "job") {
		candidate = segments[len(segments)-2]
	} else if regexp.MustCompile(`^\d+$`).MatchString(candidate) && len(segments) >= 2 {
		candidate = segments[len(segments)-2]
	}
	candidate = regexp.MustCompile(`-\d+$`).ReplaceAllString(strings.ToLower(candidate), "")
	candidate = regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(candidate, "-")
	candidate = strings.Trim(candidate, "-")
	if candidate == "" || !regexp.MustCompile(`[a-z]`).MatchString(candidate) {
		return nil
	}
	return candidate
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
	if strings.TrimSpace(htmlText) == "" {
		return []string{}
	}
	if doc, err := nethtml.Parse(strings.NewReader(htmlText)); err == nil {
		return extractTopSkillsFromDoc(doc)
	}
	return []string{}
}

func extractTopSkillsFromRegex(htmlText string) []string {
	return nil
}

func extractTopSkillsFromDoc(doc *nethtml.Node) []string {
	chipsContainer := findTopSkillsChipsContainer(doc)
	if chipsContainer == nil {
		return []string{}
	}

	values := []string{}
	seen := map[string]struct{}{}
	for child := chipsContainer.FirstChild; child != nil; child = child.NextSibling {
		if child.Type != nethtml.ElementNode || child.Data != "div" {
			continue
		}
		value := strings.TrimSpace(spacePattern.ReplaceAllString(extractNodeText(child), " "))
		if value == "" || strings.EqualFold(value, "top skills") || isNullLikeSkillToken(value) {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, value)
	}
	return values
}

func findTopSkillsChipsContainer(doc *nethtml.Node) *nethtml.Node {
	foundHeading := false
	var chips *nethtml.Node
	var walk func(*nethtml.Node)
	walk = func(n *nethtml.Node) {
		if chips != nil {
			return
		}
		if n.Type == nethtml.ElementNode && (n.Data == "h1" || n.Data == "h2" || n.Data == "h3" || n.Data == "h4" || n.Data == "h5" || n.Data == "h6") {
			text := strings.ToLower(strings.TrimSpace(spacePattern.ReplaceAllString(extractNodeText(n), " ")))
			if strings.Contains(text, "top skills") {
				foundHeading = true
			}
		}
		if foundHeading && n.Type == nethtml.ElementNode && n.Data == "div" && hasClass(n, "d-flex") && hasClass(n, "gap-sm") && hasClass(n, "flex-wrap") {
			chips = n
			return
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)
	return chips
}

func findTopSkillsHeading(doc *nethtml.Node) *nethtml.Node {
	var hit *nethtml.Node
	var walk func(*nethtml.Node)
	walk = func(n *nethtml.Node) {
		if hit != nil {
			return
		}
		if n.Type == nethtml.ElementNode && (n.Data == "h1" || n.Data == "h2" || n.Data == "h3" || n.Data == "h4" || n.Data == "h5" || n.Data == "h6") {
			text := strings.ToLower(strings.TrimSpace(spacePattern.ReplaceAllString(extractNodeText(n), " ")))
			if strings.Contains(text, "top skills") {
				hit = n
				return
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)
	return hit
}

func findAncestorWithClass(node *nethtml.Node, className string, maxDepth int) *nethtml.Node {
	current := node
	for i := 0; i < maxDepth && current != nil; i++ {
		if hasClass(current, className) {
			return current
		}
		current = current.Parent
	}
	return nil
}

func findFirstDescendantWithClasses(node *nethtml.Node, classes []string) *nethtml.Node {
	var hit *nethtml.Node
	var walk func(*nethtml.Node)
	walk = func(n *nethtml.Node) {
		if hit != nil {
			return
		}
		if n.Type == nethtml.ElementNode && n.Data == "div" {
			ok := true
			for _, className := range classes {
				if !hasClass(n, className) {
					ok = false
					break
				}
			}
			if ok {
				hit = n
				return
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(node)
	return hit
}

func hasClass(node *nethtml.Node, className string) bool {
	if node == nil || node.Type != nethtml.ElementNode {
		return false
	}
	for _, attr := range node.Attr {
		if attr.Key != "class" {
			continue
		}
		for _, token := range strings.Fields(attr.Val) {
			if token == className {
				return true
			}
		}
	}
	return false
}

func extractNodeText(node *nethtml.Node) string {
	if node == nil {
		return ""
	}
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
	requirementHeadingPattern := regexp.MustCompile(`(?i)(requirement|required|qualification|what you'll bring|what you bring|must have|who you are|experience you have|skills and qualifications|\bskills\b)`)
	stopHeadingPattern := regexp.MustCompile(`(?i)(about|responsibilit|what you'll do|what you will do|benefit|perks|compensation|salary|about the role|company)`)
	descriptionHeadingPattern := regexp.MustCompile(`(?i)(overview|about|job description|role summary|summary|duties|responsibilit|what you'll do|what you will do)`)

	sectionFromHeading := func(value string) string {
		normalized := normalizeHeadingText(value)
		switch {
		case requirementHeadingPattern.MatchString(normalized):
			return "requirements"
		case descriptionHeadingPattern.MatchString(normalized):
			return "description"
		default:
			return ""
		}
	}

	cleanLines := func(text string) []string {
		lines := strings.Split(text, "\n")
		out := []string{}
		seen := map[string]struct{}{}
		for _, line := range lines {
			line = strings.TrimSpace(spacePattern.ReplaceAllString(line, " "))
			if line == "" {
				continue
			}
			key := strings.ToLower(line)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, line)
		}
		return out
	}

	if strings.Contains(descriptionText, "<") && strings.Contains(descriptionText, ">") {
		if req, desc := extractRoleRequirementsFromHTML(descriptionText, sectionFromHeading, cleanLines, requirementHeadingPattern, stopHeadingPattern); req != nil || desc != nil {
			return req, desc
		}
	}

	plainText := descriptionText
	if strings.Contains(descriptionText, "<") && strings.Contains(descriptionText, ">") {
		plainText = normalizeHTMLForSectionParsing(descriptionText)
	}
	lines := strings.Split(plainText, "\n")
	cleanedLines := make([]string, 0, len(lines))
	requirementLines := []string{}
	capturing := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		normalized := normalizeHeadingText(trimmed)
		if requirementHeadingPattern.MatchString(normalized) && len(trimmed) <= 80 {
			remainder := strings.TrimSpace(regexp.MustCompile(`(?i)^(requirements|requirement|qualifications|qualification)\s*[:\-]\s*`).ReplaceAllString(trimmed, ""))
			capturing = true
			if remainder != "" && strings.ToLower(remainder) != strings.ToLower(trimmed) {
				requirementLines = append(requirementLines, remainder)
			}
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

func extractRoleRequirementsFromHTML(
	descriptionHTML string,
	sectionFromHeading func(string) string,
	cleanLines func(string) []string,
	requirementHeadingPattern *regexp.Regexp,
	stopHeadingPattern *regexp.Regexp,
) (any, any) {
	doc, err := nethtml.Parse(strings.NewReader(descriptionHTML))
	if err != nil {
		return nil, nil
	}

	type token struct {
		kind string
		text string
	}

	isBlockTag := func(tag string) bool {
		switch tag {
		case "p", "li", "div", "ul", "ol":
			return true
		default:
			return false
		}
	}

	isHeadingTag := func(tag string) bool {
		return len(tag) == 2 && tag[0] == 'h' && tag[1] >= '1' && tag[1] <= '6'
	}

	textContent := func(n *nethtml.Node) string {
		var b strings.Builder
		var walk func(*nethtml.Node)
		walk = func(node *nethtml.Node) {
			if node.Type == nethtml.TextNode {
				b.WriteString(node.Data)
			}
			if node.Type == nethtml.ElementNode && strings.EqualFold(node.Data, "br") {
				b.WriteString("\n")
			}
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				walk(child)
			}
			if node.Type == nethtml.ElementNode && isBlockTag(strings.ToLower(node.Data)) {
				b.WriteString("\n")
			}
		}
		walk(n)
		return b.String()
	}

	var tokens []token
	var walkTokens func(*nethtml.Node)
	walkTokens = func(node *nethtml.Node) {
		if node.Type == nethtml.ElementNode {
			tag := strings.ToLower(node.Data)
			if tag == "b" || tag == "strong" {
				heading := strings.TrimSpace(spacePattern.ReplaceAllString(textContent(node), " "))
				if heading != "" {
					tokens = append(tokens, token{kind: "heading", text: heading})
				}
				return
			}
			if tag == "br" {
				tokens = append(tokens, token{kind: "newline"})
				return
			}
			if isBlockTag(tag) {
				tokens = append(tokens, token{kind: "newline"})
			}
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				walkTokens(child)
			}
			if isBlockTag(tag) {
				tokens = append(tokens, token{kind: "newline"})
			}
			return
		}
		if node.Type == nethtml.TextNode {
			if strings.TrimSpace(node.Data) != "" {
				tokens = append(tokens, token{kind: "text", text: node.Data})
			}
		}
	}
	walkTokens(doc)

	inlineDescription := []string{}
	inlineRequirements := []string{}
	currentSection := ""
	lineParts := []string{}

	flushLine := func() {
		if len(lineParts) == 0 {
			return
		}
		line := strings.TrimSpace(spacePattern.ReplaceAllString(strings.Join(lineParts, " "), " "))
		lineParts = nil
		if line == "" {
			return
		}
		switch currentSection {
		case "requirements":
			inlineRequirements = append(inlineRequirements, line)
		case "description":
			inlineDescription = append(inlineDescription, line)
		}
	}

	for _, tok := range tokens {
		switch tok.kind {
		case "heading":
			flushLine()
			currentSection = sectionFromHeading(tok.text)
		case "newline":
			flushLine()
		case "text":
			lineParts = append(lineParts, tok.text)
		}
	}
	flushLine()

	if len(inlineDescription) > 0 || len(inlineRequirements) > 0 {
		desc := cleanLines(strings.Join(inlineDescription, "\n"))
		req := cleanLines(strings.Join(inlineRequirements, "\n"))
		var descText any
		var reqText any
		if len(desc) > 0 {
			descText = strings.Join(desc, "\n")
		}
		if len(req) > 0 {
			reqText = strings.Join(req, "\n")
		}
		return reqText, descText
	}

	blocks := []*nethtml.Node{}
	var collectBlocks func(*nethtml.Node)
	collectBlocks = func(node *nethtml.Node) {
		if node.Type == nethtml.ElementNode {
			tag := strings.ToLower(node.Data)
			if isBlockTag(tag) {
				blocks = append(blocks, node)
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			collectBlocks(child)
		}
	}
	collectBlocks(doc)

	blockDescription := []string{}
	blockRequirements := []string{}
	currentSection = ""

	for _, block := range blocks {
		tag := strings.ToLower(block.Data)
		headingText, remainder := extractBoldHeadingFromBlock(block, textContent, cleanLines)
		if headingText != "" {
			currentSection = sectionFromHeading(headingText)
			if remainder != "" && (currentSection == "requirements" || currentSection == "description") {
				lines := cleanLines(remainder)
				if currentSection == "requirements" {
					blockRequirements = append(blockRequirements, lines...)
				} else {
					blockDescription = append(blockDescription, lines...)
				}
			}
			continue
		}

		if currentSection != "requirements" && currentSection != "description" {
			continue
		}
		if tag == "div" || tag == "ul" || tag == "ol" {
			continue
		}
		lines := cleanLines(textContent(block))
		if currentSection == "requirements" {
			blockRequirements = append(blockRequirements, lines...)
		} else {
			blockDescription = append(blockDescription, lines...)
		}
	}

	if len(blockDescription) > 0 || len(blockRequirements) > 0 {
		var descText any
		var reqText any
		if len(blockDescription) > 0 {
			descText = strings.Join(cleanLines(strings.Join(blockDescription, "\n")), "\n")
		}
		if len(blockRequirements) > 0 {
			reqText = strings.Join(cleanLines(strings.Join(blockRequirements, "\n")), "\n")
		}
		return reqText, descText
	}

	var walkHeadings func(*nethtml.Node)
	var reqLines []string
	var descText any

	walkHeadings = func(node *nethtml.Node) {
		if node.Type == nethtml.ElementNode && isHeadingTag(strings.ToLower(node.Data)) {
			headingText := strings.TrimSpace(spacePattern.ReplaceAllString(textContent(node), " "))
			if headingText != "" && requirementHeadingPattern.MatchString(normalizeHeadingText(headingText)) {
				// Collect following siblings until the next heading or stop heading.
				collected := []string{}
				for sib := node.NextSibling; sib != nil; sib = sib.NextSibling {
					if sib.Type == nethtml.ElementNode && isHeadingTag(strings.ToLower(sib.Data)) {
						sibHeading := strings.TrimSpace(spacePattern.ReplaceAllString(textContent(sib), " "))
						if sibHeading != "" && stopHeadingPattern.MatchString(normalizeHeadingText(sibHeading)) {
							break
						}
						break
					}
					text := strings.TrimSpace(textContent(sib))
					if text != "" {
						collected = append(collected, cleanLines(text)...)
					}
				}
				if len(collected) > 0 {
					reqLines = append(reqLines, collected...)
					descText = strings.TrimSpace(textContent(doc))
					return
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walkHeadings(child)
		}
	}
	walkHeadings(doc)

	if len(reqLines) > 0 {
		return strings.Join(dedupeLines(reqLines), "\n"), descText
	}
	return nil, nil
}

func extractBoldHeadingFromBlock(
	node *nethtml.Node,
	textContent func(*nethtml.Node) string,
	cleanLines func(string) []string,
) (string, string) {
	if node == nil || node.Type != nethtml.ElementNode {
		return "", ""
	}
	if node.Data != "p" && node.Data != "li" && node.Data != "div" {
		return "", ""
	}

	firstTag := (*nethtml.Node)(nil)
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if child.Type == nethtml.TextNode && strings.TrimSpace(child.Data) != "" {
			return "", ""
		}
		if child.Type == nethtml.ElementNode {
			firstTag = child
			break
		}
	}
	if firstTag == nil || (firstTag.Data != "b" && firstTag.Data != "strong") {
		return "", ""
	}

	headingText := strings.TrimSpace(spacePattern.ReplaceAllString(textContent(firstTag), " "))
	if headingText == "" {
		return "", ""
	}
	fullText := textContent(node)
	lines := cleanLines(fullText)
	if len(lines) == 0 {
		return headingText, ""
	}
	normalizedHeading := normalizeHeadingText(headingText)
	if normalizeHeadingText(lines[0]) == normalizedHeading {
		lines = lines[1:]
	} else {
		filtered := []string{}
		for _, line := range lines {
			if normalizeHeadingText(line) == normalizedHeading {
				continue
			}
			filtered = append(filtered, line)
		}
		lines = filtered
	}
	return headingText, strings.Join(lines, "\n")
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
				if skill == "" || isNullLikeSkillToken(skill) {
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

func isNullLikeSkillToken(value string) bool {
	normalized := strings.TrimSpace(nullLikeTokenPattern.ReplaceAllString(strings.ToLower(value), " "))
	return normalized == "null" || normalized == "none" || normalized == "na" || normalized == "n a" || normalized == "unknown"
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
	tagline := extractCompanyTaglineFromMissionBlock(companyHTML)
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
	var externalCompanyID any
	if companySlug != "" {
		externalCompanyID = "builtin_company_" + companySlug
	}
	tagline := extractWhatWeDoTagline(jobPageHTML)
	employeeRange := normalizeEmployeeRange(matchOne(jobPageHTML, `(?is)([0-9][0-9,]*)\s+employees`))
	return map[string]any{
		"external_company_id":         externalCompanyID,
		"name":                        valueOrNil(companyName),
		"slug":                        valueOrNil(companySlug),
		"tagline":                     valueOrNil(tagline),
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

func extractWhatWeDoTagline(jobPageHTML string) string {
	if strings.TrimSpace(jobPageHTML) == "" {
		return ""
	}
	section := matchOne(jobPageHTML, `(?is)<h2[^>]*>\s*What\s+We\s+Do\s*</h2>(.*?)(?:<h2[^>]*>|$)`)
	if strings.TrimSpace(section) == "" {
		return ""
	}
	lines := []string{}
	seen := map[string]struct{}{}
	paragraphMatches := regexp.MustCompile(`(?is)<p[^>]*>(.*?)</p>`).FindAllStringSubmatch(section, -1)
	for _, match := range paragraphMatches {
		text := strings.TrimSpace(spacePattern.ReplaceAllString(html.UnescapeString(tagPattern.ReplaceAllString(match[1], " ")), " "))
		if text == "" {
			continue
		}
		key := strings.ToLower(text)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		lines = append(lines, text)
	}
	return strings.Join(lines, "\n")
}

func cleanTaglineText(value string) string {
	lines := []string{}
	for _, line := range strings.Split(value, "\n") {
		cleaned := strings.TrimSpace(spacePattern.ReplaceAllString(line, " "))
		if cleaned != "" {
			lines = append(lines, cleaned)
		}
	}
	return strings.Join(lines, "\n")
}

func extractTaglineTextWithBreaks(htmlText string) string {
	if strings.TrimSpace(htmlText) == "" {
		return ""
	}
	withBreaks := regexp.MustCompile(`(?is)<br\s*/?>`).ReplaceAllString(htmlText, "\n")
	withBreaks = regexp.MustCompile(`(?is)</(p|div|li|h[1-6])>`).ReplaceAllString(withBreaks, "\n")
	stripped := tagPattern.ReplaceAllString(withBreaks, "")
	return cleanTaglineText(html.UnescapeString(stripped))
}

func extractCompanyTaglineFromMissionBlock(companyHTML string) string {
	block := matchOne(companyHTML, `(?is)<div[^>]*x-ref=['"]companyMission['"][^>]*>(.*?)</div>`)
	if strings.TrimSpace(block) == "" {
		return ""
	}
	paragraphs := regexp.MustCompile(`(?is)<p[^>]*>(.*?)</p>`).FindAllStringSubmatch(block, -1)
	if len(paragraphs) > 0 {
		lines := []string{}
		for _, paragraph := range paragraphs {
			if len(paragraph) < 2 {
				continue
			}
			text := extractTaglineTextWithBreaks(paragraph[1])
			if text != "" {
				lines = append(lines, text)
			}
		}
		return cleanTaglineText(strings.Join(lines, "\n"))
	}
	return extractTaglineTextWithBreaks(block)
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
	normalizedLabel := strings.ToLower(strings.TrimSpace(seniorityLabel))
	normalizedLabel = spacePattern.ReplaceAllString(normalizedLabel, " ")
	normalizedLabel = strings.ReplaceAll(normalizedLabel, " / ", "/")
	normalizedLabel = strings.ReplaceAll(normalizedLabel, "/ ", "/")
	normalizedLabel = strings.ReplaceAll(normalizedLabel, " /", "/")
	switch normalizedLabel {
	case "entry level":
		return map[string]bool{"isEntryLevel": true, "isJunior": false, "isMidLevel": false, "isSenior": false, "isLead": false}
	case "junior level", "junior":
		return map[string]bool{"isEntryLevel": false, "isJunior": true, "isMidLevel": false, "isSenior": false, "isLead": false}
	case "mid level":
		return map[string]bool{"isEntryLevel": false, "isJunior": false, "isMidLevel": true, "isSenior": false, "isLead": false}
	case "senior level":
		return map[string]bool{"isEntryLevel": false, "isJunior": false, "isMidLevel": false, "isSenior": true, "isLead": false}
	case "expert / leader", "expert/leader":
		return map[string]bool{"isEntryLevel": false, "isJunior": false, "isMidLevel": false, "isSenior": false, "isLead": true}
	}
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(roleTitle), " ")
	tokens := map[string]struct{}{}
	for _, token := range strings.Fields(strings.TrimSpace(normalized)) {
		tokens[token] = struct{}{}
	}
	_, hasEntry := tokens["entry"]
	_, hasIntern := tokens["intern"]
	_, hasJunior := tokens["junior"]
	_, hasJr := tokens["jr"]
	_, hasMid := tokens["mid"]
	_, hasSenior := tokens["senior"]
	_, hasSr := tokens["sr"]
	_, hasLead := tokens["lead"]
	_, hasPrincipal := tokens["principal"]
	_, hasStaff := tokens["staff"]
	_, hasDirector := tokens["director"]
	_, hasManager := tokens["manager"]
	_, hasHead := tokens["head"]
	_, hasChief := tokens["chief"]
	return map[string]bool{
		"isEntryLevel": hasEntry || hasIntern,
		"isJunior":     hasJunior || hasJr,
		"isMidLevel":   hasMid,
		"isSenior":     hasSenior || hasSr,
		"isLead":       hasLead || hasPrincipal || hasStaff || hasDirector || hasHead || hasChief || hasManager,
	}
}

func extractSeniorityLabel(htmlText string) string {
	for _, match := range senioritySpanRegex.FindAllStringSubmatch(htmlText, -1) {
		if len(match) < 2 {
			continue
		}
		spanText := strings.TrimSpace(spacePattern.ReplaceAllString(html.UnescapeString(tagPattern.ReplaceAllString(match[1], " ")), " "))
		if spanText == "" {
			continue
		}
		if labelMatch := seniorityRegex.FindStringSubmatch(spanText); len(labelMatch) >= 2 {
			return strings.TrimSpace(spacePattern.ReplaceAllString(labelMatch[1], " "))
		}
	}
	return ""
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
		country := locationnorm.NormalizeCountryName(stringValue(address["addressCountry"]), true)
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

func extractLocationFields(jobPosting map[string]any) (any, any, any, any) {
	locations, _ := jobPosting["jobLocation"].([]any)
	if locations == nil {
		if one, ok := jobPosting["jobLocation"].(map[string]any); ok {
			locations = []any{one}
		}
	}

	type locEntry struct {
		locality string
		region   string
		country  string
	}

	locationEntries := make([]locEntry, 0, len(locations))
	usStates := []string{}
	usCountryTokens := map[string]struct{}{"USA": {}, "US": {}, "UNITED STATES": {}}

	isRemoteToken := func(value string) bool {
		normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), " ")
		normalized = strings.TrimSpace(normalized)
		return normalized == "remote" || strings.HasPrefix(normalized, "remote ")
	}
	isNullLike := func(value string) bool {
		normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), " ")
		normalized = strings.TrimSpace(normalized)
		switch normalized {
		case "null", "none", "na", "n a", "unknown":
			return true
		default:
			return false
		}
	}
	isCountryLike := func(value string) bool {
		normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), " ")
		normalized = strings.TrimSpace(normalized)
		switch normalized {
		case "us", "usa", "united states", "united states of america":
			return true
		default:
			return false
		}
	}
	stripRemotePrefix := func(value string) string {
		if strings.TrimSpace(value) == "" {
			return ""
		}
		parts := strings.Split(value, ",")
		filtered := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" || isRemoteToken(part) || isNullLike(part) {
				continue
			}
			filtered = append(filtered, part)
		}
		if len(filtered) == 0 {
			return ""
		}
		return filtered[0]
	}
	isUSCountry := func(value string) bool {
		if strings.TrimSpace(value) == "" {
			return false
		}
		_, ok := usCountryTokens[strings.ToUpper(strings.TrimSpace(value))]
		return ok
	}

	for _, location := range locations {
		entry, _ := location.(map[string]any)
		address, _ := entry["address"].(map[string]any)
		locality := stripRemotePrefix(stringValue(address["addressLocality"]))
		region := stripRemotePrefix(stringValue(address["addressRegion"]))
		if isCountryLike(locality) {
			locality = ""
		}
		if isCountryLike(region) || isNullLike(region) {
			region = ""
		}
		country := stringValue(address["addressCountry"])
		locationEntries = append(locationEntries, locEntry{locality: locality, region: region, country: country})
		if region != "" && !isRemoteToken(region) && isUSCountry(country) {
			seen := false
			for _, value := range usStates {
				if value == region {
					seen = true
					break
				}
			}
			if !seen {
				usStates = append(usStates, region)
			}
		}
	}

	localities := []string{}
	usLocalities := []string{}
	countriesFromLocations := []string{}
	for _, entry := range locationEntries {
		if entry.locality != "" && !isRemoteToken(entry.locality) {
			if !containsString(localities, entry.locality) {
				localities = append(localities, entry.locality)
			}
			if isUSCountry(entry.country) && !containsString(usLocalities, entry.locality) {
				usLocalities = append(usLocalities, entry.locality)
			}
		}
		if entry.country != "" && !containsString(countriesFromLocations, entry.country) {
			countriesFromLocations = append(countriesFromLocations, entry.country)
		}
	}
	cityValues := localities
	if len(usLocalities) > 0 {
		cityValues = usLocalities
	}
	locationCity := any(nil)
	if len(cityValues) > 0 {
		locationCity = strings.Join(cityValues, ", ")
	}

	applicantLocation := jobPosting["applicantLocationRequirements"]
	applicantLocations, _ := applicantLocation.([]any)
	if applicantLocations == nil {
		if one, ok := applicantLocation.(map[string]any); ok {
			applicantLocations = []any{one}
		}
	}
	applicantCountries := []string{}
	for _, item := range applicantLocations {
		entry, _ := item.(map[string]any)
		name := stringValue(entry["name"])
		if name != "" && !containsString(applicantCountries, name) {
			applicantCountries = append(applicantCountries, name)
		}
	}
	applicantCountryLabel := ""
	if len(applicantCountries) > 0 {
		applicantCountryLabel = applicantCountries[0]
	}

	hasUSLocation := false
	for _, country := range countriesFromLocations {
		if isUSCountry(country) {
			hasUSLocation = true
			break
		}
	}
	hasUSApplicant := false
	for _, country := range applicantCountries {
		if isUSCountry(country) {
			hasUSApplicant = true
			break
		}
	}

	primaryCountry := ""
	switch {
	case hasUSLocation || hasUSApplicant:
		primaryCountry = "United States"
	case len(countriesFromLocations) > 0:
		primaryCountry = locationnorm.NormalizeCountryName(countriesFromLocations[0], true)
	case applicantCountryLabel != "":
		primaryCountry = locationnorm.NormalizeCountryName(applicantCountryLabel, true)
	}
	locationLabel := any(nil)
	if strings.TrimSpace(primaryCountry) != "" {
		locationLabel = primaryCountry
	}

	locationCountries := []string{}
	appendCountry := func(value string) {
		normalized := locationnorm.NormalizeCountryName(value, true)
		if normalized == "" || containsString(locationCountries, normalized) {
			return
		}
		locationCountries = append(locationCountries, normalized)
	}
	for _, country := range countriesFromLocations {
		appendCountry(country)
	}
	for _, country := range applicantCountries {
		appendCountry(country)
	}

	return locationLabel, locationCity, usStates, locationCountries
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func normalizeHTMLForSectionParsing(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	withBreaks := regexp.MustCompile(`(?is)<br\s*/?>`).ReplaceAllString(value, "\n")
	withBreaks = regexp.MustCompile(`(?is)</(p|div|li|h[1-6])>`).ReplaceAllString(withBreaks, "\n")
	withoutTags := tagPattern.ReplaceAllString(withBreaks, "")
	cleaned := html.UnescapeString(withoutTags)
	lines := strings.Split(cleaned, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(spacePattern.ReplaceAllString(line, " "))
		if line != "" {
			out = append(out, line)
		}
	}
	cleaned = strings.Join(out, "\n")
	return strings.TrimSpace(cleaned)
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
		if isNullLikeToken(region) {
			continue
		}
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

func extractLocationCountries(jobPosting map[string]any) []string {
	locations, _ := jobPosting["jobLocation"].([]any)
	if locations == nil {
		if one, ok := jobPosting["jobLocation"].(map[string]any); ok {
			locations = []any{one}
		}
	}
	countries := []string{}
	seen := map[string]struct{}{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		countries = append(countries, value)
	}
	for _, location := range locations {
		entry, _ := location.(map[string]any)
		address, _ := entry["address"].(map[string]any)
		add(locationnorm.NormalizeCountryName(stringValue(address["addressCountry"]), true))
	}
	if applicantCountry := stringValueFromMap(jobPosting, "applicantLocationRequirements", "name"); applicantCountry != "" {
		add(locationnorm.NormalizeCountryName(applicantCountry, true))
	}
	return countries
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

func extractBuiltinCompanySlugFromURL(value string) string {
	slug := strings.ToLower(strings.TrimSpace(companySlugFromURL(value)))
	if slug == "" {
		return ""
	}
	slug = regexp.MustCompile(`[^a-z0-9-]+`).ReplaceAllString(slug, "-")
	return strings.Trim(slug, "-")
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
