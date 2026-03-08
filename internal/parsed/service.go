package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"goapplyjob-golang-backend/internal/database"
)

const (
	sourceRemoteRocketship = "remoterocketship"
	sourceBuiltin          = "builtin"
	envGroqAPIKey          = "GROQ_API_KEY"
	envGroqModel           = "GROQ_MODEL"
	defaultGroqModel       = "meta-llama/llama-guard-4-12b"
)

var seniorityTokens = map[string]struct{}{
	"senior": {}, "sr": {}, "junior": {}, "jr": {}, "lead": {}, "principal": {}, "staff": {}, "entry": {}, "mid": {}, "expert": {}, "leader": {}, "level": {},
}

var genericCategoryMatchTokens = map[string]struct{}{
	"accountant": {}, "administrator": {}, "engineer": {}, "developer": {}, "manager": {}, "specialist": {}, "consultant": {}, "analyst": {}, "architect": {}, "designer": {}, "director": {}, "producer": {}, "writer": {}, "support": {}, "operations": {}, "web": {}, "remote": {}, "lead": {}, "staff": {},
}

var countryAliases = map[string]string{
	"united states":  "United States",
	"usa":            "United States",
	"us":             "United States",
	"u.s.":           "United States",
	"u.s.a.":         "United States",
	"uk":             "United Kingdom",
	"gbr":            "United Kingdom",
	"united kingdom": "United Kingdom",
	"england":        "United Kingdom",
	"deu":            "Germany",
	"germany":        "Germany",
	"fra":            "France",
	"france":         "France",
	"esp":            "Spain",
	"spain":          "Spain",
	"pol":            "Poland",
	"poland":         "Poland",
	"belgium":        "Belgium",
	"switzerland":    "Switzerland",
	"netherlands":    "Netherlands",
}

var techStackAliases = map[string]string{
	"nodejs":                   "Node.js",
	"node.js":                  "Node.js",
	"node js":                  "Node.js",
	"reactjs":                  "React",
	"react.js":                 "React",
	"nextjs":                   "Next.js",
	"next.js":                  "Next.js",
	"vuejs":                    "Vue.js",
	"vue.js":                   "Vue.js",
	"angularjs":                "AngularJS",
	"javascript":               "JavaScript",
	"typescript":               "TypeScript",
	"c#":                       "C#",
	"csharp":                   "C#",
	"c++":                      "C++",
	"cplusplus":                "C++",
	"golang":                   "Go",
	"postgres":                 "PostgreSQL",
	"postgresql":               "PostgreSQL",
	"mongodb":                  "MongoDB",
	"graphql":                  "GraphQL",
	"graph ql":                 "GraphQL",
	"rest api":                 "REST API",
	"restful api":              "REST API",
	"rest apis":                "REST API",
	"restful apis":             "REST API",
	"apis":                     "API",
	"aws":                      "AWS",
	"gcp":                      "GCP",
	"azure":                    "Azure",
	".net":                     ".NET",
	"dotnet":                   ".NET",
	"asp.net":                  "ASP.NET",
	"asp.net core":             "ASP.NET Core",
	"grpc":                     "gRPC",
	"json":                     "JSON",
	"xml":                      "XML",
	"html":                     "HTML",
	"html5":                    "HTML5",
	"css":                      "CSS",
	"css3":                     "CSS3",
	"scss":                     "SCSS",
	"sass":                     "Sass",
	"sql":                      "SQL",
	"nosql":                    "NoSQL",
	"no-sql":                   "NoSQL",
	"etl":                      "ETL",
	"elt":                      "ELT",
	"etl/elt":                  "ETL/ELT",
	"ci/cd":                    "CI/CD",
	"cicd":                     "CI/CD",
	"iac":                      "IaC",
	"infrastructure as code":   "Infrastructure as Code",
	"k8s":                      "Kubernetes",
	"kubernetes (k8s)":         "Kubernetes",
	"tailwindcss":              "Tailwind CSS",
	"tailwind css":             "Tailwind CSS",
	"google tag manager (gtm)": "Google Tag Manager",
	"google tag manager":       "Google Tag Manager",
	"gtm":                      "Google Tag Manager",
	"sfdc":                     "Salesforce",
	"sfdc crm":                 "Salesforce",
	"salesforce.com":           "Salesforce",
	"salesforce crm":           "Salesforce",
}

var techStackDropValues = map[string]struct{}{
	"n/a": {}, "na": {}, "none": {}, "null": {}, "unknown": {}, "tbd": {},
}

var normalizationReplacements = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{pattern: regexp.MustCompile(`\bdev[\s\-]*ops\b`), replacement: "devops"},
	{pattern: regexp.MustCompile(`\bdev\b`), replacement: "developer"},
	{pattern: regexp.MustCompile(`\bbdr\b`), replacement: "business development representative"},
	{pattern: regexp.MustCompile(`\bsdr\b`), replacement: "sales development representative"},
	{pattern: regexp.MustCompile(`\bswe\b`), replacement: "software engineer"},
	{pattern: regexp.MustCompile(`\bvp\b`), replacement: "vice president"},
	{pattern: regexp.MustCompile(`\bta\b`), replacement: "talent acquisition"},
	{pattern: regexp.MustCompile(`\bhr\b`), replacement: "human resources"},
	{pattern: regexp.MustCompile(`\btalent acquisition\b`), replacement: "recruitment human resources"},
	{pattern: regexp.MustCompile(`\bcpg\b`), replacement: "consumer packaged goods"},
}

var (
	jobTitlePromptOnce    sync.Once
	jobTitlePromptText    string
	jobTitlePromptLoaded  bool
	jobTitlePromptLoadErr error
)

type Service struct {
	DB *database.DB
}

func New(db *database.DB) *Service { return &Service{DB: db} }

func (s *Service) SuggestCategory(ctx context.Context, source, roleTitle, roleDescription string, techStack any) (string, string, error) {
	normalizedTechStack := normalizeTechStack(techStack)
	categorizedTitle := ""
	categorizedFunction := ""

	if strings.TrimSpace(source) == sourceBuiltin && len(normalizedTechStack) == 0 {
		categorizedTitle = strings.TrimSpace(classifyJobTitleWithGroqSync(roleTitle, roleDescription))
	}
	if categorizedTitle == "" {
		inferredTitle, inferredFunction, err := s.findSimilarRemoteCategories(ctx, roleTitle, nil)
		if err != nil {
			return "", "", err
		}
		categorizedTitle = strings.TrimSpace(inferredTitle)
		categorizedFunction = strings.TrimSpace(inferredFunction)
	}
	if categorizedTitle != "" && categorizedFunction == "" {
		resolvedFunction, err := s.resolveJobFunctionForCategory(ctx, categorizedTitle)
		if err != nil {
			return "", "", err
		}
		categorizedFunction = strings.TrimSpace(resolvedFunction)
	}
	return categorizedTitle, categorizedFunction, nil
}

func parseDT(value any) *time.Time {
	raw, ok := value.(string)
	if !ok || raw == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return &parsed
	}
	return nil
}

func normalizeDT(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func isSourceOlderThanPostDate(sourceCreatedAt, postDate *time.Time) bool {
	source := normalizeDT(sourceCreatedAt)
	post := normalizeDT(postDate)
	if source == nil || post == nil {
		return false
	}
	return source.Before(*post)
}

func parseDBDatetime(value string) (*time.Time, error) {
	if value == "" {
		return nil, errors.New("empty")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return &parsed, nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return &parsed, nil
	}
	return nil, errors.New("invalid datetime")
}

func normalizeTextForMatching(value string) string {
	normalized := strings.ToLower(value)
	for _, replacement := range normalizationReplacements {
		normalized = replacement.pattern.ReplaceAllString(normalized, replacement.replacement)
	}
	return normalized
}

func tokenizeRoleTitleForSimilarity(roleTitle string) map[string]struct{} {
	rawTokens := regexp.MustCompile(`[^a-z0-9]+`).Split(normalizeTextForMatching(roleTitle), -1)
	out := map[string]struct{}{}
	for _, token := range rawTokens {
		if len(token) <= 1 {
			continue
		}
		if _, ok := seniorityTokens[token]; ok {
			continue
		}
		out[token] = struct{}{}
	}
	return out
}

func tokenizeTextForSequence(value string) []string {
	rawTokens := regexp.MustCompile(`[^a-z0-9]+`).Split(normalizeTextForMatching(value), -1)
	out := make([]string, 0, len(rawTokens))
	for _, token := range rawTokens {
		if len(token) <= 1 {
			continue
		}
		if _, ok := seniorityTokens[token]; ok {
			continue
		}
		out = append(out, token)
	}
	return out
}

func normalizeRoleTitleForExactMatch(value string) string {
	return strings.Join(tokenizeTextForSequence(value), " ")
}

func jaccardSimilarity(left, right map[string]struct{}) float64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	intersection := 0
	union := map[string]struct{}{}
	for token := range left {
		union[token] = struct{}{}
		if _, ok := right[token]; ok {
			intersection++
		}
	}
	for token := range right {
		union[token] = struct{}{}
	}
	if len(union) == 0 {
		return 0
	}
	return float64(intersection) / float64(len(union))
}

func orderedTokenMatchScore(roleTitle, categoryTitle string) float64 {
	roleTokens := orderedTokens(roleTitle)
	categoryTokens := orderedTokens(categoryTitle)
	if len(roleTokens) == 0 || len(categoryTokens) == 0 {
		return 0
	}
	matched := 0
	idx := 0
	for _, categoryToken := range categoryTokens {
		for idx < len(roleTokens) {
			if roleTokens[idx] == categoryToken {
				matched++
				idx++
				break
			}
			idx++
		}
	}
	return float64(matched) / float64(len(categoryTokens))
}

func orderedTokens(value string) []string {
	raw := regexp.MustCompile(`[^a-z0-9]+`).Split(normalizeTextForMatching(value), -1)
	out := make([]string, 0, len(raw))
	for _, token := range raw {
		if len(token) <= 1 {
			continue
		}
		if _, ok := seniorityTokens[token]; ok {
			continue
		}
		out = append(out, token)
	}
	return out
}

func (s *Service) findSimilarRemoteCategories(ctx context.Context, roleTitle string, sourceTechStack []string) (string, string, error) {
	sourceTokens := tokenizeRoleTitleForSimilarity(roleTitle)
	if len(sourceTokens) == 0 {
		return "", "", nil
	}
	sourceSequenceTokens := tokenizeTextForSequence(roleTitle)
	sourceExactTitle := normalizeRoleTitleForExactMatch(roleTitle)
	prioritizedTokens := make([]string, 0, len(sourceSequenceTokens))
	seenTokens := map[string]struct{}{}
	for _, token := range sourceSequenceTokens {
		if _, seen := seenTokens[token]; seen {
			continue
		}
		seenTokens[token] = struct{}{}
		prioritizedTokens = append(prioritizedTokens, token)
	}
	sort.SliceStable(prioritizedTokens, func(i, j int) bool {
		leftGeneric := isGenericCategoryToken(prioritizedTokens[i])
		rightGeneric := isGenericCategoryToken(prioritizedTokens[j])
		if leftGeneric != rightGeneric {
			return !leftGeneric
		}
		return len(prioritizedTokens[i]) > len(prioritizedTokens[j])
	})

	normalizedSkillValues := make([]string, 0, len(sourceTechStack))
	seenSkillValues := map[string]struct{}{}
	for _, value := range sourceTechStack {
		normalized := strings.TrimSpace(strings.ToLower(value))
		if normalized == "" {
			continue
		}
		if _, exists := seenSkillValues[normalized]; exists {
			continue
		}
		seenSkillValues[normalized] = struct{}{}
		normalizedSkillValues = append(normalizedSkillValues, normalized)
	}

	if sourceExactTitle != "" {
		for _, applySkillFilter := range []bool{true, false} {
			if applySkillFilter && len(normalizedSkillValues) == 0 {
				continue
			}
			title, function, err := s.findExactNormalizedCategoryMatch(ctx, sourceExactTitle, normalizedSkillValues, applySkillFilter)
			if err != nil {
				return "", "", err
			}
			if title != "" {
				return title, function, nil
			}
		}
	}

	buildQuery := func(applySkillFilter bool) (string, []any) {
		query := `SELECT p.role_title, p.categorized_job_title, p.categorized_job_function
		FROM parsed_jobs p
		JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		WHERE r.source = ? AND p.role_title IS NOT NULL AND p.categorized_job_title IS NOT NULL`
		args := []any{sourceRemoteRocketship}
		if len(prioritizedTokens) > 0 {
			conditions := make([]string, 0, min(len(prioritizedTokens), 5))
			for _, token := range prioritizedTokens[:min(len(prioritizedTokens), 5)] {
				conditions = append(conditions, `(LOWER(p.role_title) LIKE ? OR LOWER(p.categorized_job_title) LIKE ? OR LOWER(COALESCE(p.categorized_job_function, '')) LIKE ?)`)
				like := "%" + token + "%"
				args = append(args, like, like, like)
			}
			query += " AND (" + strings.Join(conditions, " OR ") + ")"
		}
		if applySkillFilter && len(normalizedSkillValues) > 0 {
			conditions := make([]string, 0, len(normalizedSkillValues))
			for _, skill := range normalizedSkillValues {
				conditions = append(conditions, `LOWER(COALESCE(p.tech_stack, '')) LIKE ?`)
				args = append(args, `%`+"\""+skill+"\""+"%")
			}
			query += " AND (" + strings.Join(conditions, " OR ") + ")"
		}
		query += " ORDER BY p.updated_at DESC, p.id DESC LIMIT 1000"
		return query, args
	}

	scanWithFilter := func(applySkillFilter bool) (string, string, float64, error) {
		query, args := buildQuery(applySkillFilter)
		rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
		if err != nil {
			return "", "", 0, err
		}
		defer rows.Close()
		bestScore := 0.0
		bestTitle := ""
		bestFunction := ""
		for rows.Next() {
			var candidateRoleTitle, candidateTitle sql.NullString
			var candidateFunction sql.NullString
			if err := rows.Scan(&candidateRoleTitle, &candidateTitle, &candidateFunction); err != nil {
				return "", "", 0, err
			}
			score := jaccardSimilarity(sourceTokens, tokenizeRoleTitleForSimilarity(candidateRoleTitle.String))
			titleTokens := orderedTokens(candidateTitle.String)
			score += orderedTokenMatchScore(roleTitle, candidateTitle.String)
			score += 0.1 * float64(len(titleTokens))
			if normalizeRoleTitleForExactMatch(candidateRoleTitle.String) == sourceExactTitle {
				score += 0.5
			}
			if strings.EqualFold(candidateTitle.String, "Engineer") || strings.EqualFold(candidateTitle.String, "Manager") {
				score -= 0.35
			}
			if score > bestScore {
				bestScore = score
				bestTitle = candidateTitle.String
				bestFunction = candidateFunction.String
			}
		}
		if err := rows.Err(); err != nil {
			return "", "", 0, err
		}
		return bestTitle, bestFunction, bestScore, nil
	}

	for _, applySkillFilter := range []bool{true, false} {
		if applySkillFilter && len(normalizedSkillValues) == 0 {
			continue
		}
		bestTitle, bestFunction, bestScore, err := scanWithFilter(applySkillFilter)
		if err != nil {
			return "", "", err
		}
		if bestScore >= 0.5 {
			return bestTitle, bestFunction, nil
		}
	}
	return "", "", nil
}

func loadJobTitleClassificationPrompt() (string, error) {
	jobTitlePromptOnce.Do(func() {
		path := filepath.Join("internal", "sources", "prompts", "job_title_classification.txt")
		raw, err := os.ReadFile(path)
		if err != nil {
			jobTitlePromptLoadErr = err
			jobTitlePromptLoaded = true
			return
		}
		jobTitlePromptText = string(raw)
		jobTitlePromptLoaded = true
	})
	if !jobTitlePromptLoaded {
		return "", errors.New("classification prompt not loaded")
	}
	if jobTitlePromptLoadErr != nil {
		return "", jobTitlePromptLoadErr
	}
	return jobTitlePromptText, nil
}

func buildJobTitleClassificationPrompt(jobTitle, jobDescription string) string {
	template, err := loadJobTitleClassificationPrompt()
	if err != nil || strings.TrimSpace(template) == "" {
		return ""
	}
	prompt := strings.ReplaceAll(template, "{{JOB_TITLE}}", strings.TrimSpace(jobTitle))
	return strings.ReplaceAll(prompt, "{{JOB_DESCRIPTION}}", strings.TrimSpace(jobDescription))
}

func extractJSONPayload(rawContent string) map[string]any {
	content := strings.TrimSpace(rawContent)
	if content == "" {
		return nil
	}
	if strings.Contains(content, "```") {
		re := regexp.MustCompile("(?is)```(?:json)?\\s*(\\{.*\\})\\s*```")
		if match := re.FindStringSubmatch(content); len(match) == 2 {
			content = strings.TrimSpace(match[1])
		}
	}
	if !(strings.HasPrefix(content, "{") && strings.HasSuffix(content, "}")) {
		start := strings.Index(content, "{")
		end := strings.LastIndex(content, "}")
		if start < 0 || end <= start {
			return nil
		}
		content = content[start : end+1]
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return nil
	}
	return payload
}

func classifyJobTitleWithGroqSync(jobTitle, jobDescription string) string {
	normalizedTitle := strings.TrimSpace(jobTitle)
	if normalizedTitle == "" {
		return ""
	}
	// Prompt and env are loaded for scaffolding; invocation intentionally disabled for now.
	if strings.TrimSpace(os.Getenv(envGroqAPIKey)) == "" {
		return ""
	}
	model := strings.TrimSpace(os.Getenv(envGroqModel))
	if model == "" {
		model = defaultGroqModel
	}
	_ = model
	_ = buildJobTitleClassificationPrompt(normalizedTitle, jobDescription)
	return ""
}

func (s *Service) resolveJobFunctionForCategory(ctx context.Context, category string) (string, error) {
	normalized := strings.TrimSpace(category)
	if normalized == "" {
		return "", nil
	}
	var jobFunction sql.NullString
	err := s.DB.SQL.QueryRowContext(ctx,
		`SELECT categorized_job_function
		 FROM parsed_jobs
		 WHERE categorized_job_title = ?
		   AND categorized_job_function IS NOT NULL
		   AND categorized_job_function != ''
		 GROUP BY categorized_job_function
		 ORDER BY COUNT(id) DESC, categorized_job_function ASC
		 LIMIT 1`, normalized).Scan(&jobFunction)
	if err != nil {
		return "", nil
	}
	return strings.TrimSpace(jobFunction.String), nil
}

func (s *Service) findExactNormalizedCategoryMatch(ctx context.Context, normalizedRoleTitle string, normalizedSkillValues []string, applySkillFilter bool) (string, string, error) {
	query := `SELECT p.role_title, p.categorized_job_title, p.categorized_job_function
		FROM parsed_jobs p
		JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		WHERE r.source = ? AND p.role_title IS NOT NULL AND p.categorized_job_title IS NOT NULL`
	args := []any{sourceRemoteRocketship}
	if applySkillFilter && len(normalizedSkillValues) > 0 {
		conditions := make([]string, 0, len(normalizedSkillValues))
		for _, skill := range normalizedSkillValues {
			conditions = append(conditions, `LOWER(COALESCE(p.tech_stack, '')) LIKE ?`)
			args = append(args, `%`+"\""+skill+"\""+"%")
		}
		query += " AND (" + strings.Join(conditions, " OR ") + ")"
	}
	query += " ORDER BY p.updated_at DESC, p.id DESC"
	rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return "", "", err
	}
	defer rows.Close()

	for rows.Next() {
		var roleTitle, title, function sql.NullString
		if err := rows.Scan(&roleTitle, &title, &function); err != nil {
			return "", "", err
		}
		if roleTitle.String == "" || title.String == "" {
			continue
		}
		if normalizeRoleTitleForExactMatch(roleTitle.String) == normalizedRoleTitle {
			return title.String, function.String, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", "", err
	}
	return "", "", nil
}

func isGenericCategoryToken(token string) bool {
	_, ok := genericCategoryMatchTokens[token]
	return ok
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func normalizeEmploymentTypeValue(value any) any {
	text, ok := value.(string)
	if !ok {
		return nil
	}
	normalized := strings.TrimSpace(strings.ToLower(text))
	normalized = regexp.MustCompile(`[\s_]+`).ReplaceAllString(normalized, "-")
	normalized = regexp.MustCompile(`-{2,}`).ReplaceAllString(normalized, "-")
	normalized = strings.Trim(normalized, "-")
	switch normalized {
	case "", "null":
		return nil
	case "fulltime", "full-time", "full time":
		return "full-time"
	case "parttime", "part-time", "part time":
		return "part-time"
	case "contract", "contractor":
		return "contract"
	case "intern", "internship":
		return "internship"
	case "temp", "temporary":
		return "temporary"
	default:
		return normalized
	}
}

func normalizeEducationCredentialCategory(value any) any {
	text, ok := value.(string)
	if !ok {
		return nil
	}
	normalized := strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(strings.ToLower(text), " "))
	if normalized == "" {
		return nil
	}
	return normalized
}

func normalizeTechStackValue(value string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.Trim(normalized, "\"'")
	normalized = regexp.MustCompile(`\([^)]*\)`).ReplaceAllString(normalized, "")
	if strings.Contains(normalized, "(") && !strings.Contains(normalized, ")") {
		normalized = strings.SplitN(normalized, "(", 2)[0]
	}
	normalized = strings.ReplaceAll(normalized, ")", "")
	normalized = strings.ReplaceAll(normalized, "]", "")
	normalized = strings.ReplaceAll(normalized, "}", "")
	normalized = regexp.MustCompile(`\s*/\s*`).ReplaceAllString(normalized, "/")
	normalized = regexp.MustCompile(`\s*-\s*`).ReplaceAllString(normalized, "-")
	normalized = regexp.MustCompile(`[;,:]+$`).ReplaceAllString(normalized, "")
	normalized = regexp.MustCompile(`\s+`).ReplaceAllString(normalized, " ")
	normalized = strings.Trim(normalized, " -_/")
	normalized = strings.TrimRight(normalized, ".")
	if normalized == "" {
		return ""
	}
	lowered := strings.ToLower(normalized)
	if _, ok := techStackDropValues[lowered]; ok {
		return ""
	}
	if alias, ok := techStackAliases[lowered]; ok {
		return alias
	}
	return normalized
}

func normalizeTechStack(values any) []string {
	var source []string
	switch items := values.(type) {
	case []string:
		source = items
	case []any:
		for _, item := range items {
			text, _ := item.(string)
			if strings.TrimSpace(text) != "" {
				source = append(source, text)
			}
		}
	default:
		return nil
	}
	out := make([]string, 0, len(source))
	seen := map[string]struct{}{}
	for _, value := range source {
		next := normalizeTechStackValue(value)
		if next == "" {
			continue
		}
		key := strings.ToLower(next)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, next)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeCountryName(value string) string {
	normalized := regexp.MustCompile(`[^a-zA-Z.\s]`).ReplaceAllString(value, "")
	normalized = strings.TrimSpace(strings.ToLower(regexp.MustCompile(`\s+`).ReplaceAllString(normalized, " ")))
	return countryAliases[normalized]
}

func normalizeStateName(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	normalized := strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(text, " "))
	if normalized == "" {
		return ""
	}
	if regexp.MustCompile(`^[A-Za-z]{2,3}$`).MatchString(normalized) {
		return strings.ToUpper(normalized)
	}
	parts := strings.Fields(strings.ToLower(normalized))
	for idx, part := range parts {
		if part == "" {
			continue
		}
		parts[idx] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func normalizeLocationFields(rawLocation, rawCity, rawStates any) (any, any, any) {
	states := []string{}
	switch items := rawStates.(type) {
	case []any:
		for _, item := range items {
			if state := normalizeStateName(item); state != "" && !containsString(states, state) {
				states = append(states, state)
			}
		}
	case []string:
		for _, item := range items {
			if state := normalizeStateName(item); state != "" && !containsString(states, state) {
				states = append(states, state)
			}
		}
	}
	rawLocationText, _ := rawLocation.(string)
	cityValue := normalizeStateName(rawCity)
	if strings.TrimSpace(rawLocationText) == "" {
		return nil, nilIfEmpty(cityValue), jsonStringOrNil(states)
	}

	type segment struct {
		country string
		state   string
		city    string
	}
	parsedSegments := []segment{}
	for _, chunk := range strings.Split(rawLocationText, "|") {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}
		parts := []string{}
		for _, part := range strings.Split(chunk, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				parts = append(parts, part)
			}
		}
		if len(parts) == 0 {
			continue
		}
		country := ""
		for idx := len(parts) - 1; idx >= 0; idx-- {
			if candidate := normalizeCountryName(parts[idx]); candidate != "" {
				country = candidate
				break
			}
		}
		if country == "" {
			continue
		}
		seg := segment{country: country}
		if len(parts) >= 2 {
			seg.state = normalizeStateName(parts[len(parts)-2])
		}
		if len(parts) >= 3 {
			seg.city = normalizeStateName(parts[0])
		}
		parsedSegments = append(parsedSegments, seg)
	}

	var chosen *segment
	for _, seg := range parsedSegments {
		if seg.country == "United States" {
			chosen = &seg
			break
		}
	}
	if chosen == nil && len(parsedSegments) > 0 {
		chosen = &parsedSegments[0]
	}
	chosenCountry := ""
	chosenState := ""
	if chosen != nil {
		chosenCountry = chosen.country
		chosenState = chosen.state
		if cityValue == "" {
			cityValue = chosen.city
		}
	}
	if chosenCountry == "United States" && chosenState != "" && !containsString(states, chosenState) {
		states = append(states, chosenState)
	}
	return nilIfEmpty(chosenCountry), nilIfEmpty(cityValue), jsonStringOrNil(states)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func nilIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func jsonStringOrNil(values []string) any {
	if len(values) == 0 {
		return nil
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil
	}
	return string(encoded)
}

func normalizeLocationCountries(values any) any {
	switch items := values.(type) {
	case []string:
		out := []string{}
		for _, item := range items {
			trimmed := strings.TrimSpace(item)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return jsonStringOrNil(out)
	case []any:
		out := []string{}
		for _, item := range items {
			text, _ := item.(string)
			trimmed := strings.TrimSpace(text)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return jsonStringOrNil(out)
	default:
		return nil
	}
}

func (s *Service) ProcessPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	rows, err := s.DB.SQL.QueryContext(ctx, `SELECT id, raw_json, COALESCE(source, '') FROM raw_us_jobs WHERE is_ready = 1 AND is_parsed = 0 ORDER BY post_date DESC, id DESC LIMIT ?`, batchSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type rawRow struct {
		id      int64
		rawJSON sql.NullString
		source  string
	}
	pending := make([]rawRow, 0, batchSize)
	for rows.Next() {
		var row rawRow
		if err := rows.Scan(&row.id, &row.rawJSON, &row.source); err != nil {
			return 0, err
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	log.Printf("parsed-job-worker picked_pending_rows=%d", len(pending))

	processed := 0
	skipped := 0
	for _, row := range pending {
		payload := map[string]any{}
		if !row.rawJSON.Valid || row.rawJSON.String == "" {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = 1 WHERE id = ?`, row.id); err != nil {
				return processed, err
			}
			processed++
			skipped++
			continue
		}
		if err := json.Unmarshal([]byte(row.rawJSON.String), &payload); err != nil {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = 1 WHERE id = ?`, row.id); err != nil {
				return processed, err
			}
			processed++
			skipped++
			continue
		}
		log.Printf("parsed-job-worker upsert_start raw_job_id=%d source=%s", row.id, row.source)
		sourceCreatedAt := parseDT(payload["created_at"])
		normalizedTechStack := normalizeTechStack(payload["techStack"])
		categorizedTitle := stringFromPayload(payload["categorizedJobTitle"])
		categorizedFunction := stringFromPayload(payload["categorizedJobFunction"])
		if title, ok := categorizedTitle.(string); ok && strings.TrimSpace(title) != "" && categorizedFunction == nil {
			resolvedFunction, err := s.resolveJobFunctionForCategory(ctx, title)
			if err != nil {
				return processed, err
			}
			if strings.TrimSpace(resolvedFunction) != "" {
				categorizedFunction = resolvedFunction
			}
		}
		if row.source == sourceBuiltin && categorizedTitle == nil {
			if len(normalizedTechStack) == 0 {
				groqCategory := classifyJobTitleWithGroqSync(stringValue(payload["roleTitle"]), stringValue(payload["roleDescription"]))
				if strings.TrimSpace(groqCategory) != "" {
					categorizedTitle = stringFromPayload(groqCategory)
				}
			}
			if categorizedTitle == nil {
				inferredTitle, inferredFunction, err := s.findSimilarRemoteCategories(ctx, stringValue(payload["roleTitle"]), normalizedTechStack)
				if err != nil {
					return processed, err
				}
				categorizedTitle = stringFromPayload(inferredTitle)
				categorizedFunction = stringFromPayload(inferredFunction)
			}
		}
		if title, ok := categorizedTitle.(string); ok && strings.TrimSpace(title) != "" && categorizedFunction == nil {
			resolvedFunction, err := s.resolveJobFunctionForCategory(ctx, title)
			if err != nil {
				return processed, err
			}
			if strings.TrimSpace(resolvedFunction) != "" {
				categorizedFunction = resolvedFunction
			}
		}
		_, normalizedLocationCity, normalizedUSStates := normalizeLocationFields(
			payload["location"],
			payload["locationCity"],
			payload["locationUSStates"],
		)
		normalizedLocationCountries := normalizeLocationCountries(payload["locationCountries"])
		normalizedTechStackJSON := jsonStringOrNil(normalizedTechStack)
		err = database.RetryLocked(8, 50*time.Millisecond, func() error {
			_, execErr := s.DB.SQL.ExecContext(
				ctx,
				`INSERT INTO parsed_jobs (raw_us_job_id, external_job_id, created_at_source, url, categorized_job_title, categorized_job_function, role_title, employment_type, location_city, location_us_states, location_countries, education_requirements_credential_category, tech_stack, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				 ON CONFLICT(raw_us_job_id) DO UPDATE SET
				   external_job_id = excluded.external_job_id,
				   created_at_source = excluded.created_at_source,
				   url = excluded.url,
				   categorized_job_title = excluded.categorized_job_title,
				   categorized_job_function = excluded.categorized_job_function,
				   employment_type = excluded.employment_type,
				   location_city = excluded.location_city,
				   location_us_states = excluded.location_us_states,
				   location_countries = excluded.location_countries,
				   education_requirements_credential_category = excluded.education_requirements_credential_category,
				   tech_stack = excluded.tech_stack,
				   role_title = excluded.role_title,
				   updated_at = excluded.updated_at`,
				row.id,
				stringFromPayload(payload["id"]),
				formatNullableTime(sourceCreatedAt),
				stringFromPayload(payload["url"]),
				categorizedTitle,
				categorizedFunction,
				stringFromPayload(payload["roleTitle"]),
				normalizeEmploymentTypeValue(payload["employmentType"]),
				normalizedLocationCity,
				normalizedUSStates,
				normalizedLocationCountries,
				normalizeEducationCredentialCategory(payload["educationRequirementsCredentialCategory"]),
				normalizedTechStackJSON,
				time.Now().UTC().Format(time.RFC3339Nano),
			)
			return execErr
		})
		if err != nil {
			return processed, err
		}
		if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
			_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = 1 WHERE id = ?`, row.id)
			return err
		}); err != nil {
			return processed, err
		}
		log.Printf("parsed-job-worker upsert_done raw_job_id=%d source=%s", row.id, row.source)
		processed++
	}
	log.Printf("parsed-job-worker batch_done rows=%d processed=%d skipped=%d", len(pending), processed, skipped)
	return processed, nil
}

func formatNullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func stringFromPayload(value any) any {
	switch item := value.(type) {
	case string:
		normalized := strings.TrimSpace(item)
		if normalized == "" || strings.EqualFold(normalized, "null") {
			return nil
		}
		return normalized
	case float64:
		return strings.TrimSpace(strconv.FormatInt(int64(item), 10))
	case int:
		return strconv.Itoa(item)
	case int64:
		return strconv.FormatInt(item, 10)
	default:
		return nil
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
