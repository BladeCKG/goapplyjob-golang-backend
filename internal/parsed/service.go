package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
)

const (
	sourceRemoteRocketship   = "remoterocketship"
	envParsedDBLockRetries   = "PARSED_JOB_DB_LOCK_RETRIES"
	envParsedDBLockDelay     = "PARSED_JOB_DB_LOCK_RETRY_DELAY_SECONDS"
	maxDuplicatePostDateDiff = 48 * time.Hour
	similarCategoryScanBatch = 1000
	similarCategoryMaxScan   = 2000
	similarCategoryQueryTopN = 5
)

var seniorityTokens = map[string]struct{}{
	"senior": {}, "sr": {}, "junior": {}, "jr": {}, "lead": {}, "principal": {}, "staff": {}, "entry": {}, "mid": {}, "expert": {}, "leader": {}, "level": {},
	"ii": {}, "iii": {}, "iv": {}, "v": {}, "vi": {}, "vii": {}, "viii": {}, "ix": {}, "x": {},
}

var employmentNoiseTokens = map[string]struct{}{
	"full": {}, "time": {}, "fulltime": {}, "part": {}, "parttime": {}, "contract": {}, "contractor": {}, "temp": {}, "temporary": {}, "intern": {}, "internship": {}, "freelance": {}, "permanent": {},
	"seasonal": {}, "weekend": {}, "weekends": {}, "night": {}, "evening": {}, "overnight": {}, "urgent": {}, "immediate": {}, "hiring": {}, "opening": {}, "opportunity": {},
}

var workModeNoiseTokens = map[string]struct{}{
	"remote": {}, "hybrid": {}, "onsite": {}, "wfh": {}, "office": {}, "homebased": {}, "telecommute": {}, "telecommuting": {},
}

var genericCategoryMatchTokens = map[string]struct{}{
	"and": {}, "accountant": {}, "account": {}, "administrator": {}, "engineer": {}, "developer": {}, "manager": {}, "specialist": {}, "consultant": {}, "analyst": {}, "architect": {}, "designer": {}, "director": {}, "producer": {}, "writer": {}, "support": {}, "operations": {}, "web": {}, "remote": {}, "lead": {}, "staff": {},
	"executive": {}, "sales": {}, "marketing": {}, "product": {}, "business": {}, "customer": {}, "success": {}, "representative": {}, "technical": {}, "project": {}, "program": {}, "data": {}, "software": {}, "solutions": {}, "content": {}, "security": {},
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
	{pattern: regexp.MustCompile(`\bai\b`), replacement: "artificial intelligence"},
	{pattern: regexp.MustCompile(`\bml\b`), replacement: "machine learning"},
	{pattern: regexp.MustCompile(`\bdev[\s\-]*ops\b`), replacement: "devops"},
	{pattern: regexp.MustCompile(`\bdev\b`), replacement: "developer"},
	{pattern: regexp.MustCompile(`\bbdr\b`), replacement: "business development representative"},
	{pattern: regexp.MustCompile(`\bsdr\b`), replacement: "sales development representative"},
	{pattern: regexp.MustCompile(`\bae\b`), replacement: "account executive"},
	{pattern: regexp.MustCompile(`\bcsm\b`), replacement: "customer success manager"},
	{pattern: regexp.MustCompile(`\bqa\b`), replacement: "quality assurance"},
	{pattern: regexp.MustCompile(`\bswe\b`), replacement: "software engineer"},
	{pattern: regexp.MustCompile(`\bvp\b`), replacement: "vice president"},
	{pattern: regexp.MustCompile(`\bta\b`), replacement: "talent acquisition"},
	{pattern: regexp.MustCompile(`\bhr\b`), replacement: "human resources"},
	{pattern: regexp.MustCompile(`\btalent acquisition\b`), replacement: "recruitment human resources"},
	{pattern: regexp.MustCompile(`\bcpg\b`), replacement: "consumer packaged goods"},
}

type Service struct {
	DB             *database.DB
	EnabledSources map[string]struct{}
}

func New(db *database.DB) *Service { return &Service{DB: db} }

func (s *Service) SuggestCategory(ctx context.Context, source, roleTitle, roleDescription string, techStack any) (string, string, error) {
	categorizedTitle, categorizedFunction, _, err := s.SuggestCategoryWithTechStack(ctx, source, roleTitle, roleDescription, techStack)
	return categorizedTitle, categorizedFunction, err
}

func (s *Service) SuggestCategoryWithTechStack(ctx context.Context, _ string, roleTitle, roleDescription string, techStack any) (string, string, []string, error) {
	normalizedTechStack := normalizeTechStack(techStack)
	categorizedTitle := ""
	categorizedFunction := ""

	if len(normalizedTechStack) == 0 {
		allowedCategories, _ := s.loadAllowedJobCategoriesForGroq(ctx)
		category, groqRequiredSkills := classifyJobTitleWithGroqSync(roleTitle, roleDescription, allowedCategories)
		categorizedTitle = strings.TrimSpace(category)
		if len(groqRequiredSkills) > 0 {
			normalizedTechStack = normalizeTechStack(groqRequiredSkills)
		}
	}
	if categorizedTitle == "" {
		inferredTitle, inferredFunction, err := s.findSimilarRemoteCategories(ctx, roleTitle, normalizedTechStack)
		if err != nil {
			return "", "", nil, err
		}
		categorizedTitle = strings.TrimSpace(inferredTitle)
		categorizedFunction = strings.TrimSpace(inferredFunction)
	}
	if categorizedTitle != "" && categorizedFunction == "" {
		resolvedFunction, err := s.resolveJobFunctionForCategory(ctx, categorizedTitle)
		if err != nil {
			return "", "", nil, err
		}
		categorizedFunction = strings.TrimSpace(resolvedFunction)
	}
	return categorizedTitle, categorizedFunction, normalizedTechStack, nil
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

func parsedLockRetryConfig() (int, time.Duration) {
	attempts := 8
	if raw := strings.TrimSpace(os.Getenv(envParsedDBLockRetries)); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			attempts = parsed
		}
	}
	delay := 50 * time.Millisecond
	if raw := strings.TrimSpace(os.Getenv(envParsedDBLockDelay)); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
			if parsed < 0.05 {
				parsed = 0.05
			}
			delay = time.Duration(parsed * float64(time.Second))
		}
	}
	return attempts, delay
}

func normalizeTextForMatching(value string) string {
	normalized := strings.ToLower(value)
	for _, replacement := range normalizationReplacements {
		normalized = replacement.pattern.ReplaceAllString(normalized, replacement.replacement)
	}
	return normalized
}

func shouldSkipRoleToken(token string) bool {
	if _, ok := seniorityTokens[token]; ok {
		return true
	}
	if _, ok := employmentNoiseTokens[token]; ok {
		return true
	}
	if _, ok := workModeNoiseTokens[token]; ok {
		return true
	}
	return false
}

func tokenizeRoleTitleForSimilarity(roleTitle string) map[string]struct{} {
	rawTokens := regexp.MustCompile(`[^a-z0-9]+`).Split(normalizeTextForMatching(roleTitle), -1)
	out := map[string]struct{}{}
	for _, token := range rawTokens {
		if len(token) <= 1 {
			continue
		}
		if shouldSkipRoleToken(token) {
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
		if shouldSkipRoleToken(token) {
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
	matched := orderedTokenMatchCount(roleTokens, categoryTokens)
	return float64(matched) / float64(len(categoryTokens))
}

func orderedTokenMatchCount(sourceTokens, targetTokens []string) int {
	if len(sourceTokens) == 0 || len(targetTokens) == 0 {
		return 0
	}
	sourceIndex := 0
	matched := 0
	for _, token := range targetTokens {
		for sourceIndex < len(sourceTokens) && sourceTokens[sourceIndex] != token {
			sourceIndex++
		}
		if sourceIndex >= len(sourceTokens) {
			break
		}
		matched++
		sourceIndex++
	}
	return matched
}

func orderedTokens(value string) []string {
	raw := regexp.MustCompile(`[^a-z0-9]+`).Split(normalizeTextForMatching(value), -1)
	out := make([]string, 0, len(raw))
	for _, token := range raw {
		if len(token) <= 1 {
			continue
		}
		if shouldSkipRoleToken(token) {
			continue
		}
		out = append(out, token)
	}
	return out
}

func tokenizeTechStackForSimilarity(values []string) map[string]struct{} {
	tokens := map[string]struct{}{}
	for _, value := range values {
		raw := regexp.MustCompile(`[^a-z0-9]+`).Split(strings.ToLower(value), -1)
		for _, token := range raw {
			if token != "" && len(token) > 1 {
				tokens[token] = struct{}{}
			}
		}
	}
	return tokens
}

func tokenSpecificityWeight(token string) float64 {
	base := 1.0 + float64(min(len(token), 12))/12.0
	if isGenericCategoryToken(token) {
		return base * 0.35
	}
	return base
}

func parseStringJSONArray(raw string) []string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	var values []string
	if err := json.Unmarshal([]byte(trimmed), &values); err != nil {
		return nil
	}
	return values
}

func setIntersectionCount(left, right map[string]struct{}) int {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	count := 0
	for token := range left {
		if _, ok := right[token]; ok {
			count++
		}
	}
	return count
}

func setSubsetOf(left, right map[string]struct{}) bool {
	if len(left) == 0 {
		return true
	}
	for token := range left {
		if _, ok := right[token]; !ok {
			return false
		}
	}
	return true
}

func (s *Service) findSimilarRemoteCategories(ctx context.Context, roleTitle string, sourceTechStack []string) (string, string, error) {
	sourceTokens := tokenizeRoleTitleForSimilarity(roleTitle)
	sourceSequenceTokens := tokenizeTextForSequence(roleTitle)
	sourceNormalizedTitle := normalizeRoleTitleForExactMatch(roleTitle)
	sourceSkillValues := make([]string, 0, len(sourceTechStack))
	seenSkillValues := map[string]struct{}{}
	for _, value := range sourceTechStack {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		normalizedKey := strings.ToLower(trimmed)
		if _, exists := seenSkillValues[normalizedKey]; exists {
			continue
		}
		seenSkillValues[normalizedKey] = struct{}{}
		sourceSkillValues = append(sourceSkillValues, trimmed)
	}
	sourceSkillTokens := tokenizeTechStackForSimilarity(sourceTechStack)

	if len(sourceTokens) == 0 && len(sourceSequenceTokens) == 0 && len(sourceSkillTokens) == 0 {
		return "", "", nil
	}
	sourceTokenSet := map[string]struct{}{}
	for _, token := range sourceSequenceTokens {
		sourceTokenSet[token] = struct{}{}
	}
	sourceHasSpecificTokens := false
	sourceSpecificTokens := map[string]struct{}{}
	for token := range sourceTokens {
		if !isGenericCategoryToken(token) {
			sourceHasSpecificTokens = true
			sourceSpecificTokens[token] = struct{}{}
		}
	}
	sourceTokenWeights := map[string]float64{}
	for token := range sourceTokens {
		sourceTokenWeights[token] = tokenSpecificityWeight(token)
	}
	sourceSpecificWeights := map[string]float64{}
	for token := range sourceSpecificTokens {
		sourceSpecificWeights[token] = tokenSpecificityWeight(token)
	}

	// Fast-path exact role-title match.
	normalizedInputTitle := strings.ToLower(strings.TrimSpace(roleTitle))
	if normalizedInputTitle != "" {
		query := `SELECT p.role_title, p.categorized_job_title, p.categorized_job_function, COALESCE(p.tech_stack::text, '[]')
			FROM parsed_jobs p
			JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
			WHERE r.source = ?
			  AND p.categorized_job_title IS NOT NULL
			  AND p.categorized_job_function IS NOT NULL
			  AND LOWER(p.role_title) = ?`
		args := []any{sourceRemoteRocketship}
		args = append(args, normalizedInputTitle)
		if len(sourceSkillValues) > 0 {
			query += ` AND EXISTS (
				SELECT 1
				FROM jsonb_array_elements_text(COALESCE(p.tech_stack, '[]'::jsonb)) AS skill
				WHERE skill = ANY(?::text[])
			)`
			args = append(args, sourceSkillValues)
		}
		query += ` ORDER BY p.updated_at DESC, p.id DESC LIMIT 100`
		rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
		if err != nil {
			return "", "", err
		}
		defer rows.Close()
		bestRankSet := false
		bestSkillOverlapCount := 0
		bestSkillOverlapRatio := 0.0
		bestCategoryOverlap := 0
		bestTitle := ""
		bestFunction := ""
		for rows.Next() {
			var candidateRoleTitle, candidateTitle sql.NullString
			var candidateFunction sql.NullString
			var candidateTechStackRaw sql.NullString
			if err := rows.Scan(&candidateRoleTitle, &candidateTitle, &candidateFunction, &candidateTechStackRaw); err != nil {
				return "", "", err
			}
			titleTokens := tokenizeTextForSequence(candidateTitle.String)
			titleTokenSet := map[string]struct{}{}
			for _, token := range titleTokens {
				titleTokenSet[token] = struct{}{}
			}
			skipCandidate := false
			if sourceHasSpecificTokens && len(titleTokenSet) == 1 {
				for token := range titleTokenSet {
					if isGenericCategoryToken(token) {
						skipCandidate = true
						break
					}
				}
			}
			if skipCandidate {
				continue
			}
			candidateSkillTokens := tokenizeTechStackForSimilarity(parseStringJSONArray(candidateTechStackRaw.String))
			skillOverlapCount := setIntersectionCount(sourceSkillTokens, candidateSkillTokens)
			skillOverlapRatio := 0.0
			if len(sourceSkillTokens) > 0 {
				skillOverlapRatio = float64(skillOverlapCount) / float64(len(sourceSkillTokens))
			}
			functionTokens := tokenizeTextForSequence(candidateFunction.String)
			combinedCategorySet := map[string]struct{}{}
			for _, token := range titleTokens {
				combinedCategorySet[token] = struct{}{}
			}
			for _, token := range functionTokens {
				combinedCategorySet[token] = struct{}{}
			}
			categoryOverlapCount := setIntersectionCount(sourceTokens, combinedCategorySet)
			if !bestRankSet ||
				skillOverlapCount > bestSkillOverlapCount ||
				(skillOverlapCount == bestSkillOverlapCount && skillOverlapRatio > bestSkillOverlapRatio) ||
				(skillOverlapCount == bestSkillOverlapCount && skillOverlapRatio == bestSkillOverlapRatio && categoryOverlapCount > bestCategoryOverlap) {
				bestRankSet = true
				bestSkillOverlapCount = skillOverlapCount
				bestSkillOverlapRatio = skillOverlapRatio
				bestCategoryOverlap = categoryOverlapCount
				bestTitle = candidateTitle.String
				bestFunction = candidateFunction.String
			}
		}
		if err := rows.Err(); err != nil {
			return "", "", err
		}
		if bestTitle != "" {
			return bestTitle, bestFunction, nil
		}
	}

	seenTokens := map[string]struct{}{}
	prioritizedTokens := make([]string, 0, len(sourceSequenceTokens))
	for _, token := range sourceSequenceTokens {
		if _, seen := seenTokens[token]; seen {
			continue
		}
		seenTokens[token] = struct{}{}
		if !isGenericCategoryToken(token) {
			prioritizedTokens = append(prioritizedTokens, token)
		}
	}
	if len(prioritizedTokens) == 0 {
		for _, token := range sourceSequenceTokens {
			if _, seen := seenTokens[token]; seen {
				continue
			}
			seenTokens[token] = struct{}{}
			prioritizedTokens = append(prioritizedTokens, token)
		}
	}
	sort.SliceStable(prioritizedTokens, func(i, j int) bool {
		return len(prioritizedTokens[i]) > len(prioritizedTokens[j])
	})
	queryTokens := append([]string{}, prioritizedTokens[:min(len(prioritizedTokens), similarCategoryQueryTopN)]...)
	if len(queryTokens) < similarCategoryQueryTopN {
		allUnique := make([]string, 0, len(sourceSequenceTokens))
		seen := map[string]struct{}{}
		for _, token := range sourceSequenceTokens {
			if _, ok := seen[token]; ok {
				continue
			}
			seen[token] = struct{}{}
			allUnique = append(allUnique, token)
		}
		sort.SliceStable(allUnique, func(i, j int) bool { return len(allUnique[i]) > len(allUnique[j]) })
		for _, token := range allUnique {
			exists := false
			for _, current := range queryTokens {
				if current == token {
					exists = true
					break
				}
			}
			if exists {
				continue
			}
			queryTokens = append(queryTokens, token)
			if len(queryTokens) >= similarCategoryQueryTopN {
				break
			}
		}
	}

	type matchRank struct {
		nonGenericCategoryPreference int
		exactNormalizedTitleMatch    int
		weightedSpecificTokenHits    float64
		weightedCategoryOverlap      float64
		weightedRoleOverlap          float64
		combinedSpecificTokenHits    int
		roleSpecificTokenHits        int
		categorySpecificTokenHits    int
		combinedCategoryOverlap      int
		categoryOverlapCount         int
		overlapCount                 int
		overlapRatio                 float64
		totalSequenceMatchCount      int
		categoryTokenCount           int
		functionTokenCount           int
		functionOverlapCount         int
		skillOverlapCount            int
		skillOverlapRatio            float64
		specificity                  int
		roleJaccard                  float64
	}
	rankGreater := func(left, right matchRank) bool {
		switch {
		case left.nonGenericCategoryPreference != right.nonGenericCategoryPreference:
			return left.nonGenericCategoryPreference > right.nonGenericCategoryPreference
		case left.exactNormalizedTitleMatch != right.exactNormalizedTitleMatch:
			return left.exactNormalizedTitleMatch > right.exactNormalizedTitleMatch
		case left.weightedSpecificTokenHits != right.weightedSpecificTokenHits:
			return left.weightedSpecificTokenHits > right.weightedSpecificTokenHits
		case left.weightedCategoryOverlap != right.weightedCategoryOverlap:
			return left.weightedCategoryOverlap > right.weightedCategoryOverlap
		case left.weightedRoleOverlap != right.weightedRoleOverlap:
			return left.weightedRoleOverlap > right.weightedRoleOverlap
		case left.combinedSpecificTokenHits != right.combinedSpecificTokenHits:
			return left.combinedSpecificTokenHits > right.combinedSpecificTokenHits
		case left.roleSpecificTokenHits != right.roleSpecificTokenHits:
			return left.roleSpecificTokenHits > right.roleSpecificTokenHits
		case left.categorySpecificTokenHits != right.categorySpecificTokenHits:
			return left.categorySpecificTokenHits > right.categorySpecificTokenHits
		case left.combinedCategoryOverlap != right.combinedCategoryOverlap:
			return left.combinedCategoryOverlap > right.combinedCategoryOverlap
		case left.categoryOverlapCount != right.categoryOverlapCount:
			return left.categoryOverlapCount > right.categoryOverlapCount
		case left.overlapCount != right.overlapCount:
			return left.overlapCount > right.overlapCount
		case left.overlapRatio != right.overlapRatio:
			return left.overlapRatio > right.overlapRatio
		case left.totalSequenceMatchCount != right.totalSequenceMatchCount:
			return left.totalSequenceMatchCount > right.totalSequenceMatchCount
		case left.categoryTokenCount != right.categoryTokenCount:
			return left.categoryTokenCount > right.categoryTokenCount
		case left.functionTokenCount != right.functionTokenCount:
			return left.functionTokenCount > right.functionTokenCount
		case left.functionOverlapCount != right.functionOverlapCount:
			return left.functionOverlapCount > right.functionOverlapCount
		case left.skillOverlapCount != right.skillOverlapCount:
			return left.skillOverlapCount > right.skillOverlapCount
		case left.skillOverlapRatio != right.skillOverlapRatio:
			return left.skillOverlapRatio > right.skillOverlapRatio
		case left.specificity != right.specificity:
			return left.specificity > right.specificity
		default:
			return left.roleJaccard > right.roleJaccard
		}
	}

	directMatchTitle := ""
	directMatchFunction := ""
	directMatchScore := -1
	bestTitle := ""
	bestFunction := ""
	bestCandidateRoleTitle := ""
	bestRankSet := false
	var bestRank matchRank

	buildQuery := func(applySkillFilter bool, offset int) (string, []any) {
		query := `SELECT p.role_title, p.categorized_job_title, p.categorized_job_function, COALESCE(p.tech_stack::text, '[]')
			FROM parsed_jobs p
			JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
			WHERE r.source = ?
			  AND p.role_title IS NOT NULL
			  AND p.categorized_job_title IS NOT NULL`
		args := []any{sourceRemoteRocketship}
		if len(queryTokens) > 0 {
			query += ` AND EXISTS (
				SELECT 1
				FROM unnest(?::text[]) AS token
				WHERE LOWER(p.role_title) LIKE '%' || token || '%'
				   OR LOWER(p.categorized_job_title) LIKE '%' || token || '%'
				   OR LOWER(COALESCE(p.categorized_job_function, '')) LIKE '%' || token || '%'
			)`
			args = append(args, queryTokens)
		}
		if applySkillFilter && len(sourceSkillValues) > 0 {
			query += ` AND EXISTS (
				SELECT 1
				FROM jsonb_array_elements_text(COALESCE(p.tech_stack, '[]'::jsonb)) AS skill
				WHERE skill = ANY(?::text[])
			)`
			args = append(args, sourceSkillValues)
		}
		query += ` ORDER BY p.updated_at DESC, p.id DESC OFFSET ? LIMIT ?`
		args = append(args, offset, similarCategoryScanBatch)
		return query, args
	}

	for _, applySkillFilter := range []bool{true, false} {
		if applySkillFilter && len(sourceSkillValues) == 0 {
			continue
		}
		offset := 0
		scannedRows := 0
		for {
			query, args := buildQuery(applySkillFilter, offset)
			rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
			if err != nil {
				return "", "", err
			}
			rowCount := 0
			for rows.Next() {
				rowCount++
				scannedRows++
				var candidateRoleTitle, candidateTitle, candidateFunction, candidateTechStackRaw sql.NullString
				if err := rows.Scan(&candidateRoleTitle, &candidateTitle, &candidateFunction, &candidateTechStackRaw); err != nil {
					rows.Close()
					return "", "", err
				}
				roleTokens := tokenizeTextForSequence(candidateRoleTitle.String)
				candidateNormalizedTitle := normalizeRoleTitleForExactMatch(candidateRoleTitle.String)
				titleTokens := tokenizeTextForSequence(candidateTitle.String)
				functionTokens := tokenizeTextForSequence(candidateFunction.String)

				if len(titleTokens) > 0 && len(functionTokens) > 0 {
					titleTokenSet := map[string]struct{}{}
					for _, token := range titleTokens {
						titleTokenSet[token] = struct{}{}
					}
					functionTokenSet := map[string]struct{}{}
					for _, token := range functionTokens {
						functionTokenSet[token] = struct{}{}
					}
					isSpecificDirectTitle := len(titleTokenSet) >= 2
					if isSpecificDirectTitle && setSubsetOf(titleTokenSet, sourceTokenSet) && setSubsetOf(functionTokenSet, sourceTokenSet) {
						directScore := len(titleTokenSet) + len(functionTokenSet) +
							orderedTokenMatchCount(sourceSequenceTokens, titleTokens) +
							orderedTokenMatchCount(sourceSequenceTokens, functionTokens)
						if directScore > directMatchScore {
							directMatchScore = directScore
							directMatchTitle = candidateTitle.String
							directMatchFunction = candidateFunction.String
						}
					}
				}

				candidateTokens := tokenizeRoleTitleForSimilarity(candidateRoleTitle.String)
				overlapCount := setIntersectionCount(sourceTokens, candidateTokens)
				overlapRatio := 0.0
				if len(sourceTokens) > 0 {
					overlapRatio = float64(overlapCount) / float64(len(sourceTokens))
				}
				categoryTokenSet := map[string]struct{}{}
				for _, token := range titleTokens {
					categoryTokenSet[token] = struct{}{}
				}
				functionTokenSet := map[string]struct{}{}
				for _, token := range functionTokens {
					functionTokenSet[token] = struct{}{}
				}
				categoryOverlapCount := setIntersectionCount(sourceTokens, categoryTokenSet)
				functionOverlapCount := setIntersectionCount(sourceTokens, functionTokenSet)
				matchTokens := map[string]struct{}{}
				for token := range candidateTokens {
					matchTokens[token] = struct{}{}
				}
				for _, token := range titleTokens {
					matchTokens[token] = struct{}{}
				}
				for _, token := range functionTokens {
					matchTokens[token] = struct{}{}
				}
				roleJaccard := jaccardSimilarity(sourceTokens, candidateTokens)

				candidateSkillTokens := tokenizeTechStackForSimilarity(parseStringJSONArray(candidateTechStackRaw.String))
				skillOverlapCount := setIntersectionCount(sourceSkillTokens, candidateSkillTokens)
				skillOverlapRatio := 0.0
				if len(sourceSkillTokens) > 0 {
					skillOverlapRatio = float64(skillOverlapCount) / float64(len(sourceSkillTokens))
				}
				categoryTokenCount := len(titleTokens)
				functionTokenCount := len(functionTokens)
				combinedCategorySet := map[string]struct{}{}
				for token := range categoryTokenSet {
					combinedCategorySet[token] = struct{}{}
				}
				for token := range functionTokenSet {
					combinedCategorySet[token] = struct{}{}
				}
				combinedCategoryOverlap := setIntersectionCount(sourceTokens, combinedCategorySet)
				roleTokenSet := map[string]struct{}{}
				for _, token := range roleTokens {
					roleTokenSet[token] = struct{}{}
				}
				weightedRoleOverlap := 0.0
				for token := range sourceTokens {
					if _, ok := roleTokenSet[token]; ok {
						weightedRoleOverlap += sourceTokenWeights[token]
					}
				}
				weightedCategoryOverlap := 0.0
				for token := range sourceTokens {
					if _, ok := combinedCategorySet[token]; ok {
						weightedCategoryOverlap += sourceTokenWeights[token]
					}
				}
				roleSpecificTokenHits := setIntersectionCount(sourceSpecificTokens, roleTokenSet)
				categorySpecificTokenHits := setIntersectionCount(sourceSpecificTokens, combinedCategorySet)
				roleCategoryFunctionSet := map[string]struct{}{}
				for token := range roleTokenSet {
					roleCategoryFunctionSet[token] = struct{}{}
				}
				for token := range combinedCategorySet {
					roleCategoryFunctionSet[token] = struct{}{}
				}
				combinedSpecificTokenHits := setIntersectionCount(sourceSpecificTokens, roleCategoryFunctionSet)
				weightedSpecificTokenHits := 0.0
				for token := range sourceSpecificTokens {
					if _, ok := roleCategoryFunctionSet[token]; ok {
						weightedSpecificTokenHits += sourceSpecificWeights[token]
					}
				}
				totalSequenceMatchCount := max(
					orderedTokenMatchCount(sourceSequenceTokens, roleTokens),
					max(
						orderedTokenMatchCount(sourceSequenceTokens, titleTokens),
						orderedTokenMatchCount(sourceSequenceTokens, functionTokens),
					),
				)
				isGenericOneWordCategory := false
				if categoryTokenCount == 1 {
					for token := range categoryTokenSet {
						if isGenericCategoryToken(token) {
							isGenericOneWordCategory = true
						}
					}
				}
				nonGenericCategoryPreference := 0
				if sourceHasSpecificTokens && !isGenericOneWordCategory {
					nonGenericCategoryPreference = 1
				}
				exactNormalizedTitleMatch := 0
				if sourceNormalizedTitle != "" && sourceNormalizedTitle == candidateNormalizedTitle {
					exactNormalizedTitleMatch = 1
				}
				rank := matchRank{
					nonGenericCategoryPreference: nonGenericCategoryPreference,
					exactNormalizedTitleMatch:    exactNormalizedTitleMatch,
					weightedSpecificTokenHits:    weightedSpecificTokenHits,
					weightedCategoryOverlap:      weightedCategoryOverlap,
					weightedRoleOverlap:          weightedRoleOverlap,
					combinedSpecificTokenHits:    combinedSpecificTokenHits,
					roleSpecificTokenHits:        roleSpecificTokenHits,
					categorySpecificTokenHits:    categorySpecificTokenHits,
					combinedCategoryOverlap:      combinedCategoryOverlap,
					categoryOverlapCount:         categoryOverlapCount,
					overlapCount:                 overlapCount,
					overlapRatio:                 overlapRatio,
					totalSequenceMatchCount:      totalSequenceMatchCount,
					categoryTokenCount:           categoryTokenCount,
					functionTokenCount:           functionTokenCount,
					functionOverlapCount:         functionOverlapCount,
					skillOverlapCount:            skillOverlapCount,
					skillOverlapRatio:            skillOverlapRatio,
					specificity:                  len(matchTokens),
					roleJaccard:                  roleJaccard,
				}
				if !bestRankSet || rankGreater(rank, bestRank) {
					bestRankSet = true
					bestRank = rank
					bestTitle = candidateTitle.String
					bestFunction = candidateFunction.String
					bestCandidateRoleTitle = candidateRoleTitle.String
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return "", "", err
			}
			rows.Close()
			if rowCount == 0 {
				break
			}
			offset += rowCount
			if scannedRows >= similarCategoryMaxScan {
				break
			}
		}
		if bestRankSet || !applySkillFilter {
			break
		}
	}

	if directMatchTitle != "" {
		log.Printf(
			"similar-category direct_match role_title=%s candidate_role_title=%s",
			roleTitle,
			bestCandidateRoleTitle,
		)
		return directMatchTitle, directMatchFunction, nil
	}
	if !bestRankSet {
		return "", "", nil
	}

	requiredOverlap := 2
	if len(sourceTokens) <= 1 {
		requiredOverlap = 1
	}
	hasTechSignal := len(sourceSkillTokens) > 0 && bestRank.skillOverlapCount >= 1
	minSpecificHits := 2
	if len(sourceSpecificTokens) >= 2 {
		minSpecificHits = 1
	}
	minWeightedSpecific := 0.8
	if len(sourceSpecificTokens) >= 2 {
		minWeightedSpecific = 0.35
	}
	hasSpecificSignal := bestRank.combinedSpecificTokenHits >= minSpecificHits || bestRank.weightedSpecificTokenHits >= minWeightedSpecific
	hasCategorySignal := bestRank.categoryOverlapCount >= 2 ||
		(len(sourceTokens) >= 2 && bestRank.categoryOverlapCount >= 1 && hasSpecificSignal)
	hasSequenceSignal := bestRank.totalSequenceMatchCount >= 3
	hasWeightedOverlapSignal := sourceHasSpecificTokens && (bestRank.weightedCategoryOverlap+bestRank.weightedRoleOverlap) >= 0.5
	hasConfidentSignal := hasTechSignal || hasCategorySignal || hasSequenceSignal || hasSpecificSignal || hasWeightedOverlapSignal
	lowInformationSource := len(sourceTokens) <= 1
	if lowInformationSource && !hasTechSignal && bestRank.totalSequenceMatchCount < 2 && bestRank.categoryOverlapCount < 2 {
		return "", "", nil
	}
	if (bestRank.overlapRatio < 0.35 && !hasConfidentSignal) || (bestRank.overlapCount < requiredOverlap && !hasConfidentSignal) {
		return "", "", nil
	}
	log.Printf(
		"similar-category best_match role_title=%s candidate_role_title=%s",
		roleTitle,
		bestCandidateRoleTitle,
	)
	return bestTitle, bestFunction, nil
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
		query += ` AND EXISTS (
			SELECT 1
			FROM jsonb_array_elements_text(COALESCE(p.tech_stack, '[]'::jsonb)) AS skill
			WHERE LOWER(skill) = ANY(?::text[])
		)`
		args = append(args, normalizedSkillValues)
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
	if len(s.EnabledSources) == 0 {
		log.Printf("parsed-job-worker picked_pending_rows=0")
		return 0, nil
	}
	sources := make([]string, 0, len(s.EnabledSources))
	for source := range s.EnabledSources {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	rows, err := s.DB.PGX.Query(
		ctx,
		`SELECT id, raw_json, COALESCE(source, '')
		   FROM raw_us_jobs
		  WHERE is_ready = true
		    AND is_skippable = false
		    AND is_parsed = false
		    AND raw_json IS NOT NULL
		    AND source = ANY($1::text[])
		  ORDER BY post_date DESC, id DESC
		  LIMIT $2`,
		sources,
		batchSize,
	)
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
		var (
			id      int64
			rawJSON string
			source  string
		)
		if err := rows.Scan(&id, &rawJSON, &source); err != nil {
			return 0, err
		}
		pending = append(pending, rawRow{
			id:      id,
			rawJSON: sql.NullString{String: rawJSON, Valid: true},
			source:  source,
		})
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
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true WHERE id = ?`, row.id); err != nil {
				return processed, err
			}
			processed++
			skipped++
			continue
		}
		if err := json.Unmarshal([]byte(row.rawJSON.String), &payload); err != nil {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true WHERE id = ?`, row.id); err != nil {
				return processed, err
			}
			processed++
			skipped++
			continue
		}
		log.Printf("parsed-job-worker upsert_start raw_job_id=%d source=%s", row.id, row.source)
		sourceCreatedAt := parseDT(payload["created_at"])
		normalizedTechStack := normalizeTechStack(payload["techStack"])
		plugin, pluginOK := plugins.Get(strings.TrimSpace(row.source))
		inferCategories := false
		if pluginOK {
			inferCategories = plugin.InferCategories
		}
		categorizedTitle := stringFromPayload(payload["categorizedJobTitle"])
		categorizedFunction := stringFromPayload(payload["categorizedJobFunction"])
		if title, ok := categorizedTitle.(string); ok && strings.TrimSpace(title) != "" && categorizedFunction == nil {
			resolvedFunction, err := s.resolveJobFunctionForCategory(ctx, title)
			if err != nil {
				log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
				skipped++
				continue
			}
			if strings.TrimSpace(resolvedFunction) != "" {
				categorizedFunction = resolvedFunction
			}
		}
		if inferCategories && categorizedTitle == nil {
			if len(normalizedTechStack) == 0 {
				allowedCategories, _ := s.loadAllowedJobCategoriesForGroq(ctx)
				groqCategory, groqRequiredSkills := classifyJobTitleWithGroqSync(
					stringValue(payload["roleTitle"]),
					stringValue(payload["roleDescription"]),
					allowedCategories,
				)
				if strings.TrimSpace(groqCategory) != "" {
					categorizedTitle = stringFromPayload(groqCategory)
				}
				if len(normalizedTechStack) == 0 && len(groqRequiredSkills) > 0 {
					normalizedTechStack = normalizeTechStack(groqRequiredSkills)
				}
			}
			if categorizedTitle == nil {
				inferredTitle, inferredFunction, err := s.findSimilarRemoteCategories(ctx, stringValue(payload["roleTitle"]), normalizedTechStack)
				if err != nil {
					log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
					skipped++
					continue
				}
				categorizedTitle = stringFromPayload(inferredTitle)
				categorizedFunction = stringFromPayload(inferredFunction)
			}
		}
		if title, ok := categorizedTitle.(string); ok && strings.TrimSpace(title) != "" && categorizedFunction == nil {
			resolvedFunction, err := s.resolveJobFunctionForCategory(ctx, title)
			if err != nil {
				log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
				skipped++
				continue
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
		companyID, companyErr := s.upsertCompanyFromPayload(ctx, payload, plugin, pluginOK)
		if companyErr != nil {
			log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, companyErr)
			skipped++
			continue
		}
		createdAtSourceValue := formatNullableTime(sourceCreatedAt)
		if duplicateID, isDuplicate, duplicateErr := s.findDuplicateCrossSourceParsedJob(ctx, row.id, row.source, payload, companyID); duplicateErr != nil {
			log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, duplicateErr)
			skipped++
			continue
		} else if isDuplicate {
			if strings.EqualFold(strings.TrimSpace(row.source), sourceRemoteRocketship) {
				var previousCreatedAt sql.NullTime
				if err := s.DB.SQL.QueryRowContext(ctx, `SELECT created_at_source FROM parsed_jobs WHERE id = ? LIMIT 1`, duplicateID).Scan(&previousCreatedAt); err != nil {
					log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
					skipped++
					continue
				}
				if previousCreatedAt.Valid {
					createdAtSourceValue = formatNullableTime(&previousCreatedAt.Time)
				}
				retries, retryDelay := parsedLockRetryConfig()
				if err := database.RetryLocked(retries, retryDelay, func() error {
					_, execErr := s.DB.SQL.ExecContext(
						ctx,
						`UPDATE parsed_jobs
						 SET raw_us_job_id = ?, updated_at = ?
						 WHERE id = ?`,
						row.id,
						time.Now().UTC().Format(time.RFC3339Nano),
						duplicateID,
					)
					return execErr
				}); err != nil {
					log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
					skipped++
					continue
				}
				log.Printf("parsed-job-worker duplicate_replaced existing_parsed_id=%d raw_job_id=%d source=%s", duplicateID, row.id, row.source)
			} else {
				log.Printf("parsed-job-worker duplicate_cross_source_skip raw_job_id=%d source=%s duplicate_parsed_job_id=%d", row.id, row.source, duplicateID)
				retries, retryDelay := parsedLockRetryConfig()
				if err := database.RetryLocked(retries, retryDelay, func() error {
					_, execErr := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true, is_skippable = true WHERE id = ?`, row.id)
					return execErr
				}); err != nil {
					log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
					skipped++
					continue
				}
				processed++
				continue
			}
		}
		retries, retryDelay := parsedLockRetryConfig()
		err = database.RetryLocked(retries, retryDelay, func() error {
			_, execErr := s.DB.SQL.ExecContext(
				ctx,
				`INSERT INTO parsed_jobs (
					raw_us_job_id, company_id, external_job_id, created_at_source, valid_until_date, date_deleted, description_language,
					role_title, role_description, role_requirements, benefits, job_description_summary, two_line_job_description_summary,
					role_title_brazil, role_description_brazil, role_requirements_brazil, benefits_brazil, slug_brazil, job_description_summary_brazil, two_line_job_description_summary_brazil,
					role_title_france, role_description_france, role_requirements_france, benefits_france, slug_france, job_description_summary_france, two_line_job_description_summary_france,
					role_title_germany, role_description_germany, role_requirements_germany, benefits_germany, slug_germany, job_description_summary_germany, two_line_job_description_summary_germany,
					url, slug, employment_type, location_type, location_city,
					categorized_job_title, categorized_job_function, education_requirements_credential_category,
					experience_in_place_of_education, experience_requirements_months,
					is_on_linkedin, is_promoted, is_entry_level, is_junior, is_mid_level, is_senior, is_lead,
					required_languages, location_us_states, location_countries, tech_stack,
					salary_min, salary_max, salary_type, salary_currency_code, salary_currency_symbol, salary_min_usd, salary_max_usd, salary_human_text,
					updated_at
				)
				 VALUES (
					?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?,
					?, ?, ?,
					?, ?,
					?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?,
					?, ?, ?, ?, ?, ?, ?, ?,
					?
				)
				 ON CONFLICT(raw_us_job_id) DO UPDATE SET
				   company_id = excluded.company_id,
				   external_job_id = excluded.external_job_id,
				   created_at_source = excluded.created_at_source,
				   valid_until_date = excluded.valid_until_date,
				   date_deleted = excluded.date_deleted,
				   description_language = excluded.description_language,
				   role_title = excluded.role_title,
				   role_description = excluded.role_description,
				   role_requirements = excluded.role_requirements,
				   benefits = excluded.benefits,
				   job_description_summary = excluded.job_description_summary,
				   two_line_job_description_summary = excluded.two_line_job_description_summary,
				   role_title_brazil = excluded.role_title_brazil,
				   role_description_brazil = excluded.role_description_brazil,
				   role_requirements_brazil = excluded.role_requirements_brazil,
				   benefits_brazil = excluded.benefits_brazil,
				   slug_brazil = excluded.slug_brazil,
				   job_description_summary_brazil = excluded.job_description_summary_brazil,
				   two_line_job_description_summary_brazil = excluded.two_line_job_description_summary_brazil,
				   role_title_france = excluded.role_title_france,
				   role_description_france = excluded.role_description_france,
				   role_requirements_france = excluded.role_requirements_france,
				   benefits_france = excluded.benefits_france,
				   slug_france = excluded.slug_france,
				   job_description_summary_france = excluded.job_description_summary_france,
				   two_line_job_description_summary_france = excluded.two_line_job_description_summary_france,
				   role_title_germany = excluded.role_title_germany,
				   role_description_germany = excluded.role_description_germany,
				   role_requirements_germany = excluded.role_requirements_germany,
				   benefits_germany = excluded.benefits_germany,
				   slug_germany = excluded.slug_germany,
				   job_description_summary_germany = excluded.job_description_summary_germany,
				   two_line_job_description_summary_germany = excluded.two_line_job_description_summary_germany,
				   url = excluded.url,
				   slug = excluded.slug,
				   categorized_job_title = excluded.categorized_job_title,
				   categorized_job_function = excluded.categorized_job_function,
				   employment_type = excluded.employment_type,
				   location_type = excluded.location_type,
				   location_city = excluded.location_city,
				   location_us_states = excluded.location_us_states,
				   location_countries = excluded.location_countries,
				   education_requirements_credential_category = excluded.education_requirements_credential_category,
				   experience_in_place_of_education = excluded.experience_in_place_of_education,
				   experience_requirements_months = excluded.experience_requirements_months,
				   is_on_linkedin = excluded.is_on_linkedin,
				   is_promoted = excluded.is_promoted,
				   is_entry_level = excluded.is_entry_level,
				   is_junior = excluded.is_junior,
				   is_mid_level = excluded.is_mid_level,
				   is_senior = excluded.is_senior,
				   is_lead = excluded.is_lead,
				   required_languages = excluded.required_languages,
				   tech_stack = excluded.tech_stack,
				   salary_min = excluded.salary_min,
				   salary_max = excluded.salary_max,
				   salary_type = excluded.salary_type,
				   salary_currency_code = excluded.salary_currency_code,
				   salary_currency_symbol = excluded.salary_currency_symbol,
				   salary_min_usd = excluded.salary_min_usd,
				   salary_max_usd = excluded.salary_max_usd,
				   salary_human_text = excluded.salary_human_text,
				   updated_at = excluded.updated_at`,
				row.id,
				companyID,
				stringFromPayload(payload["id"]),
				createdAtSourceValue,
				formatNullableTime(parseDT(payload["validUntilDate"])),
				formatNullableTime(parseDT(payload["dateDeleted"])),
				stringFromPayload(payload["descriptionLanguage"]),
				stringFromPayload(payload["roleTitle"]),
				stringFromPayload(payload["roleDescription"]),
				stringFromPayload(payload["roleRequirements"]),
				stringFromPayload(payload["benefits"]),
				stringFromPayload(payload["jobDescriptionSummary"]),
				stringFromPayload(payload["twoLineJobDescriptionSummary"]),
				stringFromPayload(payload["roleTitleBrazil"]),
				stringFromPayload(payload["roleDescriptionBrazil"]),
				stringFromPayload(payload["roleRequirementsBrazil"]),
				stringFromPayload(payload["benefitsBrazil"]),
				stringFromPayload(payload["slugBrazil"]),
				stringFromPayload(payload["jobDescriptionSummaryBrazil"]),
				stringFromPayload(payload["twoLineJobDescriptionSummaryBrazil"]),
				stringFromPayload(payload["roleTitleFrance"]),
				stringFromPayload(payload["roleDescriptionFrance"]),
				stringFromPayload(payload["roleRequirementsFrance"]),
				stringFromPayload(payload["benefitsFrance"]),
				stringFromPayload(payload["slugFrance"]),
				stringFromPayload(payload["jobDescriptionSummaryFrance"]),
				stringFromPayload(payload["twoLineJobDescriptionSummaryFrance"]),
				stringFromPayload(payload["roleTitleGermany"]),
				stringFromPayload(payload["roleDescriptionGermany"]),
				stringFromPayload(payload["roleRequirementsGermany"]),
				stringFromPayload(payload["benefitsGermany"]),
				stringFromPayload(payload["slugGermany"]),
				stringFromPayload(payload["jobDescriptionSummaryGermany"]),
				stringFromPayload(payload["twoLineJobDescriptionSummaryGermany"]),
				stringFromPayload(payload["url"]),
				stringFromPayload(payload["slug"]),
				normalizeEmploymentTypeValue(payload["employmentType"]),
				stringFromPayload(payload["locationType"]),
				normalizedLocationCity,
				categorizedTitle,
				categorizedFunction,
				normalizeEducationCredentialCategory(payload["educationRequirementsCredentialCategory"]),
				_normalizeNullStringToNone(payload["experienceInPlaceOfEducation"]),
				_normalizeNullStringToNone(payload["experienceRequirementsMonthsOfExperience"]),
				_normalizeNullStringToNone(payload["isOnLinkedIn"]),
				_normalizeNullStringToNone(payload["isPromoted"]),
				_normalizeNullStringToNone(payload["isEntryLevel"]),
				_normalizeNullStringToNone(payload["isJunior"]),
				_normalizeNullStringToNone(payload["isMidLevel"]),
				_normalizeNullStringToNone(payload["isSenior"]),
				_normalizeNullStringToNone(payload["isLead"]),
				normalizedJSONText(_normalizeNullStringToNone(payload["requiredLanguages"])),
				normalizedUSStates,
				normalizedLocationCountries,
				normalizedTechStackJSON,
				_normalizeNullStringToNone(mapValue(payload, "salaryRange", "min")),
				_normalizeNullStringToNone(mapValue(payload, "salaryRange", "max")),
				_normalizeNullStringToNone(mapValue(payload, "salaryRange", "salaryType")),
				_normalizeNullStringToNone(mapValue(payload, "salaryRange", "currencyCode")),
				_normalizeNullStringToNone(mapValue(payload, "salaryRange", "currencySymbol")),
				_normalizeNullStringToNone(mapValue(payload, "salaryRange", "minSalaryAsUSD")),
				_normalizeNullStringToNone(mapValue(payload, "salaryRange", "maxSalaryAsUSD")),
				_normalizeNullStringToNone(mapValue(payload, "salaryRange", "salaryHumanReadableText")),
				time.Now().UTC().Format(time.RFC3339Nano),
			)
			return execErr
		})
		if err != nil {
			log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
			skipped++
			continue
		}
		retries, retryDelay = parsedLockRetryConfig()
		if err := database.RetryLocked(retries, retryDelay, func() error {
			_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true WHERE id = ?`, row.id)
			return err
		}); err != nil {
			log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
			skipped++
			continue
		}
		log.Printf("parsed-job-worker upsert_done raw_job_id=%d source=%s", row.id, row.source)
		processed++
	}
	log.Printf("parsed-job-worker batch_done rows=%d processed=%d skipped=%d", len(pending), processed, skipped)
	return processed, nil
}

func normalizeNameForKey(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return ""
	}
	normalized = strings.ReplaceAll(normalized, "&", " and ")
	normalized = regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(normalized, "-")
	normalized = regexp.MustCompile(`-+`).ReplaceAllString(normalized, "-")
	return strings.Trim(normalized, "-")
}

func hostFromURL(rawURL string) string {
	if strings.TrimSpace(rawURL) == "" {
		return ""
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	host = strings.Trim(host, ".")
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

func linkedinIdentityFromURL(rawURL string) string {
	if strings.TrimSpace(rawURL) == "" {
		return ""
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(parsed.Hostname()), "www."))
	if !strings.Contains(host, "linkedin.com") {
		return ""
	}
	path := regexp.MustCompile(`/+`).ReplaceAllString(parsed.Path, "/")
	path = strings.Trim(strings.ToLower(path), "/")
	if path == "" {
		return host
	}
	return host + "/" + path
}

func buildCompanyMatchKeysFromPayload(companyPayload map[string]any) map[string]struct{} {
	keys := map[string]struct{}{}
	linkedinURL := stringValue(companyPayload["linkedInURL"])
	if linkedinURL != "" {
		dom := domainFromURL(linkedinURL)
		if dom != "" {
			path := ""
			if parsed, err := url.Parse(linkedinURL); err == nil {
				path = strings.Trim(strings.ToLower(parsed.Path), "/")
			}
			if path != "" {
				keys["linkedin:"+dom+"/"+path] = struct{}{}
			} else {
				keys["linkedin:"+dom] = struct{}{}
			}
		}
	}
	homePageURL := stringValue(companyPayload["homePageURL"])
	if homePageURL != "" {
		dom := domainFromURL(homePageURL)
		host := hostFromURL(homePageURL)
		if dom != "" {
			keys["domain:"+dom] = struct{}{}
		}
		if host != "" && host != dom {
			keys["subdomain:"+host] = struct{}{}
		}
	}
	if normalizedName := normalizeNameForKey(stringValue(companyPayload["name"])); normalizedName != "" {
		keys["name:"+normalizedName] = struct{}{}
	}
	if normalizedSlug := normalizeNameForKey(stringValue(companyPayload["slug"])); normalizedSlug != "" {
		keys["slug:"+normalizedSlug] = struct{}{}
	}
	return keys
}

func buildCompanyMatchKeysFromRow(name, slug, linkedinURL, homePageURL string) map[string]struct{} {
	return buildCompanyMatchKeysFromPayload(map[string]any{
		"name":        name,
		"slug":        slug,
		"linkedInURL": linkedinURL,
		"homePageURL": homePageURL,
	})
}

func (s *Service) findExistingCompanyByMatchKeys(ctx context.Context, companyPayload map[string]any) (sql.NullInt64, error) {
	incomingKeys := buildCompanyMatchKeysFromPayload(companyPayload)
	if len(incomingKeys) == 0 {
		return sql.NullInt64{}, nil
	}
	incomingSlug := strings.TrimSpace(stringValue(companyPayload["slug"]))
	incomingName := strings.TrimSpace(stringValue(companyPayload["name"]))
	homePageURL := strings.TrimSpace(stringValue(companyPayload["homePageURL"]))
	linkedinURL := strings.TrimSpace(stringValue(companyPayload["linkedInURL"]))
	incomingLinkedinIdentity := linkedinIdentityFromURL(linkedinURL)
	homeDomain := domainFromURL(homePageURL)
	linkedinDomain := domainFromURL(linkedinURL)

	rows, err := s.DB.SQL.QueryContext(
		ctx,
		`SELECT id, COALESCE(name, ''), COALESCE(slug, ''), COALESCE(linkedin_url, ''), COALESCE(home_page_url, '')
		   FROM parsed_companies
		  WHERE
		        (? <> '' AND slug = ?)
		     OR (? <> '' AND name = ?)
		     OR (? <> '' AND home_page_url ILIKE ?)
		     OR (? <> '' AND linkedin_url ILIKE ?)
		  LIMIT 200`,
		incomingSlug, incomingSlug,
		incomingName, incomingName,
		homeDomain, "%"+homeDomain+"%",
		linkedinDomain, "%"+linkedinDomain+"%",
	)
	if err != nil {
		return sql.NullInt64{}, err
	}
	defer rows.Close()
	best := sql.NullInt64{}
	bestOverlap := 0
	bestLinkedinMatch := false
	for rows.Next() {
		var id int64
		var name, slug, candidateLinkedinURL, candidateHomePageURL string
		if scanErr := rows.Scan(&id, &name, &slug, &candidateLinkedinURL, &candidateHomePageURL); scanErr != nil {
			return sql.NullInt64{}, scanErr
		}
		candidateLinkedinIdentity := linkedinIdentityFromURL(candidateLinkedinURL)
		if incomingLinkedinIdentity != "" && candidateLinkedinIdentity != "" && incomingLinkedinIdentity != candidateLinkedinIdentity {
			continue
		}
		linkedinExactMatch := incomingLinkedinIdentity != "" && candidateLinkedinIdentity != "" && incomingLinkedinIdentity == candidateLinkedinIdentity
		candidateKeys := buildCompanyMatchKeysFromRow(name, slug, candidateLinkedinURL, candidateHomePageURL)
		overlap := 0
		for key := range incomingKeys {
			if _, ok := candidateKeys[key]; ok {
				overlap++
			}
		}
		if linkedinExactMatch && !bestLinkedinMatch {
			best = sql.NullInt64{Int64: id, Valid: true}
			bestOverlap = overlap
			bestLinkedinMatch = true
			continue
		}
		if linkedinExactMatch && bestLinkedinMatch && overlap > bestOverlap {
			best = sql.NullInt64{Int64: id, Valid: true}
			bestOverlap = overlap
			continue
		}
		if !linkedinExactMatch && bestLinkedinMatch {
			continue
		}
		if overlap > bestOverlap {
			best = sql.NullInt64{Int64: id, Valid: true}
			bestOverlap = overlap
			bestLinkedinMatch = false
		}
	}
	if err := rows.Err(); err != nil {
		return sql.NullInt64{}, err
	}
	if best.Valid && bestOverlap > 0 {
		return best, nil
	}
	return sql.NullInt64{}, nil
}

func (s *Service) upsertCompanyFromPayload(ctx context.Context, payload map[string]any, plugin plugins.SourcePlugin, pluginOK bool) (any, error) {
	companyPayload, _ := payload["company"].(map[string]any)
	if len(companyPayload) == 0 {
		return nil, nil
	}

	externalCompanyID := strings.TrimSpace(stringValue(_normalizeNullStringToNone(companyPayload["id"])))
	useExternalID := pluginOK && plugin.UseExternalCompanyID
	useMatchKeys := !pluginOK || plugin.UseCompanyMatchKeys
	var companyID sql.NullInt64

	if externalCompanyID != "" {
		err := s.DB.SQL.QueryRowContext(ctx, `SELECT id FROM parsed_companies WHERE external_company_id = ? LIMIT 1`, externalCompanyID).Scan(&companyID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}
	if !companyID.Valid && useMatchKeys {
		matchID, err := s.findExistingCompanyByMatchKeys(ctx, companyPayload)
		if err != nil {
			return nil, err
		}
		companyID = matchID
	}

	strField := func(key string) string {
		return strings.TrimSpace(stringValue(_normalizeNullStringToNone(companyPayload[key])))
	}
	jsonField := func(key string) any {
		return normalizedJSONText(_normalizeNullStringToNone(companyPayload[key]))
	}

	// Incoming values.
	externalCompanyIDVal := strField("id")
	nameVal := strField("name")
	slugVal := strField("slug")
	taglineVal := strField("tagline")
	foundedYearVal := strField("foundedYear")
	homePageURLVal := strField("homePageURL")
	linkedInURLVal := strField("linkedInURL")
	employeeRangeVal := strField("employeeRange")
	profilePicURLVal := strField("profilePicURL")
	sponsorsH1BVal := _normalizeNullStringToNone(companyPayload["sponsorsH1B"])
	sponsorsUKVal := _normalizeNullStringToNone(companyPayload["sponsorsUKSkilledWorkerVisa"])
	taglineBrazilVal := strField("taglineBrazil")
	taglineFranceVal := strField("taglineFrance")
	taglineGermanyVal := strField("taglineGermany")
	chatGPTDescriptionVal := strField("chatGPTDescription")
	linkedInDescriptionVal := strField("linkedInDescription")
	chatGPTDescriptionBrazilVal := strField("chatGPTDescriptionBrazil")
	chatGPTDescriptionFranceVal := strField("chatGPTDescriptionFrance")
	chatGPTDescriptionGermanyVal := strField("chatGPTDescriptionGermany")
	linkedInDescriptionBrazilVal := strField("linkedInDescriptionBrazil")
	linkedInDescriptionFranceVal := strField("linkedInDescriptionFrance")
	linkedInDescriptionGermanyVal := strField("linkedInDescriptionGermany")
	fundingDataVal := jsonField("fundingData")
	chatGPTIndustriesVal := jsonField("chatGPTIndustries")
	industrySpecialitiesVal := jsonField("industrySpecialities")
	industrySpecialitiesBrazilVal := jsonField("industrySpecialitiesBrazil")
	industrySpecialitiesFranceVal := jsonField("industrySpecialitiesFrance")
	industrySpecialitiesGermanyVal := jsonField("industrySpecialitiesGermany")

	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if companyID.Valid {
		var curExternalID, curName, curSlug, curTagline, curFoundedYear, curHomePageURL, curLinkedInURL, curEmployeeRange, curProfilePicURL sql.NullString
		var curSponsorsH1B, curSponsorsUK sql.NullBool
		var curTaglineBrazil, curTaglineFrance, curTaglineGermany, curChatGPTDescription, curLinkedInDescription sql.NullString
		var curChatGPTDescriptionBrazil, curChatGPTDescriptionFrance, curChatGPTDescriptionGermany sql.NullString
		var curLinkedInDescriptionBrazil, curLinkedInDescriptionFrance, curLinkedInDescriptionGermany sql.NullString
		var curFundingData, curChatGPTIndustries, curIndustrySpecialities, curIndustrySpecialitiesBrazil, curIndustrySpecialitiesFrance, curIndustrySpecialitiesGermany sql.NullString
		if err := s.DB.SQL.QueryRowContext(
			ctx,
			`SELECT external_company_id, name, slug, tagline, founded_year, home_page_url, linkedin_url, sponsors_h1b, sponsors_uk_skilled_worker_visa, employee_range, profile_pic_url,
			        tagline_brazil, tagline_france, tagline_germany, chatgpt_description, linkedin_description,
			        chatgpt_description_brazil, chatgpt_description_france, chatgpt_description_germany,
			        linkedin_description_brazil, linkedin_description_france, linkedin_description_germany,
			        funding_data::text, chatgpt_industries::text, industry_specialities::text, industry_specialities_brazil::text, industry_specialities_france::text, industry_specialities_germany::text
			   FROM parsed_companies WHERE id = ? LIMIT 1`,
			companyID.Int64,
		).Scan(
			&curExternalID, &curName, &curSlug, &curTagline, &curFoundedYear, &curHomePageURL, &curLinkedInURL, &curSponsorsH1B, &curSponsorsUK, &curEmployeeRange, &curProfilePicURL,
			&curTaglineBrazil, &curTaglineFrance, &curTaglineGermany, &curChatGPTDescription, &curLinkedInDescription,
			&curChatGPTDescriptionBrazil, &curChatGPTDescriptionFrance, &curChatGPTDescriptionGermany,
			&curLinkedInDescriptionBrazil, &curLinkedInDescriptionFrance, &curLinkedInDescriptionGermany,
			&curFundingData, &curChatGPTIndustries, &curIndustrySpecialities, &curIndustrySpecialitiesBrazil, &curIndustrySpecialitiesFrance, &curIndustrySpecialitiesGermany,
		); err != nil {
			return nil, err
		}
		chooseStr := func(current sql.NullString, incoming string) any {
			if useExternalID {
				if strings.TrimSpace(incoming) == "" {
					return nil
				}
				return incoming
			}
			if !current.Valid || strings.TrimSpace(current.String) == "" {
				if strings.TrimSpace(incoming) == "" {
					return nil
				}
				return incoming
			}
			return current.String
		}
		chooseJSON := func(current sql.NullString, incoming any) any {
			if useExternalID {
				return incoming
			}
			if !current.Valid || strings.TrimSpace(current.String) == "" {
				return incoming
			}
			return current.String
		}
		chooseBool := func(current sql.NullBool, incoming any) any {
			if useExternalID {
				return incoming
			}
			if !current.Valid {
				return incoming
			}
			return current.Bool
		}
		_, err := s.DB.SQL.ExecContext(
			ctx,
			`UPDATE parsed_companies
			    SET external_company_id = ?,
			        name = ?,
			        slug = ?,
			        tagline = ?,
			        founded_year = ?,
			        home_page_url = ?,
			        linkedin_url = ?,
			        sponsors_h1b = ?,
			        sponsors_uk_skilled_worker_visa = ?,
			        employee_range = ?,
			        profile_pic_url = ?,
			        tagline_brazil = ?,
			        tagline_france = ?,
			        tagline_germany = ?,
			        chatgpt_description = ?,
			        linkedin_description = ?,
			        chatgpt_description_brazil = ?,
			        chatgpt_description_france = ?,
			        chatgpt_description_germany = ?,
			        linkedin_description_brazil = ?,
			        linkedin_description_france = ?,
			        linkedin_description_germany = ?,
			        funding_data = ?,
			        chatgpt_industries = ?,
			        industry_specialities = ?,
			        industry_specialities_brazil = ?,
			        industry_specialities_france = ?,
			        industry_specialities_germany = ?,
			        updated_at = ?
			  WHERE id = ?`,
			chooseStr(curExternalID, externalCompanyIDVal),
			chooseStr(curName, nameVal),
			chooseStr(curSlug, slugVal),
			chooseStr(curTagline, taglineVal),
			chooseStr(curFoundedYear, foundedYearVal),
			chooseStr(curHomePageURL, homePageURLVal),
			chooseStr(curLinkedInURL, linkedInURLVal),
			chooseBool(curSponsorsH1B, sponsorsH1BVal),
			chooseBool(curSponsorsUK, sponsorsUKVal),
			chooseStr(curEmployeeRange, employeeRangeVal),
			chooseStr(curProfilePicURL, profilePicURLVal),
			chooseStr(curTaglineBrazil, taglineBrazilVal),
			chooseStr(curTaglineFrance, taglineFranceVal),
			chooseStr(curTaglineGermany, taglineGermanyVal),
			chooseStr(curChatGPTDescription, chatGPTDescriptionVal),
			chooseStr(curLinkedInDescription, linkedInDescriptionVal),
			chooseStr(curChatGPTDescriptionBrazil, chatGPTDescriptionBrazilVal),
			chooseStr(curChatGPTDescriptionFrance, chatGPTDescriptionFranceVal),
			chooseStr(curChatGPTDescriptionGermany, chatGPTDescriptionGermanyVal),
			chooseStr(curLinkedInDescriptionBrazil, linkedInDescriptionBrazilVal),
			chooseStr(curLinkedInDescriptionFrance, linkedInDescriptionFranceVal),
			chooseStr(curLinkedInDescriptionGermany, linkedInDescriptionGermanyVal),
			chooseJSON(curFundingData, fundingDataVal),
			chooseJSON(curChatGPTIndustries, chatGPTIndustriesVal),
			chooseJSON(curIndustrySpecialities, industrySpecialitiesVal),
			chooseJSON(curIndustrySpecialitiesBrazil, industrySpecialitiesBrazilVal),
			chooseJSON(curIndustrySpecialitiesFrance, industrySpecialitiesFranceVal),
			chooseJSON(curIndustrySpecialitiesGermany, industrySpecialitiesGermanyVal),
			updatedAt,
			companyID.Int64,
		)
		if err != nil {
			return nil, err
		}
		return companyID.Int64, nil
	}

	var insertedID int64
	err := s.DB.SQL.QueryRowContext(
		ctx,
		`INSERT INTO parsed_companies (
		    external_company_id, name, slug, tagline, founded_year, home_page_url, linkedin_url, sponsors_h1b, sponsors_uk_skilled_worker_visa,
		    employee_range, profile_pic_url, tagline_brazil, tagline_france, tagline_germany, chatgpt_description, linkedin_description,
		    chatgpt_description_brazil, chatgpt_description_france, chatgpt_description_germany, linkedin_description_brazil, linkedin_description_france, linkedin_description_germany,
		    funding_data, chatgpt_industries, industry_specialities, industry_specialities_brazil, industry_specialities_france, industry_specialities_germany, updated_at
		  )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 RETURNING id`,
		nilIfEmpty(externalCompanyIDVal),
		nilIfEmpty(nameVal),
		nilIfEmpty(slugVal),
		nilIfEmpty(taglineVal),
		nilIfEmpty(foundedYearVal),
		nilIfEmpty(homePageURLVal),
		nilIfEmpty(linkedInURLVal),
		sponsorsH1BVal,
		sponsorsUKVal,
		nilIfEmpty(employeeRangeVal),
		nilIfEmpty(profilePicURLVal),
		nilIfEmpty(taglineBrazilVal),
		nilIfEmpty(taglineFranceVal),
		nilIfEmpty(taglineGermanyVal),
		nilIfEmpty(chatGPTDescriptionVal),
		nilIfEmpty(linkedInDescriptionVal),
		nilIfEmpty(chatGPTDescriptionBrazilVal),
		nilIfEmpty(chatGPTDescriptionFranceVal),
		nilIfEmpty(chatGPTDescriptionGermanyVal),
		nilIfEmpty(linkedInDescriptionBrazilVal),
		nilIfEmpty(linkedInDescriptionFranceVal),
		nilIfEmpty(linkedInDescriptionGermanyVal),
		fundingDataVal,
		chatGPTIndustriesVal,
		industrySpecialitiesVal,
		industrySpecialitiesBrazilVal,
		industrySpecialitiesFranceVal,
		industrySpecialitiesGermanyVal,
		updatedAt,
	).Scan(&insertedID)
	if err != nil {
		return nil, err
	}
	return insertedID, nil
}

func normalizeJobURLForMatch(rawURL string) string {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	host = strings.TrimPrefix(host, "www.")
	path := regexp.MustCompile(`/+`).ReplaceAllString(parsed.EscapedPath(), "/")
	path = strings.TrimRight(path, "/")
	if path == "" {
		path = "/"
	}
	return strings.ToLower(host + path)
}

func (s *Service) findDuplicateCrossSourceParsedJob(ctx context.Context, rawJobID int64, source string, payload map[string]any, companyID any) (int64, bool, error) {
	plugin, ok := plugins.Get(strings.TrimSpace(source))
	if ok && !plugin.RunDuplicateCheck {
		return 0, false, nil
	}
	sourceCreatedAt := parseDT(payload["created_at"])
	sourceURLNorm := normalizeJobURLForMatch(stringValue(payload["url"]))
	if sourceURLNorm == "" {
		return 0, false, nil
	}
	urlHost := sourceURLNorm
	if slashIdx := strings.Index(urlHost, "/"); slashIdx > 0 {
		urlHost = urlHost[:slashIdx]
	}
	companyIDInt, companyIDOK := companyID.(int64)
	var companyIDFilter any
	if companyIDOK {
		companyIDFilter = companyIDInt
	}
	var lowerBound any
	var upperBound any
	if sourceCreatedAt != nil {
		lowerBound = sourceCreatedAt.UTC().Add(-maxDuplicatePostDateDiff)
		upperBound = sourceCreatedAt.UTC().Add(maxDuplicatePostDateDiff)
	}
	query := `SELECT p.id, p.url
	   FROM parsed_jobs p
	   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
	  WHERE r.source <> ?
	    AND p.url IS NOT NULL
	    AND LOWER(p.url) LIKE ?
	    AND p.raw_us_job_id <> ?
		AND (?::bigint IS NULL OR p.company_id = ?::bigint)
		AND (
			?::timestamptz IS NULL
			OR (
				p.created_at_source IS NOT NULL
				AND p.created_at_source >= ?::timestamptz
				AND p.created_at_source <= ?::timestamptz
			)
		)
	  ORDER BY p.updated_at DESC, p.id DESC
	  LIMIT 1000`
	args := []any{
		source,
		"%" + strings.ToLower(urlHost) + "%",
		rawJobID,
		companyIDFilter,
		companyIDFilter,
		lowerBound,
		lowerBound,
		upperBound,
	}
	rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var duplicateID int64
		var candidateURL sql.NullString
		if scanErr := rows.Scan(&duplicateID, &candidateURL); scanErr != nil {
			return 0, false, scanErr
		}
		if normalizeJobURLForMatch(candidateURL.String) == sourceURLNorm {
			return duplicateID, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	return 0, false, nil
}

func normalizedJSONText(value any) any {
	switch item := value.(type) {
	case []any:
		body, err := json.Marshal(item)
		if err != nil {
			return nil
		}
		return string(body)
	case []string:
		body, err := json.Marshal(item)
		if err != nil {
			return nil
		}
		return string(body)
	case map[string]any:
		body, err := json.Marshal(item)
		if err != nil {
			return nil
		}
		return string(body)
	default:
		return nil
	}
}

func formatNullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func _normalizeNullStringToNone(value any) any {
	if text, ok := value.(string); ok {
		trimmed := strings.TrimSpace(text)
		if strings.EqualFold(trimmed, "null") {
			return nil
		}
		return trimmed
	}
	return value
}

func mapValue(payload map[string]any, key, nestedKey string) any {
	item, _ := payload[key].(map[string]any)
	if item == nil {
		return nil
	}
	return item[nestedKey]
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
