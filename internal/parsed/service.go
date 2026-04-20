package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/extract/techstack"
	"goapplyjob-golang-backend/internal/normalize/techstacknorm"
	"goapplyjob-golang-backend/internal/sources/plugins"
	"log"
	"net/mail"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"golang.org/x/net/publicsuffix"
)

const (
	sourceRemoteRocketship   = "remoterocketship"
	envParsedDBLockRetries   = "PARSED_JOB_DB_LOCK_RETRIES"
	envParsedDBLockDelay     = "PARSED_JOB_DB_LOCK_RETRY_DELAY_SECONDS"
	maxDuplicatePostDateDiff = 48 * time.Hour
	similarCategoryScanBatch = 1000
	similarCategoryMaxScan   = 2000
	similarCategoryQueryTopN = 5
	externalCompanyIDPrefix  = "gaj("
	externalCompanyIDSuffix  = ")gaj"
)

var seniorityTokens = map[string]struct{}{
	"senior": {}, "sr": {}, "junior": {}, "jr": {},
	"associate": {}, "lead": {}, "principal": {}, "staff": {},
	"entry": {}, "mid": {}, "midlevel": {}, "expert": {}, "leader": {},
	"level": {}, "lvl": {},
	"l1": {}, "l2": {}, "l3": {}, "l4": {}, "l5": {}, "l6": {}, "l7": {}, "l8": {},
	"ii": {}, "iii": {}, "iv": {}, "v": {}, "vi": {}, "vii": {}, "viii": {}, "ix": {}, "x": {},
}

var employmentNoiseTokens = map[string]struct{}{
	"full": {}, "time": {}, "fulltime": {}, "part": {}, "parttime": {},
	"contract": {}, "contractor": {}, "temp": {}, "temporary": {},
	"intern": {}, "internship": {}, "freelance": {}, "freelancer": {},
	"permanent": {}, "fixedterm": {}, "hourly": {}, "salaried": {},
	"apprentice": {}, "apprenticeship": {}, "volunteer": {},
	"seasonal": {}, "weekend": {}, "weekends": {}, "night": {}, "evening": {}, "overnight": {},
	"urgent": {}, "immediate": {}, "hiring": {}, "opening": {}, "opportunity": {},
}

var workModeNoiseTokens = map[string]struct{}{
	"remote": {}, "hybrid": {}, "onsite": {}, "wfh": {}, "office": {},
	"homebased": {}, "telecommute": {}, "telecommuting": {},
	"telework": {}, "teleworking": {}, "distributed": {},
	"remotefirst": {}, "remoteonly": {}, "inoffice": {},
}

var genericCategoryMatchTokens = map[string]struct{}{
	"engineer": {}, "developer": {}, "manager": {}, "specialist": {},
	"consultant": {}, "analyst": {}, "architect": {}, "designer": {},
	"director": {}, "representative": {}, "support": {}, "technical": {},
	"solutions": {}, "administrator": {}, "producer": {}, "writer": {},
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

var normalizationReplacements = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{pattern: regexp.MustCompile(`\bgai\b`), replacement: "generative artificial intelligence"},
	{pattern: regexp.MustCompile(`\bdev[\s\-]*ops\b`), replacement: "devops"},
	{pattern: regexp.MustCompile(`\brev[\s\-]*ops\b`), replacement: "revenue operations"},
	{pattern: regexp.MustCompile(`\bfin[\s\-]*ops\b`), replacement: "finance operations"},
	{pattern: regexp.MustCompile(`\bsec[\s\-]*ops\b`), replacement: "security operations"},
	{pattern: regexp.MustCompile(`\bit[\s\-]*ops\b`), replacement: "information technology operations"},
	{pattern: regexp.MustCompile(`\bdev\b`), replacement: "developer"},
	{pattern: regexp.MustCompile(`\beng\b`), replacement: "engineer"},
	{pattern: regexp.MustCompile(`\bmgr\b`), replacement: "manager"},
	{pattern: regexp.MustCompile(`\bdir\b`), replacement: "director"},
	{pattern: regexp.MustCompile(`\brep\b`), replacement: "representative"},
	{pattern: regexp.MustCompile(`\bspec\b`), replacement: "specialist"},
	{pattern: regexp.MustCompile(`\bassoc\b`), replacement: "associate"},
	{pattern: regexp.MustCompile(`\basst\b`), replacement: "assistant"},
	{pattern: regexp.MustCompile(`\bbdr\b`), replacement: "business development representative"},
	{pattern: regexp.MustCompile(`\bsdr\b`), replacement: "sales development representative"},
	{pattern: regexp.MustCompile(`\bae\b`), replacement: "account executive"},
	{pattern: regexp.MustCompile(`\bam\b`), replacement: "account manager"},
	{pattern: regexp.MustCompile(`\bcsm\b`), replacement: "customer success manager"},
	{pattern: regexp.MustCompile(`\bcs\b`), replacement: "customer success"},
	{pattern: regexp.MustCompile(`\bse\b`), replacement: "sales engineer"},
	{pattern: regexp.MustCompile(`\bsa\b`), replacement: "solutions architect"},
	{pattern: regexp.MustCompile(`\bba\b`), replacement: "business analyst"},
	{pattern: regexp.MustCompile(`\bbi\b`), replacement: "business intelligence"},
	{pattern: regexp.MustCompile(`\bda\b`), replacement: "data analyst"},
	{pattern: regexp.MustCompile(`\bds\b`), replacement: "data scientist"},
	{pattern: regexp.MustCompile(`\bqa\b`), replacement: "quality assurance"},
	{pattern: regexp.MustCompile(`\bqe\b`), replacement: "quality engineer"},
	{pattern: regexp.MustCompile(`\bsdet\b`), replacement: "software development engineer in test"},
	{pattern: regexp.MustCompile(`\bswe\b`), replacement: "software engineer"},
	{pattern: regexp.MustCompile(`\bsre\b`), replacement: "site reliability engineer"},
	{pattern: regexp.MustCompile(`\bpmm\b`), replacement: "product marketing manager"},
	{pattern: regexp.MustCompile(`\bpm\b`), replacement: "product manager"},
	{pattern: regexp.MustCompile(`\bgm\b`), replacement: "general manager"},
	{pattern: regexp.MustCompile(`\bvp\b`), replacement: "vice president"},
	{pattern: regexp.MustCompile(`\bavp\b`), replacement: "assistant vice president"},
	{pattern: regexp.MustCompile(`\bsvp\b`), replacement: "senior vice president"},
	{pattern: regexp.MustCompile(`\bevp\b`), replacement: "executive vice president"},
	{pattern: regexp.MustCompile(`\bceo\b`), replacement: "chief executive officer"},
	{pattern: regexp.MustCompile(`\bcoo\b`), replacement: "chief operating officer"},
	{pattern: regexp.MustCompile(`\bcfo\b`), replacement: "chief financial officer"},
	{pattern: regexp.MustCompile(`\bcio\b`), replacement: "chief information officer"},
	{pattern: regexp.MustCompile(`\bcto\b`), replacement: "chief technology officer"},
	{pattern: regexp.MustCompile(`\bcmo\b`), replacement: "chief marketing officer"},
	{pattern: regexp.MustCompile(`\bcro\b`), replacement: "chief revenue officer"},
	{pattern: regexp.MustCompile(`\bcpo\b`), replacement: "chief product officer"},
	{pattern: regexp.MustCompile(`\bchro\b`), replacement: "chief human resources officer"},
	{pattern: regexp.MustCompile(`\bcso\b`), replacement: "chief strategy officer"},
	{pattern: regexp.MustCompile(`\bciso\b`), replacement: "chief information security officer"},
	{pattern: regexp.MustCompile(`\bta\b`), replacement: "talent acquisition"},
	{pattern: regexp.MustCompile(`\bhr\b`), replacement: "human resources"},
	{pattern: regexp.MustCompile(`\bhrbp\b`), replacement: "human resources business partner"},
	{pattern: regexp.MustCompile(`\bl&d\b`), replacement: "learning and development"},
	{pattern: regexp.MustCompile(`\br&d\b`), replacement: "research and development"},
	{pattern: regexp.MustCompile(`\bm&a\b`), replacement: "mergers and acquisitions"},
	{pattern: regexp.MustCompile(`\bfp&a\b`), replacement: "financial planning and analysis"},
	{pattern: regexp.MustCompile(`\bseo\b`), replacement: "search engine optimization"},
	{pattern: regexp.MustCompile(`\bsem\b`), replacement: "search engine marketing"},
	{pattern: regexp.MustCompile(`\bpmo\b`), replacement: "project management office"},
	{pattern: regexp.MustCompile(`\bcrm\b`), replacement: "customer relationship management"},
	{pattern: regexp.MustCompile(`\berp\b`), replacement: "enterprise resource planning"},
	{pattern: regexp.MustCompile(`\bcpg\b`), replacement: "consumer packaged goods"},
}

type Service struct {
	DB                    *database.DB
	EnabledSources        map[string]struct{}
	Config                Config
	categorySignalCatalog map[string]categorySignalTerms
	duplicateJobURLRules  *duplicateJobURLRuleSet
	techStackExtractor    techstack.Extractor
}

type parsedJobURLDuplicateMatch struct {
	id         int64
	sameSource bool
}

type Config struct {
	BatchSize               int
	PollSeconds             float64
	RunOnce                 bool
	ErrorBackoffSeconds     int
	WorkerCount             int
	CategorySignalTokensURL string
	DuplicateJobURLRulesURL string
	TechStackCatalogURL     string
}

func New(cfg Config, db *database.DB) *Service {
	return &Service{
		DB:                    db,
		Config:                cfg,
		categorySignalCatalog: getCategorySignalCatalog(cfg.CategorySignalTokensURL),
		duplicateJobURLRules:  getDuplicateJobURLRuleSet(cfg.DuplicateJobURLRulesURL),
		techStackExtractor:    techstack.NewExtractor(cfg.TechStackCatalogURL),
	}
}

func (s *Service) RunOnce(ctx context.Context) (int, error) {
	batchSize := s.Config.BatchSize
	if batchSize < 1 {
		batchSize = 100
	}
	return s.ProcessPending(ctx, batchSize)
}

func (s *Service) RunForever() error {
	pollSeconds := s.Config.PollSeconds
	if pollSeconds < 1 {
		pollSeconds = 1
	}
	errorBackoffSeconds := s.Config.ErrorBackoffSeconds
	if errorBackoffSeconds < 1 {
		errorBackoffSeconds = 1
	}
	for {
		processed, err := s.RunOnce(context.Background())
		if err != nil {
			log.Printf("parsed-job-worker cycle_failed error=%v", err)
			if s.Config.RunOnce {
				return err
			}
			time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
			continue
		}
		if s.Config.RunOnce {
			if processed == 0 {
				log.Printf("parsed-job-worker run-once completed: no pending parsed rows")
			} else {
				log.Printf("parsed-job-worker run-once completed processed=%d", processed)
			}
			return nil
		}
		time.Sleep(time.Duration(pollSeconds * float64(time.Second)))
	}
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

var similarityTokenSplitRegexp = regexp.MustCompile(`[^a-z0-9]+`)

var pluralTokenReplacements = map[string]string{
	"engineers":       "engineer",
	"developers":      "developer",
	"managers":        "manager",
	"specialists":     "specialist",
	"consultants":     "consultant",
	"analysts":        "analyst",
	"architects":      "architect",
	"designers":       "designer",
	"directors":       "director",
	"representatives": "representative",
	"administrators":  "administrator",
	"producers":       "producer",
	"writers":         "writer",
}

func normalizeRoleToken(token string) string {
	if replacement, ok := pluralTokenReplacements[token]; ok {
		return replacement
	}
	return token
}

func tokenizeRoleTitleForSimilarity(roleTitle string) map[string]struct{} {
	rawTokens := similarityTokenSplitRegexp.Split(normalizeTextForMatching(roleTitle), -1)
	out := map[string]struct{}{}
	for _, token := range rawTokens {
		if len(token) <= 1 {
			continue
		}
		token = normalizeRoleToken(token)
		if shouldSkipRoleToken(token) {
			continue
		}
		out[token] = struct{}{}
	}
	return out
}

func tokenizeTextForSequence(value string) []string {
	rawTokens := similarityTokenSplitRegexp.Split(normalizeTextForMatching(value), -1)
	out := make([]string, 0, len(rawTokens))
	for _, token := range rawTokens {
		if len(token) <= 1 {
			continue
		}
		token = normalizeRoleToken(token)
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
	return tokenizeTextForSequence(value)
}

func tokenizeTechStackForSimilarity(values []string) map[string]struct{} {
	tokens := map[string]struct{}{}
	for _, value := range values {
		raw := similarityTokenSplitRegexp.Split(strings.ToLower(strings.TrimSpace(value)), -1)
		for _, token := range raw {
			if token != "" && len(token) > 1 {
				tokens[token] = struct{}{}
			}
		}
	}
	return tokens
}

func normalizeSkillValuesForQuery(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(techstacknorm.NormalizeValue(value)))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func buildSimilarityQueryTokens(sourceSequenceTokens []string) []string {
	seen := make(map[string]struct{}, len(sourceSequenceTokens))
	out := make([]string, 0, len(sourceSequenceTokens))

	for _, token := range sourceSequenceTokens {
		if token == "" {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}

		out = append(out, token)
	}

	sort.SliceStable(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	if len(out) > similarCategoryQueryTopN {
		out = out[:similarCategoryQueryTopN]
	}
	return out
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

func (s *Service) findSimilarRemoteRoekctshipCategories(ctx context.Context, roleTitle string, sourceTechStack []string) (string, string, error) {
	sourceTokens := tokenizeRoleTitleForSimilarity(roleTitle)
	sourceSequenceTokens := tokenizeTextForSequence(roleTitle)
	sourceNormalizedTitle := normalizeRoleTitleForExactMatch(roleTitle)
	sourceSkillValues := normalizeSkillValuesForQuery(sourceTechStack)
	sourceSkillTokens := tokenizeTechStackForSimilarity(sourceSkillValues)

	if len(sourceTokens) == 0 && len(sourceSequenceTokens) == 0 && len(sourceSkillTokens) == 0 {
		return "", "", nil
	}

	sourceTokenSet := make(map[string]struct{}, len(sourceSequenceTokens))
	for _, token := range sourceSequenceTokens {
		sourceTokenSet[token] = struct{}{}
	}

	sourceHasSpecificTokens := false
	sourceSpecificTokens := map[string]struct{}{}
	for token := range sourceTokens {
		if isGenericCategoryToken(token) {
			continue
		}
		sourceHasSpecificTokens = true
		sourceSpecificTokens[token] = struct{}{}
	}

	sourceTokenWeights := make(map[string]float64, len(sourceTokens))
	for token := range sourceTokens {
		sourceTokenWeights[token] = tokenSpecificityWeight(token)
	}

	sourceSpecificWeights := make(map[string]float64, len(sourceSpecificTokens))
	for token := range sourceSpecificTokens {
		sourceSpecificWeights[token] = tokenSpecificityWeight(token)
	}
	categorySignalCatalog := s.categorySignalCatalog

	catalogTokenSet := map[string]struct{}{}
	categoryByToken := map[string]map[string]struct{}{}
	for categoryKey, terms := range categorySignalCatalog {
		for token := range terms.tokens {
			catalogTokenSet[token] = struct{}{}
			categories := categoryByToken[token]
			if categories == nil {
				categories = map[string]struct{}{}
				categoryByToken[token] = categories
			}
			categories[categoryKey] = struct{}{}
		}
	}
	filteredSequenceTokens := make([]string, 0, len(sourceSequenceTokens))
	filteredCategorySet := map[string]struct{}{}
	for _, token := range sourceSequenceTokens {
		if _, ok := catalogTokenSet[token]; ok {
			filteredSequenceTokens = append(filteredSequenceTokens, token)
			for category := range categoryByToken[token] {
				filteredCategorySet[category] = struct{}{}
			}
		}
	}
	if len(filteredSequenceTokens) == 0 {
		filteredSequenceTokens = sourceSequenceTokens
		filteredCategorySet = map[string]struct{}{}
	}
	filteredCategories := make([]string, 0, len(filteredCategorySet))
	for category := range filteredCategorySet {
		filteredCategories = append(filteredCategories, strings.ToLower(category))
	}

	roleTokenList := make([]string, 0, len(sourceTokens))
	for token := range sourceTokens {
		roleTokenList = append(roleTokenList, token)
	}
	sourceSequenceNormalizedTokens := tokenizeTextForSequence(sourceNormalizedTitle)

	findAllTokensMatch := func(tokens []string, applySkillFilter bool, applyCategoryFilter bool) (string, string, bool, error) {
		if len(tokens) == 0 {
			return "", "", false, nil
		}
		useCategoryDistinct := applyCategoryFilter && len(filteredCategories) > 0
		query := `SELECT `
		if useCategoryDistinct {
			query += `DISTINCT ON (LOWER(p.categorized_job_title)) `
		}
		query += `p.role_title, p.categorized_job_title, p.categorized_job_function, COALESCE(p.tech_stack::text, '[]')
			FROM parsed_jobs p
			JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
			WHERE r.source = ?
			  AND p.role_title IS NOT NULL
			  AND p.categorized_job_title IS NOT NULL
			  AND p.categorized_job_function IS NOT NULL`
		args := []any{sourceRemoteRocketship}
		if applyCategoryFilter && len(filteredCategories) > 0 {
			query += ` AND LOWER(p.categorized_job_title) = ANY(?::text[])`
			args = append(args, filteredCategories)
		}
		for _, token := range tokens {
			query += ` AND LOWER(p.role_title) LIKE '%' || ? || '%'`
			args = append(args, token)
		}
		if applySkillFilter && len(sourceSkillValues) > 0 {
			query += ` AND EXISTS (
				SELECT 1
				FROM jsonb_array_elements_text(COALESCE(p.tech_stack, '[]'::jsonb)) AS skill
				WHERE LOWER(skill) = ANY(?::text[])
			)`
			args = append(args, sourceSkillValues)
		}
		if useCategoryDistinct {
			query += ` ORDER BY LOWER(p.categorized_job_title), p.updated_at DESC, p.id DESC`
		} else {
			query += ` ORDER BY p.updated_at DESC, p.id DESC LIMIT 100`
		}

		rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
		if err != nil {
			return "", "", false, err
		}
		defer rows.Close()

		bestSet := false
		bestTitle := ""
		bestFunction := ""
		bestSignalWeight := -1.0
		bestSkillOverlapCount := -1
		bestSkillOverlapRatio := -1.0
		bestCategoryOverlap := -1
		bestSequence := -1
		bestExactRoleMatch := false

		for rows.Next() {
			var candidateRoleTitle, candidateTitle sql.NullString
			var candidateFunction sql.NullString
			var candidateTechStackRaw sql.NullString
			if err := rows.Scan(&candidateRoleTitle, &candidateTitle, &candidateFunction, &candidateTechStackRaw); err != nil {
				return "", "", false, err
			}

			titleTokens := tokenizeTextForSequence(candidateTitle.String)
			titleTokenSet := map[string]struct{}{}
			for _, token := range titleTokens {
				titleTokenSet[token] = struct{}{}
			}

			combinedCategorySet := map[string]struct{}{}
			for _, token := range titleTokens {
				combinedCategorySet[token] = struct{}{}
			}
			// function tokens removed from overlap signals

			candidateSkillTokens := tokenizeTechStackForSimilarity(parseStringJSONArray(candidateTechStackRaw.String))
			skillOverlapCount := setIntersectionCount(sourceSkillTokens, candidateSkillTokens)
			skillOverlapRatio := 0.0
			if len(sourceSkillTokens) > 0 {
				skillOverlapRatio = float64(skillOverlapCount) / float64(len(sourceSkillTokens))
			}

			categoryOverlapCount := setIntersectionCount(sourceTokens, combinedCategorySet)
			candidateNormalizedRole := normalizeRoleTitleForExactMatch(candidateRoleTitle.String)
			candidateRoleSequenceTokens := tokenizeTextForSequence(candidateNormalizedRole)
			sequenceCount := orderedTokenMatchCount(sourceSequenceNormalizedTokens, candidateRoleSequenceTokens)
			exactRoleMatch := candidateNormalizedRole != "" && candidateNormalizedRole == sourceNormalizedTitle

			// skipCandidate := false
			// if sourceHasSpecificTokens && len(titleTokenSet) == 1 && !exactRoleMatch {
			// 	for token := range titleTokenSet {
			// 		if isGenericCategoryToken(token) {
			// 			skipCandidate = true
			// 			break
			// 		}
			// 	}
			// }
			// if skipCandidate {
			// 	continue
			// }

			signalWeight := categorySignalWeightFromCatalog(categorySignalCatalog, sourceNormalizedTitle, candidateTitle.String, candidateFunction.String)

			if !bestSet ||
				(exactRoleMatch && !bestExactRoleMatch) ||
				(exactRoleMatch == bestExactRoleMatch && signalWeight > bestSignalWeight) ||
				(exactRoleMatch == bestExactRoleMatch && signalWeight == bestSignalWeight && skillOverlapCount > bestSkillOverlapCount) ||
				(exactRoleMatch == bestExactRoleMatch && signalWeight == bestSignalWeight && skillOverlapCount == bestSkillOverlapCount && skillOverlapRatio > bestSkillOverlapRatio) ||
				(exactRoleMatch == bestExactRoleMatch && signalWeight == bestSignalWeight && skillOverlapCount == bestSkillOverlapCount && skillOverlapRatio == bestSkillOverlapRatio && categoryOverlapCount > bestCategoryOverlap) ||
				(exactRoleMatch == bestExactRoleMatch && signalWeight == bestSignalWeight && skillOverlapCount == bestSkillOverlapCount && skillOverlapRatio == bestSkillOverlapRatio && categoryOverlapCount == bestCategoryOverlap && sequenceCount > bestSequence) {
				bestSet = true
				bestTitle = candidateTitle.String
				bestFunction = candidateFunction.String
				bestSignalWeight = signalWeight
				bestSkillOverlapCount = skillOverlapCount
				bestSkillOverlapRatio = skillOverlapRatio
				bestCategoryOverlap = categoryOverlapCount
				bestSequence = sequenceCount
				bestExactRoleMatch = exactRoleMatch
			}
		}

		if err := rows.Err(); err != nil {
			return "", "", false, err
		}
		if bestTitle != "" {
			return bestTitle, bestFunction, true, nil
		}
		return "", "", false, nil
	}
	tokenListArrays := [][]string{roleTokenList}
	if len(roleTokenList) != len(filteredSequenceTokens) {
		tokenListArrays = append(tokenListArrays, filteredSequenceTokens)
	}

	for _, tokenList := range tokenListArrays {
		for _, applyFilterSkill := range []bool{true, false} {
			for _, applyFilterCategory := range []bool{true, false} {
				// Implementation for each combination of filters

				title, function, ok, err := findAllTokensMatch(tokenList, applyFilterSkill, applyFilterCategory)
				if err != nil {
					return "Any", "Any", err
				}
				if ok {
					return title, function, nil
				}
			}
		}
	}

	queryTokens := buildSimilarityQueryTokens(filteredSequenceTokens)

	type matchRank struct {
		exactNormalizedTitleMatch     int
		categorySignalWeight          float64
		nonGenericCategoryPreference  int
		weightedSpecificTokenHits     float64
		weightedCategoryOverlap       float64
		weightedRoleOverlap           float64
		combinedSpecificTokenHits     int
		combinedCategoryOverlap       int
		categoryOverlapCount          int
		functionOverlapCount          int
		overlapCount                  int
		overlapRatio                  float64
		totalSequenceMatchCount       int
		skillOverlapCount             int
		skillOverlapRatio             float64
		roleJaccard                   float64
		genericOneWordCategoryPenalty int
	}

	rankGreater := func(left, right matchRank) bool {
		switch {
		case left.exactNormalizedTitleMatch != right.exactNormalizedTitleMatch:
			return left.exactNormalizedTitleMatch > right.exactNormalizedTitleMatch
		case left.categorySignalWeight != right.categorySignalWeight:
			return left.categorySignalWeight > right.categorySignalWeight
		case left.nonGenericCategoryPreference != right.nonGenericCategoryPreference:
			return left.nonGenericCategoryPreference > right.nonGenericCategoryPreference
		case left.genericOneWordCategoryPenalty != right.genericOneWordCategoryPenalty:
			return left.genericOneWordCategoryPenalty < right.genericOneWordCategoryPenalty
		case left.weightedSpecificTokenHits != right.weightedSpecificTokenHits:
			return left.weightedSpecificTokenHits > right.weightedSpecificTokenHits
		case left.combinedSpecificTokenHits != right.combinedSpecificTokenHits:
			return left.combinedSpecificTokenHits > right.combinedSpecificTokenHits
		case left.weightedCategoryOverlap != right.weightedCategoryOverlap:
			return left.weightedCategoryOverlap > right.weightedCategoryOverlap
		case left.weightedRoleOverlap != right.weightedRoleOverlap:
			return left.weightedRoleOverlap > right.weightedRoleOverlap
		case left.combinedCategoryOverlap != right.combinedCategoryOverlap:
			return left.combinedCategoryOverlap > right.combinedCategoryOverlap
		case left.categoryOverlapCount != right.categoryOverlapCount:
			return left.categoryOverlapCount > right.categoryOverlapCount
		case left.skillOverlapCount != right.skillOverlapCount:
			return left.skillOverlapCount > right.skillOverlapCount
		case left.skillOverlapRatio != right.skillOverlapRatio:
			return left.skillOverlapRatio > right.skillOverlapRatio
		case left.totalSequenceMatchCount != right.totalSequenceMatchCount:
			return left.totalSequenceMatchCount > right.totalSequenceMatchCount
		case left.overlapCount != right.overlapCount:
			return left.overlapCount > right.overlapCount
		case left.overlapRatio != right.overlapRatio:
			return left.overlapRatio > right.overlapRatio
		case left.functionOverlapCount != right.functionOverlapCount:
			return left.functionOverlapCount > right.functionOverlapCount
		default:
			return left.roleJaccard > right.roleJaccard
		}
	}

	isConfidentMatch := func(rank matchRank) bool {
		requiredOverlap := 2
		if len(sourceTokens) <= 1 {
			requiredOverlap = 1
		}

		hasTechSignal := len(sourceSkillTokens) > 0 &&
			(rank.skillOverlapCount >= 2 || (rank.skillOverlapCount >= 1 && rank.skillOverlapRatio >= 0.25))

		minSpecificHits := 0
		minWeightedSpecific := 0.0
		if len(sourceSpecificTokens) > 0 {
			minSpecificHits = 1
			minWeightedSpecific = 0.35
		}
		hasSpecificSignal := rank.combinedSpecificTokenHits >= minSpecificHits ||
			rank.weightedSpecificTokenHits >= minWeightedSpecific

		hasCategorySignal := rank.categoryOverlapCount >= 2 ||
			(len(sourceTokens) >= 2 && rank.categoryOverlapCount >= 1 && hasSpecificSignal)

		requiredSequenceMatches := 2
		if len(sourceSequenceTokens) >= 4 {
			requiredSequenceMatches = 3
		}
		hasSequenceSignal := rank.totalSequenceMatchCount >= requiredSequenceMatches

		hasWeightedOverlapSignal := sourceHasSpecificTokens &&
			(rank.weightedCategoryOverlap+rank.weightedRoleOverlap) >= 0.5

		hasCategorySignalWeight := rank.categorySignalWeight > 0

		if len(sourceTokens) <= 1 {
			return rank.exactNormalizedTitleMatch == 1 ||
				hasCategorySignalWeight ||
				hasTechSignal ||
				(hasSequenceSignal && rank.categoryOverlapCount >= 1)
		}

		hasOverlapSignal := rank.overlapCount >= requiredOverlap && rank.overlapRatio >= 0.35

		return rank.exactNormalizedTitleMatch == 1 ||
			hasCategorySignalWeight ||
			hasTechSignal ||
			hasCategorySignal ||
			hasSequenceSignal ||
			hasSpecificSignal ||
			hasWeightedOverlapSignal ||
			hasOverlapSignal
	}

	hasAnySignal := func(rank matchRank) bool {
		return rank.exactNormalizedTitleMatch == 1 ||
			rank.categorySignalWeight > 0 ||
			rank.skillOverlapCount > 0 ||
			rank.categoryOverlapCount > 0 ||
			rank.functionOverlapCount > 0 ||
			rank.overlapCount > 0 ||
			rank.totalSequenceMatchCount > 0 ||
			rank.weightedSpecificTokenHits > 0 ||
			rank.weightedCategoryOverlap > 0 ||
			rank.weightedRoleOverlap > 0
	}

	directMatchTitle := ""
	directMatchFunction := ""
	directMatchCandidateRoleTitle := ""
	directMatchScore := -1

	bestTitle := ""
	bestFunction := ""
	bestCandidateRoleTitle := ""
	bestRankSet := false
	var bestRank matchRank

	softFallbackTitle := ""
	softFallbackFunction := ""
	softFallbackCandidateRoleTitle := ""
	softFallbackSet := false
	var softFallbackRank matchRank

	anyCandidateTitle := ""
	anyCandidateFunction := ""
	anyCandidateRoleTitle := ""
	anyCandidateSet := false
	var anyCandidateRank matchRank

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
				WHERE LOWER(skill) = ANY(?::text[])
			)`
			args = append(args, sourceSkillValues)
		}

		query += ` ORDER BY p.updated_at DESC, p.id DESC OFFSET ? LIMIT ?`
		args = append(args, offset, similarCategoryScanBatch)
		return query, args
	}

	runPass := func(applySkillFilter bool) error {
		offset := 0
		scannedRows := 0

		for {
			query, args := buildQuery(applySkillFilter, offset)
			rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}

			rowCount := 0
			for rows.Next() {
				rowCount++
				scannedRows++

				var candidateRoleTitle, candidateTitle, candidateFunction, candidateTechStackRaw sql.NullString
				if err := rows.Scan(&candidateRoleTitle, &candidateTitle, &candidateFunction, &candidateTechStackRaw); err != nil {
					rows.Close()
					return err
				}

				roleTokens := tokenizeTextForSequence(candidateRoleTitle.String)
				candidateNormalizedTitle := normalizeRoleTitleForExactMatch(candidateRoleTitle.String)
				titleTokens := tokenizeTextForSequence(candidateTitle.String)
				functionTokens := tokenizeTextForSequence(candidateFunction.String)

				titleTokenSet := map[string]struct{}{}
				for _, token := range titleTokens {
					titleTokenSet[token] = struct{}{}
				}

				functionTokenSet := map[string]struct{}{}
				for _, token := range functionTokens {
					functionTokenSet[token] = struct{}{}
				}

				candidateSkillTokens := tokenizeTechStackForSimilarity(parseStringJSONArray(candidateTechStackRaw.String))
				skillOverlapCount := setIntersectionCount(sourceSkillTokens, candidateSkillTokens)
				skillOverlapRatio := 0.0
				if len(sourceSkillTokens) > 0 {
					skillOverlapRatio = float64(skillOverlapCount) / float64(len(sourceSkillTokens))
				}

				isGenericOneWordCategory := false
				if len(titleTokenSet) == 1 {
					for token := range titleTokenSet {
						if isGenericCategoryToken(token) {
							isGenericOneWordCategory = true
						}
					}
				}

				if !isGenericOneWordCategory &&
					setSubsetOf(titleTokenSet, sourceTokenSet) &&
					(len(functionTokenSet) == 0 || setSubsetOf(functionTokenSet, sourceTokenSet)) {
					directScore := len(titleTokenSet)*4 + len(functionTokenSet)*2 + skillOverlapCount*4 +
						orderedTokenMatchCount(sourceSequenceTokens, titleTokens) +
						orderedTokenMatchCount(sourceSequenceTokens, functionTokens)

					if directScore > directMatchScore {
						directMatchScore = directScore
						directMatchTitle = candidateTitle.String
						directMatchFunction = candidateFunction.String
						directMatchCandidateRoleTitle = candidateRoleTitle.String
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

				categoryOverlapCount := setIntersectionCount(sourceTokens, categoryTokenSet)
				functionOverlapCount := setIntersectionCount(sourceTokens, functionTokenSet)
				roleJaccard := jaccardSimilarity(sourceTokens, candidateTokens)

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

				totalSequenceMatchCount := orderedTokenMatchCount(sourceSequenceTokens, roleTokens)
				if score := orderedTokenMatchCount(sourceSequenceTokens, titleTokens); score > totalSequenceMatchCount {
					totalSequenceMatchCount = score
				}
				if score := orderedTokenMatchCount(sourceSequenceTokens, functionTokens); score > totalSequenceMatchCount {
					totalSequenceMatchCount = score
				}

				nonGenericCategoryPreference := 0
				if sourceHasSpecificTokens && !isGenericOneWordCategory {
					nonGenericCategoryPreference = 1
				}

				exactNormalizedTitleMatch := 0
				if sourceNormalizedTitle != "" && sourceNormalizedTitle == candidateNormalizedTitle {
					exactNormalizedTitleMatch = 1
				}

				genericOneWordCategoryPenalty := 0
				if isGenericOneWordCategory {
					genericOneWordCategoryPenalty = 1
				}

				categorySignalScore := categorySignalWeightFromCatalog(categorySignalCatalog, sourceNormalizedTitle, candidateTitle.String, candidateFunction.String)

				rank := matchRank{
					exactNormalizedTitleMatch:     exactNormalizedTitleMatch,
					categorySignalWeight:          categorySignalScore,
					nonGenericCategoryPreference:  nonGenericCategoryPreference,
					weightedSpecificTokenHits:     weightedSpecificTokenHits,
					weightedCategoryOverlap:       weightedCategoryOverlap,
					weightedRoleOverlap:           weightedRoleOverlap,
					combinedSpecificTokenHits:     combinedSpecificTokenHits,
					combinedCategoryOverlap:       combinedCategoryOverlap,
					categoryOverlapCount:          categoryOverlapCount,
					functionOverlapCount:          functionOverlapCount,
					overlapCount:                  overlapCount,
					overlapRatio:                  overlapRatio,
					totalSequenceMatchCount:       totalSequenceMatchCount,
					skillOverlapCount:             skillOverlapCount,
					skillOverlapRatio:             skillOverlapRatio,
					roleJaccard:                   roleJaccard,
					genericOneWordCategoryPenalty: genericOneWordCategoryPenalty,
				}

				if !anyCandidateSet || rankGreater(rank, anyCandidateRank) {
					anyCandidateSet = true
					anyCandidateRank = rank
					anyCandidateTitle = candidateTitle.String
					anyCandidateFunction = candidateFunction.String
					anyCandidateRoleTitle = candidateRoleTitle.String
				}

				if hasAnySignal(rank) && (!softFallbackSet || rankGreater(rank, softFallbackRank)) {
					softFallbackSet = true
					softFallbackRank = rank
					softFallbackTitle = candidateTitle.String
					softFallbackFunction = candidateFunction.String
					softFallbackCandidateRoleTitle = candidateRoleTitle.String
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
				return err
			}
			rows.Close()

			if rowCount == 0 || scannedRows >= similarCategoryMaxScan {
				break
			}
			offset += rowCount
		}

		return nil
	}

	if len(sourceSkillValues) > 0 {
		if err := runPass(true); err != nil {
			return "", "", err
		}
	}

	if directMatchTitle == "" && (!bestRankSet || !isConfidentMatch(bestRank)) {
		if err := runPass(false); err != nil {
			return "", "", err
		}
	}

	if directMatchTitle != "" {
		log.Printf(
			"similar-category direct_match role_title=%s candidate_role_title=%s",
			roleTitle,
			directMatchCandidateRoleTitle,
		)
		return directMatchTitle, directMatchFunction, nil
	}

	if bestRankSet && isConfidentMatch(bestRank) {
		log.Printf(
			"similar-category best_match role_title=%s candidate_role_title=%s signal=%0.2f overlap=%d skill_overlap=%d",
			roleTitle,
			bestCandidateRoleTitle,
			bestRank.categorySignalWeight,
			bestRank.overlapCount,
			bestRank.skillOverlapCount,
		)
		return bestTitle, bestFunction, nil
	}

	if softFallbackSet {
		log.Printf(
			"similar-category soft_fallback role_title=%s candidate_role_title=%s",
			roleTitle,
			softFallbackCandidateRoleTitle,
		)
		return softFallbackTitle, softFallbackFunction, nil
	}

	if anyCandidateSet {
		log.Printf(
			"similar-category weak_fallback role_title=%s candidate_role_title=%s",
			roleTitle,
			anyCandidateRoleTitle,
		)
		return anyCandidateTitle, anyCandidateFunction, nil
	}

	return "Any", "Any", nil
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
	return stringFromPayload(value)
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

func normalizeTechStack(values any) []string {
	return techstacknorm.Normalize(values)
}

func extractManualTechStackIfNeeded(extractor techstack.Extractor, roleDescription, roleRequirements string, normalizedTechStack []string, extractionEnabled bool, categorizedTitle, categorizedFunction string) []string {
	if len(normalizedTechStack) > 0 || !extractionEnabled || !techstack.IsAllowedInference(categorizedTitle, categorizedFunction) {
		return normalizedTechStack
	}
	return techstacknorm.Normalize(extractor.ExtractDescriptionRequirements(roleDescription, roleRequirements))
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
	normalized := sanitizeUTF8String(regexp.MustCompile(`\s+`).ReplaceAllString(text, " "))
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
		parts[idx] = titleCaseWordSafe(part)
	}
	return strings.Join(parts, " ")
}

func titleCaseWordSafe(value string) string {
	runes := []rune(value)
	if len(runes) == 0 {
		return ""
	}
	return strings.ToUpper(string(runes[0])) + string(runes[1:])
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
		return "[]"
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "[]"
	}
	return sanitizeUTF8String(string(encoded))
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
		return "[]"
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

	workerCount := s.Config.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(pending) {
		workerCount = len(pending)
	}
	if workerCount == 0 {
		log.Printf("parsed-job-worker batch_done rows=0 processed=0 skipped=0")
		return 0, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var processed int64
	var skipped int64
	var firstErr atomic.Value
	var errOnce sync.Once
	reportErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr.Store(err)
			cancel()
		})
	}

	processRow := func(ctx context.Context, row rawRow) (int, int, error) {
		processedInc := 0
		skippedInc := 0
		var retries int
		var retryDelay time.Duration
		payload := map[string]any{}
		if !row.rawJSON.Valid || row.rawJSON.String == "" {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true WHERE id = ?`, row.id); err != nil {
				return processedInc, skippedInc, err
			}
			return 1, 1, nil
		}
		if err := json.Unmarshal([]byte(row.rawJSON.String), &payload); err != nil {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true WHERE id = ?`, row.id); err != nil {
				return processedInc, skippedInc, err
			}
			return 1, 1, nil
		}
		var existingParsedID int64
		err := s.DB.SQL.QueryRowContext(ctx, `SELECT id FROM parsed_jobs WHERE raw_us_job_id = ? LIMIT 1`, row.id).Scan(&existingParsedID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return processedInc, skippedInc, err
		}
		hasExistingParsedForRawID := err == nil && existingParsedID > 0
		log.Printf("parsed-job-worker upsert_start raw_job_id=%d source=%s", row.id, row.source)
		sourceCreatedAt := parseDT(payload["created_at"])
		normalizedTechStack := normalizeTechStack(payload["techStack"])
		plugin, _ := plugins.Get(row.source)

		_, normalizedLocationCity, normalizedUSStates := normalizeLocationFields(
			payload["location"],
			payload["locationCity"],
			payload["locationUSStates"],
		)
		normalizedLocationCountries := normalizeLocationCountries(payload["locationCountries"])
		companyID, companyErr := s.upsertCompanyFromPayload(ctx, payload, plugin)
		if companyErr != nil {
			log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, companyErr)
			return processedInc, skippedInc + 1, nil
		}
		createdAtSourceValue := formatNullableTime(sourceCreatedAt)
		var (
			duplicateID                    int64
			isDuplicate                    bool
			duplicateErr                   error
			isSameSourceURLDuplicate       bool
			sameSourceExternalDuplicateID  int64
			isSameSourceExternalDuplicate  bool
			sameSourceExternalDuplicateErr error
		)
		if !hasExistingParsedForRawID {
			sameSourceExternalDuplicateID, isSameSourceExternalDuplicate, sameSourceExternalDuplicateErr = s.findDuplicateSameSourceParsedJobByExternalJobID(ctx, row.id, row.source, payload)
			if sameSourceExternalDuplicateErr != nil {
				log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, sameSourceExternalDuplicateErr)
				return processedInc, skippedInc + 1, nil
			}
		}
		if !hasExistingParsedForRawID && !isSameSourceExternalDuplicate {
			var duplicateMatch parsedJobURLDuplicateMatch
			duplicateMatch, isDuplicate, duplicateErr = s.findDuplicateParsedJobByURL(ctx, row.id, row.source, payload, companyID)
			if duplicateErr != nil {
				log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, duplicateErr)
				return processedInc, skippedInc + 1, nil
			}
			duplicateID = duplicateMatch.id
			isSameSourceURLDuplicate = duplicateMatch.sameSource
		}

		// isRemoteRocketshipDuplicate := false
		isNonRemoterocketshipDuplicate := false
			if isSameSourceExternalDuplicate {
				isNonRemoterocketshipDuplicate = true
				duplicateID = sameSourceExternalDuplicateID
		}
		if isDuplicate {
			if strings.EqualFold(row.source, sourceRemoteRocketship) {
				// isRemoteRocketshipDuplicate = true
				var legacyRawJobID int64
				var previousCreatedAt sql.NullTime
				if err := s.DB.SQL.QueryRowContext(ctx, `SELECT raw_us_job_id, created_at_source FROM parsed_jobs WHERE id = ? LIMIT 1`, duplicateID).Scan(&legacyRawJobID, &previousCreatedAt); err != nil {
					log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
					return processedInc, skippedInc + 1, nil
				}
				if previousCreatedAt.Valid {
					createdAtSourceValue = formatNullableTime(&previousCreatedAt.Time)
				}
				retries, retryDelay := parsedLockRetryConfig()
				if err := database.RetryLockedWithContext(ctx, retries, retryDelay, func() error {
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
					return processedInc, skippedInc + 1, nil
				}
				if legacyRawJobID > 0 && legacyRawJobID != row.id {
					retries, retryDelay = parsedLockRetryConfig()
					if err := database.RetryLockedWithContext(ctx, retries, retryDelay, func() error {
						_, execErr := s.DB.SQL.ExecContext(
							ctx,
							`UPDATE raw_us_jobs
							 SET is_parsed = true,
							     is_skippable = true,
							     raw_json = NULL
							 WHERE id = ?`,
							legacyRawJobID,
						)
						return execErr
					}); err != nil {
						log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
						return processedInc, skippedInc + 1, nil
					}
				}
				log.Printf("parsed-job-worker duplicate_replaced existing_parsed_id=%d raw_job_id=%d source=%s", duplicateID, row.id, row.source)
			} else {
				isNonRemoterocketshipDuplicate = true
			}
		}

		mergeIntoExistingParsedID := int64(0)
		if isNonRemoterocketshipDuplicate {
			mergeIntoExistingParsedID = duplicateID
		} else if hasExistingParsedForRawID {
			mergeIntoExistingParsedID = existingParsedID
		}

		inferCategories := mergeIntoExistingParsedID == 0 && plugin.InferCategories
		categorizedTitle := stringFromPayload(payload["categorizedJobTitle"])
		categorizedFunction := stringFromPayload(payload["categorizedJobFunction"])
		if inferCategories && categorizedTitle == nil {
			if categorizedTitle == nil {
				inferredTitle, inferredFunction, err := s.findSimilarRemoteRoekctshipCategories(ctx, stringValue(payload["roleTitle"]), normalizedTechStack)
				if err != nil {
					log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
					return processedInc, skippedInc + 1, nil
				}
				categorizedTitle = stringFromPayload(inferredTitle)
				categorizedFunction = stringFromPayload(inferredFunction)
				log.Printf(
					"parsed-job-worker remoterocketship_inferred raw_job_id=%d source=%s role_title=%q category=%q function=%q",
					row.id,
					row.source,
					stringValue(payload["roleTitle"]),
					stringValue(categorizedTitle),
					stringValue(categorizedFunction),
				)
			}
		}
		normalizedTechStack = extractManualTechStackIfNeeded(
			s.techStackExtractor,
			stringValue(payload["roleDescription"]),
			stringValue(payload["roleRequirements"]),
			normalizedTechStack,
			plugin.UseManualTechStackExtraction,
			stringValue(categorizedTitle),
			stringValue(categorizedFunction),
		)
		if plugin.UseManualTechStackExtraction && len(normalizedTechStack) > 0 {
			log.Printf(
				"parsed-job-worker tech_stack_extracted raw_job_id=%d source=%s role_title=%q category=%q function=%q tech_stack_len=%d",
				row.id,
				row.source,
				stringValue(payload["roleTitle"]),
				stringValue(categorizedTitle),
				stringValue(categorizedFunction),
				len(normalizedTechStack),
			)
		}
		normalizedTechStackJSON := jsonStringOrNil(normalizedTechStack)

		if mergeIntoExistingParsedID != 0 {
			retries, retryDelay := parsedLockRetryConfig()
			if err := database.RetryLockedWithContext(ctx, retries, retryDelay, func() error {
				_, execErr := s.DB.SQL.ExecContext(
					ctx,
					`UPDATE parsed_jobs SET
					 valid_until_date = COALESCE(parsed_jobs.valid_until_date, ?),
					 date_deleted = COALESCE(parsed_jobs.date_deleted, ?),
					 description_language = COALESCE(NULLIF(parsed_jobs.description_language, ''), ?),
					 role_description = COALESCE(NULLIF(parsed_jobs.role_description, ''), ?),
					 role_requirements = COALESCE(NULLIF(parsed_jobs.role_requirements, ''), ?),
					 benefits = COALESCE(NULLIF(parsed_jobs.benefits, ''), ?),
					 job_description_summary = COALESCE(NULLIF(parsed_jobs.job_description_summary, ''), ?),
					 two_line_job_description_summary = COALESCE(NULLIF(parsed_jobs.two_line_job_description_summary, ''), ?),
					 role_title_brazil = COALESCE(NULLIF(parsed_jobs.role_title_brazil, ''), ?),
					 role_description_brazil = COALESCE(NULLIF(parsed_jobs.role_description_brazil, ''), ?),
					 role_requirements_brazil = COALESCE(NULLIF(parsed_jobs.role_requirements_brazil, ''), ?),
					 benefits_brazil = COALESCE(NULLIF(parsed_jobs.benefits_brazil, ''), ?),
					 slug_brazil = COALESCE(NULLIF(parsed_jobs.slug_brazil, ''), ?),
					 job_description_summary_brazil = COALESCE(NULLIF(parsed_jobs.job_description_summary_brazil, ''), ?),
					 two_line_job_description_summary_brazil = COALESCE(NULLIF(parsed_jobs.two_line_job_description_summary_brazil, ''), ?),
					 role_title_france = COALESCE(NULLIF(parsed_jobs.role_title_france, ''), ?),
					 role_description_france = COALESCE(NULLIF(parsed_jobs.role_description_france, ''), ?),
					 role_requirements_france = COALESCE(NULLIF(parsed_jobs.role_requirements_france, ''), ?),
					 benefits_france = COALESCE(NULLIF(parsed_jobs.benefits_france, ''), ?),
					 slug_france = COALESCE(NULLIF(parsed_jobs.slug_france, ''), ?),
					 job_description_summary_france = COALESCE(NULLIF(parsed_jobs.job_description_summary_france, ''), ?),
					 two_line_job_description_summary_france = COALESCE(NULLIF(parsed_jobs.two_line_job_description_summary_france, ''), ?),
					 role_title_germany = COALESCE(NULLIF(parsed_jobs.role_title_germany, ''), ?),
					 role_description_germany = COALESCE(NULLIF(parsed_jobs.role_description_germany, ''), ?),
					 role_requirements_germany = COALESCE(NULLIF(parsed_jobs.role_requirements_germany, ''), ?),
					 benefits_germany = COALESCE(NULLIF(parsed_jobs.benefits_germany, ''), ?),
					 slug_germany = COALESCE(NULLIF(parsed_jobs.slug_germany, ''), ?),
					 job_description_summary_germany = COALESCE(NULLIF(parsed_jobs.job_description_summary_germany, ''), ?),
					 two_line_job_description_summary_germany = COALESCE(NULLIF(parsed_jobs.two_line_job_description_summary_germany, ''), ?),
					 employment_type = COALESCE(NULLIF(parsed_jobs.employment_type, ''), ?),
					 location_type = COALESCE(NULLIF(parsed_jobs.location_type, ''), ?),
					 location_city = COALESCE(NULLIF(parsed_jobs.location_city, ''), ?),
					 education_requirements_credential_category = COALESCE(NULLIF(parsed_jobs.education_requirements_credential_category, ''), ?),
					 experience_in_place_of_education = COALESCE(parsed_jobs.experience_in_place_of_education, ?),
					 experience_requirements_months = COALESCE(parsed_jobs.experience_requirements_months, ?),
					 is_on_linkedin = COALESCE(parsed_jobs.is_on_linkedin, ?),
					 is_promoted = COALESCE(parsed_jobs.is_promoted, ?),
					 is_entry_level = COALESCE(parsed_jobs.is_entry_level, ?),
					 is_junior = COALESCE(parsed_jobs.is_junior, ?),
					 is_mid_level = COALESCE(parsed_jobs.is_mid_level, ?),
					 is_senior = COALESCE(parsed_jobs.is_senior, ?),
					 is_lead = COALESCE(parsed_jobs.is_lead, ?),
					 required_languages = COALESCE(NULLIF(parsed_jobs.required_languages::text, '[]'), ?)::json,
					 location_us_states = COALESCE(NULLIF(parsed_jobs.location_us_states::text, '[]'), ?)::jsonb,
					 location_countries = COALESCE(NULLIF(parsed_jobs.location_countries::text, '[]'), ?)::jsonb,
					 tech_stack = COALESCE(NULLIF(parsed_jobs.tech_stack::text, '[]'), ?)::jsonb,
					 salary_min = COALESCE(parsed_jobs.salary_min, ?),
					 salary_max = COALESCE(parsed_jobs.salary_max, ?),
					 salary_type = COALESCE(NULLIF(parsed_jobs.salary_type, ''), ?),
					 salary_currency_code = COALESCE(NULLIF(parsed_jobs.salary_currency_code, ''), ?),
					 salary_currency_symbol = COALESCE(NULLIF(parsed_jobs.salary_currency_symbol, ''), ?),
					 salary_min_usd = COALESCE(parsed_jobs.salary_min_usd, ?),
					 salary_max_usd = COALESCE(parsed_jobs.salary_max_usd, ?),
					 salary_human_text = COALESCE(NULLIF(parsed_jobs.salary_human_text, ''), ?),
					 updated_at = ?
					 WHERE id = ?`,
					formatNullableTime(parseDT(payload["validUntilDate"])),
					formatNullableTime(parseDT(payload["dateDeleted"])),
					stringFromPayload(payload["descriptionLanguage"]),
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
					normalizeEmploymentTypeValue(payload["employmentType"]),
					stringFromPayload(payload["locationType"]),
					normalizedLocationCity,
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
					normalizedJSONArrayText(_normalizeNullStringToNone(payload["requiredLanguages"])),
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
					mergeIntoExistingParsedID,
				)
				return execErr
			}); err != nil {
				log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
				return processedInc, skippedInc + 1, nil
			}
			if isNonRemoterocketshipDuplicate {
				if isSameSourceExternalDuplicate {
					log.Printf("parsed-job-worker duplicate_same_source_external_job_id_merge raw_job_id=%d source=%s duplicate_parsed_job_id=%d", row.id, row.source, duplicateID)
				} else if isSameSourceURLDuplicate {
					log.Printf("parsed-job-worker duplicate_same_source_url_merge raw_job_id=%d source=%s duplicate_parsed_job_id=%d", row.id, row.source, duplicateID)
				} else {
					log.Printf("parsed-job-worker duplicate_cross_source_merge raw_job_id=%d source=%s duplicate_parsed_job_id=%d", row.id, row.source, duplicateID)
				}
				retries, retryDelay = parsedLockRetryConfig()
				if err := database.RetryLockedWithContext(ctx, retries, retryDelay, func() error {
					_, execErr := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true, is_skippable = true, raw_json = NULL WHERE id = ?`, row.id)
					return execErr
				}); err != nil {
					log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
					return processedInc, skippedInc + 1, nil
				}
			} else {
				log.Printf("parsed-job-worker existing_raw_merge raw_job_id=%d source=%s parsed_job_id=%d", row.id, row.source, existingParsedID)
				retries, retryDelay = parsedLockRetryConfig()
				if err := database.RetryLockedWithContext(ctx, retries, retryDelay, func() error {
					_, execErr := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true WHERE id = ?`, row.id)
					return execErr
				}); err != nil {
					log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
					return processedInc, skippedInc + 1, nil
				}
			}
			return processedInc + 1, skippedInc, nil
		}

		retries, retryDelay = parsedLockRetryConfig()
		err = database.RetryLockedWithContext(ctx, retries, retryDelay, func() error {
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
				normalizedJSONArrayText(_normalizeNullStringToNone(payload["requiredLanguages"])),
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
			return processedInc, skippedInc + 1, nil
		}
		retries, retryDelay = parsedLockRetryConfig()
		if err := database.RetryLockedWithContext(ctx, retries, retryDelay, func() error {
			_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = true WHERE id = ?`, row.id)
			return err
		}); err != nil {
			log.Printf("parsed-job-worker row_failed raw_job_id=%d source=%s error=%v", row.id, row.source, err)
			return processedInc, skippedInc + 1, nil
		}
		log.Printf("parsed-job-worker upsert_done raw_job_id=%d source=%s", row.id, row.source)
		return processedInc + 1, skippedInc, nil
	}

	rowCh := make(chan rawRow)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case row, ok := <-rowCh:
					if !ok {
						return
					}
					if err := ctx.Err(); err != nil {
						return
					}
					proc, skip, err := processRow(ctx, row)
					if err != nil {
						reportErr(err)
						return
					}
					if proc > 0 {
						atomic.AddInt64(&processed, int64(proc))
					}
					if skip > 0 {
						atomic.AddInt64(&skipped, int64(skip))
					}
				}
			}
		}()
	}
	for _, row := range pending {
		select {
		case <-ctx.Done():
			close(rowCh)
			wg.Wait()
			if err := firstErr.Load(); err != nil {
				return int(atomic.LoadInt64(&processed)), err.(error)
			}
			return int(atomic.LoadInt64(&processed)), ctx.Err()
		case rowCh <- row:
		}
	}
	close(rowCh)
	wg.Wait()

	if err := firstErr.Load(); err != nil {
		return int(atomic.LoadInt64(&processed)), err.(error)
	}
	log.Printf("parsed-job-worker batch_done rows=%d processed=%d skipped=%d", len(pending), atomic.LoadInt64(&processed), atomic.LoadInt64(&skipped))
	return int(atomic.LoadInt64(&processed)), nil
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
	host := strings.ToLower(parsed.Hostname())
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

func externalCompanyIDToken(value string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.TrimPrefix(normalized, externalCompanyIDPrefix)
	normalized = strings.TrimSuffix(normalized, externalCompanyIDSuffix)
	if normalized == "" {
		return ""
	}
	return externalCompanyIDPrefix + normalized + externalCompanyIDSuffix
}

func appendExternalCompanyIDParts(raw string, seen map[string]struct{}, ordered *[]string) {
	for _, part := range strings.Split(raw, ",") {
		token := externalCompanyIDToken(part)
		if token == "" {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		*ordered = append(*ordered, token)
	}
}

func appendExternalCompanyIDs(existing sql.NullString, incoming string) any {
	ordered := make([]string, 0, 4)
	seen := map[string]struct{}{}
	appendExternalCompanyIDParts(existing.String, seen, &ordered)
	appendExternalCompanyIDParts(incoming, seen, &ordered)

	if len(ordered) == 0 {
		return nil
	}
	return strings.Join(ordered, ",")
}

func (s *Service) findExistingCompanyByMatchKeys(ctx context.Context, companyPayload map[string]any) (sql.NullInt64, error) {
	incomingSlug := stringValue(companyPayload["slug"])
	incomingName := stringValue(companyPayload["name"])
	homePageURL := stringValue(companyPayload["homePageURL"])
	linkedinURL := stringValue(companyPayload["linkedInURL"])
	incomingLinkedinIdentity := linkedinIdentityFromURL(linkedinURL)
	homeDomain := domainFromURL(homePageURL)
	incommingHost := hostFromURL(homePageURL)

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
		incomingLinkedinIdentity, "%"+incomingLinkedinIdentity+"%",
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
		linkedinExactMatch := incomingLinkedinIdentity != "" && incomingLinkedinIdentity == candidateLinkedinIdentity

		candidateDomain := domainFromURL(candidateHomePageURL)
		candidateHost := hostFromURL(candidateHomePageURL)
		overlap := 0

		if incomingName != "" && strings.EqualFold(incomingName, name) {
			overlap++
		}
		if incomingSlug != "" && strings.EqualFold(incomingSlug, slug) {
			overlap++
		}
		if homeDomain != "" && strings.EqualFold(homeDomain, candidateDomain) {
			overlap++
		}
		if incommingHost != "" && strings.EqualFold(incommingHost, candidateHost) {
			overlap++
		}

		// linkedin match always wins
		if linkedinExactMatch {
			if !bestLinkedinMatch || overlap > bestOverlap {
				best = sql.NullInt64{Int64: id, Valid: true}
				bestOverlap = overlap
				bestLinkedinMatch = true
			}
			continue
		}

		if bestLinkedinMatch {
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

func (s *Service) upsertCompanyFromPayload(ctx context.Context, payload map[string]any, plugin plugins.SourcePlugin) (any, error) {
	companyPayload, _ := payload["company"].(map[string]any)
	if len(companyPayload) == 0 {
		return nil, nil
	}

	externalCompanyID := stringValue(_normalizeNullStringToNone(companyPayload["id"]))
	useExternalID := plugin.UseExternalCompanyID
	useMatchKeys := plugin.UseCompanyMatchKeys
	var companyID sql.NullInt64

	if externalCompanyID != "" {
		wrappedExternalCompanyID := externalCompanyIDToken(externalCompanyID)
		err := s.DB.SQL.QueryRowContext(
			ctx,
			`SELECT id
			   FROM parsed_companies
			  WHERE external_company_id ILIKE ?
			  LIMIT 1`,
			"%"+wrappedExternalCompanyID+"%",
		).Scan(&companyID)
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
	careersPageURLVal := strField("careersPageURL")
	employeeRangeVal := strField("employeeRange")
	numberOfEmployeesOnLinkedInVal := _normalizeNullStringToNone(companyPayload["numberOfEmployeesOnLinkedIn"])
	totalFundingAmountVal := _normalizeNullStringToNone(companyPayload["totalFundingAmount"])
	industriesVal := jsonField("industries")
	hqLocationVal := strField("hqLocation")
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
		var curExternalID, curName, curSlug, curTagline, curFoundedYear, curHomePageURL, curLinkedInURL, curCareersPageURL, curEmployeeRange, curHQLocation, curProfilePicURL sql.NullString
		var curNumberOfEmployeesOnLinkedIn, curTotalFundingAmount sql.NullInt64
		var curSponsorsH1B, curSponsorsUK sql.NullBool
		var curTaglineBrazil, curTaglineFrance, curTaglineGermany, curChatGPTDescription, curLinkedInDescription sql.NullString
		var curChatGPTDescriptionBrazil, curChatGPTDescriptionFrance, curChatGPTDescriptionGermany sql.NullString
		var curLinkedInDescriptionBrazil, curLinkedInDescriptionFrance, curLinkedInDescriptionGermany sql.NullString
		var curFundingData, curChatGPTIndustries, curIndustries, curIndustrySpecialities, curIndustrySpecialitiesBrazil, curIndustrySpecialitiesFrance, curIndustrySpecialitiesGermany sql.NullString
		if err := s.DB.SQL.QueryRowContext(
			ctx,
			`SELECT external_company_id, name, slug, tagline, founded_year, home_page_url, linkedin_url, careers_page_url, sponsors_h1b, sponsors_uk_skilled_worker_visa, employee_range, number_of_employees_on_linkedin, total_funding_amount, hq_location, profile_pic_url,
			        tagline_brazil, tagline_france, tagline_germany, chatgpt_description, linkedin_description,
			        chatgpt_description_brazil, chatgpt_description_france, chatgpt_description_germany,
			        linkedin_description_brazil, linkedin_description_france, linkedin_description_germany,
			        funding_data::text, chatgpt_industries::text, industries::text, industry_specialities::text, industry_specialities_brazil::text, industry_specialities_france::text, industry_specialities_germany::text
			   FROM parsed_companies WHERE id = ? LIMIT 1`,
			companyID.Int64,
		).Scan(
			&curExternalID, &curName, &curSlug, &curTagline, &curFoundedYear, &curHomePageURL, &curLinkedInURL, &curCareersPageURL, &curSponsorsH1B, &curSponsorsUK, &curEmployeeRange, &curNumberOfEmployeesOnLinkedIn, &curTotalFundingAmount, &curHQLocation, &curProfilePicURL,
			&curTaglineBrazil, &curTaglineFrance, &curTaglineGermany, &curChatGPTDescription, &curLinkedInDescription,
			&curChatGPTDescriptionBrazil, &curChatGPTDescriptionFrance, &curChatGPTDescriptionGermany,
			&curLinkedInDescriptionBrazil, &curLinkedInDescriptionFrance, &curLinkedInDescriptionGermany,
			&curFundingData, &curChatGPTIndustries, &curIndustries, &curIndustrySpecialities, &curIndustrySpecialitiesBrazil, &curIndustrySpecialitiesFrance, &curIndustrySpecialitiesGermany,
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
		chooseInt := func(current sql.NullInt64, incoming any) any {
			if useExternalID {
				return incoming
			}
			if !current.Valid {
				return incoming
			}
			return current.Int64
		}
		externalCompanyIDUpdate := appendExternalCompanyIDs(curExternalID, externalCompanyIDVal)
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
			        careers_page_url = ?,
			        sponsors_h1b = ?,
			        sponsors_uk_skilled_worker_visa = ?,
			        employee_range = ?,
			        number_of_employees_on_linkedin = ?,
			        total_funding_amount = ?,
			        industries = ?,
			        hq_location = ?,
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
			externalCompanyIDUpdate,
			chooseStr(curName, nameVal),
			chooseStr(curSlug, slugVal),
			chooseStr(curTagline, taglineVal),
			chooseStr(curFoundedYear, foundedYearVal),
			chooseStr(curHomePageURL, homePageURLVal),
			chooseStr(curLinkedInURL, linkedInURLVal),
			chooseStr(curCareersPageURL, careersPageURLVal),
			chooseBool(curSponsorsH1B, sponsorsH1BVal),
			chooseBool(curSponsorsUK, sponsorsUKVal),
			chooseStr(curEmployeeRange, employeeRangeVal),
			chooseInt(curNumberOfEmployeesOnLinkedIn, numberOfEmployeesOnLinkedInVal),
			chooseInt(curTotalFundingAmount, totalFundingAmountVal),
			chooseJSON(curIndustries, industriesVal),
			chooseStr(curHQLocation, hqLocationVal),
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
		    external_company_id, name, slug, tagline, founded_year, home_page_url, linkedin_url, careers_page_url, sponsors_h1b, sponsors_uk_skilled_worker_visa,
		    employee_range, number_of_employees_on_linkedin, total_funding_amount, industries, hq_location, profile_pic_url, tagline_brazil, tagline_france, tagline_germany, chatgpt_description, linkedin_description,
		    chatgpt_description_brazil, chatgpt_description_france, chatgpt_description_germany, linkedin_description_brazil, linkedin_description_france, linkedin_description_germany,
		    funding_data, chatgpt_industries, industry_specialities, industry_specialities_brazil, industry_specialities_france, industry_specialities_germany, updated_at
		  )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 RETURNING id`,
		nilIfEmpty(externalCompanyIDToken(externalCompanyIDVal)),
		nilIfEmpty(nameVal),
		nilIfEmpty(slugVal),
		nilIfEmpty(taglineVal),
		nilIfEmpty(foundedYearVal),
		nilIfEmpty(homePageURLVal),
		nilIfEmpty(linkedInURLVal),
		nilIfEmpty(careersPageURLVal),
		sponsorsH1BVal,
		sponsorsUKVal,
		nilIfEmpty(employeeRangeVal),
		numberOfEmployeesOnLinkedInVal,
		totalFundingAmountVal,
		industriesVal,
		nilIfEmpty(hqLocationVal),
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
	parts := parseJobURLForMatch(rawURL)
	if !parts.valid {
		return ""
	}
	normalized := parts.registrableHost + parts.path
	if len(parts.queryPairs) > 0 {
		normalized += "?" + strings.Join(parts.queryPairs, "&")
	}
	return normalized
}

type jobURLMatchParts struct {
	valid           bool
	registrableHost string
	path            string
	queryPairs      []string
}

func parseJobURLForMatch(rawURL string) jobURLMatchParts {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" || isEmailApplyTarget(trimmed) {
		return jobURLMatchParts{}
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return jobURLMatchParts{}
	}
	host := strings.ToLower(parsed.Hostname())
	host = strings.TrimPrefix(host, "www.")
	if registrable, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil && registrable != "" {
		host = registrable
	}
	path := regexp.MustCompile(`/+`).ReplaceAllString(parsed.EscapedPath(), "/")
	path = strings.TrimRight(path, "/")
	if path == "" {
		path = "/"
	}
	filteredQuery := url.Values{}
	for key, values := range parsed.Query() {
		if shouldIgnoreJobURLQueryParam(key) {
			continue
		}
		filteredQuery[key] = values
	}
	queryPairs := []string{}
	if encoded := filteredQuery.Encode(); encoded != "" {
		queryPairs = strings.Split(strings.ToLower(encoded), "&")
	}
	return jobURLMatchParts{
		valid:           true,
		registrableHost: host,
		path:            strings.ToLower(path),
		queryPairs:      queryPairs,
	}
}

func shouldIgnoreJobURLQueryParam(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	if normalized == "" {
		return false
	}
	if strings.HasPrefix(normalized, "utm_") {
		return true
	}
	switch normalized {
	case "ref", "source", "src", "fbclid", "gclid", "mc_cid", "mc_eid", "msclkid", "dclid", "refId", "trackingId":
		return true
	default:
		return false
	}
}

func isEmailApplyTarget(value string) bool {
	if value == "" {
		return false
	}
	lowered := strings.ToLower(value)
	if strings.HasPrefix(lowered, "mailto:") {
		address := strings.TrimSpace(value[len("mailto:"):])
		if queryIndex := strings.Index(address, "?"); queryIndex >= 0 {
			address = address[:queryIndex]
		}
		parsed, err := mail.ParseAddress(address)
		return err == nil && parsed.Address != ""
	}
	parsed, err := mail.ParseAddress(value)
	return err == nil && parsed.Address != ""
}

func (s *Service) findDuplicateParsedJobByURL(ctx context.Context, rawJobID int64, source string, payload map[string]any, companyID any) (parsedJobURLDuplicateMatch, bool, error) {
	plugin, ok := plugins.Get(strings.TrimSpace(source))
	if ok && !plugin.RunDuplicateCheck {
		return parsedJobURLDuplicateMatch{}, false, nil
	}
	sourceURL := stringValue(payload["url"])
	// sourceCreatedAt := parseDT(payload["created_at"])
	sourceURLNorm := normalizeJobURLForMatch(sourceURL)
	if sourceURLNorm == "" {
		return parsedJobURLDuplicateMatch{}, false, nil
	}
	sourceURLParts := parseJobURLForMatch(sourceURL)
	if !sourceURLParts.valid {
		return parsedJobURLDuplicateMatch{}, false, nil
	}
	sourceURLSignatures := extractDuplicateJobURLSignatures(sourceURL, s.duplicateJobURLRules)

	buildDuplicateSignaturePrefilter := func(signature duplicateJobURLSignature, ignoreCompanyID bool) (string, []any) {
		query := `SELECT p.id, r.source, p.url
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE p.url IS NOT NULL
		    AND p.raw_us_job_id <> ?`
		args := []any{
			rawJobID,
		}
		for _, term := range signature.prefilterTerms {
			if strings.TrimSpace(term) == "" {
				continue
			}
			query += ` AND LOWER(p.url) LIKE ?`
			args = append(args, "%"+strings.ToLower(term)+"%")
		}
		if !ignoreCompanyID {
			query += `
			AND (?::bigint IS NULL OR p.company_id = ?::bigint)`
			companyIDInt, companyIDOK := companyID.(int64)
			var companyIDFilter any
			if companyIDOK {
				companyIDFilter = companyIDInt
			}
			args = append(args, companyIDFilter, companyIDFilter)
		}
		query += ` ORDER BY p.updated_at DESC, p.id DESC`
		if ignoreCompanyID {
			query += ` LIMIT 200`
		} else {
			query += ` LIMIT 500`
		}
		return query, args
	}

	findDuplicateByURLSignatures := func(ignoreCompanyID bool) (parsedJobURLDuplicateMatch, bool, error) {
		for _, signature := range sourceURLSignatures {
			query, args := buildDuplicateSignaturePrefilter(signature, ignoreCompanyID)
			rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
			if err != nil {
				return parsedJobURLDuplicateMatch{}, false, err
			}
			for rows.Next() {
				var duplicateID int64
				var candidateSource sql.NullString
				var candidateURL sql.NullString
				if scanErr := rows.Scan(&duplicateID, &candidateSource, &candidateURL); scanErr != nil {
					rows.Close()
					return parsedJobURLDuplicateMatch{}, false, scanErr
				}
				candidateSignatures := extractDuplicateJobURLSignatures(candidateURL.String, s.duplicateJobURLRules)
				for _, candidateSignature := range candidateSignatures {
					if candidateSignature.key == signature.key {
						rows.Close()
						return parsedJobURLDuplicateMatch{
							id:         duplicateID,
							sameSource: strings.EqualFold(strings.TrimSpace(candidateSource.String), source),
						}, true, nil
					}
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return parsedJobURLDuplicateMatch{}, false, err
			}
			rows.Close()
		}
		return parsedJobURLDuplicateMatch{}, false, nil
	}

	if len(sourceURLSignatures) > 0 {
		if duplicateMatch, isDuplicate, err := findDuplicateByURLSignatures(true); err != nil {
			return parsedJobURLDuplicateMatch{}, false, err
		} else if isDuplicate {
			return duplicateMatch, true, nil
		}
		if duplicateMatch, isDuplicate, err := findDuplicateByURLSignatures(false); err != nil {
			return parsedJobURLDuplicateMatch{}, false, err
		} else if isDuplicate {
			return duplicateMatch, true, nil
		}
	}

	buildDuplicateURLPrefilter := func(ignoreCompanyID bool) (string, []any) {
		query := `SELECT p.id, r.source, p.url
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE p.url IS NOT NULL
		    AND LOWER(p.url) LIKE ?
		    AND LOWER(p.url) LIKE ?
		    AND p.raw_us_job_id <> ?`
		args := []any{
			"%" + sourceURLParts.registrableHost + "%",
			"%" + sourceURLParts.path + "%",
			rawJobID,
		}
		for _, pair := range sourceURLParts.queryPairs {
			query += ` AND LOWER(p.url) LIKE ?`
			args = append(args, "%"+pair+"%")
		}
		if !ignoreCompanyID {
			query += `
			AND (?::bigint IS NULL OR p.company_id = ?::bigint)`
			companyIDInt, companyIDOK := companyID.(int64)
			var companyIDFilter any
			if companyIDOK {
				companyIDFilter = companyIDInt
			}
			args = append(args, companyIDFilter, companyIDFilter)
		}
		query += ` ORDER BY p.updated_at DESC, p.id DESC`
		if ignoreCompanyID {
			query += ` LIMIT 200`
		} else {
			query += ` LIMIT 1000`
		}
		return query, args
	}

	// First pass: ignore company_id and match by normalized URL only.
	{
		query, args := buildDuplicateURLPrefilter(true)
		rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
		if err != nil {
			return parsedJobURLDuplicateMatch{}, false, err
		}
		for rows.Next() {
			var duplicateID int64
			var candidateSource sql.NullString
			var candidateURL sql.NullString
			if scanErr := rows.Scan(&duplicateID, &candidateSource, &candidateURL); scanErr != nil {
				rows.Close()
				return parsedJobURLDuplicateMatch{}, false, scanErr
			}
			if normalizeJobURLForMatch(candidateURL.String) == sourceURLNorm {
				rows.Close()
				return parsedJobURLDuplicateMatch{
					id:         duplicateID,
					sameSource: strings.EqualFold(strings.TrimSpace(candidateSource.String), source),
				}, true, nil
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return parsedJobURLDuplicateMatch{}, false, err
		}
		rows.Close()
	}

	// var lowerBound any
	// var upperBound any
	// if sourceCreatedAt != nil {
	// 	lowerBound = sourceCreatedAt.UTC().Add(-maxDuplicatePostDateDiff)
	// 	upperBound = sourceCreatedAt.UTC().Add(maxDuplicatePostDateDiff)
	// }
	query, args := buildDuplicateURLPrefilter(false)

	// AND (
	// 	?::timestamptz IS NULL
	// 	OR (
	// 		p.created_at_source IS NOT NULL
	// 		AND p.created_at_source >= ?::timestamptz
	// 		AND p.created_at_source <= ?::timestamptz
	// 	)
	// )
	rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return parsedJobURLDuplicateMatch{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var duplicateID int64
		var candidateSource sql.NullString
		var candidateURL sql.NullString
		if scanErr := rows.Scan(&duplicateID, &candidateSource, &candidateURL); scanErr != nil {
			return parsedJobURLDuplicateMatch{}, false, scanErr
		}
		if normalizeJobURLForMatch(candidateURL.String) == sourceURLNorm {
			return parsedJobURLDuplicateMatch{
				id:         duplicateID,
				sameSource: strings.EqualFold(strings.TrimSpace(candidateSource.String), source),
			}, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return parsedJobURLDuplicateMatch{}, false, err
	}
	return parsedJobURLDuplicateMatch{}, false, nil
}

func (s *Service) findDuplicateCrossSourceParsedJob(ctx context.Context, rawJobID int64, source string, payload map[string]any, companyID any) (int64, bool, error) {
	duplicateMatch, isDuplicate, err := s.findDuplicateParsedJobByURL(ctx, rawJobID, source, payload, companyID)
	if err != nil || !isDuplicate {
		return 0, false, err
	}
	if duplicateMatch.sameSource {
		return 0, false, nil
	}
	return duplicateMatch.id, true, nil
}

func (s *Service) findDuplicateSameSourceParsedJobByExternalJobID(ctx context.Context, rawJobID int64, source string, payload map[string]any) (int64, bool, error) {
	externalJobID := strings.TrimSpace(stringValue(payload["id"]))
	if externalJobID == "" {
		return 0, false, nil
	}
	var duplicateID int64
	err := s.DB.SQL.QueryRowContext(
		ctx,
		`SELECT p.id
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE r.source = ?
		    AND p.raw_us_job_id <> ?
		    AND COALESCE(p.external_job_id, '') = ?
		  ORDER BY p.updated_at DESC, p.id DESC
		  LIMIT 1`,
		source,
		rawJobID,
		externalJobID,
	).Scan(&duplicateID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return duplicateID, duplicateID > 0, nil
}

func normalizedJSONText(value any) any {
	if value == nil {
		return "[]"
	}
	switch item := value.(type) {
	case []any:
		if len(item) == 0 {
			return "[]"
		}
		body, err := json.Marshal(item)
		if err != nil {
			return "[]"
		}
		return sanitizeUTF8String(string(body))
	case []string:
		if len(item) == 0 {
			return "[]"
		}
		body, err := json.Marshal(item)
		if err != nil {
			return "[]"
		}
		return sanitizeUTF8String(string(body))
	case map[string]any:
		if len(item) == 0 {
			return "{}"
		}
		body, err := json.Marshal(item)
		if err != nil {
			return "{}"
		}
		return sanitizeUTF8String(string(body))
	default:
		return nil
	}
}

func normalizedJSONArrayText(value any) any {
	if value == nil {
		return "[]"
	}
	switch item := value.(type) {
	case []any:
		if len(item) == 0 {
			return "[]"
		}
		body, err := json.Marshal(item)
		if err != nil {
			return "[]"
		}
		return sanitizeUTF8String(string(body))
	case []string:
		if len(item) == 0 {
			return "[]"
		}
		body, err := json.Marshal(item)
		if err != nil {
			return "[]"
		}
		return sanitizeUTF8String(string(body))
	case string:
		trimmed := sanitizeUTF8String(item)
		if trimmed == "" || strings.EqualFold(trimmed, "null") {
			return "[]"
		}
		if strings.HasPrefix(trimmed, "[") {
			return trimmed
		}
	}
	return "[]"
}

func formatNullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func _normalizeNullStringToNone(value any) any {
	if text, ok := value.(string); ok {
		trimmed := sanitizeUTF8String(text)
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
		normalized := sanitizeUTF8String(item)
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
	return sanitizeUTF8String(text)
}

// sanitizeUTF8String ensures payload-derived text is safe to bind into Postgres
// text columns even when upstream source content contains invalid byte sequences.
func sanitizeUTF8String(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if utf8.ValidString(trimmed) {
		return trimmed
	}
	return strings.ToValidUTF8(trimmed, "")
}
