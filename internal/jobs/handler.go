package jobs

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/normalize/locationnorm"
	"goapplyjob-golang-backend/internal/parsedaiclassifier"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gensqlc "goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct {
	cfg                       config.Config
	db                        *database.DB
	auth                      *auth.Handler
	q                         *gensqlc.Queries
	filterCache               filterOptionsCache
	filterCacheRefreshSeconds int
}

type filterOptionsCache struct {
	mu                 sync.Mutex
	maxParsedJobID     sql.NullInt64
	jobCategoryParents map[string]any
	locationParents    map[string][]string
	techStacks         []string
	locationTypes      []string
	employmentTypes    []string
	lastRefreshAt      time.Time
	refreshRunning     bool
}

const (
	defaultSalaryType                = "yearly"
	minSalaryStart                   = 30000
	minSalaryEnd                     = 300000
	minSalaryStep                    = 10000
	hoursPerYear                     = 2080.0
	daysPerYear                      = 260.0
	weeksPerYear                     = 52.0
	biweeksPerYear                   = 26.0
	monthsPerYear                    = 12.0
	minutesPerYear                   = hoursPerYear * 60.0
	defaultFilterCacheRefreshSeconds = 300
	unitedStatesCountry              = "United States"
)

var (
	postDateOptions = []string{
		"today",
		"24_hours",
		"yesterday",
		"48_hours",
		"72_hours",
		"this_week",
		"week",
		"previous_week",
		"this_month",
		"month",
		"previous_month",
		"3_months",
	}
	postDateWindows = map[string]time.Duration{
		"24_hours": 24 * time.Hour,
		"48_hours": 48 * time.Hour,
		"72_hours": 72 * time.Hour,
		"3_days":   72 * time.Hour,
		"week":     7 * 24 * time.Hour,
		"month":    30 * 24 * time.Hour,
		"3_months": 90 * 24 * time.Hour,
	}
	validSeniorities = map[string]struct{}{
		"entry":  {},
		"junior": {},
		"mid":    {},
		"senior": {},
		"lead":   {},
	}
	nonAlphaTitleChars = regexp.MustCompile(`[^A-Za-z\s]+`)
)

type jobItem struct {
	ID                    int64      `json:"id"`
	RawUSJobID            int64      `json:"raw_us_job_id"`
	RoleTitle             *string    `json:"role_title"`
	JobDescriptionSummary *string    `json:"job_description_summary"`
	CompanyName           *string    `json:"company_name"`
	CompanySlug           *string    `json:"company_slug"`
	CompanyTagline        *string    `json:"company_tagline"`
	CompanyProfilePicURL  *string    `json:"company_profile_pic_url"`
	CompanyHomePageURL    *string    `json:"company_home_page_url"`
	CompanyLinkedInURL    *string    `json:"company_linkedin_url"`
	CompanyEmployeeRange  *string    `json:"company_employee_range"`
	CompanyFoundedYear    *string    `json:"company_founded_year"`
	CompanySponsorsH1B    *bool      `json:"company_sponsors_h1b"`
	CategorizedTitle      *string    `json:"categorized_job_title"`
	CategorizedFunction   *string    `json:"categorized_job_function"`
	LocationCity          *string    `json:"location_city"`
	LocationType          *string    `json:"location_type"`
	LocationUSStates      []string   `json:"location_us_states"`
	LocationCountries     []string   `json:"location_countries"`
	EmploymentType        *string    `json:"employment_type"`
	SalaryMin             *float64   `json:"salary_min"`
	SalaryMax             *float64   `json:"salary_max"`
	SalaryMinUSD          *float64   `json:"salary_min_usd"`
	SalaryMaxUSD          *float64   `json:"salary_max_usd"`
	SalaryCurrencyCode    *string    `json:"salary_currency_code"`
	SalaryCurrencySymbol  *string    `json:"salary_currency_symbol"`
	SalaryType            *string    `json:"salary_type"`
	IsEntryLevel          *bool      `json:"is_entry_level"`
	IsJunior              *bool      `json:"is_junior"`
	IsMidLevel            *bool      `json:"is_mid_level"`
	IsSenior              *bool      `json:"is_senior"`
	IsLead                *bool      `json:"is_lead"`
	TechStack             []string   `json:"tech_stack"`
	UpdatedAt             *time.Time `json:"updated_at"`
	CreatedAtSource       *time.Time `json:"created_at_source"`
	URL                   *string    `json:"url"`
}

type jobDetail struct {
	ID                                      int64    `json:"id"`
	RawUSJobID                              int64    `json:"raw_us_job_id"`
	CompanyName                             *string  `json:"company_name"`
	CompanySlug                             *string  `json:"company_slug"`
	CompanyTagline                          *string  `json:"company_tagline"`
	CompanyProfilePicURL                    *string  `json:"company_profile_pic_url"`
	CompanyHomePageURL                      *string  `json:"company_home_page_url"`
	CompanyLinkedInURL                      *string  `json:"company_linkedin_url"`
	CompanyEmployeeRange                    *string  `json:"company_employee_range"`
	CompanyFoundedYear                      *string  `json:"company_founded_year"`
	CompanySponsorsH1B                      *bool    `json:"company_sponsors_h1b"`
	CategorizedTitle                        *string  `json:"categorized_job_title"`
	CategorizedFunction                     *string  `json:"categorized_job_function"`
	RoleTitle                               *string  `json:"role_title"`
	LocationCity                            *string  `json:"location_city"`
	LocationType                            *string  `json:"location_type"`
	LocationUSStates                        []string `json:"location_us_states"`
	LocationCountries                       []string `json:"location_countries"`
	EmploymentType                          *string  `json:"employment_type"`
	SalaryMin                               *float64 `json:"salary_min"`
	SalaryMax                               *float64 `json:"salary_max"`
	SalaryMinUSD                            *float64 `json:"salary_min_usd"`
	SalaryMaxUSD                            *float64 `json:"salary_max_usd"`
	SalaryCurrencyCode                      *string  `json:"salary_currency_code"`
	SalaryCurrencySymbol                    *string  `json:"salary_currency_symbol"`
	SalaryType                              *string  `json:"salary_type"`
	IsEntryLevel                            *bool    `json:"is_entry_level"`
	IsJunior                                *bool    `json:"is_junior"`
	IsMidLevel                              *bool    `json:"is_mid_level"`
	IsSenior                                *bool    `json:"is_senior"`
	IsLead                                  *bool    `json:"is_lead"`
	UpdatedAt                               *string  `json:"updated_at"`
	CreatedAtSource                         *string  `json:"created_at_source"`
	RoleDescription                         *string  `json:"role_description"`
	RoleRequirements                        *string  `json:"role_requirements"`
	EducationRequirementsCredentialCategory *string  `json:"education_requirements_credential_category"`
	ExperienceRequirementsMonths            *int     `json:"experience_requirements_months"`
	ExperienceInPlaceOfEducation            *bool    `json:"experience_in_place_of_education"`
	RequiredLanguages                       []string `json:"required_languages"`
	TechStack                               []string `json:"tech_stack"`
	Benefits                                *string  `json:"benefits"`
	URL                                     *string  `json:"url"`
}

type jobSitemapItem struct {
	ID               int64   `json:"id"`
	RoleTitle        *string `json:"role_title"`
	CategorizedTitle *string `json:"categorized_job_title"`
	CompanyName      *string `json:"company_name"`
	CreatedAtSource  *string `json:"created_at_source"`
}

func NewHandler(cfg config.Config, db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{
		cfg:                       cfg,
		db:                        db,
		auth:                      authHandler,
		q:                         gensqlc.New(db.PGX),
		filterCacheRefreshSeconds: max(config.GetenvInt("FILTER_OPTIONS_CACHE_REFRESH_SECONDS", defaultFilterCacheRefreshSeconds), 1),
		filterCache: filterOptionsCache{
			jobCategoryParents: map[string]any{},
			locationParents:    map[string][]string{},
			techStacks:         []string{},
			employmentTypes:    []string{},
		},
	}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/jobs/filter-options", h.filterOptions)
	router.GET("/jobs/metrics", h.metrics)
	router.GET("/jobs/related-categories", h.relatedCategories)
	router.GET("/jobs/top-categories", h.topCategories)
	router.GET("/jobs/top-functions", h.topFunctions)
	router.GET("/jobs/sitemap", h.sitemap)
	router.GET("/jobs/count", h.jobsCount)
	router.GET("/job/:jobID", h.jobDetail)
	router.GET("/jobs/:jobID", h.jobDetail)
	router.GET("/jobs", h.listJobs)
}

func annualizedSalarySQL(expr string) string {
	return fmt.Sprintf(`(%[1]s) * CASE
		WHEN lower(trim(COALESCE(salary_type, '%[2]s'))) IN ('yearly', 'year', 'annual')
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%year%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%annual%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%annually%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%per year%%'
		THEN %[3]f
		WHEN lower(trim(COALESCE(salary_type, '%[2]s'))) IN ('monthly', 'month')
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%month%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%/mo%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%% mo%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%per month%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%month-salary%%'
		THEN %[4]f
		WHEN lower(trim(COALESCE(salary_type, '%[2]s'))) IN ('biweekly', 'bi-weekly')
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%biweekly%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%bi-weekly%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%per biweekly%%'
		THEN %[5]f
		WHEN lower(trim(COALESCE(salary_type, '%[2]s'))) IN ('weekly', 'week')
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%week%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%/wk%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%% wk%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%per week%%'
		THEN %[6]f
		WHEN lower(trim(COALESCE(salary_type, '%[2]s'))) IN ('daily', 'day')
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%day%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%/day%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%per day%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%per visit%%'
		THEN %[7]f
		WHEN lower(trim(COALESCE(salary_type, '%[2]s'))) IN ('hourly', 'hour', 'hr')
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%hour%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%/hr%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%% hr%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%per hour%%'
		THEN %[8]f
		WHEN lower(trim(COALESCE(salary_type, '%[2]s'))) IN ('minute', 'min')
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%minute%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%/min%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%% min%%'
			OR lower(trim(COALESCE(salary_type, '%[2]s'))) LIKE '%%per minute%%'
		THEN %[9]f
		ELSE 1.0
	END`, expr, defaultSalaryType, 1.0, monthsPerYear, biweeksPerYear, weeksPerYear, daysPerYear, hoursPerYear, minutesPerYear)
}

func parseCSVQuery(value string) []string {
	if value == "" {
		return nil
	}
	reader := csv.NewReader(strings.NewReader(value))
	record, err := reader.Read()
	if err != nil {
		return nil
	}
	items := make([]string, 0, len(record))
	for _, item := range record {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

func tokenizeTitleSearchText(value string) []string {
	normalized := strings.ToLower(nonAlphaTitleChars.ReplaceAllString(value, " "))
	return strings.Fields(normalized)
}

func expandLocationQueryTerms(values []string) []string {
	expanded := []string{}
	seen := map[string]struct{}{}
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		candidates := []string{value}
		if strings.Contains(value, ",") {
			parts := []string{}
			for _, part := range strings.Split(value, ",") {
				trimmed := strings.TrimSpace(part)
				if trimmed != "" {
					parts = append(parts, trimmed)
				}
			}
			if len(parts) > 0 {
				// Keep "State, United States" narrowed to the state token only.
				candidates = append(candidates, parts[0])
			}
		}
		for _, candidate := range candidates {
			key := strings.ToLower(candidate)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			expanded = append(expanded, candidate)
		}
	}
	return expanded
}

func expandCountryFilterTerms(values []string) []string {
	expanded := []string{}
	seen := map[string]struct{}{}
	addValue := func(value string) {
		cleaned := strings.TrimSpace(value)
		if cleaned == "" {
			return
		}
		key := strings.ToLower(cleaned)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		expanded = append(expanded, cleaned)
	}
	for _, raw := range values {
		term := strings.TrimSpace(raw)
		if term == "" {
			continue
		}
		addValue(term)
		if region := locationnorm.NormalizeRegionName(term); region != "" {
			for _, parentRegion := range locationnorm.RegionParentNames(region) {
				addValue(parentRegion)
			}
			continue
		}
		for _, region := range locationnorm.RegionNamesForCountry(term) {
			addValue(region)
		}
	}
	return expanded
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func broadRegionTermsForUSStates(states []string) []string {
	if len(states) == 0 {
		return nil
	}
	terms := []string{}
	for _, region := range locationnorm.RegionNamesForCountry(unitedStatesCountry) {
		if region == unitedStatesCountry || containsString(terms, region) {
			continue
		}
		terms = append(terms, region)
	}
	return terms
}

func resolvePostDateTimezone(name string, offsetRaw string) *time.Location {
	trimmedName := strings.TrimSpace(name)
	if trimmedName != "" {
		if loc, err := time.LoadLocation(trimmedName); err == nil {
			return loc
		}
	}
	trimmedOffset := strings.TrimSpace(offsetRaw)
	if trimmedOffset != "" {
		if offsetMinutes, err := strconv.Atoi(trimmedOffset); err == nil && offsetMinutes >= -840 && offsetMinutes <= 840 {
			// JS getTimezoneOffset uses opposite sign: UTC = local + offset.
			return time.FixedZone("client-offset", -offsetMinutes*60)
		}
	}
	return time.UTC
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func annualizedMinSalarySQL() string {
	return `COALESCE(` + annualizedSalarySQL(`salary_min_usd`) + `, ` + annualizedSalarySQL(`salary_min`) + `)`
}

func annualizedMaxSalarySQL() string {
	return `COALESCE(` + annualizedSalarySQL(`salary_max_usd`) + `, ` + annualizedSalarySQL(`salary_max`) + `, ` + annualizedSalarySQL(`salary_min_usd`) + `, ` + annualizedSalarySQL(`salary_min`) + `)`
}

func minSalaryFilterSQL() string {
	return `(` + annualizedSalarySQL(`p.salary_min_usd`) + ` >= ? OR (p.salary_min_usd IS NULL AND ` + annualizedSalarySQL(`p.salary_min`) + ` >= ?))`
}

func cleanFilterLabel(value string) string {
	return strings.TrimSpace(value)
}

func sortedKeysCaseInsensitive[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := strings.ToLower(keys[i])
		right := strings.ToLower(keys[j])
		if left == right {
			return keys[i] < keys[j]
		}
		return left < right
	})
	return keys
}

func buildJobCategoryParentsMap(rows [][2]string) map[string]any {
	categoryFunctionCounts := map[string]map[string]int{}
	rootFunctionLabels := map[string]struct{}{}
	allLabels := map[string]struct{}{}
	for _, row := range rows {
		title := cleanFilterLabel(row[0])
		function := cleanFilterLabel(row[1])
		if function != "" {
			rootFunctionLabels[function] = struct{}{}
			allLabels[function] = struct{}{}
		}
		if title != "" {
			allLabels[title] = struct{}{}
			if function != "" && title != function {
				if _, ok := categoryFunctionCounts[title]; !ok {
					categoryFunctionCounts[title] = map[string]int{}
				}
				categoryFunctionCounts[title][function]++
			}
		}
	}
	resolved := map[string]any{}
	sortedLabels := sortedKeysCaseInsensitive(allLabels)
	for _, label := range sortedLabels {
		if _, isRoot := rootFunctionLabels[label]; isRoot {
			resolved[label] = nil
			continue
		}
		counts := categoryFunctionCounts[label]
		if len(counts) == 0 {
			resolved[label] = nil
			continue
		}
		type countItem struct {
			parent string
			count  int
		}
		items := make([]countItem, 0, len(counts))
		for parent, count := range counts {
			items = append(items, countItem{parent: parent, count: count})
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].count == items[j].count {
				return strings.ToLower(items[i].parent) < strings.ToLower(items[j].parent)
			}
			return items[i].count > items[j].count
		})
		resolved[label] = items[0].parent
	}
	return resolved
}

func buildLocationParentsMap(rows [][2][]string) map[string][]string {
	locationParents := map[string][]string{}
	seenLabels := map[string]struct{}{}
	stateToCountries := map[string]map[string]struct{}{}
	addLocationOption := func(label string, parents []string) {
		cleanLabel := cleanFilterLabel(label)
		if cleanLabel == "" {
			return
		}
		if _, ok := seenLabels[cleanLabel]; ok {
			return
		}
		seenLabels[cleanLabel] = struct{}{}
		cleanParents := []string{}
		seenParents := map[string]struct{}{}
		for _, parent := range parents {
			cleanParent := cleanFilterLabel(parent)
			if cleanParent == "" {
				continue
			}
			if _, exists := seenParents[cleanParent]; exists {
				continue
			}
			seenParents[cleanParent] = struct{}{}
			cleanParents = append(cleanParents, cleanParent)
		}
		sort.Slice(cleanParents, func(i, j int) bool { return strings.ToLower(cleanParents[i]) < strings.ToLower(cleanParents[j]) })
		locationParents[cleanLabel] = cleanParents
	}
	for _, row := range rows {
		states := uniqueStrings(row[0])
		countries := uniqueStrings(row[1])
		for _, state := range states {
			state = locationnorm.NormalizeUSStateName(state)
			state = cleanFilterLabel(state)
			if !isValidLocationOption(state) {
				continue
			}
			if _, ok := stateToCountries[state]; !ok {
				stateToCountries[state] = map[string]struct{}{}
			}
			for _, country := range countries {
				country = locationnorm.NormalizeCountryName(country)
				country = cleanFilterLabel(country)
				if isValidLocationOption(country) {
					stateToCountries[state][country] = struct{}{}
				}
			}
			parents := make([]string, 0, len(stateToCountries[state]))
			for country := range stateToCountries[state] {
				parents = append(parents, country)
			}
			addLocationOption(state, parents)
		}
		for _, country := range countries {
			country = locationnorm.NormalizeCountryName(country)
			country = cleanFilterLabel(country)
			if isValidLocationOption(country) {
				addLocationOption(country, nil)
			}
		}
	}
	sorted := map[string][]string{}
	for _, key := range sortedKeysCaseInsensitive(locationParents) {
		sorted[key] = locationParents[key]
	}
	return sorted
}

func buildTechStacks(rows [][]string) []string {
	byKey := map[string]string{}
	for _, stack := range rows {
		for _, item := range stack {
			cleaned := strings.TrimSpace(item)
			if cleaned == "" {
				continue
			}
			key := strings.ToLower(cleaned)
			if _, ok := byKey[key]; !ok {
				byKey[key] = cleaned
			}
		}
	}
	values := make([]string, 0, len(byKey))
	for _, value := range byKey {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return strings.ToLower(values[i]) < strings.ToLower(values[j]) })
	return values
}

func buildEmploymentTypes(rows []string) []string {
	seen := map[string]struct{}{}
	values := []string{}
	for _, row := range rows {
		cleaned := strings.TrimSpace(row)
		if cleaned == "" {
			continue
		}
		if _, ok := seen[cleaned]; ok {
			continue
		}
		seen[cleaned] = struct{}{}
		values = append(values, cleaned)
	}
	sort.Slice(values, func(i, j int) bool { return strings.ToLower(values[i]) < strings.ToLower(values[j]) })
	return values
}

func buildLocationTypes(rows []string) []string {
	sort.Slice(rows, func(i, j int) bool { return strings.ToLower(rows[i]) < strings.ToLower(rows[j]) })
	return rows
}

func parseJSONTextArray(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil
	}
	return values
}

func (h *Handler) listDistinctLocationCountryRows(ctx context.Context) ([][2][]string, error) {
	rows, err := h.db.SQL.QueryContext(ctx, `
		SELECT DISTINCT
			COALESCE(location_countries::text, '')
		FROM parsed_jobs
		WHERE location_countries IS NOT NULL
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	locationRows := [][2][]string{}
	for rows.Next() {
		var rawCountries string
		if err := rows.Scan(&rawCountries); err != nil {
			return nil, err
		}
		locationRows = append(locationRows, [2][]string{
			nil,
			parseJSONTextArray(rawCountries),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return locationRows, nil
}

func (h *Handler) getMaxParsedJobID(ctx context.Context) (sql.NullInt64, error) {
	maxID, err := h.q.GetMaxParsedJobID(ctx)
	if err != nil {
		return sql.NullInt64{}, err
	}
	if maxID <= 0 {
		return sql.NullInt64{}, nil
	}
	return sql.NullInt64{Int64: maxID, Valid: true}, nil
}

func parsedJobFilterRowsEqual(left, right sql.NullInt64) bool {
	if !left.Valid && !right.Valid {
		return true
	}
	return left.Valid == right.Valid && left.Int64 == right.Int64
}

func (h *Handler) refreshFilterCache(ctx context.Context) error {
	rows, err := h.q.ListDistinctJobCategoryFunctionPairs(ctx)
	if err != nil {
		return err
	}

	categoryRows := [][2]string{}
	categoryTitles := []string{}
	categoryFunctions := map[string]string{}
	techRows := [][]string{}
	for _, row := range rows {
		if row == nil {
			continue
		}
		if row.CategorizedJobTitle.Valid || row.CategorizedJobFunction.Valid {
			categoryRows = append(categoryRows, [2]string{row.CategorizedJobTitle.String, row.CategorizedJobFunction.String})
			categoryTitles = append(categoryTitles, row.CategorizedJobTitle.String)
			categoryFunctions[row.CategorizedJobTitle.String] = row.CategorizedJobFunction.String
		}
	}

	techRowsRaw, err := h.q.ListDistinctTechStackTexts(ctx)
	if err != nil {
		return err
	}
	for _, techStack := range techRowsRaw {
		if len(techStack) == 0 {
			continue
		}
		values := []string{}
		if err := json.Unmarshal(techStack, &values); err == nil {
			techRows = append(techRows, values)
		}
	}
	employmentTypeRowsRaw, err := h.q.ListDistinctEmploymentTypes(ctx)
	if err != nil {
		return err
	}
	locationCountryRows, err := h.listDistinctLocationCountryRows(ctx)
	if err != nil {
		return err
	}
	locationTypeRowsRaw, err := h.q.ListDistinctLocationTypes(ctx)
	if err != nil {
		return err
	}
	locationRows := []string{}
	for _, locationType := range locationTypeRowsRaw {
		locationRows = append(locationRows, locationType.String)
	}
	employmentRows := []string{}
	for _, employmentType := range employmentTypeRowsRaw {
		employmentRows = append(employmentRows, employmentType.String)
	}

	h.filterCache.jobCategoryParents = buildJobCategoryParentsMap(categoryRows)
	parsedaiclassifier.SetCachedGroqCategorizedJobTitles(categoryTitles, categoryFunctions)
	locationParents := buildLocationParentsMap(locationCountryRows)
	for _, state := range locationnorm.USStateNames() {
		locationParents[state] = []string{unitedStatesCountry}
	}
	if _, ok := locationParents[unitedStatesCountry]; !ok {
		locationParents[unitedStatesCountry] = []string{}
	}
	h.filterCache.locationParents = locationParents
	h.filterCache.techStacks = buildTechStacks(techRows)
	h.filterCache.locationTypes = buildLocationTypes(locationRows)
	h.filterCache.employmentTypes = buildEmploymentTypes(employmentRows)
	return nil
}

func (h *Handler) ensureFilterCacheFresh(ctx context.Context, force bool) error {
	h.filterCache.mu.Lock()
	defer h.filterCache.mu.Unlock()
	maxID, err := h.getMaxParsedJobID(ctx)
	if err != nil {
		return err
	}
	if !force && parsedJobFilterRowsEqual(h.filterCache.maxParsedJobID, maxID) {
		if !h.filterCache.lastRefreshAt.IsZero() &&
			time.Since(h.filterCache.lastRefreshAt) < time.Duration(h.filterCacheRefreshSeconds)*time.Second {
			return nil
		}
		return nil
	}
	if err := h.refreshFilterCache(ctx); err != nil {
		return err
	}
	h.filterCache.maxParsedJobID = maxID
	h.filterCache.lastRefreshAt = time.Now()
	return nil
}

func (h *Handler) scheduleFilterCacheRefresh(force bool) {
	h.filterCache.mu.Lock()
	if h.filterCache.refreshRunning {
		h.filterCache.mu.Unlock()
		return
	}
	h.filterCache.refreshRunning = true
	h.filterCache.mu.Unlock()

	go func() {
		defer func() {
			h.filterCache.mu.Lock()
			h.filterCache.refreshRunning = false
			h.filterCache.mu.Unlock()
		}()
		if err := h.ensureFilterCacheFresh(context.Background(), force); err != nil {
			fmt.Printf("failed refreshing jobs filter cache: %v\n", err)
		}
	}()
}

func (h *Handler) WarmFilterCache(_ context.Context) error {
	h.scheduleFilterCacheRefresh(true)
	return nil
}

func (h *Handler) filterOptions(c *gin.Context) {
	if err := h.ensureFilterCacheFresh(c.Request.Context(), false); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}
	minSalaryOptions := []int{}
	for salary := minSalaryStart; salary <= minSalaryEnd; salary += minSalaryStep {
		minSalaryOptions = append(minSalaryOptions, salary)
	}
	seniorities := []string{"entry", "junior", "mid", "senior", "lead"}
	c.JSON(http.StatusOK, gin.H{
		"job_category_parents": h.filterCache.jobCategoryParents,
		"location_parents":     h.filterCache.locationParents,
		"location_types":       h.filterCache.locationTypes,
		"employment_types":     h.filterCache.employmentTypes,
		"post_date_options":    postDateOptions,
		"tech_stacks":          h.filterCache.techStacks,
		"min_salary_options":   minSalaryOptions,
		"seniorities":          seniorities,
	})
}

func (h *Handler) jobDetail(c *gin.Context) {
	jobID, err := strconv.ParseInt(strings.TrimSpace(c.Param("jobID")), 10, 64)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Job not found"})
		return
	}
	row, err := h.q.GetJobDetailByID(c.Request.Context(), int32(jobID))
	if err != nil {
		var closed bool
		closedErr := h.db.SQL.QueryRowContext(
			c.Request.Context(),
			`SELECT EXISTS (
				SELECT 1
				FROM parsed_jobs
				WHERE id = ?
				  AND date_deleted IS NOT NULL
			)`,
			jobID,
		).Scan(&closed)
		if closedErr == nil && closed {
			c.JSON(http.StatusGone, gin.H{"detail": "Job is closed"})
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"detail": "Job not found"})
		return
	}
	detail := mapJobDetailRow(row)
	c.JSON(http.StatusOK, detail)
}

func (h *Handler) listJobs(c *gin.Context) {
	currentUser := h.auth.OptionalCurrentUser(c)
	hasFullAccess := false
	if currentUser != nil {
		var subscriptionID int32
		err := h.db.PGX.QueryRow(c.Request.Context(), `
SELECT s.id
FROM user_subscriptions s
JOIN pricing_plans p ON p.id = s.pricing_plan_id
WHERE s.user_id = $1 AND s.ends_at > $2 AND p.is_active = true
ORDER BY s.ends_at DESC
LIMIT 1
`,
			currentUser.ID,
			time.Now().UTC(),
		).Scan(&subscriptionID)
		hasFullAccess = err == nil
	}
	isPreview := !hasFullAccess
	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 20), 1)
	if perPage > 100 {
		perPage = 100
	}
	filterInput := h.buildListingFilterInput(c, currentUser, true)

	whereSQL, whereArgs := buildJobsWhereSQL(filterInput)
	totalSQL := `SELECT COUNT(p.id)::bigint AS total, COUNT(DISTINCT p.company_id)::bigint AS company_count FROM parsed_jobs p LEFT JOIN parsed_companies c ON c.id = p.company_id`
	if whereSQL != "" {
		totalSQL += " WHERE " + whereSQL
	}
	var rawTotal64 int64
	var companyCount64 int64
	if err := h.db.PGX.QueryRow(c.Request.Context(), totalSQL, whereArgs...).Scan(&rawTotal64, &companyCount64); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	rawTotal := int(rawTotal64)
	companyCount := int(companyCount64)
	if strings.TrimSpace(filterInput.CompanyFilter) != "" {
		if rawTotal > 0 {
			companyCount = 1
		} else {
			companyCount = 0
		}
	}
	previewPerPage := max(h.cfg.PublicJobsMaxPerPage, 1)
	previewMaxTotal := max(h.cfg.PublicJobsMaxTotal, 0)
	pageOut := page
	perPageOut := perPage
	offset := (page - 1) * perPage
	limit := perPage
	if isPreview {
		pageOut = 1
		perPageOut = previewPerPage
		offset = 0
		previewVisibleTotal := min(rawTotal, previewMaxTotal)
		limit = max(min(previewVisibleTotal, previewPerPage), 0)
	}
	idsSQL, idsArgs := buildJobsIDsSQL(filterInput, whereSQL, whereArgs, offset, limit)
	pagedIDs, err := queryJobIDs(c.Request.Context(), h.db.PGX, idsSQL, idsArgs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	items := []jobItem{}
	if len(pagedIDs) > 0 {
		rows, err := queryJobsByIDsInOrder(c.Request.Context(), h.db.PGX, pagedIDs)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
			return
		}
		items = rows
	}
	c.JSON(http.StatusOK, gin.H{
		"page":             pageOut,
		"per_page":         perPageOut,
		"total":            rawTotal,
		"company_count":    companyCount,
		"is_preview":       isPreview,
		"requires_login":   currentUser == nil && isPreview && rawTotal > len(items),
		"requires_upgrade": currentUser != nil && isPreview && rawTotal > len(items),
		"items":            items,
	})
}

type listJobsQueryRow struct {
	ID                     int32
	RawUSJobID             int32
	RoleTitle              pgtype.Text
	JobDescriptionSummary  pgtype.Text
	CompanyName            pgtype.Text
	CompanySlug            pgtype.Text
	CompanyTagline         pgtype.Text
	CompanyProfilePicURL   pgtype.Text
	CompanyHomePageURL     pgtype.Text
	CompanyLinkedInURL     pgtype.Text
	CompanyEmployeeRange   pgtype.Text
	CompanyFoundedYear     pgtype.Text
	CompanySponsorsH1B     pgtype.Bool
	CategorizedJobTitle    pgtype.Text
	CategorizedJobFunction pgtype.Text
	LocationCity           pgtype.Text
	LocationType           pgtype.Text
	LocationUsStates       []byte
	LocationCountries      []byte
	EmploymentType         pgtype.Text
	SalaryMin              pgtype.Float8
	SalaryMax              pgtype.Float8
	SalaryMinUsd           pgtype.Float8
	SalaryMaxUsd           pgtype.Float8
	SalaryCurrencyCode     pgtype.Text
	SalaryCurrencySymbol   pgtype.Text
	SalaryType             pgtype.Text
	IsEntryLevel           pgtype.Bool
	IsJunior               pgtype.Bool
	IsMidLevel             pgtype.Bool
	IsSenior               pgtype.Bool
	IsLead                 pgtype.Bool
	TechStack              []byte
	UpdatedAt              pgtype.Timestamptz
	CreatedAtSource        pgtype.Timestamptz
	Url                    pgtype.Text
}

func mapListJobsQueryRow(row listJobsQueryRow) jobItem {
	item := jobItem{
		ID:                    int64(row.ID),
		RawUSJobID:            int64(row.RawUSJobID),
		RoleTitle:             pgTextPtr(row.RoleTitle),
		JobDescriptionSummary: pgTextPtr(row.JobDescriptionSummary),
		CompanyName:           pgTextPtr(row.CompanyName),
		CompanySlug:           pgTextPtr(row.CompanySlug),
		CompanyTagline:        pgTextPtr(row.CompanyTagline),
		CompanyProfilePicURL:  pgTextPtr(row.CompanyProfilePicURL),
		CompanyHomePageURL:    pgTextPtr(row.CompanyHomePageURL),
		CompanyLinkedInURL:    pgTextPtr(row.CompanyLinkedInURL),
		CompanyEmployeeRange:  pgTextPtr(row.CompanyEmployeeRange),
		CompanyFoundedYear:    pgTextPtr(row.CompanyFoundedYear),
		CompanySponsorsH1B:    pgBoolPtr(row.CompanySponsorsH1B),
		CategorizedTitle:      pgTextPtr(row.CategorizedJobTitle),
		CategorizedFunction:   pgTextPtr(row.CategorizedJobFunction),
		LocationCity:          pgTextPtr(row.LocationCity),
		LocationType:          pgTextPtr(row.LocationType),
		EmploymentType:        pgTextPtr(row.EmploymentType),
		SalaryCurrencyCode:    pgTextPtr(row.SalaryCurrencyCode),
		SalaryCurrencySymbol:  pgTextPtr(row.SalaryCurrencySymbol),
		SalaryType:            pgTextPtr(row.SalaryType),
		IsEntryLevel:          pgBoolPtr(row.IsEntryLevel),
		IsJunior:              pgBoolPtr(row.IsJunior),
		IsMidLevel:            pgBoolPtr(row.IsMidLevel),
		IsSenior:              pgBoolPtr(row.IsSenior),
		IsLead:                pgBoolPtr(row.IsLead),
		URL:                   pgTextPtr(row.Url),
	}
	if row.SalaryMin.Valid {
		v := row.SalaryMin.Float64
		item.SalaryMin = &v
	}
	if row.SalaryMax.Valid {
		v := row.SalaryMax.Float64
		item.SalaryMax = &v
	}
	if row.SalaryMinUsd.Valid {
		v := row.SalaryMinUsd.Float64
		item.SalaryMinUSD = &v
	}
	if row.SalaryMaxUsd.Valid {
		v := row.SalaryMaxUsd.Float64
		item.SalaryMaxUSD = &v
	}
	if len(row.TechStack) > 0 {
		_ = json.Unmarshal(row.TechStack, &item.TechStack)
	}
	if len(row.LocationUsStates) > 0 {
		_ = json.Unmarshal(row.LocationUsStates, &item.LocationUSStates)
	}
	if len(row.LocationCountries) > 0 {
		_ = json.Unmarshal(row.LocationCountries, &item.LocationCountries)
	}
	item.CreatedAtSource = timestamptzTimePtr(row.CreatedAtSource)
	if row.UpdatedAt.Valid {
		updatedAt := row.UpdatedAt.Time.UTC()
		item.UpdatedAt = &updatedAt
	}
	return item
}

type sqlArgsBuilder struct {
	args []any
}

func (b *sqlArgsBuilder) add(value any) string {
	b.args = append(b.args, value)
	return fmt.Sprintf("$%d", len(b.args))
}

func buildJobsWhereSQL(input listingFilterInput) (string, []any) {
	b := sqlArgsBuilder{args: []any{}}
	clauses := make([]string, 0, 16)
	clauses = append(clauses, "p.date_deleted IS NULL")

	titleOr := make([]string, 0, 8)
	if len(input.JobCategories) > 0 {
		titleOr = append(titleOr, fmt.Sprintf("p.categorized_job_title = ANY(%s::text[])", b.add(input.JobCategories)))
	}
	if len(input.JobFunctions) > 0 {
		titleOr = append(titleOr, fmt.Sprintf("p.categorized_job_function = ANY(%s::text[])", b.add(input.JobFunctions)))
	}
	if len(input.TitleTokenGroups) > 0 {
		for _, group := range input.TitleTokenGroups {
			if len(group) == 0 {
				continue
			}
			andParts := make([]string, 0, len(group))
			companyParts := make([]string, 0, len(group))
			for _, tok := range group {
				if strings.TrimSpace(tok) == "" {
					continue
				}
				andParts = append(andParts, fmt.Sprintf("p.role_title ILIKE ('%%' || %s::text || '%%')", b.add(tok)))
				companyParts = append(companyParts, fmt.Sprintf("c.name ILIKE ('%%' || %s::text || '%%')", b.add(tok)))
			}
			if len(andParts) > 0 {
				titleOr = append(titleOr, "("+strings.Join(andParts, " AND ")+")")
			}
			if len(companyParts) > 0 {
				titleOr = append(titleOr, "("+strings.Join(companyParts, " AND ")+")")
			}
		}
	}
	if len(titleOr) > 0 {
		clauses = append(clauses, "("+strings.Join(titleOr, " OR ")+")")
	}

	if strings.TrimSpace(input.CompanyFilter) != "" {
		ph := b.add(input.CompanyFilter)
		clauses = append(clauses, fmt.Sprintf("(c.slug = %s::text OR lower(c.name) = %s::text)", ph, ph))
	}

	if input.HasStructuredLocation {
		locOr := make([]string, 0, len(input.USStates)+len(input.Countries))
		for _, state := range input.USStates {
			if strings.TrimSpace(state) == "" {
				continue
			}
			locOr = append(locOr, fmt.Sprintf("CAST(p.location_us_states AS jsonb) @> to_jsonb(ARRAY[%s::text])", b.add(state)))
		}
		if len(input.USStates) > 0 && !input.StrictLocation {
			locOr = append(
				locOr,
				fmt.Sprintf(
					"(CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[%s::text]) AND COALESCE(jsonb_array_length(CAST(p.location_us_states AS jsonb)), 0) = 0)",
					b.add(unitedStatesCountry),
				),
			)
			for _, region := range broadRegionTermsForUSStates(input.USStates) {
				locOr = append(locOr, fmt.Sprintf("CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[%s::text])", b.add(region)))
			}
		}
		for _, country := range input.Countries {
			if strings.TrimSpace(country) == "" {
				continue
			}
			locOr = append(locOr, fmt.Sprintf("CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[%s::text])", b.add(country)))
		}
		if len(locOr) > 0 {
			clauses = append(clauses, "("+strings.Join(locOr, " OR ")+")")
		}
	} else if len(input.LocationPatterns) > 0 {
		ph := b.add(input.LocationPatterns)
		clauses = append(clauses, fmt.Sprintf("(p.location_city ILIKE ANY(%s::text[]) OR CAST(p.location_us_states AS text) ILIKE ANY(%s::text[]) OR CAST(p.location_countries AS text) ILIKE ANY(%s::text[]))", ph, ph, ph))
	}

	if len(input.TechStacks) > 0 {
		stackOr := make([]string, 0, len(input.TechStacks))
		for _, stack := range input.TechStacks {
			if strings.TrimSpace(stack) == "" {
				continue
			}
			stackOr = append(stackOr, fmt.Sprintf("CAST(p.tech_stack AS jsonb) @> to_jsonb(ARRAY[%s::text])", b.add(stack)))
		}
		if len(stackOr) > 0 {
			clauses = append(clauses, "("+strings.Join(stackOr, " OR ")+")")
		}
	}

	if len(input.LocationTypes) > 0 {
		clauses = append(clauses, fmt.Sprintf("p.location_type ILIKE ANY(%s::text[])", b.add(input.LocationTypes)))
	}

	if len(input.EmploymentTypes) > 0 {
		clauses = append(clauses, fmt.Sprintf("p.employment_type ILIKE ANY(%s::text[])", b.add(input.EmploymentTypes)))
	}

	if input.HasCreatedFrom {
		clauses = append(clauses, fmt.Sprintf("p.created_at_source >= %s::timestamptz", b.add(input.CreatedFrom.Time)))
	}
	if input.HasCreatedTo {
		clauses = append(clauses, fmt.Sprintf("p.created_at_source < %s::timestamptz", b.add(input.CreatedTo.Time)))
	}

	if input.HasMinSalary {
		minPh := b.add(input.MinSalary)
		clauses = append(clauses, fmt.Sprintf("(%s >= %s::float8 OR %s >= %s::float8)",
			annualizedSalarySQL("COALESCE(p.salary_max_usd, p.salary_min_usd)"),
			minPh,
			annualizedSalarySQL("COALESCE(p.salary_max, p.salary_min)"),
			minPh,
		))
	}

	if input.HasSeniority {
		senOr := make([]string, 0, 5)
		if input.SeniorityEntry {
			senOr = append(senOr, "p.is_entry_level = true")
		}
		if input.SeniorityJunior {
			senOr = append(senOr, "p.is_junior = true")
		}
		if input.SeniorityMid {
			senOr = append(senOr, "p.is_mid_level = true")
		}
		if input.SenioritySenior {
			senOr = append(senOr, "p.is_senior = true")
		}
		if input.SeniorityLead {
			senOr = append(senOr, "p.is_lead = true")
		}
		if len(senOr) > 0 {
			clauses = append(clauses, "("+strings.Join(senOr, " OR ")+")")
		}
	}

	if input.HasUser {
		userIDPh := b.add(input.UserID)
		switch strings.TrimSpace(input.UserActionFilter) {
		case "hidden":
			clauses = append(clauses, fmt.Sprintf("EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = %s::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true)", userIDPh))
		case "applied":
			clauses = append(clauses, fmt.Sprintf("EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = %s::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true)", userIDPh))
		case "saved":
			clauses = append(clauses, fmt.Sprintf("EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = %s::bigint AND uja.parsed_job_id = p.id AND uja.is_saved = true)", userIDPh))
		case "not_applied":
			clauses = append(clauses, fmt.Sprintf("NOT EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = %s::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true)", userIDPh))
			clauses = append(clauses, fmt.Sprintf("NOT EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = %s::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true)", userIDPh))
		default:
			clauses = append(clauses, fmt.Sprintf("NOT EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = %s::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true)", userIDPh))
		}
	}

	return strings.Join(clauses, " AND "), b.args
}

func buildJobsIDsSQL(input listingFilterInput, whereSQL string, whereArgs []any, offset int, limit int) (string, []any) {
	args := make([]any, 0, len(whereArgs)+2)
	args = append(args, whereArgs...)
	b := sqlArgsBuilder{args: args}

	sqlText := `SELECT p.id FROM parsed_jobs p LEFT JOIN parsed_companies c ON c.id = p.company_id`
	if whereSQL != "" {
		sqlText += " WHERE " + whereSQL
	}
	if input.SortSalary {
		sqlText += fmt.Sprintf(" ORDER BY %s DESC, %s DESC, p.id DESC",
			annualizedSalarySQL("COALESCE(p.salary_max_usd, p.salary_min_usd)"),
			annualizedSalarySQL("COALESCE(p.salary_max, p.salary_min)"),
		)
	} else {
		sqlText += " ORDER BY p.created_at_source DESC, p.id DESC"
	}
	sqlText += fmt.Sprintf(" LIMIT %s::int OFFSET %s::int", b.add(limit), b.add(offset))
	return sqlText, b.args
}

func queryJobIDs(ctx context.Context, pool *pgxpool.Pool, sqlText string, args []any) ([]int64, error) {
	rows, err := pool.Query(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func queryJobsByIDsInOrder(ctx context.Context, pool *pgxpool.Pool, ids []int64) ([]jobItem, error) {
	sqlText := `
SELECT p.id, p.raw_us_job_id, p.role_title, p.job_description_summary,
       c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b,
       p.categorized_job_title, p.categorized_job_function, p.location_city, p.location_type, p.location_us_states, p.location_countries, p.employment_type,
       p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_currency_code, p.salary_currency_symbol, p.salary_type,
       p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead,
       p.tech_stack, p.updated_at, p.created_at_source, p.url
FROM parsed_jobs p
LEFT JOIN parsed_companies c ON c.id = p.company_id
WHERE p.id = ANY($1::bigint[])
  AND p.date_deleted IS NULL
ORDER BY array_position($1::bigint[], p.id)
`
	rows, err := pool.Query(ctx, sqlText, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []jobItem{}
	for rows.Next() {
		var row listJobsQueryRow
		if err := rows.Scan(
			&row.ID,
			&row.RawUSJobID,
			&row.RoleTitle,
			&row.JobDescriptionSummary,
			&row.CompanyName,
			&row.CompanySlug,
			&row.CompanyTagline,
			&row.CompanyProfilePicURL,
			&row.CompanyHomePageURL,
			&row.CompanyLinkedInURL,
			&row.CompanyEmployeeRange,
			&row.CompanyFoundedYear,
			&row.CompanySponsorsH1B,
			&row.CategorizedJobTitle,
			&row.CategorizedJobFunction,
			&row.LocationCity,
			&row.LocationType,
			&row.LocationUsStates,
			&row.LocationCountries,
			&row.EmploymentType,
			&row.SalaryMin,
			&row.SalaryMax,
			&row.SalaryMinUsd,
			&row.SalaryMaxUsd,
			&row.SalaryCurrencyCode,
			&row.SalaryCurrencySymbol,
			&row.SalaryType,
			&row.IsEntryLevel,
			&row.IsJunior,
			&row.IsMidLevel,
			&row.IsSenior,
			&row.IsLead,
			&row.TechStack,
			&row.UpdatedAt,
			&row.CreatedAtSource,
			&row.Url,
		); err != nil {
			return nil, err
		}
		items = append(items, mapListJobsQueryRow(row))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (h *Handler) metrics(c *gin.Context) {
	currentUser := h.auth.OptionalCurrentUser(c)
	filterInput := h.buildListingFilterInput(c, currentUser, false)
	now := time.Now().UTC()
	todayCutoff := now.Add(-24 * time.Hour)
	lastHourCutoff := now.Add(-1 * time.Hour)
	metrics, err := h.q.GetJobsMetricsFiltered(c.Request.Context(), gensqlc.GetJobsMetricsFilteredParams{
		TodayCutoff:           pgtype.Timestamptz{Time: todayCutoff, Valid: true},
		LastHourCutoff:        pgtype.Timestamptz{Time: lastHourCutoff, Valid: true},
		HasTitleFilters:       filterInput.HasTitleFilters,
		JobCategories:         filterInput.JobCategories,
		JobFunctions:          filterInput.JobFunctions,
		TitleTokenGroupsJson:  filterInput.TitleTokenGroupsJSON,
		CompanyFilter:         filterInput.CompanyFilter,
		HasStructuredLocation: filterInput.HasStructuredLocation,
		UsStates:              filterInput.USStates,
		Countries:             filterInput.Countries,
		LocationPatterns:      filterInput.LocationPatterns,
		TechStacks:            filterInput.TechStacks,
		LocationTypes:         filterInput.LocationTypes,
		EmploymentTypes:       filterInput.EmploymentTypes,
		HasCreatedFrom:        filterInput.HasCreatedFrom,
		CreatedFrom:           filterInput.CreatedFrom,
		HasCreatedTo:          filterInput.HasCreatedTo,
		CreatedTo:             filterInput.CreatedTo,
		HasMinSalary:          filterInput.HasMinSalary,
		MinSalary:             filterInput.MinSalary,
		HasSeniority:          filterInput.HasSeniority,
		SeniorityEntry:        filterInput.SeniorityEntry,
		SeniorityJunior:       filterInput.SeniorityJunior,
		SeniorityMid:          filterInput.SeniorityMid,
		SenioritySenior:       filterInput.SenioritySenior,
		SeniorityLead:         filterInput.SeniorityLead,
		HasUser:               filterInput.HasUser,
		UserActionFilter:      filterInput.UserActionFilter,
		UserID:                filterInput.UserID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs metrics"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"jobs_today":           metrics.JobsToday,
		"jobs_last_hour":       metrics.JobsLastHour,
		"companies_hiring_now": metrics.CompaniesHiringNow,
	})
}

func (h *Handler) sitemap(c *gin.Context) {
	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 500), 1)
	if perPage > 50000 {
		perPage = 50000
	}
	offset := (page - 1) * perPage
	totalCount, err := h.q.CountParsedJobs(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs sitemap"})
		return
	}
	rows, err := h.q.ListJobSitemapPage(c.Request.Context(), gensqlc.ListJobSitemapPageParams{
		Limit:  int32(perPage),
		Offset: int32(offset),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs sitemap"})
		return
	}
	items := []jobSitemapItem{}
	for _, row := range rows {
		items = append(items, jobSitemapItem{
			ID:               int64(row.ID),
			RoleTitle:        pgTextPtr(row.RoleTitle),
			CategorizedTitle: pgTextPtr(row.CategorizedJobTitle),
			CompanyName:      pgTextPtr(row.Name),
			CreatedAtSource:  timestamptzStringPtr(row.CreatedAtSource),
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"page":     page,
		"per_page": perPage,
		"total":    totalCount,
		"items":    items,
	})
}

func (h *Handler) jobsCount(c *gin.Context) {
	totalCount, err := h.q.CountParsedJobs(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs count"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"total": totalCount})
}

func (h *Handler) relatedCategories(c *gin.Context) {
	category := strings.TrimSpace(c.Query("category"))
	if category == "" {
		c.JSON(http.StatusOK, gin.H{"items": []any{}})
		return
	}
	limit := max(parseIntDefault(c.Query("limit"), 8), 1)
	if limit > 20 {
		limit = 20
	}

	topFunction, err := h.q.GetTopFunctionByCategory(c.Request.Context(), pgtype.Text{String: category, Valid: true})
	if err != nil || !topFunction.Valid || strings.TrimSpace(topFunction.String) == "" {
		c.JSON(http.StatusOK, gin.H{"items": []any{}})
		return
	}
	rows, err := h.q.ListRelatedCategoriesByFunction(c.Request.Context(), gensqlc.ListRelatedCategoriesByFunctionParams{
		CategorizedJobFunction: topFunction,
		CategorizedJobTitle:    pgtype.Text{String: category, Valid: true},
		Limit:                  int32(limit),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load related categories"})
		return
	}
	items := []gin.H{}
	for _, row := range rows {
		itemCategory := strings.TrimSpace(pgTextString(row.CategorizedJobTitle))
		if itemCategory != "" {
			items = append(items, gin.H{"category": itemCategory, "score": row.Score})
		}
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *Handler) topCategories(c *gin.Context) {
	limit := max(parseIntDefault(c.Query("limit"), 8), 1)
	if limit > 30 {
		limit = 30
	}
	days := max(parseIntDefault(c.Query("days"), 30), 1)
	if days > 365 {
		days = 365
	}
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)

	input := listingFilterInput{
		USStates:  []string{},
		Countries: []string{},
	}
	for _, state := range uniqueStrings(parseCSVQuery(c.Query("us_states"))) {
		if normalized := locationnorm.NormalizeUSStateName(state); normalized != "" {
			input.USStates = append(input.USStates, normalized)
		}
	}
	for _, country := range uniqueStrings(parseCSVQuery(c.Query("countries"))) {
		if trimmed := strings.TrimSpace(country); trimmed != "" {
			input.Countries = append(input.Countries, trimmed)
		}
	}
	if !input.StrictLocation {
		input.Countries = expandCountryFilterTerms(input.Countries)
	}
	input.HasStructuredLocation = len(input.USStates) > 0 || len(input.Countries) > 0

	args := []any{cutoff}
	clauses := []string{
		"p.date_deleted IS NULL",
		"p.categorized_job_title IS NOT NULL",
		"p.categorized_job_title != ''",
		"p.created_at_source IS NOT NULL",
		fmt.Sprintf("p.created_at_source >= $%d", len(args)),
	}
	if input.HasStructuredLocation {
		locOr := make([]string, 0, len(input.USStates)+len(input.Countries)+2)
		for _, state := range input.USStates {
			if strings.TrimSpace(state) == "" {
				continue
			}
			args = append(args, state)
			locOr = append(locOr, fmt.Sprintf("CAST(p.location_us_states AS jsonb) @> to_jsonb(ARRAY[$%d::text])", len(args)))
		}
		if len(input.USStates) > 0 && !input.StrictLocation {
			args = append(args, unitedStatesCountry)
			locOr = append(
				locOr,
				fmt.Sprintf(
					"(CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[$%d::text]) AND COALESCE(jsonb_array_length(CAST(p.location_us_states AS jsonb)), 0) = 0)",
					len(args),
				),
			)
			for _, region := range broadRegionTermsForUSStates(input.USStates) {
				args = append(args, region)
				locOr = append(locOr, fmt.Sprintf("CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[$%d::text])", len(args)))
			}
		}
		for _, country := range input.Countries {
			if strings.TrimSpace(country) == "" {
				continue
			}
			args = append(args, country)
			locOr = append(locOr, fmt.Sprintf("CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[$%d::text])", len(args)))
		}
		if len(locOr) > 0 {
			clauses = append(clauses, "("+strings.Join(locOr, " OR ")+")")
		}
	}
	args = append(args, limit)
	query := `SELECT p.categorized_job_title, COUNT(p.id)::bigint AS score
FROM parsed_jobs p
WHERE ` + strings.Join(clauses, " AND ") + `
GROUP BY p.categorized_job_title
ORDER BY score DESC, p.categorized_job_title ASC
LIMIT $` + strconv.Itoa(len(args))

	rows, err := h.db.PGX.Query(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load top categories"})
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var category string
		var score int64
		if err := rows.Scan(&category, &score); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load top categories"})
			return
		}
		category = strings.TrimSpace(category)
		if category != "" {
			items = append(items, gin.H{"category": category, "score": score})
		}
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load top categories"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *Handler) topFunctions(c *gin.Context) {
	limit := max(parseIntDefault(c.Query("limit"), 10), 1)
	if limit > 30 {
		limit = 30
	}
	days := max(parseIntDefault(c.Query("days"), 30), 1)
	if days > 365 {
		days = 365
	}
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)

	input := listingFilterInput{
		USStates:  []string{},
		Countries: []string{},
	}
	for _, state := range uniqueStrings(parseCSVQuery(c.Query("us_states"))) {
		if normalized := locationnorm.NormalizeUSStateName(state); normalized != "" {
			input.USStates = append(input.USStates, normalized)
		}
	}
	for _, country := range uniqueStrings(parseCSVQuery(c.Query("countries"))) {
		if trimmed := strings.TrimSpace(country); trimmed != "" {
			input.Countries = append(input.Countries, trimmed)
		}
	}
	if !queryBoolDefault(c, "strict_location", false) {
		input.Countries = expandCountryFilterTerms(input.Countries)
	}
	input.HasStructuredLocation = len(input.USStates) > 0 || len(input.Countries) > 0

	args := []any{cutoff}
	clauses := []string{
		"p.date_deleted IS NULL",
		"p.categorized_job_function IS NOT NULL",
		"p.categorized_job_function != ''",
		"p.created_at_source IS NOT NULL",
		fmt.Sprintf("p.created_at_source >= $%d", len(args)),
	}
	if input.HasStructuredLocation {
		locOr := make([]string, 0, len(input.USStates)+len(input.Countries)+2)
		for _, state := range input.USStates {
			if strings.TrimSpace(state) == "" {
				continue
			}
			args = append(args, state)
			locOr = append(locOr, fmt.Sprintf("CAST(p.location_us_states AS jsonb) @> to_jsonb(ARRAY[$%d::text])", len(args)))
		}
		if len(input.USStates) > 0 && !queryBoolDefault(c, "strict_location", false) {
			args = append(args, unitedStatesCountry)
			locOr = append(
				locOr,
				fmt.Sprintf(
					"(CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[$%d::text]) AND COALESCE(jsonb_array_length(CAST(p.location_us_states AS jsonb)), 0) = 0)",
					len(args),
				),
			)
			for _, region := range broadRegionTermsForUSStates(input.USStates) {
				args = append(args, region)
				locOr = append(locOr, fmt.Sprintf("CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[$%d::text])", len(args)))
			}
		}
		for _, country := range input.Countries {
			if strings.TrimSpace(country) == "" {
				continue
			}
			args = append(args, country)
			locOr = append(locOr, fmt.Sprintf("CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[$%d::text])", len(args)))
		}
		if len(locOr) > 0 {
			clauses = append(clauses, "("+strings.Join(locOr, " OR ")+")")
		}
	}
	args = append(args, limit)
	query := `SELECT p.categorized_job_function, COUNT(p.id)::bigint AS score
FROM parsed_jobs p
WHERE ` + strings.Join(clauses, " AND ") + `
GROUP BY p.categorized_job_function
ORDER BY score DESC, p.categorized_job_function ASC
LIMIT $` + strconv.Itoa(len(args))

	rows, err := h.db.PGX.Query(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load top job functions"})
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var jobFunction string
		var score int64
		if err := rows.Scan(&jobFunction, &score); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load top job functions"})
			return
		}
		jobFunction = strings.TrimSpace(jobFunction)
		if jobFunction != "" {
			items = append(items, gin.H{"job_function": jobFunction, "score": score})
		}
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load top job functions"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func isValidLocationOption(value string) bool {
	cleaned := strings.TrimSpace(value)
	if cleaned == "" {
		return false
	}
	if strings.EqualFold(cleaned, "anywhere") {
		return false
	}
	for _, r := range cleaned {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return true
		}
	}
	return false
}

func mustJSONStringArray(value string) string {
	payload, err := json.Marshal([]string{value})
	if err != nil {
		return "[]"
	}
	return string(payload)
}

type listingFilterInput struct {
	HasTitleFilters       bool
	JobCategories         []string
	JobFunctions          []string
	TitleTokenGroups      [][]string
	TitleTokenGroupsJSON  []byte
	CompanyFilter         string
	HasStructuredLocation bool
	StrictLocation        bool
	USStates              []string
	Countries             []string
	LocationPatterns      []string
	TechStacks            []string
	LocationTypes         []string
	EmploymentTypes       []string
	HasCreatedFrom        bool
	CreatedFrom           pgtype.Timestamptz
	HasCreatedTo          bool
	CreatedTo             pgtype.Timestamptz
	HasMinSalary          bool
	MinSalary             float64
	HasSeniority          bool
	SeniorityEntry        bool
	SeniorityJunior       bool
	SeniorityMid          bool
	SenioritySenior       bool
	SeniorityLead         bool
	HasUser               bool
	UserActionFilter      string
	UserID                int64
	SortSalary            bool
}

func (h *Handler) buildListingFilterInput(c *gin.Context, currentUser *auth.User, includePostDate bool) listingFilterInput {
	input := listingFilterInput{
		JobCategories:        []string{},
		JobFunctions:         []string{},
		TitleTokenGroups:     [][]string{},
		TitleTokenGroupsJSON: []byte("[]"),
		USStates:             []string{},
		Countries:            []string{},
		LocationPatterns:     []string{},
		TechStacks:           []string{},
		LocationTypes:        []string{},
		EmploymentTypes:      []string{},
		StrictLocation:       queryBoolDefault(c, "strict_location", false),
	}

	if categories := uniqueStrings(parseCSVQuery(c.Query("job_categories"))); len(categories) > 0 {
		for _, category := range categories {
			trimmed := strings.TrimSpace(category)
			if trimmed != "" {
				input.JobCategories = append(input.JobCategories, trimmed)
			}
		}
	}
	if functions := uniqueStrings(parseCSVQuery(c.Query("job_functions"))); len(functions) > 0 {
		for _, fn := range functions {
			trimmed := strings.TrimSpace(fn)
			if trimmed != "" {
				input.JobFunctions = append(input.JobFunctions, trimmed)
			}
		}
	}

	titleValues := parseCSVQuery(c.Query("job_titles"))
	if len(titleValues) == 0 {
		titleValues = parseCSVQuery(c.Query("job_title"))
	}
	titleTokenGroups := make([][]string, 0, len(titleValues))
	for _, title := range uniqueStrings(titleValues) {
		normalizedTitle := strings.TrimSpace(title)
		if normalizedTitle == "" {
			continue
		}
		if tokens := uniqueStrings(tokenizeTitleSearchText(normalizedTitle)); len(tokens) > 0 {
			titleTokenGroups = append(titleTokenGroups, tokens)
		}
	}
	input.TitleTokenGroups = titleTokenGroups
	if payload, err := json.Marshal(titleTokenGroups); err == nil {
		input.TitleTokenGroupsJSON = payload
	}
	input.HasTitleFilters = len(input.JobCategories) > 0 || len(input.JobFunctions) > 0 || len(titleTokenGroups) > 0

	for _, state := range uniqueStrings(parseCSVQuery(c.Query("us_states"))) {
		if normalized := locationnorm.NormalizeUSStateName(state); normalized != "" {
			input.USStates = append(input.USStates, normalized)
		}
	}
	for _, country := range uniqueStrings(parseCSVQuery(c.Query("countries"))) {
		if trimmed := strings.TrimSpace(country); trimmed != "" {
			input.Countries = append(input.Countries, trimmed)
		}
	}
	if !queryBoolDefault(c, "strict_location", false) {
		input.Countries = expandCountryFilterTerms(input.Countries)
	}
	input.HasStructuredLocation = len(input.USStates) > 0 || len(input.Countries) > 0

	input.CompanyFilter = strings.ToLower(strings.TrimSpace(c.Query("company")))

	for _, tech := range uniqueStrings(parseCSVQuery(c.Query("tech_stack"))) {
		if trimmed := strings.TrimSpace(tech); trimmed != "" {
			input.TechStacks = append(input.TechStacks, trimmed)
		}
	}
	for _, locationType := range uniqueStrings(parseCSVQuery(c.Query("location_type"))) {
		if trimmed := strings.TrimSpace(locationType); trimmed != "" {
			input.LocationTypes = append(input.LocationTypes, "%"+trimmed+"%")
		}
	}
	for _, employment := range uniqueStrings(parseCSVQuery(c.Query("employment_type"))) {
		if trimmed := strings.TrimSpace(employment); trimmed != "" {
			input.EmploymentTypes = append(input.EmploymentTypes, "%"+trimmed+"%")
		}
	}

	if postDateFrom := strings.TrimSpace(c.Query("post_date_from")); postDateFrom != "" {
		if cutoff, ok := parsePostDateFrom(postDateFrom); ok {
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: cutoff, Valid: true}
		}
	}
	if postDateTo := strings.TrimSpace(c.Query("post_date_to")); postDateTo != "" {
		if cutoff, ok := parsePostDateFrom(postDateTo); ok {
			input.HasCreatedTo = true
			input.CreatedTo = pgtype.Timestamptz{Time: cutoff, Valid: true}
		}
	}
	if includePostDate {
		if postDate := strings.ToLower(strings.TrimSpace(c.Query("post_date"))); postDate != "" {
			postDateLocation := resolvePostDateTimezone(c.Query("post_date_tz"), c.Query("post_date_tz_offset"))
			nowLocal := time.Now().In(postDateLocation)
			todayStartLocal := time.Date(nowLocal.Year(), nowLocal.Month(), nowLocal.Day(), 0, 0, 0, 0, postDateLocation)
			weekday := int(todayStartLocal.Weekday())
			if weekday == 0 {
				weekday = 7
			}
			thisWeekStartLocal := todayStartLocal.AddDate(0, 0, -(weekday - 1))
			thisMonthStartLocal := time.Date(nowLocal.Year(), nowLocal.Month(), 1, 0, 0, 0, 0, postDateLocation)
			lastMonthStartLocal := thisMonthStartLocal.AddDate(0, -1, 0)
			lastWeekStartLocal := thisWeekStartLocal.AddDate(0, 0, -7)
			nowUTC := nowLocal.UTC()
			todayStart := todayStartLocal.UTC()
			thisWeekStart := thisWeekStartLocal.UTC()
			thisMonthStart := thisMonthStartLocal.UTC()
			lastMonthStart := lastMonthStartLocal.UTC()
			lastWeekStart := lastWeekStartLocal.UTC()

			switch postDate {
			case "today":
				input.HasCreatedFrom = true
				input.CreatedFrom = pgtype.Timestamptz{Time: todayStart, Valid: true}
				input.HasCreatedTo = false
				input.CreatedTo = pgtype.Timestamptz{}
			case "yesterday":
				input.HasCreatedFrom = true
				input.CreatedFrom = pgtype.Timestamptz{Time: todayStart.AddDate(0, 0, -1), Valid: true}
				input.HasCreatedTo = true
				input.CreatedTo = pgtype.Timestamptz{Time: todayStart, Valid: true}
			case "this_week":
				input.HasCreatedFrom = true
				input.CreatedFrom = pgtype.Timestamptz{Time: thisWeekStart, Valid: true}
				input.HasCreatedTo = false
				input.CreatedTo = pgtype.Timestamptz{}
			case "previous_week":
				input.HasCreatedFrom = true
				input.CreatedFrom = pgtype.Timestamptz{Time: lastWeekStart, Valid: true}
				input.HasCreatedTo = true
				input.CreatedTo = pgtype.Timestamptz{Time: thisWeekStart, Valid: true}
			case "this_month":
				input.HasCreatedFrom = true
				input.CreatedFrom = pgtype.Timestamptz{Time: thisMonthStart, Valid: true}
				input.HasCreatedTo = false
				input.CreatedTo = pgtype.Timestamptz{}
			case "previous_month":
				input.HasCreatedFrom = true
				input.CreatedFrom = pgtype.Timestamptz{Time: lastMonthStart, Valid: true}
				input.HasCreatedTo = true
				input.CreatedTo = pgtype.Timestamptz{Time: thisMonthStart, Valid: true}
			default:
				if window, ok := postDateWindows[postDate]; ok {
					input.HasCreatedFrom = true
					input.CreatedFrom = pgtype.Timestamptz{Time: nowUTC.Add(-window), Valid: true}
					input.HasCreatedTo = false
					input.CreatedTo = pgtype.Timestamptz{}
				}
			}
		}
	}

	if minSalary := strings.TrimSpace(c.Query("min_salary")); minSalary != "" {
		if parsed, err := strconv.ParseFloat(minSalary, 64); err == nil {
			input.HasMinSalary = true
			input.MinSalary = parsed
		}
	}

	if seniorities := parseCSVQuery(c.Query("seniority")); len(seniorities) > 0 {
		for _, seniority := range uniqueStrings(seniorities) {
			switch strings.ToLower(strings.TrimSpace(seniority)) {
			case "entry":
				input.SeniorityEntry = true
			case "junior":
				input.SeniorityJunior = true
			case "mid":
				input.SeniorityMid = true
			case "senior":
				input.SenioritySenior = true
			case "lead":
				input.SeniorityLead = true
			}
		}
		input.HasSeniority = input.SeniorityEntry || input.SeniorityJunior || input.SeniorityMid || input.SenioritySenior || input.SeniorityLead
	}

	if currentUser != nil {
		input.HasUser = true
		input.UserID = int64(currentUser.ID)
		input.UserActionFilter = strings.ToLower(strings.TrimSpace(c.DefaultQuery("user_job_action", "all")))
	}
	input.SortSalary = strings.EqualFold(strings.TrimSpace(c.DefaultQuery("sort_criteria", "date")), "salary")
	return input
}

func scanJob(scanner interface{ Scan(dest ...any) error }) (jobItem, error) {
	var item jobItem
	var roleTitle, summary, companyName, companySlug, companyTagline, companyProfilePicURL, companyHomePageURL, companyLinkedInURL, companyEmployeeRange, companyFoundedYear, categorizedTitle, categorizedFunction, locationCity, locationType, locationUSStates, locationCountries, employmentType, salaryCurrencyCode, salaryCurrencySymbol, salaryType, techStack, updatedAt, createdAt, url sql.NullString
	var companySponsorsH1B, isEntry, isJunior, isMid, isSenior, isLead sql.NullBool
	var salaryMin, salaryMax, salaryMinUSD, salaryMaxUSD sql.NullFloat64
	err := scanner.Scan(&item.ID, &item.RawUSJobID, &roleTitle, &summary, &companyName, &companySlug, &companyTagline, &companyProfilePicURL, &companyHomePageURL, &companyLinkedInURL, &companyEmployeeRange, &companyFoundedYear, &companySponsorsH1B, &categorizedTitle, &categorizedFunction, &locationCity, &locationType, &locationUSStates, &locationCountries, &employmentType, &salaryMin, &salaryMax, &salaryMinUSD, &salaryMaxUSD, &salaryCurrencyCode, &salaryCurrencySymbol, &salaryType, &isEntry, &isJunior, &isMid, &isSenior, &isLead, &techStack, &updatedAt, &createdAt, &url)
	if err != nil {
		return item, err
	}
	item.RoleTitle = nullableString(roleTitle)
	item.JobDescriptionSummary = nullableString(summary)
	item.CompanyName = nullableString(companyName)
	item.CompanySlug = nullableString(companySlug)
	item.CompanyTagline = nullableString(companyTagline)
	item.CompanyProfilePicURL = nullableString(companyProfilePicURL)
	item.CompanyHomePageURL = nullableString(companyHomePageURL)
	item.CompanyLinkedInURL = nullableString(companyLinkedInURL)
	item.CompanyEmployeeRange = nullableString(companyEmployeeRange)
	item.CompanyFoundedYear = nullableString(companyFoundedYear)
	item.CompanySponsorsH1B = nullableBool(companySponsorsH1B)
	item.CategorizedTitle = nullableString(categorizedTitle)
	item.CategorizedFunction = nullableString(categorizedFunction)
	item.LocationCity = nullableString(locationCity)
	item.LocationType = nullableString(locationType)
	item.EmploymentType = nullableString(employmentType)
	item.SalaryMin = nullableFloatPtr(salaryMin)
	item.SalaryMax = nullableFloatPtr(salaryMax)
	item.SalaryMinUSD = nullableFloatPtr(salaryMinUSD)
	item.SalaryMaxUSD = nullableFloatPtr(salaryMaxUSD)
	item.SalaryCurrencyCode = nullableString(salaryCurrencyCode)
	item.SalaryCurrencySymbol = nullableString(salaryCurrencySymbol)
	item.SalaryType = nullableString(salaryType)
	item.IsEntryLevel = nullableBool(isEntry)
	item.IsJunior = nullableBool(isJunior)
	item.IsMidLevel = nullableBool(isMid)
	item.IsSenior = nullableBool(isSenior)
	item.IsLead = nullableBool(isLead)
	if techStack.Valid && techStack.String != "" {
		_ = json.Unmarshal([]byte(techStack.String), &item.TechStack)
	}
	if updatedAt.Valid {
		if parsed, err := time.Parse(time.RFC3339Nano, updatedAt.String); err == nil {
			item.UpdatedAt = &parsed
		}
	}
	item.URL = nullableString(url)
	if createdAt.Valid {
		if parsed, err := time.Parse(time.RFC3339Nano, createdAt.String); err == nil {
			item.CreatedAtSource = &parsed
		}
	}
	if locationUSStates.Valid && locationUSStates.String != "" {
		_ = json.Unmarshal([]byte(locationUSStates.String), &item.LocationUSStates)
	}
	if locationCountries.Valid && locationCountries.String != "" {
		_ = json.Unmarshal([]byte(locationCountries.String), &item.LocationCountries)
	}
	return item, nil
}

func scanJobDetail(scanner interface{ Scan(dest ...any) error }) (jobDetail, error) {
	var detail jobDetail
	var companyName, companySlug, companyTagline, companyProfilePicURL, companyHomePageURL, companyLinkedInURL, companyEmployeeRange, companyFoundedYear, categorizedTitle, categorizedFunction, roleTitle, locationCity, locationType, locationUSStates, locationCountries, employmentType, salaryCurrencyCode, salaryCurrencySymbol, salaryType, updatedAt, createdAt, roleDescription, roleRequirements, educationCategory, requiredLanguages, techStack, benefits, url sql.NullString
	var companySponsorsH1B, experienceInPlaceOfEducation sql.NullBool
	var salaryMin, salaryMax, salaryMinUSD, salaryMaxUSD sql.NullFloat64
	var experienceRequirementsMonths sql.NullInt64
	err := scanner.Scan(&detail.ID, &detail.RawUSJobID, &companyName, &companySlug, &companyTagline, &companyProfilePicURL, &companyHomePageURL, &companyLinkedInURL, &companyEmployeeRange, &companyFoundedYear, &companySponsorsH1B, &categorizedTitle, &categorizedFunction, &roleTitle, &locationCity, &locationType, &locationUSStates, &locationCountries, &employmentType, &salaryMin, &salaryMax, &salaryMinUSD, &salaryMaxUSD, &salaryCurrencyCode, &salaryCurrencySymbol, &salaryType, &updatedAt, &createdAt, &roleDescription, &roleRequirements, &educationCategory, &experienceRequirementsMonths, &experienceInPlaceOfEducation, &requiredLanguages, &techStack, &benefits, &url)
	if err != nil {
		return detail, err
	}
	detail.CompanyName = nullableString(companyName)
	detail.CompanySlug = nullableString(companySlug)
	detail.CompanyTagline = nullableString(companyTagline)
	detail.CompanyProfilePicURL = nullableString(companyProfilePicURL)
	detail.CompanyHomePageURL = nullableString(companyHomePageURL)
	detail.CompanyLinkedInURL = nullableString(companyLinkedInURL)
	detail.CompanyEmployeeRange = nullableString(companyEmployeeRange)
	detail.CompanyFoundedYear = nullableString(companyFoundedYear)
	detail.CompanySponsorsH1B = nullableBool(companySponsorsH1B)
	detail.CategorizedTitle = nullableString(categorizedTitle)
	detail.CategorizedFunction = nullableString(categorizedFunction)
	detail.RoleTitle = nullableString(roleTitle)
	detail.LocationCity = nullableString(locationCity)
	detail.LocationType = nullableString(locationType)
	detail.EmploymentType = nullableString(employmentType)
	detail.SalaryMin = nullableFloatPtr(salaryMin)
	detail.SalaryMax = nullableFloatPtr(salaryMax)
	detail.SalaryMinUSD = nullableFloatPtr(salaryMinUSD)
	detail.SalaryMaxUSD = nullableFloatPtr(salaryMaxUSD)
	detail.SalaryCurrencyCode = nullableString(salaryCurrencyCode)
	detail.SalaryCurrencySymbol = nullableString(salaryCurrencySymbol)
	detail.SalaryType = nullableString(salaryType)
	if updatedAt.Valid {
		detail.UpdatedAt = &updatedAt.String
	}
	detail.RoleDescription = nullableString(roleDescription)
	detail.RoleRequirements = nullableString(roleRequirements)
	detail.EducationRequirementsCredentialCategory = nullableString(educationCategory)
	if experienceRequirementsMonths.Valid {
		v := int(experienceRequirementsMonths.Int64)
		detail.ExperienceRequirementsMonths = &v
	}
	detail.ExperienceInPlaceOfEducation = nullableBool(experienceInPlaceOfEducation)
	if requiredLanguages.Valid && requiredLanguages.String != "" {
		_ = json.Unmarshal([]byte(requiredLanguages.String), &detail.RequiredLanguages)
	}
	if techStack.Valid && techStack.String != "" {
		_ = json.Unmarshal([]byte(techStack.String), &detail.TechStack)
	}
	detail.Benefits = nullableString(benefits)
	detail.URL = nullableString(url)
	if createdAt.Valid {
		detail.CreatedAtSource = &createdAt.String
	}
	if locationUSStates.Valid && locationUSStates.String != "" {
		_ = json.Unmarshal([]byte(locationUSStates.String), &detail.LocationUSStates)
	}
	if locationCountries.Valid && locationCountries.String != "" {
		_ = json.Unmarshal([]byte(locationCountries.String), &detail.LocationCountries)
	}
	return detail, nil
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	v := value.String
	return &v
}

func nullableFloatPtr(value sql.NullFloat64) *float64 {
	if !value.Valid {
		return nil
	}
	v := value.Float64
	return &v
}

func nullableBool(value sql.NullBool) *bool {
	if !value.Valid {
		return nil
	}
	v := value.Bool
	return &v
}

func pgTextString(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func pgTextPtr(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	v := value.String
	return &v
}

func pgBoolPtr(value pgtype.Bool) *bool {
	if !value.Valid {
		return nil
	}
	v := value.Bool
	return &v
}

func timestamptzStringPtr(value pgtype.Timestamptz) *string {
	if !value.Valid {
		return nil
	}
	formatted := value.Time.UTC().Format(time.RFC3339Nano)
	return &formatted
}

func timestamptzTimePtr(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	timestamp := value.Time.UTC()
	return &timestamp
}

func mapJobDetailRow(row *gensqlc.GetJobDetailByIDRow) jobDetail {
	detail := jobDetail{
		ID:                                      int64(row.ID),
		RawUSJobID:                              int64(row.RawUsJobID),
		CompanyName:                             pgTextPtr(row.Name),
		CompanySlug:                             pgTextPtr(row.Slug),
		CompanyTagline:                          pgTextPtr(row.Tagline),
		CompanyProfilePicURL:                    pgTextPtr(row.ProfilePicUrl),
		CompanyHomePageURL:                      pgTextPtr(row.HomePageUrl),
		CompanyLinkedInURL:                      pgTextPtr(row.LinkedinUrl),
		CompanyEmployeeRange:                    pgTextPtr(row.EmployeeRange),
		CompanyFoundedYear:                      pgTextPtr(row.FoundedYear),
		CompanySponsorsH1B:                      pgBoolPtr(row.SponsorsH1b),
		CategorizedTitle:                        pgTextPtr(row.CategorizedJobTitle),
		CategorizedFunction:                     pgTextPtr(row.CategorizedJobFunction),
		RoleTitle:                               pgTextPtr(row.RoleTitle),
		LocationCity:                            pgTextPtr(row.LocationCity),
		LocationType:                            pgTextPtr(row.LocationType),
		EmploymentType:                          pgTextPtr(row.EmploymentType),
		SalaryCurrencyCode:                      pgTextPtr(row.SalaryCurrencyCode),
		SalaryCurrencySymbol:                    pgTextPtr(row.SalaryCurrencySymbol),
		SalaryType:                              pgTextPtr(row.SalaryType),
		IsEntryLevel:                            pgBoolPtr(row.IsEntryLevel),
		IsJunior:                                pgBoolPtr(row.IsJunior),
		IsMidLevel:                              pgBoolPtr(row.IsMidLevel),
		IsSenior:                                pgBoolPtr(row.IsSenior),
		IsLead:                                  pgBoolPtr(row.IsLead),
		UpdatedAt:                               timestamptzStringPtr(row.UpdatedAt),
		CreatedAtSource:                         timestamptzStringPtr(row.CreatedAtSource),
		RoleDescription:                         pgTextPtr(row.RoleDescription),
		RoleRequirements:                        pgTextPtr(row.RoleRequirements),
		EducationRequirementsCredentialCategory: pgTextPtr(row.EducationRequirementsCredentialCategory),
		ExperienceInPlaceOfEducation:            pgBoolPtr(row.ExperienceInPlaceOfEducation),
		Benefits:                                pgTextPtr(row.Benefits),
		URL:                                     pgTextPtr(row.Url),
	}
	if row.SalaryMin.Valid {
		v := row.SalaryMin.Float64
		detail.SalaryMin = &v
	}
	if row.SalaryMax.Valid {
		v := row.SalaryMax.Float64
		detail.SalaryMax = &v
	}
	if row.SalaryMinUsd.Valid {
		v := row.SalaryMinUsd.Float64
		detail.SalaryMinUSD = &v
	}
	if row.SalaryMaxUsd.Valid {
		v := row.SalaryMaxUsd.Float64
		detail.SalaryMaxUSD = &v
	}
	if row.ExperienceRequirementsMonths.Valid {
		v := int(row.ExperienceRequirementsMonths.Int32)
		detail.ExperienceRequirementsMonths = &v
	}
	if len(row.LocationUsStates) > 0 {
		_ = json.Unmarshal(row.LocationUsStates, &detail.LocationUSStates)
	}
	if len(row.LocationCountries) > 0 {
		_ = json.Unmarshal(row.LocationCountries, &detail.LocationCountries)
	}
	if len(row.RequiredLanguages) > 0 {
		_ = json.Unmarshal(row.RequiredLanguages, &detail.RequiredLanguages)
	}
	if len(row.TechStack) > 0 {
		_ = json.Unmarshal(row.TechStack, &detail.TechStack)
	}
	return detail
}

func mapListJobRow(row *gensqlc.ListJobsByIDsInOrderRow) jobItem {
	item := jobItem{
		ID:                    int64(row.ID),
		RawUSJobID:            int64(row.RawUsJobID),
		RoleTitle:             pgTextPtr(row.RoleTitle),
		JobDescriptionSummary: pgTextPtr(row.JobDescriptionSummary),
		CompanyName:           pgTextPtr(row.Name),
		CompanySlug:           pgTextPtr(row.Slug),
		CompanyTagline:        pgTextPtr(row.Tagline),
		CompanyProfilePicURL:  pgTextPtr(row.ProfilePicUrl),
		CompanyHomePageURL:    pgTextPtr(row.HomePageUrl),
		CompanyLinkedInURL:    pgTextPtr(row.LinkedinUrl),
		CompanyEmployeeRange:  pgTextPtr(row.EmployeeRange),
		CompanyFoundedYear:    pgTextPtr(row.FoundedYear),
		CompanySponsorsH1B:    pgBoolPtr(row.SponsorsH1b),
		CategorizedTitle:      pgTextPtr(row.CategorizedJobTitle),
		LocationCity:          pgTextPtr(row.LocationCity),
		LocationType:          pgTextPtr(row.LocationType),
		EmploymentType:        pgTextPtr(row.EmploymentType),
		SalaryType:            pgTextPtr(row.SalaryType),
		IsEntryLevel:          pgBoolPtr(row.IsEntryLevel),
		IsJunior:              pgBoolPtr(row.IsJunior),
		IsMidLevel:            pgBoolPtr(row.IsMidLevel),
		IsSenior:              pgBoolPtr(row.IsSenior),
		IsLead:                pgBoolPtr(row.IsLead),
		URL:                   pgTextPtr(row.Url),
	}
	if row.SalaryMin.Valid {
		v := row.SalaryMin.Float64
		item.SalaryMin = &v
	}
	if row.SalaryMax.Valid {
		v := row.SalaryMax.Float64
		item.SalaryMax = &v
	}
	if row.SalaryMinUsd.Valid {
		v := row.SalaryMinUsd.Float64
		item.SalaryMinUSD = &v
	}
	if row.SalaryMaxUsd.Valid {
		v := row.SalaryMaxUsd.Float64
		item.SalaryMaxUSD = &v
	}
	if len(row.TechStack) > 0 {
		_ = json.Unmarshal(row.TechStack, &item.TechStack)
	}
	if len(row.LocationUsStates) > 0 {
		_ = json.Unmarshal(row.LocationUsStates, &item.LocationUSStates)
	}
	if len(row.LocationCountries) > 0 {
		_ = json.Unmarshal(row.LocationCountries, &item.LocationCountries)
	}
	if row.UpdatedAt.Valid {
		updatedAt := row.UpdatedAt.Time.UTC()
		item.UpdatedAt = &updatedAt
	}
	item.CreatedAtSource = timestamptzTimePtr(row.CreatedAtSource)
	return item
}

func parseIntDefault(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func queryBoolDefault(c *gin.Context, key string, fallback bool) bool {
	value := strings.TrimSpace(c.Query(key))
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func parsePostDateFrom(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), true
		}
	}
	if parsed, err := time.Parse("2006-01-02", raw); err == nil {
		return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC), true
	}
	return time.Time{}, false
}

func sortStrings(values []string) {
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
