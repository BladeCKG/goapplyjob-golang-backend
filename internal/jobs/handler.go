package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/locationnorm"
	"goapplyjob-golang-backend/internal/parsed"
	gensqlc "goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgtype"
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
	LocationCity          *string    `json:"location_city"`
	LocationType          *string    `json:"location_type"`
	LocationUSStates      []string   `json:"location_us_states"`
	LocationCountries     []string   `json:"location_countries"`
	EmploymentType        *string    `json:"employment_type"`
	SalaryMin             *float64   `json:"salary_min"`
	SalaryMax             *float64   `json:"salary_max"`
	SalaryMinUSD          *float64   `json:"salary_min_usd"`
	SalaryMaxUSD          *float64   `json:"salary_max_usd"`
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
	SalaryType                              *string  `json:"salary_type"`
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

type companySitemapItem struct {
	Slug              string  `json:"slug"`
	Name              *string `json:"name"`
	LatestJobPostedAt *string `json:"latest_job_posted_at"`
}

type companyProfileItem struct {
	ID                 int64    `json:"id"`
	Slug               string   `json:"slug"`
	Name               *string  `json:"name"`
	Tagline            *string  `json:"tagline"`
	ProfilePicURL      *string  `json:"profile_pic_url"`
	HomePageURL        *string  `json:"home_page_url"`
	LinkedInURL        *string  `json:"linkedin_url"`
	EmployeeRange      *string  `json:"employee_range"`
	FoundedYear        *string  `json:"founded_year"`
	SponsorsH1B        *bool    `json:"sponsors_h1b"`
	IndustrySpecialies []string `json:"industry_specialities"`
	TotalJobs          int64    `json:"total_jobs"`
	LatestJobPostedAt  *string  `json:"latest_job_posted_at"`
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
	router.GET("/jobs/sitemap", h.sitemap)
	router.GET("/companies/sitemap", h.companiesSitemap)
	router.GET("/companies/:companySlug", h.companyProfile)
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
	items := []string{}
	for _, item := range strings.Split(value, ",") {
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
				country = locationnorm.NormalizeCountryName(country, true)
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
			country = locationnorm.NormalizeCountryName(country, true)
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
	techRows := [][]string{}
	for _, row := range rows {
		if row == nil {
			continue
		}
		if row.CategorizedJobTitle.Valid || row.CategorizedJobFunction.Valid {
			categoryRows = append(categoryRows, [2]string{row.CategorizedJobTitle.String, row.CategorizedJobFunction.String})
			if row.CategorizedJobTitle.Valid && strings.TrimSpace(row.CategorizedJobTitle.String) != "" {
				categoryTitles = append(categoryTitles, strings.TrimSpace(row.CategorizedJobTitle.String))
			}
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
	employmentRows := []string{}
	for _, employmentType := range employmentTypeRowsRaw {
		if employmentType.Valid && strings.TrimSpace(employmentType.String) != "" {
			employmentRows = append(employmentRows, employmentType.String)
		}
	}

	h.filterCache.jobCategoryParents = buildJobCategoryParentsMap(categoryRows)
	parsed.SetCachedGroqCategorizedJobTitles(categoryTitles)
	locationParents := map[string][]string{}
	for _, state := range locationnorm.USStateNames() {
		locationParents[state] = []string{unitedStatesCountry}
	}
	locationParents[unitedStatesCountry] = []string{}
	h.filterCache.locationParents = locationParents
	h.filterCache.techStacks = buildTechStacks(techRows)
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
		_, err := h.q.GetActiveSubscriptionIDForUser(c.Request.Context(), gensqlc.GetActiveSubscriptionIDForUserParams{
			UserID: currentUser.ID,
			EndsAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		})
		hasFullAccess = err == nil
	}
	isPreview := !hasFullAccess
	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 20), 1)
	if perPage > 100 {
		perPage = 100
	}
	filterInput := h.buildListingFilterInput(c, currentUser, true)
	countRow, err := h.q.CountJobsForListingFiltered(c.Request.Context(), gensqlc.CountJobsForListingFilteredParams{
		CompanyFilter:          filterInput.CompanyFilter,
		HasTitleFilters:        filterInput.HasTitleFilters,
		JobCategories:          filterInput.JobCategories,
		JobFunctions:           filterInput.JobFunctions,
		TitleExactTerms:        filterInput.TitleExactTerms,
		TitleLikePatterns:      filterInput.TitleLikePatterns,
		TitleTokenGroupsJson:   filterInput.TitleTokenGroupsJSON,
		HasStructuredLocation:  filterInput.HasStructuredLocation,
		UsStates:               filterInput.USStates,
		Countries:              filterInput.Countries,
		LocationPatterns:       filterInput.LocationPatterns,
		TechStacks:             filterInput.TechStacks,
		EmploymentTypePatterns: filterInput.EmploymentTypePatterns,
		HasCreatedFrom:         filterInput.HasCreatedFrom,
		CreatedFrom:            filterInput.CreatedFrom,
		HasCreatedTo:           filterInput.HasCreatedTo,
		CreatedTo:              filterInput.CreatedTo,
		HasMinSalary:           filterInput.HasMinSalary,
		MinSalary:              filterInput.MinSalary,
		HasSeniority:           filterInput.HasSeniority,
		SeniorityEntry:         filterInput.SeniorityEntry,
		SeniorityJunior:        filterInput.SeniorityJunior,
		SeniorityMid:           filterInput.SeniorityMid,
		SenioritySenior:        filterInput.SenioritySenior,
		SeniorityLead:          filterInput.SeniorityLead,
		HasUser:                filterInput.HasUser,
		UserActionFilter:       filterInput.UserActionFilter,
		UserID:                 filterInput.UserID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	rawTotal := int(countRow.Total)
	companyCount := int(countRow.CompanyCount)
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
	pagedIDs, err := h.q.ListFilteredJobIDs(c.Request.Context(), gensqlc.ListFilteredJobIDsParams{
		HasTitleFilters:        filterInput.HasTitleFilters,
		JobCategories:          filterInput.JobCategories,
		JobFunctions:           filterInput.JobFunctions,
		TitleExactTerms:        filterInput.TitleExactTerms,
		TitleLikePatterns:      filterInput.TitleLikePatterns,
		TitleTokenGroupsJson:   filterInput.TitleTokenGroupsJSON,
		CompanyFilter:          filterInput.CompanyFilter,
		HasStructuredLocation:  filterInput.HasStructuredLocation,
		UsStates:               filterInput.USStates,
		Countries:              filterInput.Countries,
		LocationPatterns:       filterInput.LocationPatterns,
		TechStacks:             filterInput.TechStacks,
		EmploymentTypePatterns: filterInput.EmploymentTypePatterns,
		HasCreatedFrom:         filterInput.HasCreatedFrom,
		CreatedFrom:            filterInput.CreatedFrom,
		HasCreatedTo:           filterInput.HasCreatedTo,
		CreatedTo:              filterInput.CreatedTo,
		HasMinSalary:           filterInput.HasMinSalary,
		MinSalary:              filterInput.MinSalary,
		HasSeniority:           filterInput.HasSeniority,
		SeniorityEntry:         filterInput.SeniorityEntry,
		SeniorityJunior:        filterInput.SeniorityJunior,
		SeniorityMid:           filterInput.SeniorityMid,
		SenioritySenior:        filterInput.SenioritySenior,
		SeniorityLead:          filterInput.SeniorityLead,
		HasUser:                filterInput.HasUser,
		UserActionFilter:       filterInput.UserActionFilter,
		UserID:                 filterInput.UserID,
		SortSalary:             filterInput.SortSalary,
		OffsetRows:             int32(offset),
		LimitRows:              int32(limit),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	items := []jobItem{}
	if len(pagedIDs) > 0 {
		idList := make([]int64, 0, len(pagedIDs))
		for _, id := range pagedIDs {
			idList = append(idList, int64(id))
		}
		rows, err := h.q.ListJobsByIDsInOrder(c.Request.Context(), idList)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
			return
		}
		for _, row := range rows {
			items = append(items, mapListJobRow(row))
		}
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

func (h *Handler) metrics(c *gin.Context) {
	currentUser := h.auth.OptionalCurrentUser(c)
	filterInput := h.buildListingFilterInput(c, currentUser, false)
	now := time.Now().UTC()
	todayCutoff := now.Add(-24 * time.Hour)
	lastHourCutoff := now.Add(-1 * time.Hour)
	metrics, err := h.q.GetJobsMetricsFiltered(c.Request.Context(), gensqlc.GetJobsMetricsFilteredParams{
		TodayCutoff:            pgtype.Timestamptz{Time: todayCutoff, Valid: true},
		LastHourCutoff:         pgtype.Timestamptz{Time: lastHourCutoff, Valid: true},
		HasTitleFilters:        filterInput.HasTitleFilters,
		JobCategories:          filterInput.JobCategories,
		JobFunctions:           filterInput.JobFunctions,
		TitleExactTerms:        filterInput.TitleExactTerms,
		TitleLikePatterns:      filterInput.TitleLikePatterns,
		TitleTokenGroupsJson:   filterInput.TitleTokenGroupsJSON,
		CompanyFilter:          filterInput.CompanyFilter,
		HasStructuredLocation:  filterInput.HasStructuredLocation,
		UsStates:               filterInput.USStates,
		Countries:              filterInput.Countries,
		LocationPatterns:       filterInput.LocationPatterns,
		TechStacks:             filterInput.TechStacks,
		EmploymentTypePatterns: filterInput.EmploymentTypePatterns,
		HasCreatedFrom:         filterInput.HasCreatedFrom,
		CreatedFrom:            filterInput.CreatedFrom,
		HasCreatedTo:           filterInput.HasCreatedTo,
		CreatedTo:              filterInput.CreatedTo,
		HasMinSalary:           filterInput.HasMinSalary,
		MinSalary:              filterInput.MinSalary,
		HasSeniority:           filterInput.HasSeniority,
		SeniorityEntry:         filterInput.SeniorityEntry,
		SeniorityJunior:        filterInput.SeniorityJunior,
		SeniorityMid:           filterInput.SeniorityMid,
		SenioritySenior:        filterInput.SenioritySenior,
		SeniorityLead:          filterInput.SeniorityLead,
		HasUser:                filterInput.HasUser,
		UserActionFilter:       filterInput.UserActionFilter,
		UserID:                 filterInput.UserID,
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

func (h *Handler) companiesSitemap(c *gin.Context) {
	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 500), 1)
	if perPage > 50000 {
		perPage = 50000
	}
	offset := (page - 1) * perPage
	totalCount, err := h.q.CountCompaniesWithJobsForSitemap(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies sitemap"})
		return
	}
	rows, err := h.q.ListCompanySitemapPage(c.Request.Context(), gensqlc.ListCompanySitemapPageParams{
		Limit:  int32(perPage),
		Offset: int32(offset),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies sitemap"})
		return
	}
	items := []companySitemapItem{}
	for _, row := range rows {
		slugValue := strings.TrimSpace(pgTextString(row.Slug))
		if slugValue == "" {
			continue
		}
		items = append(items, companySitemapItem{
			Slug:              slugValue,
			Name:              pgTextPtr(row.Name),
			LatestJobPostedAt: timestamptzStringPtr(row.LatestJobPostedAt),
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"page":     page,
		"per_page": perPage,
		"total":    totalCount,
		"items":    items,
	})
}

func (h *Handler) companyProfile(c *gin.Context) {
	slug := strings.ToLower(strings.TrimSpace(c.Param("companySlug")))
	if slug == "" {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Company not found"})
		return
	}
	row, err := h.q.GetCompanyProfileBySlug(c.Request.Context(), pgtype.Text{String: slug, Valid: true})
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Company not found"})
		return
	}
	item := companyProfileItem{
		ID:            int64(row.ID),
		Slug:          strings.TrimSpace(pgTextString(row.Slug)),
		Name:          pgTextPtr(row.Name),
		Tagline:       pgTextPtr(row.Tagline),
		ProfilePicURL: pgTextPtr(row.ProfilePicUrl),
		HomePageURL:   pgTextPtr(row.HomePageUrl),
		LinkedInURL:   pgTextPtr(row.LinkedinUrl),
		EmployeeRange: pgTextPtr(row.EmployeeRange),
		FoundedYear:   pgTextPtr(row.FoundedYear),
		SponsorsH1B:   pgBoolPtr(row.SponsorsH1b),
	}
	if len(row.IndustrySpecialities) > 0 {
		_ = json.Unmarshal(row.IndustrySpecialities, &item.IndustrySpecialies)
	}
	stats, err := h.q.GetCompanyProfileStats(c.Request.Context(), pgtype.Int4{Int32: row.ID, Valid: true})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load company profile"})
		return
	}
	item.TotalJobs = stats.TotalJobs
	item.LatestJobPostedAt = timestamptzStringPtr(stats.LatestJobPostedAt)
	c.JSON(http.StatusOK, item)
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
	location := strings.TrimSpace(c.Query("location"))
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)

	locationPattern := "%" + location + "%"
	rows, err := h.q.ListTopCategories(c.Request.Context(), gensqlc.ListTopCategoriesParams{
		CreatedAtSource:   pgtype.Timestamptz{Time: cutoff, Valid: true},
		Column2:           location != "",
		LocationUsStates:  []byte(locationPattern),
		LocationCountries: []byte(locationPattern),
		Limit:             int32(limit),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load top categories"})
		return
	}
	items := []gin.H{}
	for _, row := range rows {
		category := strings.TrimSpace(pgTextString(row.CategorizedJobTitle))
		if category != "" {
			items = append(items, gin.H{"category": category, "score": row.Score})
		}
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
	HasTitleFilters        bool
	JobCategories          []string
	JobFunctions           []string
	TitleExactTerms        []string
	TitleLikePatterns      []string
	TitleTokenGroupsJSON   []byte
	CompanyFilter          string
	HasStructuredLocation  bool
	USStates               []string
	Countries              []string
	LocationPatterns       []string
	TechStacks             []string
	EmploymentTypePatterns []string
	HasCreatedFrom         bool
	CreatedFrom            pgtype.Timestamptz
	HasCreatedTo           bool
	CreatedTo              pgtype.Timestamptz
	HasMinSalary           bool
	MinSalary              float64
	HasSeniority           bool
	SeniorityEntry         bool
	SeniorityJunior        bool
	SeniorityMid           bool
	SenioritySenior        bool
	SeniorityLead          bool
	HasUser                bool
	UserActionFilter       string
	UserID                 int64
	SortSalary             bool
}

func (h *Handler) buildListingFilterInput(c *gin.Context, currentUser *auth.User, includePostDate bool) listingFilterInput {
	input := listingFilterInput{
		JobCategories:          []string{},
		JobFunctions:           []string{},
		TitleExactTerms:        []string{},
		TitleLikePatterns:      []string{},
		TitleTokenGroupsJSON:   []byte("[]"),
		USStates:               []string{},
		Countries:              []string{},
		LocationPatterns:       []string{},
		TechStacks:             []string{},
		EmploymentTypePatterns: []string{},
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
		input.TitleExactTerms = append(input.TitleExactTerms, strings.ToLower(normalizedTitle))
		input.TitleLikePatterns = append(input.TitleLikePatterns, "%"+normalizedTitle+"%")
		if tokens := uniqueStrings(tokenizeTitleSearchText(normalizedTitle)); len(tokens) > 0 {
			titleTokenGroups = append(titleTokenGroups, tokens)
		}
	}
	input.TitleExactTerms = uniqueStrings(input.TitleExactTerms)
	input.TitleLikePatterns = uniqueStrings(input.TitleLikePatterns)
	if payload, err := json.Marshal(titleTokenGroups); err == nil {
		input.TitleTokenGroupsJSON = payload
	}
	input.HasTitleFilters = len(input.JobCategories) > 0 || len(input.JobFunctions) > 0 || len(input.TitleExactTerms) > 0 || len(input.TitleLikePatterns) > 0

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
	input.HasStructuredLocation = len(input.USStates) > 0 || len(input.Countries) > 0

	if !input.HasStructuredLocation {
		locations := []string{}
		rawLocationValue := strings.TrimSpace(c.Query("location"))
		if rawLocationValue != "" {
			rawParts := []string{}
			for _, part := range strings.Split(rawLocationValue, ",") {
				trimmed := strings.TrimSpace(part)
				if trimmed != "" {
					rawParts = append(rawParts, trimmed)
				}
			}
			if len(rawParts) == 2 && strings.EqualFold(rawParts[1], "United States") {
				locations = []string{rawLocationValue}
			} else {
				locations = parseCSVQuery(c.Query("location"))
			}
		}
		for _, location := range uniqueStrings(expandLocationQueryTerms(locations)) {
			if strings.TrimSpace(location) == "" {
				continue
			}
			input.LocationPatterns = append(input.LocationPatterns, "%"+location+"%")
		}
	}

	input.CompanyFilter = strings.ToLower(strings.TrimSpace(c.Query("company")))

	for _, tech := range uniqueStrings(parseCSVQuery(c.Query("tech_stack"))) {
		if trimmed := strings.TrimSpace(tech); trimmed != "" {
			input.TechStacks = append(input.TechStacks, trimmed)
		}
	}
	for _, employment := range uniqueStrings(parseCSVQuery(c.Query("employment_type"))) {
		if trimmed := strings.TrimSpace(employment); trimmed != "" {
			input.EmploymentTypePatterns = append(input.EmploymentTypePatterns, "%"+trimmed+"%")
		}
	}

	if postDateFrom := strings.TrimSpace(c.Query("post_date_from")); postDateFrom != "" {
		if cutoff, ok := parsePostDateFrom(postDateFrom); ok {
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: cutoff, Valid: true}
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
	var roleTitle, summary, companyName, companySlug, companyTagline, companyProfilePicURL, companyHomePageURL, companyLinkedInURL, companyEmployeeRange, companyFoundedYear, categorizedTitle, locationCity, locationType, locationUSStates, locationCountries, employmentType, salaryType, techStack, updatedAt, createdAt, url sql.NullString
	var companySponsorsH1B, isEntry, isJunior, isMid, isSenior, isLead sql.NullBool
	var salaryMin, salaryMax, salaryMinUSD, salaryMaxUSD sql.NullFloat64
	err := scanner.Scan(&item.ID, &item.RawUSJobID, &roleTitle, &summary, &companyName, &companySlug, &companyTagline, &companyProfilePicURL, &companyHomePageURL, &companyLinkedInURL, &companyEmployeeRange, &companyFoundedYear, &companySponsorsH1B, &categorizedTitle, &locationCity, &locationType, &locationUSStates, &locationCountries, &employmentType, &salaryMin, &salaryMax, &salaryMinUSD, &salaryMaxUSD, &salaryType, &isEntry, &isJunior, &isMid, &isSenior, &isLead, &techStack, &updatedAt, &createdAt, &url)
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
	item.LocationCity = nullableString(locationCity)
	item.LocationType = nullableString(locationType)
	item.EmploymentType = nullableString(employmentType)
	item.SalaryMin = nullableFloatPtr(salaryMin)
	item.SalaryMax = nullableFloatPtr(salaryMax)
	item.SalaryMinUSD = nullableFloatPtr(salaryMinUSD)
	item.SalaryMaxUSD = nullableFloatPtr(salaryMaxUSD)
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
	var companyName, companySlug, companyTagline, companyProfilePicURL, companyHomePageURL, companyLinkedInURL, companyEmployeeRange, companyFoundedYear, categorizedTitle, categorizedFunction, roleTitle, locationCity, locationType, locationUSStates, locationCountries, employmentType, salaryType, updatedAt, createdAt, roleDescription, roleRequirements, educationCategory, requiredLanguages, techStack, benefits, url sql.NullString
	var companySponsorsH1B, experienceInPlaceOfEducation sql.NullBool
	var salaryMin, salaryMax, salaryMinUSD, salaryMaxUSD sql.NullFloat64
	var experienceRequirementsMonths sql.NullInt64
	err := scanner.Scan(&detail.ID, &detail.RawUSJobID, &companyName, &companySlug, &companyTagline, &companyProfilePicURL, &companyHomePageURL, &companyLinkedInURL, &companyEmployeeRange, &companyFoundedYear, &companySponsorsH1B, &categorizedTitle, &categorizedFunction, &roleTitle, &locationCity, &locationType, &locationUSStates, &locationCountries, &employmentType, &salaryMin, &salaryMax, &salaryMinUSD, &salaryMaxUSD, &salaryType, &updatedAt, &createdAt, &roleDescription, &roleRequirements, &educationCategory, &experienceRequirementsMonths, &experienceInPlaceOfEducation, &requiredLanguages, &techStack, &benefits, &url)
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
		SalaryType:                              pgTextPtr(row.SalaryType),
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
