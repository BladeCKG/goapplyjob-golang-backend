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

	"github.com/gin-gonic/gin"
)

type Handler struct {
	cfg                       config.Config
	db                        *database.DB
	auth                      *auth.Handler
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
			if function != "" && !strings.EqualFold(title, function) {
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
			state = cleanFilterLabel(state)
			if !isValidLocationOption(state) {
				continue
			}
			if _, ok := stateToCountries[state]; !ok {
				stateToCountries[state] = map[string]struct{}{}
			}
			for _, country := range countries {
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
	var maxID sql.NullInt64
	err := h.db.SQL.QueryRowContext(ctx, `SELECT MAX(id) FROM parsed_jobs`).Scan(&maxID)
	return maxID, err
}

func parsedJobFilterRowsEqual(left, right sql.NullInt64) bool {
	if !left.Valid && !right.Valid {
		return true
	}
	return left.Valid == right.Valid && left.Int64 == right.Int64
}

func (h *Handler) refreshFilterCache(ctx context.Context) error {
	rows, err := h.db.SQL.QueryContext(ctx,
		`SELECT categorized_job_title, categorized_job_function, location_us_states, location_countries, tech_stack, employment_type
		 FROM parsed_jobs
		 WHERE categorized_job_title IS NOT NULL
		    OR categorized_job_function IS NOT NULL
		    OR location_us_states IS NOT NULL
		    OR location_countries IS NOT NULL
		    OR tech_stack IS NOT NULL
		    OR employment_type IS NOT NULL`)
	if err != nil {
		return err
	}
	defer rows.Close()

	categoryRows := [][2]string{}
	locationRows := [][2][]string{}
	techRows := [][]string{}
	employmentRows := []string{}
	for rows.Next() {
		var categorizedJobTitle, categorizedJobFunction, locationUSStates, locationCountries, techStack, employmentType sql.NullString
		if err := rows.Scan(&categorizedJobTitle, &categorizedJobFunction, &locationUSStates, &locationCountries, &techStack, &employmentType); err != nil {
			continue
		}
		if categorizedJobTitle.Valid || categorizedJobFunction.Valid {
			categoryRows = append(categoryRows, [2]string{categorizedJobTitle.String, categorizedJobFunction.String})
		}
		if locationUSStates.Valid || locationCountries.Valid {
			states := []string{}
			countries := []string{}
			if locationUSStates.Valid && strings.TrimSpace(locationUSStates.String) != "" {
				_ = json.Unmarshal([]byte(locationUSStates.String), &states)
			}
			if locationCountries.Valid && strings.TrimSpace(locationCountries.String) != "" {
				_ = json.Unmarshal([]byte(locationCountries.String), &countries)
			}
			locationRows = append(locationRows, [2][]string{states, countries})
		}
		if techStack.Valid && strings.TrimSpace(techStack.String) != "" {
			values := []string{}
			if err := json.Unmarshal([]byte(techStack.String), &values); err == nil {
				techRows = append(techRows, values)
			}
		}
		if employmentType.Valid {
			employmentRows = append(employmentRows, employmentType.String)
		}
	}
	h.filterCache.jobCategoryParents = buildJobCategoryParentsMap(categoryRows)
	h.filterCache.locationParents = buildLocationParentsMap(locationRows)
	h.filterCache.techStacks = buildTechStacks(techRows)
	h.filterCache.employmentTypes = buildEmploymentTypes(employmentRows)
	return nil
}

func (h *Handler) ensureFilterCacheFresh(ctx context.Context, force bool) error {
	h.filterCache.mu.Lock()
	defer h.filterCache.mu.Unlock()
	if !force && !h.filterCache.lastRefreshAt.IsZero() {
		if time.Since(h.filterCache.lastRefreshAt) < time.Duration(h.filterCacheRefreshSeconds)*time.Second {
			return nil
		}
	}
	maxID, err := h.getMaxParsedJobID(ctx)
	if err != nil {
		return err
	}
	if !force && parsedJobFilterRowsEqual(h.filterCache.maxParsedJobID, maxID) {
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
	h.scheduleFilterCacheRefresh(false)
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
	row := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT p.id, p.raw_us_job_id, c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b, p.categorized_job_title, p.role_title, p.location_city, p.location_type, p.location_us_states, p.location_countries, p.employment_type, p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_type, p.updated_at, p.created_at_source, p.role_description, p.role_requirements, p.education_requirements_credential_category, p.experience_requirements_months, p.experience_in_place_of_education, p.required_languages, p.tech_stack, p.benefits, p.url FROM parsed_jobs p LEFT JOIN parsed_companies c ON c.id = p.company_id WHERE p.id = ? LIMIT 1`, c.Param("jobID"))
	detail, err := scanJobDetail(row)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Job not found"})
		return
	}
	c.JSON(http.StatusOK, detail)
}

func (h *Handler) listJobs(c *gin.Context) {
	currentUser := h.auth.OptionalCurrentUser(c)
	hasFullAccess := false
	if currentUser != nil {
		var activeID int64
		err := h.db.SQL.QueryRowContext(
			c.Request.Context(),
			`SELECT s.id
			 FROM user_subscriptions s
			 JOIN pricing_plans p ON p.id = s.pricing_plan_id
			 WHERE s.user_id = ? AND s.ends_at > ? AND p.is_active = 1
			 ORDER BY s.ends_at DESC
			 LIMIT 1`,
			currentUser.ID,
			time.Now().UTC().Format(time.RFC3339Nano),
		).Scan(&activeID)
		hasFullAccess = err == nil
	}
	isPreview := !hasFullAccess
	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 20), 1)
	if perPage > 100 {
		perPage = 100
	}

	exactCandidateValues := []string{}
	exactCandidateValues = append(exactCandidateValues, parseCSVQuery(c.Query("job_categories"))...)
	exactCandidateValues = append(exactCandidateValues, parseCSVQuery(c.Query("job_functions"))...)
	if len(exactCandidateValues) == 0 {
		exactCandidateValues = append(exactCandidateValues, parseCSVQuery(c.Query("job_title"))...)
	}
	exactTitleValues, _ := h.resolveExactJobTitleValues(c, exactCandidateValues)
	filters, args := h.buildJobFilters(c, currentUser, true, exactTitleValues)

	where := ""
	if len(filters) > 0 {
		where = " WHERE " + strings.Join(filters, " AND ")
	}

	var rawTotal, companyCount int
	hasCompanyFilter := strings.TrimSpace(c.Query("company")) != ""
	if hasCompanyFilter {
		countQuery := `SELECT COUNT(p.id) FROM parsed_jobs p` + where
		if err := h.db.SQL.QueryRowContext(c.Request.Context(), countQuery, args...).Scan(&rawTotal); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
			return
		}
		if rawTotal > 0 {
			companyCount = 1
		}
	} else {
		countQuery := `SELECT COUNT(p.id), COUNT(DISTINCT p.company_id) FROM parsed_jobs p` + where
		if err := h.db.SQL.QueryRowContext(c.Request.Context(), countQuery, args...).Scan(&rawTotal, &companyCount); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
			return
		}
	}

	previewPerPage := max(h.cfg.PublicJobsMaxPerPage, 1)
	idOrderBy := `created_at_source DESC, id DESC`
	if c.DefaultQuery("sort_criteria", "date") == "salary" {
		idOrderBy = `COALESCE(salary_max_usd, salary_min_usd) DESC, ` + annualizedSalarySQL(`COALESCE(salary_max, salary_min)`) + ` DESC, id DESC`
	}

	pageOut := page
	perPageOut := perPage
	offset := (page - 1) * perPage
	limit := perPage
	if isPreview {
		pageOut = 1
		perPageOut = previewPerPage
		offset = 0
		limit = min(rawTotal, previewPerPage)
	}

	idArgs := append([]any{}, args...)
	idArgs = append(idArgs, limit, offset)
	idRows, err := h.db.SQL.QueryContext(
		c.Request.Context(),
		`SELECT p.id
		 FROM parsed_jobs p`+where+`
		 ORDER BY `+idOrderBy+`
		 LIMIT ? OFFSET ?`,
		idArgs...,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	defer idRows.Close()

	pagedIDs := []int64{}
	for idRows.Next() {
		var id int64
		if err := idRows.Scan(&id); err == nil {
			pagedIDs = append(pagedIDs, id)
		}
	}

	items := []jobItem{}
	if len(pagedIDs) > 0 {
		placeholders := make([]string, 0, len(pagedIDs))
		detailArgs := make([]any, 0, len(pagedIDs))
		for _, id := range pagedIDs {
			placeholders = append(placeholders, "?")
			detailArgs = append(detailArgs, id)
		}
		rows, err := h.db.SQL.QueryContext(
			c.Request.Context(),
			`SELECT p.id, p.raw_us_job_id, p.role_title, p.job_description_summary, c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b, p.categorized_job_title, p.location_city, p.location_type, p.location_us_states, p.location_countries, p.employment_type, p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_type, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead, p.tech_stack, p.updated_at, p.created_at_source, p.url
			 FROM parsed_jobs p
			 LEFT JOIN parsed_companies c ON c.id = p.company_id
			 WHERE p.id IN (`+strings.Join(placeholders, ",")+`)`,
			detailArgs...,
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
			return
		}
		defer rows.Close()

		rowsByID := map[int64]jobItem{}
		for rows.Next() {
			item, err := scanJob(rows)
			if err == nil {
				rowsByID[item.ID] = item
			}
		}
		for _, id := range pagedIDs {
			if item, ok := rowsByID[id]; ok {
				items = append(items, item)
			}
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
	exactCandidateValues := []string{}
	exactCandidateValues = append(exactCandidateValues, parseCSVQuery(c.Query("job_categories"))...)
	exactCandidateValues = append(exactCandidateValues, parseCSVQuery(c.Query("job_functions"))...)
	if len(exactCandidateValues) == 0 {
		exactCandidateValues = append(exactCandidateValues, parseCSVQuery(c.Query("job_title"))...)
	}
	exactTitleValues, _ := h.resolveExactJobTitleValues(c, exactCandidateValues)
	filters, args := h.buildJobFilters(c, currentUser, false, exactTitleValues)
	where := ""
	if len(filters) > 0 {
		where = " WHERE " + strings.Join(filters, " AND ")
	}
	now := time.Now().UTC()
	todayCutoff := now.Add(-24 * time.Hour).Format(time.RFC3339Nano)
	lastHourCutoff := now.Add(-1 * time.Hour).Format(time.RFC3339Nano)

	todayArgs := append(append([]any{}, args...), todayCutoff)
	lastHourArgs := append(append([]any{}, args...), lastHourCutoff)
	companyArgs := append(append([]any{}, args...), todayCutoff)

	var jobsToday, jobsLastHour, companiesHiringNow int
	if err := h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT COUNT(p.id) FROM parsed_jobs p`+where+appendWhere(where, `p.created_at_source IS NOT NULL AND p.created_at_source >= ?`),
		todayArgs...).Scan(&jobsToday); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs metrics"})
		return
	}
	if err := h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT COUNT(p.id) FROM parsed_jobs p`+where+appendWhere(where, `p.created_at_source IS NOT NULL AND p.created_at_source >= ?`),
		lastHourArgs...).Scan(&jobsLastHour); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs metrics"})
		return
	}
	if err := h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT COUNT(DISTINCT p.company_id) FROM parsed_jobs p`+where+appendWhere(where, `p.company_id IS NOT NULL AND p.created_at_source IS NOT NULL AND p.created_at_source >= ?`),
		companyArgs...).Scan(&companiesHiringNow); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs metrics"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"jobs_today":           jobsToday,
		"jobs_last_hour":       jobsLastHour,
		"companies_hiring_now": companiesHiringNow,
	})
}

func (h *Handler) sitemap(c *gin.Context) {
	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 500), 1)
	if perPage > 50000 {
		perPage = 50000
	}
	offset := (page - 1) * perPage
	var total int
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(id) FROM parsed_jobs`).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs sitemap"})
		return
	}
	rows, err := h.db.SQL.QueryContext(c.Request.Context(),
		`SELECT p.id, p.role_title, p.categorized_job_title, c.name, p.created_at_source
		 FROM parsed_jobs p
		 LEFT JOIN parsed_companies c ON c.id = p.company_id
		 ORDER BY p.created_at_source DESC, p.id DESC
		 LIMIT ? OFFSET ?`, perPage, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs sitemap"})
		return
	}
	defer rows.Close()
	items := []jobSitemapItem{}
	for rows.Next() {
		var item jobSitemapItem
		var roleTitle, categorizedTitle, companyName, createdAt sql.NullString
		if err := rows.Scan(&item.ID, &roleTitle, &categorizedTitle, &companyName, &createdAt); err == nil {
			item.RoleTitle = nullableString(roleTitle)
			item.CategorizedTitle = nullableString(categorizedTitle)
			item.CompanyName = nullableString(companyName)
			item.CreatedAtSource = nullableString(createdAt)
			items = append(items, item)
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"page":     page,
		"per_page": perPage,
		"total":    total,
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
	var total int
	if err := h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT COUNT(DISTINCT c.id)
		 FROM parsed_companies c
		 JOIN parsed_jobs p ON p.company_id = c.id
		 WHERE c.slug IS NOT NULL AND trim(c.slug) != ''`,
	).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies sitemap"})
		return
	}
	rows, err := h.db.SQL.QueryContext(
		c.Request.Context(),
		`SELECT c.slug, c.name, MAX(p.created_at_source) AS latest_job_posted_at
		 FROM parsed_companies c
		 JOIN parsed_jobs p ON p.company_id = c.id
		 WHERE c.slug IS NOT NULL AND trim(c.slug) != ''
		 GROUP BY c.id, c.slug, c.name
		 ORDER BY latest_job_posted_at DESC, c.id DESC
		 LIMIT ? OFFSET ?`,
		perPage, offset,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies sitemap"})
		return
	}
	defer rows.Close()
	items := []companySitemapItem{}
	for rows.Next() {
		var slug sql.NullString
		var name sql.NullString
		var latest sql.NullString
		if err := rows.Scan(&slug, &name, &latest); err != nil {
			continue
		}
		slugValue := strings.TrimSpace(slug.String)
		if !slug.Valid || slugValue == "" {
			continue
		}
		items = append(items, companySitemapItem{
			Slug:              slugValue,
			Name:              nullableString(name),
			LatestJobPostedAt: nullableString(latest),
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"page":     page,
		"per_page": perPage,
		"total":    total,
		"items":    items,
	})
}

func (h *Handler) companyProfile(c *gin.Context) {
	slug := strings.ToLower(strings.TrimSpace(c.Param("companySlug")))
	if slug == "" {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Company not found"})
		return
	}
	row := h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT id, slug, name, tagline, profile_pic_url, home_page_url, linkedin_url, employee_range, founded_year, sponsors_h1b, industry_specialities
		 FROM parsed_companies
		 WHERE lower(trim(COALESCE(slug, ''))) = ?
		 LIMIT 1`,
		slug,
	)
	var item companyProfileItem
	var itemSlug sql.NullString
	var name, tagline, profilePicURL, homePageURL, linkedInURL, employeeRange, foundedYear, industrySpecialities sql.NullString
	var sponsorsH1B sql.NullBool
	if err := row.Scan(
		&item.ID,
		&itemSlug,
		&name,
		&tagline,
		&profilePicURL,
		&homePageURL,
		&linkedInURL,
		&employeeRange,
		&foundedYear,
		&sponsorsH1B,
		&industrySpecialities,
	); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Company not found"})
		return
	}
	item.Slug = strings.TrimSpace(itemSlug.String)
	item.Name = nullableString(name)
	item.Tagline = nullableString(tagline)
	item.ProfilePicURL = nullableString(profilePicURL)
	item.HomePageURL = nullableString(homePageURL)
	item.LinkedInURL = nullableString(linkedInURL)
	item.EmployeeRange = nullableString(employeeRange)
	item.FoundedYear = nullableString(foundedYear)
	item.SponsorsH1B = nullableBool(sponsorsH1B)
	if industrySpecialities.Valid && strings.TrimSpace(industrySpecialities.String) != "" {
		_ = json.Unmarshal([]byte(industrySpecialities.String), &item.IndustrySpecialies)
	}
	statsRow := h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT COUNT(id), MAX(created_at_source)
		 FROM parsed_jobs
		 WHERE company_id = ?`,
		item.ID,
	)
	var latestJobPostedAt sql.NullString
	if err := statsRow.Scan(&item.TotalJobs, &latestJobPostedAt); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load company profile"})
		return
	}
	item.LatestJobPostedAt = nullableString(latestJobPostedAt)
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

	var topFunction sql.NullString
	if err := h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT categorized_job_function
		 FROM parsed_jobs
		 WHERE categorized_job_title = ?
		   AND categorized_job_function IS NOT NULL
		   AND categorized_job_function != ''
		 GROUP BY categorized_job_function
		 ORDER BY COUNT(id) DESC, categorized_job_function ASC
		 LIMIT 1`, category).Scan(&topFunction); err != nil || !topFunction.Valid || strings.TrimSpace(topFunction.String) == "" {
		c.JSON(http.StatusOK, gin.H{"items": []any{}})
		return
	}
	rows, err := h.db.SQL.QueryContext(c.Request.Context(),
		`SELECT categorized_job_title, COUNT(id) AS score
		 FROM parsed_jobs
		 WHERE categorized_job_title IS NOT NULL
		   AND categorized_job_title != ''
		   AND categorized_job_function = ?
		 GROUP BY categorized_job_title
		 ORDER BY CASE WHEN categorized_job_title = ? THEN 0 ELSE 1 END ASC,
		          score DESC,
		          categorized_job_title ASC
		 LIMIT ?`,
		topFunction.String, category, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load related categories"})
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var itemCategory string
		var score int
		if err := rows.Scan(&itemCategory, &score); err == nil && strings.TrimSpace(itemCategory) != "" {
			items = append(items, gin.H{"category": itemCategory, "score": score})
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
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour).Format(time.RFC3339Nano)

	query := `SELECT categorized_job_title, COUNT(id) AS score
		FROM parsed_jobs
		WHERE categorized_job_title IS NOT NULL
		  AND categorized_job_title != ''
		  AND created_at_source IS NOT NULL
		  AND created_at_source >= ?`
	args := []any{cutoff}
	if location != "" {
		query += ` AND (location_us_states LIKE ? OR location_countries LIKE ?)`
		args = append(args, "%"+location+"%", "%"+location+"%")
	}
	query += ` GROUP BY categorized_job_title ORDER BY score DESC, categorized_job_title ASC LIMIT ?`
	args = append(args, limit)
	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load top categories"})
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var category string
		var score int
		if err := rows.Scan(&category, &score); err == nil && strings.TrimSpace(category) != "" {
			items = append(items, gin.H{"category": category, "score": score})
		}
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func appendWhere(where, predicate string) string {
	if strings.TrimSpace(where) == "" {
		return " WHERE " + predicate
	}
	return " AND " + predicate
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

func (h *Handler) buildJobFilters(c *gin.Context, currentUser *auth.User, includePostDate bool, exactJobTitleValues map[string]struct{}) ([]string, []any) {
	filters := []string{}
	args := []any{}
	titleValues := parseCSVQuery(c.Query("job_titles"))
	if len(titleValues) == 0 {
		titleValues = parseCSVQuery(c.Query("job_title"))
	}
	if len(titleValues) > 0 {
		titles := titleValues
		titles = uniqueStrings(titles)
		parts := make([]string, 0, len(titles))
		for _, title := range titles {
			normalizedTitle := strings.ToLower(strings.TrimSpace(title))
			if normalizedTitle == "" {
				continue
			}
			if _, ok := exactJobTitleValues[normalizedTitle]; ok {
				parts = append(parts, `(lower(trim(COALESCE(p.role_title, ''))) = ? OR lower(trim(COALESCE(p.categorized_job_title, ''))) = ? OR lower(trim(COALESCE(p.categorized_job_function, ''))) = ?)`)
				args = append(args, normalizedTitle, normalizedTitle, normalizedTitle)
				continue
			}
			titleParts := []string{
				`p.categorized_job_title LIKE ?`,
				`p.role_title LIKE ?`,
			}
			args = append(args, "%"+title+"%", "%"+title+"%")
			if tokens := tokenizeTitleSearchText(title); len(tokens) > 0 {
				tokenParts := make([]string, 0, len(tokens))
				for _, token := range tokens {
					tokenParts = append(tokenParts, `p.role_title LIKE ?`)
					args = append(args, "%"+token+"%")
				}
				titleParts = append(titleParts, "("+strings.Join(tokenParts, " AND ")+")")
			}
			parts = append(parts, "("+strings.Join(titleParts, " OR ")+")")
		}
		if len(parts) > 0 {
			filters = append(filters, "("+strings.Join(parts, " OR ")+")")
		}
	}
	if categories := parseCSVQuery(c.Query("job_categories")); len(categories) > 0 {
		categories = uniqueStrings(categories)
		parts := make([]string, 0, len(categories))
		for _, category := range categories {
			parts = append(parts, `lower(trim(COALESCE(p.categorized_job_title, ''))) = ?`)
			args = append(args, strings.ToLower(strings.TrimSpace(category)))
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if functions := parseCSVQuery(c.Query("job_functions")); len(functions) > 0 {
		functions = uniqueStrings(functions)
		parts := make([]string, 0, len(functions))
		for _, function := range functions {
			parts = append(parts, `lower(trim(COALESCE(p.categorized_job_function, ''))) = ?`)
			args = append(args, strings.ToLower(strings.TrimSpace(function)))
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	states := uniqueStrings(parseCSVQuery(c.Query("us_states")))
	countries := uniqueStrings(parseCSVQuery(c.Query("countries")))
	if len(states) > 0 {
		parts := make([]string, 0, len(states))
		for _, state := range states {
			parts = append(parts, `p.location_us_states LIKE ?`)
			args = append(args, "%"+state+"%")
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if len(countries) > 0 {
		parts := make([]string, 0, len(countries))
		for _, country := range countries {
			parts = append(parts, `p.location_countries LIKE ?`)
			args = append(args, "%"+country+"%")
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if len(states) == 0 && len(countries) == 0 {
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
		if len(locations) > 0 {
			locations = uniqueStrings(expandLocationQueryTerms(locations))
			parts := make([]string, 0, len(locations))
			for _, location := range locations {
				parts = append(parts, `(p.location_us_states LIKE ? OR p.location_countries LIKE ?)`)
				args = append(args, "%"+location+"%", "%"+location+"%")
			}
			filters = append(filters, "("+strings.Join(parts, " OR ")+")")
		}
	}
	if normalizedCompany := strings.ToLower(strings.TrimSpace(c.Query("company"))); normalizedCompany != "" {
		subquery := `p.company_id IN (
			SELECT c.id FROM parsed_companies c
			WHERE lower(trim(COALESCE(c.slug, ''))) = ?
			   OR lower(trim(COALESCE(c.name, ''))) = ?
		)`
		filters = append(filters, subquery)
		args = append(args, normalizedCompany, normalizedCompany)
	}
	if techStacks := parseCSVQuery(c.Query("tech_stack")); len(techStacks) > 0 {
		techStacks = uniqueStrings(techStacks)
		parts := make([]string, 0, len(techStacks))
		for _, techStack := range techStacks {
			normalizedStack := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(techStack)), `"`, `\"`)
			parts = append(parts, `lower(COALESCE(p.tech_stack, '')) LIKE ?`)
			args = append(args, `%`+"\""+normalizedStack+"\""+`%`)
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if employmentTypes := parseCSVQuery(c.Query("employment_type")); len(employmentTypes) > 0 {
		employmentTypes = uniqueStrings(employmentTypes)
		parts := make([]string, 0, len(employmentTypes))
		for _, employmentType := range employmentTypes {
			parts = append(parts, `p.employment_type LIKE ?`)
			args = append(args, "%"+employmentType+"%")
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if postDateFrom := strings.TrimSpace(c.Query("post_date_from")); postDateFrom != "" {
		if cutoff, ok := parsePostDateFrom(postDateFrom); ok {
			filters = append(filters, `p.created_at_source >= ?`)
			args = append(args, cutoff.Format(time.RFC3339Nano))
		}
	}
	if includePostDate {
		if postDate := strings.ToLower(strings.TrimSpace(c.Query("post_date"))); postDate != "" {
			now := time.Now().UTC()
			todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
			thisWeekStart := todayStart.AddDate(0, 0, -int(todayStart.Weekday()-time.Monday))
			if todayStart.Weekday() == time.Sunday {
				thisWeekStart = todayStart.AddDate(0, 0, -6)
			}
			thisMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
			lastMonthStart := thisMonthStart.AddDate(0, -1, 0)
			lastWeekStart := thisWeekStart.AddDate(0, 0, -7)

			switch postDate {
			case "today":
				filters = append(filters, `p.created_at_source >= ?`)
				args = append(args, todayStart.Format(time.RFC3339Nano))
			case "yesterday":
				filters = append(filters, `p.created_at_source >= ?`)
				args = append(args, todayStart.AddDate(0, 0, -1).Format(time.RFC3339Nano))
				filters = append(filters, `p.created_at_source < ?`)
				args = append(args, todayStart.Format(time.RFC3339Nano))
			case "this_week":
				filters = append(filters, `p.created_at_source >= ?`)
				args = append(args, thisWeekStart.Format(time.RFC3339Nano))
			case "previous_week":
				filters = append(filters, `p.created_at_source >= ?`)
				args = append(args, lastWeekStart.Format(time.RFC3339Nano))
				filters = append(filters, `p.created_at_source < ?`)
				args = append(args, thisWeekStart.Format(time.RFC3339Nano))
			case "this_month":
				filters = append(filters, `p.created_at_source >= ?`)
				args = append(args, thisMonthStart.Format(time.RFC3339Nano))
			case "previous_month":
				filters = append(filters, `p.created_at_source >= ?`)
				args = append(args, lastMonthStart.Format(time.RFC3339Nano))
				filters = append(filters, `p.created_at_source < ?`)
				args = append(args, thisMonthStart.Format(time.RFC3339Nano))
			default:
				window, ok := postDateWindows[postDate]
				if !ok {
					break
				}
				cutoff := now.Add(-window)
				filters = append(filters, `p.created_at_source >= ?`)
				args = append(args, cutoff.Format(time.RFC3339Nano))
			}
		}
	}
	if minSalary := strings.TrimSpace(c.Query("min_salary")); minSalary != "" {
		if parsed, err := strconv.ParseFloat(minSalary, 64); err == nil {
			filters = append(filters, minSalaryFilterSQL())
			args = append(args, parsed, parsed)
		}
	}
	if seniorities := parseCSVQuery(c.Query("seniority")); len(seniorities) > 0 {
		seniorities = uniqueStrings(seniorities)
		parts := []string{}
		fieldMap := map[string]string{"entry": "p.is_entry_level = 1", "junior": "p.is_junior = 1", "mid": "p.is_mid_level = 1", "senior": "p.is_senior = 1", "lead": "p.is_lead = 1"}
		for _, seniority := range seniorities {
			if _, valid := validSeniorities[seniority]; valid {
				if predicate, ok := fieldMap[seniority]; ok {
					parts = append(parts, predicate)
				}
			}
		}
		if len(parts) > 0 {
			filters = append(filters, "("+strings.Join(parts, " OR ")+")")
		}
	}
	if currentUser != nil {
		actionFilter := strings.ToLower(strings.TrimSpace(c.DefaultQuery("user_job_action", "all")))
		hiddenExists := `EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = ? AND uja.parsed_job_id = p.id AND uja.is_hidden = 1)`
		appliedExists := `EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = ? AND uja.parsed_job_id = p.id AND uja.is_applied = 1)`
		savedExists := `EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = ? AND uja.parsed_job_id = p.id AND uja.is_saved = 1)`
		switch actionFilter {
		case "hidden":
			filters = append(filters, hiddenExists)
			args = append(args, currentUser.ID)
		case "applied":
			filters = append(filters, appliedExists)
			args = append(args, currentUser.ID)
		case "not_applied":
			filters = append(filters, "NOT ("+appliedExists+")")
			args = append(args, currentUser.ID)
		case "saved":
			filters = append(filters, savedExists)
			args = append(args, currentUser.ID)
		default:
			filters = append(filters, "NOT ("+hiddenExists+")")
			args = append(args, currentUser.ID)
		}
	}
	return filters, args
}

func (h *Handler) resolveExactJobTitleValues(c *gin.Context, titleValues []string) (map[string]struct{}, error) {
	normalized := []string{}
	seen := map[string]struct{}{}
	for _, value := range titleValues {
		next := strings.ToLower(strings.TrimSpace(value))
		if next == "" {
			continue
		}
		if _, ok := seen[next]; ok {
			continue
		}
		seen[next] = struct{}{}
		normalized = append(normalized, next)
	}
	matches := map[string]struct{}{}
	if len(normalized) == 0 {
		return matches, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(normalized)), ",")
	args := make([]any, 0, len(normalized)*3)
	for _, value := range normalized {
		args = append(args, value)
	}
	for _, value := range normalized {
		args = append(args, value)
	}
	for _, value := range normalized {
		args = append(args, value)
	}
	query := `SELECT p.role_title, p.categorized_job_title, p.categorized_job_function
		FROM parsed_jobs p
		WHERE lower(trim(COALESCE(p.role_title, ''))) IN (` + placeholders + `)
		   OR lower(trim(COALESCE(p.categorized_job_title, ''))) IN (` + placeholders + `)
		   OR lower(trim(COALESCE(p.categorized_job_function, ''))) IN (` + placeholders + `)`
	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		return matches, err
	}
	defer rows.Close()
	for rows.Next() {
		var roleTitle, jobTitle, jobFunction sql.NullString
		if err := rows.Scan(&roleTitle, &jobTitle, &jobFunction); err != nil {
			continue
		}
		for _, value := range []sql.NullString{roleTitle, jobTitle, jobFunction} {
			if value.Valid && strings.TrimSpace(value.String) != "" {
				matches[strings.ToLower(strings.TrimSpace(value.String))] = struct{}{}
			}
		}
	}
	return matches, nil
}

func selectStrings(c *gin.Context, db *database.DB, query string) ([]string, error) {
	rows, err := db.SQL.QueryContext(c.Request.Context(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err == nil && value != "" {
			values = append(values, value)
		}
	}
	return values, nil
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
	var companyName, companySlug, companyTagline, companyProfilePicURL, companyHomePageURL, companyLinkedInURL, companyEmployeeRange, companyFoundedYear, categorizedTitle, roleTitle, locationCity, locationType, locationUSStates, locationCountries, employmentType, salaryType, updatedAt, createdAt, roleDescription, roleRequirements, educationCategory, requiredLanguages, techStack, benefits, url sql.NullString
	var companySponsorsH1B, experienceInPlaceOfEducation sql.NullBool
	var salaryMin, salaryMax, salaryMinUSD, salaryMaxUSD sql.NullFloat64
	var experienceRequirementsMonths sql.NullInt64
	err := scanner.Scan(&detail.ID, &detail.RawUSJobID, &companyName, &companySlug, &companyTagline, &companyProfilePicURL, &companyHomePageURL, &companyLinkedInURL, &companyEmployeeRange, &companyFoundedYear, &companySponsorsH1B, &categorizedTitle, &roleTitle, &locationCity, &locationType, &locationUSStates, &locationCountries, &employmentType, &salaryMin, &salaryMax, &salaryMinUSD, &salaryMaxUSD, &salaryType, &updatedAt, &createdAt, &roleDescription, &roleRequirements, &educationCategory, &experienceRequirementsMonths, &experienceInPlaceOfEducation, &requiredLanguages, &techStack, &benefits, &url)
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
