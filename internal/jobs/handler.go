package jobs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	cfg  config.Config
	db   *database.DB
	auth *auth.Handler
}

const (
	defaultSalaryType = "yearly"
	minSalaryStart    = 30000
	minSalaryEnd      = 300000
	minSalaryStep     = 10000
	hoursPerYear      = 2080.0
	daysPerYear       = 260.0
	weeksPerYear      = 52.0
	biweeksPerYear    = 26.0
	monthsPerYear     = 12.0
	minutesPerYear    = hoursPerYear * 60.0
)

var (
	postDateOptions = []string{"24_hours", "48_hours", "3_days", "week", "month", "3_months"}
	postDateWindows = map[string]time.Duration{
		"24_hours": 24 * time.Hour,
		"48_hours": 48 * time.Hour,
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
	Location              *string    `json:"location"`
	LocationCity          *string    `json:"location_city"`
	LocationType          *string    `json:"location_type"`
	LocationUSStates      []string   `json:"location_us_states"`
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
	Location                                *string  `json:"location"`
	LocationCity                            *string  `json:"location_city"`
	LocationType                            *string  `json:"location_type"`
	LocationUSStates                        []string `json:"location_us_states"`
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

func NewHandler(cfg config.Config, db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{cfg: cfg, db: db, auth: authHandler}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/jobs/filter-options", h.filterOptions)
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
	return `COALESCE(salary_min_usd, ` + annualizedSalarySQL(`salary_min`) + `)`
}

func annualizedMaxSalarySQL() string {
	return `COALESCE(salary_max_usd, ` + annualizedSalarySQL(`salary_max`) + `, salary_min_usd, ` + annualizedSalarySQL(`salary_min`) + `)`
}

func minSalaryFilterSQL() string {
	return `(p.salary_min_usd >= ? OR (p.salary_min_usd IS NULL AND ` + annualizedSalarySQL(`p.salary_min`) + ` >= ?))`
}

func distinctNonEmptyStrings(c *gin.Context, db *database.DB, column string) ([]string, error) {
	return selectStrings(c, db, `SELECT DISTINCT `+column+` FROM parsed_jobs WHERE `+column+` IS NOT NULL AND `+column+` != '' ORDER BY `+column+` ASC`)
}

func (h *Handler) filterOptions(c *gin.Context) {
	categories, err := distinctNonEmptyStrings(c, h.db, "categorized_job_title")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}

	locationSet := map[string]struct{}{}
	for _, column := range []string{"location_city", "location"} {
		values, err := distinctNonEmptyStrings(c, h.db, column)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
			return
		}
		for _, value := range values {
			locationSet[value] = struct{}{}
		}
	}
	locations := make([]string, 0, len(locationSet))
	for value := range locationSet {
		locations = append(locations, value)
	}
	sortStrings(locations)

	techStackSet := map[string]struct{}{}
	rows, err := h.db.SQL.QueryContext(c.Request.Context(), `SELECT tech_stack FROM parsed_jobs WHERE tech_stack IS NOT NULL AND tech_stack != ''`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}
	defer rows.Close()
	for rows.Next() {
		var rawTechStack sql.NullString
		if err := rows.Scan(&rawTechStack); err != nil || !rawTechStack.Valid || rawTechStack.String == "" {
			continue
		}
		var values []string
		if err := json.Unmarshal([]byte(rawTechStack.String), &values); err != nil {
			continue
		}
		for _, value := range values {
			trimmed := strings.TrimSpace(value)
			if trimmed != "" {
				techStackSet[trimmed] = struct{}{}
			}
		}
	}
	techStacks := make([]string, 0, len(techStackSet))
	for value := range techStackSet {
		techStacks = append(techStacks, value)
	}
	sortStrings(techStacks)
	employmentTypes, err := distinctNonEmptyStrings(c, h.db, "employment_type")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}

	minSalaryOptions := []int{}
	for salary := minSalaryStart; salary <= minSalaryEnd; salary += minSalaryStep {
		minSalaryOptions = append(minSalaryOptions, salary)
	}

	seniorities := []string{"entry", "junior", "mid", "senior", "lead"}

	c.JSON(http.StatusOK, gin.H{
		"job_categories":     categories,
		"locations":          locations,
		"employment_types":   employmentTypes,
		"post_date_options":  postDateOptions,
		"tech_stacks":        techStacks,
		"min_salary_options": minSalaryOptions,
		"seniorities":        seniorities,
	})
}

func (h *Handler) jobDetail(c *gin.Context) {
	row := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT p.id, p.raw_us_job_id, c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b, p.categorized_job_title, p.role_title, p.location, p.location_city, p.location_type, p.location_us_states, p.employment_type, p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_type, p.updated_at, p.created_at_source, p.role_description, p.role_requirements, p.education_requirements_credential_category, p.experience_requirements_months, p.experience_in_place_of_education, p.required_languages, p.tech_stack, p.benefits, p.url FROM parsed_jobs p LEFT JOIN parsed_companies c ON c.id = p.company_id WHERE p.id = ? LIMIT 1`, c.Param("jobID"))
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

	filters := []string{}
	args := []any{}
	if titles := parseCSVQuery(c.Query("job_title")); len(titles) > 0 {
		titles = uniqueStrings(titles)
		parts := make([]string, 0, len(titles))
		normalizedTitleSet := map[string]struct{}{}
		for _, title := range titles {
			normalizedTitle := strings.ToLower(strings.TrimSpace(title))
			normalizedTitleSet[normalizedTitle] = struct{}{}
			titleParts := []string{
				`lower(trim(COALESCE(p.role_title, ''))) = ?`,
				`lower(trim(COALESCE(p.categorized_job_function, ''))) = ?`,
				`p.categorized_job_title LIKE ?`,
				`p.role_title LIKE ?`,
			}
			args = append(args, normalizedTitle)
			args = append(args, normalizedTitle)
			args = append(args, "%"+title+"%")
			args = append(args, "%"+title+"%")
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
		if len(normalizedTitleSet) > 0 {
			normalizedTitles := make([]string, 0, len(normalizedTitleSet))
			for normalizedTitle := range normalizedTitleSet {
				normalizedTitles = append(normalizedTitles, normalizedTitle)
			}
			sortStrings(normalizedTitles)
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if locations := parseCSVQuery(c.Query("location")); len(locations) > 0 {
		locations = uniqueStrings(locations)
		parts := make([]string, 0, len(locations))
		for _, location := range locations {
			parts = append(parts, `(p.location LIKE ? OR p.location_city LIKE ? OR p.location_us_states LIKE ?)`)
			args = append(args, "%"+location+"%", "%"+location+"%", "%"+location+"%")
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
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
	if postDate := strings.ToLower(strings.TrimSpace(c.Query("post_date"))); postDate != "" {
		if window, ok := postDateWindows[postDate]; ok {
			cutoff := time.Now().UTC().Add(-window)
			filters = append(filters, `p.created_at_source >= ?`)
			args = append(args, cutoff.Format(time.RFC3339Nano))
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

	where := ""
	if len(filters) > 0 {
		where = " WHERE " + strings.Join(filters, " AND ")
	}

	var rawTotal int
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(p.id) FROM parsed_jobs p`+where, args...).Scan(&rawTotal); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
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
			`SELECT p.id, p.raw_us_job_id, p.role_title, p.job_description_summary, c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b, p.categorized_job_title, p.location, p.location_city, p.location_type, p.location_us_states, p.employment_type, p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_type, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead, p.tech_stack, p.updated_at, p.created_at_source, p.url
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
		"is_preview":       isPreview,
		"requires_login":   currentUser == nil && isPreview && rawTotal > len(items),
		"requires_upgrade": currentUser != nil && isPreview && rawTotal > len(items),
		"items":            items,
	})
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
	var roleTitle, summary, companyName, companySlug, companyTagline, companyProfilePicURL, companyHomePageURL, companyLinkedInURL, companyEmployeeRange, companyFoundedYear, categorizedTitle, location, locationCity, locationType, locationUSStates, employmentType, salaryType, techStack, updatedAt, createdAt, url sql.NullString
	var companySponsorsH1B, isEntry, isJunior, isMid, isSenior, isLead sql.NullBool
	var salaryMin, salaryMax, salaryMinUSD, salaryMaxUSD sql.NullFloat64
	err := scanner.Scan(&item.ID, &item.RawUSJobID, &roleTitle, &summary, &companyName, &companySlug, &companyTagline, &companyProfilePicURL, &companyHomePageURL, &companyLinkedInURL, &companyEmployeeRange, &companyFoundedYear, &companySponsorsH1B, &categorizedTitle, &location, &locationCity, &locationType, &locationUSStates, &employmentType, &salaryMin, &salaryMax, &salaryMinUSD, &salaryMaxUSD, &salaryType, &isEntry, &isJunior, &isMid, &isSenior, &isLead, &techStack, &updatedAt, &createdAt, &url)
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
	item.Location = nullableString(location)
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
	return item, nil
}

func scanJobDetail(scanner interface{ Scan(dest ...any) error }) (jobDetail, error) {
	var detail jobDetail
	var companyName, companySlug, companyTagline, companyProfilePicURL, companyHomePageURL, companyLinkedInURL, companyEmployeeRange, companyFoundedYear, categorizedTitle, roleTitle, location, locationCity, locationType, locationUSStates, employmentType, salaryType, updatedAt, createdAt, roleDescription, roleRequirements, educationCategory, requiredLanguages, techStack, benefits, url sql.NullString
	var companySponsorsH1B, experienceInPlaceOfEducation sql.NullBool
	var salaryMin, salaryMax, salaryMinUSD, salaryMaxUSD sql.NullFloat64
	var experienceRequirementsMonths sql.NullInt64
	err := scanner.Scan(&detail.ID, &detail.RawUSJobID, &companyName, &companySlug, &companyTagline, &companyProfilePicURL, &companyHomePageURL, &companyLinkedInURL, &companyEmployeeRange, &companyFoundedYear, &companySponsorsH1B, &categorizedTitle, &roleTitle, &location, &locationCity, &locationType, &locationUSStates, &employmentType, &salaryMin, &salaryMax, &salaryMinUSD, &salaryMaxUSD, &salaryType, &updatedAt, &createdAt, &roleDescription, &roleRequirements, &educationCategory, &experienceRequirementsMonths, &experienceInPlaceOfEducation, &requiredLanguages, &techStack, &benefits, &url)
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
	detail.Location = nullableString(location)
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
