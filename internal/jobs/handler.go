package jobs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
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
	return fmt.Sprintf(`(%s) * CASE lower(COALESCE(salary_type, 'yearly')) WHEN 'hourly' THEN 2080.0 WHEN 'hour' THEN 2080.0 WHEN 'hr' THEN 2080.0 WHEN 'daily' THEN 260.0 WHEN 'day' THEN 260.0 WHEN 'weekly' THEN 52.0 WHEN 'week' THEN 52.0 WHEN 'monthly' THEN 12.0 WHEN 'month' THEN 12.0 ELSE 1.0 END`, expr)
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

func buildDenseSalaryOptions(values []float64, step int, window int) []int {
	if len(values) == 0 {
		return nil
	}
	cleaned := make([]float64, 0, len(values))
	for _, value := range values {
		if value > 0 {
			cleaned = append(cleaned, value)
		}
	}
	if len(cleaned) == 0 {
		return nil
	}
	sortFloat64s(cleaned)
	percentile := func(sortedValues []float64, q float64) float64 {
		if len(sortedValues) == 1 {
			return sortedValues[0]
		}
		pos := float64(len(sortedValues)-1) * q
		lo := int(math.Floor(pos))
		hi := int(math.Ceil(pos))
		if lo == hi {
			return sortedValues[lo]
		}
		weight := pos - float64(lo)
		return sortedValues[lo]*(1-weight) + sortedValues[hi]*weight
	}
	p05 := percentile(cleaned, 0.05)
	p95 := percentile(cleaned, 0.95)
	trimmed := make([]float64, 0, len(cleaned))
	for _, value := range cleaned {
		if value >= p05 && value <= p95 {
			trimmed = append(trimmed, value)
		}
	}
	targetValues := cleaned
	if len(trimmed) >= 10 {
		targetValues = trimmed
	}
	binCounts := map[int]int{}
	for _, value := range targetValues {
		binStart := int(math.Floor(value/float64(step))) * step
		binCounts[binStart]++
	}
	binsPerWindow := max(window/step, 1)
	sortedStarts := make([]int, 0, len(binCounts))
	for start := range binCounts {
		sortedStarts = append(sortedStarts, start)
	}
	sortInts(sortedStarts)
	bestStart := sortedStarts[0]
	bestCount := -1
	for _, start := range sortedStarts {
		windowCount := 0
		for i := 0; i < binsPerWindow; i++ {
			windowCount += binCounts[start+(i*step)]
		}
		if windowCount > bestCount {
			bestCount = windowCount
			bestStart = start
		}
	}
	options := make([]int, 0, binsPerWindow+1)
	for i := 0; i <= binsPerWindow; i++ {
		options = append(options, bestStart+(i*step))
	}
	return options
}

func (h *Handler) filterOptions(c *gin.Context) {
	categories, err := selectStrings(c, h.db, `SELECT DISTINCT categorized_job_title FROM parsed_jobs WHERE categorized_job_title IS NOT NULL AND categorized_job_title != '' ORDER BY categorized_job_title ASC`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}

	locationSet := map[string]struct{}{}
	for _, query := range []string{
		`SELECT DISTINCT location_city FROM parsed_jobs WHERE location_city IS NOT NULL AND location_city != ''`,
		`SELECT DISTINCT location FROM parsed_jobs WHERE location IS NOT NULL AND location != ''`,
	} {
		values, err := selectStrings(c, h.db, query)
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
	employmentTypes, err := selectStrings(c, h.db, `SELECT DISTINCT employment_type FROM parsed_jobs WHERE employment_type IS NOT NULL AND employment_type != '' ORDER BY employment_type ASC`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}

	salaryRows, err := h.db.SQL.QueryContext(c.Request.Context(), `SELECT `+annualizedSalarySQL(`COALESCE(salary_min_usd, salary_min)`)+` FROM parsed_jobs WHERE salary_min_usd IS NOT NULL OR salary_min IS NOT NULL`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}
	defer salaryRows.Close()
	salaryValues := []float64{}
	for salaryRows.Next() {
		var value sql.NullFloat64
		if err := salaryRows.Scan(&value); err == nil && value.Valid {
			salaryValues = append(salaryValues, value.Float64)
		}
	}
	minSalaryOptions := buildDenseSalaryOptions(salaryValues, 10000, 100000)

	seniorities := []string{}
	for _, check := range []struct{ Name, Field string }{{"entry", "is_entry_level"}, {"junior", "is_junior"}, {"mid", "is_mid_level"}, {"senior", "is_senior"}, {"lead", "is_lead"}} {
		var count int
		if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(id) FROM parsed_jobs WHERE `+check.Field+` = 1`).Scan(&count); err == nil && count > 0 {
			seniorities = append(seniorities, check.Name)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"job_categories":   categories,
		"locations":        locations,
		"employment_types": employmentTypes,
		"post_date_options": []string{
			"24_hours",
			"48_hours",
			"3_days",
			"week",
			"month",
			"3_months",
		},
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
		parts := make([]string, 0, len(titles))
		for _, title := range titles {
			normalizedTitle := strings.ToLower(strings.TrimSpace(title))
			parts = append(parts, `(lower(trim(COALESCE(p.role_title, ''))) = ? OR lower(trim(COALESCE(p.categorized_job_function, ''))) = ? OR p.categorized_job_title LIKE ? OR p.role_title LIKE ?)`)
			args = append(args, normalizedTitle)
			args = append(args, normalizedTitle)
			args = append(args, "%"+title+"%")
			args = append(args, "%"+title+"%")
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if regions := parseCSVQuery(c.Query("region")); len(regions) > 0 {
		parts := make([]string, 0, len(regions))
		for _, region := range regions {
			parts = append(parts, `(p.location LIKE ? OR p.location_city LIKE ? OR p.location_us_states LIKE ?)`)
			args = append(args, "%"+region+"%", "%"+region+"%", "%"+region+"%")
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if techStacks := parseCSVQuery(c.Query("tech_stack")); len(techStacks) > 0 {
		parts := make([]string, 0, len(techStacks))
		for _, techStack := range techStacks {
			normalizedStack := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(techStack)), `"`, `\"`)
			parts = append(parts, `lower(COALESCE(p.tech_stack, '')) LIKE ?`)
			args = append(args, `%`+"\""+normalizedStack+"\""+`%`)
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if employmentTypes := parseCSVQuery(c.Query("employment_type")); len(employmentTypes) > 0 {
		parts := make([]string, 0, len(employmentTypes))
		for _, employmentType := range employmentTypes {
			parts = append(parts, `p.employment_type LIKE ?`)
			args = append(args, "%"+employmentType+"%")
		}
		filters = append(filters, "("+strings.Join(parts, " OR ")+")")
	}
	if postDate := strings.ToLower(strings.TrimSpace(c.Query("post_date"))); postDate != "" {
		now := time.Now().UTC()
		var cutoff time.Time
		switch postDate {
		case "24_hours":
			cutoff = now.Add(-24 * time.Hour)
		case "48_hours":
			cutoff = now.Add(-48 * time.Hour)
		case "3_days":
			cutoff = now.Add(-72 * time.Hour)
		case "week":
			cutoff = now.Add(-7 * 24 * time.Hour)
		case "month":
			cutoff = now.Add(-30 * 24 * time.Hour)
		case "3_months":
			cutoff = now.Add(-90 * 24 * time.Hour)
		}
		if !cutoff.IsZero() {
			filters = append(filters, `p.created_at_source >= ?`)
			args = append(args, cutoff.Format(time.RFC3339Nano))
		}
	}
	if minSalary := strings.TrimSpace(c.Query("min_salary")); minSalary != "" {
		if parsed, err := strconv.ParseFloat(minSalary, 64); err == nil {
			filters = append(filters, annualizedSalarySQL(`COALESCE(p.salary_min_usd, p.salary_min)`)+` >= ?`)
			args = append(args, parsed)
		}
	}
	if seniorities := parseCSVQuery(c.Query("seniority")); len(seniorities) > 0 {
		parts := []string{}
		fieldMap := map[string]string{"entry": "p.is_entry_level = 1", "junior": "p.is_junior = 1", "mid": "p.is_mid_level = 1", "senior": "p.is_senior = 1", "lead": "p.is_lead = 1"}
		for _, seniority := range seniorities {
			if predicate, ok := fieldMap[seniority]; ok {
				parts = append(parts, predicate)
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
	orderBy := `p.created_at_source DESC, p.id DESC`
	idOrderBy := `created_at_source DESC, id DESC`
	if c.DefaultQuery("sort_criteria", "date") == "salary" {
		orderBy = annualizedSalarySQL(`COALESCE(p.salary_max_usd, p.salary_max, p.salary_min_usd, p.salary_min)`) + ` DESC, p.id DESC`
		idOrderBy = annualizedSalarySQL(`COALESCE(salary_max_usd, salary_max, salary_min_usd, salary_min)`) + ` DESC, id DESC`
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
	rows, err := h.db.SQL.QueryContext(
		c.Request.Context(),
		`SELECT p.id, p.raw_us_job_id, p.role_title, p.job_description_summary, c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b, p.categorized_job_title, p.location, p.location_city, p.location_type, p.location_us_states, p.employment_type, p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_type, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead, p.tech_stack, p.updated_at, p.created_at_source, p.url
		 FROM parsed_jobs p
		 JOIN (
		   SELECT id
		   FROM parsed_jobs p`+where+`
		   ORDER BY `+idOrderBy+`
		   LIMIT ? OFFSET ?
		 ) page_ids ON p.id = page_ids.id
		 LEFT JOIN parsed_companies c ON c.id = p.company_id
		 ORDER BY `+orderBy,
		idArgs...,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	defer rows.Close()

	items := []jobItem{}
	for rows.Next() {
		item, err := scanJob(rows)
		if err == nil {
			items = append(items, item)
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
func nullFloatValue(value sql.NullFloat64) any {
	if !value.Valid {
		return nil
	}
	return value.Float64
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
func sortFloat64s(values []float64) {
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}
func sortInts(values []int) {
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
