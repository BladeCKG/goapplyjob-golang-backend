package jobs

import (
	"database/sql"
	"encoding/json"
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
	ID               int64      `json:"id"`
	RawUSJobID       int64      `json:"raw_us_job_id"`
	CategorizedTitle *string    `json:"categorized_job_title"`
	Location         *string    `json:"location"`
	LocationCity     *string    `json:"location_city"`
	LocationUSStates []string   `json:"location_us_states"`
	SalaryMin        *float64   `json:"salary_min"`
	SalaryMax        *float64   `json:"salary_max"`
	SalaryMinUSD     *float64   `json:"salary_min_usd"`
	SalaryMaxUSD     *float64   `json:"salary_max_usd"`
	IsEntryLevel     *bool      `json:"is_entry_level"`
	IsJunior         *bool      `json:"is_junior"`
	IsMidLevel       *bool      `json:"is_mid_level"`
	IsSenior         *bool      `json:"is_senior"`
	IsLead           *bool      `json:"is_lead"`
	CreatedAtSource  *time.Time `json:"created_at_source"`
	URL              *string    `json:"url"`
}

func NewHandler(cfg config.Config, db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{cfg: cfg, db: db, auth: authHandler}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/jobs/filter-options", h.filterOptions)
	router.GET("/jobs", h.listJobs)
}

func (h *Handler) filterOptions(c *gin.Context) {
	categories, err := selectDistinctStrings(c, h.db, `SELECT DISTINCT categorized_job_title FROM parsed_jobs WHERE categorized_job_title IS NOT NULL AND categorized_job_title != '' ORDER BY categorized_job_title ASC`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}

	locationSet := map[string]struct{}{}
	for _, query := range []string{
		`SELECT DISTINCT location_city FROM parsed_jobs WHERE location_city IS NOT NULL AND location_city != ''`,
		`SELECT DISTINCT location FROM parsed_jobs WHERE location IS NOT NULL AND location != ''`,
	} {
		values, err := selectDistinctStrings(c, h.db, query)
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

	var salaryMin, salaryMax sql.NullFloat64
	if err := h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT MIN(COALESCE(salary_min_usd, salary_min)), MAX(COALESCE(salary_max_usd, salary_max, salary_min_usd, salary_min))
		 FROM parsed_jobs
		 WHERE COALESCE(salary_min_usd, salary_min) IS NOT NULL`,
	).Scan(&salaryMin, &salaryMax); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load filter options"})
		return
	}

	seniorities := []string{}
	for _, check := range []struct {
		Name  string
		Field string
	}{
		{"entry", "is_entry_level"},
		{"junior", "is_junior"},
		{"mid", "is_mid_level"},
		{"senior", "is_senior"},
		{"lead", "is_lead"},
	} {
		var count int
		if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(id) FROM parsed_jobs WHERE `+check.Field+` = 1`).Scan(&count); err == nil && count > 0 {
			seniorities = append(seniorities, check.Name)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"job_categories": categories,
		"locations":      locations,
		"min_salary_range": gin.H{
			"min": nullableFloatValue(salaryMin),
			"max": nullableFloatValue(salaryMax),
		},
		"seniorities": seniorities,
	})
}

func (h *Handler) listJobs(c *gin.Context) {
	isPreview := h.auth.OptionalCurrentUser(c) == nil

	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 20), 1)
	if perPage > 100 {
		perPage = 100
	}
	effectivePerPage := perPage
	if isPreview && effectivePerPage > max(h.cfg.PublicJobsMaxPerPage, 1) {
		effectivePerPage = max(h.cfg.PublicJobsMaxPerPage, 1)
	}

	filters := []string{}
	args := []any{}
	if jobTitle := strings.TrimSpace(c.Query("job_title")); jobTitle != "" {
		filters = append(filters, "categorized_job_title LIKE ?")
		args = append(args, "%"+jobTitle+"%")
	}
	if region := strings.TrimSpace(c.Query("region")); region != "" {
		filters = append(filters, "(location LIKE ? OR location_city LIKE ? OR location_us_states LIKE ?)")
		args = append(args, "%"+region+"%", "%"+region+"%", "%"+region+"%")
	}
	if minSalary := strings.TrimSpace(c.Query("min_salary")); minSalary != "" {
		if parsed, err := strconv.ParseFloat(minSalary, 64); err == nil {
			filters = append(filters, "COALESCE(salary_min_usd, salary_min) >= ?")
			args = append(args, parsed)
		}
	}
	switch c.Query("seniority") {
	case "entry":
		filters = append(filters, "is_entry_level = 1")
	case "junior":
		filters = append(filters, "is_junior = 1")
	case "mid":
		filters = append(filters, "is_mid_level = 1")
	case "senior":
		filters = append(filters, "is_senior = 1")
	case "lead":
		filters = append(filters, "is_lead = 1")
	}

	where := ""
	if len(filters) > 0 {
		where = " WHERE " + strings.Join(filters, " AND ")
	}

	var rawTotal int
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(id) FROM parsed_jobs`+where, args...).Scan(&rawTotal); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	total := rawTotal
	if isPreview && total > max(h.cfg.PublicJobsMaxTotal, 0) {
		total = max(h.cfg.PublicJobsMaxTotal, 0)
	}

	offset := (page - 1) * effectivePerPage
	if isPreview && offset >= total {
		c.JSON(http.StatusOK, gin.H{
			"page":           page,
			"per_page":       effectivePerPage,
			"total":          total,
			"is_preview":     true,
			"requires_login": rawTotal > total,
			"items":          []any{},
		})
		return
	}

	limit := effectivePerPage
	if isPreview && total-offset < limit {
		limit = total - offset
		if limit < 0 {
			limit = 0
		}
	}

	orderBy := "created_at_source DESC, id DESC"
	if c.DefaultQuery("sort_criteria", "date") == "salary" {
		orderBy = "COALESCE(salary_max_usd, salary_max, salary_min_usd, salary_min) DESC, id DESC"
	}

	queryArgs := append([]any{}, args...)
	queryArgs = append(queryArgs, limit, offset)
	rows, err := h.db.SQL.QueryContext(
		c.Request.Context(),
		`SELECT id, raw_us_job_id, categorized_job_title, location, location_city, location_us_states, salary_min, salary_max, salary_min_usd, salary_max_usd, is_entry_level, is_junior, is_mid_level, is_senior, is_lead, created_at_source, url
		 FROM parsed_jobs`+where+` ORDER BY `+orderBy+` LIMIT ? OFFSET ?`,
		queryArgs...,
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
		"page":           page,
		"per_page":       effectivePerPage,
		"total":          total,
		"is_preview":     isPreview,
		"requires_login": isPreview && rawTotal > total,
		"items":          items,
	})
}

func selectDistinctStrings(c *gin.Context, db *database.DB, query string) ([]string, error) {
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
	var title, location, city, statesText, url sql.NullString
	var salaryMin, salaryMax, salaryMinUSD, salaryMaxUSD sql.NullFloat64
	var isEntry, isJunior, isMid, isSenior, isLead sql.NullBool
	var createdAt sql.NullString
	err := scanner.Scan(
		&item.ID,
		&item.RawUSJobID,
		&title,
		&location,
		&city,
		&statesText,
		&salaryMin,
		&salaryMax,
		&salaryMinUSD,
		&salaryMaxUSD,
		&isEntry,
		&isJunior,
		&isMid,
		&isSenior,
		&isLead,
		&createdAt,
		&url,
	)
	if err != nil {
		return item, err
	}
	item.CategorizedTitle = nullableString(title)
	item.Location = nullableString(location)
	item.LocationCity = nullableString(city)
	item.URL = nullableString(url)
	item.SalaryMin = nullableFloatPtr(salaryMin)
	item.SalaryMax = nullableFloatPtr(salaryMax)
	item.SalaryMinUSD = nullableFloatPtr(salaryMinUSD)
	item.SalaryMaxUSD = nullableFloatPtr(salaryMaxUSD)
	item.IsEntryLevel = nullableBool(isEntry)
	item.IsJunior = nullableBool(isJunior)
	item.IsMidLevel = nullableBool(isMid)
	item.IsSenior = nullableBool(isSenior)
	item.IsLead = nullableBool(isLead)
	if createdAt.Valid {
		if parsed, err := time.Parse(time.RFC3339Nano, createdAt.String); err == nil {
			item.CreatedAtSource = &parsed
		}
	}
	if statesText.Valid && statesText.String != "" {
		_ = json.Unmarshal([]byte(statesText.String), &item.LocationUSStates)
	}
	return item, nil
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

func nullableFloatValue(value sql.NullFloat64) any {
	if !value.Valid {
		return nil
	}
	return value.Float64
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

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
