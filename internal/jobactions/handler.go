package jobactions

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/database"
	jobspayload "goapplyjob-golang-backend/internal/jobs"
	gensqlc "goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Handler struct {
	db   *database.DB
	auth *auth.Handler
	q    *gensqlc.Queries
}

type actionItem struct {
	JobID     int64  `json:"job_id"`
	IsApplied bool   `json:"is_applied"`
	IsSaved   bool   `json:"is_saved"`
	IsHidden  bool   `json:"is_hidden"`
	UpdatedAt string `json:"updated_at"`
}

type companyCompanionJobItem struct {
	ForJobID int64 `json:"for_job_id"`
	jobspayload.ListingJobItem
}

func NewHandler(db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{db: db, auth: authHandler, q: gensqlc.New(db.PGX)}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/job-actions", h.getJobActions)
	router.GET("/job-actions/company-companions", h.getCompanyCompanionJobs)
	router.GET("/job-actions/summary", h.getJobActionsSummary)
	router.PUT("/job-actions/:jobID", h.updateJobAction)
	router.POST("/job-actions/clear", h.clearJobActions)
}

func (h *Handler) getJobActions(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	jobIDs := parseJobIDsCSV(c.Query("job_ids"))
	if len(jobIDs) == 0 {
		c.JSON(http.StatusOK, gin.H{"items": []actionItem{}})
		return
	}

	rows, err := h.q.GetUserJobActionsByJobIDs(c.Request.Context(), gensqlc.GetUserJobActionsByJobIDsParams{
		UserID:  user.ID,
		Column2: jobIDs,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load job actions"})
		return
	}

	items := []actionItem{}
	for _, row := range rows {
		items = append(items, actionItem{
			JobID:     int64(row.ParsedJobID),
			IsApplied: row.IsApplied,
			IsSaved:   row.IsSaved,
			IsHidden:  row.IsHidden,
			UpdatedAt: row.UpdatedAt.Time.UTC().Format(time.RFC3339Nano),
		})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *Handler) getCompanyCompanionJobs(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	jobIDs := parseJobIDsCSV(c.Query("job_ids"))
	if len(jobIDs) == 0 {
		c.JSON(http.StatusOK, gin.H{"items": []companyCompanionJobItem{}})
		return
	}

	rows, err := h.db.PGX.Query(
		c.Request.Context(),
		`WITH requested AS (
			SELECT * FROM unnest($2::bigint[]) WITH ORDINALITY AS t(base_job_id, ord)
		),
		base_jobs AS (
			SELECT r.base_job_id, p.company_id, r.ord
			FROM requested r
			JOIN parsed_jobs p ON p.id = r.base_job_id
			WHERE p.company_id IS NOT NULL
		),
		ranked AS (
			SELECT
				b.base_job_id,
				p.id AS companion_job_id,
				ROW_NUMBER() OVER (
					PARTITION BY b.base_job_id
					ORDER BY uja.updated_at DESC, p.created_at_source DESC NULLS LAST, p.id DESC
				) AS rn
			FROM base_jobs b
			JOIN parsed_jobs p
				ON p.company_id = b.company_id
				AND p.id <> b.base_job_id
			JOIN user_job_actions uja
				ON uja.parsed_job_id = p.id
				AND uja.user_id = $1
				AND uja.is_applied = true
				AND uja.is_hidden = false
		)
		SELECT
			r.base_job_id,
			p.id,
			p.role_title,
			p.role_description,
			p.role_requirements,
			p.job_description_summary,
			c.name,
			c.slug,
			c.tagline,
			c.profile_pic_url,
			c.home_page_url,
			c.linkedin_url,
			c.employee_range,
			c.founded_year,
			c.sponsors_h1b,
			p.categorized_job_title,
			p.categorized_job_function,
			p.location_type,
			p.location_us_states,
			p.location_countries,
			p.employment_type,
			p.salary_min,
			p.salary_max,
			p.salary_min_usd,
			p.salary_max_usd,
			p.salary_currency_code,
			p.salary_currency_symbol,
			p.salary_type,
			p.education_requirements_credential_category,
			p.experience_requirements_months,
			p.experience_in_place_of_education,
			p.required_languages,
			p.tech_stack,
			p.benefits,
			p.created_at_source,
			p.date_deleted,
			p.url
		FROM ranked r
		JOIN parsed_jobs p ON p.id = r.companion_job_id
		LEFT JOIN parsed_companies c ON c.id = p.company_id
		WHERE r.rn = 1
		ORDER BY r.base_job_id ASC`,
		user.ID,
		jobIDs,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companion jobs"})
		return
	}
	defer rows.Close()

	items := []companyCompanionJobItem{}
	for rows.Next() {
		var (
			item                 companyCompanionJobItem
			roleTitle            pgtype.Text
			roleDescription      pgtype.Text
			roleRequirements     pgtype.Text
			jobDescription       pgtype.Text
			companyName          pgtype.Text
			companySlug          pgtype.Text
			companyTagline       pgtype.Text
			companyProfilePicURL pgtype.Text
			companyHomePageURL   pgtype.Text
			companyLinkedInURL   pgtype.Text
			companyEmployeeRange pgtype.Text
			companyFoundedYear   pgtype.Text
			companySponsorsH1B   pgtype.Bool
			categoryTitle        pgtype.Text
			categoryFunction     pgtype.Text
			locationType         pgtype.Text
			locationUSStates     []byte
			locationCountries    []byte
			employmentType       pgtype.Text
			salaryMin            pgtype.Float8
			salaryMax            pgtype.Float8
			salaryMinUSD         pgtype.Float8
			salaryMaxUSD         pgtype.Float8
			salaryCurrencyCode   pgtype.Text
			salaryCurrencySymbol pgtype.Text
			salaryType           pgtype.Text
			educationRequirement pgtype.Text
			experienceMonths     pgtype.Int4
			experienceInLieu     pgtype.Bool
			requiredLanguages    []byte
			techStack            []byte
			benefits             pgtype.Text
			createdAtSource      pgtype.Timestamptz
			dateDeleted          pgtype.Timestamptz
			jobURL               pgtype.Text
		)
		if err := rows.Scan(
			&item.ForJobID,
			&item.ID,
			&roleTitle,
			&roleDescription,
			&roleRequirements,
			&jobDescription,
			&companyName,
			&companySlug,
			&companyTagline,
			&companyProfilePicURL,
			&companyHomePageURL,
			&companyLinkedInURL,
			&companyEmployeeRange,
			&companyFoundedYear,
			&companySponsorsH1B,
			&categoryTitle,
			&categoryFunction,
			&locationType,
			&locationUSStates,
			&locationCountries,
			&employmentType,
			&salaryMin,
			&salaryMax,
			&salaryMinUSD,
			&salaryMaxUSD,
			&salaryCurrencyCode,
			&salaryCurrencySymbol,
			&salaryType,
			&educationRequirement,
			&experienceMonths,
			&experienceInLieu,
			&requiredLanguages,
			&techStack,
			&benefits,
			&createdAtSource,
			&dateDeleted,
			&jobURL,
		); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companion jobs"})
			return
		}
		item.RoleTitle = pgTextPtr(roleTitle)
		item.RoleDescription = pgTextPtr(roleDescription)
		item.RoleRequirements = pgTextPtr(roleRequirements)
		item.JobDescriptionSummary = pgTextPtr(jobDescription)
		item.CompanyName = pgTextPtr(companyName)
		item.CompanySlug = pgTextPtr(companySlug)
		item.CompanyTagline = pgTextPtr(companyTagline)
		item.CompanyProfilePicURL = pgTextPtr(companyProfilePicURL)
		item.CompanyHomePageURL = pgTextPtr(companyHomePageURL)
		item.CompanyLinkedInURL = pgTextPtr(companyLinkedInURL)
		item.CompanyEmployeeRange = pgTextPtr(companyEmployeeRange)
		item.CompanyFoundedYear = pgTextPtr(companyFoundedYear)
		item.CompanySponsorsH1B = pgBoolPtr(companySponsorsH1B)
		item.CategorizedTitle = pgTextPtr(categoryTitle)
		item.CategorizedFunction = pgTextPtr(categoryFunction)
		item.LocationType = pgTextPtr(locationType)
		item.EmploymentType = pgTextPtr(employmentType)
		item.SalaryMin = pgFloat64Ptr(salaryMin)
		item.SalaryMax = pgFloat64Ptr(salaryMax)
		item.SalaryMinUSD = pgFloat64Ptr(salaryMinUSD)
		item.SalaryMaxUSD = pgFloat64Ptr(salaryMaxUSD)
		item.SalaryCurrencyCode = pgTextPtr(salaryCurrencyCode)
		item.SalaryCurrencySymbol = pgTextPtr(salaryCurrencySymbol)
		item.SalaryType = pgTextPtr(salaryType)
		item.EducationRequirementsCredentialCategory = pgTextPtr(educationRequirement)
		if experienceMonths.Valid {
			v := int(experienceMonths.Int32)
			item.ExperienceRequirementsMonths = &v
		}
		item.ExperienceInPlaceOfEducation = pgBoolPtr(experienceInLieu)
		item.Benefits = pgTextPtr(benefits)
		item.CreatedAtSource = pgTimePtr(createdAtSource)
		item.DateDeleted = timestamptzStringPtr(dateDeleted)
		item.URL = pgTextPtr(jobURL)
		_ = json.Unmarshal(locationUSStates, &item.LocationUSStates)
		_ = json.Unmarshal(locationCountries, &item.LocationCountries)
		_ = json.Unmarshal(requiredLanguages, &item.RequiredLanguages)
		_ = json.Unmarshal(techStack, &item.TechStack)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companion jobs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *Handler) updateJobAction(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	jobID64, err := strconv.ParseInt(strings.TrimSpace(c.Param("jobID")), 10, 64)
	if err != nil || jobID64 <= 0 || jobID64 > int64(^uint32(0)>>1) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid job id"})
		return
	}
	jobID := int32(jobID64)

	var payload struct {
		IsApplied *bool `json:"is_applied"`
		IsSaved   *bool `json:"is_saved"`
		IsHidden  *bool `json:"is_hidden"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	if payload.IsApplied == nil && payload.IsSaved == nil && payload.IsHidden == nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "At least one action flag is required"})
		return
	}
	exists, err := h.q.CountParsedJobsByID(c.Request.Context(), jobID)
	if err != nil || exists == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Job not found"})
		return
	}

	tx, err := h.db.PGX.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	qtx := h.q.WithTx(tx)

	var current actionItem
	row, rowErr := qtx.GetUserJobActionByUserAndJob(c.Request.Context(), gensqlc.GetUserJobActionByUserAndJobParams{
		UserID:      user.ID,
		ParsedJobID: jobID,
	})
	if rowErr != nil && !errors.Is(rowErr, sql.ErrNoRows) && !errors.Is(rowErr, pgx.ErrNoRows) {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
		return
	}
	if errors.Is(rowErr, sql.ErrNoRows) || errors.Is(rowErr, pgx.ErrNoRows) {
		now := time.Now().UTC()
		err = qtx.InsertUserJobActionDefaults(c.Request.Context(), gensqlc.InsertUserJobActionDefaultsParams{
			UserID:      user.ID,
			ParsedJobID: jobID,
			UpdatedAt:   pgTimestamptz(now),
			CreatedAt:   pgTimestamptz(now),
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
			return
		}
		current = actionItem{JobID: int64(jobID), UpdatedAt: now.Format(time.RFC3339Nano)}
	} else {
		current.JobID = int64(row.ParsedJobID)
		current.IsApplied = row.IsApplied
		current.IsSaved = row.IsSaved
		current.IsHidden = row.IsHidden
		current.UpdatedAt = row.UpdatedAt.Time.UTC().Format(time.RFC3339Nano)
	}

	if payload.IsApplied != nil {
		current.IsApplied = *payload.IsApplied
	}
	if payload.IsSaved != nil {
		current.IsSaved = *payload.IsSaved
	}
	if payload.IsHidden != nil {
		current.IsHidden = *payload.IsHidden
	}
	now := time.Now().UTC()
	current.UpdatedAt = now.Format(time.RFC3339Nano)
	err = qtx.UpdateUserJobActionByUserAndJob(c.Request.Context(), gensqlc.UpdateUserJobActionByUserAndJobParams{
		IsApplied:   current.IsApplied,
		IsSaved:     current.IsSaved,
		IsHidden:    current.IsHidden,
		UpdatedAt:   pgTimestamptz(now),
		UserID:      user.ID,
		ParsedJobID: jobID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
		return
	}
	c.JSON(http.StatusOK, current)
}

func (h *Handler) getJobActionsSummary(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	summary, err := h.q.GetUserJobActionsSummary(c.Request.Context(), user.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load job action summary"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"applied_count": summary.Column1,
		"saved_count":   summary.Column2,
		"hidden_count":  summary.Column3,
	})
}

func (h *Handler) clearJobActions(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	action := strings.ToLower(strings.TrimSpace(c.Query("action")))
	if action != "applied" && action != "saved" && action != "hidden" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid action"})
		return
	}
	now := time.Now().UTC()
	var clearedCount int64
	switch action {
	case "applied":
		clearedCount, err = h.q.ClearAppliedJobActionsByUser(c.Request.Context(), gensqlc.ClearAppliedJobActionsByUserParams{
			UpdatedAt: pgTimestamptz(now),
			UserID:    user.ID,
		})
	case "saved":
		clearedCount, err = h.q.ClearSavedJobActionsByUser(c.Request.Context(), gensqlc.ClearSavedJobActionsByUserParams{
			UpdatedAt: pgTimestamptz(now),
			UserID:    user.ID,
		})
	default:
		clearedCount, err = h.q.ClearHiddenJobActionsByUser(c.Request.Context(), gensqlc.ClearHiddenJobActionsByUserParams{
			UpdatedAt: pgTimestamptz(now),
			UserID:    user.ID,
		})
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to clear job actions"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"cleared_count": clearedCount,
		"action":        action,
	})
}

func parseJobIDsCSV(raw string) []int64 {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	result := []int64{}
	seen := map[int64]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		value, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func pgTimestamptz(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: true}
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

func pgFloat64Ptr(value pgtype.Float8) *float64 {
	if !value.Valid {
		return nil
	}
	v := value.Float64
	return &v
}

func pgTimePtr(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	v := value.Time.UTC()
	return &v
}

func timestamptzStringPtr(value pgtype.Timestamptz) *string {
	if !value.Valid {
		return nil
	}
	formatted := value.Time.UTC().Format(time.RFC3339Nano)
	return &formatted
}
