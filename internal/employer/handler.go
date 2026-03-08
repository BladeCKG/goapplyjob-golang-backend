package employer

import (
	"database/sql"
	"encoding/json"
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

const (
	statusDraft     = "draft"
	statusReview    = "review"
	statusPublished = "published"
	statusClosed    = "closed"
	statusExpired   = "expired"
)

type Handler struct {
	cfg  config.Config
	db   *database.DB
	auth *auth.Handler
}

type organizationResponse struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"`
}

type employerJobPayload struct {
	OrganizationID *int64   `json:"organization_id"`
	Title          *string  `json:"title"`
	Department     *string  `json:"department"`
	Description    *string  `json:"description"`
	Requirements   *string  `json:"requirements"`
	Benefits       *string  `json:"benefits"`
	EmploymentType *string  `json:"employment_type"`
	LocationType   *string  `json:"location_type"`
	Locations      []string `json:"locations"`
	Seniority      *string  `json:"seniority"`
	TechStack      []string `json:"tech_stack"`
	ApplyURL       *string  `json:"apply_url"`
	ApplyEmail     *string  `json:"apply_email"`
	SalaryCurrency *string  `json:"salary_currency"`
	SalaryPeriod   *string  `json:"salary_period"`
	SalaryMin      *float64 `json:"salary_min"`
	SalaryMax      *float64 `json:"salary_max"`
}

type jobResponse struct {
	ID             int64    `json:"id"`
	OrganizationID int64    `json:"organization_id"`
	Status         string   `json:"status"`
	Title          *string  `json:"title"`
	Slug           *string  `json:"slug"`
	Department     *string  `json:"department"`
	Description    *string  `json:"description"`
	Requirements   *string  `json:"requirements"`
	Benefits       *string  `json:"benefits"`
	EmploymentType *string  `json:"employment_type"`
	LocationType   *string  `json:"location_type"`
	Locations      []string `json:"locations"`
	Seniority      *string  `json:"seniority"`
	TechStack      []string `json:"tech_stack"`
	ApplyURL       *string  `json:"apply_url"`
	ApplyEmail     *string  `json:"apply_email"`
	SalaryCurrency *string  `json:"salary_currency"`
	SalaryPeriod   *string  `json:"salary_period"`
	SalaryMin      *float64 `json:"salary_min"`
	SalaryMax      *float64 `json:"salary_max"`
	PostingFeeUSD  int      `json:"posting_fee_usd"`
	PostingStatus  string   `json:"posting_fee_status"`
	PostingPaidAt  *string  `json:"posting_fee_paid_at"`
	PublishedAt    *string  `json:"published_at"`
	ClosedAt       *string  `json:"closed_at"`
	ExpiresAt      *string  `json:"expires_at"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at"`
}

func NewHandler(cfg config.Config, db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{cfg: cfg, db: db, auth: authHandler}
}

func (h *Handler) Register(router gin.IRouter) {
	router.POST("/employer/organizations", h.createOrganization)
	router.GET("/employer/organizations", h.listOrganizations)
	router.POST("/employer/jobs", h.createJobDraft)
	router.GET("/employer/jobs", h.listJobs)
	router.GET("/employer/jobs/:jobID", h.getJob)
	router.PATCH("/employer/jobs/:jobID", h.updateJob)
	router.GET("/employer/jobs/:jobID/preview", h.getJob)
	router.POST("/employer/jobs/:jobID/submit-review", h.submitReview)
	router.POST("/employer/jobs/:jobID/pay", h.payPostingFee)
	router.POST("/employer/jobs/:jobID/publish", h.publishJob)
	router.POST("/employer/jobs/:jobID/close", h.closeJob)
}

func (h *Handler) createOrganization(c *gin.Context) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	var payload struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil || strings.TrimSpace(payload.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Organization name is required"})
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var orgID int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `INSERT INTO employer_organizations (name, created_by_user_id, created_at) VALUES (?, ?, ?) RETURNING id`, strings.TrimSpace(payload.Name), user.ID, now).Scan(&orgID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create organization"})
		return
	}
	if _, err := h.db.SQL.ExecContext(c.Request.Context(), `INSERT INTO employer_organization_members (organization_id, user_id, role, created_at) VALUES (?, ?, 'owner', ?)`, orgID, user.ID, now); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create organization membership"})
		return
	}
	c.JSON(http.StatusOK, organizationResponse{ID: orgID, Name: strings.TrimSpace(payload.Name), Role: "owner"})
}

func (h *Handler) listOrganizations(c *gin.Context) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	rows, err := h.db.SQL.QueryContext(c.Request.Context(), `SELECT o.id, o.name, m.role
		FROM employer_organizations o
		JOIN employer_organization_members m ON m.organization_id = o.id
		WHERE m.user_id = ?
		ORDER BY o.created_at DESC, o.id DESC`, user.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load organizations"})
		return
	}
	defer rows.Close()
	result := []organizationResponse{}
	for rows.Next() {
		var item organizationResponse
		if err := rows.Scan(&item.ID, &item.Name, &item.Role); err == nil {
			result = append(result, item)
		}
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) createJobDraft(c *gin.Context) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	var payload employerJobPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	if payload.SalaryMin != nil && payload.SalaryMax != nil && *payload.SalaryMin > *payload.SalaryMax {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "salary_min cannot exceed salary_max"})
		return
	}
	orgID, role, err := h.resolveOrganizationForCreate(c, user.ID, payload.OrganizationID)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
		return
	}
	if role != "owner" && role != "admin" && role != "recruiter" {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Insufficient role"})
		return
	}
	now := time.Now().UTC()
	locationsJSON := marshalStringSlice(payload.Locations)
	techStackJSON := marshalStringSlice(payload.TechStack)
	var jobID int64
	err = h.db.SQL.QueryRowContext(c.Request.Context(), `INSERT INTO employer_jobs (
		organization_id, created_by_user_id, status, title, department, description, requirements, benefits,
		employment_type, location_type, locations_json, seniority, tech_stack, apply_url, apply_email,
		salary_currency, salary_period, salary_min, salary_max, posting_fee_usd, posting_fee_status, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unpaid', ?, ?) RETURNING id`,
		orgID, user.ID, statusDraft, payload.Title, payload.Department, payload.Description, payload.Requirements, payload.Benefits,
		payload.EmploymentType, payload.LocationType, locationsJSON, payload.Seniority, techStackJSON, payload.ApplyURL, payload.ApplyEmail,
		payload.SalaryCurrency, payload.SalaryPeriod, payload.SalaryMin, payload.SalaryMax, max(h.cfg.EmployerPostingFeeUSD, 0),
		now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
	).Scan(&jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create draft"})
		return
	}
	if payload.Title != nil && strings.TrimSpace(*payload.Title) != "" {
		slug := slugify(*payload.Title) + "-" + strconv.FormatInt(jobID, 10)
		_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE employer_jobs SET slug = ?, updated_at = ? WHERE id = ?`, slug, time.Now().UTC().Format(time.RFC3339Nano), jobID)
	}
	_ = h.addAuditEvent(c, jobID, user.ID, "draft_created", map[string]any{"organization_id": orgID})
	h.respondJob(c, jobID)
}

func (h *Handler) listJobs(c *gin.Context) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	query := `SELECT j.id
		FROM employer_jobs j
		JOIN employer_organization_members m ON m.organization_id = j.organization_id
		WHERE m.user_id = ?`
	args := []any{user.ID}
	if orgID := strings.TrimSpace(c.Query("organization_id")); orgID != "" {
		query += ` AND j.organization_id = ?`
		args = append(args, orgID)
	}
	if statusFilter := strings.TrimSpace(c.Query("status_filter")); statusFilter != "" {
		query += ` AND j.status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY j.updated_at DESC, j.id DESC`
	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	defer rows.Close()
	result := []jobResponse{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		item, err := h.loadJobByID(c, id)
		if err == nil {
			result = append(result, item)
		}
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) getJob(c *gin.Context) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	jobID, err := parseJobID(c.Param("jobID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid job id"})
		return
	}
	if _, _, err := h.requireJobMember(c, jobID, user.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
		return
	}
	h.respondJob(c, jobID)
}

func (h *Handler) updateJob(c *gin.Context) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	jobID, err := parseJobID(c.Param("jobID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid job id"})
		return
	}
	job, role, err := h.requireJobMember(c, jobID, user.ID)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
		return
	}
	if role != "owner" && role != "admin" && role != "recruiter" {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Insufficient role"})
		return
	}
	if job.Status == statusClosed || job.Status == statusExpired {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Cannot edit closed or expired job"})
		return
	}
	var payload employerJobPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	locationsJSON := marshalStringSlice(payload.Locations)
	techStackJSON := marshalStringSlice(payload.TechStack)
	nextStatus := job.Status
	if job.Status == statusPublished {
		nextStatus = statusReview
	}
	_, err = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE employer_jobs SET
		title = COALESCE(?, title),
		department = COALESCE(?, department),
		description = COALESCE(?, description),
		requirements = COALESCE(?, requirements),
		benefits = COALESCE(?, benefits),
		employment_type = COALESCE(?, employment_type),
		location_type = COALESCE(?, location_type),
		locations_json = COALESCE(?, locations_json),
		seniority = COALESCE(?, seniority),
		tech_stack = COALESCE(?, tech_stack),
		apply_url = COALESCE(?, apply_url),
		apply_email = COALESCE(?, apply_email),
		salary_currency = COALESCE(?, salary_currency),
		salary_period = COALESCE(?, salary_period),
		salary_min = COALESCE(?, salary_min),
		salary_max = COALESCE(?, salary_max),
		status = ?,
		updated_at = ?
		WHERE id = ?`,
		payload.Title, payload.Department, payload.Description, payload.Requirements, payload.Benefits,
		payload.EmploymentType, payload.LocationType, locationsJSON, payload.Seniority, techStackJSON,
		payload.ApplyURL, payload.ApplyEmail, payload.SalaryCurrency, payload.SalaryPeriod, payload.SalaryMin, payload.SalaryMax,
		nextStatus, time.Now().UTC().Format(time.RFC3339Nano), jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job"})
		return
	}
	_ = h.addAuditEvent(c, jobID, user.ID, "job_updated", map[string]any{"status": nextStatus})
	h.respondJob(c, jobID)
}

func (h *Handler) submitReview(c *gin.Context) {
	h.transitionStatus(c, statusReview, func(job employerJob, _ string) (bool, int, string) {
		if job.Status != statusDraft && job.Status != statusReview {
			return false, http.StatusBadRequest, "Job cannot be submitted for review"
		}
		return true, http.StatusOK, ""
	}, "submitted_for_review")
}

func (h *Handler) payPostingFee(c *gin.Context) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	jobID, err := parseJobID(c.Param("jobID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid job id"})
		return
	}
	_, role, err := h.requireJobMember(c, jobID, user.ID)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
		return
	}
	if role != "owner" && role != "admin" && role != "recruiter" {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Insufficient role"})
		return
	}
	_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE employer_jobs SET posting_fee_status = 'paid', posting_fee_paid_at = ?, updated_at = ? WHERE id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano), jobID)
	_ = h.addAuditEvent(c, jobID, user.ID, "posting_fee_paid", map[string]any{"posting_fee_usd": max(h.cfg.EmployerPostingFeeUSD, 0)})
	h.respondJob(c, jobID)
}

func (h *Handler) publishJob(c *gin.Context) {
	h.transitionStatus(c, statusPublished, func(job employerJob, role string) (bool, int, string) {
		if role != "owner" && role != "admin" {
			return false, http.StatusForbidden, "Only owner/admin can publish"
		}
		if job.PostingFeeStatus != "paid" {
			return false, http.StatusPaymentRequired, "Posting fee payment required before publish"
		}
		if job.Status != statusDraft && job.Status != statusReview {
			return false, http.StatusBadRequest, "Job is not publishable"
		}
		return true, http.StatusOK, ""
	}, "job_published")
}

func (h *Handler) closeJob(c *gin.Context) {
	h.transitionStatus(c, statusClosed, func(_ employerJob, role string) (bool, int, string) {
		if role != "owner" && role != "admin" {
			return false, http.StatusForbidden, "Only owner/admin can close"
		}
		return true, http.StatusOK, ""
	}, "job_closed")
}

type employerJob struct {
	ID               int64
	Status           string
	PostingFeeStatus string
}

func (h *Handler) transitionStatus(c *gin.Context, nextStatus string, allowed func(employerJob, string) (bool, int, string), eventType string) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	jobID, err := parseJobID(c.Param("jobID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid job id"})
		return
	}
	job, role, err := h.requireJobMember(c, jobID, user.ID)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
		return
	}
	okStatus, code, detail := allowed(job, role)
	if !okStatus {
		c.JSON(code, gin.H{"detail": detail})
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	switch nextStatus {
	case statusPublished:
		_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE employer_jobs SET status = ?, published_at = COALESCE(published_at, ?), expires_at = COALESCE(expires_at, ?), closed_at = NULL, updated_at = ? WHERE id = ?`,
			nextStatus, now, time.Now().UTC().Add(30*24*time.Hour).Format(time.RFC3339Nano), now, jobID)
	case statusClosed:
		_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE employer_jobs SET status = ?, closed_at = ?, updated_at = ? WHERE id = ?`,
			nextStatus, now, now, jobID)
	default:
		_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE employer_jobs SET status = ?, updated_at = ? WHERE id = ?`,
			nextStatus, now, jobID)
	}
	_ = h.addAuditEvent(c, jobID, user.ID, eventType, nil)
	h.respondJob(c, jobID)
}

func (h *Handler) requireUser(c *gin.Context) (*auth.User, bool) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return nil, false
	}
	return user, true
}

func (h *Handler) resolveOrganizationForCreate(c *gin.Context, userID int64, requested *int64) (int64, string, error) {
	if requested != nil && *requested > 0 {
		return h.requireOrganizationMember(c, *requested, userID)
	}
	var orgID int64
	var role string
	err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT organization_id, role FROM employer_organization_members WHERE user_id = ? AND role = 'owner' ORDER BY id ASC LIMIT 1`, userID).Scan(&orgID, &role)
	if err == nil {
		return orgID, role, nil
	}
	if err != sql.ErrNoRows {
		return 0, "", err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	name := strings.SplitN("user-"+strconv.FormatInt(userID, 10), "@", 2)[0] + " jobs"
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `INSERT INTO employer_organizations (name, created_by_user_id, created_at) VALUES (?, ?, ?) RETURNING id`, name, userID, now).Scan(&orgID); err != nil {
		return 0, "", err
	}
	_, err = h.db.SQL.ExecContext(c.Request.Context(), `INSERT INTO employer_organization_members (organization_id, user_id, role, created_at) VALUES (?, ?, 'owner', ?)`, orgID, userID, now)
	return orgID, "owner", err
}

func (h *Handler) requireOrganizationMember(c *gin.Context, orgID, userID int64) (int64, string, error) {
	var role string
	err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT role FROM employer_organization_members WHERE organization_id = ? AND user_id = ? LIMIT 1`, orgID, userID).Scan(&role)
	if err != nil {
		return 0, "", err
	}
	return orgID, role, nil
}

func (h *Handler) requireJobMember(c *gin.Context, jobID, userID int64) (employerJob, string, error) {
	var job employerJob
	var orgID int64
	err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id, organization_id, status, posting_fee_status FROM employer_jobs WHERE id = ? LIMIT 1`, jobID).Scan(&job.ID, &orgID, &job.Status, &job.PostingFeeStatus)
	if err != nil {
		return employerJob{}, "", err
	}
	var role string
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT role FROM employer_organization_members WHERE organization_id = ? AND user_id = ? LIMIT 1`, orgID, userID).Scan(&role); err != nil {
		return employerJob{}, "", err
	}
	return job, role, nil
}

func (h *Handler) addAuditEvent(c *gin.Context, jobID, actorUserID int64, eventType string, detail map[string]any) error {
	var detailJSON any
	if detail != nil {
		body, _ := json.Marshal(detail)
		detailJSON = string(body)
	}
	_, err := h.db.SQL.ExecContext(c.Request.Context(), `INSERT INTO employer_job_audit_events (employer_job_id, actor_user_id, event_type, detail_json, created_at) VALUES (?, ?, ?, ?, ?)`,
		jobID, actorUserID, eventType, detailJSON, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (h *Handler) respondJob(c *gin.Context, jobID int64) {
	item, err := h.loadJobByID(c, jobID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Job not found"})
		return
	}
	c.JSON(http.StatusOK, item)
}

func (h *Handler) loadJobByID(c *gin.Context, jobID int64) (jobResponse, error) {
	row := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT
		id, organization_id, status, title, slug, department, description, requirements, benefits, employment_type,
		location_type, locations_json, seniority, tech_stack, apply_url, apply_email, salary_currency, salary_period,
		salary_min, salary_max, posting_fee_usd, posting_fee_status, posting_fee_paid_at, published_at, closed_at, expires_at, created_at, updated_at
		FROM employer_jobs WHERE id = ? LIMIT 1`, jobID)
	var item jobResponse
	var title, slug, department, description, requirements, benefits, employmentType, locationType, locationsJSON, seniority, techStackJSON, applyURL, applyEmail, salaryCurrency, salaryPeriod sql.NullString
	var salaryMin, salaryMax sql.NullFloat64
	var postingPaidAt, publishedAt, closedAt, expiresAt sql.NullString
	if err := row.Scan(&item.ID, &item.OrganizationID, &item.Status, &title, &slug, &department, &description, &requirements, &benefits, &employmentType, &locationType, &locationsJSON, &seniority, &techStackJSON, &applyURL, &applyEmail, &salaryCurrency, &salaryPeriod, &salaryMin, &salaryMax, &item.PostingFeeUSD, &item.PostingStatus, &postingPaidAt, &publishedAt, &closedAt, &expiresAt, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return jobResponse{}, err
	}
	item.Title = nullableString(title)
	item.Slug = nullableString(slug)
	item.Department = nullableString(department)
	item.Description = nullableString(description)
	item.Requirements = nullableString(requirements)
	item.Benefits = nullableString(benefits)
	item.EmploymentType = nullableString(employmentType)
	item.LocationType = nullableString(locationType)
	item.Seniority = nullableString(seniority)
	item.ApplyURL = nullableString(applyURL)
	item.ApplyEmail = nullableString(applyEmail)
	item.SalaryCurrency = nullableString(salaryCurrency)
	item.SalaryPeriod = nullableString(salaryPeriod)
	item.SalaryMin = nullableFloat64(salaryMin)
	item.SalaryMax = nullableFloat64(salaryMax)
	item.PostingPaidAt = nullableString(postingPaidAt)
	item.PublishedAt = nullableString(publishedAt)
	item.ClosedAt = nullableString(closedAt)
	item.ExpiresAt = nullableString(expiresAt)
	if locationsJSON.Valid && strings.TrimSpace(locationsJSON.String) != "" {
		_ = json.Unmarshal([]byte(locationsJSON.String), &item.Locations)
	}
	if techStackJSON.Valid && strings.TrimSpace(techStackJSON.String) != "" {
		_ = json.Unmarshal([]byte(techStackJSON.String), &item.TechStack)
	}
	return item, nil
}

func nullableString(value sql.NullString) *string {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	v := value.String
	return &v
}

func nullableFloat64(value sql.NullFloat64) *float64 {
	if !value.Valid {
		return nil
	}
	v := value.Float64
	return &v
}

func marshalStringSlice(values []string) any {
	if len(values) == 0 {
		return nil
	}
	body, _ := json.Marshal(values)
	return string(body)
}

func parseJobID(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
}

func slugify(value string) string {
	text := strings.ToLower(strings.TrimSpace(value))
	text = regexp.MustCompile(`[^a-z0-9\s-]`).ReplaceAllString(text, "")
	text = regexp.MustCompile(`\s+`).ReplaceAllString(text, "-")
	text = regexp.MustCompile(`-{2,}`).ReplaceAllString(text, "-")
	text = strings.Trim(text, "-")
	if text == "" {
		return "job"
	}
	return text
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
