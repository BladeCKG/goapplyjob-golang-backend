package employer

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	gensqlc "goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
	q    *gensqlc.Queries
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
	return &Handler{cfg: cfg, db: db, auth: authHandler, q: gensqlc.New(db.PGX)}
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
	orgID, err := h.q.CreateEmployerOrganization(c.Request.Context(), gensqlc.CreateEmployerOrganizationParams{
		Name:            strings.TrimSpace(payload.Name),
		CreatedByUserID: user.ID,
		CreatedAt:       now,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create organization"})
		return
	}
	if err := h.q.CreateEmployerOrganizationOwnerMembership(c.Request.Context(), gensqlc.CreateEmployerOrganizationOwnerMembershipParams{
		OrganizationID: orgID,
		UserID:         user.ID,
		CreatedAt:      now,
	}); err != nil {
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
	rows, err := h.q.ListEmployerOrganizationsByUser(c.Request.Context(), user.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load organizations"})
		return
	}
	result := []organizationResponse{}
	for _, row := range rows {
		result = append(result, organizationResponse{ID: row.ID, Name: row.Name, Role: row.Role})
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
	jobID, err := h.q.CreateEmployerJobDraft(c.Request.Context(), gensqlc.CreateEmployerJobDraftParams{
		OrganizationID:  orgID,
		CreatedByUserID: user.ID,
		Status:          statusDraft,
		Title:           nullableText(payload.Title),
		Department:      nullableText(payload.Department),
		Description:     nullableText(payload.Description),
		Requirements:    nullableText(payload.Requirements),
		Benefits:        nullableText(payload.Benefits),
		EmploymentType:  nullableText(payload.EmploymentType),
		LocationType:    nullableText(payload.LocationType),
		LocationsJson:   nullableText(locationsJSON),
		Seniority:       nullableText(payload.Seniority),
		TechStack:       nullableText(techStackJSON),
		ApplyUrl:        nullableText(payload.ApplyURL),
		ApplyEmail:      nullableText(payload.ApplyEmail),
		SalaryCurrency:  nullableText(payload.SalaryCurrency),
		SalaryPeriod:    nullableText(payload.SalaryPeriod),
		SalaryMin:       nullableFloat(payload.SalaryMin),
		SalaryMax:       nullableFloat(payload.SalaryMax),
		PostingFeeUsd:   int32(max(h.cfg.EmployerPostingFeeUSD, 0)),
		CreatedAt:       now.Format(time.RFC3339Nano),
		UpdatedAt:       now.Format(time.RFC3339Nano),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create draft"})
		return
	}
	if payload.Title != nil && strings.TrimSpace(*payload.Title) != "" {
		slug := slugify(*payload.Title) + "-" + strconv.FormatInt(jobID, 10)
		_ = h.q.UpdateEmployerJobSlug(c.Request.Context(), gensqlc.UpdateEmployerJobSlugParams{
			Slug:      pgtype.Text{String: slug, Valid: true},
			UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			ID:        jobID,
		})
	}
	_ = h.addAuditEvent(c, jobID, user.ID, "draft_created", map[string]any{"organization_id": orgID})
	h.respondJob(c, jobID)
}

func (h *Handler) listJobs(c *gin.Context) {
	user, ok := h.requireUser(c)
	if !ok {
		return
	}
	orgID := int64(0)
	if raw := strings.TrimSpace(c.Query("organization_id")); raw != "" {
		if parsed, parseErr := strconv.ParseInt(raw, 10, 64); parseErr == nil {
			orgID = parsed
		}
	}
	statusFilter := strings.TrimSpace(c.Query("status_filter"))
	ids, err := h.q.ListEmployerJobIDsByUser(c.Request.Context(), gensqlc.ListEmployerJobIDsByUserParams{
		UserID:  user.ID,
		Column2: orgID,
		Column3: statusFilter,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load jobs"})
		return
	}
	result := []jobResponse{}
	for _, id := range ids {
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
	err = h.q.UpdateEmployerJobPatch(c.Request.Context(), gensqlc.UpdateEmployerJobPatchParams{
		Title:          nullableText(payload.Title),
		Department:     nullableText(payload.Department),
		Description:    nullableText(payload.Description),
		Requirements:   nullableText(payload.Requirements),
		Benefits:       nullableText(payload.Benefits),
		EmploymentType: nullableText(payload.EmploymentType),
		LocationType:   nullableText(payload.LocationType),
		LocationsJson:  nullableText(locationsJSON),
		Seniority:      nullableText(payload.Seniority),
		TechStack:      nullableText(techStackJSON),
		ApplyUrl:       nullableText(payload.ApplyURL),
		ApplyEmail:     nullableText(payload.ApplyEmail),
		SalaryCurrency: nullableText(payload.SalaryCurrency),
		SalaryPeriod:   nullableText(payload.SalaryPeriod),
		SalaryMin:      nullableFloat(payload.SalaryMin),
		SalaryMax:      nullableFloat(payload.SalaryMax),
		Status:         nextStatus,
		UpdatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		ID:             jobID,
	})
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = h.q.MarkEmployerJobPostingFeePaid(c.Request.Context(), gensqlc.MarkEmployerJobPostingFeePaidParams{
		PostingFeePaidAt: pgtype.Text{String: now, Valid: true},
		UpdatedAt:        now,
		ID:               jobID,
	})
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
		_ = h.q.UpdateEmployerJobStatusPublished(c.Request.Context(), gensqlc.UpdateEmployerJobStatusPublishedParams{
			Status:      nextStatus,
			PublishedAt: pgtype.Text{String: now, Valid: true},
			ExpiresAt:   pgtype.Text{String: time.Now().UTC().Add(30 * 24 * time.Hour).Format(time.RFC3339Nano), Valid: true},
			UpdatedAt:   now,
			ID:          jobID,
		})
	case statusClosed:
		_ = h.q.UpdateEmployerJobStatusClosed(c.Request.Context(), gensqlc.UpdateEmployerJobStatusClosedParams{
			Status:    nextStatus,
			ClosedAt:  pgtype.Text{String: now, Valid: true},
			UpdatedAt: now,
			ID:        jobID,
		})
	default:
		_ = h.q.UpdateEmployerJobStatusSimple(c.Request.Context(), gensqlc.UpdateEmployerJobStatusSimpleParams{
			Status:    nextStatus,
			UpdatedAt: now,
			ID:        jobID,
		})
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
	ownerMembership, err := h.q.GetOwnerEmployerOrganizationByUser(c.Request.Context(), userID)
	if err == nil {
		return ownerMembership.OrganizationID, ownerMembership.Role, nil
	}
	if !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	name := strings.SplitN("user-"+strconv.FormatInt(userID, 10), "@", 2)[0] + " jobs"
	orgID, err := h.q.CreateEmployerOrganization(c.Request.Context(), gensqlc.CreateEmployerOrganizationParams{
		Name:            name,
		CreatedByUserID: userID,
		CreatedAt:       now,
	})
	if err != nil {
		return 0, "", err
	}
	err = h.q.CreateEmployerOrganizationOwnerMembership(c.Request.Context(), gensqlc.CreateEmployerOrganizationOwnerMembershipParams{
		OrganizationID: orgID,
		UserID:         userID,
		CreatedAt:      now,
	})
	return orgID, "owner", err
}

func (h *Handler) requireOrganizationMember(c *gin.Context, orgID, userID int64) (int64, string, error) {
	role, err := h.q.GetEmployerOrganizationMemberRole(c.Request.Context(), gensqlc.GetEmployerOrganizationMemberRoleParams{
		OrganizationID: orgID,
		UserID:         userID,
	})
	if err != nil {
		return 0, "", err
	}
	return orgID, role, nil
}

func (h *Handler) requireJobMember(c *gin.Context, jobID, userID int64) (employerJob, string, error) {
	jobRow, err := h.q.GetEmployerJobForMemberCheck(c.Request.Context(), jobID)
	if err != nil {
		return employerJob{}, "", err
	}
	role, err := h.q.GetEmployerOrganizationMemberRole(c.Request.Context(), gensqlc.GetEmployerOrganizationMemberRoleParams{
		OrganizationID: jobRow.OrganizationID,
		UserID:         userID,
	})
	if err != nil {
		return employerJob{}, "", err
	}
	return employerJob{
		ID:               jobRow.ID,
		Status:           jobRow.Status,
		PostingFeeStatus: jobRow.PostingFeeStatus,
	}, role, nil
}

func (h *Handler) addAuditEvent(c *gin.Context, jobID, actorUserID int64, eventType string, detail map[string]any) error {
	var detailJSON string
	detailValid := false
	if detail != nil {
		body, _ := json.Marshal(detail)
		detailJSON = string(body)
		detailValid = true
	}
	return h.q.InsertEmployerJobAuditEvent(c.Request.Context(), gensqlc.InsertEmployerJobAuditEventParams{
		EmployerJobID: jobID,
		ActorUserID:   pgtype.Int8{Int64: actorUserID, Valid: true},
		EventType:     eventType,
		DetailJson:    pgtype.Text{String: detailJSON, Valid: detailValid},
		CreatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
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
	row, err := h.q.GetEmployerJobByID(c.Request.Context(), jobID)
	if err != nil {
		return jobResponse{}, err
	}
	var item jobResponse
	item.ID = row.ID
	item.OrganizationID = row.OrganizationID
	item.Status = row.Status
	item.PostingFeeUSD = int(row.PostingFeeUsd)
	item.PostingStatus = row.PostingFeeStatus
	item.CreatedAt = row.CreatedAt
	item.UpdatedAt = row.UpdatedAt
	item.Title = nullableString(row.Title)
	item.Slug = nullableString(row.Slug)
	item.Department = nullableString(row.Department)
	item.Description = nullableString(row.Description)
	item.Requirements = nullableString(row.Requirements)
	item.Benefits = nullableString(row.Benefits)
	item.EmploymentType = nullableString(row.EmploymentType)
	item.LocationType = nullableString(row.LocationType)
	item.Seniority = nullableString(row.Seniority)
	item.ApplyURL = nullableString(row.ApplyUrl)
	item.ApplyEmail = nullableString(row.ApplyEmail)
	item.SalaryCurrency = nullableString(row.SalaryCurrency)
	item.SalaryPeriod = nullableString(row.SalaryPeriod)
	item.SalaryMin = nullableFloat64(row.SalaryMin)
	item.SalaryMax = nullableFloat64(row.SalaryMax)
	item.PostingPaidAt = nullableString(row.PostingFeePaidAt)
	item.PublishedAt = nullableString(row.PublishedAt)
	item.ClosedAt = nullableString(row.ClosedAt)
	item.ExpiresAt = nullableString(row.ExpiresAt)
	if row.LocationsJson.Valid && strings.TrimSpace(row.LocationsJson.String) != "" {
		_ = json.Unmarshal([]byte(row.LocationsJson.String), &item.Locations)
	}
	if row.TechStack.Valid && strings.TrimSpace(row.TechStack.String) != "" {
		_ = json.Unmarshal([]byte(row.TechStack.String), &item.TechStack)
	}
	return item, nil
}

func nullableString(value pgtype.Text) *string {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	v := value.String
	return &v
}

func nullableFloat64(value pgtype.Float8) *float64 {
	if !value.Valid {
		return nil
	}
	v := value.Float64
	return &v
}

func marshalStringSlice(values []string) *string {
	if len(values) == 0 {
		return nil
	}
	body, _ := json.Marshal(values)
	value := string(body)
	return &value
}

func nullableText(value *string) pgtype.Text {
	if value == nil {
		return pgtype.Text{}
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: trimmed, Valid: true}
}

func nullableFloat(value *float64) pgtype.Float8 {
	if value == nil {
		return pgtype.Float8{}
	}
	return pgtype.Float8{Float64: *value, Valid: true}
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
