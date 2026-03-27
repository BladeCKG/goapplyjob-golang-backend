package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/email"
	"goapplyjob-golang-backend/internal/jobs"
	"goapplyjob-golang-backend/internal/parsedaiclassifier"
	"net/mail"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const adminEmailsEnv = "ADMIN_EMAILS"

type Handler struct {
	cfg          config.Config
	db           *database.DB
	auth         *auth.Handler
	emailService *email.Service
}

func NewHandler(cfg config.Config, db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{
		cfg:          cfg,
		db:           db,
		auth:         authHandler,
		emailService: email.NewService(cfg),
	}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/admin/status", h.status)
	router.GET("/admin/users", h.listUsers)
	router.PATCH("/admin/users/:userID/subscription", h.upsertUserSubscription)
	router.DELETE("/admin/users/:userID", h.deleteUser)
	router.POST("/admin/users/marketing-email", h.sendMarketingEmail)
	router.POST("/admin/parsed-jobs/marketing-email", h.sendMarketingEmailForParsedJobs)
	router.GET("/admin/watcher-payloads", h.listWatcherPayloads)
	router.PATCH("/admin/watcher-payloads/:payloadID", h.updateWatcherPayload)
	router.DELETE("/admin/watcher-payloads/:payloadID", h.deleteWatcherPayload)
	router.GET("/admin/raw-us-jobs", h.listRawUSJobs)
	router.PATCH("/admin/raw-us-jobs/:jobID", h.updateRawUSJob)
	router.DELETE("/admin/raw-us-jobs/:jobID", h.deleteRawUSJob)
	router.GET("/admin/watcher-states", h.listWatcherStates)
	router.PATCH("/admin/watcher-states/:stateID", h.updateWatcherState)
	router.DELETE("/admin/watcher-states/:stateID", h.deleteWatcherState)
	router.GET("/admin/worker-states", h.listWorkerStates)
	router.PATCH("/admin/worker-states/:stateID", h.updateWorkerState)
	router.DELETE("/admin/worker-states/:stateID", h.deleteWorkerState)
	router.GET("/admin/parsed-jobs", h.listParsedJobs)
	router.POST("/admin/parsed-jobs/:jobID/auto-categorize", h.autoCategorizeParsedJob)
	router.PATCH("/admin/parsed-jobs/:jobID", h.updateParsedJob)
	router.DELETE("/admin/parsed-jobs/:jobID", h.deleteParsedJob)
	router.GET("/admin/parsed-companies", h.listParsedCompanies)
	router.PATCH("/admin/parsed-companies/:companyID", h.updateParsedCompany)
	router.DELETE("/admin/parsed-companies/:companyID", h.deleteParsedCompany)
}

func (h *Handler) status(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"is_admin": isAdminEmail(user.Email)})
}

func (h *Handler) listUsers(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	limit, offset := queryLimitOffset(c, 200, 1000)
	parsedFilters, err := parseAdminFilters(c.Query("filters"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	orderClause, err := queryOrderClause(c, map[string]string{
		"id":                    "id",
		"email":                 "email",
		"created_at":            "created_at",
		"last_seen_at":          "last_seen_at",
		"last_job_filters_json": "last_job_filters_json",
	}, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	filters := []string{}
	args := []any{}
	filterDefinitions := map[string]filterDef{
		"id":                    {columnExpr: "id", valueType: "int"},
		"email":                 {columnExpr: "email", valueType: "text"},
		"created_at":            {columnExpr: "created_at", valueType: "datetime"},
		"last_seen_at":          {columnExpr: "last_seen_at", valueType: "datetime"},
		"last_job_filters_json": {columnExpr: "last_job_filters_json::text", valueType: "text"},
	}
	for _, item := range parsedFilters {
		def, ok := filterDefinitions[item.Column]
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Unsupported user filter column: " + item.Column})
			return
		}
		predicate, predicateArgs, err := buildColumnFilterSQL(def, item)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
			return
		}
		filters = append(filters, predicate)
		args = append(args, predicateArgs...)
	}

	total := 0
	totalQuery := `SELECT COUNT(id) FROM auth_users`
	query := `SELECT id, email, last_seen_at, last_job_filters_json, created_at
		 FROM auth_users`
	if len(filters) > 0 {
		where := " WHERE " + strings.Join(filters, " AND ")
		totalQuery += where
		query += where
	}
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), totalQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list users"})
		return
	}

	query += orderClause + ` LIMIT ? OFFSET ?`
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list users"})
		return
	}
	defer rows.Close()

	items := make([]gin.H, 0, limit)
	for rows.Next() {
		var (
			id             int64
			email          string
			lastSeen       sql.NullString
			lastJobFilters sql.NullString
			createdAt      string
		)
		if err := rows.Scan(&id, &email, &lastSeen, &lastJobFilters, &createdAt); err != nil {
			continue
		}
		item := gin.H{
			"id":                    id,
			"email":                 email,
			"created_at":            createdAt,
			"last_seen_at":          nil,
			"last_job_filters_json": nil,
		}
		if lastSeen.Valid && strings.TrimSpace(lastSeen.String) != "" {
			item["last_seen_at"] = lastSeen.String
		}
		if lastJobFilters.Valid && strings.TrimSpace(lastJobFilters.String) != "" {
			var parsed any
			if err := json.Unmarshal([]byte(lastJobFilters.String), &parsed); err == nil {
				item["last_job_filters_json"] = parsed
			} else {
				item["last_job_filters_json"] = lastJobFilters.String
			}
		}
		var (
			subID       int64
			planCode    string
			planName    string
			startsAt    string
			endsAt      string
			subIsActive bool
		)
		err := h.db.SQL.QueryRowContext(c.Request.Context(),
			`SELECT s.id, p.code, p.name, s.starts_at, s.ends_at, s.is_active
			 FROM user_subscriptions s
			 JOIN pricing_plans p ON p.id = s.pricing_plan_id
			 WHERE s.user_id = ?
			 ORDER BY s.ends_at DESC, s.created_at DESC
			 LIMIT 1`, id).Scan(&subID, &planCode, &planName, &startsAt, &endsAt, &subIsActive)
		if err == nil {
			item["latest_subscription"] = gin.H{
				"id":        subID,
				"plan_code": planCode,
				"plan_name": planName,
				"starts_at": startsAt,
				"ends_at":   endsAt,
				"is_active": subIsActive,
			}
		}
		items = append(items, item)
	}
	c.JSON(http.StatusOK, gin.H{"total": total, "items": items})
}

func (h *Handler) upsertUserSubscription(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	userID, err := strconv.ParseInt(c.Param("userID"), 10, 64)
	if err != nil || userID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid user id"})
		return
	}
	var payload struct {
		PlanCode string `json:"plan_code"`
		StartsAt string `json:"starts_at"`
		EndsAt   string `json:"ends_at"`
		IsActive *bool  `json:"is_active"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	payload.PlanCode = strings.TrimSpace(payload.PlanCode)
	payload.StartsAt = strings.TrimSpace(payload.StartsAt)
	payload.EndsAt = strings.TrimSpace(payload.EndsAt)
	if payload.PlanCode == "" || payload.StartsAt == "" || payload.EndsAt == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "plan_code, starts_at and ends_at are required"})
		return
	}
	if _, err := parseTimestamp(payload.StartsAt); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid starts_at"})
		return
	}
	if _, err := parseTimestamp(payload.EndsAt); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid ends_at"})
		return
	}

	isActive := payload.IsActive != nil && *payload.IsActive
	isActive = isFutureTimestamp(payload.EndsAt)

	var (
		planID   int64
		planName string
	)
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id, name FROM pricing_plans WHERE code = ? LIMIT 1`, payload.PlanCode).Scan(&planID, &planName); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Pricing plan not found"})
		return
	}

	if _, execErr := h.db.SQL.ExecContext(c.Request.Context(),
		`UPDATE user_subscriptions
			 SET pricing_plan_id = ?, starts_at = ?, ends_at = ?, is_active = ?
			 WHERE user_id = ?`,
		planID, payload.StartsAt, payload.EndsAt, isActive, userID); execErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to upsert subscription"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id":   userID,
		"plan_code": payload.PlanCode,
		"plan_name": planName,
		"starts_at": payload.StartsAt,
		"ends_at":   payload.EndsAt,
		"is_active": isActive && isFutureTimestamp(payload.EndsAt),
	})
}

func (h *Handler) deleteUser(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	userID, err := strconv.ParseInt(c.Param("userID"), 10, 64)
	if err != nil || userID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid user id"})
		return
	}
	tx, err := h.db.SQL.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete user"})
		return
	}
	defer tx.Rollback()

	var exists int64
	if err := tx.QueryRowContext(c.Request.Context(), `SELECT id FROM auth_users WHERE id = ? LIMIT 1`, userID).Scan(&exists); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	ownedOrgQuery := `SELECT id FROM employer_organizations WHERE created_by_user_id = ?`
	if _, err := tx.ExecContext(c.Request.Context(),
		`DELETE FROM employer_job_audit_events
		  WHERE actor_user_id = ?
		     OR employer_job_id IN (
		          SELECT id
		            FROM employer_jobs
		           WHERE created_by_user_id = ?
		              OR organization_id IN (`+ownedOrgQuery+`)
		     )`,
		userID, userID, userID,
	); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete user"})
		return
	}
	if _, err := tx.ExecContext(c.Request.Context(),
		`DELETE FROM employer_jobs
		  WHERE created_by_user_id = ?
		     OR organization_id IN (`+ownedOrgQuery+`)`,
		userID, userID,
	); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete user"})
		return
	}
	if _, err := tx.ExecContext(c.Request.Context(),
		`DELETE FROM employer_organization_members
		  WHERE user_id = ?
		     OR organization_id IN (`+ownedOrgQuery+`)`,
		userID, userID,
	); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete user"})
		return
	}
	if _, err := tx.ExecContext(c.Request.Context(), `DELETE FROM employer_organizations WHERE created_by_user_id = ?`, userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete user"})
		return
	}
	for _, query := range []string{
		`DELETE FROM user_job_actions WHERE user_id = ?`,
		`DELETE FROM pricing_payments WHERE user_id = ?`,
		`DELETE FROM user_subscriptions WHERE user_id = ?`,
		`DELETE FROM auth_sessions WHERE user_id = ?`,
		`DELETE FROM auth_verification_codes WHERE user_id = ?`,
		`DELETE FROM auth_password_credentials WHERE user_id = ?`,
		`DELETE FROM auth_users WHERE id = ?`,
	} {
		if _, err := tx.ExecContext(c.Request.Context(), query, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete user"})
			return
		}
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete user"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": userID})
}

func (h *Handler) sendMarketingEmail(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	var payload struct {
		UserIDs   []int64 `json:"user_ids"`
		JobsLimit int     `json:"jobs_limit"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	if len(payload.UserIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_ids is required"})
		return
	}
	limit := payload.JobsLimit
	if limit <= 0 {
		limit = 8
	}
	if limit > 25 {
		limit = 25
	}

	type resultRow struct {
		UserID  int64  `json:"user_id"`
		Email   string `json:"email"`
		Status  string `json:"status"`
		Message string `json:"message,omitempty"`
	}
	results := []resultRow{}

	siteURL := strings.TrimRight(h.cfg.SiteURL, "/")
	siteName := strings.TrimSpace(h.cfg.SiteName)

	for _, userID := range payload.UserIDs {
		var emailAddr string
		var rawFilters sql.NullString
		if err := h.db.SQL.QueryRowContext(
			c.Request.Context(),
			`SELECT email, last_job_filters_json::text FROM auth_users WHERE id = ?`,
			userID,
		).Scan(&emailAddr, &rawFilters); err != nil {
			results = append(results, resultRow{UserID: userID, Status: "error", Message: "User not found"})
			continue
		}
		if strings.TrimSpace(emailAddr) == "" {
			results = append(results, resultRow{UserID: userID, Status: "error", Message: "Missing email"})
			continue
		}

		var filters jobs.LastJobFiltersPayload
		if rawFilters.Valid && strings.TrimSpace(rawFilters.String) != "" {
			if err := json.Unmarshal([]byte(rawFilters.String), &filters); err != nil {
				results = append(results, resultRow{UserID: userID, Email: emailAddr, Status: "error", Message: "Invalid filters JSON"})
				continue
			}
		}

		jobQuery, args := jobs.BuildEmailJobsQuery(filters, userID, limit)
		rows, err := h.db.SQL.QueryContext(c.Request.Context(), jobQuery, args...)
		if err != nil {
			results = append(results, resultRow{UserID: userID, Email: emailAddr, Status: "error", Message: "Query failed"})
			continue
		}
		jobsList := []email.MarketingJob{}
		for rows.Next() {
			var roleTitle, companyName, companyLogoURL, jobURL, slug sql.NullString
			var createdAt sql.NullString
			var categorizedTitle, categorizedFunction, salaryHumanText sql.NullString
			if err := rows.Scan(&roleTitle, &companyName, &companyLogoURL, &jobURL, &slug, &createdAt, &categorizedTitle, &categorizedFunction, &salaryHumanText); err != nil {
				continue
			}
			link := strings.TrimSpace(jobURL.String)
			if link == "" && strings.TrimSpace(slug.String) != "" {
				link = siteURL + "/jobs/" + strings.TrimSpace(slug.String)
			}
			jobsList = append(jobsList, email.MarketingJob{
				Title:               strings.TrimSpace(roleTitle.String),
				Company:             strings.TrimSpace(companyName.String),
				CompanyLogoURL:      strings.TrimSpace(companyLogoURL.String),
				URL:                 link,
				PostedAt:            strings.TrimSpace(createdAt.String),
				CategorizedTitle:    strings.TrimSpace(categorizedTitle.String),
				CategorizedFunction: strings.TrimSpace(categorizedFunction.String),
				Salary:              strings.TrimSpace(salaryHumanText.String),
			})
		}
		rows.Close()

		firstName := strings.Split(strings.TrimSpace(emailAddr), "@")[0]
		mailData := email.MarketingEmailData{
			SiteName:       siteName,
			SiteURL:        siteURL,
			SiteLogoURL:    siteURL + "/logo.png",
			FirstName:      firstName,
			BrowseJobsURL:  siteURL + "/us-remote-jobs",
			ManagePrefsURL: siteURL + "/account",
			UnsubscribeURL: siteURL + "/account",
			Jobs:           jobsList,
		}

		if err := h.emailService.SendMarketingEmail(emailAddr, mailData); err != nil {
			results = append(results, resultRow{UserID: userID, Email: emailAddr, Status: "error", Message: err.Error()})
			continue
		}
		results = append(results, resultRow{UserID: userID, Email: emailAddr, Status: "sent"})
	}

	c.JSON(http.StatusOK, gin.H{"items": results})
}

func (h *Handler) sendMarketingEmailForParsedJobs(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	var payload struct {
		Emails []string `json:"emails"`
		JobIDs []int64  `json:"job_ids"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}

	emails := normalizeMarketingEmails(payload.Emails)
	if len(emails) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "emails is required"})
		return
	}
	jobIDs := uniquePositiveInt64s(payload.JobIDs)
	if len(jobIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "job_ids is required"})
		return
	}

	jobsList, err := h.fetchMarketingJobsByParsedJobIDs(c.Request.Context(), jobIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load selected jobs"})
		return
	}

	type resultRow struct {
		Email   string `json:"email"`
		Status  string `json:"status"`
		Message string `json:"message,omitempty"`
	}
	results := make([]resultRow, 0, len(emails))
	for _, emailAddr := range emails {
		mailData := h.buildMarketingEmailData(emailAddr, jobsList)
		if err := h.emailService.SendMarketingEmail(emailAddr, mailData); err != nil {
			results = append(results, resultRow{Email: emailAddr, Status: "error", Message: err.Error()})
			continue
		}
		results = append(results, resultRow{Email: emailAddr, Status: "sent"})
	}

	c.JSON(http.StatusOK, gin.H{"items": results})
}

func (h *Handler) buildMarketingEmailData(emailAddr string, jobsList []email.MarketingJob) email.MarketingEmailData {
	siteURL := strings.TrimRight(h.cfg.SiteURL, "/")
	siteName := strings.TrimSpace(h.cfg.SiteName)
	firstName := strings.Split(strings.TrimSpace(emailAddr), "@")[0]
	return email.MarketingEmailData{
		SiteName:       siteName,
		SiteURL:        siteURL,
		SiteLogoURL:    siteURL + "/logo.png",
		FirstName:      firstName,
		BrowseJobsURL:  siteURL + "/us-remote-jobs",
		ManagePrefsURL: siteURL + "/account",
		UnsubscribeURL: siteURL + "/account",
		Jobs:           jobsList,
	}
}

func (h *Handler) fetchMarketingJobsByParsedJobIDs(ctx context.Context, jobIDs []int64) ([]email.MarketingJob, error) {
	placeholders := make([]string, 0, len(jobIDs))
	args := make([]any, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		placeholders = append(placeholders, "?")
		args = append(args, jobID)
	}

	rows, err := h.db.SQL.QueryContext(
		ctx,
		`SELECT p.id, p.role_title, c.name, c.profile_pic_url, p.url, p.slug, p.created_at_source, p.categorized_job_title, p.categorized_job_function, p.salary_human_text
		   FROM parsed_jobs p
		   LEFT JOIN parsed_companies c ON c.id = p.company_id
		  WHERE p.id IN (`+strings.Join(placeholders, ", ")+`)`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	siteURL := strings.TrimRight(h.cfg.SiteURL, "/")
	byID := map[int64]email.MarketingJob{}
	for rows.Next() {
		var (
			jobID                                              int64
			roleTitle, companyName, companyLogoURL, jobURL     sql.NullString
			slug, createdAt, categorizedTitle, categorizedFunc sql.NullString
			salaryHumanText                                    sql.NullString
		)
		if err := rows.Scan(&jobID, &roleTitle, &companyName, &companyLogoURL, &jobURL, &slug, &createdAt, &categorizedTitle, &categorizedFunc, &salaryHumanText); err != nil {
			return nil, err
		}
		link := strings.TrimSpace(jobURL.String)
		if link == "" && strings.TrimSpace(slug.String) != "" {
			link = siteURL + "/jobs/" + strings.TrimSpace(slug.String)
		}
		byID[jobID] = email.MarketingJob{
			Title:               strings.TrimSpace(roleTitle.String),
			Company:             strings.TrimSpace(companyName.String),
			CompanyLogoURL:      strings.TrimSpace(companyLogoURL.String),
			URL:                 link,
			PostedAt:            strings.TrimSpace(createdAt.String),
			CategorizedTitle:    strings.TrimSpace(categorizedTitle.String),
			CategorizedFunction: strings.TrimSpace(categorizedFunc.String),
			Salary:              strings.TrimSpace(salaryHumanText.String),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]email.MarketingJob, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		job, ok := byID[jobID]
		if !ok {
			continue
		}
		out = append(out, job)
	}
	return out, nil
}

func normalizeMarketingEmails(values []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		emailAddr := strings.TrimSpace(value)
		if emailAddr == "" {
			continue
		}
		parsed, err := mail.ParseAddress(emailAddr)
		if err != nil {
			continue
		}
		normalized := strings.ToLower(parsed.Address)
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, parsed.Address)
	}
	return out
}

func uniquePositiveInt64s(values []int64) []int64 {
	out := []int64{}
	seen := map[int64]struct{}{}
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (h *Handler) listWatcherPayloads(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	limit, offset := queryLimitOffset(c, 200, 1000)
	source := strings.TrimSpace(c.Query("source"))
	payloadType := strings.TrimSpace(c.Query("payload_type"))
	onlyUnconsumed := queryBoolDefault(c, "only_unconsumed", true)
	orderClause, err := queryOrderClause(c, map[string]string{
		"id":           "id",
		"source":       "source",
		"source_url":   "source_url",
		"payload_type": "payload_type",
		"body_text":    "body_text",
		"created_at":   "created_at",
		"consumed_at":  "consumed_at",
	}, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	filters := []string{}
	args := []any{}
	if source != "" {
		filters = append(filters, "source = ?")
		args = append(args, source)
	}
	if payloadType != "" {
		filters = append(filters, "payload_type = ?")
		args = append(args, payloadType)
	}
	if onlyUnconsumed {
		filters = append(filters, "consumed_at IS NULL")
	}

	query := `SELECT id, source, source_url, payload_type, body_text, created_at, consumed_at
		FROM watcher_payloads`
	totalQuery := `SELECT COUNT(id) FROM watcher_payloads`
	if len(filters) > 0 {
		where := " WHERE " + strings.Join(filters, " AND ")
		query += where
		totalQuery += where
	}
	query += orderClause + " LIMIT ? OFFSET ?"
	total := 0
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), totalQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list watcher payloads"})
		return
	}
	queryArgs := append(append([]any{}, args...), limit, offset)

	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list watcher payloads"})
		return
	}
	defer rows.Close()

	items := []gin.H{}
	for rows.Next() {
		var (
			id         int64
			sourceVal  string
			sourceURL  string
			payloadVal string
			bodyText   string
			createdAt  string
			consumedAt sql.NullString
		)
		if err := rows.Scan(&id, &sourceVal, &sourceURL, &payloadVal, &bodyText, &createdAt, &consumedAt); err != nil {
			continue
		}
		item := gin.H{
			"id":           id,
			"source":       sourceVal,
			"source_url":   sourceURL,
			"payload_type": payloadVal,
			"body_text":    bodyText,
			"created_at":   createdAt,
			"consumed_at":  nil,
		}
		if consumedAt.Valid {
			item["consumed_at"] = consumedAt.String
		}
		items = append(items, item)
	}
	c.JSON(http.StatusOK, gin.H{"total": total, "items": items})
}

func (h *Handler) updateWatcherPayload(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	payloadID, err := strconv.ParseInt(c.Param("payloadID"), 10, 64)
	if err != nil || payloadID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid payload id"})
		return
	}
	var exists int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM watcher_payloads WHERE id = ? LIMIT 1`, payloadID).Scan(&exists); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Watcher payload not found"})
		return
	}
	updates, args, ok := parsePatchUpdates(c, map[string]func(*json.RawMessage) (any, bool){
		"source":       jsonPatchString,
		"source_url":   jsonPatchString,
		"payload_type": jsonPatchString,
		"body_text":    jsonPatchString,
		"consumed_at":  jsonPatchStringOrNull,
	})
	if !ok {
		return
	}
	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No fields to update"})
		return
	}
	query := `UPDATE watcher_payloads SET ` + strings.Join(updates, ", ") + ` WHERE id = ?`
	args = append(args, payloadID)
	if _, err := h.db.SQL.ExecContext(c.Request.Context(), query, args...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update watcher payload"})
		return
	}
	h.respondWatcherPayload(c, payloadID)
}

func (h *Handler) deleteWatcherPayload(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	payloadID, err := strconv.ParseInt(c.Param("payloadID"), 10, 64)
	if err != nil || payloadID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid payload id"})
		return
	}
	result, err := h.db.SQL.ExecContext(c.Request.Context(), `DELETE FROM watcher_payloads WHERE id = ?`, payloadID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete watcher payload"})
		return
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Watcher payload not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": payloadID})
}

func (h *Handler) listRawUSJobs(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	limit, offset := queryLimitOffset(c, 200, 1000)
	source := strings.TrimSpace(c.Query("source"))
	onlyNotReady := queryBoolDefault(c, "only_not_ready", false)
	parsedFilters, err := parseAdminFilters(c.Query("filters"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	orderClause, err := queryOrderClause(c, map[string]string{
		"id":           "id",
		"source":       "source",
		"url":          "url",
		"post_date":    "post_date",
		"is_ready":     "is_ready",
		"is_skippable": "is_skippable",
		"is_parsed":    "is_parsed",
		"retry_count":  "retry_count",
		"raw_json":     "raw_json",
	}, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	filters := []string{}
	args := []any{}
	if source != "" {
		filters = append(filters, "source = ?")
		args = append(args, source)
	}
	if onlyNotReady {
		filters = append(filters, "is_ready = false")
	}
	filterDefinitions := map[string]filterDef{
		"id":           {columnExpr: "id", valueType: "int"},
		"source":       {columnExpr: "source", valueType: "text"},
		"url":          {columnExpr: "url", valueType: "text"},
		"post_date":    {columnExpr: "post_date", valueType: "datetime"},
		"is_ready":     {columnExpr: "is_ready", valueType: "bool"},
		"is_skippable": {columnExpr: "is_skippable", valueType: "bool"},
		"is_parsed":    {columnExpr: "is_parsed", valueType: "bool"},
		"retry_count":  {columnExpr: "retry_count", valueType: "int"},
		"raw_json":     {columnExpr: "raw_json", valueType: "text"},
	}
	for _, item := range parsedFilters {
		def, ok := filterDefinitions[item.Column]
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Unsupported raw job filter column: " + item.Column})
			return
		}
		predicate, predicateArgs, err := buildColumnFilterSQL(def, item)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
			return
		}
		filters = append(filters, predicate)
		args = append(args, predicateArgs...)
	}
	query := `SELECT id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json
		FROM raw_us_jobs`
	totalQuery := `SELECT COUNT(id) FROM raw_us_jobs`
	if len(filters) > 0 {
		where := " WHERE " + strings.Join(filters, " AND ")
		query += where
		totalQuery += where
	}
	query += orderClause + " LIMIT ? OFFSET ?"
	total := 0
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), totalQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list raw jobs"})
		return
	}
	queryArgs := append(append([]any{}, args...), limit, offset)

	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list raw jobs"})
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var (
			id          int64
			sourceVal   string
			url         string
			postDate    string
			isReady     bool
			isSkippable bool
			isParsed    bool
			retryCount  int
			rawJSON     sql.NullString
		)
		if err := rows.Scan(&id, &sourceVal, &url, &postDate, &isReady, &isSkippable, &isParsed, &retryCount, &rawJSON); err != nil {
			continue
		}
		item := gin.H{
			"id":           id,
			"source":       sourceVal,
			"url":          url,
			"post_date":    postDate,
			"is_ready":     isReady,
			"is_skippable": isSkippable,
			"is_parsed":    isParsed,
			"retry_count":  retryCount,
			"raw_json":     nil,
		}
		if rawJSON.Valid {
			item["raw_json"] = rawJSON.String
		}
		items = append(items, item)
	}
	c.JSON(http.StatusOK, gin.H{"total": total, "items": items})
}

func (h *Handler) updateRawUSJob(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	jobID, err := strconv.ParseInt(c.Param("jobID"), 10, 64)
	if err != nil || jobID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid raw job id"})
		return
	}
	var exists int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM raw_us_jobs WHERE id = ? LIMIT 1`, jobID).Scan(&exists); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Raw job not found"})
		return
	}
	updates, args, ok := parsePatchUpdates(c, map[string]func(*json.RawMessage) (any, bool){
		"source":       jsonPatchString,
		"url":          jsonPatchString,
		"post_date":    jsonPatchString,
		"is_ready":     jsonPatchBool,
		"is_skippable": jsonPatchBool,
		"is_parsed":    jsonPatchBool,
		"retry_count":  jsonPatchInt,
		"raw_json":     jsonPatchStringOrNull,
	})
	if !ok {
		return
	}
	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No fields to update"})
		return
	}
	query := `UPDATE raw_us_jobs SET ` + strings.Join(updates, ", ") + ` WHERE id = ?`
	args = append(args, jobID)
	if _, err := h.db.SQL.ExecContext(c.Request.Context(), query, args...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update raw job"})
		return
	}
	h.respondRawUSJob(c, jobID)
}

func (h *Handler) listWatcherStates(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	limit, offset := queryLimitOffset(c, 200, 1000)
	source := strings.TrimSpace(c.Query("source"))
	orderClause, err := queryOrderClause(c, map[string]string{
		"id":         "id",
		"source":     "source",
		"state_json": "state_json",
		"updated_at": "updated_at",
	}, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	query := `SELECT id, source, state_json, updated_at
		FROM watcher_states`
	totalQuery := `SELECT COUNT(id) FROM watcher_states`
	args := []any{}
	if source != "" {
		query += " WHERE source = ?"
		totalQuery += " WHERE source = ?"
		args = append(args, source)
	}
	query += orderClause + " LIMIT ? OFFSET ?"
	total := 0
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), totalQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list watcher states"})
		return
	}
	queryArgs := append(append([]any{}, args...), limit, offset)

	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list watcher states"})
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var (
			id        int64
			sourceVal string
			stateJSON sql.NullString
			updatedAt string
		)
		if err := rows.Scan(&id, &sourceVal, &stateJSON, &updatedAt); err != nil {
			continue
		}
		item := gin.H{
			"id":         id,
			"source":     sourceVal,
			"state_json": nil,
			"updated_at": updatedAt,
		}
		if stateJSON.Valid && strings.TrimSpace(stateJSON.String) != "" {
			var parsed any
			if err := json.Unmarshal([]byte(stateJSON.String), &parsed); err == nil {
				item["state_json"] = parsed
			} else {
				item["state_json"] = stateJSON.String
			}
		}
		items = append(items, item)
	}
	c.JSON(http.StatusOK, gin.H{"total": total, "items": items})
}

func (h *Handler) updateWatcherState(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	stateID, err := strconv.ParseInt(c.Param("stateID"), 10, 64)
	if err != nil || stateID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid state id"})
		return
	}
	var exists int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM watcher_states WHERE id = ? LIMIT 1`, stateID).Scan(&exists); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Watcher state not found"})
		return
	}
	updates, args, ok := parsePatchUpdates(c, map[string]func(*json.RawMessage) (any, bool){
		"source":     jsonPatchString,
		"state_json": jsonPatchJSONOrNull,
	})
	if !ok {
		return
	}
	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No fields to update"})
		return
	}
	query := `UPDATE watcher_states SET ` + strings.Join(updates, ", ") + ` WHERE id = ?`
	args = append(args, stateID)
	if _, err := h.db.SQL.ExecContext(c.Request.Context(), query, args...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update watcher state"})
		return
	}
	h.respondWatcherState(c, stateID)
}

func (h *Handler) deleteWatcherState(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	stateID, err := strconv.ParseInt(c.Param("stateID"), 10, 64)
	if err != nil || stateID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid watcher state id"})
		return
	}
	result, err := h.db.SQL.ExecContext(c.Request.Context(), `DELETE FROM watcher_states WHERE id = ?`, stateID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete watcher state"})
		return
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Watcher state not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": stateID})
}

func (h *Handler) listWorkerStates(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	limit, offset := queryLimitOffset(c, 200, 1000)
	workerName := strings.TrimSpace(c.Query("worker_name"))
	orderClause, err := queryOrderClause(c, map[string]string{
		"id":          "id",
		"worker_name": "worker_name",
		"state":       "state",
		"updated_at":  "updated_at",
	}, "updated_at")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	query := `SELECT id, worker_name, state::text, updated_at
		FROM worker_states`
	totalQuery := `SELECT COUNT(id) FROM worker_states`
	args := []any{}
	if workerName != "" {
		query += " WHERE worker_name = ?"
		totalQuery += " WHERE worker_name = ?"
		args = append(args, workerName)
	}
	query += orderClause + " LIMIT ? OFFSET ?"
	total := 0
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), totalQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list worker states"})
		return
	}
	queryArgs := append(append([]any{}, args...), limit, offset)

	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list worker states"})
		return
	}
	defer rows.Close()

	items := []gin.H{}
	for rows.Next() {
		var (
			id         int64
			workerName string
			stateText  sql.NullString
			updatedAt  string
		)
		if err := rows.Scan(&id, &workerName, &stateText, &updatedAt); err != nil {
			continue
		}
		item := gin.H{
			"id":          id,
			"worker_name": workerName,
			"state":       nil,
			"updated_at":  updatedAt,
		}
		if stateText.Valid && strings.TrimSpace(stateText.String) != "" {
			var parsed any
			if err := json.Unmarshal([]byte(stateText.String), &parsed); err == nil {
				item["state"] = parsed
			} else {
				item["state"] = stateText.String
			}
		}
		items = append(items, item)
	}
	c.JSON(http.StatusOK, gin.H{"total": total, "items": items})
}

func (h *Handler) updateWorkerState(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	stateID, err := strconv.ParseInt(c.Param("stateID"), 10, 64)
	if err != nil || stateID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid worker state id"})
		return
	}
	var exists int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM worker_states WHERE id = ? LIMIT 1`, stateID).Scan(&exists); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Worker state not found"})
		return
	}
	updates, args, ok := parsePatchUpdates(c, map[string]func(*json.RawMessage) (any, bool){
		"worker_name": jsonPatchString,
		"state":       jsonPatchJSONOrNull,
	})
	if !ok {
		return
	}
	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No fields to update"})
		return
	}
	updates = append(updates, "updated_at = ?")
	args = append(args, time.Now().UTC().Format(time.RFC3339Nano))
	query := `UPDATE worker_states SET ` + strings.Join(updates, ", ") + ` WHERE id = ?`
	args = append(args, stateID)
	if _, err := h.db.SQL.ExecContext(c.Request.Context(), query, args...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update worker state"})
		return
	}
	h.respondWorkerState(c, stateID)
}

func (h *Handler) deleteWorkerState(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	stateID, err := strconv.ParseInt(c.Param("stateID"), 10, 64)
	if err != nil || stateID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid worker state id"})
		return
	}
	result, err := h.db.SQL.ExecContext(c.Request.Context(), `DELETE FROM worker_states WHERE id = ?`, stateID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete worker state"})
		return
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Worker state not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": stateID})
}

func (h *Handler) listParsedJobs(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	limit, offset := queryLimitOffset(c, 200, 1000)
	q := strings.TrimSpace(c.Query("q"))
	source := strings.TrimSpace(c.Query("source"))
	parsedFilters, err := parseAdminFilters(c.Query("filters"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	orderClause, err := queryOrderClause(c, map[string]string{
		"id":                       "p.id",
		"raw_us_job_id":            "p.raw_us_job_id",
		"source":                   "r.source",
		"company_id":               "p.company_id",
		"external_job_id":          "p.external_job_id",
		"role_title":               "p.role_title",
		"role_description":         "p.role_description",
		"url":                      "p.url",
		"slug":                     "p.slug",
		"employment_type":          "p.employment_type",
		"location_type":            "p.location_type",
		"location_city":            "p.location_city",
		"location_us_states":       "p.location_us_states",
		"location_countries":       "p.location_countries",
		"categorized_job_title":    "p.categorized_job_title",
		"categorized_job_function": "p.categorized_job_function",
		"tech_stack":               "p.tech_stack",
		"salary_type":              "p.salary_type",
		"salary_currency_code":     "p.salary_currency_code",
		"salary_currency_symbol":   "p.salary_currency_symbol",
		"salary_min_usd":           "p.salary_min_usd",
		"salary_max_usd":           "p.salary_max_usd",
		"is_entry_level":           "p.is_entry_level",
		"is_junior":                "p.is_junior",
		"is_mid_level":             "p.is_mid_level",
		"is_senior":                "p.is_senior",
		"is_lead":                  "p.is_lead",
		"created_at_source":        "p.created_at_source",
		"updated_at":               "p.updated_at",
	}, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	filters := []string{}
	args := []any{}
	if source != "" {
		filters = append(filters, "r.source = ?")
		args = append(args, source)
	}
	if q != "" {
		needle := "%" + q + "%"
		filters = append(filters, `(p.role_title LIKE ? OR p.external_job_id LIKE ? OR p.url LIKE ? OR p.categorized_job_title LIKE ? OR p.categorized_job_function LIKE ? OR p.location_us_states::text LIKE ? OR p.location_countries::text LIKE ? OR p.tech_stack::text LIKE ?)`)
		for i := 0; i < 8; i++ {
			args = append(args, needle)
		}
	}
	filterDefinitions := map[string]filterDef{
		"id":                       {columnExpr: "p.id", valueType: "int"},
		"raw_us_job_id":            {columnExpr: "p.raw_us_job_id", valueType: "int"},
		"source":                   {columnExpr: "r.source", valueType: "text"},
		"company_id":               {columnExpr: "p.company_id", valueType: "int"},
		"external_job_id":          {columnExpr: "p.external_job_id", valueType: "text"},
		"role_title":               {columnExpr: "p.role_title", valueType: "text"},
		"role_description":         {columnExpr: "p.role_description", valueType: "text"},
		"url":                      {columnExpr: "p.url", valueType: "text"},
		"slug":                     {columnExpr: "p.slug", valueType: "text"},
		"employment_type":          {columnExpr: "p.employment_type", valueType: "text"},
		"location_type":            {columnExpr: "p.location_type", valueType: "text"},
		"location_city":            {columnExpr: "p.location_city", valueType: "text"},
		"location_us_states":       {columnExpr: "p.location_us_states::text", valueType: "text"},
		"location_countries":       {columnExpr: "p.location_countries::text", valueType: "text"},
		"categorized_job_title":    {columnExpr: "p.categorized_job_title", valueType: "text"},
		"categorized_job_function": {columnExpr: "p.categorized_job_function", valueType: "text"},
		"tech_stack":               {columnExpr: "p.tech_stack::text", valueType: "text"},
		"salary_type":              {columnExpr: "p.salary_type", valueType: "text"},
		"salary_currency_code":     {columnExpr: "p.salary_currency_code", valueType: "text"},
		"salary_currency_symbol":   {columnExpr: "p.salary_currency_symbol", valueType: "text"},
		"salary_min_usd":           {columnExpr: "p.salary_min_usd", valueType: "float"},
		"salary_max_usd":           {columnExpr: "p.salary_max_usd", valueType: "float"},
		"is_entry_level":           {columnExpr: "p.is_entry_level", valueType: "bool"},
		"is_junior":                {columnExpr: "p.is_junior", valueType: "bool"},
		"is_mid_level":             {columnExpr: "p.is_mid_level", valueType: "bool"},
		"is_senior":                {columnExpr: "p.is_senior", valueType: "bool"},
		"is_lead":                  {columnExpr: "p.is_lead", valueType: "bool"},
		"created_at_source":        {columnExpr: "p.created_at_source", valueType: "datetime"},
		"updated_at":               {columnExpr: "p.updated_at", valueType: "datetime"},
	}
	for _, item := range parsedFilters {
		def, ok := filterDefinitions[item.Column]
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Unsupported parsed job filter column: " + item.Column})
			return
		}
		predicate, predicateArgs, err := buildColumnFilterSQL(def, item)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
			return
		}
		filters = append(filters, predicate)
		args = append(args, predicateArgs...)
	}

	baseFrom := ` FROM parsed_jobs p JOIN raw_us_jobs r ON r.id = p.raw_us_job_id`
	where := ""
	if len(filters) > 0 {
		where = " WHERE " + strings.Join(filters, " AND ")
	}
	total := 0
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(p.id)`+baseFrom+where, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list parsed jobs"})
		return
	}
	query := `SELECT p.id, p.raw_us_job_id, r.source, p.company_id, p.external_job_id, p.role_title, p.role_description, p.url, p.slug, p.employment_type, p.location_type, p.location_city, p.location_us_states::text, p.location_countries::text, p.categorized_job_title, p.categorized_job_function, p.tech_stack::text, p.salary_type, p.salary_currency_code, p.salary_currency_symbol, p.salary_min_usd, p.salary_max_usd, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead, p.created_at_source, p.updated_at` +
		baseFrom + where + orderClause + ` LIMIT ? OFFSET ?`
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list parsed jobs"})
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var (
			id, rawUSJobID    int64
			sourceVal         string
			companyID         sql.NullInt64
			externalJobID     sql.NullString
			roleTitle         sql.NullString
			roleDesc          sql.NullString
			url               sql.NullString
			slug              sql.NullString
			employmentType    sql.NullString
			locationType      sql.NullString
			locationCity      sql.NullString
			locationStates    sql.NullString
			locationCountries sql.NullString
			categoryTitle     sql.NullString
			categoryFunc      sql.NullString
			techStack         sql.NullString
			salaryType        sql.NullString
			salaryCurrency    sql.NullString
			salarySymbol      sql.NullString
			salaryMinUSD      sql.NullFloat64
			salaryMaxUSD      sql.NullFloat64
			isEntry           sql.NullBool
			isJunior          sql.NullBool
			isMid             sql.NullBool
			isSenior          sql.NullBool
			isLead            sql.NullBool
			createdAt         sql.NullString
			updatedAt         sql.NullString
		)
		if err := rows.Scan(&id, &rawUSJobID, &sourceVal, &companyID, &externalJobID, &roleTitle, &roleDesc, &url, &slug, &employmentType, &locationType, &locationCity, &locationStates, &locationCountries, &categoryTitle, &categoryFunc, &techStack, &salaryType, &salaryCurrency, &salarySymbol, &salaryMinUSD, &salaryMaxUSD, &isEntry, &isJunior, &isMid, &isSenior, &isLead, &createdAt, &updatedAt); err != nil {
			continue
		}
		items = append(items, gin.H{
			"id":                       id,
			"raw_us_job_id":            rawUSJobID,
			"source":                   sourceVal,
			"company_id":               nullableInt(companyID),
			"external_job_id":          nullableString(externalJobID),
			"role_title":               nullableString(roleTitle),
			"role_description":         nullableString(roleDesc),
			"url":                      nullableString(url),
			"slug":                     nullableString(slug),
			"employment_type":          nullableString(employmentType),
			"location_type":            nullableString(locationType),
			"location_city":            nullableString(locationCity),
			"location_us_states":       parseJSONStringArray(locationStates),
			"location_countries":       parseJSONStringArray(locationCountries),
			"categorized_job_title":    nullableString(categoryTitle),
			"categorized_job_function": nullableString(categoryFunc),
			"tech_stack":               parseJSONStringArray(techStack),
			"salary_type":              nullableString(salaryType),
			"salary_currency_code":     nullableString(salaryCurrency),
			"salary_currency_symbol":   nullableString(salarySymbol),
			"salary_min_usd":           nullableFloatPtr(salaryMinUSD),
			"salary_max_usd":           nullableFloatPtr(salaryMaxUSD),
			"is_entry_level":           nullableBool(isEntry),
			"is_junior":                nullableBool(isJunior),
			"is_mid_level":             nullableBool(isMid),
			"is_senior":                nullableBool(isSenior),
			"is_lead":                  nullableBool(isLead),
			"created_at_source":        nullableString(createdAt),
			"updated_at":               nullableString(updatedAt),
		})
	}
	c.JSON(http.StatusOK, gin.H{"total": total, "items": items})
}

func (h *Handler) updateParsedJob(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	jobID, err := strconv.ParseInt(c.Param("jobID"), 10, 64)
	if err != nil || jobID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid parsed job id"})
		return
	}
	var exists int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM parsed_jobs WHERE id = ? LIMIT 1`, jobID).Scan(&exists); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Parsed job not found"})
		return
	}
	updates, args, ok := parsePatchUpdates(c, map[string]func(*json.RawMessage) (any, bool){
		"company_id":               jsonPatchIntOrNull,
		"external_job_id":          jsonPatchStringOrNull,
		"role_title":               jsonPatchStringOrNull,
		"role_description":         jsonPatchStringOrNull,
		"url":                      jsonPatchStringOrNull,
		"slug":                     jsonPatchStringOrNull,
		"employment_type":          jsonPatchStringOrNull,
		"location_type":            jsonPatchStringOrNull,
		"location_city":            jsonPatchStringOrNull,
		"location_us_states":       jsonPatchStringArrayOrNull,
		"location_countries":       jsonPatchStringArrayOrNull,
		"categorized_job_title":    jsonPatchStringOrNull,
		"categorized_job_function": jsonPatchStringOrNull,
		"tech_stack":               jsonPatchStringArrayOrNull,
		"salary_type":              jsonPatchStringOrNull,
		"salary_currency_code":     jsonPatchStringOrNull,
		"salary_currency_symbol":   jsonPatchStringOrNull,
		"salary_min_usd":           jsonPatchFloatOrNull,
		"salary_max_usd":           jsonPatchFloatOrNull,
		"is_entry_level":           jsonPatchBoolOrNull,
		"is_junior":                jsonPatchBoolOrNull,
		"is_mid_level":             jsonPatchBoolOrNull,
		"is_senior":                jsonPatchBoolOrNull,
		"is_lead":                  jsonPatchBoolOrNull,
		"created_at_source":        jsonPatchStringOrNull,
	})
	if !ok {
		return
	}
	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No fields to update"})
		return
	}
	updates = append(updates, "updated_at = ?")
	args = append(args, time.Now().UTC().Format(time.RFC3339Nano), jobID)
	query := `UPDATE parsed_jobs SET ` + strings.Join(updates, ", ") + ` WHERE id = ?`
	if _, err := h.db.SQL.ExecContext(c.Request.Context(), query, args...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update parsed job"})
		return
	}
	// Reuse list endpoint shape.
	req := c.Request.Clone(c.Request.Context())
	q := req.URL.Query()
	q.Set("filters", fmt.Sprintf(`[{"column":"id","operator":"=","value":%d}]`, jobID))
	q.Set("limit", "1")
	q.Set("offset", "0")
	req.URL.RawQuery = q.Encode()
	c.Request = req
	h.listParsedJobs(c)
}

func (h *Handler) deleteParsedJob(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	jobID, err := strconv.ParseInt(c.Param("jobID"), 10, 64)
	if err != nil || jobID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid parsed job id"})
		return
	}
	result, err := h.db.SQL.ExecContext(c.Request.Context(), `DELETE FROM parsed_jobs WHERE id = ?`, jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete parsed job"})
		return
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Parsed job not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": jobID})
}

func (h *Handler) autoCategorizeParsedJob(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	jobID, err := strconv.ParseInt(c.Param("jobID"), 10, 64)
	if err != nil || jobID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid parsed job id"})
		return
	}

	var (
		source           string
		roleTitle        sql.NullString
		roleDescription  sql.NullString
		roleRequirements sql.NullString
		techStack        sql.NullString
	)
	err = h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT COALESCE(r.source, ''), p.role_title, p.role_description, p.role_requirements, p.tech_stack
		 FROM parsed_jobs p
		 LEFT JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		 WHERE p.id = ? LIMIT 1`, jobID).
		Scan(&source, &roleTitle, &roleDescription, &roleRequirements, &techStack)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Parsed job not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to auto-categorize parsed job"})
		return
	}

	aiClassifierSvc := parsedaiclassifier.New(parsedaiclassifier.Config{}, h.db)
	nextTitle, nextFunction, nextTechStack, err := aiClassifierSvc.SuggestCategoryWithTechStack(
		c.Request.Context(),
		roleRequirements.String,
		roleTitle.String,
		roleDescription.String,
		parseJSONStringArray(techStack),
		true, // overrideTechStack
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to auto-categorize parsed job"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"categorized_job_title":    nilIfBlank(nextTitle),
		"categorized_job_function": nilIfBlank(nextFunction),
		"tech_stack":               nextTechStack,
	})
}

func (h *Handler) listParsedCompanies(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	limit, offset := queryLimitOffset(c, 200, 1000)
	q := strings.TrimSpace(c.Query("q"))
	parsedFilters, err := parseAdminFilters(c.Query("filters"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	orderClause, err := queryOrderClause(c, map[string]string{
		"id":                    "id",
		"external_company_id":   "external_company_id",
		"name":                  "name",
		"slug":                  "slug",
		"tagline":               "tagline",
		"founded_year":          "founded_year",
		"home_page_url":         "home_page_url",
		"linkedin_url":          "linkedin_url",
		"profile_pic_url":       "profile_pic_url",
		"sponsors_h1b":          "sponsors_h1b",
		"employee_range":        "employee_range",
		"industry_specialities": "industry_specialities",
		"updated_at":            "updated_at",
	}, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	filters := []string{}
	args := []any{}
	if q != "" {
		needle := "%" + q + "%"
		filters = append(filters, `(name LIKE ? OR slug LIKE ? OR external_company_id LIKE ? OR home_page_url LIKE ? OR linkedin_url LIKE ?)`)
		for i := 0; i < 5; i++ {
			args = append(args, needle)
		}
	}
	filterDefinitions := map[string]filterDef{
		"id":                  {columnExpr: "id", valueType: "int"},
		"external_company_id": {columnExpr: "external_company_id", valueType: "text"},
		"name":                {columnExpr: "name", valueType: "text"},
		"slug":                {columnExpr: "slug", valueType: "text"},
		"tagline":             {columnExpr: "tagline", valueType: "text"},
		"founded_year":        {columnExpr: "founded_year", valueType: "text"},
		"home_page_url":       {columnExpr: "home_page_url", valueType: "text"},
		"linkedin_url":        {columnExpr: "linkedin_url", valueType: "text"},
		"profile_pic_url":     {columnExpr: "profile_pic_url", valueType: "text"},
		"sponsors_h1b":        {columnExpr: "sponsors_h1b", valueType: "bool"},
		"employee_range":      {columnExpr: "employee_range", valueType: "text"},
	}
	for _, item := range parsedFilters {
		def, ok := filterDefinitions[item.Column]
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Unsupported parsed company filter column: " + item.Column})
			return
		}
		predicate, predicateArgs, err := buildColumnFilterSQL(def, item)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
			return
		}
		filters = append(filters, predicate)
		args = append(args, predicateArgs...)
	}
	where := ""
	if len(filters) > 0 {
		where = " WHERE " + strings.Join(filters, " AND ")
	}
	total := 0
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(id) FROM parsed_companies`+where, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list parsed companies"})
		return
	}
	query := `SELECT id, external_company_id, name, slug, tagline, founded_year, home_page_url, linkedin_url, profile_pic_url, sponsors_h1b, employee_range FROM parsed_companies` + where + orderClause + ` LIMIT ? OFFSET ?`
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list parsed companies"})
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var (
			id                              int64
			externalID, name, slug, tagline sql.NullString
			foundedYear, homePage, linkedin sql.NullString
			profilePicURL                   sql.NullString
			sponsors                        sql.NullBool
			employeeRange                   sql.NullString
		)
		if err := rows.Scan(&id, &externalID, &name, &slug, &tagline, &foundedYear, &homePage, &linkedin, &profilePicURL, &sponsors, &employeeRange); err != nil {
			continue
		}
		items = append(items, gin.H{
			"id":                  id,
			"external_company_id": nullableString(externalID),
			"name":                nullableString(name),
			"slug":                nullableString(slug),
			"tagline":             nullableString(tagline),
			"founded_year":        nullableString(foundedYear),
			"home_page_url":       nullableString(homePage),
			"linkedin_url":        nullableString(linkedin),
			"profile_pic_url":     nullableString(profilePicURL),
			"sponsors_h1b":        nullableBool(sponsors),
			"employee_range":      nullableString(employeeRange),
		})
	}
	c.JSON(http.StatusOK, gin.H{"total": total, "items": items})
}

func (h *Handler) updateParsedCompany(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	companyID, err := strconv.ParseInt(c.Param("companyID"), 10, 64)
	if err != nil || companyID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid parsed company id"})
		return
	}
	var exists int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM parsed_companies WHERE id = ? LIMIT 1`, companyID).Scan(&exists); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Parsed company not found"})
		return
	}
	updates, args, ok := parsePatchUpdates(c, map[string]func(*json.RawMessage) (any, bool){
		"external_company_id": jsonPatchStringOrNull,
		"name":                jsonPatchStringOrNull,
		"slug":                jsonPatchStringOrNull,
		"tagline":             jsonPatchStringOrNull,
		"founded_year":        jsonPatchStringOrNull,
		"home_page_url":       jsonPatchStringOrNull,
		"linkedin_url":        jsonPatchStringOrNull,
		"profile_pic_url":     jsonPatchStringOrNull,
		"sponsors_h1b":        jsonPatchBoolOrNull,
		"employee_range":      jsonPatchStringOrNull,
	})
	if !ok {
		return
	}
	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No fields to update"})
		return
	}
	query := `UPDATE parsed_companies SET ` + strings.Join(updates, ", ") + ` WHERE id = ?`
	args = append(args, companyID)
	if _, err := h.db.SQL.ExecContext(c.Request.Context(), query, args...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update parsed company"})
		return
	}
	// Reuse list endpoint shape.
	req := c.Request.Clone(c.Request.Context())
	q := req.URL.Query()
	q.Set("filters", fmt.Sprintf(`[{"column":"id","operator":"=","value":%d}]`, companyID))
	q.Set("limit", "1")
	q.Set("offset", "0")
	req.URL.RawQuery = q.Encode()
	c.Request = req
	h.listParsedCompanies(c)
}

func (h *Handler) deleteParsedCompany(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	companyID, err := strconv.ParseInt(c.Param("companyID"), 10, 64)
	if err != nil || companyID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid parsed company id"})
		return
	}
	result, err := h.db.SQL.ExecContext(c.Request.Context(), `DELETE FROM parsed_companies WHERE id = ?`, companyID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete parsed company"})
		return
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Parsed company not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": companyID})
}

func (h *Handler) deleteRawUSJob(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	jobID, err := strconv.ParseInt(c.Param("jobID"), 10, 64)
	if err != nil || jobID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid raw job id"})
		return
	}
	result, err := h.db.SQL.ExecContext(c.Request.Context(), `DELETE FROM raw_us_jobs WHERE id = ?`, jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete raw job"})
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to delete raw job"})
		return
	}
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Raw job not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": jobID})
}

func (h *Handler) respondWatcherPayload(c *gin.Context, id int64) {
	var (
		sourceVal  string
		sourceURL  string
		payloadVal string
		bodyText   string
		createdAt  string
		consumedAt sql.NullString
	)
	if err := h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT source, source_url, payload_type, body_text, created_at, consumed_at
		 FROM watcher_payloads
		 WHERE id = ?`, id).Scan(&sourceVal, &sourceURL, &payloadVal, &bodyText, &createdAt, &consumedAt); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load watcher payload"})
		return
	}
	resp := gin.H{
		"id":           id,
		"source":       sourceVal,
		"source_url":   sourceURL,
		"payload_type": payloadVal,
		"body_text":    bodyText,
		"created_at":   createdAt,
		"consumed_at":  nil,
	}
	if consumedAt.Valid {
		resp["consumed_at"] = consumedAt.String
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) respondRawUSJob(c *gin.Context, id int64) {
	var (
		sourceVal   string
		url         string
		postDate    string
		isReady     bool
		isSkippable bool
		isParsed    bool
		retryCount  int
		rawJSON     sql.NullString
	)
	if err := h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json
		 FROM raw_us_jobs
		 WHERE id = ?`, id).Scan(&sourceVal, &url, &postDate, &isReady, &isSkippable, &isParsed, &retryCount, &rawJSON); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load raw job"})
		return
	}
	resp := gin.H{
		"id":           id,
		"source":       sourceVal,
		"url":          url,
		"post_date":    postDate,
		"is_ready":     isReady,
		"is_skippable": isSkippable,
		"is_parsed":    isParsed,
		"retry_count":  retryCount,
		"raw_json":     nil,
	}
	if rawJSON.Valid {
		resp["raw_json"] = rawJSON.String
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) respondWatcherState(c *gin.Context, id int64) {
	var (
		sourceVal string
		stateJSON sql.NullString
		updatedAt string
	)
	if err := h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT source, state_json, updated_at
		 FROM watcher_states
		 WHERE id = ?`, id).Scan(&sourceVal, &stateJSON, &updatedAt); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load watcher state"})
		return
	}
	resp := gin.H{
		"id":         id,
		"source":     sourceVal,
		"state_json": nil,
		"updated_at": updatedAt,
	}
	if stateJSON.Valid && strings.TrimSpace(stateJSON.String) != "" {
		var parsed any
		if err := json.Unmarshal([]byte(stateJSON.String), &parsed); err == nil {
			resp["state_json"] = parsed
		} else {
			resp["state_json"] = stateJSON.String
		}
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) respondWorkerState(c *gin.Context, id int64) {
	var (
		workerName string
		stateText  sql.NullString
		updatedAt  string
	)
	if err := h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT worker_name, state::text, updated_at
		 FROM worker_states
		 WHERE id = ?`, id).Scan(&workerName, &stateText, &updatedAt); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load worker state"})
		return
	}
	resp := gin.H{
		"id":          id,
		"worker_name": workerName,
		"state":       nil,
		"updated_at":  updatedAt,
	}
	if stateText.Valid && strings.TrimSpace(stateText.String) != "" {
		var parsed any
		if err := json.Unmarshal([]byte(stateText.String), &parsed); err == nil {
			resp["state"] = parsed
		} else {
			resp["state"] = stateText.String
		}
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) requireAdmin(c *gin.Context) (*auth.User, bool) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return nil, false
	}
	if !isAdminEmail(user.Email) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Admin access required"})
		return nil, false
	}
	return user, true
}

func isAdminEmail(email string) bool {
	adminEmails := config.GetenvCSVSet(adminEmailsEnv, "")
	if len(adminEmails) == 0 {
		return false
	}
	_, ok := adminEmails[strings.ToLower(strings.TrimSpace(email))]
	return ok
}

func queryLimitOffset(c *gin.Context, defaultLimit, maxLimit int) (int, int) {
	limit := defaultLimit
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	offset := 0
	if raw := strings.TrimSpace(c.Query("offset")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	return limit, offset
}

func queryOrderClause(c *gin.Context, allowed map[string]string, defaultColumn string) (string, error) {
	column := strings.TrimSpace(c.Query("order_by"))
	if column == "" {
		column = defaultColumn
	}
	expr, ok := allowed[column]
	if !ok {
		return "", fmt.Errorf("Unsupported order column: %s", column)
	}

	direction := strings.ToLower(strings.TrimSpace(c.Query("order_dir")))
	if direction == "" {
		direction = "desc"
	}
	if direction != "asc" && direction != "desc" {
		return "", fmt.Errorf("Unsupported order direction: %s", direction)
	}
	return fmt.Sprintf(" ORDER BY %s %s NULLS LAST", expr, strings.ToUpper(direction)), nil
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

func parseTimestamp(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, sql.ErrNoRows
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse("2006-01-02T15:04", value); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse("2006-01-02T15:04:05", value); err == nil {
		return parsed, nil
	}
	if parsed, err := time.ParseInLocation("2006-01-02T15:04", value, time.Local); err == nil {
		return parsed, nil
	}
	if parsed, err := time.ParseInLocation("2006-01-02T15:04:05", value, time.Local); err == nil {
		return parsed, nil
	}
	return time.Parse(time.RFC3339, value)
}

func isFutureTimestamp(value string) bool {
	timestamp, err := parseTimestamp(value)
	if err != nil {
		return false
	}
	return timestamp.UTC().After(time.Now().UTC())
}

type filterDef struct {
	columnExpr string
	valueType  string
}

type adminColumnFilter struct {
	Column   string      `json:"column"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
	ValueTo  interface{} `json:"value_to"`
}

var supportedFilterOperators = map[string]struct{}{
	"=": {}, "!=": {}, "<": {}, "<=": {}, ">": {}, ">=": {},
	"contains": {}, "does not contain": {}, "begins with": {}, "does not begin with": {}, "ends with": {}, "does not end with": {},
	"is null": {}, "is not null": {}, "is empty": {}, "is not empty": {}, "is between": {}, "is not between": {}, "is in list": {}, "is not in list": {},
}

func parseAdminFilters(raw string) ([]adminColumnFilter, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var filters []adminColumnFilter
	if err := json.Unmarshal([]byte(raw), &filters); err != nil {
		return nil, fmt.Errorf("Invalid filters JSON: %v", err)
	}
	for idx := range filters {
		filters[idx].Column = strings.TrimSpace(filters[idx].Column)
		filters[idx].Operator = strings.ToLower(strings.TrimSpace(filters[idx].Operator))
		if filters[idx].Column == "" || filters[idx].Operator == "" {
			return nil, fmt.Errorf("Invalid filter: column and operator are required")
		}
		if _, ok := supportedFilterOperators[filters[idx].Operator]; !ok {
			return nil, fmt.Errorf("Unsupported operator: %s", filters[idx].Operator)
		}
	}
	return filters, nil
}

func buildColumnFilterSQL(def filterDef, filter adminColumnFilter) (string, []any, error) {
	column := def.columnExpr
	op := filter.Operator
	if op == "is null" {
		return column + " IS NULL", nil, nil
	}
	if op == "is not null" {
		return column + " IS NOT NULL", nil, nil
	}
	if op == "is empty" {
		if def.valueType == "text" {
			return "(" + column + " = '' OR " + column + " IS NULL)", nil, nil
		}
		return column + " IS NULL", nil, nil
	}
	if op == "is not empty" {
		if def.valueType == "text" {
			return "(" + column + " IS NOT NULL AND " + column + " != '')", nil, nil
		}
		return column + " IS NOT NULL", nil, nil
	}

	parseOne := func(value interface{}) (interface{}, error) {
		switch def.valueType {
		case "text":
			return fmt.Sprintf("%v", value), nil
		case "datetime":
			switch item := value.(type) {
			case string:
				parsed, err := parseTimestamp(item)
				if err != nil {
					return nil, fmt.Errorf("Invalid datetime value: %v", value)
				}
				return parsed, nil
			default:
				return nil, fmt.Errorf("Invalid datetime value: %v", value)
			}
		case "int":
			switch item := value.(type) {
			case float64:
				return int(item), nil
			case int:
				return item, nil
			case string:
				parsed, err := strconv.Atoi(strings.TrimSpace(item))
				if err != nil {
					return nil, fmt.Errorf("Invalid integer value: %v", value)
				}
				return parsed, nil
			default:
				return nil, fmt.Errorf("Invalid integer value: %v", value)
			}
		case "float":
			switch item := value.(type) {
			case float64:
				return item, nil
			case int:
				return float64(item), nil
			case string:
				parsed, err := strconv.ParseFloat(strings.TrimSpace(item), 64)
				if err != nil {
					return nil, fmt.Errorf("Invalid float value: %v", value)
				}
				return parsed, nil
			default:
				return nil, fmt.Errorf("Invalid float value: %v", value)
			}
		case "bool":
			switch item := value.(type) {
			case bool:
				return item, nil
			case float64:
				return item != 0, nil
			case string:
				switch strings.ToLower(strings.TrimSpace(item)) {
				case "1", "true", "yes", "on":
					return true, nil
				case "0", "false", "no", "off":
					return false, nil
				default:
					return nil, fmt.Errorf("Invalid boolean value: %v", value)
				}
			default:
				return nil, fmt.Errorf("Invalid boolean value: %v", value)
			}
		default:
			return value, nil
		}
	}

	if op == "contains" || op == "does not contain" || op == "begins with" || op == "does not begin with" || op == "ends with" || op == "does not end with" {
		value, err := parseOne(filter.Value)
		if err != nil {
			return "", nil, err
		}
		textValue := fmt.Sprintf("%v", value)
		pattern := "%" + textValue + "%"
		if op == "begins with" || op == "does not begin with" {
			pattern = textValue + "%"
		}
		if op == "ends with" || op == "does not end with" {
			pattern = "%" + textValue
		}
		expr := "LOWER(CAST(" + column + " AS TEXT)) LIKE LOWER(?)"
		if strings.HasPrefix(op, "does not") {
			expr = "NOT (" + expr + ")"
		}
		return expr, []any{pattern}, nil
	}
	if op == "is between" || op == "is not between" {
		left, err := parseOne(filter.Value)
		if err != nil {
			return "", nil, err
		}
		right, err := parseOne(filter.ValueTo)
		if err != nil {
			return "", nil, err
		}
		expr := column + " BETWEEN ? AND ?"
		if op == "is not between" {
			expr = "NOT (" + expr + ")"
		}
		return expr, []any{left, right}, nil
	}
	if op == "is in list" || op == "is not in list" {
		values := []any{}
		switch item := filter.Value.(type) {
		case []interface{}:
			for _, raw := range item {
				parsed, err := parseOne(raw)
				if err != nil {
					return "", nil, err
				}
				values = append(values, parsed)
			}
		default:
			for _, part := range strings.Split(fmt.Sprintf("%v", filter.Value), ",") {
				parsed, err := parseOne(strings.TrimSpace(part))
				if err != nil {
					return "", nil, err
				}
				values = append(values, parsed)
			}
		}
		if len(values) == 0 {
			return "", nil, fmt.Errorf("List filter requires values")
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")
		expr := column + " IN (" + placeholders + ")"
		if op == "is not in list" {
			expr = "NOT (" + expr + ")"
		}
		return expr, values, nil
	}

	value, err := parseOne(filter.Value)
	if err != nil {
		return "", nil, err
	}
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		return column + " " + op + " ?", []any{value}, nil
	default:
		return "", nil, fmt.Errorf("Unsupported operator: %s", op)
	}
}

func parsePatchUpdates(c *gin.Context, parsers map[string]func(*json.RawMessage) (any, bool)) ([]string, []any, bool) {
	var payload map[string]json.RawMessage
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return nil, nil, false
	}
	updates := []string{}
	args := []any{}
	for field, parser := range parsers {
		raw, exists := payload[field]
		if !exists {
			continue
		}
		parsed, ok := parser(&raw)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid " + field})
			return nil, nil, false
		}
		updates = append(updates, field+" = ?")
		args = append(args, parsed)
	}
	return updates, args, true
}

func jsonPatchString(raw *json.RawMessage) (any, bool) {
	var value string
	if err := json.Unmarshal(*raw, &value); err != nil {
		return nil, false
	}
	return strings.TrimSpace(value), true
}

func jsonPatchStringOrNull(raw *json.RawMessage) (any, bool) {
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "null" {
		return nil, true
	}
	value, ok := jsonPatchString(raw)
	if !ok {
		return nil, false
	}
	text, _ := value.(string)
	if text == "" {
		return nil, true
	}
	return text, true
}

func jsonPatchBool(raw *json.RawMessage) (any, bool) {
	var value bool
	if err := json.Unmarshal(*raw, &value); err != nil {
		return nil, false
	}
	return value, true
}

func jsonPatchInt(raw *json.RawMessage) (any, bool) {
	var value int
	if err := json.Unmarshal(*raw, &value); err != nil {
		return nil, false
	}
	return value, true
}

func jsonPatchIntOrNull(raw *json.RawMessage) (any, bool) {
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "null" {
		return nil, true
	}
	return jsonPatchInt(raw)
}

func jsonPatchFloatOrNull(raw *json.RawMessage) (any, bool) {
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "null" {
		return nil, true
	}
	var value float64
	if err := json.Unmarshal(*raw, &value); err != nil {
		return nil, false
	}
	return value, true
}

func jsonPatchBoolOrNull(raw *json.RawMessage) (any, bool) {
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "null" {
		return nil, true
	}
	return jsonPatchBool(raw)
}

func jsonPatchStringArrayOrNull(raw *json.RawMessage) (any, bool) {
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "null" {
		return nil, true
	}
	var values []string
	if err := json.Unmarshal(*raw, &values); err == nil {
		encoded, marshalErr := json.Marshal(values)
		if marshalErr != nil {
			return nil, false
		}
		return string(encoded), true
	}
	var generic []any
	if err := json.Unmarshal(*raw, &generic); err != nil {
		return nil, false
	}
	normalized := []string{}
	for _, item := range generic {
		text, _ := item.(string)
		text = strings.TrimSpace(text)
		if text != "" {
			normalized = append(normalized, text)
		}
	}
	encoded, marshalErr := json.Marshal(normalized)
	if marshalErr != nil {
		return nil, false
	}
	return string(encoded), true
}

func jsonPatchJSONOrNull(raw *json.RawMessage) (any, bool) {
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "null" {
		return nil, true
	}
	var asAny any
	if err := json.Unmarshal(*raw, &asAny); err != nil {
		return nil, false
	}
	encoded, err := json.Marshal(asAny)
	if err != nil {
		return nil, false
	}
	return string(encoded), true
}

func parseJSONStringArray(value sql.NullString) any {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	var parsed []string
	if err := json.Unmarshal([]byte(value.String), &parsed); err == nil {
		return parsed
	}
	var generic []any
	if err := json.Unmarshal([]byte(value.String), &generic); err == nil {
		out := []string{}
		for _, item := range generic {
			text, _ := item.(string)
			text = strings.TrimSpace(text)
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	}
	return nil
}

func nilIfBlank(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func nullableInt(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func nullableString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

func nullableBool(value sql.NullBool) any {
	if !value.Valid {
		return nil
	}
	return value.Bool
}

func nullableFloatPtr(value sql.NullFloat64) any {
	if !value.Valid {
		return nil
	}
	return value.Float64
}
