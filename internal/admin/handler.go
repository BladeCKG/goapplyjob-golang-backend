package admin

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/parsed"

	"github.com/gin-gonic/gin"
)

const adminEmailsEnv = "ADMIN_EMAILS"

type Handler struct {
	db   *database.DB
	auth *auth.Handler
}

func NewHandler(db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{db: db, auth: authHandler}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/admin/status", h.status)
	router.GET("/admin/users", h.listUsers)
	router.PATCH("/admin/users/:userID/subscription", h.upsertUserSubscription)
	router.GET("/admin/watcher-payloads", h.listWatcherPayloads)
	router.PATCH("/admin/watcher-payloads/:payloadID", h.updateWatcherPayload)
	router.GET("/admin/raw-us-jobs", h.listRawUSJobs)
	router.PATCH("/admin/raw-us-jobs/:jobID", h.updateRawUSJob)
	router.GET("/admin/watcher-states", h.listWatcherStates)
	router.PATCH("/admin/watcher-states/:stateID", h.updateWatcherState)
	router.GET("/admin/parsed-jobs", h.listParsedJobs)
	router.POST("/admin/parsed-jobs/:jobID/auto-categorize", h.autoCategorizeParsedJob)
	router.PATCH("/admin/parsed-jobs/:jobID", h.updateParsedJob)
	router.GET("/admin/parsed-companies", h.listParsedCompanies)
	router.PATCH("/admin/parsed-companies/:companyID", h.updateParsedCompany)
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
	total := 0
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(id) FROM auth_users`).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list users"})
		return
	}

	rows, err := h.db.SQL.QueryContext(c.Request.Context(),
		`SELECT id, email, created_at
		 FROM auth_users
		 ORDER BY id DESC
		 LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to list users"})
		return
	}
	defer rows.Close()

	items := make([]gin.H, 0, limit)
	for rows.Next() {
		var (
			id        int64
			email     string
			createdAt string
		)
		if err := rows.Scan(&id, &email, &createdAt); err != nil {
			continue
		}
		item := gin.H{
			"id":         id,
			"email":      email,
			"created_at": createdAt,
		}
		var (
			subID       int64
			planCode    string
			planName    string
			startsAt    string
			endsAt      string
			subIsActive int
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
				"is_active": subIsActive == 1 && isFutureTimestamp(endsAt),
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
	isActive := true
	if payload.IsActive != nil {
		isActive = *payload.IsActive
	}

	var existingUserID int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM auth_users WHERE id = ? LIMIT 1`, userID).Scan(&existingUserID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	var (
		planID   int64
		planName string
	)
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id, name FROM pricing_plans WHERE code = ? LIMIT 1`, payload.PlanCode).Scan(&planID, &planName); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Pricing plan not found"})
		return
	}

	var subscriptionID int64
	err = h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT id
		 FROM user_subscriptions
		 WHERE user_id = ?
		 ORDER BY ends_at DESC, created_at DESC
		 LIMIT 1`, userID).Scan(&subscriptionID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	isActiveInt := 0
	if isActive {
		isActiveInt = 1
	}
	if err == sql.ErrNoRows {
		result, execErr := h.db.SQL.ExecContext(c.Request.Context(),
			`INSERT INTO user_subscriptions (user_id, pricing_plan_id, starts_at, ends_at, is_active, created_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			userID, planID, payload.StartsAt, payload.EndsAt, isActiveInt, now)
		if execErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to upsert subscription"})
			return
		}
		subscriptionID, _ = result.LastInsertId()
	} else if err == nil {
		if _, execErr := h.db.SQL.ExecContext(c.Request.Context(),
			`UPDATE user_subscriptions
			 SET pricing_plan_id = ?, starts_at = ?, ends_at = ?, is_active = ?
			 WHERE id = ?`,
			planID, payload.StartsAt, payload.EndsAt, isActiveInt, subscriptionID); execErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to upsert subscription"})
			return
		}
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to upsert subscription"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":        subscriptionID,
		"plan_code": payload.PlanCode,
		"plan_name": planName,
		"starts_at": payload.StartsAt,
		"ends_at":   payload.EndsAt,
		"is_active": isActive && isFutureTimestamp(payload.EndsAt),
	})
}

func (h *Handler) listWatcherPayloads(c *gin.Context) {
	if _, ok := h.requireAdmin(c); !ok {
		return
	}
	limit, offset := queryLimitOffset(c, 200, 1000)
	source := strings.TrimSpace(c.Query("source"))
	payloadType := strings.TrimSpace(c.Query("payload_type"))
	onlyUnconsumed := queryBoolDefault(c, "only_unconsumed", true)

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
	query += " ORDER BY id DESC LIMIT ? OFFSET ?"
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

	filters := []string{}
	args := []any{}
	if source != "" {
		filters = append(filters, "source = ?")
		args = append(args, source)
	}
	if onlyNotReady {
		filters = append(filters, "is_ready = 0")
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
	query += " ORDER BY id DESC LIMIT ? OFFSET ?"
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
			isReady     int
			isSkippable int
			isParsed    int
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
			"is_ready":     isReady == 1,
			"is_skippable": isSkippable == 1,
			"is_parsed":    isParsed == 1,
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
		"is_ready":     jsonPatchBoolAsInt,
		"is_skippable": jsonPatchBoolAsInt,
		"is_parsed":    jsonPatchBoolAsInt,
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

	query := `SELECT id, source, state_json, updated_at
		FROM watcher_states`
	totalQuery := `SELECT COUNT(id) FROM watcher_states`
	args := []any{}
	if source != "" {
		query += " WHERE source = ?"
		totalQuery += " WHERE source = ?"
		args = append(args, source)
	}
	query += " ORDER BY id DESC LIMIT ? OFFSET ?"
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

	filters := []string{}
	args := []any{}
	if source != "" {
		filters = append(filters, "r.source = ?")
		args = append(args, source)
	}
	if q != "" {
		needle := "%" + q + "%"
		filters = append(filters, `(p.role_title LIKE ? OR p.external_job_id LIKE ? OR p.url LIKE ? OR p.categorized_job_title LIKE ? OR p.categorized_job_function LIKE ? OR p.location_us_states LIKE ? OR p.location_countries LIKE ? OR p.tech_stack LIKE ?)`)
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
		"employment_type":          {columnExpr: "p.employment_type", valueType: "text"},
		"location_type":            {columnExpr: "p.location_type", valueType: "text"},
		"location_city":            {columnExpr: "p.location_city", valueType: "text"},
		"location_us_states":       {columnExpr: "p.location_us_states", valueType: "text"},
		"location_countries":       {columnExpr: "p.location_countries", valueType: "text"},
		"categorized_job_title":    {columnExpr: "p.categorized_job_title", valueType: "text"},
		"categorized_job_function": {columnExpr: "p.categorized_job_function", valueType: "text"},
		"tech_stack":               {columnExpr: "p.tech_stack", valueType: "text"},
		"salary_type":              {columnExpr: "p.salary_type", valueType: "text"},
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
	query := `SELECT p.id, p.raw_us_job_id, r.source, p.company_id, p.external_job_id, p.role_title, p.role_description, p.url, p.employment_type, p.location_type, p.location_city, p.location_us_states, p.location_countries, p.categorized_job_title, p.categorized_job_function, p.tech_stack, p.salary_type, p.salary_min_usd, p.salary_max_usd, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead, p.created_at_source, p.updated_at` +
		baseFrom + where + ` ORDER BY p.id DESC LIMIT ? OFFSET ?`
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
			employmentType    sql.NullString
			locationType      sql.NullString
			locationCity      sql.NullString
			locationStates    sql.NullString
			locationCountries sql.NullString
			categoryTitle     sql.NullString
			categoryFunc      sql.NullString
			techStack         sql.NullString
			salaryType        sql.NullString
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
		if err := rows.Scan(&id, &rawUSJobID, &sourceVal, &companyID, &externalJobID, &roleTitle, &roleDesc, &url, &employmentType, &locationType, &locationCity, &locationStates, &locationCountries, &categoryTitle, &categoryFunc, &techStack, &salaryType, &salaryMinUSD, &salaryMaxUSD, &isEntry, &isJunior, &isMid, &isSenior, &isLead, &createdAt, &updatedAt); err != nil {
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
			"employment_type":          nullableString(employmentType),
			"location_type":            nullableString(locationType),
			"location_city":            nullableString(locationCity),
			"location_us_states":       parseJSONStringArray(locationStates),
			"location_countries":       parseJSONStringArray(locationCountries),
			"categorized_job_title":    nullableString(categoryTitle),
			"categorized_job_function": nullableString(categoryFunc),
			"tech_stack":               parseJSONStringArray(techStack),
			"salary_type":              nullableString(salaryType),
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
		"employment_type":          jsonPatchStringOrNull,
		"location_type":            jsonPatchStringOrNull,
		"location_city":            jsonPatchStringOrNull,
		"location_us_states":       jsonPatchStringArrayOrNull,
		"location_countries":       jsonPatchStringArrayOrNull,
		"categorized_job_title":    jsonPatchStringOrNull,
		"categorized_job_function": jsonPatchStringOrNull,
		"tech_stack":               jsonPatchStringArrayOrNull,
		"salary_type":              jsonPatchStringOrNull,
		"salary_min_usd":           jsonPatchFloatOrNull,
		"salary_max_usd":           jsonPatchFloatOrNull,
		"is_entry_level":           jsonPatchBoolAsIntOrNull,
		"is_junior":                jsonPatchBoolAsIntOrNull,
		"is_mid_level":             jsonPatchBoolAsIntOrNull,
		"is_senior":                jsonPatchBoolAsIntOrNull,
		"is_lead":                  jsonPatchBoolAsIntOrNull,
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
		source          string
		roleTitle       sql.NullString
		roleDescription sql.NullString
		techStack       sql.NullString
	)
	err = h.db.SQL.QueryRowContext(c.Request.Context(),
		`SELECT COALESCE(r.source, ''), p.role_title, p.role_description, p.tech_stack
		 FROM parsed_jobs p
		 LEFT JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		 WHERE p.id = ? LIMIT 1`, jobID).
		Scan(&source, &roleTitle, &roleDescription, &techStack)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Parsed job not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to auto-categorize parsed job"})
		return
	}

	parsedSvc := parsed.New(h.db)
	nextTitle, nextFunction, err := parsedSvc.SuggestCategory(
		c.Request.Context(),
		source,
		roleTitle.String,
		roleDescription.String,
		parseJSONStringArray(techStack),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to auto-categorize parsed job"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"categorized_job_title":    nilIfBlank(nextTitle),
		"categorized_job_function": nilIfBlank(nextFunction),
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
	query := `SELECT id, external_company_id, name, slug, tagline, founded_year, home_page_url, linkedin_url, sponsors_h1b, employee_range FROM parsed_companies` + where + ` ORDER BY id DESC LIMIT ? OFFSET ?`
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
			sponsors                        sql.NullBool
			employeeRange                   sql.NullString
		)
		if err := rows.Scan(&id, &externalID, &name, &slug, &tagline, &foundedYear, &homePage, &linkedin, &sponsors, &employeeRange); err != nil {
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
		"sponsors_h1b":        jsonPatchBoolAsIntOrNull,
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
		isReady     int
		isSkippable int
		isParsed    int
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
		"is_ready":     isReady == 1,
		"is_skippable": isSkippable == 1,
		"is_parsed":    isParsed == 1,
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
		case "text", "datetime":
			return fmt.Sprintf("%v", value), nil
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
				if item {
					return 1, nil
				}
				return 0, nil
			case float64:
				if item != 0 {
					return 1, nil
				}
				return 0, nil
			case string:
				switch strings.ToLower(strings.TrimSpace(item)) {
				case "1", "true", "yes", "on":
					return 1, nil
				case "0", "false", "no", "off":
					return 0, nil
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
	return jsonPatchString(raw)
}

func jsonPatchBoolAsInt(raw *json.RawMessage) (any, bool) {
	var value bool
	if err := json.Unmarshal(*raw, &value); err != nil {
		return nil, false
	}
	if value {
		return 1, true
	}
	return 0, true
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

func jsonPatchBoolAsIntOrNull(raw *json.RawMessage) (any, bool) {
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "null" {
		return nil, true
	}
	return jsonPatchBoolAsInt(raw)
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
