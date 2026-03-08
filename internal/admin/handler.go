package admin

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

	rows, err := h.db.SQL.QueryContext(c.Request.Context(),
		`SELECT id, email, created_at
		 FROM auth_users
		 ORDER BY created_at DESC, id DESC
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
	c.JSON(http.StatusOK, gin.H{"items": items})
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
	if len(filters) > 0 {
		query += " WHERE " + strings.Join(filters, " AND ")
	}
	query += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, args...)
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
	c.JSON(http.StatusOK, gin.H{"items": items})
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
	onlyNotReady := queryBoolDefault(c, "only_not_ready", true)

	filters := []string{}
	args := []any{}
	if source != "" {
		filters = append(filters, "source = ?")
		args = append(args, source)
	}
	if onlyNotReady {
		filters = append(filters, "is_ready = 0")
	}
	query := `SELECT id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json
		FROM raw_us_jobs`
	if len(filters) > 0 {
		query += " WHERE " + strings.Join(filters, " AND ")
	}
	query += " ORDER BY post_date DESC, id DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, args...)
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
	c.JSON(http.StatusOK, gin.H{"items": items})
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
	args := []any{}
	if source != "" {
		query += " WHERE source = ?"
		args = append(args, source)
	}
	query += " ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := h.db.SQL.QueryContext(c.Request.Context(), query, args...)
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
	c.JSON(http.StatusOK, gin.H{"items": items})
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
