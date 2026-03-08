package jobactions

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/database"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	db   *database.DB
	auth *auth.Handler
}

type actionItem struct {
	JobID     int64  `json:"job_id"`
	IsApplied bool   `json:"is_applied"`
	IsSaved   bool   `json:"is_saved"`
	IsHidden  bool   `json:"is_hidden"`
	UpdatedAt string `json:"updated_at"`
}

func NewHandler(db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{db: db, auth: authHandler}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/job-actions", h.getJobActions)
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

	placeholders := make([]string, 0, len(jobIDs))
	args := make([]any, 0, len(jobIDs)+1)
	args = append(args, user.ID)
	for _, jobID := range jobIDs {
		placeholders = append(placeholders, "?")
		args = append(args, jobID)
	}
	rows, err := h.db.SQL.QueryContext(c.Request.Context(),
		`SELECT parsed_job_id, is_applied, is_saved, is_hidden, updated_at
		 FROM user_job_actions
		 WHERE user_id = ? AND parsed_job_id IN (`+strings.Join(placeholders, ",")+`)`, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load job actions"})
		return
	}
	defer rows.Close()

	items := []actionItem{}
	for rows.Next() {
		var item actionItem
		var isApplied, isSaved, isHidden int
		if err := rows.Scan(&item.JobID, &isApplied, &isSaved, &isHidden, &item.UpdatedAt); err == nil {
			item.IsApplied = isApplied == 1
			item.IsSaved = isSaved == 1
			item.IsHidden = isHidden == 1
			items = append(items, item)
		}
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *Handler) updateJobAction(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	jobID, err := strconv.ParseInt(strings.TrimSpace(c.Param("jobID")), 10, 64)
	if err != nil || jobID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid job id"})
		return
	}

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
	var exists int
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(1) FROM parsed_jobs WHERE id = ?`, jobID).Scan(&exists); err != nil || exists == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Job not found"})
		return
	}

	tx, err := h.db.SQL.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
		return
	}
	defer tx.Rollback()

	var current actionItem
	var isApplied, isSaved, isHidden int
	rowErr := tx.QueryRowContext(c.Request.Context(), `SELECT parsed_job_id, is_applied, is_saved, is_hidden, updated_at FROM user_job_actions WHERE user_id = ? AND parsed_job_id = ? LIMIT 1`, user.ID, jobID).Scan(&current.JobID, &isApplied, &isSaved, &isHidden, &current.UpdatedAt)
	if rowErr != nil && rowErr != sql.ErrNoRows {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
		return
	}
	if rowErr == sql.ErrNoRows {
		now := time.Now().UTC().Format(time.RFC3339Nano)
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO user_job_actions (user_id, parsed_job_id, is_applied, is_saved, is_hidden, updated_at, created_at) VALUES (?, ?, 0, 0, 0, ?, ?)`,
			user.ID, jobID, now, now)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
			return
		}
		current = actionItem{JobID: jobID, UpdatedAt: now}
	} else {
		current.IsApplied = isApplied == 1
		current.IsSaved = isSaved == 1
		current.IsHidden = isHidden == 1
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
	current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	_, err = tx.ExecContext(c.Request.Context(),
		`UPDATE user_job_actions SET is_applied = ?, is_saved = ?, is_hidden = ?, updated_at = ? WHERE user_id = ? AND parsed_job_id = ?`,
		boolToInt(current.IsApplied), boolToInt(current.IsSaved), boolToInt(current.IsHidden), current.UpdatedAt, user.ID, jobID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
		return
	}
	if err := tx.Commit(); err != nil {
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
	var appliedCount, savedCount, hiddenCount int
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT
		COALESCE(SUM(is_applied), 0),
		COALESCE(SUM(is_saved), 0),
		COALESCE(SUM(is_hidden), 0)
		FROM user_job_actions
		WHERE user_id = ?`, user.ID).Scan(&appliedCount, &savedCount, &hiddenCount); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load job action summary"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"applied_count": appliedCount,
		"saved_count":   savedCount,
		"hidden_count":  hiddenCount,
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var result sql.Result
	switch action {
	case "applied":
		result, err = h.db.SQL.ExecContext(c.Request.Context(),
			`UPDATE user_job_actions
			 SET is_applied = 0, updated_at = ?
			 WHERE user_id = ? AND is_applied = 1`,
			now, user.ID,
		)
	case "saved":
		result, err = h.db.SQL.ExecContext(c.Request.Context(),
			`UPDATE user_job_actions
			 SET is_saved = 0, updated_at = ?
			 WHERE user_id = ? AND is_saved = 1`,
			now, user.ID,
		)
	default:
		result, err = h.db.SQL.ExecContext(c.Request.Context(),
			`UPDATE user_job_actions
			 SET is_hidden = 0, updated_at = ?
			 WHERE user_id = ? AND is_hidden = 1`,
			now, user.ID,
		)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to clear job actions"})
		return
	}
	clearedCount, _ := result.RowsAffected()
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

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
