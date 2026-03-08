package jobactions

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/database"
	gensqlc "goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
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

func NewHandler(db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{db: db, auth: authHandler, q: gensqlc.New(db.PGX)}
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
			JobID:     row.ParsedJobID,
			IsApplied: row.IsApplied == 1,
			IsSaved:   row.IsSaved == 1,
			IsHidden:  row.IsHidden == 1,
			UpdatedAt: row.UpdatedAt,
		})
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
		now := time.Now().UTC().Format(time.RFC3339Nano)
		err = qtx.InsertUserJobActionDefaults(c.Request.Context(), gensqlc.InsertUserJobActionDefaultsParams{
			UserID:      user.ID,
			ParsedJobID: jobID,
			UpdatedAt:   now,
			CreatedAt:   now,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update job action"})
			return
		}
		current = actionItem{JobID: jobID, UpdatedAt: now}
	} else {
		current.JobID = row.ParsedJobID
		current.IsApplied = row.IsApplied == 1
		current.IsSaved = row.IsSaved == 1
		current.IsHidden = row.IsHidden == 1
		current.UpdatedAt = row.UpdatedAt
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
	err = qtx.UpdateUserJobActionByUserAndJob(c.Request.Context(), gensqlc.UpdateUserJobActionByUserAndJobParams{
		IsApplied:   int32(boolToInt(current.IsApplied)),
		IsSaved:     int32(boolToInt(current.IsSaved)),
		IsHidden:    int32(boolToInt(current.IsHidden)),
		UpdatedAt:   current.UpdatedAt,
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var clearedCount int64
	switch action {
	case "applied":
		clearedCount, err = h.q.ClearAppliedJobActionsByUser(c.Request.Context(), gensqlc.ClearAppliedJobActionsByUserParams{
			UpdatedAt: now,
			UserID:    user.ID,
		})
	case "saved":
		clearedCount, err = h.q.ClearSavedJobActionsByUser(c.Request.Context(), gensqlc.ClearSavedJobActionsByUserParams{
			UpdatedAt: now,
			UserID:    user.ID,
		})
	default:
		clearedCount, err = h.q.ClearHiddenJobActionsByUser(c.Request.Context(), gensqlc.ClearHiddenJobActionsByUserParams{
			UpdatedAt: now,
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

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
