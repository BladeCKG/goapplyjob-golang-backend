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
			JobID:     int64(row.ParsedJobID),
			IsApplied: row.IsApplied,
			IsSaved:   row.IsSaved,
			IsHidden:  row.IsHidden,
			UpdatedAt: row.UpdatedAt.Time.UTC().Format(time.RFC3339Nano),
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
