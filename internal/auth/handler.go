package auth

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	cfg config.Config
	db  *database.DB
}

type User struct {
	ID    int64
	Email string
}

func NewHandler(cfg config.Config, db *database.DB) *Handler {
	return &Handler{cfg: cfg, db: db}
}

func (h *Handler) Register(router gin.IRouter) {
	router.POST("/auth/login/request-code", h.requestCode)
	router.POST("/auth/login/verify-code", h.verifyCode)
	router.POST("/auth/logout", h.logout)
}

func (h *Handler) CurrentUser(c *gin.Context) (*User, error) {
	token, err := c.Cookie(h.cfg.AuthCookieName)
	if err != nil || token == "" {
		return nil, errors.New("missing auth cookie")
	}

	row := h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT u.id, u.email
		FROM auth_sessions s
		JOIN auth_users u ON u.id = s.user_id
		WHERE s.session_token_hash = ? AND s.expires_at > ?
		LIMIT 1`,
		hashText(token),
		utcNow().Format(time.RFC3339Nano),
	)
	var user User
	if err := row.Scan(&user.ID, &user.Email); err != nil {
		return nil, err
	}
	return &user, nil
}

func (h *Handler) OptionalCurrentUser(c *gin.Context) *User {
	user, err := h.CurrentUser(c)
	if err != nil {
		return nil
	}
	return user
}

func (h *Handler) requestCode(c *gin.Context) {
	var payload struct {
		Email string `json:"email"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	email, err := normalizeEmail(payload.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid email"})
		return
	}

	userID, err := h.getOrCreateUser(c, email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}

	code := generateCode()
	now := utcNow()
	expiresAt := now.Add(time.Duration(max(h.cfg.AuthCodeTTLMinutes, 1)) * time.Minute)
	tx, err := h.db.SQL.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(
		c.Request.Context(),
		`UPDATE auth_verification_codes SET consumed_at = ? WHERE user_id = ? AND consumed_at IS NULL`,
		now.Format(time.RFC3339Nano),
		userID,
	); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	if _, err := tx.ExecContext(
		c.Request.Context(),
		`INSERT INTO auth_verification_codes (user_id, code_hash, expires_at, created_at) VALUES (?, ?, ?, ?)`,
		userID,
		hashText(code),
		expiresAt.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}

	response := gin.H{"ok": true}
	if h.cfg.AuthDebugReturnCode {
		response["debug_code"] = code
	}
	c.JSON(http.StatusOK, response)
}

func (h *Handler) verifyCode(c *gin.Context) {
	var payload struct {
		Email string `json:"email"`
		Code  string `json:"code"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}

	email, err := normalizeEmail(payload.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid email"})
		return
	}
	code := strings.TrimSpace(payload.Code)
	if code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Code is required"})
		return
	}

	tx, err := h.db.SQL.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	defer tx.Rollback()

	var userID int64
	if err := tx.QueryRowContext(c.Request.Context(), `SELECT id FROM auth_users WHERE email = ? LIMIT 1`, email).Scan(&userID); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or code"})
		return
	}

	var codeID int64
	if err := tx.QueryRowContext(
		c.Request.Context(),
		`SELECT id
		FROM auth_verification_codes
		WHERE user_id = ? AND code_hash = ? AND consumed_at IS NULL AND expires_at > ?
		ORDER BY created_at DESC
		LIMIT 1`,
		userID,
		hashText(code),
		utcNow().Format(time.RFC3339Nano),
	).Scan(&codeID); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or code"})
		return
	}

	now := utcNow()
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE auth_verification_codes SET consumed_at = ? WHERE id = ?`, now.Format(time.RFC3339Nano), codeID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}

	sessionToken := randomToken()
	expiresAt := now.Add(time.Duration(max(h.cfg.AuthSessionTTLMin, 1)) * time.Minute)
	if _, err := tx.ExecContext(
		c.Request.Context(),
		`INSERT INTO auth_sessions (user_id, session_token_hash, expires_at, created_at) VALUES (?, ?, ?, ?)`,
		userID,
		hashText(sessionToken),
		expiresAt.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}

	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}

	c.SetSameSite(parseSameSite(h.cfg.AuthCookieSameSite))
	c.SetCookie(
		h.cfg.AuthCookieName,
		sessionToken,
		max(h.cfg.AuthSessionTTLMin, 1)*60,
		"/",
		h.cfg.AuthCookieDomain,
		h.cfg.AuthCookieSecure,
		true,
	)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) logout(c *gin.Context) {
	if token, err := c.Cookie(h.cfg.AuthCookieName); err == nil && token != "" {
		_, _ = h.db.SQL.ExecContext(c.Request.Context(), `DELETE FROM auth_sessions WHERE session_token_hash = ?`, hashText(token))
	}
	c.SetSameSite(parseSameSite(h.cfg.AuthCookieSameSite))
	c.SetCookie(h.cfg.AuthCookieName, "", -1, "/", h.cfg.AuthCookieDomain, h.cfg.AuthCookieSecure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) getOrCreateUser(c *gin.Context, email string) (int64, error) {
	var userID int64
	err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM auth_users WHERE email = ? LIMIT 1`, email).Scan(&userID)
	if err == nil {
		return userID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	now := utcNow().Format(time.RFC3339Nano)
	result, err := h.db.SQL.ExecContext(
		c.Request.Context(),
		`INSERT INTO auth_users (email, created_at) VALUES (?, ?)`,
		email,
		now,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func normalizeEmail(email string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(email))
	if _, err := mail.ParseAddress(value); err != nil {
		return "", err
	}
	return value, nil
}

func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func parseSameSite(value string) http.SameSite {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "strict":
		return http.SameSiteStrictMode
	case "none":
		return http.SameSiteNoneMode
	default:
		return http.SameSiteLaxMode
	}
}

func utcNow() time.Time {
	return time.Now().UTC()
}

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func generateCode() string {
	return fmt.Sprintf("%06d", time.Now().UnixNano()%1000000)
}
