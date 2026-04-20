package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"net/url"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	gensqlc "goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/crypto/pbkdf2"
)

const (
	passwordHashIterations = 120000
	passwordSaltBytes      = 16
	passwordMinLength      = 8
	freePlanCode           = "free"
)

type Handler struct {
	cfg        config.Config
	db         *database.DB
	queries    *gensqlc.Queries
	httpClient *http.Client
}

type User struct {
	ID    int32
	Email string
}

func NewHandler(cfg config.Config, db *database.DB) *Handler {
	return &Handler{
		cfg:     cfg,
		db:      db,
		queries: gensqlc.New(db.PGX),
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (h *Handler) Register(router gin.IRouter) {
	router.POST("/auth/login/request-code", h.requestCode)
	router.POST("/auth/login/verify-code", h.verifyCode)
	router.POST("/auth/login/verify-link", h.verifyLink)
	router.POST("/auth/oauth/supabase/google", h.supabaseGoogleLogin)
	router.POST("/auth/password/signup", h.passwordSignup)
	router.POST("/auth/password/login", h.passwordLogin)
	router.GET("/auth/me", h.me)
	router.PATCH("/auth/me", h.updateMe)
	router.POST("/auth/password/change", h.passwordChange)
	router.POST("/auth/logout", h.logout)
}

func (h *Handler) CurrentUser(c *gin.Context) (*User, error) {
	token, err := c.Cookie(h.cfg.AuthCookieName)
	if err != nil || token == "" {
		return nil, errors.New("missing auth cookie")
	}

	row, err := h.queries.GetCurrentUserBySession(c.Request.Context(), gensqlc.GetCurrentUserBySessionParams{
		SessionTokenHash: hashText(token),
		ExpiresAt:        pgTimestamptz(utcNow()),
	})
	if err != nil {
		return nil, err
	}
	h.touchLastSeen(c.Request.Context(), row.ID)
	return &User{ID: row.ID, Email: row.Email}, nil
}

func (h *Handler) OptionalCurrentUser(c *gin.Context) *User {
	user, err := h.CurrentUser(c)
	if err != nil {
		return nil
	}
	return user
}

func (h *Handler) requestCode(c *gin.Context) {
	if !h.cfg.AuthEnableCodeLogin {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Email verification login is temporarily disabled"})
		return
	}
	var payload struct {
		Email          string `json:"email"`
		TurnstileToken string `json:"turnstile_token"`
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
	if err := h.verifyTurnstileToken(c, payload.TurnstileToken, "login"); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
		return
	}

	userID, err := h.getOrCreateUser(c, email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}

	code := generateCode()
	magicToken := randomToken()
	now := utcNow()
	expiresAt := now.Add(time.Duration(max(h.cfg.AuthCodeTTLMinutes, 1)) * time.Minute)
	tx, err := h.db.PGX.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	qtx := h.queries.WithTx(tx)

	if err := qtx.ConsumeActiveVerificationCodesByUser(c.Request.Context(), gensqlc.ConsumeActiveVerificationCodesByUserParams{
		ConsumedAt: pgTimestamptz(now),
		UserID:     userID,
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	if err := qtx.InsertVerificationCode(c.Request.Context(), gensqlc.InsertVerificationCodeParams{
		UserID:    userID,
		CodeHash:  hashText(code),
		ExpiresAt: pgTimestamptz(expiresAt),
		CreatedAt: pgTimestamptz(now),
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	if err := qtx.InsertVerificationCode(c.Request.Context(), gensqlc.InsertVerificationCodeParams{
		UserID:    userID,
		CodeHash:  hashText(magicToken),
		ExpiresAt: pgTimestamptz(expiresAt),
		CreatedAt: pgTimestamptz(now),
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}

	magicLink := buildMagicLoginURL(h.cfg, magicToken)
	if err := sendVerificationEmail(h.cfg, email, code, max(h.cfg.AuthCodeTTLMinutes, 1), magicLink); err != nil {
		logVerificationEmailFailure(email, err)
		if !h.cfg.AuthDebugReturnCode {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to send code"})
			return
		}
	}

	response := gin.H{"ok": true}
	if h.cfg.AuthDebugReturnCode {
		response["debug_code"] = code
		response["debug_link"] = magicLink
	}
	c.JSON(http.StatusOK, response)
}

func (h *Handler) verifyCode(c *gin.Context) {
	if !h.cfg.AuthEnableCodeLogin {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Email verification login is temporarily disabled"})
		return
	}
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

	tx, err := h.db.PGX.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	qtx := h.queries.WithTx(tx)

	userRow, err := qtx.GetAuthUserByEmail(c.Request.Context(), email)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or code"})
		return
	}
	userID := userRow.ID

	codeID, err := qtx.GetVerificationCodeIDByUser(c.Request.Context(), gensqlc.GetVerificationCodeIDByUserParams{
		UserID:    userID,
		CodeHash:  hashText(code),
		ExpiresAt: pgTimestamptz(utcNow()),
	})
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or code"})
		return
	}

	now := utcNow()
	if err := qtx.MarkVerificationCodeConsumed(c.Request.Context(), gensqlc.MarkVerificationCodeConsumedParams{
		ConsumedAt: pgTimestamptz(now),
		ID:         codeID,
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}

	if err := h.ensureDefaultFreeSubscription(c, userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	sessionToken, err := h.createSessionForUser(c, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	h.setAuthCookie(c, sessionToken)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) verifyLink(c *gin.Context) {
	if !h.cfg.AuthEnableCodeLogin {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Email verification login is temporarily disabled"})
		return
	}
	var payload struct {
		Token string `json:"token"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	token := strings.TrimSpace(payload.Token)
	if token == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Token is required"})
		return
	}

	tx, err := h.db.PGX.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	qtx := h.queries.WithTx(tx)

	codeRow, err := qtx.GetMagicLinkVerificationCode(c.Request.Context(), gensqlc.GetMagicLinkVerificationCodeParams{
		CodeHash:  hashText(token),
		ExpiresAt: pgTimestamptz(utcNow()),
	})
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid or expired sign-in link"})
		return
	}
	codeID := codeRow.ID
	userID := codeRow.UserID

	now := utcNow()
	if err := qtx.MarkVerificationCodeConsumed(c.Request.Context(), gensqlc.MarkVerificationCodeConsumedParams{
		ConsumedAt: pgTimestamptz(now),
		ID:         codeID,
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}

	if err := h.ensureDefaultFreeSubscription(c, userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	sessionToken, err := h.createSessionForUser(c, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	h.setAuthCookie(c, sessionToken)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) passwordSignup(c *gin.Context) {
	c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Password authentication is disabled"})
	return

	var payload struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		TurnstileToken string `json:"turnstile_token"`
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
	password, err := validatePassword(payload.Password)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	if err := h.verifyTurnstileToken(c, payload.TurnstileToken, "login"); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
		return
	}

	userID, err := h.getOrCreateUser(c, email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create password account"})
		return
	}
	existing, err := h.queries.CountPasswordCredentialsByUser(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create password account"})
		return
	}
	if existing > 0 {
		c.JSON(http.StatusConflict, gin.H{"detail": "Password account already exists"})
		return
	}

	salt := make([]byte, passwordSaltBytes)
	_, _ = rand.Read(salt)
	now := utcNow()
	if err := h.queries.InsertPasswordCredential(c.Request.Context(), gensqlc.InsertPasswordCredentialParams{
		UserID:       userID,
		PasswordSalt: hex.EncodeToString(salt),
		PasswordHash: passwordHash(password, salt),
		CreatedAt:    pgTimestamptz(now),
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create password account"})
		return
	}

	if err := h.ensureDefaultFreeSubscription(c, userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	sessionToken, err := h.createSessionForUser(c, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	h.setAuthCookie(c, sessionToken)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) supabaseGoogleLogin(c *gin.Context) {
	if !h.cfg.AuthEnableGoogleLogin {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Google login is temporarily disabled"})
		return
	}
	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	email, err := h.fetchSupabaseUserEmail(payload.AccessToken)
	if err != nil {
		status := http.StatusUnauthorized
		switch {
		case errors.Is(err, errMissingAccessToken):
			status = http.StatusBadRequest
		case errors.Is(err, errSupabaseNotConfigured):
			status = http.StatusInternalServerError
		case errors.Is(err, errSupabaseUnavailable):
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"detail": err.Error()})
		return
	}

	userID, err := h.getOrCreateUser(c, email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	if err := h.ensureDefaultFreeSubscription(c, userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	sessionToken, err := h.createSessionForUser(c, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	h.setAuthCookie(c, sessionToken)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) passwordLogin(c *gin.Context) {
	c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Password authentication is disabled"})
	return

	var payload struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		TurnstileToken string `json:"turnstile_token"`
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
	password, err := validatePassword(payload.Password)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	if err := h.verifyTurnstileToken(c, payload.TurnstileToken, "login"); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
		return
	}

	userRow, err := h.queries.GetAuthUserByEmail(c.Request.Context(), email)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or password"})
		return
	}
	userID := userRow.ID

	cred, err := h.queries.GetPasswordCredentialByUser(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or password"})
		return
	}
	saltHex, storedHash := cred.PasswordSalt, cred.PasswordHash
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or password"})
		return
	}
	actualHash := passwordHash(password, salt)
	if !hmac.Equal([]byte(actualHash), []byte(storedHash)) {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or password"})
		return
	}

	sessionToken, err := h.createSessionForUser(c, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	h.setAuthCookie(c, sessionToken)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) me(c *gin.Context) {
	user, err := h.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	lastSeen, lastJobFilters := h.getUserMeta(c.Request.Context(), user.ID)
	c.JSON(http.StatusOK, gin.H{
		"id":                    user.ID,
		"email":                 user.Email,
		"last_seen_at":          lastSeen,
		"last_job_filters_json": lastJobFilters,
	})
}

func (h *Handler) updateMe(c *gin.Context) {
	user, err := h.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	var payload struct {
		LastJobFiltersJSON *json.RawMessage `json:"last_job_filters_json"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}

	var stored any = nil
	if payload.LastJobFiltersJSON != nil {
		raw := strings.TrimSpace(string(*payload.LastJobFiltersJSON))
		if raw != "" && raw != "null" {
			var parsed any
			if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid last_job_filters_json"})
				return
			}
			stored = raw
		}
	}

	if _, err := h.db.SQL.ExecContext(
		c.Request.Context(),
		`UPDATE auth_users SET last_job_filters_json = ? WHERE id = ?`,
		stored, user.ID,
	); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update user profile"})
		return
	}

	lastSeen, lastJobFilters := h.getUserMeta(c.Request.Context(), user.ID)
	c.JSON(http.StatusOK, gin.H{
		"id":                    user.ID,
		"email":                 user.Email,
		"last_seen_at":          lastSeen,
		"last_job_filters_json": lastJobFilters,
	})
}
func (h *Handler) passwordChange(c *gin.Context) {
	c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Password authentication is disabled"})
	return

	user, err := h.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	var payload struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	currentPassword, err := validatePassword(payload.CurrentPassword)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	newPassword, err := validatePassword(payload.NewPassword)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	cred, err := h.queries.GetPasswordCredentialByUser(c.Request.Context(), user.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Password account is not configured"})
		return
	}
	saltHex, storedHash := cred.PasswordSalt, cred.PasswordHash
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid current password"})
		return
	}
	if !hmac.Equal([]byte(passwordHash(currentPassword, salt)), []byte(storedHash)) {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid current password"})
		return
	}
	newSalt := make([]byte, passwordSaltBytes)
	_, _ = rand.Read(newSalt)
	if err := h.queries.UpdatePasswordCredentialByUser(c.Request.Context(), gensqlc.UpdatePasswordCredentialByUserParams{
		PasswordSalt: hex.EncodeToString(newSalt),
		PasswordHash: passwordHash(newPassword, newSalt),
		UserID:       user.ID,
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update password"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
func (h *Handler) logout(c *gin.Context) {
	if token, err := c.Cookie(h.cfg.AuthCookieName); err == nil && token != "" {
		_ = h.queries.DeleteAuthSessionByTokenHash(c.Request.Context(), hashText(token))
	}
	c.SetSameSite(parseSameSite(h.cfg.AuthCookieSameSite))
	c.SetCookie(h.cfg.AuthCookieName, "", -1, "/", h.cfg.AuthCookieDomain, h.cfg.AuthCookieSecure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) getOrCreateUser(c *gin.Context, email string) (int32, error) {
	row, err := h.queries.GetAuthUserByEmail(c.Request.Context(), email)
	if err == nil {
		return row.ID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	now := utcNow()
	userID, err := h.queries.CreateAuthUser(c.Request.Context(), gensqlc.CreateAuthUserParams{
		Email:     email,
		CreatedAt: pgTimestamptz(now),
	})
	if err != nil {
		return 0, err
	}
	return userID, nil
}

func (h *Handler) createSessionForUser(c *gin.Context, userID int32) (string, error) {
	now := utcNow()
	expiresAt := now.Add(time.Duration(max(h.cfg.AuthSessionTTLMin, 1)) * time.Minute)
	sessionToken := randomToken()
	if err := h.queries.InsertAuthSession(c.Request.Context(), gensqlc.InsertAuthSessionParams{
		UserID:           userID,
		SessionTokenHash: hashText(sessionToken),
		ExpiresAt:        pgTimestamptz(expiresAt),
		CreatedAt:        pgTimestamptz(now),
	}); err != nil {
		return "", err
	}
	h.touchLastSeen(c.Request.Context(), userID)
	return sessionToken, nil
}

func (h *Handler) touchLastSeen(ctx context.Context, userID int32) {
	if userID <= 0 {
		return
	}
	var lastSeen sql.NullTime
	if err := h.db.SQL.QueryRowContext(ctx, `SELECT last_seen_at FROM auth_users WHERE id = ?`, userID).Scan(&lastSeen); err != nil {
		return
	}
	now := utcNow()
	if lastSeen.Valid {
		lastSeenUTC := lastSeen.Time.UTC()
		if now.Sub(lastSeenUTC) < 5*time.Minute {
			return
		}
	}
	_, _ = h.db.SQL.ExecContext(ctx, `UPDATE auth_users SET last_seen_at = ? WHERE id = ?`, now, userID)
}

func (h *Handler) getUserMeta(ctx context.Context, userID int32) (any, any) {
	var lastSeen sql.NullTime
	var lastJobFilters sql.NullString
	if err := h.db.SQL.QueryRowContext(
		ctx,
		`SELECT last_seen_at, last_job_filters_json::text FROM auth_users WHERE id = ?`,
		userID,
	).Scan(&lastSeen, &lastJobFilters); err != nil {
		return nil, nil
	}

	var lastSeenValue any = nil
	if lastSeen.Valid {
		lastSeenValue = lastSeen.Time.UTC().Format(time.RFC3339Nano)
	}

	var lastJobFiltersValue any = nil
	if lastJobFilters.Valid && strings.TrimSpace(lastJobFilters.String) != "" {
		var parsed any
		if err := json.Unmarshal([]byte(lastJobFilters.String), &parsed); err == nil {
			lastJobFiltersValue = parsed
		} else {
			lastJobFiltersValue = lastJobFilters.String
		}
	}

	return lastSeenValue, lastJobFiltersValue
}

func (h *Handler) ensureDefaultFreeSubscription(c *gin.Context, userID int32) error {
	now := utcNow()
	if err := h.queries.UpsertPricingPlanByCode(c.Request.Context(), gensqlc.UpsertPricingPlanByCodeParams{
		Code:         freePlanCode,
		Name:         "Free",
		BillingCycle: "free",
		DurationDays: int32(max(h.cfg.FreePlanDurationDays, 1)),
		PriceUsd:     0,
		CreatedAt:    pgTimestamptz(now),
	}); err != nil {
		return err
	}

	subscriptionCount, err := h.queries.CountUserSubscriptions(c.Request.Context(), userID)
	if err != nil {
		return err
	}
	if subscriptionCount > 0 {
		return nil
	}

	planID, err := h.queries.GetActivePricingPlanIDByCode(c.Request.Context(), freePlanCode)
	if err != nil {
		return err
	}
	endsAt := now.Add(time.Duration(max(h.cfg.FreePlanDurationDays, 1)) * 24 * time.Hour)
	return h.queries.InsertUserSubscription(c.Request.Context(), gensqlc.InsertUserSubscriptionParams{
		UserID:        userID,
		PricingPlanID: planID,
		StartsAt:      pgTimestamptz(now),
		EndsAt:        pgTimestamptz(endsAt),
		CreatedAt:     pgTimestamptz(now),
	})
}

func (h *Handler) setAuthCookie(c *gin.Context, sessionToken string) {
	c.SetSameSite(parseSameSite(h.cfg.AuthCookieSameSite))
	c.SetCookie(h.cfg.AuthCookieName, sessionToken, max(h.cfg.AuthSessionTTLMin, 1)*60, "/", h.cfg.AuthCookieDomain, h.cfg.AuthCookieSecure, true)
}

func normalizeEmail(email string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(email))
	if _, err := mail.ParseAddress(value); err != nil {
		return "", err
	}
	return value, nil
}

func validatePassword(password string) (string, error) {
	value := strings.TrimSpace(password)
	if len(value) < passwordMinLength {
		return "", fmt.Errorf("Password must be at least %d characters", passwordMinLength)
	}
	return value, nil
}

func passwordHash(password string, salt []byte) string {
	digest := pbkdf2.Key([]byte(password), salt, passwordHashIterations, 32, sha256.New)
	return hex.EncodeToString(digest)
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

func pgTimestamptz(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: true}
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

func randomToken() string {
	buf := make([]byte, 36)
	_, _ = rand.Read(buf)
	return base64.RawURLEncoding.EncodeToString(buf)
}

var (
	errMissingAccessToken    = errors.New("Access token is required")
	errSupabaseNotConfigured = errors.New("Supabase OAuth is not configured")
	errSupabaseUnavailable   = errors.New("Could not reach Supabase auth")
	errInvalidSupabaseToken  = errors.New("Invalid Supabase access token")
	errSupabaseEmailMissing  = errors.New("Supabase user email is missing")
	errSupabaseWrongProvider = errors.New("Supabase provider is not Google")
)

func (h *Handler) fetchSupabaseUserEmail(accessToken string) (string, error) {
	token := strings.TrimSpace(accessToken)
	if token == "" {
		return "", errMissingAccessToken
	}
	baseURL := strings.TrimRight(strings.TrimSpace(h.cfg.SupabaseURL), "/")
	anonKey := strings.TrimSpace(h.cfg.SupabaseAnonKey)
	if baseURL == "" || anonKey == "" {
		return "", errSupabaseNotConfigured
	}

	endpoint, err := url.Parse(baseURL + "/auth/v1/user")
	if err != nil {
		return "", errSupabaseUnavailable
	}
	req, err := http.NewRequest(http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", errSupabaseUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("apikey", anonKey)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return "", errSupabaseUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", errInvalidSupabaseToken
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errSupabaseUnavailable
	}
	var payload struct {
		Email       string `json:"email"`
		AppMetadata struct {
			Provider string `json:"provider"`
		} `json:"app_metadata"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", errInvalidSupabaseToken
	}
	email, err := normalizeEmail(payload.Email)
	if err != nil {
		return "", errSupabaseEmailMissing
	}
	provider := strings.ToLower(strings.TrimSpace(payload.AppMetadata.Provider))
	if provider != "" && provider != "google" {
		return "", errSupabaseWrongProvider
	}
	return email, nil
}
