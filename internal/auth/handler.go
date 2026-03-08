package auth

import (
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

	"github.com/gin-gonic/gin"
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
	httpClient *http.Client
}

type User struct {
	ID    int64
	Email string
}

func NewHandler(cfg config.Config, db *database.DB) *Handler {
	return &Handler{
		cfg: cfg,
		db:  db,
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
	router.POST("/auth/password/change", h.passwordChange)
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
	if !h.cfg.AuthEnableCodeLogin {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Email verification login is temporarily disabled"})
		return
	}
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
	magicToken := randomToken()
	now := utcNow()
	expiresAt := now.Add(time.Duration(max(h.cfg.AuthCodeTTLMinutes, 1)) * time.Minute)
	tx, err := h.db.SQL.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE auth_verification_codes SET consumed_at = ? WHERE user_id = ? AND consumed_at IS NULL`, now.Format(time.RFC3339Nano), userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	if _, err := tx.ExecContext(c.Request.Context(), `INSERT INTO auth_verification_codes (user_id, code_hash, expires_at, created_at) VALUES (?, ?, ?, ?)`, userID, hashText(code), expiresAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	if _, err := tx.ExecContext(c.Request.Context(), `INSERT INTO auth_verification_codes (user_id, code_hash, expires_at, created_at) VALUES (?, ?, ?, ?)`, userID, hashText(magicToken), expiresAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store login code"})
		return
	}
	if err := tx.Commit(); err != nil {
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
	if err := tx.QueryRowContext(c.Request.Context(), `SELECT id FROM auth_verification_codes WHERE user_id = ? AND code_hash = ? AND consumed_at IS NULL AND expires_at > ? ORDER BY created_at DESC LIMIT 1`, userID, hashText(code), utcNow().Format(time.RFC3339Nano)).Scan(&codeID); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or code"})
		return
	}

	now := utcNow()
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE auth_verification_codes SET consumed_at = ? WHERE id = ?`, now.Format(time.RFC3339Nano), codeID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	if err := tx.Commit(); err != nil {
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

	tx, err := h.db.SQL.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	defer tx.Rollback()

	var (
		codeID int64
		userID int64
	)
	if err := tx.QueryRowContext(
		c.Request.Context(),
		`SELECT avc.id, u.id
		 FROM auth_verification_codes avc
		 JOIN auth_users u ON u.id = avc.user_id
		 WHERE avc.code_hash = ? AND avc.consumed_at IS NULL AND avc.expires_at > ?
		 ORDER BY avc.created_at DESC
		 LIMIT 1`,
		hashText(token),
		utcNow().Format(time.RFC3339Nano),
	).Scan(&codeID, &userID); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid or expired sign-in link"})
		return
	}

	now := utcNow()
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE auth_verification_codes SET consumed_at = ? WHERE id = ?`, now.Format(time.RFC3339Nano), codeID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create session"})
		return
	}
	if err := tx.Commit(); err != nil {
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
	var payload struct {
		Email    string `json:"email"`
		Password string `json:"password"`
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

	userID, err := h.getOrCreateUser(c, email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create password account"})
		return
	}
	var existing int
	err = h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT COUNT(1) FROM auth_password_credentials WHERE user_id = ?`, userID).Scan(&existing)
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
	now := utcNow().Format(time.RFC3339Nano)
	if _, err := h.db.SQL.ExecContext(c.Request.Context(), `INSERT INTO auth_password_credentials (user_id, password_salt, password_hash, created_at) VALUES (?, ?, ?, ?)`, userID, hex.EncodeToString(salt), passwordHash(password, salt), now); err != nil {
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
	var payload struct {
		Email    string `json:"email"`
		Password string `json:"password"`
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

	var userID int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM auth_users WHERE email = ? LIMIT 1`, email).Scan(&userID); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or password"})
		return
	}

	var saltHex, storedHash string
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT password_salt, password_hash FROM auth_password_credentials WHERE user_id = ? LIMIT 1`, userID).Scan(&saltHex, &storedHash); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid email or password"})
		return
	}
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
	c.JSON(http.StatusOK, gin.H{"id": user.ID, "email": user.Email})
}
func (h *Handler) passwordChange(c *gin.Context) {
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
	var saltHex, storedHash string
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT password_salt, password_hash FROM auth_password_credentials WHERE user_id = ? LIMIT 1`, user.ID).Scan(&saltHex, &storedHash); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Password account is not configured"})
		return
	}
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
	_, err = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE auth_password_credentials SET password_salt = ?, password_hash = ? WHERE user_id = ?`, hex.EncodeToString(newSalt), passwordHash(newPassword, newSalt), user.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to update password"})
		return
	}
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
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `INSERT INTO auth_users (email, created_at) VALUES (?, ?) RETURNING id`, email, now).Scan(&userID); err != nil {
		return 0, err
	}
	return userID, nil
}

func (h *Handler) createSessionForUser(c *gin.Context, userID int64) (string, error) {
	now := utcNow()
	expiresAt := now.Add(time.Duration(max(h.cfg.AuthSessionTTLMin, 1)) * time.Minute)
	sessionToken := randomToken()
	_, err := h.db.SQL.ExecContext(c.Request.Context(), `INSERT INTO auth_sessions (user_id, session_token_hash, expires_at, created_at) VALUES (?, ?, ?, ?)`, userID, hashText(sessionToken), expiresAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return "", err
	}
	return sessionToken, nil
}

func (h *Handler) ensureDefaultFreeSubscription(c *gin.Context, userID int64) error {
	now := utcNow()
	_, err := h.db.SQL.ExecContext(
		c.Request.Context(),
		`INSERT INTO pricing_plans (code, name, billing_cycle, duration_days, price_usd, is_active, created_at)
		 VALUES (?, ?, ?, ?, ?, 1, ?)
		 ON CONFLICT(code) DO UPDATE SET
		   name = excluded.name,
		   billing_cycle = excluded.billing_cycle,
		   duration_days = excluded.duration_days,
		   price_usd = excluded.price_usd,
		   is_active = 1`,
		freePlanCode,
		"Free",
		"free",
		max(h.cfg.FreePlanDurationDays, 1),
		0,
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return err
	}

	var subscriptionCount int
	if err := h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT COUNT(1)
		 FROM user_subscriptions
		 WHERE user_id = ?`,
		userID,
	).Scan(&subscriptionCount); err != nil {
		return err
	}
	if subscriptionCount > 0 {
		return nil
	}

	var planID int64
	if err := h.db.SQL.QueryRowContext(c.Request.Context(), `SELECT id FROM pricing_plans WHERE code = ? AND is_active = 1 LIMIT 1`, freePlanCode).Scan(&planID); err != nil {
		return err
	}
	endsAt := now.Add(time.Duration(max(h.cfg.FreePlanDurationDays, 1)) * 24 * time.Hour)
	_, err = h.db.SQL.ExecContext(
		c.Request.Context(),
		`INSERT INTO user_subscriptions (user_id, pricing_plan_id, starts_at, ends_at, is_active, created_at) VALUES (?, ?, ?, ?, 1, ?)`,
		userID,
		planID,
		now.Format(time.RFC3339Nano),
		endsAt.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	)
	return err
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
