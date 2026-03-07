package pricing

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	cfg  config.Config
	db   *database.DB
	auth *auth.Handler
}

var planDefinitions = []struct {
	Code         string
	Name         string
	BillingCycle string
	DurationDays int
	PriceUSD     int
}{
	{"weekly", "Weekly", "weekly", 7, 3},
	{"monthly", "Monthly", "monthly", 30, 10},
	{"quarterly", "Quarterly", "quarterly", 90, 20},
	{"yearly", "Yearly", "yearly", 365, 60},
}

func NewHandler(cfg config.Config, db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{cfg: cfg, db: db, auth: authHandler}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/pricing/plans", h.listPlans)
	router.POST("/pricing/subscribe", h.subscribe)
	router.GET("/pricing/payments/:paymentID", h.paymentStatus)
	router.POST("/pricing/payments/:paymentID/confirm", h.confirmPayment)
	router.POST("/pricing/webhooks/stripe", h.stripeWebhook)
	router.POST("/pricing/webhooks/crypto", h.cryptoWebhook)
}

func (h *Handler) listPlans(c *gin.Context) {
	if err := h.ensurePlans(c); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load pricing plans"})
		return
	}
	rows, err := h.db.SQL.QueryContext(
		c.Request.Context(),
		`SELECT code, name, billing_cycle, duration_days, price_usd
		 FROM pricing_plans
		 WHERE is_active = 1
		 ORDER BY price_usd ASC`,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load pricing plans"})
		return
	}
	defer rows.Close()

	items := []gin.H{}
	for rows.Next() {
		var code, name, cycle string
		var duration, price int
		if err := rows.Scan(&code, &name, &cycle, &duration, &price); err == nil {
			items = append(items, gin.H{
				"code":          code,
				"name":          name,
				"billing_cycle": cycle,
				"duration_days": duration,
				"price_usd":     price,
			})
		}
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *Handler) subscribe(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}

	var payload struct {
		PlanCode      string `json:"plan_code"`
		Provider      string `json:"provider"`
		PaymentMethod string `json:"payment_method"`
		SuccessURL    string `json:"success_url"`
		CancelURL     string `json:"cancel_url"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	if payload.Provider == "" {
		payload.Provider = "stripe"
	}
	if payload.PaymentMethod == "" {
		payload.PaymentMethod = "card"
	}
	if err := validateProviderMethod(payload.Provider, payload.PaymentMethod); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	if err := h.ensurePlans(c); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load pricing plans"})
		return
	}

	var planID int64
	var planCode string
	var priceUSD int
	err = h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT id, code, price_usd
		 FROM pricing_plans
		 WHERE code = ? AND is_active = 1
		 LIMIT 1`,
		strings.ToLower(strings.TrimSpace(payload.PlanCode)),
	).Scan(&planID, &planCode, &priceUSD)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid pricing plan"})
		return
	}

	successURL := payload.SuccessURL
	if successURL == "" {
		successURL = h.cfg.PaymentSuccessURL
	}
	cancelURL := payload.CancelURL
	if cancelURL == "" {
		cancelURL = h.cfg.PaymentCancelURL
	}
	checkoutID, checkoutURL, providerPayload := createCheckoutPayload(payload.Provider, successURL, cancelURL)
	payloadBytes, _ := json.Marshal(providerPayload)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := h.db.SQL.ExecContext(
		c.Request.Context(),
		`INSERT INTO pricing_payments
		(user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, checkout_url, provider_payload, created_at)
		VALUES (?, ?, ?, ?, 'USD', ?, 'pending', ?, ?, ?, ?)`,
		user.ID,
		planID,
		payload.Provider,
		payload.PaymentMethod,
		priceUSD*100,
		checkoutID,
		checkoutURL,
		string(payloadBytes),
		now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
		return
	}
	paymentID, _ := result.LastInsertId()
	c.JSON(http.StatusOK, gin.H{
		"ok":             true,
		"payment_id":     paymentID,
		"plan_code":      planCode,
		"provider":       payload.Provider,
		"payment_method": payload.PaymentMethod,
		"payment_status": "pending",
		"checkout_url":   checkoutURL,
	})
}

func (h *Handler) paymentStatus(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	h.renderPaymentStatus(c, user.ID)
}

func (h *Handler) confirmPayment(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	if err := h.activateSubscription(c, c.Param("paymentID"), user.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Payment not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to confirm payment"})
		return
	}
	h.renderPaymentStatus(c, user.ID)
}

func (h *Handler) stripeWebhook(c *gin.Context) {
	var payload struct {
		Type string `json:"type"`
		Data struct {
			Object struct {
				Metadata map[string]any `json:"metadata"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := c.ShouldBindJSON(&payload); err == nil && payload.Data.Object.Metadata != nil {
		if raw, ok := payload.Data.Object.Metadata["payment_id"].(string); ok {
			switch payload.Type {
			case "checkout.session.completed":
				_ = h.activateSubscription(c, raw, 0)
			case "checkout.session.expired", "payment_intent.payment_failed":
				_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE pricing_payments SET status = 'failed' WHERE id = ?`, raw)
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) cryptoWebhook(c *gin.Context) {
	if h.cfg.CryptoWebhookToken != "" && c.GetHeader("X-Webhook-Token") != h.cfg.CryptoWebhookToken {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid webhook token"})
		return
	}

	var payload struct {
		PaymentID int64  `json:"payment_id"`
		Status    string `json:"status"`
	}
	if err := c.ShouldBindJSON(&payload); err == nil {
		switch payload.Status {
		case "paid":
			_ = h.activateSubscription(c, fmt.Sprintf("%d", payload.PaymentID), 0)
		case "failed":
			_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE pricing_payments SET status = 'failed' WHERE id = ?`, payload.PaymentID)
		}
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) ensurePlans(c *gin.Context) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, plan := range planDefinitions {
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
			plan.Code,
			plan.Name,
			plan.BillingCycle,
			plan.DurationDays,
			plan.PriceUSD,
			now,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) activateSubscription(c *gin.Context, paymentID string, userID int64) error {
	tx, err := h.db.SQL.BeginTx(c.Request.Context(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `SELECT id, user_id, pricing_plan_id, status FROM pricing_payments WHERE id = ?`
	args := []any{paymentID}
	if userID != 0 {
		query += ` AND user_id = ?`
		args = append(args, userID)
	}

	var paymentRowID, paymentUserID, planID int64
	var paymentStatus string
	if err := tx.QueryRowContext(c.Request.Context(), query, args...).Scan(&paymentRowID, &paymentUserID, &planID, &paymentStatus); err != nil {
		return err
	}
	if paymentStatus == "paid" {
		return tx.Commit()
	}

	var durationDays int
	if err := tx.QueryRowContext(c.Request.Context(), `SELECT duration_days FROM pricing_plans WHERE id = ? LIMIT 1`, planID).Scan(&durationDays); err != nil {
		return err
	}

	now := time.Now().UTC()
	endsAt := now.Add(time.Duration(durationDays) * 24 * time.Hour)
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE user_subscriptions SET is_active = 0 WHERE user_id = ? AND is_active = 1`, paymentUserID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		c.Request.Context(),
		`INSERT INTO user_subscriptions (user_id, pricing_plan_id, starts_at, ends_at, is_active, created_at) VALUES (?, ?, ?, ?, 1, ?)`,
		paymentUserID,
		planID,
		now.Format(time.RFC3339Nano),
		endsAt.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE pricing_payments SET status = 'paid', paid_at = ? WHERE id = ?`, now.Format(time.RFC3339Nano), paymentRowID); err != nil {
		return err
	}
	return tx.Commit()
}

func (h *Handler) renderPaymentStatus(c *gin.Context, userID int64) {
	row := h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT p.id, plan.code, p.provider, p.payment_method, p.status, p.checkout_url, p.paid_at
		 FROM pricing_payments p
		 JOIN pricing_plans plan ON plan.id = p.pricing_plan_id
		 WHERE p.id = ? AND p.user_id = ?
		 LIMIT 1`,
		c.Param("paymentID"),
		userID,
	)

	var paymentID int64
	var planCode, provider, paymentMethod, paymentStatus string
	var checkoutURL, paidAt sql.NullString
	if err := row.Scan(&paymentID, &planCode, &provider, &paymentMethod, &paymentStatus, &checkoutURL, &paidAt); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payment not found"})
		return
	}

	response := gin.H{
		"payment_id":     paymentID,
		"plan_code":      planCode,
		"provider":       provider,
		"payment_method": paymentMethod,
		"payment_status": paymentStatus,
		"checkout_url":   nil,
		"paid_at":        nil,
	}
	if checkoutURL.Valid {
		response["checkout_url"] = checkoutURL.String
	}
	if paidAt.Valid {
		response["paid_at"] = paidAt.String
	}
	c.JSON(http.StatusOK, response)
}

func validateProviderMethod(provider, paymentMethod string) error {
	if provider == "stripe" && paymentMethod != "card" && paymentMethod != "paypal" {
		return errors.New("Stripe supports card or paypal")
	}
	if provider == "crypto" && paymentMethod != "crypto" {
		return errors.New("Crypto provider requires payment_method=crypto")
	}
	return nil
}

func createCheckoutPayload(provider, successURL, cancelURL string) (string, string, map[string]any) {
	return provider + "_local", successURL, map[string]any{"mode": "local_stub", "cancel_url": cancelURL}
}
