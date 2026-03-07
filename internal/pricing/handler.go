package pricing

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	paymentcrypto "goapplyjob-golang-backend/internal/payments/crypto"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	cfg  config.Config
	db   *database.DB
	auth *auth.Handler
}

var basePlanDefinitions = []struct {
	Code         string
	Name         string
	BillingCycle string
	DurationDays int
	PriceUSD     int
}{
	{"free", "Free", "free", 7, 0},
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
	router.GET("/pricing/crypto/currencies", h.listCryptoCurrencies)
	router.GET("/pricing/subscription", h.subscriptionStatus)
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
		PayCurrency   string `json:"pay_currency"`
		SuccessURL    string `json:"success_url"`
		CancelURL     string `json:"cancel_url"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}
	if payload.Provider == "" {
		payload.Provider = "crypto"
	}
	if payload.PaymentMethod == "" {
		payload.PaymentMethod = "crypto"
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
	var planCode, planName string
	var priceUSD, durationDays int
	err = h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT id, code, name, duration_days, price_usd
		 FROM pricing_plans
		 WHERE code = ? AND is_active = 1
		 LIMIT 1`,
		strings.ToLower(strings.TrimSpace(payload.PlanCode)),
	).Scan(&planID, &planCode, &planName, &durationDays, &priceUSD)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid pricing plan"})
		return
	}
	if priceUSD == 0 {
		now := time.Now().UTC()
		result, err := h.db.SQL.ExecContext(
			c.Request.Context(),
			`INSERT INTO pricing_payments
			(user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, checkout_url, provider_payload, paid_at, created_at)
			VALUES (?, ?, 'internal', 'free', 'USD', 0, 'paid', NULL, NULL, '{}', ?, ?)`,
			user.ID,
			planID,
			now.Format(time.RFC3339Nano),
			now.Format(time.RFC3339Nano),
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
			return
		}
		paymentID, _ := result.LastInsertId()
		if err := h.activatePlan(c, user.ID, planID, durationDays, paymentID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to activate subscription"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"ok":             true,
			"payment_id":     paymentID,
			"plan_code":      planCode,
			"provider":       "internal",
			"payment_method": "free",
			"payment_status": "paid",
			"checkout_url":   nil,
			"invoice_url":    nil,
			"crypto_payment": nil,
		})
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := h.db.SQL.ExecContext(
		c.Request.Context(),
		`INSERT INTO pricing_payments
		(user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, checkout_url, provider_payload, created_at)
		VALUES (?, ?, ?, ?, 'USD', ?, 'pending', NULL, NULL, '{}', ?)`,
		user.ID,
		planID,
		payload.Provider,
		payload.PaymentMethod,
		priceUSD*100,
		now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
		return
	}
	paymentID, _ := result.LastInsertId()

	checkoutID, checkoutURL, providerPayload, err := h.createCryptoInvoice(planName, priceUSD, payload.PayCurrency, successURL, cancelURL, paymentID, user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
		return
	}
	payloadBytes, _ := json.Marshal(providerPayload)
	_, err = h.db.SQL.ExecContext(
		c.Request.Context(),
		`UPDATE pricing_payments
		 SET provider_checkout_id = ?, checkout_url = ?, provider_payload = ?
		 WHERE id = ?`,
		checkoutID,
		checkoutURL,
		string(payloadBytes),
		paymentID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ok":             true,
		"payment_id":     paymentID,
		"plan_code":      planCode,
		"provider":       payload.Provider,
		"payment_method": payload.PaymentMethod,
		"payment_status": "pending",
		"checkout_url":   checkoutURL,
		"invoice_url":    checkoutURL,
		"crypto_payment": extractCryptoPayment(providerPayload, checkoutURL),
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
	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid webhook payload"})
		return
	}
	payload := map[string]any{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid webhook payload"})
		return
	}
	gateway, err := paymentcrypto.GetGateway(h.cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Invalid crypto gateway"})
		return
	}
	headers := map[string]string{}
	for key, values := range c.Request.Header {
		if len(values) > 0 {
			headers[strings.ToLower(key)] = values[0]
		}
	}
	if err := gateway.VerifyWebhookSignature(payload, headers); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": err.Error()})
		return
	}
	info := gateway.ParseWebhook(payload)
	paymentID := int64(0)
	if strings.TrimSpace(info.OrderID) != "" {
		fmt.Sscanf(info.OrderID, "%d", &paymentID)
	}
	providerCheckoutID := info.ProviderPaymentID
	paymentRowID, userID, planID, durationDays, err := h.lookupPaymentForWebhook(c, paymentID, providerCheckoutID)
	if err == nil {
		switch info.Status {
		case paymentcrypto.PaymentPaid:
			_ = h.activatePlan(c, userID, planID, durationDays, paymentRowID)
		case paymentcrypto.PaymentFailed:
			_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE pricing_payments SET status = 'failed' WHERE id = ?`, paymentRowID)
		}
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) ensurePlans(c *gin.Context) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, plan := range h.planDefinitions() {
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

func (h *Handler) activatePlan(c *gin.Context, userID, planID int64, durationDays int, paymentID int64) error {
	now := time.Now().UTC()
	endsAt := now.Add(time.Duration(durationDays) * 24 * time.Hour)
	tx, err := h.db.SQL.BeginTx(c.Request.Context(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE user_subscriptions SET is_active = 0 WHERE user_id = ? AND is_active = 1`, userID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		c.Request.Context(),
		`INSERT INTO user_subscriptions (user_id, pricing_plan_id, starts_at, ends_at, is_active, created_at) VALUES (?, ?, ?, ?, 1, ?)`,
		userID,
		planID,
		now.Format(time.RFC3339Nano),
		endsAt.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE pricing_payments SET status = 'paid', paid_at = ? WHERE id = ?`, now.Format(time.RFC3339Nano), paymentID); err != nil {
		return err
	}
	return tx.Commit()
}

func (h *Handler) renderPaymentStatus(c *gin.Context, userID int64) {
	var paymentID int64
	var planCode, provider, paymentMethod, paymentStatus string
	var checkoutURL, paidAt, providerPayload sql.NullString
	row := h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT p.id, plan.code, p.provider, p.payment_method, p.status, p.checkout_url, p.paid_at, p.provider_payload
		 FROM pricing_payments p
		 JOIN pricing_plans plan ON plan.id = p.pricing_plan_id
		 WHERE p.id = ? AND p.user_id = ?
		 LIMIT 1`,
		c.Param("paymentID"),
		userID,
	)
	if err := row.Scan(&paymentID, &planCode, &provider, &paymentMethod, &paymentStatus, &checkoutURL, &paidAt, &providerPayload); err != nil {
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
		response["invoice_url"] = checkoutURL.String
	}
	if paidAt.Valid {
		response["paid_at"] = paidAt.String
	}
	if providerPayload.Valid {
		payload := map[string]any{}
		if json.Unmarshal([]byte(providerPayload.String), &payload) == nil {
			response["crypto_payment"] = extractCryptoPayment(payload, response["checkout_url"])
		}
	}
	c.JSON(http.StatusOK, response)
}

func (h *Handler) listCryptoCurrencies(c *gin.Context) {
	var amountUSD *float64
	if raw := strings.TrimSpace(c.Query("amount_usd")); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
			amountUSD = &parsed
		}
	}
	gateway, err := paymentcrypto.GetGateway(h.cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Invalid crypto gateway"})
		return
	}
	options := gateway.ListCurrencies(amountUSD)
	out := make([]gin.H, 0, len(options))
	for _, item := range options {
		out = append(out, gin.H{"code": item.Code, "min_usd": item.MinUSD})
	}
	c.JSON(http.StatusOK, gin.H{"items": out})
}

func (h *Handler) subscriptionStatus(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	var subscriptionID int64
	var planCode, planName string
	var startsAt, endsAt sql.NullString
	err = h.db.SQL.QueryRowContext(
		c.Request.Context(),
		`SELECT s.id, p.code, p.name, s.starts_at, s.ends_at
		 FROM user_subscriptions s
		 JOIN pricing_plans p ON p.id = s.pricing_plan_id
		 WHERE s.user_id = ? AND p.is_active = 1
		 ORDER BY s.ends_at DESC
		 LIMIT 1`,
		user.ID,
	).Scan(&subscriptionID, &planCode, &planName, &startsAt, &endsAt)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"is_active": false, "status": "none", "days_left": nil, "plan_code": nil, "plan_name": nil, "starts_at": nil, "ends_at": nil})
		return
	}
	now := time.Now().UTC()
	isActive := false
	status := "expired"
	var daysLeft any
	if endsAt.Valid {
		if parsed, err := parseUTCDateTime(endsAt.String); err == nil {
			nowUTC := ensureUTC(now)
			if parsed.After(nowUTC) {
				isActive = true
				status = "active"
			}
			daysLeft = getDaysLeft(parsed, nowUTC)
		}
	}
	if !isActive {
		_, _ = h.db.SQL.ExecContext(c.Request.Context(), `UPDATE user_subscriptions SET is_active = 0 WHERE id = ? AND is_active = 1`, subscriptionID)
	}
	c.JSON(http.StatusOK, gin.H{
		"is_active": isActive,
		"status":    status,
		"days_left": daysLeft,
		"plan_code": planCode,
		"plan_name": planName,
		"starts_at": nullStringValue(startsAt),
		"ends_at":   nullStringValue(endsAt),
	})
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

func (h *Handler) createCryptoInvoice(planName string, priceUSD int, payCurrency, successURL, cancelURL string, paymentID int64, user *auth.User) (string, string, map[string]any, error) {
	gateway, err := paymentcrypto.GetGateway(h.cfg)
	if err != nil {
		return "", "", nil, err
	}
	callbackURL := strings.TrimSpace(h.cfg.CryptoIPNCallbackURL)
	email := strings.TrimSpace(user.Email)
	nameGuess := email
	if parts := strings.SplitN(email, "@", 2); len(parts) == 2 {
		nameGuess = parts[0]
	}
	result, err := gateway.CreateInvoice(paymentcrypto.InvoiceRequest{
		OrderID:       fmt.Sprintf("%d", paymentID),
		AmountUSD:     float64(priceUSD),
		Description:   fmt.Sprintf("GoApplyJob %s plan", planName),
		SuccessURL:    successURL,
		CancelURL:     cancelURL,
		CallbackURL:   callbackURL,
		PayCurrency:   payCurrency,
		CustomerEmail: email,
		CustomerName:  nameGuess,
	})
	if err != nil {
		return "", "", nil, err
	}
	return result.ProviderInvoiceID, result.InvoiceURL, result.ProviderPayload, nil
}

func (h *Handler) planDefinitions() []struct {
	Code         string
	Name         string
	BillingCycle string
	DurationDays int
	PriceUSD     int
} {
	plans := append([]struct {
		Code         string
		Name         string
		BillingCycle string
		DurationDays int
		PriceUSD     int
	}{}, basePlanDefinitions...)
	plans[0].DurationDays = max(h.cfg.FreePlanDurationDays, 1)
	return plans
}

func lookupNowPaymentsRefs(payload map[string]any) (int64, string) {
	var paymentID int64
	if rawOrderID, ok := payload["order_id"]; ok {
		switch value := rawOrderID.(type) {
		case string:
			fmt.Sscanf(value, "%d", &paymentID)
		case float64:
			paymentID = int64(value)
		}
	}
	providerID := ""
	if rawProviderID, ok := payload["payment_id"].(string); ok {
		providerID = rawProviderID
	}
	return paymentID, providerID
}

func (h *Handler) lookupPaymentForWebhook(c *gin.Context, paymentID int64, providerCheckoutID string) (int64, int64, int64, int, error) {
	query := `SELECT pay.id, pay.user_id, pay.pricing_plan_id, plan.duration_days
		FROM pricing_payments pay
		JOIN pricing_plans plan ON plan.id = pay.pricing_plan_id`
	args := []any{}
	switch {
	case paymentID != 0:
		query += ` WHERE pay.id = ? LIMIT 1`
		args = append(args, paymentID)
	case providerCheckoutID != "":
		query += ` WHERE pay.provider_checkout_id = ? LIMIT 1`
		args = append(args, providerCheckoutID)
	default:
		return 0, 0, 0, 0, sql.ErrNoRows
	}
	var rowID, userID, planID int64
	var durationDays int
	err := h.db.SQL.QueryRowContext(c.Request.Context(), query, args...).Scan(&rowID, &userID, &planID, &durationDays)
	return rowID, userID, planID, durationDays, err
}

func extractCryptoPayment(providerPayload map[string]any, checkoutURL any) any {
	if len(providerPayload) == 0 {
		return nil
	}

	nestedData := nestedMap(providerPayload["data"])
	nestedPayment := nestedMap(providerPayload["payment"])
	nestedHotWallet := nestedMap(providerPayload["hotWallet"])
	expiresAt := firstNonEmpty(
		providerPayload["expiration_estimate_date"],
		providerPayload["expires_at"],
		providerPayload["expired_at"],
		nestedData["expiration_estimate_date"],
		nestedData["expires_at"],
		nestedData["expired_at"],
		nestedHotWallet["expiresAt"],
		nestedHotWallet["expiredAt"],
		nestedPayment["expiresAt"],
		nestedPayment["expiredAt"],
	)

	return gin.H{
		"payment_id": firstNonEmpty(
			providerPayload["payment_id"],
			providerPayload["id"],
			nestedData["payment_id"],
			nestedData["id"],
			nestedData["track_id"],
			nestedPayment["id"],
		),
		"invoice_id": firstNonEmpty(
			providerPayload["invoice_id"],
			providerPayload["id"],
			nestedData["invoice_id"],
			nestedData["id"],
			nestedData["track_id"],
		),
		"pay_currency": firstNonEmpty(
			providerPayload["pay_currency"],
			nestedData["pay_currency"],
			nestedHotWallet["currency"],
			nestedPayment["paymentCurrency"],
		),
		"pay_amount": firstNonEmpty(
			providerPayload["pay_amount"],
			nestedData["pay_amount"],
			nestedHotWallet["paymentAmount"],
			nestedHotWallet["amount"],
		),
		"pay_address": firstNonEmpty(
			providerPayload["pay_address"],
			nestedData["pay_address"],
			nestedHotWallet["address"],
			nestedHotWallet["paymentAddress"],
		),
		"network": firstNonEmpty(
			providerPayload["network"],
			nestedData["network"],
			nestedHotWallet["network"],
		),
		"invoice_url": firstNonEmpty(
			providerPayload["invoice_url"],
			providerPayload["checkout_url"],
			nestedData["invoice_url"],
			nestedData["checkout_url"],
			nestedData["payment_url"],
			nestedData["link"],
			checkoutURL,
		),
		"expiration_estimate_date": expiresAt,
		"expires_at":               expiresAt,
		"created_at":               firstNonEmpty(providerPayload["created_at"], nestedData["created_at"], nestedData["date"]),
		"updated_at":               firstNonEmpty(providerPayload["updated_at"], nestedData["updated_at"]),
		"payment_status": firstNonEmpty(
			providerPayload["payment_status"],
			providerPayload["status"],
			nestedData["payment_status"],
			nestedData["status"],
		),
	}
}

func nestedMap(value any) map[string]any {
	if result, ok := value.(map[string]any); ok {
		return result
	}
	return map[string]any{}
}

func nullStringValue(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

func firstNonEmpty(values ...any) any {
	for _, value := range values {
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			return text
		}
		if value != nil && value != "<nil>" {
			return value
		}
	}
	return nil
}

func getDaysLeft(endsAt, now time.Time) int {
	diffSeconds := ensureUTC(endsAt).Sub(ensureUTC(now)).Seconds()
	days := int((diffSeconds + 86399) / 86400)
	if days < 0 {
		return 0
	}
	return days
}

func ensureUTC(value time.Time) time.Time {
	if value.Location() == nil {
		return time.Date(value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), time.UTC)
	}
	return value.UTC()
}

func parseUTCDateTime(raw string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ensureUTC(parsed), nil
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return ensureUTC(parsed), nil
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", raw); err == nil {
		return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(), parsed.Nanosecond(), time.UTC), nil
	}
	return time.Time{}, fmt.Errorf("invalid datetime")
}
