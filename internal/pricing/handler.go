package pricing

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	paymentcrypto "goapplyjob-golang-backend/internal/payments/crypto"
	gensqlc "goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Handler struct {
	cfg  config.Config
	db   *database.DB
	auth *auth.Handler
	q    *gensqlc.Queries
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
	return &Handler{cfg: cfg, db: db, auth: authHandler, q: gensqlc.New(db.PGX)}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/pricing/plans", h.listPlans)
	router.GET("/pricing/providers", h.listProviders)
	router.GET("/pricing/crypto/currencies", h.listCryptoCurrencies)
	router.GET("/pricing/subscription", h.subscriptionStatus)
	router.POST("/pricing/subscription/cancel", h.cancelSubscription)
	router.POST("/pricing/subscribe", h.subscribe)
	router.GET("/pricing/payments/:paymentID", h.paymentStatus)
	router.POST("/pricing/payments/:paymentID/confirm", h.confirmPayment)
	router.POST("/pricing/webhooks/stripe", h.stripeWebhook)
	router.POST("/pricing/webhooks/crypto", h.cryptoWebhook)
}

func (h *Handler) listProviders(c *gin.Context) {
	stripeEnabled := strings.TrimSpace(os.Getenv("STRIPE_SECRET_KEY")) != ""
	cryptoEnabled := true
	cryptoReason := any(nil)
	if _, err := paymentcrypto.GetGateway(h.cfg); err != nil {
		cryptoEnabled = false
		cryptoReason = "Crypto gateway is not configured"
	}
	c.JSON(http.StatusOK, gin.H{"items": []gin.H{
		{
			"provider":        "stripe",
			"payment_methods": []string{"card"},
			"enabled":         stripeEnabled,
			"reason":          map[bool]any{true: nil, false: "Stripe is not configured"}[stripeEnabled],
		},
		{
			"provider":        "crypto",
			"payment_methods": []string{"crypto"},
			"enabled":         cryptoEnabled,
			"reason":          cryptoReason,
		},
	}})
}

func (h *Handler) listPlans(c *gin.Context) {
	if err := h.ensurePlans(c); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load pricing plans"})
		return
	}
	rows, err := h.q.ListActivePricingPlans(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load pricing plans"})
		return
	}

	items := []gin.H{}
	for _, row := range rows {
		items = append(items, gin.H{
			"code":          row.Code,
			"name":          row.Name,
			"billing_cycle": row.BillingCycle,
			"duration_days": row.DurationDays,
			"price_usd":     row.PriceUsd,
		})
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

	plan, err := h.q.GetActivePricingPlanByCode(c.Request.Context(), strings.ToLower(strings.TrimSpace(payload.PlanCode)))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid pricing plan"})
		return
	}
	planID := plan.ID
	planCode := plan.Code
	planName := plan.Name
	durationDays := int(plan.DurationDays)
	priceUSD := int(plan.PriceUsd)
	if priceUSD == 0 {
		now := time.Now().UTC()
		paymentID, err := h.q.CreatePaidInternalPayment(
			c.Request.Context(),
			gensqlc.CreatePaidInternalPaymentParams{
				UserID:        user.ID,
				PricingPlanID: planID,
				PaidAt:        pgtype.Text{String: now.Format(time.RFC3339Nano), Valid: true},
				CreatedAt:     now.Format(time.RFC3339Nano),
			},
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
			return
		}
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
	paymentID, err := h.q.CreatePendingPayment(
		c.Request.Context(),
		gensqlc.CreatePendingPaymentParams{
			UserID:        user.ID,
			PricingPlanID: planID,
			Provider:      payload.Provider,
			PaymentMethod: payload.PaymentMethod,
			AmountMinor:   int32(priceUSD * 100),
			CreatedAt:     now,
		},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
		return
	}
	successURL = appendQueryParam(successURL, "payment_id", strconv.FormatInt(paymentID, 10))
	cancelURL = appendQueryParam(cancelURL, "payment_id", strconv.FormatInt(paymentID, 10))

	checkoutID, checkoutURL, providerPayload, err := h.createCryptoInvoice(planName, priceUSD, payload.PayCurrency, successURL, cancelURL, paymentID, user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
		return
	}
	payloadBytes, _ := json.Marshal(providerPayload)
	err = h.q.UpdatePaymentCheckoutInfo(
		c.Request.Context(),
		gensqlc.UpdatePaymentCheckoutInfoParams{
			ProviderCheckoutID: pgtype.Text{String: checkoutID, Valid: true},
			CheckoutUrl:        pgtype.Text{String: checkoutURL, Valid: true},
			ProviderPayload:    pgtype.Text{String: string(payloadBytes), Valid: true},
			ID:                 paymentID,
		},
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
	if err := h.confirmOrActivatePayment(c, c.Param("paymentID"), user.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Payment not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to confirm payment"})
		return
	}
	h.renderPaymentStatus(c, user.ID)
}

func (h *Handler) confirmOrActivatePayment(c *gin.Context, paymentID string, userID int64) error {
	paymentIDInt, err := strconv.ParseInt(strings.TrimSpace(paymentID), 10, 64)
	if err != nil {
		return sql.ErrNoRows
	}
	row, err := h.q.GetPaymentForUser(
		c.Request.Context(),
		gensqlc.GetPaymentForUserParams{
			ID:     paymentIDInt,
			UserID: userID,
		},
	)
	if err != nil {
		return err
	}
	if row.Status == "paid" {
		return nil
	}

	resolvedStatus := "pending"
	if row.Provider == "crypto" {
		gateway, err := paymentcrypto.GetGateway(h.cfg)
		if err == nil {
			verification, verifyErr := gateway.VerifyPayment(strings.TrimSpace(row.ProviderCheckoutID.String), paymentID)
			if verifyErr == nil && verification != nil {
				resolvedStatus = string(verification.Status)
				if verification.ProviderPayload != nil {
					mergedPayload, mergeErr := mergeProviderPayload(row.ProviderPayload, verification.ProviderPayload)
					if mergeErr == nil {
						mergedPayload["last_verified_at"] = time.Now().UTC().Format(time.RFC3339Nano)
						payloadBytes, _ := json.Marshal(mergedPayload)
						_ = h.q.UpdatePaymentPayloadByID(c.Request.Context(), gensqlc.UpdatePaymentPayloadByIDParams{
							ProviderPayload: pgtype.Text{String: string(payloadBytes), Valid: true},
							ID:              row.ID,
						})
					}
				}
			} else {
				resolvedStatus = extractProviderPayloadStatus(row.ProviderPayload)
			}
		}
	} else if row.Provider == "internal" {
		resolvedStatus = "paid"
	}

	switch resolvedStatus {
	case "paid":
		return h.activatePlan(c, row.UserID, row.PricingPlanID, int(row.DurationDays), row.ID)
	case "failed":
		return h.q.UpdatePaymentFailedByID(c.Request.Context(), row.ID)
	default:
		return h.activateSubscription(c, paymentID, userID)
	}
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
				if paymentID, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
					_ = h.q.UpdatePaymentFailedByID(c.Request.Context(), paymentID)
				}
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
	if err := gateway.VerifyWebhookSignature(payload, headers, rawBody); err != nil {
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
			_ = h.q.UpdatePaymentFailedByID(c.Request.Context(), paymentRowID)
		}
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) ensurePlans(c *gin.Context) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, plan := range h.planDefinitions() {
		err := h.q.UpsertPricingPlanByCode(
			c.Request.Context(),
			gensqlc.UpsertPricingPlanByCodeParams{
				Code:         plan.Code,
				Name:         plan.Name,
				BillingCycle: plan.BillingCycle,
				DurationDays: int32(plan.DurationDays),
				PriceUsd:     int32(plan.PriceUSD),
				CreatedAt:    now,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) activateSubscription(c *gin.Context, paymentID string, userID int64) error {
	paymentIDInt, err := strconv.ParseInt(strings.TrimSpace(paymentID), 10, 64)
	if err != nil {
		return sql.ErrNoRows
	}
	tx, err := h.db.PGX.Begin(c.Request.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(c.Request.Context())
	qtx := h.q.WithTx(tx)

	var paymentRowID, paymentUserID, planID int64
	var paymentStatus string
	if userID != 0 {
		row, err := qtx.GetPaymentByIDAndUser(c.Request.Context(), gensqlc.GetPaymentByIDAndUserParams{
			ID:     paymentIDInt,
			UserID: userID,
		})
		if err != nil {
			return err
		}
		paymentRowID = row.ID
		paymentUserID = row.UserID
		planID = row.PricingPlanID
		paymentStatus = row.Status
	} else {
		row, err := qtx.GetPaymentByID(c.Request.Context(), paymentIDInt)
		if err != nil {
			return err
		}
		paymentRowID = row.ID
		paymentUserID = row.UserID
		planID = row.PricingPlanID
		paymentStatus = row.Status
	}
	if paymentStatus == "paid" {
		return tx.Commit(c.Request.Context())
	}

	durationDays, err := qtx.GetPlanDurationByID(c.Request.Context(), planID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	endsAt := now.Add(time.Duration(durationDays) * 24 * time.Hour)
	if err := qtx.DeactivateActiveSubscriptionsByUser(c.Request.Context(), paymentUserID); err != nil {
		return err
	}
	if err := qtx.CreateUserSubscriptionActive(
		c.Request.Context(),
		gensqlc.CreateUserSubscriptionActiveParams{
			UserID:        paymentUserID,
			PricingPlanID: planID,
			StartsAt:      now.Format(time.RFC3339Nano),
			EndsAt:        endsAt.Format(time.RFC3339Nano),
			CreatedAt:     now.Format(time.RFC3339Nano),
		},
	); err != nil {
		return err
	}
	if err := qtx.MarkPaymentPaidByID(
		c.Request.Context(),
		gensqlc.MarkPaymentPaidByIDParams{
			PaidAt: pgtype.Text{String: now.Format(time.RFC3339Nano), Valid: true},
			ID:     paymentRowID,
		},
	); err != nil {
		return err
	}
	return tx.Commit(c.Request.Context())
}

func (h *Handler) activatePlan(c *gin.Context, userID, planID int64, durationDays int, paymentID int64) error {
	now := time.Now().UTC()
	endsAt := now.Add(time.Duration(durationDays) * 24 * time.Hour)
	tx, err := h.db.PGX.Begin(c.Request.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(c.Request.Context())
	qtx := h.q.WithTx(tx)
	if err := qtx.DeactivateActiveSubscriptionsByUser(c.Request.Context(), userID); err != nil {
		return err
	}
	if err := qtx.CreateUserSubscriptionActive(
		c.Request.Context(),
		gensqlc.CreateUserSubscriptionActiveParams{
			UserID:        userID,
			PricingPlanID: planID,
			StartsAt:      now.Format(time.RFC3339Nano),
			EndsAt:        endsAt.Format(time.RFC3339Nano),
			CreatedAt:     now.Format(time.RFC3339Nano),
		},
	); err != nil {
		return err
	}
	if err := qtx.MarkPaymentPaidByID(
		c.Request.Context(),
		gensqlc.MarkPaymentPaidByIDParams{
			PaidAt: pgtype.Text{String: now.Format(time.RFC3339Nano), Valid: true},
			ID:     paymentID,
		},
	); err != nil {
		return err
	}
	return tx.Commit(c.Request.Context())
}

func (h *Handler) renderPaymentStatus(c *gin.Context, userID int64) {
	paymentID, err := strconv.ParseInt(strings.TrimSpace(c.Param("paymentID")), 10, 64)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payment not found"})
		return
	}
	row, err := h.q.GetPaymentStatusViewByIDAndUser(c.Request.Context(), gensqlc.GetPaymentStatusViewByIDAndUserParams{
		ID:     paymentID,
		UserID: userID,
	})
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payment not found"})
		return
	}

	response := gin.H{
		"payment_id":     row.ID,
		"plan_code":      row.Code,
		"provider":       row.Provider,
		"payment_method": row.PaymentMethod,
		"payment_status": row.Status,
		"checkout_url":   nil,
		"paid_at":        nil,
	}
	if row.CheckoutUrl.Valid {
		response["checkout_url"] = row.CheckoutUrl.String
		response["invoice_url"] = row.CheckoutUrl.String
	}
	if row.PaidAt.Valid {
		response["paid_at"] = row.PaidAt.String
	}
	if row.ProviderPayload.Valid {
		payload := map[string]any{}
		if json.Unmarshal([]byte(row.ProviderPayload.String), &payload) == nil {
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
	subscription, err := h.q.GetLatestSubscriptionWithPlanByUser(c.Request.Context(), user.ID)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"is_active":              false,
			"status":                 "none",
			"days_left":              nil,
			"plan_code":              nil,
			"plan_name":              nil,
			"starts_at":              nil,
			"ends_at":                nil,
			"provider":               nil,
			"payment_method":         nil,
			"can_cancel":             false,
			"cancel_at_period_end":   false,
			"stripe_subscription_id": nil,
		})
		return
	}
	now := time.Now().UTC()
	isActive := false
	status := "expired"
	var daysLeft any
	if parsed, parseErr := parseUTCDateTime(subscription.EndsAt); parseErr == nil {
		nowUTC := ensureUTC(now)
		if parsed.After(nowUTC) {
			isActive = true
			status = "active"
		}
		daysLeft = getDaysLeft(parsed, nowUTC)
	}
	if !isActive {
		_ = h.q.DeactivateSubscriptionByID(c.Request.Context(), subscription.ID)
	}
	var provider, paymentMethod, providerPayload pgtype.Text
	if meta, metaErr := h.q.GetLatestPaidPaymentMetaByUserAndPlan(c.Request.Context(), gensqlc.GetLatestPaidPaymentMetaByUserAndPlanParams{
		UserID:        user.ID,
		PricingPlanID: subscription.ID_2,
	}); metaErr == nil {
		provider = pgtype.Text{String: meta.Provider, Valid: true}
		paymentMethod = pgtype.Text{String: meta.PaymentMethod, Valid: true}
		providerPayload = meta.ProviderPayload
	}
	cancelAtPeriodEnd := false
	canCancel := false
	var stripeSubscriptionID any
	if provider.Valid && provider.String == "stripe" && providerPayload.Valid && strings.TrimSpace(providerPayload.String) != "" {
		payload := map[string]any{}
		if json.Unmarshal([]byte(providerPayload.String), &payload) == nil {
			if rawID, ok := payload["stripe_subscription_id"].(string); ok && strings.TrimSpace(rawID) != "" {
				stripeSubscriptionID = strings.TrimSpace(rawID)
			}
			cancelAtPeriodEnd = payload["stripe_cancel_at_period_end"] == true
			canCancel = !cancelAtPeriodEnd && stripeSubscriptionID != nil
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"is_active":              isActive,
		"status":                 status,
		"days_left":              daysLeft,
		"plan_code":              subscription.Code,
		"plan_name":              subscription.Name,
		"starts_at":              nullStringValue(pgtype.Text{String: subscription.StartsAt, Valid: strings.TrimSpace(subscription.StartsAt) != ""}),
		"ends_at":                nullStringValue(pgtype.Text{String: subscription.EndsAt, Valid: strings.TrimSpace(subscription.EndsAt) != ""}),
		"provider":               nullStringValue(provider),
		"payment_method":         nullStringValue(paymentMethod),
		"can_cancel":             canCancel,
		"cancel_at_period_end":   cancelAtPeriodEnd,
		"stripe_subscription_id": stripeSubscriptionID,
	})
}

func (h *Handler) cancelSubscription(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	row, err := h.q.GetStripeCancelablePaymentByUser(c.Request.Context(), gensqlc.GetStripeCancelablePaymentByUserParams{
		UserID:   user.ID,
		UserID_2: user.ID,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No active Stripe subscription to cancel"})
		return
	}
	payload := map[string]any{}
	if row.ProviderPayload.Valid && strings.TrimSpace(row.ProviderPayload.String) != "" {
		_ = json.Unmarshal([]byte(row.ProviderPayload.String), &payload)
	}
	stripeSubscriptionID, _ := payload["stripe_subscription_id"].(string)
	if strings.TrimSpace(stripeSubscriptionID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No Stripe subscription id found"})
		return
	}
	payload["stripe_cancel_at_period_end"] = true
	payloadBytes, _ := json.Marshal(payload)
	if err := h.q.UpdatePaymentPayloadByID(c.Request.Context(), gensqlc.UpdatePaymentPayloadByIDParams{
		ProviderPayload: pgtype.Text{String: string(payloadBytes), Valid: true},
		ID:              row.ID,
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to cancel Stripe subscription"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ok":                     true,
		"stripe_subscription_id": stripeSubscriptionID,
		"cancel_at_period_end":   true,
		"current_period_end":     nil,
	})
}

func validateProviderMethod(provider, paymentMethod string) error {
	if provider == "stripe" && paymentMethod != "card" {
		return errors.New("Stripe supports card only")
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

func appendQueryParam(rawURL, key, value string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	query := parsed.Query()
	query.Del(key)
	query.Set(key, value)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func mergeProviderPayload(existing pgtype.Text, latest map[string]any) (map[string]any, error) {
	merged := map[string]any{}
	if existing.Valid && strings.TrimSpace(existing.String) != "" {
		if err := json.Unmarshal([]byte(existing.String), &merged); err != nil {
			return nil, err
		}
	}
	for key, value := range latest {
		merged[key] = value
	}
	return merged, nil
}

func extractProviderPayloadStatus(existing pgtype.Text) string {
	if !existing.Valid || strings.TrimSpace(existing.String) == "" {
		return "pending"
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(existing.String), &payload); err != nil {
		return "pending"
	}
	if strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", payload["mode"]))) == "local_stub" {
		return "paid"
	}
	candidates := []any{
		payload["payment_status"],
		payload["status"],
	}
	if nestedData, ok := payload["data"].(map[string]any); ok {
		candidates = append(candidates, nestedData["payment_status"], nestedData["status"])
	}
	if nestedPayment, ok := payload["payment"].(map[string]any); ok {
		candidates = append(candidates, nestedPayment["status"])
	}
	if nestedHotWallet, ok := payload["hotWallet"].(map[string]any); ok {
		candidates = append(candidates, nestedHotWallet["status"])
	}
	for _, candidate := range candidates {
		mapped := paymentcrypto.ParseGenericPaymentStatus(candidate)
		if mapped != "pending" {
			return mapped
		}
	}
	return "pending"
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
	switch {
	case paymentID != 0:
		row, err := h.q.GetPaymentForWebhookByPaymentID(c.Request.Context(), paymentID)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		return row.ID, row.UserID, row.PricingPlanID, int(row.DurationDays), nil
	case providerCheckoutID != "":
		row, err := h.q.GetPaymentForWebhookByCheckoutID(c.Request.Context(), pgtype.Text{String: providerCheckoutID, Valid: true})
		if err != nil {
			return 0, 0, 0, 0, err
		}
		return row.ID, row.UserID, row.PricingPlanID, int(row.DurationDays), nil
	default:
		return 0, 0, 0, 0, sql.ErrNoRows
	}
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

func nullStringValue(value pgtype.Text) any {
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
