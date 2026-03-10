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
	paymentdodo "goapplyjob-golang-backend/internal/payments/dodo"
	paymentstripe "goapplyjob-golang-backend/internal/payments/stripe"
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
	router.POST("/pricing/webhooks/dodo", h.dodoWebhook)
	router.POST("/pricing/webhooks/crypto", h.cryptoWebhook)
}

func (h *Handler) listProviders(c *gin.Context) {
	stripeEnabled := strings.TrimSpace(os.Getenv("STRIPE_SECRET_KEY")) != ""
	dodoEnabled, dodoReason := paymentdodo.IsConfigured()
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
			"provider":        "dodo",
			"payment_methods": []string{"card"},
			"enabled":         dodoEnabled,
			"reason":          map[bool]any{true: nil, false: dodoReason}[dodoEnabled],
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
	effectiveProvider := payload.Provider
	if payload.PaymentMethod == "card" {
		effectiveProvider, err = resolveCardProvider(payload.Provider)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
			return
		}
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
				PaidAt:        pgTimestamptz(now),
				CreatedAt:     pgTimestamptz(now),
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
	now := time.Now().UTC()
	paymentID, err := h.q.CreatePendingPayment(
		c.Request.Context(),
		gensqlc.CreatePendingPaymentParams{
			UserID:        user.ID,
			PricingPlanID: planID,
			Provider:      effectiveProvider,
			PaymentMethod: payload.PaymentMethod,
			AmountMinor:   int32(priceUSD * 100),
			CreatedAt:     pgTimestamptz(now),
		},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
		return
	}
	successURL = appendQueryParam(successURL, "payment_id", strconv.FormatInt(int64(paymentID), 10))
	cancelURL = appendQueryParam(cancelURL, "payment_id", strconv.FormatInt(int64(paymentID), 10))

	var checkoutID, checkoutURL string
	var providerPayload map[string]any
	switch strings.ToLower(strings.TrimSpace(effectiveProvider)) {
	case "dodo":
		checkoutID, checkoutURL, providerPayload, err = paymentdodo.CreateCheckout(paymentID, user.ID, planCode, user.Email, successURL)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
			return
		}
	case "stripe":
		checkoutID, checkoutURL, providerPayload, err = paymentstripe.CreateCheckoutSession(
			planName,
			strings.TrimSpace(plan.BillingCycle),
			priceUSD,
			successURL,
			cancelURL,
			int64(paymentID),
			user.Email,
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
			return
		}
	default:
		checkoutID, checkoutURL, providerPayload, err = h.createCryptoInvoice(planName, priceUSD, payload.PayCurrency, successURL, cancelURL, paymentID, user)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create payment"})
			return
		}
	}
	payloadBytes, _ := json.Marshal(providerPayload)
	err = h.q.UpdatePaymentCheckoutInfo(
		c.Request.Context(),
		gensqlc.UpdatePaymentCheckoutInfoParams{
			ProviderCheckoutID: pgtype.Text{String: checkoutID, Valid: true},
			CheckoutUrl:        pgtype.Text{String: checkoutURL, Valid: true},
			ProviderPayload:    payloadBytes,
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
		"provider":       effectiveProvider,
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

func (h *Handler) confirmOrActivatePayment(c *gin.Context, paymentID string, userID int32) error {
	paymentIDInt64, err := strconv.ParseInt(strings.TrimSpace(paymentID), 10, 64)
	if err != nil || paymentIDInt64 <= 0 || paymentIDInt64 > int64(^uint32(0)>>1) {
		return sql.ErrNoRows
	}
	paymentIDInt := int32(paymentIDInt64)
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
							ProviderPayload: payloadBytes,
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
	} else if row.Provider == "stripe" {
		status, payload := paymentstripe.ResolveCheckoutStatus(strings.TrimSpace(row.ProviderCheckoutID.String))
		if len(payload) > 0 {
			mergedPayload, mergeErr := mergeProviderPayload(row.ProviderPayload, payload)
			if mergeErr == nil {
				payloadBytes, _ := json.Marshal(mergedPayload)
				_ = h.q.UpdatePaymentPayloadByID(c.Request.Context(), gensqlc.UpdatePaymentPayloadByIDParams{
					ProviderPayload: payloadBytes,
					ID:              row.ID,
				})
			}
		}
		resolvedStatus = status
	} else if row.Provider == "dodo" {
		var merged map[string]any
		resolvedStatus, merged = paymentdodo.ResolveCheckoutStatus(strings.TrimSpace(row.ProviderCheckoutID.String), row.ProviderPayload)
		if len(merged) > 0 {
			payloadBytes, _ := json.Marshal(merged)
			_ = h.q.UpdatePaymentPayloadByID(c.Request.Context(), gensqlc.UpdatePaymentPayloadByIDParams{
				ProviderPayload: payloadBytes,
				ID:              row.ID,
			})
		}
	}

	switch resolvedStatus {
	case "paid":
		return h.activatePlan(c, row.UserID, row.PricingPlanID, int(row.DurationDays), row.ID)
	case "failed":
		return h.q.UpdatePaymentFailedByID(c.Request.Context(), row.ID)
	default:
		return nil
	}
}

func (h *Handler) stripeWebhook(c *gin.Context) {
	stripeWebhookSecret := strings.TrimSpace(os.Getenv("STRIPE_WEBHOOK_SECRET"))
	if stripeWebhookSecret != "" && strings.TrimSpace(c.GetHeader("Stripe-Signature")) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Missing Stripe-Signature"})
		return
	}
	var payload struct {
		Type string `json:"type"`
		Data struct {
			Object struct {
				ID            string         `json:"id"`
				Subscription  any            `json:"subscription"`
				Status        string         `json:"status"`
				PaymentStatus string         `json:"payment_status"`
				Metadata      map[string]any `json:"metadata"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := c.ShouldBindJSON(&payload); err == nil && payload.Data.Object.Metadata != nil {
		var paymentID int32
		if raw, ok := payload.Data.Object.Metadata["payment_id"]; ok {
			if parsed, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprintf("%v", raw)), 10, 64); err == nil && parsed > 0 && parsed <= int64(^uint32(0)>>1) {
				paymentID = int32(parsed)
			}
		}
		checkoutID := strings.TrimSpace(payload.Data.Object.ID)
		providerCheckoutID := checkoutID
		rowID, userID, planID, durationDays, lookupErr := h.lookupPaymentForWebhook(c, paymentID, providerCheckoutID)
		if lookupErr == nil {
			payloadUpdate := map[string]any{
				"stripe_checkout_status": strings.TrimSpace(payload.Data.Object.Status),
				"stripe_payment_status":  strings.TrimSpace(payload.Data.Object.PaymentStatus),
			}
			if rawSub := payload.Data.Object.Subscription; rawSub != nil {
				if sub := strings.TrimSpace(fmt.Sprintf("%v", rawSub)); sub != "" {
					payloadUpdate["stripe_subscription_id"] = sub
				}
			}
			if len(payloadUpdate) > 0 {
				if row, err := h.q.GetPaymentForWebhookByPaymentID(c.Request.Context(), rowID); err == nil {
					merged, mergeErr := mergeProviderPayload(row.ProviderPayload, payloadUpdate)
					if mergeErr == nil {
						body, _ := json.Marshal(merged)
						_ = h.q.UpdatePaymentPayloadByID(c.Request.Context(), gensqlc.UpdatePaymentPayloadByIDParams{
							ProviderPayload: body,
							ID:              row.ID,
						})
					}
				}
			}
			switch payload.Type {
			case "checkout.session.completed":
				_ = h.activatePlan(c, userID, planID, durationDays, rowID)
			case "checkout.session.expired", "payment_intent.payment_failed":
				_ = h.q.UpdatePaymentFailedByID(c.Request.Context(), rowID)
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) dodoWebhook(c *gin.Context) {
	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid webhook payload"})
		return
	}
	eventType, metadata, obj, err := paymentdodo.ParseWebhook(rawBody, c.Request.Header)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid webhook payload"})
		return
	}

	var paymentID int32
	if raw, ok := metadata["payment_id"]; ok {
		if parsed, parseErr := strconv.ParseInt(strings.TrimSpace(fmt.Sprintf("%v", raw)), 10, 64); parseErr == nil && parsed > 0 && parsed <= int64(^uint32(0)>>1) {
			paymentID = int32(parsed)
		}
	}
	row, err := h.lookupDodoWebhookPayment(c, paymentID, eventType, obj)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}

	update := map[string]any{}
	if statusRaw, ok := obj["status"]; ok {
		update[paymentdodo.PayloadPaymentStatus] = strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", statusRaw)))
	}
	if checkoutRaw, ok := obj["checkout_status"]; ok {
		update[paymentdodo.PayloadCheckoutStatus] = strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", checkoutRaw)))
	}
	if subRaw, ok := obj["subscription_id"]; ok {
		if subID := strings.TrimSpace(fmt.Sprintf("%v", subRaw)); subID != "" {
			update[paymentdodo.PayloadSubscriptionID] = subID
		}
	} else if strings.HasPrefix(eventType, "subscription.") {
		if subID := strings.TrimSpace(fmt.Sprintf("%v", obj["id"])); subID != "" {
			update[paymentdodo.PayloadSubscriptionID] = subID
		}
	}
	if subID := strings.TrimSpace(fmt.Sprintf("%v", update[paymentdodo.PayloadSubscriptionID])); subID != "" {
		if refreshed := paymentdodo.RefreshSubscriptionState(subID); len(refreshed) > 0 {
			for key, value := range refreshed {
				update[key] = value
			}
		}
	}
	if len(update) > 0 {
		if pay, payErr := h.q.GetPaymentForUser(c.Request.Context(), gensqlc.GetPaymentForUserParams{ID: row.ID, UserID: row.UserID}); payErr == nil {
			merged, mergeErr := mergeProviderPayload(pay.ProviderPayload, update)
			if mergeErr == nil {
				body, _ := json.Marshal(merged)
				_ = h.q.UpdatePaymentPayloadByID(c.Request.Context(), gensqlc.UpdatePaymentPayloadByIDParams{
					ProviderPayload: body,
					ID:              row.ID,
				})
			}
		}
	}

	mappedStatus := "pending"
	if strings.HasPrefix(eventType, "payment.") {
		mappedStatus = paymentcrypto.ParseGenericPaymentStatus(update[paymentdodo.PayloadPaymentStatus])
	} else if strings.HasPrefix(eventType, "subscription.") {
		subStatus := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", update[paymentdodo.PayloadSubscriptionStatus])))
		switch subStatus {
		case "active", "trialing":
			mappedStatus = "paid"
		case "failed", "cancelled", "canceled", "expired":
			mappedStatus = "failed"
		}
	}
	switch mappedStatus {
	case "paid":
		_ = h.activatePlan(c, row.UserID, row.PricingPlanID, int(row.DurationDays), row.ID)
	case "failed":
		_ = h.q.UpdatePaymentFailedByID(c.Request.Context(), row.ID)
	}
	if strings.HasPrefix(eventType, "subscription.") {
		_ = h.syncUserSubscriptionFromDodoStatus(
			c,
			row.UserID,
			row.PricingPlanID,
			strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", update[paymentdodo.PayloadSubscriptionStatus]))),
			update[paymentdodo.PayloadCurrentPeriodEnd],
		)
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
	paymentID := int32(0)
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
	now := time.Now().UTC()
	for _, plan := range h.planDefinitions() {
		err := h.q.UpsertPricingPlanByCode(
			c.Request.Context(),
			gensqlc.UpsertPricingPlanByCodeParams{
				Code:         plan.Code,
				Name:         plan.Name,
				BillingCycle: plan.BillingCycle,
				DurationDays: int32(plan.DurationDays),
				PriceUsd:     int32(plan.PriceUSD),
				CreatedAt:    pgTimestamptz(now),
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) activateSubscription(c *gin.Context, paymentID string, userID int32) error {
	paymentIDInt64, err := strconv.ParseInt(strings.TrimSpace(paymentID), 10, 64)
	if err != nil || paymentIDInt64 <= 0 || paymentIDInt64 > int64(^uint32(0)>>1) {
		return sql.ErrNoRows
	}
	paymentIDInt := int32(paymentIDInt64)
	tx, err := h.db.PGX.Begin(c.Request.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(c.Request.Context())
	qtx := h.q.WithTx(tx)

	var paymentRowID, paymentUserID, planID int32
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
			StartsAt:      pgTimestamptz(now),
			EndsAt:        pgTimestamptz(endsAt),
			CreatedAt:     pgTimestamptz(now),
		},
	); err != nil {
		return err
	}
	if err := qtx.MarkPaymentPaidByID(
		c.Request.Context(),
		gensqlc.MarkPaymentPaidByIDParams{
			PaidAt: pgTimestamptz(now),
			ID:     paymentRowID,
		},
	); err != nil {
		return err
	}
	return tx.Commit(c.Request.Context())
}

func (h *Handler) activatePlan(c *gin.Context, userID, planID int32, durationDays int, paymentID int32) error {
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
			StartsAt:      pgTimestamptz(now),
			EndsAt:        pgTimestamptz(endsAt),
			CreatedAt:     pgTimestamptz(now),
		},
	); err != nil {
		return err
	}
	if err := qtx.MarkPaymentPaidByID(
		c.Request.Context(),
		gensqlc.MarkPaymentPaidByIDParams{
			PaidAt: pgTimestamptz(now),
			ID:     paymentID,
		},
	); err != nil {
		return err
	}
	return tx.Commit(c.Request.Context())
}

func (h *Handler) renderPaymentStatus(c *gin.Context, userID int32) {
	paymentID64, err := strconv.ParseInt(strings.TrimSpace(c.Param("paymentID")), 10, 64)
	if err != nil || paymentID64 <= 0 || paymentID64 > int64(^uint32(0)>>1) {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payment not found"})
		return
	}
	paymentID := int32(paymentID64)
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
		response["paid_at"] = row.PaidAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if len(row.ProviderPayload) > 0 {
		payload := map[string]any{}
		if json.Unmarshal(row.ProviderPayload, &payload) == nil {
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
	if subscription.EndsAt.Valid {
		parsed := ensureUTC(subscription.EndsAt.Time)
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
	var provider, paymentMethod string
	var providerPayload []byte
	if meta, metaErr := h.q.GetLatestPaidPaymentMetaByUserAndPlan(c.Request.Context(), gensqlc.GetLatestPaidPaymentMetaByUserAndPlanParams{
		UserID:        user.ID,
		PricingPlanID: subscription.ID_2,
	}); metaErr == nil {
		provider = meta.Provider
		paymentMethod = meta.PaymentMethod
		providerPayload = meta.ProviderPayload
	}
	cancelAtPeriodEnd := false
	canCancel := false
	var stripeSubscriptionID any
	var subscriptionID any
	if provider == "stripe" && len(providerPayload) > 0 {
		payload := map[string]any{}
		if json.Unmarshal(providerPayload, &payload) == nil {
			if rawID, ok := payload["stripe_subscription_id"].(string); ok && strings.TrimSpace(rawID) != "" {
				stripeSubscriptionID = strings.TrimSpace(rawID)
				subscriptionID = stripeSubscriptionID
			}
			cancelAtPeriodEnd = payload["stripe_cancel_at_period_end"] == true
			canCancel = !cancelAtPeriodEnd && stripeSubscriptionID != nil
		}
	} else if provider == "dodo" && len(providerPayload) > 0 {
		dodoSubID := paymentdodo.ExtractSubscriptionID(providerPayload)
		if dodoSubID != "" {
			subscriptionID = dodoSubID
		}
		payload := map[string]any{}
		if json.Unmarshal(providerPayload, &payload) == nil {
			cancelAtPeriodEnd = payload[paymentdodo.PayloadCancelAtPeriodEnd] == true
			status := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", payload[paymentdodo.PayloadSubscriptionStatus])))
			canCancel = !cancelAtPeriodEnd && dodoSubID != "" && status != "cancelled" && status != "canceled" && status != "expired" && status != "failed"
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"is_active":              isActive,
		"status":                 status,
		"days_left":              daysLeft,
		"plan_code":              subscription.Code,
		"plan_name":              subscription.Name,
		"starts_at":              timestamptzStringOrNil(subscription.StartsAt),
		"ends_at":                timestamptzStringOrNil(subscription.EndsAt),
		"provider":               emptyStringToNil(provider),
		"payment_method":         emptyStringToNil(paymentMethod),
		"can_cancel":             canCancel,
		"cancel_at_period_end":   cancelAtPeriodEnd,
		"subscription_id":        subscriptionID,
		"stripe_subscription_id": stripeSubscriptionID,
	})
}

func (h *Handler) cancelSubscription(c *gin.Context) {
	user, err := h.auth.CurrentUser(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Not authenticated"})
		return
	}
	row, err := h.q.GetCancelableCardPaymentByUser(c.Request.Context(), gensqlc.GetCancelableCardPaymentByUserParams{
		UserID:   user.ID,
		UserID_2: user.ID,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No active subscription to cancel"})
		return
	}
	payload := map[string]any{}
	if len(row.ProviderPayload) > 0 {
		_ = json.Unmarshal(row.ProviderPayload, &payload)
	}
	subscriptionID := ""
	updated := map[string]any{}
	if row.Provider == "stripe" {
		stripeSubscriptionID, _ := payload["stripe_subscription_id"].(string)
		if strings.TrimSpace(stripeSubscriptionID) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Stripe subscription id not found"})
			return
		}
		var err error
		updated, err = paymentstripe.CancelSubscriptionAtPeriodEnd(stripeSubscriptionID)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"detail": "Failed to cancel Stripe subscription: " + err.Error()})
			return
		}
		payload["stripe_subscription_id"] = stripeSubscriptionID
		payload["stripe_subscription_status"] = strings.TrimSpace(fmt.Sprintf("%v", updated["status"]))
		payload["stripe_cancel_at_period_end"] = updated["cancel_at_period_end"] == true
		payload["stripe_current_period_end"] = updated["current_period_end"]
		payload["stripe_canceled_at"] = updated["canceled_at"]
		subscriptionID = stripeSubscriptionID
	} else if row.Provider == "dodo" {
		dodoSubscriptionID := paymentdodo.ExtractSubscriptionID(row.ProviderPayload)
		if strings.TrimSpace(dodoSubscriptionID) == "" {
			if refreshed, _ := paymentdodo.ResolveCheckoutStatus("", row.ProviderPayload); refreshed != "" {
				// no-op; keep compatibility with existing payload parsing path
			}
		}
		if strings.TrimSpace(dodoSubscriptionID) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Dodo subscription id not found"})
			return
		}
		var err error
		updated, err = paymentdodo.CancelSubscription(dodoSubscriptionID)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"detail": "Failed to cancel Dodo subscription: " + err.Error()})
			return
		}
		for key, value := range updated {
			payload[key] = value
		}
		subscriptionID = dodoSubscriptionID
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Provider does not support cancellation"})
		return
	}

	payloadBytes, _ := json.Marshal(payload)
	if err := h.q.UpdatePaymentPayloadByID(c.Request.Context(), gensqlc.UpdatePaymentPayloadByIDParams{
		ProviderPayload: payloadBytes,
		ID:              row.ID,
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to cancel subscription"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ok":                   true,
		"subscription_id":      subscriptionID,
		"cancel_at_period_end": updated["cancel_at_period_end"] == true || updated[paymentdodo.PayloadCancelAtPeriodEnd] == true,
		"current_period_end":   firstNonEmpty(parseEpochOrNil(updated["current_period_end"]), parseEpochOrNil(updated[paymentdodo.PayloadCurrentPeriodEnd])),
	})
}

func validateProviderMethod(provider, paymentMethod string) error {
	if provider == "stripe" && paymentMethod != "card" {
		return errors.New("Stripe supports card only")
	}
	if provider == "card" && paymentMethod != "card" {
		return errors.New("Card provider requires payment_method=card")
	}
	if provider == "dodo" && paymentMethod != "card" {
		return errors.New("Dodo supports card only")
	}
	if provider == "crypto" && paymentMethod != "crypto" {
		return errors.New("Crypto provider requires payment_method=crypto")
	}
	return nil
}

func resolveCardProvider(provider string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(provider))
	if normalized == "stripe" || normalized == "dodo" {
		return normalized, nil
	}
	if normalized != "card" {
		return "", errors.New("Unsupported card provider")
	}
	dodoEnabled, _ := paymentdodo.IsConfigured()
	if dodoEnabled {
		return "dodo", nil
	}
	if strings.TrimSpace(os.Getenv("STRIPE_SECRET_KEY")) != "" {
		return "stripe", nil
	}
	return "", errors.New("No card provider is configured")
}

func (h *Handler) createCryptoInvoice(planName string, priceUSD int, payCurrency, successURL, cancelURL string, paymentID int32, user *auth.User) (string, string, map[string]any, error) {
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

func mergeProviderPayload(existing []byte, latest map[string]any) (map[string]any, error) {
	merged := map[string]any{}
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, &merged); err != nil {
			return nil, err
		}
	}
	for key, value := range latest {
		merged[key] = value
	}
	return merged, nil
}

func extractProviderPayloadStatus(existing []byte) string {
	if len(existing) == 0 {
		return "pending"
	}
	payload := map[string]any{}
	if err := json.Unmarshal(existing, &payload); err != nil {
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

func lookupNowPaymentsRefs(payload map[string]any) (int32, string) {
	var paymentID int32
	if rawOrderID, ok := payload["order_id"]; ok {
		switch value := rawOrderID.(type) {
		case string:
			fmt.Sscanf(value, "%d", &paymentID)
		case float64:
			paymentID = int32(value)
		}
	}
	providerID := ""
	if rawProviderID, ok := payload["payment_id"].(string); ok {
		providerID = rawProviderID
	}
	return paymentID, providerID
}

func (h *Handler) lookupPaymentForWebhook(c *gin.Context, paymentID int32, providerCheckoutID string) (int32, int32, int32, int, error) {
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

func emptyStringToNil(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func timestamptzStringOrNil(value pgtype.Timestamptz) any {
	if !value.Valid {
		return nil
	}
	return value.Time.UTC().Format(time.RFC3339Nano)
}

func pgTimestamptz(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: true}
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

func parseEpochOrNil(value any) any {
	switch raw := value.(type) {
	case float64:
		return time.Unix(int64(raw), 0).UTC().Format(time.RFC3339)
	case int64:
		return time.Unix(raw, 0).UTC().Format(time.RFC3339)
	case int:
		return time.Unix(int64(raw), 0).UTC().Format(time.RFC3339)
	case json.Number:
		parsed, err := raw.Int64()
		if err != nil {
			return nil
		}
		return time.Unix(parsed, 0).UTC().Format(time.RFC3339)
	default:
		return nil
	}
}

func parseDateTimeValue(value any) (time.Time, bool) {
	switch raw := value.(type) {
	case string:
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return time.Time{}, false
		}
		if asInt, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return time.Unix(asInt, 0).UTC(), true
		}
		if parsed, err := parseUTCDateTime(trimmed); err == nil {
			return parsed, true
		}
	case float64:
		return time.Unix(int64(raw), 0).UTC(), true
	case int64:
		return time.Unix(raw, 0).UTC(), true
	case int:
		return time.Unix(int64(raw), 0).UTC(), true
	case json.Number:
		asInt, err := raw.Int64()
		if err == nil {
			return time.Unix(asInt, 0).UTC(), true
		}
	}
	return time.Time{}, false
}

func (h *Handler) syncUserSubscriptionFromDodoStatus(c *gin.Context, userID, planID int32, subscriptionStatus string, periodEndValue any) error {
	statusValue := strings.ToLower(strings.TrimSpace(subscriptionStatus))
	switch statusValue {
	case "cancelled", "canceled", "expired", "failed", "on_hold":
		return h.q.DeactivateActiveSubscriptionsByUser(c.Request.Context(), userID)
	case "active", "trialing":
		periodEnd, ok := parseDateTimeValue(periodEndValue)
		if !ok || !periodEnd.After(time.Now().UTC()) {
			return nil
		}
		now := time.Now().UTC()
		tx, err := h.db.PGX.Begin(c.Request.Context())
		if err != nil {
			return err
		}
		defer tx.Rollback(c.Request.Context())
		qtx := h.q.WithTx(tx)
		if err := qtx.DeactivateActiveSubscriptionsByUser(c.Request.Context(), userID); err != nil {
			return err
		}
		if err := qtx.CreateUserSubscriptionActive(c.Request.Context(), gensqlc.CreateUserSubscriptionActiveParams{
			UserID:        userID,
			PricingPlanID: planID,
			StartsAt:      pgTimestamptz(now),
			EndsAt:        pgTimestamptz(periodEnd),
			CreatedAt:     pgTimestamptz(now),
		}); err != nil {
			return err
		}
		return tx.Commit(c.Request.Context())
	default:
		return nil
	}
}

func (h *Handler) lookupDodoWebhookPayment(c *gin.Context, paymentID int32, eventType string, obj map[string]any) (*gensqlc.GetPaymentForWebhookByPaymentIDRow, error) {
	if paymentID != 0 {
		return h.q.GetPaymentForWebhookByPaymentID(c.Request.Context(), paymentID)
	}
	subscriptionID := strings.TrimSpace(fmt.Sprintf("%v", obj["subscription_id"]))
	if subscriptionID == "" && strings.HasPrefix(eventType, "subscription.") {
		subscriptionID = strings.TrimSpace(fmt.Sprintf("%v", obj["id"]))
	}
	if subscriptionID == "" {
		return nil, sql.ErrNoRows
	}
	rows, err := h.q.ListRecentDodoCardPayments(c.Request.Context(), 2000)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if paymentdodo.ExtractSubscriptionID(row.ProviderPayload) == subscriptionID {
			return &gensqlc.GetPaymentForWebhookByPaymentIDRow{
				ID:            row.ID,
				UserID:        row.UserID,
				PricingPlanID: row.PricingPlanID,
				DurationDays:  row.DurationDays,
			}, nil
		}
	}
	return nil, sql.ErrNoRows
}
