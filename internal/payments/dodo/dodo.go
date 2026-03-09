package dodo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	dodopayments "github.com/dodopayments/dodopayments-go"
	"github.com/dodopayments/dodopayments-go/option"
)

const (
	Provider = "dodo"

	PayloadSubscriptionID     = "dodo_subscription_id"
	PayloadSubscriptionStatus = "dodo_subscription_status"
	PayloadCancelAtPeriodEnd  = "dodo_cancel_at_period_end"
	PayloadCurrentPeriodEnd   = "dodo_current_period_end"
	PayloadCanceledAt         = "dodo_canceled_at"
	PayloadPaymentID          = "dodo_payment_id"
	PayloadPaymentStatus      = "dodo_payment_status"
	PayloadCheckoutStatus     = "dodo_checkout_status"
)

const (
	envDodoAPIKey      = "DODO_API_KEY"
	envDodoEnvironment = "DODO_ENVIRONMENT"
	envDodoWebhookKey  = "DODO_WEBHOOK_KEY"
	envDodoAPIBaseURL  = "DODO_API_BASE_URL"
)

func ProductIDForPlan(planCode string) string {
	switch strings.ToLower(strings.TrimSpace(planCode)) {
	case "weekly":
		return strings.TrimSpace(os.Getenv("DODO_PRODUCT_ID_WEEKLY"))
	case "monthly":
		return strings.TrimSpace(os.Getenv("DODO_PRODUCT_ID_MONTHLY"))
	case "quarterly":
		return strings.TrimSpace(os.Getenv("DODO_PRODUCT_ID_QUARTERLY"))
	case "yearly":
		return strings.TrimSpace(os.Getenv("DODO_PRODUCT_ID_YEARLY"))
	default:
		return ""
	}
}

func IsConfigured() (bool, string) {
	apiKey := strings.TrimSpace(os.Getenv(envDodoAPIKey))
	if apiKey == "" {
		return false, "Dodo API key is not configured"
	}
	missing := []string{}
	for _, code := range []string{"weekly", "monthly", "quarterly", "yearly"} {
		if ProductIDForPlan(code) == "" {
			missing = append(missing, code)
		}
	}
	if len(missing) > 0 {
		return false, "Dodo product IDs missing for: " + strings.Join(missing, ", ")
	}
	return true, ""
}

func CreateCheckout(paymentID int32, userID int32, planCode string, userEmail string, successURL string) (string, string, map[string]any, error) {
	productID := ProductIDForPlan(planCode)
	if productID == "" {
		return "", "", nil, fmt.Errorf("Dodo product ID not configured for '%s'", strings.TrimSpace(planCode))
	}
	client, err := newClient()
	if err != nil {
		return "", "", nil, err
	}
	session, err := client.CheckoutSessions.New(context.Background(), dodopayments.CheckoutSessionNewParams{
		CheckoutSessionRequest: dodopayments.CheckoutSessionRequestParam{
			ProductCart: dodopayments.F([]dodopayments.ProductItemReqParam{{
				ProductID: dodopayments.F(productID),
				Quantity:  dodopayments.F(int64(1)),
			}}),
			Customer: dodopayments.F[dodopayments.CustomerRequestUnionParam](dodopayments.CustomerRequestParam{
				Email: dodopayments.F(userEmail),
			}),
			ReturnURL: dodopayments.F(successURL),
			Metadata: dodopayments.F(map[string]string{
				"payment_id": fmt.Sprintf("%d", paymentID),
				"user_id":    fmt.Sprintf("%d", userID),
				"plan_code":  strings.ToLower(strings.TrimSpace(planCode)),
			}),
		},
	})
	if err != nil {
		return "", "", nil, err
	}
	sessionID := strings.TrimSpace(session.SessionID)
	checkoutURL := strings.TrimSpace(session.CheckoutURL)
	if sessionID == "" || checkoutURL == "" {
		return "", "", nil, fmt.Errorf("Dodo checkout response missing session_id or checkout_url")
	}
	return sessionID, checkoutURL, map[string]any{
		"session_id":   sessionID,
		"checkout_url": checkoutURL,
	}, nil
}

func ResolveCheckoutStatus(providerCheckoutID string, providerPayload []byte) (string, map[string]any) {
	payload := map[string]any{}
	_ = json.Unmarshal(providerPayload, &payload)
	if strings.TrimSpace(providerCheckoutID) == "" {
		return "pending", payload
	}
	client, err := newClient()
	if err != nil {
		return "pending", payload
	}
	checkout, err := client.CheckoutSessions.Get(context.Background(), strings.TrimSpace(providerCheckoutID))
	if err != nil {
		return "pending", payload
	}
	payload["id"] = checkout.ID
	payload["payment_id"] = checkout.PaymentID
	payload["payment_status"] = string(checkout.PaymentStatus)
	checkoutStatus := strings.ToLower(strings.TrimSpace(string(checkout.PaymentStatus)))
	if checkoutStatus != "" {
		payload[PayloadCheckoutStatus] = checkoutStatus
	}
	paymentStatusCandidate := checkoutStatus
	paymentID := strings.TrimSpace(checkout.PaymentID)
	if paymentID != "" {
		payload[PayloadPaymentID] = paymentID
		payment, paymentErr := client.Payments.Get(context.Background(), paymentID)
		if paymentErr == nil {
			paymentPayload := map[string]any{
				"payment_id":       payment.PaymentID,
				"status":           string(payment.Status),
				"subscription_id":  payment.SubscriptionID,
				"checkout_session": payment.CheckoutSessionID,
			}
			payload["payment"] = paymentPayload
			if paymentStatus := strings.ToLower(strings.TrimSpace(string(payment.Status))); paymentStatus != "" {
				paymentStatusCandidate = paymentStatus
			}
			if subscriptionID := strings.TrimSpace(payment.SubscriptionID); subscriptionID != "" {
				payload[PayloadSubscriptionID] = subscriptionID
			}
		}
	}
	payload[PayloadPaymentStatus] = paymentStatusCandidate
	return mapStatus(paymentStatusCandidate), payload
}

func ExtractSubscriptionID(providerPayload []byte) string {
	payload := map[string]any{}
	if err := json.Unmarshal(providerPayload, &payload); err != nil {
		return ""
	}
	if raw, ok := payload[PayloadSubscriptionID].(string); ok {
		return strings.TrimSpace(raw)
	}
	if nested, ok := payload["payment"].(map[string]any); ok {
		if raw, ok := nested["subscription_id"].(string); ok {
			return strings.TrimSpace(raw)
		}
	}
	return ""
}

func RefreshSubscriptionState(subscriptionID string) map[string]any {
	if strings.TrimSpace(subscriptionID) == "" {
		return nil
	}
	client, err := newClient()
	if err != nil {
		return nil
	}
	sub, err := client.Subscriptions.Get(context.Background(), strings.TrimSpace(subscriptionID))
	if err != nil {
		return nil
	}
	var currentPeriodEnd any
	if !sub.NextBillingDate.IsZero() {
		currentPeriodEnd = sub.NextBillingDate.UTC().Format(timeRFC3339)
	} else if !sub.ExpiresAt.IsZero() {
		currentPeriodEnd = sub.ExpiresAt.UTC().Format(timeRFC3339)
	}
	var canceledAt any
	if !sub.CancelledAt.IsZero() {
		canceledAt = sub.CancelledAt.UTC().Format(timeRFC3339)
	}
	return map[string]any{
		PayloadSubscriptionID:     strings.TrimSpace(subscriptionID),
		PayloadSubscriptionStatus: emptyToNil(strings.ToLower(strings.TrimSpace(string(sub.Status)))),
		PayloadCancelAtPeriodEnd:  sub.CancelAtNextBillingDate,
		PayloadCurrentPeriodEnd:   currentPeriodEnd,
		PayloadCanceledAt:         canceledAt,
	}
}

func CancelSubscription(subscriptionID string) (map[string]any, error) {
	if strings.TrimSpace(subscriptionID) == "" {
		return nil, fmt.Errorf("Dodo subscription id not found")
	}
	client, err := newClient()
	if err != nil {
		return nil, err
	}
	sub, err := client.Subscriptions.Update(context.Background(), strings.TrimSpace(subscriptionID), dodopayments.SubscriptionUpdateParams{
		CancelAtNextBillingDate: dodopayments.F(true),
	})
	if err != nil {
		return nil, err
	}
	var currentPeriodEnd any
	if !sub.NextBillingDate.IsZero() {
		currentPeriodEnd = sub.NextBillingDate.UTC().Format(timeRFC3339)
	} else if !sub.ExpiresAt.IsZero() {
		currentPeriodEnd = sub.ExpiresAt.UTC().Format(timeRFC3339)
	}
	var canceledAt any
	if !sub.CancelledAt.IsZero() {
		canceledAt = sub.CancelledAt.UTC().Format(timeRFC3339)
	}
	return map[string]any{
		PayloadSubscriptionID:     strings.TrimSpace(subscriptionID),
		PayloadSubscriptionStatus: emptyToNil(strings.ToLower(strings.TrimSpace(string(sub.Status)))),
		PayloadCancelAtPeriodEnd:  sub.CancelAtNextBillingDate,
		PayloadCurrentPeriodEnd:   currentPeriodEnd,
		PayloadCanceledAt:         canceledAt,
	}, nil
}

func ParseWebhook(rawPayload []byte, headers http.Header) (eventType string, metadata map[string]any, obj map[string]any, err error) {
	client, err := newClient()
	if err != nil {
		return "", nil, nil, err
	}
	legacyEventType, legacyMetadata, legacyObj, hasLegacyShape := parseLegacyWebhook(rawPayload)
	webhookKey := strings.TrimSpace(os.Getenv(envDodoWebhookKey))
	if webhookKey != "" {
		event, unwrapErr := client.Webhooks.Unwrap(rawPayload, headers)
		if unwrapErr == nil {
			eventType, metadata, obj = parseUnwrappedEvent(string(event.Type), event.Data)
			if hasLegacyShape && shouldUseLegacyWebhook(eventType, metadata, obj) {
				return legacyEventType, legacyMetadata, legacyObj, nil
			}
			return eventType, metadata, obj, nil
		}
	}
	event, unsafeErr := client.Webhooks.UnsafeUnwrap(rawPayload)
	if unsafeErr == nil {
		eventType, metadata, obj = parseUnwrappedEvent(string(event.Type), event.Data)
		if hasLegacyShape && shouldUseLegacyWebhook(eventType, metadata, obj) {
			return legacyEventType, legacyMetadata, legacyObj, nil
		}
		return eventType, metadata, obj, nil
	}
	if hasLegacyShape {
		return legacyEventType, legacyMetadata, legacyObj, nil
	}
	return "", nil, nil, fmt.Errorf("invalid Dodo webhook payload")
}

func mapStatus(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "succeeded", "paid", "completed", "active", "trialing":
		return "paid"
	case "failed", "cancelled", "canceled", "expired":
		return "failed"
	default:
		return "pending"
	}
}

func first(values ...any) any {
	for _, value := range values {
		if text, ok := value.(string); ok {
			if strings.TrimSpace(text) != "" {
				return text
			}
			continue
		}
		if value != nil {
			return value
		}
	}
	return ""
}

func emptyToNil(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func newClient() (*dodopayments.Client, error) {
	apiKey := strings.TrimSpace(os.Getenv(envDodoAPIKey))
	if apiKey == "" {
		return nil, fmt.Errorf("Dodo API key is not configured")
	}
	opts := []option.RequestOption{option.WithBearerToken(apiKey)}
	if baseURL := strings.TrimSpace(os.Getenv(envDodoAPIBaseURL)); baseURL != "" {
		opts = append(opts, option.WithBaseURL(baseURL))
	} else {
		switch strings.ToLower(strings.TrimSpace(os.Getenv(envDodoEnvironment))) {
		case "test_mode":
			opts = append(opts, option.WithEnvironmentTestMode())
		default:
			opts = append(opts, option.WithEnvironmentLiveMode())
		}
	}
	if webhookKey := strings.TrimSpace(os.Getenv(envDodoWebhookKey)); webhookKey != "" {
		opts = append(opts, option.WithWebhookKey(webhookKey))
	}
	client := dodopayments.NewClient(opts...)
	return client, nil
}

func parseUnwrappedEvent(eventType string, data any) (string, map[string]any, map[string]any) {
	metadata := map[string]any{}
	obj := map[string]any{}
	switch typed := data.(type) {
	case dodopayments.Payment:
		obj["status"] = strings.ToLower(strings.TrimSpace(string(typed.Status)))
		obj["payment_id"] = typed.PaymentID
		obj["subscription_id"] = typed.SubscriptionID
		for key, value := range typed.Metadata {
			metadata[key] = value
		}
	case dodopayments.Subscription:
		status := strings.ToLower(strings.TrimSpace(string(typed.Status)))
		obj["status"] = status
		obj["subscription_id"] = typed.SubscriptionID
		obj["id"] = typed.SubscriptionID
		obj[PayloadSubscriptionStatus] = status
		obj[PayloadCancelAtPeriodEnd] = typed.CancelAtNextBillingDate
		if !typed.NextBillingDate.IsZero() {
			obj[PayloadCurrentPeriodEnd] = typed.NextBillingDate.UTC().Format(timeRFC3339)
		} else if !typed.ExpiresAt.IsZero() {
			obj[PayloadCurrentPeriodEnd] = typed.ExpiresAt.UTC().Format(timeRFC3339)
		}
		for key, value := range typed.Metadata {
			metadata[key] = value
		}
	default:
		raw, err := json.Marshal(data)
		if err == nil {
			_ = json.Unmarshal(raw, &obj)
			if parsedMetadata, ok := obj["metadata"].(map[string]any); ok {
				for key, value := range parsedMetadata {
					metadata[key] = value
				}
			}
		}
	}
	return strings.ToLower(strings.TrimSpace(eventType)), metadata, obj
}

const timeRFC3339 = "2006-01-02T15:04:05Z07:00"

func parseLegacyWebhook(rawPayload []byte) (string, map[string]any, map[string]any, bool) {
	payload := map[string]any{}
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		return "", nil, nil, false
	}
	eventType := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", first(payload["type"], payload["event_type"], ""))))
	data, _ := payload["data"].(map[string]any)
	obj, _ := data["object"].(map[string]any)
	if obj == nil {
		obj = data
	}
	if eventType == "" || len(obj) == 0 {
		return "", nil, nil, false
	}
	metadata := map[string]any{}
	for _, candidate := range []any{payload["metadata"], data["metadata"], obj["metadata"]} {
		if current, ok := candidate.(map[string]any); ok {
			for key, value := range current {
				metadata[key] = value
			}
		}
	}
	return eventType, metadata, obj, true
}

func shouldUseLegacyWebhook(eventType string, metadata map[string]any, obj map[string]any) bool {
	if len(obj) == 0 || strings.TrimSpace(eventType) == "" {
		return true
	}
	if strings.HasPrefix(eventType, "payment.") {
		status := strings.TrimSpace(fmt.Sprintf("%v", obj["status"]))
		paymentID := strings.TrimSpace(fmt.Sprintf("%v", metadata["payment_id"]))
		return status == "" || paymentID == ""
	}
	if strings.HasPrefix(eventType, "subscription.") {
		status := strings.TrimSpace(fmt.Sprintf("%v", obj["status"]))
		subscriptionID := strings.TrimSpace(fmt.Sprintf("%v", first(obj["subscription_id"], obj["id"])))
		return status == "" || subscriptionID == ""
	}
	return false
}
