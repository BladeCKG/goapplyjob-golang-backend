package stripe

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	Provider = "stripe"
)

func CreateCheckoutSession(planName, billingCycle string, priceUSD int, successURL, cancelURL string, paymentID int64, userEmail string) (string, string, map[string]any, error) {
	stripeSecretKey := strings.TrimSpace(os.Getenv("STRIPE_SECRET_KEY"))
	if stripeSecretKey == "" {
		return "", "", nil, fmt.Errorf("Stripe is not configured")
	}
	interval, intervalCount := recurringConfig(billingCycle)

	form := url.Values{}
	form.Set("mode", "subscription")
	form.Set("success_url", successURL)
	form.Set("cancel_url", cancelURL)
	form.Set("customer_email", userEmail)
	form.Set("metadata[payment_id]", fmt.Sprintf("%d", paymentID))
	form.Set("line_items[0][price_data][currency]", "usd")
	form.Set("line_items[0][price_data][product_data][name]", planName)
	form.Set("line_items[0][price_data][unit_amount]", fmt.Sprintf("%d00", priceUSD))
	form.Set("line_items[0][price_data][recurring][interval]", interval)
	form.Set("line_items[0][price_data][recurring][interval_count]", fmt.Sprintf("%d", intervalCount))
	form.Set("line_items[0][quantity]", "1")

	endpoint := apiBase() + "/checkout/sessions"
	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", "", nil, err
	}
	req.Header.Set("Authorization", "Bearer "+stripeSecretKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return "", "", nil, fmt.Errorf("stripe status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	parsed := map[string]any{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", "", nil, err
	}
	id, _ := parsed["id"].(string)
	checkoutURL, _ := parsed["url"].(string)
	if strings.TrimSpace(id) == "" || strings.TrimSpace(checkoutURL) == "" {
		return "", "", nil, fmt.Errorf("stripe checkout response missing id or url")
	}
	return id, checkoutURL, parsed, nil
}

func ResolveCheckoutStatus(checkoutID string) (string, map[string]any) {
	checkoutID = strings.TrimSpace(checkoutID)
	if checkoutID == "" {
		return "pending", nil
	}
	stripeSecretKey := strings.TrimSpace(os.Getenv("STRIPE_SECRET_KEY"))
	if stripeSecretKey == "" {
		return "pending", nil
	}
	endpoint := apiBase() + "/checkout/sessions/" + url.PathEscape(checkoutID)
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return "pending", nil
	}
	req.Header.Set("Authorization", "Bearer "+stripeSecretKey)

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "pending", nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "pending", nil
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return "pending", nil
	}

	parsed := map[string]any{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "pending", nil
	}
	checkoutStatus := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", parsed["status"])))
	if checkoutStatus != "" {
		parsed["stripe_checkout_status"] = checkoutStatus
	}
	paymentStatus := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", parsed["payment_status"])))
	if paymentStatus != "" {
		parsed["stripe_payment_status"] = paymentStatus
	}
	if subID, ok := parsed["subscription"].(string); ok && strings.TrimSpace(subID) != "" {
		parsed["stripe_subscription_id"] = strings.TrimSpace(subID)
	}
	switch checkoutStatus {
	case "complete":
		return "paid", parsed
	case "expired":
		return "failed", parsed
	default:
		return "pending", parsed
	}
}

func CancelSubscriptionAtPeriodEnd(subscriptionID string) (map[string]any, error) {
	stripeSecretKey := strings.TrimSpace(os.Getenv("STRIPE_SECRET_KEY"))
	if stripeSecretKey == "" {
		return nil, fmt.Errorf("Stripe is not configured")
	}
	endpoint := apiBase() + "/subscriptions/" + url.PathEscape(strings.TrimSpace(subscriptionID))
	form := url.Values{}
	form.Set("cancel_at_period_end", "true")

	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+stripeSecretKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("stripe status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	parsed := map[string]any{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	return parsed, nil
}

func apiBase() string {
	apiBase := strings.TrimSpace(os.Getenv("STRIPE_API_BASE_URL"))
	if apiBase == "" {
		apiBase = "https://api.stripe.com/v1"
	}
	return strings.TrimRight(apiBase, "/")
}

func recurringConfig(billingCycle string) (string, int) {
	switch strings.ToLower(strings.TrimSpace(billingCycle)) {
	case "weekly":
		return "week", 1
	case "monthly":
		return "month", 1
	case "quarterly":
		return "month", 3
	case "yearly", "annual":
		return "year", 1
	default:
		return "month", 1
	}
}
