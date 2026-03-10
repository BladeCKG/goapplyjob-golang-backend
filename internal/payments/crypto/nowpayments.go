package crypto

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
)

type NowPaymentsGateway struct {
	cfg config.Config
}

func NewNowPaymentsGateway(cfg config.Config) *NowPaymentsGateway {
	return &NowPaymentsGateway{cfg: cfg}
}

func (g *NowPaymentsGateway) CreateInvoice(request InvoiceRequest) (InvoiceResult, error) {
	payCode := strings.ToLower(strings.TrimSpace(request.PayCurrency))
	if payCode == "" {
		payCode = strings.ToLower(strings.TrimSpace(g.cfg.NowPaymentsDefaultPayCurrency))
		if payCode == "" {
			payCode = "usdttrc20"
		}
	}
	if strings.TrimSpace(g.cfg.NowPaymentsAPIKey) == "" {
		return InvoiceResult{
			ProviderInvoiceID: "nowpayments_local_" + request.OrderID,
			InvoiceURL:        request.SuccessURL + "?payment_id=" + request.OrderID,
			ProviderPayload: map[string]any{
				"mode":                     "local_stub",
				"provider":                 "nowpayments",
				"pay_currency":             payCode,
				"pay_amount":               request.AmountUSD,
				"invoice_url":              request.SuccessURL + "?payment_id=" + request.OrderID,
				"expiration_estimate_date": time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339Nano),
			},
		}, nil
	}

	endpoint := strings.TrimRight(strings.TrimSpace(g.cfg.NowPaymentsAPIBaseURL), "/") + "/invoice"
	payload := map[string]any{
		"price_amount":      request.AmountUSD,
		"price_currency":    "usd",
		"order_id":          request.OrderID,
		"order_description": request.Description,
		"success_url":       request.SuccessURL,
		"cancel_url":        request.CancelURL,
		"is_fixed_rate":     true,
	}
	if payCode != "" {
		payload["pay_currency"] = payCode
	}
	if strings.TrimSpace(request.CallbackURL) != "" {
		payload["ipn_callback_url"] = request.CallbackURL
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return InvoiceResult{}, err
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return InvoiceResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-api-key", g.cfg.NowPaymentsAPIKey)

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return InvoiceResult{}, fmt.Errorf("NOWPayments request failed: %T", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return InvoiceResult{}, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return InvoiceResult{}, fmt.Errorf("NOWPayments invoice creation failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	payloadResp := map[string]any{}
	if err := json.Unmarshal(raw, &payloadResp); err != nil {
		payloadResp = map[string]any{"raw": strings.TrimSpace(string(raw))}
	}
	providerID := strings.TrimSpace(fmt.Sprintf("%v", firstAny(
		payloadResp["id"],
		payloadResp["invoice_id"],
		payloadResp["payment_id"],
		"nowpayments_invoice_"+request.OrderID,
	)))
	invoiceURL := strings.TrimSpace(fmt.Sprintf("%v", firstAny(payloadResp["invoice_url"], payloadResp["checkout_url"], "")))
	if invoiceURL == "" {
		invoiceURL = request.SuccessURL
	}

	return InvoiceResult{
		ProviderInvoiceID: providerID,
		InvoiceURL:        invoiceURL,
		ProviderPayload:   payloadResp,
	}, nil
}

func (g *NowPaymentsGateway) ListCurrencies(amountUSD *float64) []CurrencyOption {
	if strings.TrimSpace(g.cfg.NowPaymentsAPIKey) != "" {
		if codes := g.fetchCurrencies(); len(codes) > 0 {
			out := make([]CurrencyOption, 0, len(codes))
			for _, code := range codes {
				out = append(out, CurrencyOption{Code: code, MinUSD: nil})
			}
			return out
		}
	}
	values := []string{}
	raw := strings.TrimSpace(g.cfg.NowPaymentsCurrencyCandidates)
	if raw == "" {
		raw = "btc,eth,ltc,usdttrc20,usdterc20,usdtbsc,usdc"
	}
	for _, item := range strings.Split(raw, ",") {
		code := strings.ToLower(strings.TrimSpace(item))
		if code != "" && !contains(values, code) {
			values = append(values, code)
		}
	}
	if len(values) == 0 {
		values = []string{"btc", "eth", "ltc", "usdttrc20"}
	}
	out := make([]CurrencyOption, 0, len(values))
	for _, code := range values {
		out = append(out, CurrencyOption{Code: code, MinUSD: nil})
	}
	return out
}

func (g *NowPaymentsGateway) VerifyWebhookSignature(payload map[string]any, headers map[string]string, rawBody []byte) error {
	secret := strings.TrimSpace(g.cfg.NowPaymentsIPNSecret)
	if secret == "" {
		return nil
	}
	signature := strings.TrimSpace(getHeader(headers, "x-nowpayments-sig"))
	if signature == "" {
		return fmt.Errorf("Missing NOWPayments signature")
	}
	message := rawBody
	if len(message) == 0 {
		message = canonicalJSON(payload)
	}
	mac := hmac.New(sha512.New, []byte(secret))
	_, _ = mac.Write(message)
	expected := fmt.Sprintf("%x", mac.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(expected)), []byte(strings.ToLower(signature))) {
		return fmt.Errorf("Invalid NOWPayments signature")
	}
	return nil
}

func (g *NowPaymentsGateway) ParseWebhook(payload map[string]any) WebhookParseResult {
	status := PaymentPending
	switch strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", firstAny(payload["payment_status"], payload["status"], "pending")))) {
	case "confirmed", "finished", "sending":
		status = PaymentPaid
	case "failed", "expired", "refunded":
		status = PaymentFailed
	}
	return WebhookParseResult{
		OrderID:           fmt.Sprintf("%v", firstAny(payload["order_id"], "")),
		ProviderPaymentID: fmt.Sprintf("%v", firstAny(payload["payment_id"], "")),
		Status:            status,
	}
}

func (g *NowPaymentsGateway) VerifyPayment(providerPaymentID string, orderID string) (*VerificationResult, error) {
	if strings.TrimSpace(g.cfg.NowPaymentsAPIKey) == "" || strings.TrimSpace(providerPaymentID) == "" {
		return nil, nil
	}
	endpoint := strings.TrimRight(strings.TrimSpace(g.cfg.NowPaymentsAPIBaseURL), "/") + "/payment/" + providerPaymentID
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-api-key", g.cfg.NowPaymentsAPIKey)
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil
	}
	payload := map[string]any{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, nil
	}
	return &VerificationResult{
		Status:          PaymentStatus(ParseGenericPaymentStatus(firstAny(payload["payment_status"], payload["status"], "pending"))),
		ProviderPayload: payload,
	}, nil
}

func (g *NowPaymentsGateway) fetchCurrencies() []string {
	endpoint := strings.TrimRight(strings.TrimSpace(g.cfg.NowPaymentsAPIBaseURL), "/") + "/currencies"
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-api-key", g.cfg.NowPaymentsAPIKey)

	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return nil
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	rawItems, ok := payload["currencies"].([]any)
	if !ok {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, item := range rawItems {
		code := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", item)))
		if code == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

func canonicalJSON(payload map[string]any) []byte {
	if payload == nil {
		return []byte("{}")
	}
	encoded := buildCanonicalJSON(payload)
	if encoded == "" {
		return []byte("{}")
	}
	return []byte(encoded)
}

func buildCanonicalJSON(value any) string {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		builder := strings.Builder{}
		builder.WriteString("{")
		for idx, key := range keys {
			if idx > 0 {
				builder.WriteString(",")
			}
			keyBytes, _ := json.Marshal(key)
			builder.Write(keyBytes)
			builder.WriteString(":")
			builder.WriteString(buildCanonicalJSON(typed[key]))
		}
		builder.WriteString("}")
		return builder.String()
	case []any:
		builder := strings.Builder{}
		builder.WriteString("[")
		for idx, item := range typed {
			if idx > 0 {
				builder.WriteString(",")
			}
			builder.WriteString(buildCanonicalJSON(item))
		}
		builder.WriteString("]")
		return builder.String()
	default:
		bytes, _ := json.Marshal(typed)
		return string(bytes)
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
