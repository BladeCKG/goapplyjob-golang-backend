package crypto

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
)

type OxaPayGateway struct {
	cfg config.Config
}

func NewOxaPayGateway(cfg config.Config) *OxaPayGateway {
	return &OxaPayGateway{cfg: cfg}
}

func (g *OxaPayGateway) CreateInvoice(request InvoiceRequest) (InvoiceResult, error) {
	if strings.TrimSpace(g.cfg.OxaPayMerchantAPIKey) == "" {
		return InvoiceResult{
			ProviderInvoiceID: "oxapay_local_" + request.OrderID,
			InvoiceURL:        request.SuccessURL + "?payment_id=" + request.OrderID,
			ProviderPayload: map[string]any{
				"mode":     "local_stub",
				"provider": "oxapay",
			},
		}, nil
	}

	endpoint := strings.TrimRight(strings.TrimSpace(g.cfg.OxaPayAPIBaseURL), "/") + "/payment/invoice"
	payload := map[string]any{
		"amount":              request.AmountUSD,
		"currency":            "USD",
		"lifetime":            60,
		"fee_paid_by_payer":   1,
		"under_paid_coverage": 2.5,
		"auto_withdrawal":     false,
		"mixed_payment":       true,
		"order_id":            request.OrderID,
		"description":         request.Description,
		"return_url":          request.SuccessURL,
		"callback_url":        request.CallbackURL,
		"email":               request.CustomerEmail,
	}
	if payCurrency := strings.TrimSpace(request.PayCurrency); payCurrency != "" {
		payload["to_currency"] = strings.ToUpper(payCurrency)
	}
	switch strings.ToLower(strings.TrimSpace(g.cfg.OxaPayEnv)) {
	case "sandbox":
		payload["sandbox"] = true
	case "prod":
		payload["sandbox"] = false
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return InvoiceResult{}, err
	}

	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return InvoiceResult{}, err
	}
	req.Header.Set("merchant_api_key", g.cfg.OxaPayMerchantAPIKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return InvoiceResult{}, fmt.Errorf("OxaPay request failed: %T", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return InvoiceResult{}, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return InvoiceResult{}, fmt.Errorf("OxaPay invoice creation failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	responsePayload := map[string]any{}
	if err := json.Unmarshal(raw, &responsePayload); err != nil {
		responsePayload = map[string]any{"raw": strings.TrimSpace(string(raw))}
	}
	providerID := strings.TrimSpace(fmt.Sprintf("%v", firstAny(
		nestedValue(responsePayload, "data", "track_id"),
		nestedValue(responsePayload, "data", "trackId"),
		responsePayload["track_id"],
		responsePayload["trackId"],
		responsePayload["id"],
		responsePayload["payment_id"],
		"oxapay_invoice_"+request.OrderID,
	)))
	invoiceURL := extractOxaPayInvoiceURL(responsePayload)
	if invoiceURL == "" {
		invoiceURL = request.SuccessURL
	}

	return InvoiceResult{
		ProviderInvoiceID: providerID,
		InvoiceURL:        invoiceURL,
		ProviderPayload:   responsePayload,
	}, nil
}

func (g *OxaPayGateway) ListCurrencies(amountUSD *float64) []CurrencyOption {
	return []CurrencyOption{{Code: "USD", MinUSD: nil}}
}

func (g *OxaPayGateway) VerifyWebhookSignature(payload map[string]any, headers map[string]string, rawBody []byte) error {
	if strings.TrimSpace(g.cfg.OxaPayMerchantAPIKey) == "" {
		return nil
	}
	signature := strings.TrimSpace(getHeader(headers, "hmac"))
	if signature == "" {
		return fmt.Errorf("Missing OxaPay webhook HMAC")
	}
	message := rawBody
	if len(message) == 0 {
		message, _ = json.Marshal(payload)
	}
	mac := hmac.New(sha512.New, []byte(g.cfg.OxaPayMerchantAPIKey))
	_, _ = mac.Write(message)
	expected := fmt.Sprintf("%x", mac.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(expected)), []byte(strings.ToLower(signature))) {
		return fmt.Errorf("Invalid OxaPay webhook HMAC")
	}
	return nil
}

func (g *OxaPayGateway) ParseWebhook(payload map[string]any) WebhookParseResult {
	eventType := strings.ToLower(strings.TrimSpace(asString(payload["type"])))
	if eventType != "" && eventType != "invoice" {
		return WebhookParseResult{Status: PaymentPending}
	}
	status := PaymentPending
	switch strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", firstAny(payload["status"], payload["payment_status"], payload["statusText"], "")))) {
	case "paid", "success", "completed", "confirmed", "paid_success":
		status = PaymentPaid
	case "failed", "cancelled", "canceled", "expired", "error":
		status = PaymentFailed
	}
	return WebhookParseResult{
		OrderID:           fmt.Sprintf("%v", firstAny(payload["order_id"], payload["orderId"], payload["orderID"], "")),
		ProviderPaymentID: fmt.Sprintf("%v", firstAny(payload["trackId"], payload["track_id"], payload["payment_id"], "")),
		Status:            status,
	}
}

func (g *OxaPayGateway) VerifyPayment(providerPaymentID string, orderID string) (*VerificationResult, error) {
	if strings.TrimSpace(g.cfg.OxaPayMerchantAPIKey) == "" || strings.TrimSpace(providerPaymentID) == "" {
		return nil, nil
	}
	_ = orderID
	endpoint := strings.TrimRight(strings.TrimSpace(g.cfg.OxaPayAPIBaseURL), "/") + "/payment/" + strings.TrimSpace(providerPaymentID)
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("merchant_api_key", g.cfg.OxaPayMerchantAPIKey)
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
	nestedData, _ := payload["data"].(map[string]any)
	statusValue := firstAny(
		nestedData["status"],
		nestedData["payment_status"],
		payload["status"],
		payload["payment_status"],
		"pending",
	)
	return &VerificationResult{
		Status:          PaymentStatus(ParseGenericPaymentStatus(statusValue)),
		ProviderPayload: payload,
	}, nil
}

func asString(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func extractOxaPayInvoiceURL(payload map[string]any) string {
	candidates := []any{
		payload["payLink"],
		payload["pay_link"],
		payload["payment_link"],
		payload["payment_url"],
		payload["link"],
		payload["invoice_url"],
		payload["url"],
	}
	for _, key := range []string{"result", "data"} {
		if nested, ok := payload[key].(map[string]any); ok {
			candidates = append(candidates,
				nested["payLink"],
				nested["pay_link"],
				nested["payment_link"],
				nested["payment_url"],
				nested["link"],
				nested["invoice_url"],
				nested["url"],
			)
		}
	}
	for _, candidate := range candidates {
		if value := strings.TrimSpace(fmt.Sprintf("%v", candidate)); value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func nestedValue(payload map[string]any, parentKey, childKey string) any {
	if payload == nil {
		return nil
	}
	nested, ok := payload[parentKey].(map[string]any)
	if !ok {
		return nil
	}
	return nested[childKey]
}
