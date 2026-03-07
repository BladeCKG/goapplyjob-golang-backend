package crypto

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/json"
	"fmt"
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
		payCode = "usdttrc20"
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
	return InvoiceResult{
		ProviderInvoiceID: "nowpayments_invoice_" + request.OrderID,
		InvoiceURL:        request.SuccessURL,
		ProviderPayload: map[string]any{
			"id":                "nowpayments_invoice_" + request.OrderID,
			"invoice_id":        "nowpayments_invoice_" + request.OrderID,
			"invoice_url":       request.SuccessURL,
			"order_description": request.Description,
			"price_amount":      request.AmountUSD,
		},
	}, nil
}

func (g *NowPaymentsGateway) ListCurrencies(amountUSD *float64) []CurrencyOption {
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

func (g *NowPaymentsGateway) VerifyWebhookSignature(payload map[string]any, headers map[string]string) error {
	secret := strings.TrimSpace(g.cfg.NowPaymentsIPNSecret)
	if secret == "" {
		return nil
	}
	signature := strings.TrimSpace(getHeader(headers, "x-nowpayments-sig"))
	if signature == "" {
		return fmt.Errorf("Missing NOWPayments signature")
	}
	message, _ := json.Marshal(payload)
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

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
