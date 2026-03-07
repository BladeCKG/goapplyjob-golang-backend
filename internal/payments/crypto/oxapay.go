package crypto

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"strings"

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
	return InvoiceResult{
		ProviderInvoiceID: "oxapay_invoice_" + request.OrderID,
		InvoiceURL:        request.SuccessURL,
		ProviderPayload: map[string]any{
			"track_id":    "oxapay_invoice_" + request.OrderID,
			"invoice_url": request.SuccessURL,
			"provider":    "oxapay",
		},
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

func asString(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}
