package crypto

import (
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

func (g *OxaPayGateway) VerifyWebhookSignature(payload map[string]any, headers map[string]string) error {
	return nil
}

func (g *OxaPayGateway) ParseWebhook(payload map[string]any) WebhookParseResult {
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
