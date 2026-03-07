package crypto

import (
	"fmt"
	"strings"

	"goapplyjob-golang-backend/internal/config"
)

type MaxelPayGateway struct {
	cfg config.Config
}

func NewMaxelPayGateway(cfg config.Config) *MaxelPayGateway {
	return &MaxelPayGateway{cfg: cfg}
}

func (g *MaxelPayGateway) CreateInvoice(request InvoiceRequest) (InvoiceResult, error) {
	if strings.TrimSpace(g.cfg.MaxelPayAPIKey) == "" || strings.TrimSpace(g.cfg.MaxelPaySecretKey) == "" {
		return InvoiceResult{
			ProviderInvoiceID: "maxelpay_local_" + request.OrderID,
			InvoiceURL:        request.SuccessURL + "?payment_id=" + request.OrderID,
			ProviderPayload: map[string]any{
				"mode":     "local_stub",
				"provider": "maxelpay",
			},
		}, nil
	}
	if strings.TrimSpace(request.CustomerEmail) == "" {
		return InvoiceResult{}, fmt.Errorf("Maxelpay requires a customer email address")
	}
	return InvoiceResult{
		ProviderInvoiceID: "maxelpay_invoice_" + request.OrderID,
		InvoiceURL:        request.SuccessURL,
		ProviderPayload: map[string]any{
			"orderID":   request.OrderID,
			"provider":  "maxelpay",
			"userEmail": request.CustomerEmail,
			"userName":  firstAny(request.CustomerName, strings.Split(request.CustomerEmail, "@")[0]),
		},
	}, nil
}

func (g *MaxelPayGateway) ListCurrencies(amountUSD *float64) []CurrencyOption {
	return []CurrencyOption{{Code: "USD", MinUSD: nil}}
}

func (g *MaxelPayGateway) VerifyWebhookSignature(payload map[string]any, headers map[string]string) error {
	return nil
}

func (g *MaxelPayGateway) ParseWebhook(payload map[string]any) WebhookParseResult {
	status := PaymentPending
	switch strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", firstAny(payload["status"], payload["paymentStatus"], payload["orderStatus"], "")))) {
	case "paid", "success", "completed", "confirmed":
		status = PaymentPaid
	case "failed", "cancelled", "canceled", "expired", "error":
		status = PaymentFailed
	}
	return WebhookParseResult{
		OrderID:           fmt.Sprintf("%v", firstAny(payload["orderID"], payload["orderId"], payload["order_id"], "")),
		ProviderPaymentID: fmt.Sprintf("%v", firstAny(payload["paymentId"], payload["transactionId"], payload["id"], "")),
		Status:            status,
	}
}
