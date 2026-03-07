package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"goapplyjob-golang-backend/internal/config"
)

type CoinPaymentsGateway struct {
	cfg config.Config
}

func NewCoinPaymentsGateway(cfg config.Config) *CoinPaymentsGateway {
	return &CoinPaymentsGateway{cfg: cfg}
}

func (g *CoinPaymentsGateway) CreateInvoice(request InvoiceRequest) (InvoiceResult, error) {
	if strings.TrimSpace(g.cfg.CoinPaymentsClientID) == "" || strings.TrimSpace(g.cfg.CoinPaymentsClientSecret) == "" {
		return InvoiceResult{
			ProviderInvoiceID: "coinpayments_local_" + request.OrderID,
			InvoiceURL:        request.SuccessURL + "?payment_id=" + request.OrderID,
			ProviderPayload: map[string]any{
				"mode":     "local_stub",
				"provider": "coinpayments",
			},
		}, nil
	}
	return InvoiceResult{
		ProviderInvoiceID: "coinpayments_invoice_" + request.OrderID,
		InvoiceURL:        request.SuccessURL,
		ProviderPayload: map[string]any{
			"id":           "coinpayments_invoice_" + request.OrderID,
			"invoice_url":  request.SuccessURL,
			"pay_currency": strings.ToUpper(strings.TrimSpace(request.PayCurrency)),
		},
	}, nil
}

func (g *CoinPaymentsGateway) ListCurrencies(amountUSD *float64) []CurrencyOption {
	return []CurrencyOption{
		{Code: "BTC", MinUSD: nil},
		{Code: "ETH", MinUSD: nil},
		{Code: "LTC", MinUSD: nil},
		{Code: "USDT.TRC20", MinUSD: nil},
	}
}

func (g *CoinPaymentsGateway) VerifyWebhookSignature(payload map[string]any, headers map[string]string) error {
	clientID := strings.TrimSpace(getHeader(headers, "x-coinpayments-client"))
	timestamp := strings.TrimSpace(getHeader(headers, "x-coinpayments-timestamp"))
	signature := strings.TrimSpace(getHeader(headers, "x-coinpayments-signature"))
	secret := strings.TrimSpace(g.cfg.CoinPaymentsClientSecret)
	if secret == "" {
		return nil
	}
	if clientID == "" || timestamp == "" || signature == "" {
		return fmt.Errorf("Missing CoinPayments signature headers")
	}
	webhookURL := strings.TrimSpace(g.cfg.CoinPaymentsWebhookURL)
	if webhookURL == "" {
		webhookURL = strings.TrimSpace(g.cfg.CryptoIPNCallbackURL)
	}
	payloadText, _ := json.Marshal(payload)
	message := "\ufeffPOST" + webhookURL + clientID + timestamp + string(payloadText)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(message))
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("Invalid CoinPayments signature")
	}
	return nil
}

func (g *CoinPaymentsGateway) ParseWebhook(payload map[string]any) WebhookParseResult {
	status := PaymentPending
	switch strings.TrimSpace(fmt.Sprintf("%v", payload["status"])) {
	case "100", "2":
		status = PaymentPaid
	case "-1":
		status = PaymentFailed
	}
	return WebhookParseResult{
		OrderID:           fmt.Sprintf("%v", firstAny(payload["invoice"], payload["invoiceNumber"], payload["custom"], "")),
		ProviderPaymentID: fmt.Sprintf("%v", firstAny(payload["txn_id"], payload["id"], "")),
		Status:            status,
	}
}

func firstAny(values ...any) any {
	for _, value := range values {
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			return text
		}
		if value != nil {
			return value
		}
	}
	return ""
}

func getHeader(headers map[string]string, key string) string {
	for headerKey, value := range headers {
		if strings.EqualFold(headerKey, key) {
			return value
		}
	}
	return ""
}
