package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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

	endpoint := strings.TrimRight(strings.TrimSpace(g.cfg.CoinPaymentsAPIBaseURL), "/") + "/v2/merchant/invoices"
	amountText := fmt.Sprintf("%.2f", request.AmountUSD)
	payload := map[string]any{
		"currency":  "USD",
		"invoiceId": request.OrderID,
		"items": []map[string]any{
			{
				"customId": request.OrderID,
				"name":     request.Description,
				"quantity": map[string]any{
					"value": 1,
					"type":  "2",
				},
				"amount": amountText,
			},
		},
		"amount": map[string]any{
			"breakdown": map[string]any{
				"subtotal": amountText,
			},
			"total": amountText,
		},
	}
	if payCurrency := strings.TrimSpace(request.PayCurrency); payCurrency != "" {
		payload["payment"] = map[string]any{
			"paymentCurrency": strings.ToUpper(payCurrency),
		}
	}
	if strings.TrimSpace(request.CallbackURL) != "" {
		payload["webhooks"] = []map[string]any{
			{
				"notificationsUrl": request.CallbackURL,
				"notifications": []string{
					"invoiceCreated",
					"invoicePending",
					"invoicePaid",
					"invoiceCompleted",
					"invoiceCancelled",
					"invoiceTimedOut",
				},
			},
		}
	}

	headers, payloadText := g.buildSignedHeaders("POST", endpoint, payload)
	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(payloadText))
	if err != nil {
		return InvoiceResult{}, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return InvoiceResult{}, fmt.Errorf("CoinPayments request failed: %T", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return InvoiceResult{}, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return InvoiceResult{}, fmt.Errorf("CoinPayments invoice creation failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	responsePayload := map[string]any{}
	if err := json.Unmarshal(body, &responsePayload); err != nil {
		responsePayload = map[string]any{"raw": strings.TrimSpace(string(body))}
	}
	invoicePayload := extractCoinPaymentsInvoicePayload(responsePayload)
	providerID := strings.TrimSpace(fmt.Sprintf("%v", firstAny(
		invoicePayload["id"],
		invoicePayload["invoiceId"],
		invoicePayload["invoice_id"],
		invoicePayload["txn_id"],
		"coinpayments_invoice_"+request.OrderID,
	)))
	invoiceURL := strings.TrimSpace(fmt.Sprintf("%v", firstAny(
		invoicePayload["checkoutLink"],
		invoicePayload["link"],
		invoicePayload["checkout_url"],
		invoicePayload["checkoutUrl"],
		invoicePayload["url"],
		"",
	)))

	return InvoiceResult{
		ProviderInvoiceID: providerID,
		InvoiceURL:        invoiceURL,
		ProviderPayload:   invoicePayload,
	}, nil
}

func (g *CoinPaymentsGateway) ListCurrencies(amountUSD *float64) []CurrencyOption {
	endpoint := strings.TrimRight(strings.TrimSpace(g.cfg.CoinPaymentsAPIBaseURL), "/") + "/v2/currencies"
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err == nil {
		req.Header.Set("Accept", "application/json")
		client := &http.Client{Timeout: 6 * time.Second}
		resp, err := client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode < http.StatusBadRequest {
				raw, readErr := io.ReadAll(resp.Body)
				if readErr == nil {
					payload := map[string]any{}
					if jsonErr := json.Unmarshal(raw, &payload); jsonErr == nil {
						codes := extractCoinPaymentsCurrencyCodes(payload)
						if len(codes) > 0 {
							out := make([]CurrencyOption, 0, len(codes))
							for code := range codes {
								out = append(out, CurrencyOption{Code: code, MinUSD: nil})
							}
							return out
						}
					}
				}
			}
		}
	}
	return fallbackCoinPaymentsCurrencies()
}

func (g *CoinPaymentsGateway) VerifyWebhookSignature(payload map[string]any, headers map[string]string, rawBody []byte) error {
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
	payloadText := rawBody
	if len(payloadText) == 0 {
		payloadText, _ = json.Marshal(payload)
	}
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
	eventType := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", payload["type"])))
	invoice, _ := payload["invoice"].(map[string]any)
	status := g.mapEventType(eventType)
	if status == "" {
		status = PaymentPending
		switch strings.TrimSpace(fmt.Sprintf("%v", firstAny(invoice["status"], payload["status"]))) {
		case "100", "2":
			status = PaymentPaid
		case "-1":
			status = PaymentFailed
		default:
			switch strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", firstAny(invoice["statusText"], payload["statusText"], payload["status_text"])))) {
			case "failed", "cancelled", "canceled", "error", "refunded", "expired":
				status = PaymentFailed
			case "complete", "confirmed", "finished", "paid":
				status = PaymentPaid
			}
		}
	}
	return WebhookParseResult{
		OrderID:           fmt.Sprintf("%v", firstAny(invoice["invoiceNumber"], invoice["invoiceId"], invoice["id"], payload["invoiceNumber"], payload["invoiceId"], payload["invoice"], "")),
		ProviderPaymentID: fmt.Sprintf("%v", firstAny(invoice["id"], payload["id"], payload["txn_id"], "")),
		Status:            status,
	}
}

func (g *CoinPaymentsGateway) VerifyPayment(providerPaymentID string, orderID string) (*VerificationResult, error) {
	return nil, nil
}

func (g *CoinPaymentsGateway) mapEventType(eventType string) PaymentStatus {
	switch eventType {
	case "invoicepaid", "invoicecompleted":
		return PaymentPaid
	case "invoicecancelled", "invoicetimedout", "invoicepaymenttimedout":
		return PaymentFailed
	case "invoicecreated", "invoicepending", "invoicepaymentcreated":
		return PaymentPending
	default:
		return ""
	}
}

func (g *CoinPaymentsGateway) buildSignedHeaders(method string, endpoint string, payload map[string]any) (map[string]string, string) {
	timestamp := time.Now().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05")
	payloadText, _ := json.Marshal(payload)
	message := "\ufeff" + strings.ToUpper(method) + endpoint + g.cfg.CoinPaymentsClientID + timestamp + string(payloadText)
	mac := hmac.New(sha256.New, []byte(g.cfg.CoinPaymentsClientSecret))
	_, _ = mac.Write([]byte(message))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return map[string]string{
		"Content-Type":             "application/json",
		"Accept":                   "application/json",
		"X-CoinPayments-Client":    g.cfg.CoinPaymentsClientID,
		"X-CoinPayments-Timestamp": timestamp,
		"X-CoinPayments-Signature": signature,
	}, string(payloadText)
}

func extractCoinPaymentsInvoicePayload(payload map[string]any) map[string]any {
	if invoices, ok := payload["invoices"].([]any); ok && len(invoices) > 0 {
		if first, ok := invoices[0].(map[string]any); ok {
			return first
		}
	}
	if result, ok := payload["result"].(map[string]any); ok {
		if invoice, ok := result["invoice"].(map[string]any); ok {
			return invoice
		}
		return result
	}
	if invoice, ok := payload["invoice"].(map[string]any); ok {
		return invoice
	}
	return payload
}

func extractCoinPaymentsCurrencyCodes(payload map[string]any) map[string]struct{} {
	candidates := []any{}
	for _, key := range []string{"result", "items", "data", "currencies"} {
		value := payload[key]
		switch typed := value.(type) {
		case []any:
			candidates = append(candidates, typed...)
		case map[string]any:
			for _, item := range typed {
				candidates = append(candidates, item)
			}
		}
	}
	codes := map[string]struct{}{}
	for _, item := range candidates {
		if text, ok := item.(string); ok {
			code := strings.TrimSpace(strings.ToUpper(text))
			if code != "" {
				codes[code] = struct{}{}
			}
			continue
		}
		if itemMap, ok := item.(map[string]any); ok {
			for _, key := range []string{"symbol", "code", "currency"} {
				if raw, ok := itemMap[key]; ok {
					code := strings.TrimSpace(strings.ToUpper(fmt.Sprintf("%v", raw)))
					if code != "" {
						codes[code] = struct{}{}
						break
					}
				}
			}
		}
	}
	return codes
}

func fallbackCoinPaymentsCurrencies() []CurrencyOption {
	return []CurrencyOption{
		{Code: "BTC", MinUSD: nil},
		{Code: "ETH", MinUSD: nil},
		{Code: "LTC", MinUSD: nil},
		{Code: "USDT.TRC20", MinUSD: nil},
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
