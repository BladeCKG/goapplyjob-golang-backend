package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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

	endpoint := strings.TrimRight(strings.TrimSpace(g.cfg.MaxelPayAPIBaseURL), "/")
	env := strings.TrimSpace(g.cfg.MaxelPayEnv)
	if env == "" {
		env = "prod"
	}
	endpoint = endpoint + "/" + env + "/merchant/order/checkout"

	username := request.CustomerName
	if strings.TrimSpace(username) == "" {
		parts := strings.SplitN(request.CustomerEmail, "@", 2)
		username = parts[0]
	}

	payloadData := map[string]any{
		"orderID":     request.OrderID,
		"amount":      fmt.Sprintf("%.2f", request.AmountUSD),
		"currency":    "USD",
		"timestamp":   fmt.Sprintf("%d", time.Now().Unix()),
		"userName":    username,
		"siteName":    strings.TrimSpace(fmt.Sprintf("%v", firstAny(g.cfg.MaxelPaySiteName, "GoApplyJob"))),
		"userEmail":   request.CustomerEmail,
		"redirectUrl": request.SuccessURL,
		"websiteUrl":  strings.TrimSpace(fmt.Sprintf("%v", firstAny(g.cfg.MaxelPaySiteURL, request.SuccessURL))),
		"cancelUrl":   request.CancelURL,
	}
	if strings.TrimSpace(request.CallbackURL) != "" {
		payloadData["webhookUrl"] = request.CallbackURL
	} else if strings.TrimSpace(g.cfg.MaxelPayWebhookURL) != "" {
		payloadData["webhookUrl"] = g.cfg.MaxelPayWebhookURL
	}

	encrypted, err := encryptMaxelPayPayload(g.cfg.MaxelPaySecretKey, payloadData)
	if err != nil {
		return InvoiceResult{}, err
	}
	body := []byte(fmt.Sprintf(`{"data":"%s"}`, encrypted))

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return InvoiceResult{}, err
	}
	req.Header.Set("api-key", g.cfg.MaxelPayAPIKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return InvoiceResult{}, fmt.Errorf("Maxelpay request failed: %T", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return InvoiceResult{}, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return InvoiceResult{}, fmt.Errorf("Maxelpay checkout creation failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		payload = map[string]any{"raw": strings.TrimSpace(string(raw))}
	}
	providerID := strings.TrimSpace(fmt.Sprintf("%v", firstAny(
		payload["orderID"],
		payload["orderId"],
		payload["order_id"],
		payload["id"],
		payload["paymentId"],
		payload["transactionId"],
		"maxelpay_invoice_"+request.OrderID,
	)))
	invoiceURL := ""
	if value := strings.TrimSpace(fmt.Sprintf("%v", payload["result"])); value != "" && value != "<nil>" {
		invoiceURL = value
	}
	if invoiceURL == "" {
		invoiceURL = extractMaxelPayInvoiceURL(payload)
	}
	if invoiceURL == "" {
		invoiceURL = request.SuccessURL
	}
	return InvoiceResult{
		ProviderInvoiceID: providerID,
		InvoiceURL:        invoiceURL,
		ProviderPayload:   payload,
	}, nil
}

func (g *MaxelPayGateway) ListCurrencies(amountUSD *float64) []CurrencyOption {
	return []CurrencyOption{{Code: "USD", MinUSD: nil}}
}

func (g *MaxelPayGateway) VerifyWebhookSignature(payload map[string]any, headers map[string]string, rawBody []byte) error {
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

func (g *MaxelPayGateway) VerifyPayment(providerPaymentID string, orderID string) (*VerificationResult, error) {
	return nil, nil
}

func encryptMaxelPayPayload(secretKey string, payloadData map[string]any) (string, error) {
	keyBytes := []byte(secretKey)
	switch len(keyBytes) {
	case 16, 24, 32:
	default:
		return "", fmt.Errorf("invalid Maxelpay secret key length")
	}
	iv := []byte(secretKey)
	if len(iv) < 16 {
		return "", fmt.Errorf("invalid Maxelpay secret key length")
	}
	iv = iv[:16]

	plain, err := json.Marshal(payloadData)
	if err != nil {
		return "", err
	}
	blockSize := 256
	padding := blockSize - (len(plain) % blockSize)
	padded := append(plain, bytes.Repeat([]byte(" "), padding)...)

	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return "", err
	}
	mode := cipher.NewCBCEncrypter(block, iv)
	encrypted := make([]byte, len(padded))
	mode.CryptBlocks(encrypted, padded)
	return base64.StdEncoding.EncodeToString(encrypted), nil
}

func extractMaxelPayInvoiceURL(payload map[string]any) string {
	candidates := []any{
		payload["checkoutUrl"],
		payload["checkout_url"],
		payload["paymentUrl"],
		payload["payment_url"],
		payload["redirectUrl"],
		payload["redirect_url"],
		payload["url"],
	}
	for _, key := range []string{"data", "result"} {
		if nested, ok := payload[key].(map[string]any); ok {
			candidates = append(candidates,
				nested["checkoutUrl"],
				nested["checkout_url"],
				nested["paymentUrl"],
				nested["payment_url"],
				nested["redirectUrl"],
				nested["redirect_url"],
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
