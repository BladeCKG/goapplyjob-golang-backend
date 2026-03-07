package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/config"
)

func TestCryptoGatewayFactoryDefaultsToNowPayments(t *testing.T) {
	cfg := config.Config{}
	gateway, err := GetGateway(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := gateway.(*NowPaymentsGateway); !ok {
		t.Fatalf("expected NowPaymentsGateway, got %T", gateway)
	}
}

func TestCryptoGatewayFactoryRejectsUnknownProvider(t *testing.T) {
	cfg := config.Config{CryptoPaymentProvider: "unknown-gateway"}
	if _, err := GetGateway(cfg); err == nil {
		t.Fatal("expected unsupported provider error")
	}
}

func TestCryptoGatewayFactorySupportsCoinPayments(t *testing.T) {
	cfg := config.Config{CryptoPaymentProvider: "coinpayment"}
	gateway, err := GetGateway(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := gateway.(*CoinPaymentsGateway); !ok {
		t.Fatalf("expected CoinPaymentsGateway, got %T", gateway)
	}
}

func TestCryptoGatewayFactorySupportsMaxelPay(t *testing.T) {
	cfg := config.Config{CryptoPaymentProvider: "maxel-pay"}
	gateway, err := GetGateway(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := gateway.(*MaxelPayGateway); !ok {
		t.Fatalf("expected MaxelPayGateway, got %T", gateway)
	}
}

func TestCryptoGatewayFactorySupportsOxaPay(t *testing.T) {
	cfg := config.Config{CryptoPaymentProvider: "oxa-pay"}
	gateway, err := GetGateway(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := gateway.(*OxaPayGateway); !ok {
		t.Fatalf("expected OxaPayGateway, got %T", gateway)
	}
}

func TestNowPaymentsVerifyWebhookSignatureAcceptsValidSignature(t *testing.T) {
	cfg := config.Config{NowPaymentsIPNSecret: "secret-token"}
	gateway := NewNowPaymentsGateway(cfg)
	payload := map[string]any{"order_id": "123", "payment_status": "finished"}
	raw, _ := json.Marshal(payload)
	mac := hmac.New(sha512.New, []byte("secret-token"))
	_, _ = mac.Write(raw)
	signature := fmt.Sprintf("%x", mac.Sum(nil))
	if err := gateway.VerifyWebhookSignature(payload, map[string]string{"x-nowpayments-sig": signature}); err != nil {
		t.Fatal(err)
	}
}

func TestNowPaymentsVerifyWebhookSignatureRejectsInvalidSignature(t *testing.T) {
	cfg := config.Config{NowPaymentsIPNSecret: "secret-token"}
	gateway := NewNowPaymentsGateway(cfg)
	payload := map[string]any{"order_id": "123", "payment_status": "finished"}
	if err := gateway.VerifyWebhookSignature(payload, map[string]string{"x-nowpayments-sig": "invalid"}); err == nil {
		t.Fatal("expected invalid signature error")
	}
}

func TestNowPaymentsParseWebhookMapsStatusAndIdentifiers(t *testing.T) {
	gateway := NewNowPaymentsGateway(config.Config{})
	paid := gateway.ParseWebhook(map[string]any{"order_id": "77", "payment_id": "np_88", "payment_status": "confirmed"})
	if paid.OrderID != "77" || paid.ProviderPaymentID != "np_88" || paid.Status != PaymentPaid {
		t.Fatalf("unexpected paid webhook %#v", paid)
	}
	failed := gateway.ParseWebhook(map[string]any{"order_id": "1", "payment_status": "failed"})
	if failed.Status != PaymentFailed {
		t.Fatalf("unexpected failed webhook %#v", failed)
	}
	pending := gateway.ParseWebhook(map[string]any{"order_id": "2", "payment_status": "waiting"})
	if pending.Status != PaymentPending {
		t.Fatalf("unexpected pending webhook %#v", pending)
	}
}

func TestCoinPaymentsVerifyWebhookSignatureAcceptsValidSignature(t *testing.T) {
	cfg := config.Config{
		CryptoPaymentProvider:    "coinpayments",
		CoinPaymentsClientID:     "client-1",
		CoinPaymentsClientSecret: "secret-token",
		CoinPaymentsWebhookURL:   "http://localhost:8000/pricing/webhooks/crypto",
	}
	gateway := NewCoinPaymentsGateway(cfg)
	payload := map[string]any{"invoice": "123", "status": "100"}
	raw, _ := json.Marshal(payload)
	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05")
	message := "\ufeffPOSThttp://localhost:8000/pricing/webhooks/cryptoclient-1" + timestamp + string(raw)
	mac := hmac.New(sha256.New, []byte("secret-token"))
	_, _ = mac.Write([]byte(message))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	err := gateway.VerifyWebhookSignature(payload, map[string]string{
		"X-CoinPayments-Client":    "client-1",
		"X-CoinPayments-Timestamp": timestamp,
		"X-CoinPayments-Signature": signature,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCoinPaymentsParseWebhookMapsStatusAndIdentifiers(t *testing.T) {
	gateway := NewCoinPaymentsGateway(config.Config{})
	paid := gateway.ParseWebhook(map[string]any{
		"type":    "invoicePaid",
		"invoice": map[string]any{"invoiceNumber": "55", "id": "cp_inv_1"},
	})
	if paid.OrderID != "55" || paid.ProviderPaymentID != "cp_inv_1" || paid.Status != PaymentPaid {
		t.Fatalf("unexpected paid webhook %#v", paid)
	}
	failed := gateway.ParseWebhook(map[string]any{
		"type":    "invoiceCancelled",
		"invoice": map[string]any{"invoiceNumber": "56", "id": "cp_inv_2"},
	})
	if failed.Status != PaymentFailed {
		t.Fatalf("unexpected failed webhook %#v", failed)
	}
	pending := gateway.ParseWebhook(map[string]any{
		"type":    "invoicePending",
		"invoice": map[string]any{"invoiceNumber": "57", "id": "cp_inv_3"},
	})
	if pending.Status != PaymentPending {
		t.Fatalf("unexpected pending webhook %#v", pending)
	}
}
