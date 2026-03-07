package crypto

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"testing"

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
