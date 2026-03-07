package crypto

import (
	"fmt"
	"strings"

	"goapplyjob-golang-backend/internal/config"
)

func GetGateway(cfg config.Config) (Gateway, error) {
	provider := strings.ToLower(strings.TrimSpace(cfg.CryptoPaymentProvider))
	switch provider {
	case "", "nowpayments", "now_payment", "now-payments":
		return NewNowPaymentsGateway(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported crypto payment provider: %s", provider)
	}
}
