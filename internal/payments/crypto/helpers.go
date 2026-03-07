package crypto

import (
	"fmt"
	"strings"
)

func ParseGenericPaymentStatus(value any) string {
	normalized := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", value)))
	switch normalized {
	case "paid", "success", "completed", "confirmed", "finished", "sending", "paid_success":
		return "paid"
	case "failed", "cancelled", "canceled", "expired", "error", "refunded":
		return "failed"
	default:
		return "pending"
	}
}
