package crypto

type PaymentStatus string

const (
	PaymentPending PaymentStatus = "pending"
	PaymentPaid    PaymentStatus = "paid"
	PaymentFailed  PaymentStatus = "failed"
)

type CheckoutRequest struct {
	OrderID     string
	AmountUSD   float64
	Description string
	SuccessURL  string
	CancelURL   string
	CallbackURL string
	PayCurrency string
}

type CheckoutResult struct {
	ProviderCheckoutID string
	CheckoutURL        string
	ProviderPayload    map[string]any
}

type CurrencyOption struct {
	Code   string
	MinUSD *float64
}

type WebhookParseResult struct {
	OrderID           string
	ProviderPaymentID string
	Status            PaymentStatus
}

type Gateway interface {
	CreateInvoice(request CheckoutRequest) (CheckoutResult, error)
	ListCurrencies(amountUSD *float64) []CurrencyOption
	VerifyWebhookSignature(payload map[string]any, headers map[string]string) error
	ParseWebhook(payload map[string]any) WebhookParseResult
}
