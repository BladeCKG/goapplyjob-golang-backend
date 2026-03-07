package crypto

type PaymentStatus string

const (
	PaymentPending PaymentStatus = "pending"
	PaymentPaid    PaymentStatus = "paid"
	PaymentFailed  PaymentStatus = "failed"
)

type InvoiceRequest struct {
	OrderID       string
	AmountUSD     float64
	Description   string
	SuccessURL    string
	CancelURL     string
	CallbackURL   string
	PayCurrency   string
	CustomerEmail string
	CustomerName  string
}

type InvoiceResult struct {
	ProviderInvoiceID string
	InvoiceURL        string
	ProviderPayload   map[string]any
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
	CreateInvoice(request InvoiceRequest) (InvoiceResult, error)
	ListCurrencies(amountUSD *float64) []CurrencyOption
	VerifyWebhookSignature(payload map[string]any, headers map[string]string, rawBody []byte) error
	ParseWebhook(payload map[string]any) WebhookParseResult
}
