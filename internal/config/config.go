package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPHost                      string
	HTTPPort                      string
	DatabaseURL                   string
	AuthCodeTTLMinutes            int
	AuthSessionTTLMin             int
	AuthMagicLinkBaseURL          string
	AuthCookieName                string
	AuthCookieSecure              bool
	AuthCookieSameSite            string
	AuthCookieDomain              string
	AuthDebugReturnCode           bool
	SMTPHost                      string
	SMTPPort                      int
	SMTPUser                      string
	SMTPPass                      string
	SMTPFrom                      string
	SMTPTLS                       bool
	PublicJobsMaxPerPage          int
	PublicJobsMaxTotal            int
	PaymentSuccessURL             string
	PaymentCancelURL              string
	FreePlanDurationDays          int
	CryptoPaymentProvider         string
	CryptoIPNCallbackURL          string
	CoinPaymentsAPIBaseURL        string
	CoinPaymentsClientID          string
	CoinPaymentsClientSecret      string
	CoinPaymentsWebhookURL        string
	MaxelPayEnv                   string
	MaxelPayAPIBaseURL            string
	MaxelPayAPIKey                string
	MaxelPaySecretKey             string
	MaxelPaySiteName              string
	MaxelPaySiteURL               string
	MaxelPayWebhookURL            string
	OxaPayEnv                     string
	OxaPayAPIBaseURL              string
	OxaPayMerchantAPIKey          string
	NowPaymentsAPIBaseURL         string
	NowPaymentsAPIKey             string
	NowPaymentsDefaultPayCurrency string
	NowPaymentsCurrencyCandidates string
	NowPaymentsIPNSecret          string
	SkippableRecheckBatchSize     int
}

func Load() Config {
	return Config{
		HTTPHost:                      getenv("HTTP_HOSTNAME", "0.0.0.0"),
		HTTPPort:                      getenv("HTTP_PORT", "8080"),
		DatabaseURL:                   getenv("DATABASE_URL", "file:page_extract.db?_foreign_keys=on"),
		AuthCodeTTLMinutes:            getenvInt("AUTH_CODE_TTL_MINUTES", 10),
		AuthSessionTTLMin:             getenvInt("AUTH_SESSION_TTL_MINUTES", 60*24*7),
		AuthMagicLinkBaseURL:          getenv("AUTH_MAGIC_LINK_BASE_URL", "http://localhost:3000/auth/verify"),
		AuthCookieName:                getenv("AUTH_COOKIE_NAME", "session_token"),
		AuthCookieSecure:              getenvBool("AUTH_COOKIE_SECURE", false),
		AuthCookieSameSite:            getenv("AUTH_COOKIE_SAMESITE", "lax"),
		AuthCookieDomain:              os.Getenv("AUTH_COOKIE_DOMAIN"),
		AuthDebugReturnCode:           getenvBool("AUTH_DEBUG_RETURN_CODE", false),
		SMTPHost:                      getenv("SMTP_HOST", ""),
		SMTPPort:                      getenvInt("SMTP_PORT", 587),
		SMTPUser:                      getenv("SMTP_USER", ""),
		SMTPPass:                      getenv("SMTP_PASS", ""),
		SMTPFrom:                      getenv("SMTP_FROM", ""),
		SMTPTLS:                       getenvBool("SMTP_TLS", true),
		PublicJobsMaxPerPage:          getenvInt("PUBLIC_JOBS_MAX_PER_PAGE", 10),
		PublicJobsMaxTotal:            getenvInt("PUBLIC_JOBS_MAX_TOTAL", 50),
		PaymentSuccessURL:             getenv("PAYMENT_SUCCESS_URL", "http://localhost:3000/billing/success"),
		PaymentCancelURL:              getenv("PAYMENT_CANCEL_URL", "http://localhost:3000/billing/cancel"),
		FreePlanDurationDays:          getenvInt("FREE_PLAN_DURATION_DAYS", 7),
		CryptoPaymentProvider:         getenv("CRYPTO_PAYMENT_PROVIDER", "nowpayments"),
		CryptoIPNCallbackURL:          getenv("CRYPTO_IPN_CALLBACK_URL", "http://localhost:8000/pricing/webhooks/crypto"),
		CoinPaymentsAPIBaseURL:        getenv("COINPAYMENTS_API_BASE_URL", "https://a-api.coinpayments.net/api"),
		CoinPaymentsClientID:          getenv("COINPAYMENTS_CLIENT_ID", ""),
		CoinPaymentsClientSecret:      getenv("COINPAYMENTS_CLIENT_SECRET", ""),
		CoinPaymentsWebhookURL:        getenv("COINPAYMENTS_WEBHOOK_URL", "http://localhost:8000/pricing/webhooks/crypto"),
		MaxelPayEnv:                   getenv("MAXELPAY_ENV", "prod"),
		MaxelPayAPIBaseURL:            getenv("MAXELPAY_API_BASE_URL", "https://api.maxelpay.com/v1"),
		MaxelPayAPIKey:                getenv("MAXELPAY_API_KEY", ""),
		MaxelPaySecretKey:             getenv("MAXELPAY_SECRET_KEY", ""),
		MaxelPaySiteName:              getenv("MAXELPAY_SITE_NAME", "GoApplyJob"),
		MaxelPaySiteURL:               getenv("MAXELPAY_SITE_URL", "http://localhost:3000"),
		MaxelPayWebhookURL:            getenv("MAXELPAY_WEBHOOK_URL", "http://localhost:8000/pricing/webhooks/crypto"),
		OxaPayEnv:                     getenv("OXAPAY_ENV", ""),
		OxaPayAPIBaseURL:              getenv("OXAPAY_API_BASE_URL", "https://api.oxapay.com/v1"),
		OxaPayMerchantAPIKey:          getenv("OXAPAY_MERCHANT_API_KEY", ""),
		NowPaymentsAPIBaseURL:         getenv("NOWPAYMENTS_API_BASE_URL", "https://api.nowpayments.io/v1"),
		NowPaymentsAPIKey:             getenv("NOWPAYMENTS_API_KEY", ""),
		NowPaymentsDefaultPayCurrency: getenv("NOWPAYMENTS_DEFAULT_PAY_CURRENCY", "usdttrc20"),
		NowPaymentsCurrencyCandidates: getenv("NOWPAYMENTS_CURRENCY_CANDIDATES", "btc,eth,ltc,usdttrc20,usdterc20,usdtbsc,usdc"),
		NowPaymentsIPNSecret:          getenv("NOWPAYMENTS_IPN_SECRET", ""),
		SkippableRecheckBatchSize:     getenvInt("SKIPPABLE_RECHECK_BATCH_SIZE", 100),
	}
}

func LoadDotEnv(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, strings.TrimSpace(value))
	}
	return nil
}

func LoadDotEnvIfExists(path string) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return LoadDotEnv(path)
}

func Getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func GetenvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func GetenvFloat(key string, fallback float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func GetenvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "TRUE", "True", "yes", "on":
		return true
	default:
		return false
	}
}

func getenv(key, fallback string) string        { return Getenv(key, fallback) }
func getenvInt(key string, fallback int) int    { return GetenvInt(key, fallback) }
func getenvBool(key string, fallback bool) bool { return GetenvBool(key, fallback) }
