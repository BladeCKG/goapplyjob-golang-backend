package config

import (
	"net/url"
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
	AuthEnableCodeLogin           bool
	AuthEnableGoogleLogin         bool
	EmployerPostingFeeUSD         int
	EmailProvider                 string
	EmailProviders                string
	MailtrapAPIToken              string
	MailtrapAPITokens             string
	MailtrapFromEmail             string
	MailtrapFromName              string
	MailtrapUseSandbox            bool
	MailtrapInboxID               string
	BrevoAPIKey                   string
	BrevoAPIKeys                  string
	BrevoFromEmail                string
	BrevoFromName                 string
	BrevoAPIURL                   string
	SiteName                      string
	SiteURL                       string
	SupabaseURL                   string
	SupabaseAnonKey               string
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
		HTTPPort:                      getenv("HTTP_PORT", "8000"),
		DatabaseURL:                   normalizeDatabaseURL(getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")),
		AuthCodeTTLMinutes:            getenvInt("AUTH_CODE_TTL_MINUTES", 10),
		AuthSessionTTLMin:             getenvInt("AUTH_SESSION_TTL_MINUTES", 60*24*7),
		AuthMagicLinkBaseURL:          getenv("AUTH_MAGIC_LINK_BASE_URL", "/auth/verify"),
		AuthCookieName:                getenv("AUTH_COOKIE_NAME", "session_token"),
		AuthCookieSecure:              getenvBool("AUTH_COOKIE_SECURE", false),
		AuthCookieSameSite:            getenv("AUTH_COOKIE_SAMESITE", "lax"),
		AuthCookieDomain:              os.Getenv("AUTH_COOKIE_DOMAIN"),
		AuthDebugReturnCode:           getenvBool("AUTH_DEBUG_RETURN_CODE", false),
		AuthEnableCodeLogin:           getenvBool("AUTH_ENABLE_CODE_LOGIN", true),
		AuthEnableGoogleLogin:         getenvBool("AUTH_ENABLE_GOOGLE_LOGIN", false),
		EmployerPostingFeeUSD:         getenvInt("EMPLOYER_POSTING_FEE_USD", 10),
		EmailProvider:                 getenv("EMAIL_PROVIDER", "brevo"),
		EmailProviders:                getenv("EMAIL_PROVIDERS", ""),
		MailtrapAPIToken:              getenv("MAILTRAP_API_TOKEN", ""),
		MailtrapAPITokens:             getenv("MAILTRAP_API_TOKENS", ""),
		MailtrapFromEmail:             getenv("MAILTRAP_FROM_EMAIL", ""),
		MailtrapFromName:              getenv("MAILTRAP_FROM_NAME", "GoApplyJob"),
		MailtrapUseSandbox:            getenvBool("MAILTRAP_USE_SANDBOX", false),
		MailtrapInboxID:               getenv("MAILTRAP_INBOX_ID", ""),
		BrevoAPIKey:                   getenv("BREVO_API_KEY", ""),
		BrevoAPIKeys:                  getenv("BREVO_API_KEYS", ""),
		BrevoFromEmail:                getenv("BREVO_FROM_EMAIL", ""),
		BrevoFromName:                 getenv("BREVO_FROM_NAME", "GoApplyJob"),
		BrevoAPIURL:                   getenv("BREVO_API_URL", "https://api.brevo.com/v3/smtp/email"),
		SiteName:                      getenv("SITE_NAME", "GoApplyJob"),
		SiteURL:                       getenv("SITE_URL", "http://localhost:3000"),
		SupabaseURL:                   getenv("SUPABASE_URL", ""),
		SupabaseAnonKey:               getenv("SUPABASE_ANON_KEY", ""),
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
		FreePlanDurationDays:          getenvInt("FREE_PLAN_DURATION_DAYS", 365),
		CryptoPaymentProvider:         getenv("CRYPTO_PAYMENT_PROVIDER", "oxapay"),
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

func normalizeDatabaseURL(raw string) string {
	value := strings.TrimSpace(raw)
	if strings.HasPrefix(value, "postgres://") {
		if parsed, err := url.Parse(value); err == nil {
			parsed.Scheme = "postgresql"
			return parsed.String()
		}
		return "postgresql://" + strings.TrimPrefix(value, "postgres://")
	}
	return value
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

func GetenvCSVSet(key, fallback string) map[string]struct{} {
	raw := os.Getenv(key)
	if strings.TrimSpace(raw) == "" {
		raw = fallback
	}
	values := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		value := strings.ToLower(strings.TrimSpace(part))
		if value == "" {
			continue
		}
		values[value] = struct{}{}
	}
	return values
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
