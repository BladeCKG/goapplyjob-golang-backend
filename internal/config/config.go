package config

import (
	"os"
	"strconv"
)

type Config struct {
	HTTPHost             string
	HTTPPort             string
	DatabaseURL          string
	AuthCodeTTLMinutes   int
	AuthSessionTTLMin    int
	AuthCookieName       string
	AuthCookieSecure     bool
	AuthCookieSameSite   string
	AuthCookieDomain     string
	AuthDebugReturnCode  bool
	PublicJobsMaxPerPage int
	PublicJobsMaxTotal   int
	CryptoWebhookToken   string
	PaymentSuccessURL    string
	PaymentCancelURL     string
}

func Load() Config {
	return Config{
		HTTPHost:             getenv("HTTP_HOSTNAME", "0.0.0.0"),
		HTTPPort:             getenv("HTTP_PORT", "8080"),
		DatabaseURL:          getenv("DATABASE_URL", "file:page_extract.db?_foreign_keys=on"),
		AuthCodeTTLMinutes:   getenvInt("AUTH_CODE_TTL_MINUTES", 10),
		AuthSessionTTLMin:    getenvInt("AUTH_SESSION_TTL_MINUTES", 60*24*7),
		AuthCookieName:       getenv("AUTH_COOKIE_NAME", "session_token"),
		AuthCookieSecure:     getenvBool("AUTH_COOKIE_SECURE", false),
		AuthCookieSameSite:   getenv("AUTH_COOKIE_SAMESITE", "lax"),
		AuthCookieDomain:     os.Getenv("AUTH_COOKIE_DOMAIN"),
		AuthDebugReturnCode:  getenvBool("AUTH_DEBUG_RETURN_CODE", false),
		PublicJobsMaxPerPage: getenvInt("PUBLIC_JOBS_MAX_PER_PAGE", 10),
		PublicJobsMaxTotal:   getenvInt("PUBLIC_JOBS_MAX_TOTAL", 50),
		CryptoWebhookToken:   getenv("CRYPTO_WEBHOOK_TOKEN", ""),
		PaymentSuccessURL:    getenv("PAYMENT_SUCCESS_URL", "http://localhost:3000/billing/success"),
		PaymentCancelURL:     getenv("PAYMENT_CANCEL_URL", "http://localhost:3000/billing/cancel"),
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
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

func getenvBool(key string, fallback bool) bool {
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
