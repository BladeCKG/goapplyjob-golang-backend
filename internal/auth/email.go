package auth

import (
	"goapplyjob-golang-backend/internal/config"
	"log"
	"net/url"
	"strings"

	emailservice "goapplyjob-golang-backend/internal/email"
)

func buildMagicLoginURL(cfg config.Config, token string) string {
	baseURL := cfg.SiteURL + cfg.AuthMagicLinkBaseURL
	separator := "?"
	if strings.Contains(baseURL, "?") {
		separator = "&"
	}
	return baseURL + separator + "token=" + url.QueryEscape(token)
}

func sendVerificationEmail(cfg config.Config, email, code string, ttlMinutes int, magicLink string) error {
	return emailservice.NewService(cfg).SendVerificationEmail(email, cfg.SiteName, cfg.SiteURL, code, ttlMinutes, magicLink)
}

func logVerificationEmailFailure(email string, err error) {
	log.Printf("failed sending verification email to %s: %v", email, err)
}
