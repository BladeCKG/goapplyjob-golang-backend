package auth

import (
	"log"
	"net/url"
	"strings"

	"goapplyjob-golang-backend/internal/config"
	emailservice "goapplyjob-golang-backend/internal/email"
)

const (
	siteName = "GoApplyJob"
	siteURL  = "https://www.goapplyjob.online/"
)

func buildMagicLoginURL(cfg config.Config, token string) string {
	baseURL := strings.TrimSpace(cfg.AuthMagicLinkBaseURL)
	if baseURL == "" {
		baseURL = strings.TrimRight(siteURL, "/") + "/auth/verify"
	}
	separator := "?"
	if strings.Contains(baseURL, "?") {
		separator = "&"
	}
	return baseURL + separator + "token=" + url.QueryEscape(token)
}

func sendVerificationEmail(cfg config.Config, email, code string, ttlMinutes int, magicLink string) error {
	return emailservice.NewService(cfg).SendVerificationEmail(email, siteName, siteURL, code, ttlMinutes, magicLink)
}

func logVerificationEmailFailure(email string, err error) {
	log.Printf("failed sending verification email to %s: %v", email, err)
}
