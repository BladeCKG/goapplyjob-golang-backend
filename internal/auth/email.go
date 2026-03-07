package auth

import (
	"bytes"
	"embed"
	"fmt"
	"log"
	"net/smtp"
	"net/url"
	"strings"

	"goapplyjob-golang-backend/internal/config"
)

const (
	siteName = "GoApplyJob"
	siteURL  = "https://www.goapplyjob.online/"
)

//go:embed templates/verification_email.html
var authTemplates embed.FS

func buildVerificationEmailHTML(code string, ttlMinutes int, magicLink string) string {
	templateBody, err := authTemplates.ReadFile("templates/verification_email.html")
	if err != nil {
		return "<html><body><h2>" + siteName + " verification code</h2><p>Your code is: <strong>" + code + "</strong></p><p>It expires in " + fmt.Sprintf("%d", ttlMinutes) + " minutes.</p><p><a href=\"" + magicLink + "\">Log In Instantly</a></p></body></html>"
	}

	replacer := strings.NewReplacer(
		"__SITE_NAME__", siteName,
		"__SITE_URL__", siteURL,
		"__CODE__", code,
		"__TTL_MINUTES__", fmt.Sprintf("%d", ttlMinutes),
		"__MAGIC_LINK__", magicLink,
	)
	return replacer.Replace(string(templateBody))
}

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
	smtpHost := strings.TrimSpace(cfg.SMTPHost)
	smtpUser := strings.TrimSpace(cfg.SMTPUser)
	smtpFrom := strings.TrimSpace(cfg.SMTPFrom)
	if smtpFrom == "" {
		smtpFrom = smtpUser
	}
	if smtpHost == "" || smtpFrom == "" {
		return fmt.Errorf("SMTP is not configured")
	}

	var body bytes.Buffer
	body.WriteString("Subject: ")
	body.WriteString(siteName)
	body.WriteString(" verification code: ")
	body.WriteString(code)
	body.WriteString("\r\n")
	body.WriteString("MIME-Version: 1.0\r\n")
	body.WriteString("From: ")
	body.WriteString(smtpFrom)
	body.WriteString("\r\n")
	body.WriteString("To: ")
	body.WriteString(email)
	body.WriteString("\r\n")
	body.WriteString("Content-Type: multipart/alternative; boundary=BOUNDARY\r\n\r\n")
	body.WriteString("--BOUNDARY\r\n")
	body.WriteString("Content-Type: text/plain; charset=UTF-8\r\n\r\n")
	body.WriteString(siteName + " verification code\r\n\r\n")
	body.WriteString("Your code is: " + code + "\r\n")
	body.WriteString(fmt.Sprintf("It expires in %d minutes.\r\n\r\n", ttlMinutes))
	body.WriteString("One-click sign-in link: " + magicLink + "\r\n\r\n")
	body.WriteString("Sign in at: " + siteURL + "\r\n\r\n")
	body.WriteString("If you did not request this code, you can ignore this email.\r\n")
	body.WriteString("--BOUNDARY\r\n")
	body.WriteString("Content-Type: text/html; charset=UTF-8\r\n\r\n")
	body.WriteString(buildVerificationEmailHTML(code, ttlMinutes, magicLink))
	body.WriteString("\r\n--BOUNDARY--\r\n")

	addr := fmt.Sprintf("%s:%d", smtpHost, cfg.SMTPPort)
	var auth smtp.Auth
	if smtpUser != "" {
		auth = smtp.PlainAuth("", smtpUser, cfg.SMTPPass, smtpHost)
	}

	if cfg.SMTPTLS {
		client, err := smtp.Dial(addr)
		if err != nil {
			return err
		}
		defer client.Close()
		if err := client.StartTLS(nil); err != nil {
			return err
		}
		if auth != nil {
			if err := client.Auth(auth); err != nil {
				return err
			}
		}
		if err := client.Mail(smtpFrom); err != nil {
			return err
		}
		if err := client.Rcpt(email); err != nil {
			return err
		}
		writer, err := client.Data()
		if err != nil {
			return err
		}
		if _, err := writer.Write(body.Bytes()); err != nil {
			_ = writer.Close()
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
		return client.Quit()
	}

	return smtp.SendMail(addr, auth, smtpFrom, []string{email}, body.Bytes())
}

func logVerificationEmailFailure(email string, err error) {
	log.Printf("failed sending verification email to %s: %v", email, err)
}
