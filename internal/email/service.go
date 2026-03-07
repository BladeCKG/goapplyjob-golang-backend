package email

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/smtp"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
)

const (
	defaultProvider = "brevo"
	defaultBrevoURL = "https://api.brevo.com/v3/smtp/email"
)

//go:embed templates/verification_email.html
var templates embed.FS

type Service struct {
	cfg        config.Config
	httpClient *http.Client
}

func NewService(cfg config.Config) *Service {
	return &Service{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (s *Service) BuildVerificationEmailHTML(siteName, siteURL, code string, ttlMinutes int, magicLink string) string {
	templateBody, err := templates.ReadFile("templates/verification_email.html")
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

func (s *Service) SendVerificationEmail(toEmail, siteName, siteURL, code string, ttlMinutes int, magicLink string) error {
	htmlContent := s.BuildVerificationEmailHTML(siteName, siteURL, code, ttlMinutes, magicLink)
	textContent := siteName + " verification code\r\n\r\n" +
		"Your code is: " + code + "\r\n" +
		fmt.Sprintf("It expires in %d minutes.\r\n\r\n", ttlMinutes) +
		"One-click sign-in link: " + magicLink + "\r\n\r\n" +
		"Sign in at: " + siteURL + "\r\n\r\n" +
		"If you did not request this code, you can ignore this email.\r\n"

	switch s.normalizeProvider() {
	case "brevo":
		return s.sendViaBrevo(toEmail, siteName, code, textContent, htmlContent)
	case "smtp":
		return s.sendViaSMTP(toEmail, siteName, code, textContent, htmlContent)
	case "auto":
		if strings.TrimSpace(s.cfg.BrevoAPIKey) != "" && strings.TrimSpace(s.cfg.BrevoFromEmail) != "" {
			return s.sendViaBrevo(toEmail, siteName, code, textContent, htmlContent)
		}
		return s.sendViaSMTP(toEmail, siteName, code, textContent, htmlContent)
	default:
		return fmt.Errorf("unsupported EMAIL_PROVIDER: %s", s.normalizeProvider())
	}
}

func (s *Service) normalizeProvider() string {
	value := strings.ToLower(strings.TrimSpace(s.cfg.EmailProvider))
	if value == "" {
		value = defaultProvider
	}
	switch value {
	case "brevo", "brevo_api", "api":
		return "brevo"
	case "smtp":
		return "smtp"
	case "auto":
		return "auto"
	default:
		return value
	}
}

func (s *Service) sendViaBrevo(toEmail, siteName, code, textContent, htmlContent string) error {
	if strings.TrimSpace(s.cfg.BrevoAPIKey) == "" || strings.TrimSpace(s.cfg.BrevoFromEmail) == "" {
		return fmt.Errorf("Brevo email API is not configured")
	}
	apiURL := strings.TrimSpace(s.cfg.BrevoAPIURL)
	if apiURL == "" {
		apiURL = defaultBrevoURL
	}
	body := map[string]any{
		"sender": map[string]any{
			"name":  firstNonEmpty(s.cfg.BrevoFromName, siteName),
			"email": s.cfg.BrevoFromEmail,
		},
		"to":          []map[string]any{{"email": toEmail}},
		"subject":     siteName + " verification code: " + code,
		"htmlContent": htmlContent,
		"textContent": textContent,
	}
	rawBody, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewReader(rawBody))
	if err != nil {
		return err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("api-key", s.cfg.BrevoAPIKey)
	req.Header.Set("content-type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Brevo API email send failed: %T", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		bodyText, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("Brevo API email send failed: status=%d body=%s", resp.StatusCode, string(bodyText))
	}
	return nil
}

func (s *Service) sendViaSMTP(toEmail, siteName, code, textContent, htmlContent string) error {
	smtpHost := strings.TrimSpace(s.cfg.SMTPHost)
	smtpUser := strings.TrimSpace(s.cfg.SMTPUser)
	smtpFrom := strings.TrimSpace(s.cfg.SMTPFrom)
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
	body.WriteString(toEmail)
	body.WriteString("\r\n")
	body.WriteString("Content-Type: multipart/alternative; boundary=BOUNDARY\r\n\r\n")
	body.WriteString("--BOUNDARY\r\n")
	body.WriteString("Content-Type: text/plain; charset=UTF-8\r\n\r\n")
	body.WriteString(textContent)
	body.WriteString("--BOUNDARY\r\n")
	body.WriteString("Content-Type: text/html; charset=UTF-8\r\n\r\n")
	body.WriteString(htmlContent)
	body.WriteString("\r\n--BOUNDARY--\r\n")

	addr := fmt.Sprintf("%s:%d", smtpHost, s.cfg.SMTPPort)
	var auth smtp.Auth
	if smtpUser != "" {
		auth = smtp.PlainAuth("", smtpUser, s.cfg.SMTPPass, smtpHost)
	}
	if s.cfg.SMTPTLS {
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
		if err := client.Rcpt(toEmail); err != nil {
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
	return smtp.SendMail(addr, auth, smtpFrom, []string{toEmail}, body.Bytes())
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
