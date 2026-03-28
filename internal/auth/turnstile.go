package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
)

type turnstileVerifyResponse struct {
	Success    bool     `json:"success"`
	ErrorCodes []string `json:"error-codes"`
	Action     string   `json:"action"`
}

func (h *Handler) verifyTurnstileToken(c *gin.Context, token string, expectedAction string) error {
	secret := strings.TrimSpace(h.cfg.AuthTurnstileSecretKey)
	if secret == "" {
		return nil
	}
	trimmedToken := strings.TrimSpace(token)
	if trimmedToken == "" {
		return errors.New("Please complete human verification")
	}

	form := url.Values{}
	form.Set("secret", secret)
	form.Set("response", trimmedToken)
	remoteIP := strings.TrimSpace(c.ClientIP())
	if remoteIP != "" {
		form.Set("remoteip", remoteIP)
	}

	req, err := http.NewRequest(http.MethodPost, h.cfg.AuthTurnstileVerifyURL, strings.NewReader(form.Encode()))
	if err != nil {
		return errors.New("Human verification failed")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return errors.New("Human verification failed")
	}
	defer resp.Body.Close()

	var payload turnstileVerifyResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return errors.New("Human verification failed")
	}
	if !payload.Success {
		return errors.New("Please complete human verification")
	}
	if expectedAction != "" && payload.Action != "" && payload.Action != expectedAction {
		return errors.New("Human verification failed")
	}
	return nil
}
