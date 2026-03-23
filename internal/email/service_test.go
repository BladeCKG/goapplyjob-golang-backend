package email

import (
	"encoding/json"
	"goapplyjob-golang-backend/internal/config"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNormalizeProviderSupportsCyberPanel(t *testing.T) {
	svc := NewService(config.Config{EmailProvider: "cyberpanel"})
	if got := svc.normalizeProvider(); got != "cyberpanel" {
		t.Fatalf("expected cyberpanel provider, got %q", got)
	}
}

func TestResolveProvidersIncludesCyberPanelWhenConfigured(t *testing.T) {
	svc := NewService(config.Config{
		EmailProvider:       "auto",
		CyberPanelAPIKey:    "sk_test_123",
		CyberPanelFromEmail: "hello@example.com",
	})
	providers := svc.resolveProviders()
	if len(providers) == 0 || providers[0] != "cyberpanel" {
		t.Fatalf("expected cyberpanel provider in auto mode, got %#v", providers)
	}
}

func TestSendViaCyberPanelUsesBearerAuthAndExpectedPayload(t *testing.T) {
	var gotAuth string
	var gotContentType string
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()

	svc := NewService(config.Config{
		EmailProvider:       "cyberpanel",
		CyberPanelAPIKey:    "sk_test_123",
		CyberPanelFromEmail: "hello@example.com",
		CyberPanelAPIURL:    server.URL,
	})

	if err := svc.SendEmail("user@example.com", "Hello", "Plain text", "<p>Hello</p>"); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer sk_test_123" {
		t.Fatalf("unexpected auth header %q", gotAuth)
	}
	if gotContentType != "application/json" {
		t.Fatalf("unexpected content type %q", gotContentType)
	}
	if gotBody["from"] != "hello@example.com" {
		t.Fatalf("unexpected from field %#v", gotBody["from"])
	}
	if gotBody["to"] != "user@example.com" {
		t.Fatalf("unexpected to field %#v", gotBody["to"])
	}
	if gotBody["subject"] != "Hello" {
		t.Fatalf("unexpected subject %#v", gotBody["subject"])
	}
	if gotBody["html"] != "<p>Hello</p>" {
		t.Fatalf("unexpected html %#v", gotBody["html"])
	}
	if gotBody["text"] != "Plain text" {
		t.Fatalf("unexpected text %#v", gotBody["text"])
	}
}
