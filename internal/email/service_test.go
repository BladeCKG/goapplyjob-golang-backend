package email

import (
	"encoding/json"
	"goapplyjob-golang-backend/internal/config"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

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

func TestSendEmailBatchViaBrevoUsesPerRecipientMessageVersions(t *testing.T) {
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageIds":["one","two"]}`))
	}))
	defer server.Close()

	svc := NewService(config.Config{
		EmailProvider:   "brevo",
		BrevoAPIKey:     "sk_test_123",
		BrevoFromEmail:  "hello@example.com",
		BrevoFromName:   "GoApplyJob",
		BrevoAPIURL:     server.URL,
	})

	result, err := svc.SendEmailBatchDetailed([]string{"first@example.com", "second@example.com"}, "Hello", "Plain text", "<p>Hello</p>")
	if err != nil {
		t.Fatal(err)
	}
	if result.Provider != "brevo" {
		t.Fatalf("unexpected provider %q", result.Provider)
	}
	if len(result.Items) != 2 {
		t.Fatalf("unexpected result items %#v", result.Items)
	}

	if _, hasOuterTo := gotBody["to"]; hasOuterTo {
		t.Fatalf("did not expect outer to field in batch payload")
	}
	if gotBody["subject"] != "Hello" {
		t.Fatalf("unexpected outer subject %#v", gotBody["subject"])
	}
	if gotBody["htmlContent"] != "<p>Hello</p>" {
		t.Fatalf("unexpected outer htmlContent %#v", gotBody["htmlContent"])
	}
	if gotBody["textContent"] != "Plain text" {
		t.Fatalf("unexpected outer textContent %#v", gotBody["textContent"])
	}

	messageVersions, ok := gotBody["messageVersions"].([]any)
	if !ok || len(messageVersions) != 2 {
		t.Fatalf("unexpected messageVersions %#v", gotBody["messageVersions"])
	}

	firstVersion, ok := messageVersions[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected first message version %#v", messageVersions[0])
	}

	recipients, ok := firstVersion["to"].([]any)
	if !ok || len(recipients) != 1 {
		t.Fatalf("unexpected recipients %#v", firstVersion["to"])
	}
	if _, ok := firstVersion["subject"]; ok {
		t.Fatalf("did not expect per-version subject %#v", firstVersion["subject"])
	}
	if _, ok := firstVersion["htmlContent"]; ok {
		t.Fatalf("did not expect per-version htmlContent %#v", firstVersion["htmlContent"])
	}
}

func TestSendEmailBatchViaBrevoAcceptsSingularMessageIDForSingleVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"messageId":"one"}`))
	}))
	defer server.Close()

	svc := NewService(config.Config{
		EmailProvider:  "brevo",
		BrevoAPIKey:    "sk_test_123",
		BrevoFromEmail: "hello@example.com",
		BrevoFromName:  "GoApplyJob",
		BrevoAPIURL:    server.URL,
	})

	result, err := svc.SendEmailBatchDetailed([]string{"first@example.com"}, "Hello", "Plain text", "<p>Hello</p>")
	if err != nil {
		t.Fatal(err)
	}
	if result.Provider != "brevo" {
		t.Fatalf("unexpected provider %q", result.Provider)
	}
	if result.Status != "accepted" {
		t.Fatalf("unexpected status %q", result.Status)
	}
	if len(result.Items) != 1 {
		t.Fatalf("unexpected result items %#v", result.Items)
	}
	if result.Items[0].Status != "accepted" || result.Items[0].MessageID != "one" {
		t.Fatalf("unexpected item %#v", result.Items[0])
	}
}

func TestSendEmailBatchDetailedViaMailtrapKeepsPartialRecipientResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"success": true,
			"responses": [
				{"success": true, "message_ids": ["id-1"], "errors": []},
				{"success": true, "message_ids": ["id-2"], "errors": []}
			],
			"errors": ["quota reached"]
		}`))
	}))
	defer server.Close()

	svc := NewService(config.Config{
		EmailProvider:      "mailtrap",
		MailtrapAPIToken:   "sk_test_123",
		MailtrapFromEmail:  "hello@example.com",
		MailtrapFromName:   "GoApplyJob",
	})
	svc.httpClient = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			req.URL.Scheme = "http"
			req.URL.Host = server.Listener.Addr().String()
			return server.Client().Transport.RoundTrip(req)
		}),
	}

	result, err := svc.SendEmailBatchDetailed(
		[]string{"first@example.com", "second@example.com", "third@example.com"},
		"Hello",
		"Plain text",
		"<p>Hello</p>",
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Provider != "mailtrap" {
		t.Fatalf("unexpected provider %q", result.Provider)
	}
	if result.Status != "partial" {
		t.Fatalf("unexpected status %q", result.Status)
	}
	if len(result.Items) != 3 {
		t.Fatalf("unexpected items %#v", result.Items)
	}
	if result.Items[0].Status != "sent" || result.Items[1].Status != "sent" {
		t.Fatalf("unexpected sent items %#v", result.Items)
	}
	if result.Items[2].Status != "error" {
		t.Fatalf("unexpected third item %#v", result.Items[2])
	}
}

func TestSendEmailBatchDetailedViaCyberPanelKeepsPartialRecipientResults(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts <= 2 {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"success":true,"data":{"status":"sent"}}`))
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"success":false,"error":{"code":"rate_limit_exceeded"}}`))
	}))
	defer server.Close()

	svc := NewService(config.Config{
		EmailProvider:       "cyberpanel",
		CyberPanelAPIKey:    "sk_test_123",
		CyberPanelFromEmail: "hello@example.com",
		CyberPanelAPIURL:    server.URL,
	})

	result, err := svc.SendEmailBatchDetailed(
		[]string{"first@example.com", "second@example.com", "third@example.com"},
		"Hello",
		"Plain text",
		"<p>Hello</p>",
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Provider != "cyberpanel" {
		t.Fatalf("unexpected provider %q", result.Provider)
	}
	if result.Status != "partial" {
		t.Fatalf("unexpected status %q", result.Status)
	}
	if len(result.Items) != 3 {
		t.Fatalf("unexpected items %#v", result.Items)
	}
	if result.Items[0].Status != "sent" || result.Items[1].Status != "sent" {
		t.Fatalf("unexpected sent items %#v", result.Items)
	}
	if result.Items[2].Status != "error" {
		t.Fatalf("unexpected third item %#v", result.Items[2])
	}
}
