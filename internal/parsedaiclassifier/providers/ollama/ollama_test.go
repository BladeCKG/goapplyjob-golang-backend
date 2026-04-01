package ollama

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	providercommon "goapplyjob-golang-backend/internal/parsedaiclassifier/providers/common"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestClassifySyncUsesHostedServer(t *testing.T) {
	providercommon.ResetKeyRingForTest("ollama")
	classifier := &Classifier{
		Config: Config{
			BaseURL:       "https://ollama.example.com",
			Model:         "llama3.1:70b",
			APIKey:        "test-key",
			PromptContent: "prompt",
		},
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != "https://ollama.example.com/v1/chat/completions" {
					t.Fatalf("unexpected ollama url %q", req.URL.String())
				}
				if got := req.Header.Get("Authorization"); got != "Bearer test-key" {
					t.Fatalf("unexpected auth header %q", got)
				}
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				var payload map[string]any
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatal(err)
				}
				if payload["model"] != "llama3.1:70b" {
					t.Fatalf("unexpected model %#v", payload["model"])
				}
				if payload["stream"] != false {
					t.Fatalf("expected stream=false, got %#v", payload["stream"])
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body: io.NopCloser(strings.NewReader(`{
						"message":{"content":"{\"job_category\":\"Software Engineer\",\"required_skills\":[\"Go\"]}"}
					}`)),
					Header: make(http.Header),
				}, nil
			}),
		},
	}

	category, skills, err := classifier.ClassifySync(context.Background(), "Software Engineer", "Build services", []string{"Software Engineer", "Blank"})
	if err != nil {
		t.Fatal(err)
	}
	if category != "Software Engineer" {
		t.Fatalf("expected category, got %q", category)
	}
	if len(skills) != 1 || skills[0] != "Go" {
		t.Fatalf("expected required skills, got %#v", skills)
	}
}

func TestClassifyCategoryOnlySyncReturnsErrorWhenNotConfigured(t *testing.T) {
	category, err := (&Classifier{}).ClassifyCategoryOnlySync(context.Background(), "Software Engineer", []string{"Software Engineer", "Blank"})
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("expected missing-config error, got %v", err)
	}
	if category != "" {
		t.Fatalf("expected empty category, got %q", category)
	}
}

func TestClassifySyncRotatesModelsThenAPIKeys(t *testing.T) {
	providercommon.ResetKeyRingForTest("ollama")
	requests := make([]string, 0, 3)
	classifier := &Classifier{
		Config: Config{
			BaseURL:       "https://ollama.example.com",
			Model:         "llama3.1:70b",
			Models:        "llama3.1:8b",
			APIKeys:       "key-a,key-b",
			PromptContent: "prompt",
		},
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				var payload map[string]any
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatal(err)
				}
				model, _ := payload["model"].(string)
				auth := req.Header.Get("Authorization")
				requests = append(requests, auth+"|"+model)
				if auth == "Bearer key-a" {
					return &http.Response{
						StatusCode: http.StatusServiceUnavailable,
						Body:       io.NopCloser(strings.NewReader(`{"error":"temporarily unavailable"}`)),
						Header:     make(http.Header),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body: io.NopCloser(strings.NewReader(`{
						"message":{"content":"{\"job_category\":\"Software Engineer\",\"required_skills\":[\"Go\"]}"}
					}`)),
					Header: make(http.Header),
				}, nil
			}),
		},
	}

	category, skills, err := classifier.ClassifySync(context.Background(), "Software Engineer", "Build services", []string{"Software Engineer", "Blank"})
	if err != nil {
		t.Fatal(err)
	}
	if category != "Software Engineer" {
		t.Fatalf("expected category, got %q", category)
	}
	if len(skills) != 1 || skills[0] != "Go" {
		t.Fatalf("expected required skills, got %#v", skills)
	}
	expected := []string{
		"Bearer key-a|llama3.1:70b",
		"Bearer key-a|llama3.1:8b",
		"Bearer key-b|llama3.1:70b",
	}
	if len(requests) != len(expected) {
		t.Fatalf("expected requests %#v, got %#v", expected, requests)
	}
	for i := range expected {
		if requests[i] != expected[i] {
			t.Fatalf("expected request %d to be %q, got %q", i, expected[i], requests[i])
		}
	}
}
