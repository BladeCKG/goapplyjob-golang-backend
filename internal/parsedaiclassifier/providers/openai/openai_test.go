package openai

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

func TestClassifySyncUsesStructuredOutputs(t *testing.T) {
	providercommon.ResetKeyRingForTest("openai")
	classifier := &Classifier{
		Config: Config{
			APIKey:        "test-key",
			Model:         "gpt-4.1-mini",
			BaseURL:       "https://api.openai.com/v1",
			PromptContent: "prompt",
		},
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != "https://api.openai.com/v1/chat/completions" {
					t.Fatalf("unexpected openai url %q", req.URL.String())
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
				responseFormat, _ := payload["response_format"].(map[string]any)
				if responseFormat["type"] != "json_schema" {
					t.Fatalf("expected json_schema response format, got %#v", responseFormat["type"])
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body: io.NopCloser(strings.NewReader(`{
						"choices":[{"message":{"content":"{\"job_category\":\"Software Engineer\",\"required_skills\":[\"Go\"]}"}}]
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
	providercommon.ResetKeyRingForTest("openai")
	requests := make([]string, 0, 3)
	classifier := &Classifier{
		Config: Config{
			APIKeys:       "key-a,key-b",
			Model:         "gpt-4.1-mini",
			Models:        "gpt-4.1-nano",
			BaseURL:       "https://api.openai.com/v1",
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
						"choices":[{"message":{"content":"{\"job_category\":\"Software Engineer\",\"required_skills\":[\"Go\"]}"}}]
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
		"Bearer key-a|gpt-4.1-mini",
		"Bearer key-a|gpt-4.1-nano",
		"Bearer key-b|gpt-4.1-mini",
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
