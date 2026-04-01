package groq

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

func TestClassifySyncRotatesModelOn503(t *testing.T) {
	providercommon.ResetKeyRingForTest("groq")
	requestModels := make([]string, 0, 2)
	cfg := Config{APIKey: "test-key", Model: "openai/gpt-oss-20b", PromptContent: "prompt"}
	models := CollectModels(cfg)
	if len(models) < 2 {
		t.Fatalf("expected at least two configured Groq models, got %d", len(models))
	}

	classifier := &Classifier{
		Config: cfg,
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
				requestModels = append(requestModels, model)
				if model == models[0] {
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
		t.Fatalf("expected rotated model to classify category, got %q", category)
	}
	if len(skills) != 1 || skills[0] != "Go" {
		t.Fatalf("expected rotated model to return skills, got %#v", skills)
	}
	if len(requestModels) < 2 {
		t.Fatalf("expected at least two model attempts, got %d", len(requestModels))
	}
	if requestModels[0] != models[0] {
		t.Fatalf("expected first attempt to use primary model %q, got %q", models[0], requestModels[0])
	}
	if requestModels[1] != models[1] {
		t.Fatalf("expected second attempt to rotate to %q, got %q", models[1], requestModels[1])
	}
}

func TestClassifySyncReturnsErrorWhenNoAPIKeysConfigured(t *testing.T) {
	category, skills, err := (&Classifier{}).ClassifySync(context.Background(), "Software Engineer", "Build services", []string{"Software Engineer", "Blank"})
	if !errors.Is(err, ErrAPIKeysNotConfigured) {
		t.Fatalf("expected missing-api-keys error, got %v", err)
	}
	if category != "" {
		t.Fatalf("expected empty category, got %q", category)
	}
	if skills != nil {
		t.Fatalf("expected nil skills, got %#v", skills)
	}
}
