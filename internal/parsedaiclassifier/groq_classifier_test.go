package parsedaiclassifier

import (
	"context"
	"encoding/json"
	"errors"
	"goapplyjob-golang-backend/internal/database"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestCleanGroqDescriptionStripsHTML(t *testing.T) {
	raw := "  <p>Hello&nbsp;<b>World</b></p>\n<div>Line&nbsp;Two</div> "
	cleaned := cleanGroqDescription(raw)
	if cleaned != "Hello World Line Two" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanGroqDescriptionHandlesEmpty(t *testing.T) {
	if cleanGroqDescription("") != "" {
		t.Fatalf("expected empty result for empty input")
	}
}

func TestCleanGroqDescriptionCollapsesWhitespace(t *testing.T) {
	raw := "Line one\n\n\tLine two   Line   three"
	cleaned := cleanGroqDescription(raw)
	if cleaned != "Line one Line two Line three" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanGroqDescriptionUnescapesEntities(t *testing.T) {
	raw := "AT&amp;T &lt;span&gt;Rock&amp;Roll&lt;/span&gt; &quot;Quoted&quot;"
	cleaned := cleanGroqDescription(raw)
	if cleaned != "AT&T Rock&Roll \"Quoted\"" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanGroqDescriptionStripsNestedTags(t *testing.T) {
	raw := "<div><h2>Role</h2><ul><li>Item 1</li><li>Item 2</li></ul></div>"
	cleaned := cleanGroqDescription(raw)
	if cleaned != "Role Item 1 Item 2" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestLoadAllowedJobCategoriesAndFunctionsForGroq(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_groq_categories"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES
		  (1, 'builtin', 'https://example.com/jobs/1', NOW(), true, false, true, 0, '{}'),
		  (2, 'builtin', 'https://example.com/jobs/2', NOW(), true, false, true, 0, '{}'),
		  (3, 'builtin', 'https://example.com/jobs/3', NOW(), true, false, true, 0, '{}'),
		  (4, 'builtin', 'https://example.com/jobs/4', NOW(), true, false, true, 0, '{}'),
		  (5, 'builtin', 'https://example.com/jobs/5', NOW(), true, false, true, 0, '{}'),
		  (6, 'builtin', 'https://example.com/jobs/6', NOW(), true, false, true, 0, '{}'),
		  (7, 'builtin', 'https://example.com/jobs/7', NOW(), true, false, true, 0, '{}')`,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, updated_at)
		 VALUES
		  (1, 'Data Analyst', 'Data Analyst', 'Analytics', NOW()),
		  (2, 'Data Analyst', 'Data Analyst', 'Analytics', NOW()),
		  (3, 'Data Analyst', 'Data Analyst', 'Business', NOW()),
		  (4, 'Engineer', 'Software Engineer', 'Engineering', NOW()),
		  (5, 'Ignore Title', '', 'Engineering', NOW()),
		  (6, 'Ignore Function', 'Product Manager', NULL, NOW()),
		  (7, 'Ignore Null Function', 'Product Manager', NULL, NOW())`,
	)
	if err != nil {
		t.Fatal(err)
	}

	groqCategoryCache.mu.Lock()
	groqCategoryCache.items = nil
	groqCategoryCache.functions = nil
	groqCategoryCache.mu.Unlock()

	svc := New(Config{}, db)
	categories, functions, err := svc.loadAllowedJobCategoriesAndFunctions(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !containsCaseSensitive(categories, "Data Analyst") {
		t.Fatalf("expected Data Analyst category in %v", categories)
	}
	if !containsCaseSensitive(categories, "Software Engineer") {
		t.Fatalf("expected Software Engineer category in %v", categories)
	}
	if !containsCaseSensitive(categories, "Blank") {
		t.Fatalf("expected Blank category in %v", categories)
	}
	if _, ok := functions["Data Analyst"]; !ok || functions["Data Analyst"] != "Analytics" {
		t.Fatalf("expected function Analytics for Data Analyst, got %q", functions["Data Analyst"])
	}
	if _, ok := functions["Software Engineer"]; !ok || functions["Software Engineer"] != "Engineering" {
		t.Fatalf("expected function Engineering for Software Engineer, got %q", functions["Software Engineer"])
	}
	if _, ok := functions["Product Manager"]; ok {
		t.Fatalf("did not expect function mapping for Product Manager when function is empty")
	}
}

func TestClassifySyncRotatesModelOn503(t *testing.T) {
	t.Setenv(envGroqAPIKey, "test-key")
	t.Setenv(envGroqModel, "openai/gpt-oss-20b")

	requestModels := make([]string, 0, 2)
	models := collectGroqModels()
	if len(models) < 2 {
		t.Fatalf("expected at least two configured Groq models, got %d", len(models))
	}

	classifier := &GroqJobClassifier{
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

	category, skills, err := classifier.classifySync("Software Engineer", "Build services", []string{"Software Engineer", "Blank"})
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
	t.Setenv(envGroqAPIKey, "")
	t.Setenv(envGroqAPIKeys, "")

	category, skills, err := defaultGroqClassifier.classifySync("Software Engineer", "Build services", []string{"Software Engineer", "Blank"})
	if !errors.Is(err, errGroqAPIKeysNotConfigured) {
		t.Fatalf("expected missing-api-keys error, got %v", err)
	}
	if category != "" {
		t.Fatalf("expected empty category, got %q", category)
	}
	if skills != nil {
		t.Fatalf("expected nil skills, got %#v", skills)
	}
}
