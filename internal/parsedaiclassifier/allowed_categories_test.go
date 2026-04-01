package parsedaiclassifier

import (
	"context"
	"goapplyjob-golang-backend/internal/database"
	"testing"
)

func TestLoadAllowedJobCategoriesAndFunctions(t *testing.T) {
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

	categoryCache.mu.Lock()
	categoryCache.items = nil
	categoryCache.functions = nil
	categoryCache.mu.Unlock()

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
