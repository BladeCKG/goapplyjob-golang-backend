package parsedaiclassifier

import (
	"context"
	"encoding/json"
	"goapplyjob-golang-backend/internal/constants"
	"goapplyjob-golang-backend/internal/database"
	"testing"
	"time"
)

func TestProcessPendingSkipsRowsThatShouldNotBeAIClassified(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_ai_skip"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES
		 (1, 'remoterocketship', 'https://example.com/jobs/1', ?, true, false, true, 0, '{}'),
		 (2, 'remoterocketship', 'https://example.com/jobs/2', ?, true, false, true, 0, '{}')`,
		now,
		now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, role_title, tech_stack, updated_at)
		 VALUES
		 (1, 1, 'Backend Engineer', '["Go"]', ?),
		 (2, 2, 'Frontend Engineer', NULL, ?)`,
		now,
		now,
	); err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{
		"remoterocketship": {},
	}
	svc.Classify = func(context.Context, string, string, string) (string, string, []string, error) {
		t.Fatal("classify should not be called for skipped rows")
		return "", "", nil, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 2 {
		t.Fatalf("expected 2 rows processed, got %d", processed)
	}

	var stateText string
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT COALESCE(state::text, '') FROM worker_states WHERE worker_name = ?`,
		constants.WorkerNameParsedAIClassifier,
	).Scan(&stateText); err != nil {
		t.Fatal(err)
	}
	state := map[string]any{}
	if err := json.Unmarshal([]byte(stateText), &state); err != nil {
		t.Fatal(err)
	}
	lastParsedJobID, _ := state[workerStateLastClassifiedParsedJobIDKey].(float64)
	if lastParsedJobID != 2 {
		t.Fatalf("expected worker checkpoint at parsed_job_id 2, got %v", lastParsedJobID)
	}
}

func TestProcessPendingClassifiesEligibleParsedJobs(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_ai_classify"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'builtin', 'https://example.com/jobs/1', ?, true, false, true, 0, '{}')`,
		now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, role_title, role_description, role_requirements, updated_at)
		 VALUES (1, 1, 'Backend Engineer', 'Build APIs', 'Go, PostgreSQL', ?)`,
		now,
	); err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{
		"builtin": {},
	}
	svc.Classify = func(_ context.Context, roleRequirements, roleTitle, roleDescription string) (string, string, []string, error) {
		if roleTitle != "Backend Engineer" {
			t.Fatalf("unexpected role title %q", roleTitle)
		}
		if roleDescription != "Build APIs" {
			t.Fatalf("unexpected role description %q", roleDescription)
		}
		if roleRequirements != "Go, PostgreSQL" {
			t.Fatalf("unexpected role requirements %q", roleRequirements)
		}
		return "Software Engineer", "Engineering", []string{"Go", "PostgreSQL"}, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 row processed, got %d", processed)
	}

	var categorizedTitle, categorizedFunction, techStack string
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT COALESCE(categorized_job_title, ''), COALESCE(categorized_job_function, ''), COALESCE(tech_stack::text, '')
		   FROM parsed_jobs
		  WHERE id = 1`,
	).Scan(&categorizedTitle, &categorizedFunction, &techStack); err != nil {
		t.Fatal(err)
	}
	if categorizedTitle != "Software Engineer" {
		t.Fatalf("expected category to be updated, got %q", categorizedTitle)
	}
	if categorizedFunction != "Engineering" {
		t.Fatalf("expected function to be updated, got %q", categorizedFunction)
	}
	if techStack != `["Go", "PostgreSQL"]` && techStack != `["Go","PostgreSQL"]` {
		t.Fatalf("expected tech stack JSON to be updated, got %q", techStack)
	}
}

func TestProcessPendingClassifiesEvenWhenCategorizedTitleAlreadySet(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_ai_recategorize"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'builtin', 'https://example.com/jobs/1', ?, true, false, true, 0, '{}')`,
		now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, role_title, role_description, categorized_job_title, updated_at)
		 VALUES (1, 1, 'Backend Engineer', 'Build APIs', 'Old Category', ?)`,
		now,
	); err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{
		"builtin": {},
	}
	svc.Classify = func(_ context.Context, roleRequirements, roleTitle, roleDescription string) (string, string, []string, error) {
		if roleRequirements != "" {
			t.Fatalf("unexpected role requirements %q", roleRequirements)
		}
		if roleTitle != "Backend Engineer" {
			t.Fatalf("unexpected role title %q", roleTitle)
		}
		if roleDescription != "Build APIs" {
			t.Fatalf("unexpected role description %q", roleDescription)
		}
		return "Software Engineer", "Engineering", []string{"Go"}, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 row processed, got %d", processed)
	}

	var categorizedTitle string
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT COALESCE(categorized_job_title, '') FROM parsed_jobs WHERE id = 1`,
	).Scan(&categorizedTitle); err != nil {
		t.Fatal(err)
	}
	if categorizedTitle != "Software Engineer" {
		t.Fatalf("expected category to be replaced, got %q", categorizedTitle)
	}
}

func TestProcessPendingDoesNotAdvanceCheckpointWhenClassificationFails(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_ai_fail_checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES
		 (1, 'builtin', 'https://example.com/jobs/1', ?, true, false, true, 0, '{}'),
		 (2, 'builtin', 'https://example.com/jobs/2', ?, true, false, true, 0, '{}')`,
		now,
		now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, role_title, updated_at)
		 VALUES
		 (1, 1, 'Backend Engineer', ?),
		 (2, 2, 'Data Engineer', ?)`,
		now,
		now,
	); err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{
		"builtin": {},
	}
	svc.Classify = func(_ context.Context, _, roleTitle, _ string) (string, string, []string, error) {
		if roleTitle == "Backend Engineer" {
			return "", "", nil, context.DeadlineExceeded
		}
		return "Data Engineer", "Engineering", []string{"SQL"}, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err == nil {
		t.Fatal("expected classification failure")
	}
	if processed != 0 {
		t.Fatalf("expected no checkpointed rows, got %d", processed)
	}

	var count int
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM worker_states WHERE worker_name = ?`,
		constants.WorkerNameParsedAIClassifier,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no worker state checkpoint on failure, got %d rows", count)
	}
}

func TestProcessPendingAdvancesCheckpointWhenClassificationReturnsEmptyCategory(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_ai_empty_category"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'builtin', 'https://example.com/jobs/1', ?, true, false, true, 0, '{}')`,
		now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, role_title, updated_at)
		 VALUES (1, 1, 'Extraordinary Builder', ?)`,
		now,
	); err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{
		"builtin": {},
	}
	svc.Classify = func(_ context.Context, _, roleTitle, _ string) (string, string, []string, error) {
		if roleTitle != "Extraordinary Builder" {
			t.Fatalf("unexpected role title %q", roleTitle)
		}
		return "", "", nil, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 row processed, got %d", processed)
	}

	var categorizedTitle, categorizedFunction, techStack string
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT COALESCE(categorized_job_title, ''),
		        COALESCE(categorized_job_function, ''),
		        COALESCE(tech_stack::text, '')
		   FROM parsed_jobs
		  WHERE id = 1`,
	).Scan(&categorizedTitle, &categorizedFunction, &techStack); err != nil {
		t.Fatal(err)
	}
	if categorizedTitle != "" {
		t.Fatalf("expected empty categorized title, got %q", categorizedTitle)
	}
	if categorizedFunction != "" {
		t.Fatalf("expected empty categorized function, got %q", categorizedFunction)
	}
	if techStack != "" {
		t.Fatalf("expected empty tech stack, got %q", techStack)
	}

	var stateText string
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT COALESCE(state::text, '') FROM worker_states WHERE worker_name = ?`,
		constants.WorkerNameParsedAIClassifier,
	).Scan(&stateText); err != nil {
		t.Fatal(err)
	}
	state := map[string]any{}
	if err := json.Unmarshal([]byte(stateText), &state); err != nil {
		t.Fatal(err)
	}
	lastParsedJobID, _ := state[workerStateLastClassifiedParsedJobIDKey].(float64)
	if lastParsedJobID != 1 {
		t.Fatalf("expected worker checkpoint at parsed_job_id 1, got %v", lastParsedJobID)
	}
}

func TestProcessPendingDoesNotFetchDisabledSources(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_ai_disabled_sources"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'builtin', 'https://example.com/jobs/1', ?, true, false, true, 0, '{}')`,
		now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, role_title, updated_at)
		 VALUES (1, 1, 'Backend Engineer', ?)`,
		now,
	); err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"remoterocketship": {}}
	svc.Classify = func(context.Context, string, string, string) (string, string, []string, error) {
		t.Fatal("classify should not be called for disabled sources")
		return "", "", nil, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("expected disabled-source row to be ignored at fetch time, got %d", processed)
	}

	var count int
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM worker_states WHERE worker_name = ?`,
		constants.WorkerNameParsedAIClassifier,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no worker state for disabled-source fetch skip, got %d rows", count)
	}
}
