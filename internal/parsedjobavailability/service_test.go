package parsedjobavailability

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/constants"
	"goapplyjob-golang-backend/internal/database"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func requirePostgresTestDB(t *testing.T) {
	t.Helper()
	if !database.HasTestDatabaseURL() {
		t.Skip("TEST_DATABASE_URL is required for DB-backed tests")
	}
}

func testDatabaseURL(t *testing.T, schemaName string) string {
	t.Helper()
	requirePostgresTestDB(t)
	baseURL := database.TestDatabaseBaseURL()
	adminDB, err := sql.Open("pgx", baseURL)
	if err != nil {
		t.Fatalf("open test postgres connection: %v", err)
	}
	defer adminDB.Close()
	schema := "test_" + strings.ReplaceAll(strings.ToLower(schemaName), "-", "_") + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := adminDB.ExecContext(context.Background(), `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`); err != nil {
		t.Fatalf("create test schema %q: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanupDB, openErr := sql.Open("pgx", baseURL)
		if openErr != nil {
			return
		}
		defer cleanupDB.Close()
		_, _ = cleanupDB.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
	})
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	q := parsedURL.Query()
	q.Set("search_path", schema)
	parsedURL.RawQuery = q.Encode()
	return parsedURL.String()
}

func insertAvailabilityJob(t *testing.T, db *database.DB, parsedJobID, rawJobID int64, source, targetURL string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (?, ?, ?, ?, true, false, true, 0, '{}')`,
		rawJobID,
		source,
		targetURL,
		now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, role_title, updated_at, url)
		 VALUES (?, ?, 'Example Role', ?, ?)`,
		parsedJobID,
		rawJobID,
		now,
		targetURL,
	); err != nil {
		t.Fatal(err)
	}
}

func TestProcessPendingMarksClosedJobsAndAdvancesCheckpoint(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "parsed_job_availability_closed"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	insertAvailabilityJob(t, db, 1, 1, "builtin", "https://example.com/jobs/closed")

	svc := New(Config{WorkerCount: 2}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	svc.ReadHTMLForSource = func(context.Context, string, string) (string, int, error) {
		return "sorry, this job was removed", 200, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 processed row, got %d", processed)
	}

	var deletedAt sql.NullString
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COALESCE(date_deleted::text, '') FROM parsed_jobs WHERE id = 1`).Scan(&deletedAt); err != nil {
		t.Fatal(err)
	}
	if !deletedAt.Valid || deletedAt.String == "" {
		t.Fatal("expected date_deleted to be set for closed job")
	}

	var stateText string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COALESCE(state::text, '') FROM worker_states WHERE worker_name = ?`, constants.WorkerNameParsedAvailability).Scan(&stateText); err != nil {
		t.Fatal(err)
	}
	state := map[string]any{}
	if err := json.Unmarshal([]byte(stateText), &state); err != nil {
		t.Fatal(err)
	}
	if state[workerStateLastCheckedParsedJobIDKey].(float64) != 1 {
		t.Fatalf("expected checkpoint 1, got %#v", state)
	}
}

func TestProcessPendingPreservesOrderedCheckpointingWithParallelWorkers(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "parsed_job_availability_parallel"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	insertAvailabilityJob(t, db, 1, 1, "builtin", "https://example.com/jobs/open")
	insertAvailabilityJob(t, db, 2, 2, "builtin", "https://example.com/jobs/closed")

	svc := New(Config{WorkerCount: 2}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	svc.ReadHTMLForSource = func(_ context.Context, _ string, targetURL string) (string, int, error) {
		if strings.HasSuffix(targetURL, "/open") {
			time.Sleep(50 * time.Millisecond)
			return "<html>open</html>", 200, nil
		}
		return "sorry, this job was removed", 200, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 2 {
		t.Fatalf("expected 2 processed rows, got %d", processed)
	}

	var stateText string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COALESCE(state::text, '') FROM worker_states WHERE worker_name = ?`, constants.WorkerNameParsedAvailability).Scan(&stateText); err != nil {
		t.Fatal(err)
	}
	state := map[string]any{}
	if err := json.Unmarshal([]byte(stateText), &state); err != nil {
		t.Fatal(err)
	}
	if state[workerStateLastCheckedParsedJobIDKey].(float64) != 2 {
		t.Fatalf("expected checkpoint 2, got %#v", state)
	}

	var deletedCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs WHERE date_deleted IS NOT NULL`).Scan(&deletedCount); err != nil {
		t.Fatal(err)
	}
	if deletedCount != 1 {
		t.Fatalf("expected exactly one closed row, got %d", deletedCount)
	}
}

func TestProcessPendingDoesNotAdvanceCheckpointPastFetchFailure(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "parsed_job_availability_failure"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	insertAvailabilityJob(t, db, 1, 1, "builtin", "https://example.com/jobs/fail")
	insertAvailabilityJob(t, db, 2, 2, "builtin", "https://example.com/jobs/open")

	svc := New(Config{WorkerCount: 2}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	svc.ReadHTMLForSource = func(_ context.Context, _ string, targetURL string) (string, int, error) {
		if strings.HasSuffix(targetURL, "/fail") {
			return "", 0, errors.New("fetch failed")
		}
		return "<html>open</html>", 200, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err == nil {
		t.Fatal("expected fetch failure")
	}
	if processed != 0 {
		t.Fatalf("expected zero checkpointed rows, got %d", processed)
	}

	var count int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_states WHERE worker_name = ?`, constants.WorkerNameParsedAvailability).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no checkpoint row, got %d", count)
	}
}

func TestProcessPendingResetsCheckpointAfterReachingEnd(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "parsed_job_availability_reset"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	insertAvailabilityJob(t, db, 1, 1, "builtin", "https://example.com/jobs/open")

	svc := New(Config{WorkerCount: 1}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	svc.ReadHTMLForSource = func(context.Context, string, string) (string, int, error) {
		return "<html>open</html>", 200, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 processed row, got %d", processed)
	}

	processed, err = svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("expected 0 processed rows after reaching end, got %d", processed)
	}

	var stateText string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COALESCE(state::text, '') FROM worker_states WHERE worker_name = ?`, constants.WorkerNameParsedAvailability).Scan(&stateText); err != nil {
		t.Fatal(err)
	}
	state := map[string]any{}
	if err := json.Unmarshal([]byte(stateText), &state); err != nil {
		t.Fatal(err)
	}
	if state[workerStateLastCheckedParsedJobIDKey].(float64) != 0 {
		t.Fatalf("expected checkpoint reset to 0, got %#v", state)
	}
}
