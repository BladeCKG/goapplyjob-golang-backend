package raw

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"goapplyjob-golang-backend/internal/database"
)

func TestToTargetJobURLRemovesCountryCodeBeforeCompany(t *testing.T) {
	rawURL := "https://www.remoterocketship.com/us/company/premierinc/jobs/account-support-manager-united-states-remote/"
	expected := "https://www.remoterocketship.com/company/premierinc/jobs/account-support-manager-united-states-remote/"
	if toTargetJobURL(rawURL) != expected {
		t.Fatalf("expected normalized URL %s", expected)
	}
}

func TestToTargetJobURLKeepsAlreadyTargetURL(t *testing.T) {
	rawURL := "https://www.remoterocketship.com/company/premierinc/jobs/account-support-manager-united-states-remote/"
	if toTargetJobURL(rawURL) != rawURL {
		t.Fatalf("expected URL to stay unchanged")
	}
}

func TestToTargetJobURLPreservesQueryAndFragment(t *testing.T) {
	rawURL := "https://www.remoterocketship.com/us/company/acme/jobs/dev/?x=1#top"
	expected := "https://www.remoterocketship.com/company/acme/jobs/dev/?x=1#top"
	if toTargetJobURL(rawURL) != expected {
		t.Fatalf("expected normalized URL %s", expected)
	}
}

func TestToTargetJobURLForSourceBuiltinKeepsURLUnchanged(t *testing.T) {
	rawURL := "https://builtin.com/job/u-s-senior-staff-product-designer/8511517"
	if toTargetJobURLForSource("builtin", rawURL) != rawURL {
		t.Fatalf("expected builtin URL to remain unchanged")
	}
}

func TestIsRemovedBuiltinJobHTMLDetectsRemovedMarkerCaseInsensitive(t *testing.T) {
	html := "<html><body><span>Sorry, this job was removed at 05:05 p.m.</span></body></html>"
	if !isRemovedBuiltinJobHTML("builtin", html) {
		t.Fatalf("expected builtin removed marker to be detected")
	}
}

func TestIsRemovedBuiltinJobHTMLIgnoresNonBuiltinSources(t *testing.T) {
	html := "<html><body><span>Sorry, this job was removed.</span></body></html>"
	if isRemovedBuiltinJobHTML("workable", html) {
		t.Fatalf("expected non-builtin source to be ignored")
	}
}

func TestIsRemovedBuiltinJobHTMLFalseWhenTextMissing(t *testing.T) {
	html := "<html><body><span>Job still active</span></body></html>"
	if isRemovedBuiltinJobHTML("builtin", html) {
		t.Fatalf("expected false when removed marker is missing")
	}
}

func TestProcessPendingSkipsRemainingSourceJobsAfter429(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_raw_429_throttle"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES
		('remoterocketship', 'https://remote.example/job/1', '2026-02-12T10:00:00Z', false, false, false, 0, NULL),
		('remoterocketship', 'https://remote.example/job/2', '2026-02-12T09:00:00Z', false, false, false, 0, NULL)`)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"remoterocketship": {}}
	fetchCount := 0
	svc.ReadHTML = func(ctx context.Context, targetURL string) (string, int, error) {
		fetchCount++
		return "", 429, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 2 {
		t.Fatalf("expected two processed jobs, got %d", processed)
	}
	if fetchCount != 1 {
		t.Fatalf("expected one fetch call due source throttling, got %d", fetchCount)
	}

	rows, err := db.SQL.QueryContext(context.Background(), `SELECT retry_count, is_ready FROM raw_us_jobs WHERE source='remoterocketship' ORDER BY id ASC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var retryCount int
		var isReady bool
		if err := rows.Scan(&retryCount, &isReady); err != nil {
			t.Fatal(err)
		}
		if retryCount != 1 || isReady {
			t.Fatalf("unexpected row state retry_count=%d is_ready=%t", retryCount, isReady)
		}
	}
}

func TestProcessPendingMarks404AsTerminalSkip(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_raw_404_terminal_skip"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, extra_json, raw_json)
		 VALUES ('remoterocketship', 'https://remote.example/job/404', '2026-02-12T10:00:00Z', false, false, true, 0, '{"foo":"bar"}', '{"old":"payload"}')`,
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"remoterocketship": {}}
	svc.ReadHTML = func(ctx context.Context, targetURL string) (string, int, error) {
		return "", 404, nil
	}
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed job, got %d", processed)
	}

	var isReady, isSkippable, isParsed bool
	var retryCount int
	var rawJSON, extraJSON sql.NullString
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT is_ready, is_skippable, is_parsed, retry_count, raw_json, extra_json
		 FROM raw_us_jobs WHERE url = 'https://remote.example/job/404'`,
	).Scan(&isReady, &isSkippable, &isParsed, &retryCount, &rawJSON, &extraJSON); err != nil {
		t.Fatal(err)
	}
	if !isReady || !isSkippable || isParsed {
		t.Fatalf("unexpected state is_ready=%t is_skippable=%t is_parsed=%t", isReady, isSkippable, isParsed)
	}
	if retryCount != 0 {
		t.Fatalf("expected retry_count unchanged, got %d", retryCount)
	}
	if rawJSON.Valid || extraJSON.Valid {
		t.Fatalf("expected raw_json/extra_json cleared, got raw_json=%#v extra_json=%#v", rawJSON, extraJSON)
	}
}

func TestProcessPendingMarksDailyRemote410GoneAsTerminalSkip(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_raw_dailyremote_410_terminal_skip"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, extra_json, raw_json)
		 VALUES ('dailyremote', 'https://dailyremote.com/remote-job/test-410', '2026-02-12T10:00:00Z', false, false, true, 0, '{"foo":"bar"}', '{"old":"payload"}')`,
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"dailyremote": {}}
	svc.ReadHTML = func(ctx context.Context, targetURL string) (string, int, error) {
		_ = targetURL
		return "<html><body>Job No Longer Available</body></html>", 410, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed job, got %d", processed)
	}

	var isReady, isSkippable, isParsed bool
	var retryCount int
	var rawJSON, extraJSON sql.NullString
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT is_ready, is_skippable, is_parsed, retry_count, raw_json, extra_json
		 FROM raw_us_jobs WHERE url = 'https://dailyremote.com/remote-job/test-410'`,
	).Scan(&isReady, &isSkippable, &isParsed, &retryCount, &rawJSON, &extraJSON); err != nil {
		t.Fatal(err)
	}
	if !isReady || !isSkippable || isParsed {
		t.Fatalf("unexpected state is_ready=%t is_skippable=%t is_parsed=%t", isReady, isSkippable, isParsed)
	}
	if retryCount != 0 {
		t.Fatalf("expected retry_count unchanged, got %d", retryCount)
	}
	if rawJSON.Valid || extraJSON.Valid {
		t.Fatalf("expected raw_json/extra_json cleared, got raw_json=%#v extra_json=%#v", rawJSON, extraJSON)
	}
}

func TestProcessPendingBuiltinRemovedIsTerminalSkipWithoutRetryIncrement(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_raw_builtin_removed_terminal_skip"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, extra_json, raw_json)
		 VALUES ('builtin', 'https://builtin.example/job/removed', '2026-02-12T10:00:00Z', false, false, true, 0, '{"foo":"bar"}', '{"old":"payload"}')`,
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	svc.ReadHTML = func(ctx context.Context, targetURL string) (string, int, error) {
		return "<html><body>Sorry, this job was removed.</body></html>", 200, nil
	}
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed job, got %d", processed)
	}

	var isReady, isSkippable, isParsed bool
	var retryCount int
	var rawJSON, extraJSON sql.NullString
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT is_ready, is_skippable, is_parsed, retry_count, raw_json, extra_json
		 FROM raw_us_jobs WHERE url = 'https://builtin.example/job/removed'`,
	).Scan(&isReady, &isSkippable, &isParsed, &retryCount, &rawJSON, &extraJSON); err != nil {
		t.Fatal(err)
	}
	if !isReady || !isSkippable || isParsed {
		t.Fatalf("unexpected state is_ready=%t is_skippable=%t is_parsed=%t", isReady, isSkippable, isParsed)
	}
	if retryCount != 0 {
		t.Fatalf("expected retry_count unchanged, got %d", retryCount)
	}
	if rawJSON.Valid || extraJSON.Valid {
		t.Fatalf("expected raw_json/extra_json cleared, got raw_json=%#v extra_json=%#v", rawJSON, extraJSON)
	}
}

func TestProcessPendingSkipsWhenEnabledSourcesEmpty(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_raw_enabled_sources_empty"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES ('remoterocketship', 'https://remote.example/job/enabled-empty', '2026-02-12T10:00:00Z', false, false, false, 0, NULL)`,
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{}
	called := false
	svc.ReadHTML = func(ctx context.Context, targetURL string) (string, int, error) {
		called = true
		return "<html></html>", 200, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("expected zero processed jobs, got %d", processed)
	}
	if called {
		t.Fatal("expected no fetch when enabled sources is empty")
	}
}

func TestProcessPendingDoesNotParseNon2xxResponse(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_raw_non_2xx_no_parse"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES ('remoterocketship', 'https://remote.example/job/non-2xx', '2026-02-12T10:00:00Z', false, false, false, 0, NULL)`,
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"remoterocketship": {}}
	parsedCalled := false
	svc.ReadHTML = func(ctx context.Context, targetURL string) (string, int, error) {
		return "<html><body>upstream error page</body></html>", 500, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed job, got %d", processed)
	}
	if parsedCalled {
		t.Fatal("expected parser not to be called for non-2xx response")
	}

	var isReady bool
	var retryCount int
	var rawJSON sql.NullString
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT is_ready, retry_count, raw_json FROM raw_us_jobs WHERE url = 'https://remote.example/job/non-2xx'`,
	).Scan(&isReady, &retryCount, &rawJSON); err != nil {
		t.Fatal(err)
	}
	if isReady {
		t.Fatalf("expected row to stay not ready")
	}
	if retryCount != 1 {
		t.Fatalf("expected retry_count increment, got %d", retryCount)
	}
	if rawJSON.Valid {
		t.Fatalf("expected raw_json unchanged/null, got %#v", rawJSON.String)
	}
}

func TestIsTransientDBErrorDetectsClosedConnectionMessage(t *testing.T) {
	if !isTransientDBError(errors.New("InterfaceError: connection is closed")) {
		t.Fatal("expected closed connection to be treated as transient")
	}
}

func TestIsTransientDBErrorReturnsFalseForNonTransientError(t *testing.T) {
	if isTransientDBError(errors.New("syntax error")) {
		t.Fatal("expected non-transient error")
	}
}
