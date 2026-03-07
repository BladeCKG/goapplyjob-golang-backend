package parsed

import (
	"context"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/database"
)

func TestFreshnessSourceOlderThanPostDateReturnsTrue(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 9, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if !isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected source to be older than post date")
	}
}

func TestFreshnessSourceEqualToPostDateReturnsFalse(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected equal timestamps to be allowed")
	}
}

func TestFreshnessNaivePostDateIsHandled(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 9, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.Local)
	if !isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected timestamps to be normalized")
	}
}

func TestFreshnessMissingCreatedAtSourceReturnsFalse(t *testing.T) {
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if isSourceOlderThanPostDate(nil, &postDate) {
		t.Fatal("expected missing source date to be ignored")
	}
}

func TestResetStaleParsedRemovesStaleParsedRows(t *testing.T) {
	db, err := database.Open("file:test_parsed_freshness?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES
		 (1, 'https://example.com/jobs/stale', '2026-02-12T10:00:00Z', 1, 0, 1, 0, '{}'),
		 (2, 'https://example.com/jobs/fresh', '2026-02-12T10:00:00Z', 1, 0, 1, 0, '{}')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, created_at_source, url, role_title) VALUES
		 (1, '2026-02-12T09:00:00Z', 'https://example.com/jobs/stale', 'Stale Engineer'),
		 (2, '2026-02-12T10:00:00Z', 'https://example.com/jobs/fresh', 'Fresh Engineer')`,
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(db)
	checkedCount, staleCount, err := svc.ResetStaleParsed(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if checkedCount != 2 || staleCount != 1 {
		t.Fatalf("unexpected counts checked=%d stale=%d", checkedCount, staleCount)
	}

	var staleParsedCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs WHERE raw_us_job_id = 1`).Scan(&staleParsedCount); err != nil {
		t.Fatal(err)
	}
	if staleParsedCount != 0 {
		t.Fatalf("expected stale parsed row to be deleted, got %d", staleParsedCount)
	}

	var isReady, isParsed int
	var rawJSON any
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_ready, is_parsed, raw_json FROM raw_us_jobs WHERE id = 1`).Scan(&isReady, &isParsed, &rawJSON); err != nil {
		t.Fatal(err)
	}
	if isReady != 0 || isParsed != 0 {
		t.Fatalf("expected stale raw row to be reset, got is_ready=%d is_parsed=%d", isReady, isParsed)
	}
	if rawJSON != nil {
		t.Fatalf("expected stale raw row payload cleared, got %#v", rawJSON)
	}

	var freshParsedCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs WHERE raw_us_job_id = 2`).Scan(&freshParsedCount); err != nil {
		t.Fatal(err)
	}
	if freshParsedCount != 1 {
		t.Fatalf("expected fresh parsed row to remain, got %d", freshParsedCount)
	}
}
