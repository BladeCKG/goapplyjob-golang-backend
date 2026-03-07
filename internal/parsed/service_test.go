package parsed

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/database"
)

func TestSourceOlderThanPostDateReturnsTrue(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 9, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if !isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected source to be older than post date")
	}
}

func TestSourceEqualToPostDateReturnsFalse(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected equal timestamps to be allowed")
	}
}

func TestSourceNewerThanPostDateReturnsFalse(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 11, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected newer source timestamp to be allowed")
	}
}

func TestMissingSourceDateReturnsFalse(t *testing.T) {
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if isSourceOlderThanPostDate(nil, &postDate) {
		t.Fatal("expected missing source timestamp to be allowed")
	}
}

func TestNaiveAndAwareDatetimesAreComparedSafely(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 9, 0, 0, 0, time.FixedZone("UTC", 0))
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.Local)
	if !isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected timestamps to be normalized before comparison")
	}
}

func TestProcessPendingClearsStalePayloadWhenSourceCreatedAtIsOlderThanPostDate(t *testing.T) {
	db, err := database.Open("file:test_parsed_stale?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	payload, err := json.Marshal(map[string]any{
		"created_at":          "2026-02-12T09:00:00Z",
		"url":                 "https://example.com/jobs/1",
		"categorizedJobTitle": "Software Engineer",
		"roleTitle":           "Backend Engineer",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, 1, 0, 0, 0, ?)`,
		"https://example.com/jobs/1",
		"2026-02-12T10:00:00Z",
		string(payload),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(db)
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected one row processed, got %d", processed)
	}

	var isReady, isParsed int
	var rawJSON any
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_ready, is_parsed, raw_json FROM raw_us_jobs WHERE url = ?`, "https://example.com/jobs/1").Scan(&isReady, &isParsed, &rawJSON); err != nil {
		t.Fatal(err)
	}
	if isReady != 0 || isParsed != 0 {
		t.Fatalf("expected stale row to be reset, got is_ready=%d is_parsed=%d", isReady, isParsed)
	}
	if rawJSON != nil {
		t.Fatalf("expected stale row raw_json to be cleared, got %#v", rawJSON)
	}
}
