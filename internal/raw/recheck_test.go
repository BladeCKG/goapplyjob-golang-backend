package raw

import (
	"context"
	"testing"

	"goapplyjob-golang-backend/internal/database"
)

func TestRecheckSkippableClearsRecoveringJobs(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "recheck_skippable_clear"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES
		(1, 'https://example.com/jobs/1', '2026-02-12T10:00:00Z', 1, 1, 0, 0, NULL),
		(2, 'https://example.com/jobs/2', '2026-02-12T10:00:00Z', 1, 1, 0, 0, NULL),
		(3, 'https://example.com/jobs/3', '2026-02-12T10:00:00Z', 1, 0, 0, 0, NULL)`)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(db)
	svc.Status = func(url string) (int, error) {
		switch url {
		case "https://example.com/jobs/1":
			return 200, nil
		case "https://example.com/jobs/2":
			return 404, nil
		default:
			return 500, nil
		}
	}

	checked, cleared, err := svc.RecheckSkippable(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if checked != 2 || cleared != 1 {
		t.Fatalf("checked=%d cleared=%d", checked, cleared)
	}

	var isReady, isSkippable int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_ready, is_skippable FROM raw_us_jobs WHERE id = 1`).Scan(&isReady, &isSkippable); err != nil {
		t.Fatal(err)
	}
	if isReady != 0 || isSkippable != 0 {
		t.Fatalf("expected cleared row to reset, got is_ready=%d is_skippable=%d", isReady, isSkippable)
	}
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_ready, is_skippable FROM raw_us_jobs WHERE id = 2`).Scan(&isReady, &isSkippable); err != nil {
		t.Fatal(err)
	}
	if isReady != 1 || isSkippable != 1 {
		t.Fatalf("expected 404 row to stay skippable, got is_ready=%d is_skippable=%d", isReady, isSkippable)
	}
}
