package raw

import (
	"context"
	"encoding/json"
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

func TestProcessPendingUsesNormalizedURLForFetchAndPayload(t *testing.T) {
	db, err := database.Open("file:test_raw_process_pending?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	jobURL := "https://www.remoterocketship.com/us/company/acme/jobs/dev/"
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, '2026-02-12T10:00:00Z', 0, 0, 0, 0, NULL)`, jobURL)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(db)
	fetchedURLs := []string{}
	svc.ReadHTML = func(targetURL string) (string, int, error) {
		fetchedURLs = append(fetchedURLs, targetURL)
		return "<html></html>", 200, nil
	}
	svc.ParseHTML = func(html string) (map[string]any, error) {
		return map[string]any{"roleTitle": "Backend Engineer"}, nil
	}

	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed job, got %d", processed)
	}
	expectedTargetURL := "https://www.remoterocketship.com/company/acme/jobs/dev/"
	if len(fetchedURLs) != 1 || fetchedURLs[0] != expectedTargetURL {
		t.Fatalf("expected fetch to use normalized URL, got %#v", fetchedURLs)
	}

	var isReady int
	var rawJSONText string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_ready, raw_json FROM raw_us_jobs WHERE url = ?`, jobURL).Scan(&isReady, &rawJSONText); err != nil {
		t.Fatal(err)
	}
	if isReady != 1 {
		t.Fatalf("expected job to become ready, got %d", isReady)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(rawJSONText), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["url"] != expectedTargetURL {
		t.Fatalf("expected payload url %s, got %#v", expectedTargetURL, payload["url"])
	}
}
