package dailyremote

import (
	"strings"
	"testing"
	"time"
)

func TestParseImportRows(t *testing.T) {
	rows, skipped := ParseImportRows(`[{"url":"https://dailyremote.com/remote-job/backend-engineer-12345","post_date":"2026-03-01T12:00:00Z"}]`)
	if skipped != 0 {
		t.Fatalf("unexpected skipped rows: %d", skipped)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestExtractJobListings(t *testing.T) {
	now := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)
	html := `<article class="card js-card"><h2 class="job-position"><a href="/remote-job/backend-engineer-12345">Backend Engineer</a></h2><span>2 hours ago</span></article>`
	rows := ExtractJobListings(html, "https://dailyremote.com/?page=1", now)
	if len(rows) != 1 {
		t.Fatalf("expected 1 listing, got %d", len(rows))
	}
	if ExtractExternalIDFromURL(anyString(rows[0]["url"])) != 12345 {
		t.Fatalf("unexpected extracted external id from url: %#v", rows[0]["url"])
	}
	postDate, _ := rows[0]["post_date"].(time.Time)
	if postDate.IsZero() {
		t.Fatalf("expected non-zero post_date")
	}
}

func TestParseRawHTMLNonUSSkip(t *testing.T) {
	html := `<script type="application/ld+json">{"@type":"JobPosting","url":"https://dailyremote.com/remote-job/test-12345","datePosted":"2026-03-01T12:00:00Z","title":"Role","description":"Desc","employmentType":"full_time","jobLocationType":"TELECOMMUTE","applicantLocationRequirements":[{"@type":"Country","name":"Germany"}],"hiringOrganization":{"@type":"Organization","name":"Acme","sameAs":"https://acme.example"}}</script>`
	payload := ParseRawHTML(html, "https://dailyremote.com/remote-job/test-12345")
	if skip, _ := payload["_skip_for_non_us"].(bool); !skip {
		t.Fatalf("expected non-us skip payload, got %#v", payload)
	}
}

func TestToTargetJobURL(t *testing.T) {
	target := ToTargetJobURL("https://dailyremote.com/remote-job/backend-engineer-12345?ref=a#section")
	if !strings.Contains(target, "/apply/12345") {
		t.Fatalf("unexpected target url: %s", target)
	}
}

func anyString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
