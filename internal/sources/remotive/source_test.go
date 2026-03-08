package remotive

import "testing"

func TestParseImportRowsFromRemotivePayload(t *testing.T) {
	rows, skipped := ParseImportRows(`[{"url":"https://remotive.com/remote-jobs/software-dev/backend-engineer-1234","scrapt_Date":"2026-02-18T03:21:08Z"}]`)
	if skipped != 0 || len(rows) != 1 {
		t.Fatalf("unexpected parse result skipped=%d len=%d", skipped, len(rows))
	}
}

func TestParseRawHTMLSkipsNonUS(t *testing.T) {
	htmlText := `
<html><head>
<script type="application/ld+json">
{"@type":"JobPosting","title":"Backend Engineer","description":"<p>Build.</p>","applicantLocationRequirements":{"@type":"Country","name":"Canada"}}
</script>
</head></html>`
	payload := ParseRawHTML(htmlText, "https://remotive.com/job-123")
	if payload["_skip_for_non_us"] != true {
		t.Fatalf("expected non-us skip marker, got %#v", payload)
	}
}
