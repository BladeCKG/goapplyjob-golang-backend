package dailyremote

import (
	"encoding/json"
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

func TestParseRawHTMLExtractsSalarySummaryAndCompanyEnrichment(t *testing.T) {
	html := `
<div class="job_head_info_container">
  <div class="inline-flex items-center">$104K - $175K per year</div>
</div>
<h3>AI Summary</h3>
<div><div class="px-3 py-3">Build systems with modern backend tooling and infrastructure.</div></div>
<div class="detailed-job-company-profile">
  <div class="company-profile-tags">
    <span class="tag">Employees 10,001+ employees</span>
    <span class="tag">Industry IT Services and IT Consulting</span>
  </div>
</div>
<script type="application/ld+json">
{
  "@type":"JobPosting",
  "url":"https://dailyremote.com/remote-job/c-c-senior-software-engineer-4683161",
  "title":"Senior Platform Engineer",
  "description":"&lt;p&gt;Role&lt;/p&gt;",
  "datePosted":"2026-03-04T14:00:16.000Z",
  "employmentType":"FULL_TIME",
  "jobLocationType":"TELECOMMUTE",
  "applicantLocationRequirements":[{"@type":"Country","name":"United States"}],
  "hiringOrganization":{"@type":"Organization","name":"Acme","logo":"https://assets.example/logo.png"}
}
</script>`

	payload := ParseRawHTML(html, "https://dailyremote.com/remote-job/c-c-senior-software-engineer-4683161")
	if payload["id"] != "4683161" {
		t.Fatalf("expected extracted id, got %#v", payload["id"])
	}
	if payload["_skip_for_retry"] != nil {
		t.Fatalf("did not expect retry marker for resolved url, got %#v", payload["_skip_for_retry"])
	}
	if payload["isSenior"] != true || payload["isMidLevel"] != false {
		t.Fatalf("unexpected seniority flags %#v", payload)
	}
	if payload["jobDescriptionSummary"] == nil {
		t.Fatalf("expected AI summary mapped into jobDescriptionSummary")
	}
	salary, _ := payload["salaryRange"].(map[string]any)
	if salary != nil {
		minValue, minOK := salary["min"].(float64)
		maxValue, maxOK := salary["max"].(float64)
		if !minOK || !maxOK || minValue != 104000 || maxValue != 175000 {
			t.Fatalf("unexpected salaryRange %#v", salary)
		}
	}
	company, _ := payload["company"].(map[string]any)
	if company == nil {
		t.Fatalf("expected company payload")
	}
	if company["employeeRange"] != "10001+" {
		t.Fatalf("expected employeeRange 10001+, got %#v", company["employeeRange"])
	}
	switch industries := company["industrySpecialities"].(type) {
	case []string:
		if len(industries) == 0 || industries[0] != "IT Services and IT Consulting" {
			t.Fatalf("unexpected company industries %#v", company["industrySpecialities"])
		}
	case []any:
		if len(industries) == 0 || industries[0] != "IT Services and IT Consulting" {
			t.Fatalf("unexpected company industries %#v", company["industrySpecialities"])
		}
	default:
		t.Fatalf("unexpected company industries type %#T", company["industrySpecialities"])
	}
}

func TestParseRawHTMLMarksRetryWhenUnresolvedApplyURL(t *testing.T) {
	html := `
<script type="application/ld+json">
{
  "@type":"JobPosting",
  "url":"https://dailyremote.com/remote-job/senior-platform-engineer-4683999",
  "title":"Senior Platform Engineer",
  "description":"&lt;p&gt;Role&lt;/p&gt;",
  "datePosted":"2026-03-04T14:00:16.000Z",
  "employmentType":"FULL_TIME",
  "applicantLocationRequirements":[{"@type":"Country","name":"United States"}],
  "hiringOrganization":{"@type":"Organization","name":"Acme"}
}
</script>`

	orig := resolveRedirectURLDailyRemoteFunc
	resolveRedirectURLDailyRemoteFunc = func(_url string) string {
		return "https://dailyremote.com/apply/4683999"
	}
	defer func() { resolveRedirectURLDailyRemoteFunc = orig }()

	payload := ParseRawHTML(html, "https://dailyremote.com/remote-job/senior-platform-engineer-4683999")
	if payload["_skip_for_retry"] != true {
		t.Fatalf("expected retry marker, got %#v", payload["_skip_for_retry"])
	}
}

func TestParseSalaryRangeFromTextNormalizesMojibakeGBP(t *testing.T) {
	raw := parseSalaryRangeFromText("Â£70K - Â£90K /year")
	payload, _ := raw.(map[string]any)
	if payload == nil {
		t.Fatalf("expected salary payload")
	}
	if payload["currencyCode"] != "GBP" || payload["salaryType"] != "per year" {
		t.Fatalf("unexpected payload %#v", payload)
	}
}

func TestLooksLikeSalaryTextIgnoresExperienceHints(t *testing.T) {
	if looksLikeSalaryText("5-10 yrs exp") {
		t.Fatal("expected experience text not to be treated as salary")
	}
	if !looksLikeSalaryText("$120K - $140K per year") {
		t.Fatal("expected salary-like text to be detected")
	}
}

func TestParseImportRowsRoundTrip(t *testing.T) {
	rows := []map[string]any{
		{
			"url":       "https://dailyremote.com/remote-job/test-4683001",
			"post_date": time.Date(2026, 3, 4, 10, 11, 12, 0, time.UTC),
		},
	}
	body := SerializeImportRows(rows)
	parsed, skipped := ParseImportRows(body)
	if skipped != 0 || len(parsed) != 1 {
		t.Fatalf("unexpected parsed rows len=%d skipped=%d", len(parsed), skipped)
	}
	got := map[string]any{
		"url":       parsed[0]["url"],
		"post_date": parsed[0]["post_date"].(time.Time).UTC().Format(time.RFC3339Nano),
	}
	want := map[string]any{
		"url":       "https://dailyremote.com/remote-job/test-4683001",
		"post_date": time.Date(2026, 3, 4, 10, 11, 12, 0, time.UTC).Format(time.RFC3339Nano),
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("roundtrip mismatch got=%s want=%s", gotJSON, wantJSON)
	}
}

func anyString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
