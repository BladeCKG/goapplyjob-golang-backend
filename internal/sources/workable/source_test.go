package workable

import (
	"strings"
	"testing"
)

func TestBuildAPIURLSetsTokenAndLimit(t *testing.T) {
	got := BuildAPIURL("https://jobs.workable.com/api/v1/jobs?location=United%20States", "abc", 100)
	if !strings.Contains(got, "pageToken=abc") || !strings.Contains(got, "limit=100") {
		t.Fatalf("unexpected workable api url %q", got)
	}
}

func TestNormalizeJobsBuildsRawPayloadRows(t *testing.T) {
	rows, skipped := NormalizeJobs(`{
		"jobs": [{
			"url": "https://jobs.workable.com/view/a",
			"title": "Backend Engineer",
			"created": "2026-02-18T03:21:08.178Z",
			"updated": "2026-02-18T03:21:08.178Z",
			"language": "en",
			"company": {"title": "Acme", "website": "https://acme.com"},
			"location": {"city": "Austin", "subregion": "Texas", "countryName": "United States"},
			"locations": ["Austin, Texas, United States"],
			"workplace": "remote"
		}]
	}`)
	if skipped != 0 || len(rows) != 1 {
		t.Fatalf("unexpected rows skipped=%d len=%d", skipped, len(rows))
	}
	rawPayload, _ := rows[0]["raw_payload"].(map[string]any)
	if rawPayload["roleTitle"] != "Backend Engineer" {
		t.Fatalf("unexpected raw payload %#v", rawPayload)
	}
	if rawPayload["slug"] != "backend-engineer" {
		t.Fatalf("expected workable job slug from url path, got %#v", rawPayload["slug"])
	}
	company, _ := rawPayload["company"].(map[string]any)
	if company == nil || company["slug"] != "acme" {
		t.Fatalf("expected workable company slug, got %#v", rawPayload["company"])
	}
}

func TestNormalizeJobsDetectsSeniorFromSrWithPeriod(t *testing.T) {
	rows, skipped := NormalizeJobs(`{
		"jobs": [{
			"url": "https://jobs.workable.com/view/a",
			"title": "Sr., Backend Engineer",
			"created": "2026-02-18T03:21:08.178Z",
			"updated": "2026-02-18T03:21:08.178Z",
			"company": {"title": "Acme", "website": "https://acme.com"},
			"location": {"city": "Austin", "subregion": "Texas", "countryName": "United States"},
			"locations": ["Austin, Texas, United States"]
		}]
	}`)
	if skipped != 0 || len(rows) != 1 {
		t.Fatalf("unexpected rows skipped=%d len=%d", skipped, len(rows))
	}
	rawPayload, _ := rows[0]["raw_payload"].(map[string]any)
	if rawPayload["isSenior"] != true || rawPayload["isMidLevel"] != false {
		t.Fatalf("unexpected seniority flags %#v", rawPayload)
	}
}

func TestNormalizeJobsFiltersNullLikeLanguageAndState(t *testing.T) {
	rows, skipped := NormalizeJobs(`{
		"jobs": [{
			"url": "https://jobs.workable.com/view/a",
			"title": "Backend Engineer",
			"created": "2026-02-18T03:21:08.178Z",
			"updated": "2026-02-18T03:21:08.178Z",
			"language": "null",
			"company": {"title": "Acme", "website": "https://acme.com"},
			"location": {"city": "Austin", "subregion": "null", "countryName": "United States"},
			"locations": ["Austin, null, United States"]
		}]
	}`)
	if skipped != 0 || len(rows) != 1 {
		t.Fatalf("unexpected rows skipped=%d len=%d", skipped, len(rows))
	}
	rawPayload, _ := rows[0]["raw_payload"].(map[string]any)
	required, _ := rawPayload["requiredLanguages"].([]string)
	if len(required) != 0 {
		t.Fatalf("expected empty requiredLanguages, got %#v", rawPayload["requiredLanguages"])
	}
	states, _ := rawPayload["locationUSStates"].([]string)
	if len(states) != 0 {
		t.Fatalf("expected empty locationUSStates, got %#v", rawPayload["locationUSStates"])
	}
}
