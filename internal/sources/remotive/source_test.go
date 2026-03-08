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

func TestParseRawHTMLExtractsSectionsAndSalaryHandling(t *testing.T) {
	htmlText := `
<html><head>
<script type="application/ld+json">
{
  "@context": "http://schema.org/",
  "@type": "JobPosting",
  "title": "[Hiring] Senior Backend Engineer @ExampleCo",
  "url": "https://remotive.com/remote/jobs/software/senior-backend-engineer-9990001",
  "description": "<p class='h2 tw-mt-4 remotive-text-bigger'>Role Description</p><p>This role is responsible for platform APIs.</p><p class='h2 tw-mt-4 remotive-text-bigger'>Requirements</p><p>Active WA State RN License</p><p class='h2 tw-mt-4 remotive-text-bigger'>Benefits</p><p>401(k)</p><p class='h2 tw-mt-4 remotive-text-bigger'>Company Description</p><p>Flexible work from home options available.</p>",
  "baseSalary": {"@type":"MonetaryAmount","currency":"USD","value":{"minValue":0,"maxValue":0,"unitText":"YEAR"}},
  "applicantLocationRequirements": [{"@type":"Country","name":"United States"}],
  "hiringOrganization": {"@type":"Organization","name":"Example Co"}
}
</script>
</head><body data-publication-date="2026-02-27 08:00:00"></body></html>`
	payload := ParseRawHTML(htmlText, "https://remotive.com/remote/jobs/software/senior-backend-engineer-9990001")
	if payload["roleDescription"] == nil || payload["roleRequirements"] == nil || payload["benefits"] == nil {
		t.Fatalf("expected extracted sections, got %#v", payload)
	}
	if payload["salaryRange"] != nil {
		t.Fatalf("expected nil salaryRange when base salary is zero, got %#v", payload["salaryRange"])
	}
	company, _ := payload["company"].(map[string]any)
	if company == nil || company["profilePicURL"] == nil || company["tagline"] == nil {
		t.Fatalf("expected remotive company enrichment, got %#v", payload["company"])
	}
}
