package remotive

import (
	"strings"
	"testing"
	"time"
)

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
  "validThrough": "2026-03-15T00:00:00Z",
  "applicantLocationRequirements": [{"@type":"Country","name":"United States"}],
  "hiringOrganization": {"@type":"Organization","name":"Example Co"}
}
</script>
</head><body data-publication-date="2026-02-27 08:00:00"></body></html>`
	payload := ParseRawHTML(htmlText, "https://remotive.com/remote/jobs/software/senior-backend-engineer-9990001")
	if payload["slug"] != "senior-backend-engineer" {
		t.Fatalf("expected remotive slug from role title, got %#v", payload["slug"])
	}
	if payload["roleDescription"] == nil || payload["roleRequirements"] == nil || payload["benefits"] == nil {
		t.Fatalf("expected extracted sections, got %#v", payload)
	}
	roleDescription, _ := payload["roleDescription"].(string)
	if roleDescription == "" || !strings.Contains(roleDescription, "<p") {
		t.Fatalf("expected raw html role description, got %#v", payload["roleDescription"])
	}
	if payload["salaryRange"] != nil {
		t.Fatalf("expected nil salaryRange when base salary is zero, got %#v", payload["salaryRange"])
	}
	if payload["validUntilDate"] == nil {
		t.Fatalf("expected validUntilDate mapping, got %#v", payload)
	}
	company, _ := payload["company"].(map[string]any)
	if company == nil || company["profilePicURL"] == nil || company["tagline"] == nil {
		t.Fatalf("expected remotive company enrichment, got %#v", payload["company"])
	}
}

func TestParseRawHTMLNormalizesTitleAndCountryTokens(t *testing.T) {
	htmlText := `
<html><head>
<script type="application/ld+json">
{
  "@type":"JobPosting",
  "title":"[Hiring] Senior Backend Engineer @ExampleCo",
  "url":"https://remotive.com/remote-jobs/software/senior-backend-engineer-9990001",
  "description":"<p>Build systems.</p>",
  "employmentType":"FULL_TIME",
  "applicantLocationRequirements":[{"@type":"Country","name":"United States of America"}],
  "hiringOrganization":{"@type":"Organization","name":"Example Co"}
}
</script>
</head></html>`

	payload := ParseRawHTML(htmlText, "https://remotive.com/remote-jobs/software/senior-backend-engineer-9990001")
	if payload["roleTitle"] != "Senior Backend Engineer" {
		t.Fatalf("expected normalized roleTitle, got %#v", payload["roleTitle"])
	}
	countries, _ := payload["locationCountries"].([]string)
	if len(countries) != 1 || countries[0] != "United States" {
		t.Fatalf("expected normalized country token, got %#v", payload["locationCountries"])
	}
}

func TestParseRawHTMLHandlesMissingOptionalSections(t *testing.T) {
	htmlText := `
<html><head>
<script type="application/ld+json">
{
  "@type":"JobPosting",
  "title":"Backend Engineer",
  "url":"https://remotive.com/remote-jobs/software/backend-engineer-111",
  "description":"<p>Simple description only.</p>",
  "employmentType":"CONTRACT",
  "applicantLocationRequirements":[{"@type":"Country","name":"United States"}],
  "hiringOrganization":{"@type":"Organization","name":"Acme"}
}
</script>
</head></html>`
	payload := ParseRawHTML(htmlText, "https://remotive.com/remote-jobs/software/backend-engineer-111")
	if payload["roleRequirements"] != nil || payload["benefits"] != nil {
		t.Fatalf("expected nil optional sections, got %#v", payload)
	}
}

func TestParseSitemapRowsAndSerializeRoundTrip(t *testing.T) {
	xmlText := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://remotive.com/remote-jobs/software/job-100</loc><lastmod>2026-03-04T12:00:00+00:00</lastmod></url>
</urlset>`
	rows, skipped := ParseSitemapRows(xmlText)
	if skipped != 0 || len(rows) != 1 {
		t.Fatalf("unexpected rows len=%d skipped=%d", len(rows), skipped)
	}
	body := SerializeImportRows(rows)
	parsedRows, parsedSkipped := ParseImportRows(body)
	if parsedSkipped != 0 || len(parsedRows) != 1 {
		t.Fatalf("unexpected parsed rows len=%d skipped=%d", len(parsedRows), parsedSkipped)
	}
	postDate, _ := parsedRows[0]["post_date"].(time.Time)
	if postDate.IsZero() {
		t.Fatalf("expected non-zero post_date")
	}
}
