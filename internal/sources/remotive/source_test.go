package remotive

import (
	"os"
	"path/filepath"
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

func TestParseRawHTMLKeepsNonUSLocationCountries(t *testing.T) {
	htmlText := `
<html><head>
<script type="application/ld+json">
{"@type":"JobPosting","title":"Backend Engineer","description":"<p>Build.</p>","applicantLocationRequirements":{"@type":"Country","name":"Canada"}}
</script>
</head><body><h1>Backend Engineer</h1><p class="tw-mt-4 tw-text-sm">Example Co is hiring a remote Backend Engineer. Location: Canada.</p></body></html>`
	payload := ParseRawHTML(htmlText, "https://remotive.com/job-123")
	countries, _ := payload["locationCountries"].([]string)
	if len(countries) != 1 || countries[0] != "Canada" {
		t.Fatalf("expected Canada locationCountries, got %#v", payload["locationCountries"])
	}
}

func TestParseRawHTMLKeepsPayloadWhenLocationCountriesMissing(t *testing.T) {
	htmlText := `
<html><head>
<script type="application/ld+json">
{"@type":"JobPosting","title":"Backend Engineer","description":"<p>Build.</p>"}
</script>
</head></html>`
	payload := ParseRawHTML(htmlText, "https://remotive.com/job-123")
	if len(payload) == 0 {
		t.Fatalf("expected payload when locationCountries are missing, got %#v", payload)
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
</head><body data-publication-date="2026-02-27 08:00:00"><h1>Senior Backend Engineer</h1><p class="tw-mt-4 tw-text-sm">Feb 27, 2026 - Example Co is hiring a remote Senior Backend Engineer. 📍Location: United States.</p></body></html>`
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
</head><body><h1>Senior Backend Engineer</h1><p class="tw-mt-4 tw-text-sm">Example Co is hiring a remote Senior Backend Engineer. 📍Location: United States.</p></body></html>`

	payload := ParseRawHTML(htmlText, "https://remotive.com/remote-jobs/software/senior-backend-engineer-9990001")
	if payload["roleTitle"] != "Senior Backend Engineer" {
		t.Fatalf("expected normalized roleTitle, got %#v", payload["roleTitle"])
	}
	if payload["isSenior"] != true || payload["isMidLevel"] != false {
		t.Fatalf("unexpected seniority flags %#v", payload)
	}
	countries, _ := payload["locationCountries"].([]string)
	if len(countries) != 1 || countries[0] != "United States" {
		t.Fatalf("expected normalized country token, got %#v", payload["locationCountries"])
	}
}

func TestParseRawHTMLExtractsCountriesFromLocationComponentFixtures(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		want     []string
	}{
		{name: "russia", fileName: "raw-job-1.html", want: []string{"Russian Federation"}},
		{name: "regional plus israel", fileName: "raw-job-2.html", want: []string{"Americas", "Europe", "Israel"}},
		{name: "worldwide", fileName: "raw-job-3.html", want: []string{"Worldwide"}},
		{name: "usa and canada", fileName: "raw-job-4.html", want: []string{"United States", "Canada", "USA Timezones"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join("..", "..", "..", "test-extract", "remotive", tc.fileName)
			htmlBytes, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			got := extractLocationCountriesFromLocationComponent(string(htmlBytes))
			if len(got) != len(tc.want) {
				t.Fatalf("len mismatch got=%v want=%v", got, tc.want)
			}
			for index := range tc.want {
				if got[index] != tc.want[index] {
					t.Fatalf("mismatch got=%v want=%v", got, tc.want)
				}
			}
		})
	}
}

func TestParseRawHTMLExtractsSalaryFromSummaryFixtures(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		want     map[string]any
	}{
		{name: "unpaid internship", fileName: "raw-job-1.html", want: nil},
		{name: "hourly range", fileName: "raw-job-2.html", want: map[string]any{
			"min":                     int64(90),
			"max":                     int64(150),
			"salaryType":              "per hour",
			"currencyCode":            "USD",
			"currencySymbol":          "$",
			"salaryHumanReadableText": "$90 - $150 /hour.",
			"minSalaryAsUSD":          int64(90),
			"maxSalaryAsUSD":          int64(150),
		}},
		{name: "hourly compact range", fileName: "raw-job-3.html", want: map[string]any{
			"min":                     int64(50),
			"max":                     int64(75),
			"salaryType":              "per hour",
			"currencyCode":            "USD",
			"currencySymbol":          "$",
			"salaryHumanReadableText": "$50-$75 /hour.",
			"minSalaryAsUSD":          int64(50),
			"maxSalaryAsUSD":          int64(75),
		}},
		{name: "yearly k range", fileName: "raw-job-4.html", want: map[string]any{
			"min":                     int64(160000),
			"max":                     int64(180000),
			"salaryType":              "per year",
			"currencyCode":            "USD",
			"currencySymbol":          "$",
			"salaryHumanReadableText": "$160k - $180k.",
			"minSalaryAsUSD":          int64(160000),
			"maxSalaryAsUSD":          int64(180000),
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join("..", "..", "..", "test-extract", "remotive", tc.fileName)
			htmlBytes, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			got, _ := extractSalaryRangeFromSummaryHTML(string(htmlBytes)).(map[string]any)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("expected nil salary, got %#v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected salary payload, got nil")
			}
			for key, wantValue := range tc.want {
				if got[key] != wantValue {
					t.Fatalf("key %q mismatch got=%#v want=%#v payload=%#v", key, got[key], wantValue, got)
				}
			}
		})
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

func TestParseSitemapRowsKeepsRowWhenLastmodInvalid(t *testing.T) {
	xmlText := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://remotive.com/remote-jobs/software/job-100</loc><lastmod>invalid-date</lastmod></url>
</urlset>`
	rows, skipped := ParseSitemapRows(xmlText)
	if skipped != 0 {
		t.Fatalf("expected skipped=0, got %d", skipped)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	postDate, _ := rows[0]["post_date"].(time.Time)
	if !postDate.IsZero() {
		t.Fatalf("expected zero post_date for invalid lastmod, got %s", postDate.Format(time.RFC3339Nano))
	}
}
