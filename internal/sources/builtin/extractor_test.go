package builtin

import "testing"

func TestExtractJobBuildsBuiltInRawJobShape(t *testing.T) {
	htmlText := `
<html>
  <head>
    <link rel="canonical" href="https://builtin.com/job/platform-engineer/12345">
    <script type="application/ld+json">
      {
        "@type": "JobPosting",
        "title": "Senior Platform Engineer",
        "datePosted": "2026-02-12T10:00:00Z",
        "validThrough": "2026-03-12T10:00:00Z",
        "description": "<p>Build internal systems.</p><p>Ship reliable services.</p>",
        "employmentType": "FULL_TIME",
        "jobLocationType": "TELECOMMUTE",
        "jobLocation": {
          "address": {
            "addressLocality": "New York",
            "addressRegion": "NY",
            "addressCountry": "USA"
          }
        },
        "identifier": {"value": "12345"},
        "hiringOrganization": {"sameAs": "https://builtin.com/company/acme"}
      }
    </script>
  </head>
  <body>
    <h2>Top Skills</h2>
    <div>Go, Kubernetes, PostgreSQL</div>
  </body>
</html>`
	companyHTML := `
<html>
  <head>
    <link rel="canonical" href="https://builtin.com/company/acme">
    <meta name="description" content="Acme builds infrastructure products.">
    <script>Builtin.companyProfileInit({"companyId":987,"companyName":"Acme","companyAlias":"acme"});</script>
  </head>
  <body>
    <a href="https://acme.example.com">View Website</a>
    <a href="https://linkedin.com/company/acme">LinkedIn</a>
    Year Founded: 2014
    300 Total Employees
  </body>
</html>`

	payload := ExtractJob(htmlText, companyHTML)
	if payload["id"] != 12345 {
		t.Fatalf("expected external job id, got %#v", payload["id"])
	}
	if payload["url"] != "https://builtin.com/job/platform-engineer/12345" {
		t.Fatalf("expected canonical url, got %#v", payload["url"])
	}
	if payload["employmentType"] != "full-time" {
		t.Fatalf("expected normalized employment type, got %#v", payload["employmentType"])
	}
	if payload["locationType"] != "remote" {
		t.Fatalf("expected normalized location type, got %#v", payload["locationType"])
	}
	if payload["location"] != "New York, NY, USA" {
		t.Fatalf("expected location label, got %#v", payload["location"])
	}
	techStack, _ := payload["techStack"].([]string)
	if len(techStack) != 3 || techStack[0] != "Go" {
		t.Fatalf("unexpected tech stack %#v", payload["techStack"])
	}
	company, _ := payload["company"].(map[string]any)
	if company["id"] != float64(987) && company["id"] != 987 {
		t.Fatalf("expected raw company id, got %#v", company["id"])
	}
	if company["slug"] != "acme" {
		t.Fatalf("expected raw company slug, got %#v", company["slug"])
	}
	if payload["isSenior"] != true || payload["isLead"] != false {
		t.Fatalf("unexpected seniority flags senior=%#v lead=%#v", payload["isSenior"], payload["isLead"])
	}
}
