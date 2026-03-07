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
        "title": "Platform Engineer",
        "datePosted": "2026-02-12T10:00:00Z",
        "validThrough": "2026-03-12T10:00:00Z",
        "description": "<p>Build internal systems.</p><p>Ship reliable services.</p>",
        "employmentType": "FULL_TIME",
        "jobLocationType": "TELECOMMUTE",
        "jobLocation": [
          {
            "address": {
              "addressLocality": "New York",
              "addressRegion": "NY",
              "addressCountry": "USA"
            }
          },
          {
            "address": {
              "addressLocality": "Austin",
              "addressRegion": "TX",
              "addressCountry": "USA"
            }
          }
        ],
        "identifier": {"value": "12345"},
        "hiringOrganization": {"sameAs": "https://builtin.com/company/acme"},
        "skills": ["Go", "Kubernetes"],
        "keywords": "PostgreSQL, Terraform",
        "baseSalary": {
          "currency": "USD",
          "value": {
            "minValue": 180000,
            "maxValue": 220000,
            "unitText": "year"
          }
        }
      }
    </script>
  </head>
  <body>
    <div>Senior level</div>
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
	if payload["location"] != "New York, NY, USA | Austin, TX, USA" {
		t.Fatalf("expected location label, got %#v", payload["location"])
	}
	techStack, _ := payload["techStack"].([]string)
	if len(techStack) != 4 || techStack[0] != "Go" || techStack[3] != "Terraform" {
		t.Fatalf("unexpected tech stack %#v", payload["techStack"])
	}
	company, _ := payload["company"].(map[string]any)
	if company["id"] != float64(987) && company["id"] != 987 {
		t.Fatalf("expected raw company id, got %#v", company["id"])
	}
	if company["slug"] != "acme" {
		t.Fatalf("expected raw company slug, got %#v", company["slug"])
	}
	salaryRange, _ := payload["salaryRange"].(map[string]any)
	if salaryRange["min"] != 180000 || salaryRange["salaryType"] != "per year" {
		t.Fatalf("unexpected salary range %#v", salaryRange)
	}
	if payload["isSenior"] != true || payload["isLead"] != false {
		t.Fatalf("unexpected seniority flags senior=%#v lead=%#v", payload["isSenior"], payload["isLead"])
	}
	states, _ := payload["locationUSStates"].([]string)
	if len(states) != 2 || states[0] != "NY" || states[1] != "TX" {
		t.Fatalf("unexpected state list %#v", payload["locationUSStates"])
	}
}
