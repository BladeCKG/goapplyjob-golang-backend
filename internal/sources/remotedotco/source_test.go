package remotedotco

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFilterUSStatesNormalizesNames(t *testing.T) {
	got := filterUSStates([]string{"CA", "California", " texas ", "NSW", "Ontario", "DC"})
	want := []string{"California", "Texas", "District Of Columbia"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filterUSStates mismatch got=%#v want=%#v", got, want)
	}
}

func TestParseRawHTMLStrictFields(t *testing.T) {
	htmlPath := filepath.Join("..", "..", "..", "test-extract", "remotedotco", "raw-job-1.html")
	jsonPath := filepath.Join("..", "..", "..", "test-extract", "remotedotco", "raw-job-1.json")
	htmlBytes, err := os.ReadFile(htmlPath)
	if err != nil {
		t.Fatalf("read html: %v", err)
	}
	jsonBytes, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		t.Fatalf("parse json: %v", err)
	}
	props, _ := data["props"].(map[string]any)
	pageProps, _ := props["pageProps"].(map[string]any)
	jobDetails, _ := pageProps["jobDetails"].(map[string]any)
	if jobDetails == nil {
		t.Fatalf("missing jobDetails in fixture")
	}

	payload := ParseRawHTML(string(htmlBytes), "")
	if len(payload) == 0 {
		t.Fatalf("expected payload from html")
	}
	gotCountries, _ := payload["locationCountries"].([]string)
	if len(gotCountries) == 0 {
		t.Fatalf("expected non-us locationCountries to be preserved, got %#v", payload["locationCountries"])
	}

	assertEqual := func(label string, got any, want any) {
		if got != want {
			t.Fatalf("%s mismatch got=%v want=%v", label, got, want)
		}
	}

	assertEqual("url", payload["url"], jobDetails["applyURL"])
	assertEqual("roleTitle", payload["roleTitle"], jobDetails["title"])
	assertEqual("roleDescription", payload["roleDescription"], jobDetails["description"])
	assertEqual("jobSummary", payload["jobDescriptionSummary"], jobDetails["jobSummary"])

	postedDate := normalizeISO(jobDetails["postedDate"])
	assertEqual("created_at", payload["created_at"], postedDate)
	assertEqual("validUntilDate", payload["validUntilDate"], jobDetails["expireOn"])

	employmentType := normalizeToken(firstString(jobDetails["jobSchedules"]))
	assertEqual("employmentType", payload["employmentType"], employmentType)

	locationType := normalizeRemoteOption(jobDetails["remoteOptions"])
	assertEqual("locationType", payload["locationType"], locationType)

	cities := stringSlice(jobDetails["cities"])
	if len(cities) > 0 {
		assertEqual("locationCity", payload["locationCity"], cities[0])
	}

	states := stringSlice(jobDetails["states"])
	if got, ok := payload["locationUSStates"].([]string); ok {
		wantStates := filterUSStates(states)
		if len(wantStates) != len(got) {
			t.Fatalf("locationUSStates len mismatch got=%d want=%d", len(got), len(wantStates))
		}
	}

	countries := stringSlice(jobDetails["countries"])
	if got, ok := payload["locationCountries"].([]string); ok {
		if len(countries) != len(got) {
			t.Fatalf("locationCountries len mismatch got=%d want=%d", len(got), len(countries))
		}
	}

	companyPayload, _ := payload["company"].(map[string]any)
	companyFixture, _ := jobDetails["company"].(map[string]any)
	if companyPayload == nil || companyFixture == nil {
		t.Fatalf("missing company payload")
	}
	assertEqual("company.id", companyPayload["id"], Source+"_"+stringValue(companyFixture["companyId"]))
	assertEqual("company.name", companyPayload["name"], companyFixture["name"])
	assertEqual("company.slug", companyPayload["slug"], companyFixture["slug"])
	assertEqual("company.homePageURL", companyPayload["homePageURL"], companyFixture["website"])
	assertEqual("company.profilePicURL", companyPayload["profilePicURL"], normalizeRemoteCoURL(companyFixture["logo"]))
}

func TestParseRawHTMLPrefixesCompanyIDWithSource(t *testing.T) {
	htmlText := `
<html>
<body>
<script type="application/json">
{"props":{"pageProps":{"jobDetails":{
  "id":"job-1",
  "title":"Engineer",
  "description":"Build things",
  "jobSummary":"Summary",
  "applyURL":"https://remote.co/remote-jobs/job-1",
  "postedDate":"2026-03-01T10:00:00Z",
  "remoteOptions":["100% remote work"],
  "jobSchedules":["Full-time"],
  "cities":["Austin"],
  "states":["Texas"],
  "countries":["United States"],
  "company":{"companyId":"abc123","name":"Acme","slug":"acme","website":"https://acme.com"}
}}}}
</script>
</body>
</html>`
	payload := ParseRawHTML(htmlText, "")
	company, _ := payload["company"].(map[string]any)
	if company["id"] != "remotedotco_abc123" {
		t.Fatalf("expected namespaced company.id, got %#v", company["id"])
	}
}
