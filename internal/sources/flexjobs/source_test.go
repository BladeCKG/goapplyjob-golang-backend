package flexjobs

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseSitemapRowsFallbacksMissingLastmodToNow(t *testing.T) {
	xmlPath := filepath.Join("..", "..", "..", "test-extract", "flexjobs", "sitemap.xml")
	xmlBytes, err := os.ReadFile(xmlPath)
	if err != nil {
		t.Fatalf("read sitemap: %v", err)
	}
	now := time.Date(2026, 4, 13, 12, 34, 56, 0, time.UTC)
	rows, skipped := ParseSitemapRows(string(xmlBytes), now)
	if skipped != 0 {
		t.Fatalf("unexpected skipped=%d", skipped)
	}
	if len(rows) == 0 {
		t.Fatal("expected sitemap rows")
	}
	firstURL, _ := rows[0]["url"].(string)
	firstPostDate, _ := rows[0]["post_date"].(time.Time)
	if firstURL == "" {
		t.Fatal("expected first url")
	}
	if !firstPostDate.Equal(now) {
		t.Fatalf("expected missing lastmod fallback to now, got %s", firstPostDate.Format(time.RFC3339Nano))
	}
}

func TestParseRawHTMLFixtures(t *testing.T) {
	tests := []struct {
		name         string
		fileName     string
		wantID       string
		wantTitle    string
		wantApplyURL string
		wantCompany  string
		wantCountry  string
	}{
		{
			name:         "raw-job-1",
			fileName:     "raw-job-1.html",
			wantID:       "ff70ac49-e0c3-4db2-8e79-5f13cc841cd8",
			wantTitle:    "Commercial Credit Card Team - Assistant Vice President",
			wantApplyURL: "https://mufgub.wd3.myworkdayjobs.com/MUFG-Careers/job/Tampa-FL/Commercial-Credit-Card-Team---AVP_10075900-WD",
			wantCompany:  "Mitsubishi UFJ Financial Group - MUFG",
			wantCountry:  "United States",
		},
		{
			name:         "raw-job-2",
			fileName:     "raw-job-2.html",
			wantID:       "732f7baa-3d66-469d-ae6f-7a9343536020",
			wantTitle:    "Marketing Growth Insights Specialist",
			wantApplyURL: "https://job-boards.greenhouse.io/gympass/jobs/8469249002",
			wantCompany:  "Wellhub",
			wantCountry:  "Brazil",
		},
		{
			name:         "raw-job-3",
			fileName:     "raw-job-3.html",
			wantID:       "64f454d8-02e8-475e-8bbb-f241a6868866",
			wantTitle:    "Paid Media Manager",
			wantApplyURL: "https://halstead-media-group.crewrecruiter.co/job/1013236",
			wantCompany:  "Halstead Media Group",
			wantCountry:  "United States",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			htmlPath := filepath.Join("..", "..", "..", "test-extract", "flexjobs", tc.fileName)
			htmlBytes, err := os.ReadFile(htmlPath)
			if err != nil {
				t.Fatalf("read html: %v", err)
			}
			payload, err := ParseRawHTML(string(htmlBytes), "")
			if err != nil {
				t.Fatalf("ParseRawHTML failed: %v", err)
			}
			if got := payload["id"]; got != tc.wantID {
				t.Fatalf("id mismatch got=%v want=%v", got, tc.wantID)
			}
			if got := payload["roleTitle"]; got != tc.wantTitle {
				t.Fatalf("roleTitle mismatch got=%v want=%v", got, tc.wantTitle)
			}
			if got := payload["url"]; got != tc.wantApplyURL {
				t.Fatalf("url mismatch got=%v want=%v", got, tc.wantApplyURL)
			}
			company, _ := payload["company"].(map[string]any)
			if company == nil {
				t.Fatal("expected company payload")
			}
			if got := company["name"]; got != tc.wantCompany {
				t.Fatalf("company.name mismatch got=%v want=%v", got, tc.wantCompany)
			}
			locationCountries, _ := payload["locationCountries"].([]string)
			if len(locationCountries) == 0 || locationCountries[0] != tc.wantCountry {
				t.Fatalf("locationCountries mismatch got=%#v want_first=%q", payload["locationCountries"], tc.wantCountry)
			}
			if payload["roleDescription"] == nil {
				t.Fatal("expected roleDescription")
			}
		})
	}
}
