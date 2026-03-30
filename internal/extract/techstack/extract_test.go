package techstack

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func resetMatchersForTest() {
	catalogMu.Lock()
	defaultMatchers = nil
	defaultCanonicals = nil
	catalogMu.Unlock()
}

func TestExtractMatchesMajorTechStackAliases(t *testing.T) {
	extractor := NewExtractor("")
	text := `We build APIs with Node.js, PostgreSQL, Redis, Docker, Kubernetes, and React.`
	got := extractor.Extract(text)
	want := []string{"Node.js", "PostgreSQL", "Redis", "Docker", "Kubernetes", "React"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Extract mismatch got=%#v want=%#v", got, want)
	}
}

func TestExtractAllowsSpecialCharactersAroundAlias(t *testing.T) {
	extractor := NewExtractor("")
	text := `(C#) / .NET / Node.js / ASP.NET Core / Kafka`
	got := extractor.Extract(text)
	want := []string{"C#", ".NET", "Node.js", "ASP.NET Core", "Apache Kafka"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Extract mismatch got=%#v want=%#v", got, want)
	}
}

func TestExtractKeepsDelimitedTechStacksSeparate(t *testing.T) {
	extractor := NewExtractor("")
	text := `Strong systems work in C/C++ with Python/Golang integrations.`
	got := extractor.Extract(text)
	want := []string{"C", "C++", "Python", "Go"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Extract mismatch got=%#v want=%#v", got, want)
	}
}

func TestExtractDescriptionRequirements(t *testing.T) {
	extractor := NewExtractor("")
	got := extractor.ExtractDescriptionRequirements(
		`Experience with React and TypeScript.`,
		`Must have PostgreSQL and Docker.`,
	)
	want := []string{"React", "TypeScript", "PostgreSQL", "Docker"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExtractDescriptionRequirements mismatch got=%#v want=%#v", got, want)
	}
}

func TestExtractMatchesDotNetVariants(t *testing.T) {
	extractor := NewExtractor("")
	text := `Strong experience with .Net, dotnet core, ASP Net Core, and NodeJS.`
	got := extractor.Extract(text)
	want := []string{".NET", ".NET Core", "ASP.NET Core", "Node.js"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Extract mismatch got=%#v want=%#v", got, want)
	}
}

func TestExtractRespectsCaseSensitiveAliases(t *testing.T) {
	extractor := NewExtractor("")
	text := `We need react, go, and restful apis in general prose.`
	got := extractor.Extract(text)
	if len(got) != 0 {
		t.Fatalf("expected no case-sensitive matches, got=%#v", got)
	}
}

func TestExtractKeepsCaseInsensitiveAliasesFlexible(t *testing.T) {
	extractor := NewExtractor("")
	text := `Strong experience with NODEJS, POSTGRES, and docker required.`
	got := extractor.Extract(text)
	want := []string{"Node.js", "PostgreSQL", "Docker"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Extract mismatch got=%#v want=%#v", got, want)
	}
}

func TestExtractMatchesBroaderTechnologyCategories(t *testing.T) {
	extractor := NewExtractor("")
	text := `We use OAuth2, OpenAPI, Kafka, Redis, Terraform, GitHub Actions, Playwright, Keycloak, Pandas, and Cloudflare Workers.`
	got := extractor.Extract(text)
	want := []string{
		"OAuth 2.0",
		"OpenAPI",
		"Apache Kafka",
		"Redis",
		"Terraform",
		"GitHub Actions",
		"Playwright",
		"Keycloak",
		"Pandas",
		"Cloudflare Workers",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Extract mismatch got=%#v want=%#v", got, want)
	}
}

func TestExtractReloadsFromExternalCatalog(t *testing.T) {
	t.Cleanup(resetMatchersForTest)

	version := "v1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch version {
		case "v1":
			w.Header().Set("ETag", `"v1"`)
			if r.Header.Get("If-None-Match") == `"v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = w.Write([]byte(`[
  {"canonical":"Alpha","aliases":["alpha"],"caseSensitive":false}
]`))
		case "v2":
			w.Header().Set("ETag", `"v2"`)
			if r.Header.Get("If-None-Match") == `"v2"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = w.Write([]byte(`[
  {"canonical":"Beta","aliases":["beta"],"caseSensitive":false}
]`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	extractor := NewExtractor(server.URL)

	if got := extractor.Extract("alpha"); !reflect.DeepEqual(got, []string{"Alpha"}) {
		t.Fatalf("expected alpha from external catalog, got=%#v", got)
	}

	version = "v2"
	extractor = NewExtractor(server.URL)

	if got := extractor.Extract("alpha"); len(got) != 0 {
		t.Fatalf("expected alpha to disappear after reload, got=%#v", got)
	}
	if got := extractor.Extract("beta"); !reflect.DeepEqual(got, []string{"Beta"}) {
		t.Fatalf("expected beta after reload, got=%#v", got)
	}
}
