package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSourceOlderThanPostDateReturnsTrue(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 9, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if !isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected source to be older than post date")
	}
}

func TestNormalizeLocationFieldsKeepsNonASCIIUnicodeCityValid(t *testing.T) {
	country, city, states := normalizeLocationFields("Japan", "横浜市", nil)

	if country != nil {
		t.Fatalf("expected nil country for plain non-segmented location input, got %#v", country)
	}
	if got, ok := city.(string); !ok || got != "横浜市" {
		t.Fatalf("expected city 横浜市, got %#v", city)
	}
	if got, ok := states.(string); !ok || got != "[]" {
		t.Fatalf("expected empty states json, got %#v", states)
	}
}

func TestFixtureRawJSONContainsOnlyValidUTF8Strings(t *testing.T) {
	for _, relativePath := range []string{
		filepath.Join("test-extract", "remoterocketship", "raw-json-1.json"),
		filepath.Join("test-extract", "workable", "raw-json-1.json"),
		filepath.Join("test-extract", "workable", "raw-json-2.json"),
	} {
		relativePath := relativePath
		t.Run(filepath.ToSlash(relativePath), func(t *testing.T) {
			body, err := os.ReadFile(parsedFixturePath(t, relativePath))
			if err != nil {
				t.Fatal(err)
			}

			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("unmarshal fixture: %v", err)
			}

			assertParsedFixtureUTF8(t, relativePath, "payload", payload)
		})
	}
}

func TestProcessPendingWithFixtureRawJSONs(t *testing.T) {
	cases := []struct {
		name         string
		source       string
		fixturePath  string
		rawURL       string
		roleTitleKey string
	}{
		{
			name:         "remoterocketship_raw_json_1",
			source:       "remoterocketship",
			fixturePath:  filepath.Join("test-extract", "remoterocketship", "raw-json-1.json"),
			rawURL:       "https://www.remoterocketship.com/company/sgs/jobs/ai-hybrid",
			roleTitleKey: "roleTitle",
		},
		{
			name:         "workable_raw_json_1",
			source:       "workable",
			fixturePath:  filepath.Join("test-extract", "workable", "raw-json-1.json"),
			rawURL:       "https://jobs.workable.com/view/9axVwP295CRkysSH12WeRD/remote-polish-speaking-customer-service-agent---work-in-sofia---fully-paid-relocation-in-%C5%82%C3%B3d%C5%BA-at-patrique-mercier-recruitment-by-nellie",
			roleTitleKey: "roleTitle",
		},
		{
			name:         "workable_raw_json_2",
			source:       "workable",
			fixturePath:  filepath.Join("test-extract", "workable", "raw-json-2.json"),
			rawURL:       "https://jobs.workable.com/view/acFhKm7FnqhfvbQ68ew3T4/remote-slovak-speaking-customer-service-agent---work-in-sofia---fully-paid-relocation-in-%C5%BEilina-at-patrique-mercier-recruitment-by-nellie",
			roleTitleKey: "roleTitle",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db, err := database.Open(testDatabaseURL(t, "test_parsed_fixture_process_"+tc.name))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			body, err := os.ReadFile(parsedFixturePath(t, tc.fixturePath))
			if err != nil {
				t.Fatal(err)
			}

			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("unmarshal fixture: %v", err)
			}

			_, err = db.SQL.ExecContext(
				context.Background(),
				`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
				 VALUES (1, ?, ?, ?, true, false, false, 0, ?)`,
				tc.source,
				tc.rawURL,
				time.Now().UTC().Format(time.RFC3339Nano),
				string(body),
			)
			if err != nil {
				t.Fatal(err)
			}

			svc := New(Config{}, db)
			svc.EnabledSources = map[string]struct{}{tc.source: {}}
			processed, err := svc.ProcessPending(context.Background(), 10)
			if err != nil {
				t.Fatalf("ProcessPending failed: %v", err)
			}
			if processed != 1 {
				t.Fatalf("expected processed=1, got %d", processed)
			}

			var isParsed bool
			if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_parsed FROM raw_us_jobs WHERE id = 1`).Scan(&isParsed); err != nil {
				t.Fatal(err)
			}
			if !isParsed {
				t.Fatal("expected raw row to be marked parsed")
			}

			var parsedCount int
			if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs WHERE raw_us_job_id = 1`).Scan(&parsedCount); err != nil {
				t.Fatal(err)
			}
			if parsedCount != 1 {
				t.Fatalf("expected one parsed job row, got %d", parsedCount)
			}

			var storedRoleTitle sql.NullString
			if err := db.SQL.QueryRowContext(context.Background(), `SELECT role_title FROM parsed_jobs WHERE raw_us_job_id = 1`).Scan(&storedRoleTitle); err != nil {
				t.Fatal(err)
			}
			expectedRoleTitle := strings.TrimSpace(stringValue(payload[tc.roleTitleKey]))
			if expectedRoleTitle != "" && storedRoleTitle.String != expectedRoleTitle {
				t.Fatalf("expected role_title %q, got %q", expectedRoleTitle, storedRoleTitle.String)
			}
		})
	}
}

func TestUpsertCompanyFromFixtureRawJSONs(t *testing.T) {
	cases := []struct {
		name        string
		source      string
		fixturePath string
	}{
		{
			name:        "remoterocketship_raw_json_1",
			source:      "remoterocketship",
			fixturePath: filepath.Join("test-extract", "remoterocketship", "raw-json-1.json"),
		},
		{
			name:        "workable_raw_json_1",
			source:      "workable",
			fixturePath: filepath.Join("test-extract", "workable", "raw-json-1.json"),
		},
		{
			name:        "workable_raw_json_2",
			source:      "workable",
			fixturePath: filepath.Join("test-extract", "workable", "raw-json-2.json"),
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db, err := database.Open(testDatabaseURL(t, "test_parsed_fixture_company_"+tc.name))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			body, err := os.ReadFile(parsedFixturePath(t, tc.fixturePath))
			if err != nil {
				t.Fatal(err)
			}

			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("unmarshal fixture: %v", err)
			}

			svc := New(Config{}, db)
			plugin, ok := plugins.Get(tc.source)
			if !ok {
				t.Fatalf("missing plugin for source %q", tc.source)
			}

			if _, err := svc.upsertCompanyFromPayload(context.Background(), payload, plugin); err != nil {
				t.Fatalf("upsertCompanyFromPayload failed: %v", err)
			}
		})
	}
}

func TestParsedJobInsertArgsFromFixtureAreUTF8Safe(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_fixture_insert_args_utf8"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	body, err := os.ReadFile(parsedFixturePath(t, filepath.Join("test-extract", "remoterocketship", "raw-json-1.json")))
	if err != nil {
		t.Fatal(err)
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}

	svc := New(Config{}, db)
	plugin, ok := plugins.Get("remoterocketship")
	if !ok {
		t.Fatal("missing remoterocketship plugin")
	}

	normalizedTechStack := normalizeTechStack(payload["techStack"])
	_, normalizedLocationCity, normalizedUSStates := normalizeLocationFields(
		payload["location"],
		payload["locationCity"],
		payload["locationUSStates"],
	)
	normalizedLocationCountries := normalizeLocationCountries(payload["locationCountries"])
	companyID, err := svc.upsertCompanyFromPayload(context.Background(), payload, plugin)
	if err != nil {
		t.Fatalf("upsertCompanyFromPayload failed: %v", err)
	}
	categorizedTitle := stringFromPayload(payload["categorizedJobTitle"])
	categorizedFunction := stringFromPayload(payload["categorizedJobFunction"])
	normalizedTechStack = extractManualTechStackIfNeeded(
		svc.techStackExtractor,
		stringValue(payload["roleDescription"]),
		stringValue(payload["roleRequirements"]),
		normalizedTechStack,
		plugin.UseManualTechStackExtraction,
		stringValue(categorizedTitle),
		stringValue(categorizedFunction),
	)
	normalizedTechStackJSON := jsonStringOrNil(normalizedTechStack)

	argNames := []string{
		"raw_us_job_id", "company_id", "external_job_id", "created_at_source", "valid_until_date", "date_deleted", "description_language",
		"role_title", "role_description", "role_requirements", "benefits", "job_description_summary", "two_line_job_description_summary",
		"role_title_brazil", "role_description_brazil", "role_requirements_brazil", "benefits_brazil", "slug_brazil", "job_description_summary_brazil", "two_line_job_description_summary_brazil",
		"role_title_france", "role_description_france", "role_requirements_france", "benefits_france", "slug_france", "job_description_summary_france", "two_line_job_description_summary_france",
		"role_title_germany", "role_description_germany", "role_requirements_germany", "benefits_germany", "slug_germany", "job_description_summary_germany", "two_line_job_description_summary_germany",
		"url", "slug", "employment_type", "location_type", "location_city",
		"categorized_job_title", "categorized_job_function", "education_requirements_credential_category",
		"experience_in_place_of_education", "experience_requirements_months",
		"is_on_linkedin", "is_promoted", "is_entry_level", "is_junior", "is_mid_level", "is_senior", "is_lead",
		"required_languages", "location_us_states", "location_countries", "tech_stack",
		"salary_min", "salary_max", "salary_type", "salary_currency_code", "salary_currency_symbol", "salary_min_usd", "salary_max_usd", "salary_human_text",
		"updated_at",
	}

	args := []any{
		int64(1),
		companyID,
		stringFromPayload(payload["id"]),
		formatNullableTime(parseDT(payload["created_at"])),
		formatNullableTime(parseDT(payload["validUntilDate"])),
		formatNullableTime(parseDT(payload["dateDeleted"])),
		stringFromPayload(payload["descriptionLanguage"]),
		stringFromPayload(payload["roleTitle"]),
		stringFromPayload(payload["roleDescription"]),
		stringFromPayload(payload["roleRequirements"]),
		stringFromPayload(payload["benefits"]),
		stringFromPayload(payload["jobDescriptionSummary"]),
		stringFromPayload(payload["twoLineJobDescriptionSummary"]),
		stringFromPayload(payload["roleTitleBrazil"]),
		stringFromPayload(payload["roleDescriptionBrazil"]),
		stringFromPayload(payload["roleRequirementsBrazil"]),
		stringFromPayload(payload["benefitsBrazil"]),
		stringFromPayload(payload["slugBrazil"]),
		stringFromPayload(payload["jobDescriptionSummaryBrazil"]),
		stringFromPayload(payload["twoLineJobDescriptionSummaryBrazil"]),
		stringFromPayload(payload["roleTitleFrance"]),
		stringFromPayload(payload["roleDescriptionFrance"]),
		stringFromPayload(payload["roleRequirementsFrance"]),
		stringFromPayload(payload["benefitsFrance"]),
		stringFromPayload(payload["slugFrance"]),
		stringFromPayload(payload["jobDescriptionSummaryFrance"]),
		stringFromPayload(payload["twoLineJobDescriptionSummaryFrance"]),
		stringFromPayload(payload["roleTitleGermany"]),
		stringFromPayload(payload["roleDescriptionGermany"]),
		stringFromPayload(payload["roleRequirementsGermany"]),
		stringFromPayload(payload["benefitsGermany"]),
		stringFromPayload(payload["slugGermany"]),
		stringFromPayload(payload["jobDescriptionSummaryGermany"]),
		stringFromPayload(payload["twoLineJobDescriptionSummaryGermany"]),
		stringFromPayload(payload["url"]),
		stringFromPayload(payload["slug"]),
		normalizeEmploymentTypeValue(payload["employmentType"]),
		stringFromPayload(payload["locationType"]),
		normalizedLocationCity,
		categorizedTitle,
		categorizedFunction,
		normalizeEducationCredentialCategory(payload["educationRequirementsCredentialCategory"]),
		_normalizeNullStringToNone(payload["experienceInPlaceOfEducation"]),
		_normalizeNullStringToNone(payload["experienceRequirementsMonthsOfExperience"]),
		_normalizeNullStringToNone(payload["isOnLinkedIn"]),
		_normalizeNullStringToNone(payload["isPromoted"]),
		_normalizeNullStringToNone(payload["isEntryLevel"]),
		_normalizeNullStringToNone(payload["isJunior"]),
		_normalizeNullStringToNone(payload["isMidLevel"]),
		_normalizeNullStringToNone(payload["isSenior"]),
		_normalizeNullStringToNone(payload["isLead"]),
		normalizedJSONArrayText(_normalizeNullStringToNone(payload["requiredLanguages"])),
		normalizedUSStates,
		normalizedLocationCountries,
		normalizedTechStackJSON,
		_normalizeNullStringToNone(mapValue(payload, "salaryRange", "min")),
		_normalizeNullStringToNone(mapValue(payload, "salaryRange", "max")),
		_normalizeNullStringToNone(mapValue(payload, "salaryRange", "salaryType")),
		_normalizeNullStringToNone(mapValue(payload, "salaryRange", "currencyCode")),
		_normalizeNullStringToNone(mapValue(payload, "salaryRange", "currencySymbol")),
		_normalizeNullStringToNone(mapValue(payload, "salaryRange", "minSalaryAsUSD")),
		_normalizeNullStringToNone(mapValue(payload, "salaryRange", "maxSalaryAsUSD")),
		_normalizeNullStringToNone(mapValue(payload, "salaryRange", "salaryHumanReadableText")),
		time.Now().UTC().Format(time.RFC3339Nano),
	}

	for idx, arg := range args {
		if idx >= len(argNames) {
			break
		}
		text, ok := arg.(string)
		if !ok || text == "" {
			continue
		}
		if _, err := db.SQL.ExecContext(context.Background(), `SELECT ?::text`, text); err != nil {
			t.Fatalf("arg %d (%s) failed text probe: %v", idx+1, argNames[idx], err)
		}
	}
}

func parsedFixturePath(t *testing.T, relativePath string) string {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	return filepath.Join(repoRoot, relativePath)
}

func assertParsedFixtureUTF8(t *testing.T, fixtureName, path string, value any) {
	t.Helper()

	switch item := value.(type) {
	case string:
		if !utf8.ValidString(item) {
			t.Fatalf("%s has invalid UTF-8 at %s", fixtureName, path)
		}
	case []any:
		for idx, child := range item {
			assertParsedFixtureUTF8(t, fixtureName, fmt.Sprintf("%s[%d]", path, idx), child)
		}
	case map[string]any:
		for key, child := range item {
			assertParsedFixtureUTF8(t, fixtureName, fmt.Sprintf("%s.%s", path, key), child)
		}
	}
}

func TestSourceEqualToPostDateReturnsFalse(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected equal timestamps to be allowed")
	}
}

func TestSourceNewerThanPostDateReturnsFalse(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 11, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected newer source timestamp to be allowed")
	}
}

func TestMissingSourceDateReturnsFalse(t *testing.T) {
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if isSourceOlderThanPostDate(nil, &postDate) {
		t.Fatal("expected missing source timestamp to be allowed")
	}
}

func TestNaiveAndAwareDatetimesAreComparedSafely(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 9, 0, 0, 0, time.FixedZone("UTC", 0))
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.Local)
	if !isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected timestamps to be normalized before comparison")
	}
}

func TestProcessPendingKeepsParsingWhenSourceCreatedAtIsOlderThanPostDate(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_stale"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	payload, err := json.Marshal(map[string]any{
		"created_at":          "2026-02-12T09:00:00Z",
		"url":                 "https://example.com/jobs/1",
		"categorizedJobTitle": "Software Engineer",
		"roleTitle":           "Backend Engineer",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, true, false, false, 0, ?)`,
		"https://example.com/jobs/1",
		"2026-02-12T10:00:00Z",
		string(payload),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"remoterocketship": {}}
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected one row processed, got %d", processed)
	}

	var isReady, isParsed bool
	var rawJSONText string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_ready, is_parsed, raw_json FROM raw_us_jobs WHERE url = ?`, "https://example.com/jobs/1").Scan(&isReady, &isParsed, &rawJSONText); err != nil {
		t.Fatal(err)
	}
	if !isReady || !isParsed {
		t.Fatalf("expected row to stay ready and parsed, got is_ready=%t is_parsed=%t", isReady, isParsed)
	}
	if rawJSONText == "" {
		t.Fatal("expected raw payload to remain populated")
	}

	var parsedCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs WHERE raw_us_job_id = 1`).Scan(&parsedCount); err != nil {
		t.Fatal(err)
	}
	if parsedCount != 1 {
		t.Fatalf("expected parsed row to be created, got %d", parsedCount)
	}
}

func TestTokenizeRoleTitleRemovesSeniorityTokens(t *testing.T) {
	tokens := tokenizeRoleTitleForSimilarity("Senior Staff Backend Engineer Python")
	if _, ok := tokens["senior"]; ok {
		t.Fatal("expected senior to be removed")
	}
	if _, ok := tokens["staff"]; ok {
		t.Fatal("expected staff to be removed")
	}
	if _, ok := tokens["backend"]; !ok {
		t.Fatal("expected backend token")
	}
}

func TestNormalizeTextForMatchingExpandsCommonAbbreviations(t *testing.T) {
	normalized := normalizeTextForMatching("AVP PMM SRE HRBP SEO")
	expectedParts := []string{
		"assistant vice president",
		"product marketing manager",
		"site reliability engineer",
		"human resources business partner",
		"search engine optimization",
	}
	for _, expected := range expectedParts {
		if !strings.Contains(normalized, expected) {
			t.Fatalf("expected %q in normalized text %q", expected, normalized)
		}
	}
}

func TestTokenizeRoleTitleRemovesEmploymentTokens(t *testing.T) {
	tokens := tokenizeRoleTitleForSimilarity("Full Time Contract Backend Engineer")
	if _, ok := tokens["full"]; ok {
		t.Fatal("expected full to be removed")
	}
	if _, ok := tokens["contract"]; ok {
		t.Fatal("expected contract to be removed")
	}
	if _, ok := tokens["backend"]; !ok {
		t.Fatal("expected backend token")
	}
}

func TestTokenizeRoleTitleRemovesWorkModeNoise(t *testing.T) {
	tokens := tokenizeRoleTitleForSimilarity("Remote Hybrid Backend Engineer")
	if _, ok := tokens["remote"]; ok {
		t.Fatal("expected remote to be removed")
	}
	if _, ok := tokens["hybrid"]; ok {
		t.Fatal("expected hybrid to be removed")
	}
	if _, ok := tokens["backend"]; !ok {
		t.Fatal("expected backend token")
	}
}

func TestJaccardSimilarityWorksForOverlap(t *testing.T) {
	left := map[string]struct{}{"backend": {}, "engineer": {}, "python": {}}
	right := map[string]struct{}{"backend": {}, "engineer": {}, "go": {}}
	if got := jaccardSimilarity(left, right); got != 0.5 {
		t.Fatalf("expected 0.5 similarity, got %v", got)
	}
}

func TestBuiltinBackfillsCategoriesFromSimilarRemoteJob(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_builtin_backfill"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (1, 'remoterocketship', 'https://remote.example/jobs/1', ?, true, false, true, 0, '{}')`, time.Date(2026, 2, 17, 0, 0, 0, 0, time.UTC).Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, updated_at) VALUES (1, 'Senior Backend Platform Engineer Python', 'Backend Engineer', 'Engineering', ?)`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"created_at": "2026-02-12T09:00:00Z",
		"url":        "https://builtin.com/job/acme/200",
		"roleTitle":  "Backend Platform Engineer",
		"techStack":  []string{"Python"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (2, 'builtin', 'https://builtin.com/job/acme/200', ?, true, false, false, 0, ?)`, time.Date(2026, 2, 17, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), string(payload))
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	if _, err := svc.ProcessPending(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	var title, function sql.NullString
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT categorized_job_title, categorized_job_function FROM parsed_jobs WHERE raw_us_job_id = 2`).Scan(&title, &function); err != nil {
		t.Fatal(err)
	}
	if title.String != "Backend Engineer" || function.String != "Engineering" {
		t.Fatalf("expected inferred categories, got %q / %q", title.String, function.String)
	}
}

func TestOrderedTokenMatchScorePrefersSpecificCategory(t *testing.T) {
	if score := orderedTokenMatchScore("Senior Product Implementation Engineer I II", "Implementation Engineer"); score <= 0.5 {
		t.Fatalf("expected strong ordered match score, got %v", score)
	}
}

func TestFindSimilarRemoteCategoriesAvoidsGenericEngineer(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_weighted_similarity"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (1, 'remoterocketship', 'https://remote.example/jobs/engineer', ?, true, false, true, 0, '{}')`, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (2, 'remoterocketship', 'https://remote.example/jobs/implementation-engineer', ?, true, false, true, 0, '{}')`, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, updated_at) VALUES (1, 'Engineer', 'Engineer', 'Engineering', ?), (2, 'Consultant', 'Implementation Specialist', 'Engineering', ?)`, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Product Implementation Engineer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Implementation Specialist" || function != "Engineering" {
		t.Fatalf("expected specific category match, got %q / %q", title, function)
	}
}

func TestNormalizeRoleTitleForExactMatchHandlesCommonAbbreviations(t *testing.T) {
	got := normalizeRoleTitleForExactMatch("Senior SWE, Dev Ops")
	if got != "software engineer devops" {
		t.Fatalf("unexpected normalized title %q", got)
	}
}

func TestCategorySignalWeightBoostsSalesForSalespersonTitles(t *testing.T) {
	score := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "salesperson tech services", "Sales", "Sales")
	if score <= 0 {
		t.Fatalf("expected positive category signal score, got %v", score)
	}
}

func TestCategorySignalWeightUsesConfiguredCategoryTokens(t *testing.T) {
	score := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "sales software project managing engineer", "Sales", "Sales")
	if score <= 0 {
		t.Fatalf("expected positive category signal score from configured category tokens, got %v", score)
	}
}

func TestCategorySignalWeightGivesGenericEngineerLowPositiveWeight(t *testing.T) {
	score := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "principal engineer", "Engineer", "Engineering")
	if score <= 0 {
		t.Fatalf("expected low positive category signal score for generic engineer, got %v", score)
	}
}

func TestCategorySignalWeightPrefersGenericEngineerOverSpecificEngineerWhenOnlyEngineerMatches(t *testing.T) {
	engineerScore := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "washingmachine engineer", "Engineer", "Engineering")
	backendScore := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "washingmachine engineer", "Backend Engineer", "Engineering")
	if engineerScore <= backendScore {
		t.Fatalf("expected generic engineer score %v to exceed backend engineer score %v", engineerScore, backendScore)
	}
}

func TestCategorySignalWeightPrefersGenericDesignerOverSpecificDesignerWhenOnlyDesignerMatches(t *testing.T) {
	designerScore := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "washingmachine designer", "Designer", "Design")
	webDesignerScore := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "washingmachine designer", "Web Designer", "Design")
	if designerScore <= webDesignerScore {
		t.Fatalf("expected generic designer score %v to exceed web designer score %v", designerScore, webDesignerScore)
	}
}

func TestCategorySignalWeightUsesDeveloperForSoftwareEngineer(t *testing.T) {
	softwareScore := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "react web software developer", "Software Engineer", "Engineering")
	webDesignerScore := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "react web software developer", "Web Designer", "Design")
	if softwareScore <= webDesignerScore {
		t.Fatalf("expected software engineer score %v to exceed web designer score %v", softwareScore, webDesignerScore)
	}
}

func TestCategorySignalWeightUsesDeveloperForBackendEngineer(t *testing.T) {
	score := categorySignalWeightFromCatalog(getCategorySignalCatalog(""), "senior backend developer", "Backend Engineer", "Engineering")
	if score <= 0 {
		t.Fatalf("expected positive category signal score for backend developer, got %v", score)
	}
}

func TestFindSimilarRemoteCategoriesPrefersExactNormalizedRoleTitle(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_exact_normalized"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES
		(1, 'remoterocketship', 'https://remote.example/jobs/devops', ?, true, false, true, 0, '{}'),
		(2, 'remoterocketship', 'https://remote.example/jobs/software-engineer', ?, true, false, true, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, updated_at) VALUES
		(1, 'Senior SWE DevOps', 'DevOps Engineer', 'Engineering', ?),
		(2, 'Senior Software Engineer', 'Software Engineer', 'Engineering', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Senior SWE Dev Ops", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "DevOps Engineer" || function != "Engineering" {
		t.Fatalf("expected exact normalized match, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesPrefersSalesCandidateWhenSalespersonSignalExists(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_salesperson_signal"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES
		(1, 'remoterocketship', 'https://remote.example/jobs/software', ?, true, false, true, 0, '{}'),
		(2, 'remoterocketship', 'https://remote.example/jobs/sales', ?, true, false, true, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, updated_at) VALUES
		(1, 'Tech Services Software Engineer', 'Software Engineer', 'Engineering', ?),
		(2, 'Wholesale Parts Salesperson', 'Sales', 'Sales', ?)`,
		time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Salesperson - Tech Services (Remote)", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Sales" || function != "Sales" {
		t.Fatalf("expected Sales/Sales, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesUsesTechStackFilter(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_tech_stack_filter"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES
		(1, 'remoterocketship', 'https://remote.example/jobs/platform-ruby', ?, true, false, true, 0, '{}'),
		(2, 'remoterocketship', 'https://remote.example/jobs/platform-java', ?, true, false, true, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, tech_stack, updated_at) VALUES
		(1, 'Platform Engineer', 'Software Engineer', 'Engineering', '["Ruby"]', ?),
		(2, 'Platform Engineer', 'Account Executive', 'Sales', '["Java"]', ?)`,
		time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Platform Engineer", []string{"Ruby"})
	if err != nil {
		t.Fatal(err)
	}
	if title != "Software Engineer" || function != "Engineering" {
		t.Fatalf("expected tech-filtered category match, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesFallsBackWhenTechStackFilterHasNoMatch(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_tech_stack_filter_fallback"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES
		(1, 'remoterocketship', 'https://remote.example/jobs/backend-python', ?, true, false, true, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, tech_stack, updated_at) VALUES
		(1, 'Backend Engineer', 'Backend Engineer', 'Engineering', '["Python"]', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Backend Engineer", []string{"Rust"})
	if err != nil {
		t.Fatal(err)
	}
	if title != "Backend Engineer" || function != "Engineering" {
		t.Fatalf("expected fallback category match, got %q / %q", title, function)
	}
}

func TestNormalizeEmploymentTypeValuePreservesPayloadValue(t *testing.T) {
	if got := normalizeEmploymentTypeValue("full_time"); got != "full_time" {
		t.Fatalf("expected full_time, got %#v", got)
	}
	if got := normalizeEmploymentTypeValue("contractor"); got != "contractor" {
		t.Fatalf("expected contractor, got %#v", got)
	}
	if got := normalizeEmploymentTypeValue("intern"); got != "intern" {
		t.Fatalf("expected intern, got %#v", got)
	}
	if got := normalizeEmploymentTypeValue("temporary"); got != "temporary" {
		t.Fatalf("expected temporary, got %#v", got)
	}
}

func TestNormalizeLocationFieldsPrefersUnitedStatesWhenMultipleCountries(t *testing.T) {
	location, city, states := normalizeLocationFields(
		"ESP | FRA | Belgium, Wisconsin, USA | London, England, GBR",
		nil,
		[]any{},
	)
	if location != "United States" {
		t.Fatalf("expected location United States, got %#v", location)
	}
	if city != "Belgium" {
		t.Fatalf("expected city Belgium, got %#v", city)
	}
	if states != "[\"Wisconsin\"]" {
		t.Fatalf("expected states json [\"Wisconsin\"], got %#v", states)
	}
}

func TestNormalizeLocationFieldsTitleCasesCityAndState(t *testing.T) {
	location, city, states := normalizeLocationFields("new york, new york, usa", nil, []any{})
	if location != "United States" {
		t.Fatalf("expected United States location, got %#v", location)
	}
	if city != "New York" {
		t.Fatalf("expected New York city, got %#v", city)
	}
	if states != "[\"New York\"]" {
		t.Fatalf("expected states json [\"New York\"], got %#v", states)
	}
}

func TestNormalizeEducationCredentialCategoryLowercases(t *testing.T) {
	if got := normalizeEducationCredentialCategory("Bachelor Degree"); got != "bachelor degree" {
		t.Fatalf("expected lowercase value, got %#v", got)
	}
	if got := normalizeEducationCredentialCategory("  MASTER'S   DEGREE  "); got != "master's degree" {
		t.Fatalf("expected normalized lowercase spacing, got %#v", got)
	}
}

func TestNormalizeTechStackAliasesAndDedupes(t *testing.T) {
	got := normalizeTechStack([]any{"react.js", "React", "  node js  ", "NODE.JS", "GoLang", ""})
	if len(got) != 3 || got[0] != "React" || got[1] != "Node.js" || got[2] != "Go" {
		t.Fatalf("unexpected normalized tech stack %#v", got)
	}
}

func TestNormalizeTechStackKeepsUnknownValuesTrimmed(t *testing.T) {
	got := normalizeTechStack([]any{"  Elixir  ", "Phoenix LiveView"})
	if len(got) != 2 || got[0] != "Elixir" || got[1] != "Phoenix LiveView" {
		t.Fatalf("unexpected normalized tech stack %#v", got)
	}
}

func TestNormalizeTechStackCleansNoisyValuesAndAliases(t *testing.T) {
	got := normalizeTechStack([]any{"Css)", "Ci/Cd (Azure Devops", "Google Tag Manager (Gtm)", "SFDC", "APIs", "n/a"})
	if len(got) != 5 || got[0] != "CSS" || got[1] != "CI/CD" || got[2] != "Google Tag Manager" || got[3] != "Salesforce" || got[4] != "API" {
		t.Fatalf("unexpected normalized tech stack %#v", got)
	}
}

func TestNormalizeTechStackPreservesLeadingDot(t *testing.T) {
	got := normalizeTechStack([]any{".net", " dotnet. "})
	if len(got) != 1 || got[0] != ".NET" {
		t.Fatalf("unexpected normalized tech stack %#v", got)
	}
}

func TestStringFromPayloadTreatsStringNullAsNil(t *testing.T) {
	if got := stringFromPayload("null"); got != nil {
		t.Fatalf("expected nil from string null payload, got %#v", got)
	}
	if got := stringFromPayload(" NULL "); got != nil {
		t.Fatalf("expected nil from uppercase string null payload, got %#v", got)
	}
}

func TestUpsertCompanyFromPayloadUsesExternalCompanyIDForRemoteRocketship(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_company_external_id"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_companies (external_company_id, name, home_page_url, updated_at) VALUES (?, ?, ?, ?)`,
		externalCompanyIDPrefix+"rr_company_1"+externalCompanyIDSuffix,
		"Old Name",
		"https://old.example",
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	plugin, ok := plugins.Get("remoterocketship")
	if !ok {
		t.Fatal("missing remoterocketship plugin")
	}
	payload := map[string]any{
		"company": map[string]any{
			"id":          "rr_company_1",
			"name":        "New Name",
			"homePageURL": "https://new.example",
		},
	}
	companyID, err := svc.upsertCompanyFromPayload(context.Background(), payload, plugin)
	if err != nil {
		t.Fatal(err)
	}
	if companyID == nil {
		t.Fatal("expected company id")
	}

	var name, homePage string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT name, home_page_url FROM parsed_companies WHERE external_company_id ILIKE ?`, "%"+externalCompanyIDPrefix+"rr_company_1"+externalCompanyIDSuffix+"%").Scan(&name, &homePage); err != nil {
		t.Fatal(err)
	}
	if name != "New Name" || homePage != "https://new.example" {
		t.Fatalf("expected updated company fields, got name=%q home_page_url=%q", name, homePage)
	}
}

func TestUpsertCompanyFromPayloadMatchesMergedExternalCompanyIDList(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_company_external_id_merged"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_companies (external_company_id, name, home_page_url, updated_at) VALUES (?, ?, ?, ?)`,
		externalCompanyIDPrefix+"rr_company_1"+externalCompanyIDSuffix+","+externalCompanyIDPrefix+"rr_company_2"+externalCompanyIDSuffix,
		"Existing Name",
		"https://existing.example",
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	plugin, ok := plugins.Get("remoterocketship")
	if !ok {
		t.Fatal("missing remoterocketship plugin")
	}
	payload := map[string]any{
		"company": map[string]any{
			"id":          "rr_company_2",
			"name":        "Merged Match Name",
			"homePageURL": "https://merged.example",
		},
	}
	companyID, err := svc.upsertCompanyFromPayload(context.Background(), payload, plugin)
	if err != nil {
		t.Fatal(err)
	}
	if companyID == nil {
		t.Fatal("expected company id")
	}

	var count int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_companies`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected merged external id list to update existing row, got %d company rows", count)
	}

	var name, homePage string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT name, home_page_url FROM parsed_companies WHERE external_company_id ILIKE ?`, "%"+externalCompanyIDPrefix+"rr_company_2"+externalCompanyIDSuffix+"%").Scan(&name, &homePage); err != nil {
		t.Fatal(err)
	}
	if name != "Merged Match Name" || homePage != "https://merged.example" {
		t.Fatalf("expected updated company fields, got name=%q home_page_url=%q", name, homePage)
	}
}

func TestUpsertCompanyFromPayloadAppendsIncomingExternalCompanyIDWhenMatchedByKeys(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_company_external_id_append_on_match"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_companies (external_company_id, name, slug, home_page_url, updated_at) VALUES (?, ?, ?, ?, ?)`,
		externalCompanyIDPrefix+"rr_company_1"+externalCompanyIDSuffix,
		"Acme",
		"acme",
		"https://acme.example",
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	plugin, ok := plugins.Get("remoterocketship")
	if !ok {
		t.Fatal("missing remoterocketship plugin")
	}
	payload := map[string]any{
		"company": map[string]any{
			"id":          "rr_company_2",
			"name":        "Acme",
			"slug":        "acme",
			"homePageURL": "https://acme.example",
		},
	}
	companyID, err := svc.upsertCompanyFromPayload(context.Background(), payload, plugin)
	if err != nil {
		t.Fatal(err)
	}
	if companyID == nil {
		t.Fatal("expected company id")
	}

	var externalIDs string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT external_company_id FROM parsed_companies WHERE name = ?`, "Acme").Scan(&externalIDs); err != nil {
		t.Fatal(err)
	}
	if externalIDs != externalCompanyIDPrefix+"rr_company_1"+externalCompanyIDSuffix+","+externalCompanyIDPrefix+"rr_company_2"+externalCompanyIDSuffix {
		t.Fatalf("expected appended external company ids, got %q", externalIDs)
	}
}

func TestFindDuplicateCrossSourceParsedJobByURL(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_by_url"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'remoterocketship', 'https://example.com/job/a', ?, true, false, true, 0, '{}'),
		        (2, 'builtin', 'https://example.com/job/b', ?, true, false, false, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, url, role_title, updated_at) VALUES (1, 'https://example.com/job/shared', 'Backend Engineer', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	payload := map[string]any{
		"url":       "https://example.com/job/shared",
		"roleTitle": "Backend Engineer",
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "builtin", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate || duplicateID <= 0 {
		t.Fatalf("expected duplicate by url, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}

func TestFindDuplicateCrossSourceParsedJobByNormalizedURLWithinDateWindow(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_by_external_id"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'remoterocketship', 'https://example.com/job/a', ?, true, false, true, 0, '{}'),
		        (2, 'dailyremote', 'https://example.com/job/b', ?, true, false, false, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, url, role_title, created_at_source, updated_at)
		 VALUES (1, 'https://example.com/job/shared', 'Backend Engineer', ?, ?)`,
		time.Now().UTC().Add(-12*time.Hour).Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	payload := map[string]any{
		"url":        "https://www.example.com/job/shared/",
		"created_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "dailyremote", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate || duplicateID <= 0 {
		t.Fatalf("expected duplicate by normalized url, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}

func TestFindDuplicateCrossSourceParsedJobByNormalizedURLIgnoringSubdomain(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_by_subdomain"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'dailyremote', 'https://job-boards.greenhouse.io/techholding/jobs/4676811005?ref=dailyremote', ?, true, false, true, 0, '{}'),
		        (2, 'remoterocketship', 'https://boards.greenhouse.io/techholding/jobs/4676811005', ?, true, false, false, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, url, role_title, updated_at)
		 VALUES (1, 'https://job-boards.greenhouse.io/techholding/jobs/4676811005?ref=dailyremote&utm_source=dailyremote.com', 'Backend Engineer', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	payload := map[string]any{
		"url": "https://boards.greenhouse.io/techholding/jobs/4676811005",
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "remoterocketship", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate || duplicateID <= 0 {
		t.Fatalf("expected duplicate by normalized subdomain-insensitive url, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}

func TestFindDuplicateCrossSourceParsedJobSkipsEmailApplyTargets(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_skip_email_url"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'dailyremote', 'https://example.com/source-a', ?, true, false, true, 0, '{}'),
		        (2, 'builtin', 'https://example.com/source-b', ?, true, false, false, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, url, role_title, updated_at)
		 VALUES (1, 'jobs@example.com', 'Backend Engineer', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	payload := map[string]any{
		"url":       "jobs@example.com",
		"roleTitle": "Backend Engineer",
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "builtin", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if isDuplicate || duplicateID != 0 {
		t.Fatalf("expected email apply target to skip duplicate match, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}

func TestNormalizeJobURLForMatchKeepsURLsContainingAtSymbol(t *testing.T) {
	normalized := normalizeJobURLForMatch("https://example.com/jobs/@platform-engineer")
	if normalized != "example.com/jobs/@platform-engineer" {
		t.Fatalf("expected URL with @ in path to normalize normally, got %q", normalized)
	}
}

func TestNormalizeJobURLForMatchStripsTrackingQueryParams(t *testing.T) {
	base := "https://jobs.lever.co/coupa/3cac0beb-8734-489f-b67c-b37a0ae74ca8"
	base1 := "https://jobs.lever.co/coupa/3cac0beb-8734-489f-b67c-b37a0ae74ca8?ref=dailyremote&utm_source=dailyremote.com&utm_campaign=dailyremote.com&utm_medium=dailyremote.com"
	if normalizeJobURLForMatch(base) != normalizeJobURLForMatch(base1) {
		t.Fatalf("expected tracking params to be ignored for job URL match")
	}
}

func TestNormalizeJobURLForMatchKeepsMeaningfulQueryParams(t *testing.T) {
	base := "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193"
	other := "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=90ee0c52-4563-4bbb-a44d-0a6cd6964c61&jobId=588193"
	if normalizeJobURLForMatch(base) == normalizeJobURLForMatch(other) {
		t.Fatalf("expected meaningful query params to be preserved for job URL match")
	}
}

func TestNormalizeJobURLForMatchKeepsMeaningfulQueryParamsWithIgnoredTracking(t *testing.T) {
	base := "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193"
	withTracking := "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?utm_source=dailyremote&ref=newsletter&cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193"
	if normalizeJobURLForMatch(base) != normalizeJobURLForMatch(withTracking) {
		t.Fatalf("expected ignored tracking params to be dropped while preserving meaningful params")
	}
}

func TestExtractDuplicateJobURLSignaturesLeverApplyMatchesBasePosting(t *testing.T) {
	rules := getDuplicateJobURLRuleSet("")
	base := extractDuplicateJobURLSignatures("https://jobs.lever.co/coupa/3cac0beb-8734-489f-b67c-b37a0ae74ca8", rules)
	apply := extractDuplicateJobURLSignatures("https://jobs.lever.co/coupa/3cac0beb-8734-489f-b67c-b37a0ae74ca8/apply?utm_source=dailyremote", rules)
	if len(base) == 0 || len(apply) == 0 {
		t.Fatalf("expected lever signatures to be extracted")
	}
	if base[0].key != apply[0].key {
		t.Fatalf("expected lever apply and base URLs to share duplicate signature, got %q vs %q", base[0].key, apply[0].key)
	}
}

func TestExtractDuplicateJobURLSignaturesADPUsesCIDAndJobIDPair(t *testing.T) {
	rules := getDuplicateJobURLRuleSet("")
	base := extractDuplicateJobURLSignatures("https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193", rules)
	withTracking := extractDuplicateJobURLSignatures("https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?utm_source=dailyremote&ref=newsletter&cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193", rules)
	otherJob := extractDuplicateJobURLSignatures("https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=999999", rules)
	if len(base) == 0 || len(withTracking) == 0 || len(otherJob) == 0 {
		t.Fatalf("expected adp signatures to be extracted")
	}
	if base[0].key != withTracking[0].key {
		t.Fatalf("expected ADP signature to ignore tracking params")
	}
	if base[0].key == otherJob[0].key {
		t.Fatalf("expected ADP signature to change when jobId changes")
	}
}

func TestExtractDuplicateJobURLSignaturesTaleoUsesJobQueryParam(t *testing.T) {
	rules := getDuplicateJobURLRuleSet("")
	direct := extractDuplicateJobURLSignatures("https://example.taleo.net/careersection/jobdetail.ftl?job=12345", rules)
	withTracking := extractDuplicateJobURLSignatures("https://example.taleo.net/careersection/jobdetail.ftl?utm_source=dailyremote&job=12345", rules)
	other := extractDuplicateJobURLSignatures("https://example.taleo.net/careersection/jobdetail.ftl?job=67890", rules)
	if len(direct) == 0 || len(withTracking) == 0 || len(other) == 0 {
		t.Fatalf("expected taleo signatures to be extracted")
	}
	if direct[0].key != withTracking[0].key {
		t.Fatalf("expected taleo signature to be driven by job query param")
	}
	if direct[0].key == other[0].key {
		t.Fatalf("expected taleo signature to change when job query changes")
	}
}

func TestExtractDuplicateJobURLSignaturesBreezyUsesPostingToken(t *testing.T) {
	rules := getDuplicateJobURLRuleSet("")
	base := extractDuplicateJobURLSignatures("https://company.breezy.hr/p/4f3e1c2d9ab8-senior-backend-engineer", rules)
	withExtraPath := extractDuplicateJobURLSignatures("https://company.breezy.hr/p/4f3e1c2d9ab8-senior-backend-engineer/apply?utm_source=dailyremote", rules)
	other := extractDuplicateJobURLSignatures("https://company.breezy.hr/p/9a8b7c6d5e4f-platform-engineer", rules)
	if len(base) == 0 || len(withExtraPath) == 0 || len(other) == 0 {
		t.Fatalf("expected breezy signatures to be extracted")
	}
	if base[0].key != withExtraPath[0].key {
		t.Fatalf("expected breezy signature to ignore extra path/query noise")
	}
	if base[0].key == other[0].key {
		t.Fatalf("expected breezy signature to change when posting token changes")
	}
}

func TestExtractDuplicateJobURLSignaturesAppleMatchesDetailsAndApply(t *testing.T) {
	rules := getDuplicateJobURLRuleSet("")
	details := extractDuplicateJobURLSignatures("https://jobs.apple.com/en-us/details/200612345/software-engineer", rules)
	apply := extractDuplicateJobURLSignatures("https://jobs.apple.com/en-us/app/software-engineer/apply/200612345", rules)
	otherLocale := extractDuplicateJobURLSignatures("https://jobs.apple.com/fr-fr/details/200612345/software-engineer", rules)
	if len(details) == 0 || len(apply) == 0 || len(otherLocale) == 0 {
		t.Fatalf("expected apple signatures to be extracted")
	}
	if details[0].key != apply[0].key {
		t.Fatalf("expected apple details and apply URLs to share signature")
	}
	if details[0].key == otherLocale[0].key {
		t.Fatalf("expected apple signature to include locale")
	}
}

func TestExtractDuplicateJobURLSignaturesOracleCloudUsesHostAndPostingID(t *testing.T) {
	rules := getDuplicateJobURLRuleSet("")
	preview := extractDuplicateJobURLSignatures("https://acme.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions/preview/300000123456789", rules)
	job := extractDuplicateJobURLSignatures("https://acme.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/job/300000123456789", rules)
	otherHost := extractDuplicateJobURLSignatures("https://other.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/job/300000123456789", rules)
	if len(preview) == 0 || len(job) == 0 || len(otherHost) == 0 {
		t.Fatalf("expected oracle cloud signatures to be extracted")
	}
	if preview[0].key != job[0].key {
		t.Fatalf("expected oracle cloud preview and job URLs to share signature")
	}
	if preview[0].key == otherHost[0].key {
		t.Fatalf("expected oracle cloud signature to include host")
	}
}

func TestFindDuplicateCrossSourceParsedJobMatchesMeaningfulQueryParamsIgnoringCompanyID(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_meaningful_query_ignore_company"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'dailyremote', 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193', ?, true, false, true, 0, '{}'),
		        (2, 'builtin', 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?utm_source=builtin&cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193', ?, true, false, false, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_companies (id, external_company_id, name, updated_at)
		 VALUES (101, 'company-101', 'Company 101', ?),
		        (202, 'company-202', 'Company 202', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, company_id, url, role_title, updated_at)
		 VALUES (1, 101, 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193', 'Backend Engineer', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	payload := map[string]any{
		"url": "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?utm_source=builtin&cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193",
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "builtin", payload, int64(202))
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate || duplicateID <= 0 {
		t.Fatalf("expected duplicate by base URL and meaningful query params, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}

func TestFindDuplicateCrossSourceParsedJobMatchesGreenhouseBoardPathAndGHJID(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_greenhouse_board_path_and_gh_jid"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'dailyremote', 'https://companycam.com/job?gh_jid=7692268003&ref=dailyremote&utm_source=dailyremote.com&utm_campaign=dailyremote.com&utm_medium=dailyremote.com', ?, true, false, true, 0, '{}'),
		        (2, 'builtin', 'https://boards.greenhouse.io/companycam/jobs/7692268003', ?, true, false, false, 0, '{}')`,
		now,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, url, role_title, updated_at)
		 VALUES (1, 'https://companycam.com/job?gh_jid=7692268003&ref=dailyremote&utm_source=dailyremote.com&utm_campaign=dailyremote.com&utm_medium=dailyremote.com', 'Software Engineer', ?)`,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	payload := map[string]any{
		"url": "https://boards.greenhouse.io/companycam/jobs/7692268003",
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "builtin", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate || duplicateID <= 0 {
		t.Fatalf("expected duplicate by greenhouse board path and gh_jid signature, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}

func TestFindDuplicateCrossSourceParsedJobMatchesGreenhouseGHJIDAcrossDomains(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_by_greenhouse_gh_jid"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'dailyremote', 'https://company.example.com/careers/open-role?gh_jid=4676811005&utm_source=dailyremote', ?, true, false, true, 0, '{}'),
		        (2, 'builtin', 'https://jobs.example.net/apply/software-engineer?gh_jid=4676811005', ?, true, false, false, 0, '{}')`,
		now,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, url, role_title, updated_at)
		 VALUES (1, 'https://company.example.com/careers/open-role?gh_jid=4676811005&utm_source=dailyremote', 'Backend Engineer', ?)`,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	payload := map[string]any{
		"url": "https://jobs.example.net/apply/software-engineer?gh_jid=4676811005",
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "builtin", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate || duplicateID <= 0 {
		t.Fatalf("expected duplicate by greenhouse gh_jid signature, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}

func TestFindDuplicateCrossSourceParsedJobDoesNotMatchDifferentMeaningfulQueryParams(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_meaningful_query_mismatch"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'dailyremote', 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193', ?, true, false, true, 0, '{}'),
		        (2, 'builtin', 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=90ee0c52-4563-4bbb-a44d-0a6cd6964c61&jobId=588193', ?, true, false, false, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, url, role_title, updated_at)
		 VALUES (1, 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=a158e395-baec-4e08-8f75-7d0702e34f73&jobId=588193', 'Backend Engineer', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	payload := map[string]any{
		"url": "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=90ee0c52-4563-4bbb-a44d-0a6cd6964c61&jobId=588193",
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "builtin", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if isDuplicate || duplicateID != 0 {
		t.Fatalf("expected different meaningful query params to avoid duplicate match, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}

func TestProcessPendingRemoterocketshipDuplicateReplacementKeepsCreatedAtSource(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_remoterocketship_duplicate_keep_created_at_source"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	originalCreatedAtSource := time.Date(2026, 2, 17, 0, 0, 0, 0, time.UTC)
	newPayloadCreatedAt := time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC)

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'builtin', 'https://builtin.example/jobs/abc', ?, true, false, true, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, url, role_title, categorized_job_title, categorized_job_function, created_at_source, updated_at)
		 VALUES (1, 1, 'https://remote.example/jobs/abc', 'Backend Engineer', 'Backend Engineer', 'Engineering', ?, ?)`,
		originalCreatedAtSource.Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(map[string]any{
		"id":                     "remote-abc",
		"url":                    "https://remote.example/jobs/abc",
		"roleTitle":              "Backend Engineer",
		"categorizedJobTitle":    "Backend Engineer",
		"categorizedJobFunction": "Engineering",
		"jobDescriptionSummary":  "Updated from remoterocketship",
		"created_at":             newPayloadCreatedAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (2, 'remoterocketship', 'https://remoterocketship.example/jobs/abc', ?, true, false, false, 0, ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
		string(payload),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"remoterocketship": {}}
	if _, err := svc.ProcessPending(context.Background(), 10); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected single parsed row after duplicate replacement, got %d", count)
	}

	var rawUSJobID int64
	var createdAtSource sql.NullTime
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT raw_us_job_id, created_at_source FROM parsed_jobs WHERE id = 1`,
	).Scan(&rawUSJobID, &createdAtSource); err != nil {
		t.Fatal(err)
	}
	if rawUSJobID != 2 {
		t.Fatalf("expected parsed row to be rebound to remoterocketship raw job, got raw_us_job_id=%d", rawUSJobID)
	}
	if !createdAtSource.Valid {
		t.Fatal("expected created_at_source to remain set")
	}
	if !createdAtSource.Time.UTC().Equal(originalCreatedAtSource) {
		t.Fatalf("expected created_at_source=%s, got %s", originalCreatedAtSource.Format(time.RFC3339Nano), createdAtSource.Time.UTC().Format(time.RFC3339Nano))
	}

	var legacyIsParsed, legacyIsSkippable bool
	var legacyRawJSON sql.NullString
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT is_parsed, is_skippable, raw_json FROM raw_us_jobs WHERE id = 1`,
	).Scan(&legacyIsParsed, &legacyIsSkippable, &legacyRawJSON); err != nil {
		t.Fatal(err)
	}
	if !legacyIsParsed || !legacyIsSkippable {
		t.Fatalf("expected legacy raw row marked parsed+skippable after replacement, got is_parsed=%t is_skippable=%t", legacyIsParsed, legacyIsSkippable)
	}
	if legacyRawJSON.Valid && strings.TrimSpace(legacyRawJSON.String) != "" {
		t.Fatalf("expected legacy raw row raw_json cleared after replacement, got %q", legacyRawJSON.String)
	}
}

func TestProcessPendingReturnsZeroWhenNoEnabledSources(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_no_enabled_sources"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	payload, err := json.Marshal(map[string]any{
		"url":       "https://example.com/jobs/no-source",
		"roleTitle": "Backend Engineer",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'builtin', 'https://example.com/jobs/no-source', ?, true, false, false, 0, ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
		string(payload),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("expected processed=0 when no enabled sources, got %d", processed)
	}

	var isParsed bool
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_parsed FROM raw_us_jobs WHERE id = 1`).Scan(&isParsed); err != nil {
		t.Fatal(err)
	}
	if isParsed {
		t.Fatal("expected raw row to remain unparsed")
	}
}

func TestProcessPendingCrossSourceDuplicateMergesEmptyFieldsAndSkipsRawRow(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_duplicate_merge"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'remoterocketship', 'https://example.com/jobs/a', ?, true, false, true, 0, '{}'),
		        (2, 'builtin', 'https://example.com/jobs/b', ?, true, false, false, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (
		   id, raw_us_job_id, url, role_title, role_description, required_languages, location_us_states, tech_stack, salary_currency_code, updated_at
		 )
		 VALUES (1, 1, 'https://example.com/job/shared', 'Backend Engineer', '', '[]'::json, '[]'::jsonb, '["Go"]'::jsonb, 'USD', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(map[string]any{
		"url":                    "https://www.example.com/job/shared/",
		"roleTitle":              "Backend Engineer",
		"roleDescription":        "Filled from duplicate",
		"requiredLanguages":      []string{"English"},
		"locationUSStates":       []string{"NY"},
		"locationCountries":      []string{"United States"},
		"techStack":              []string{"Python"},
		"categorizedJobTitle":    "Backend Engineer",
		"categorizedJobFunction": "Engineering",
		"salaryRange": map[string]any{
			"currencyCode": "EUR",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `UPDATE raw_us_jobs SET raw_json = ? WHERE id = 2`, string(payload))
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1 for duplicate skip path, got %d", processed)
	}

	var isParsed, isSkippable bool
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_parsed, is_skippable FROM raw_us_jobs WHERE id = 2`).Scan(&isParsed, &isSkippable); err != nil {
		t.Fatal(err)
	}
	if !isParsed || !isSkippable {
		t.Fatalf("expected raw duplicate row marked parsed+skippable, got is_parsed=%t is_skippable=%t", isParsed, isSkippable)
	}

	var (
		roleDescription    sql.NullString
		requiredLanguages  sql.NullString
		locationUSStates   sql.NullString
		locationCountries  sql.NullString
		techStack          sql.NullString
		salaryCurrencyCode sql.NullString
	)
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT role_description, required_languages::text, location_us_states::text, location_countries::text, tech_stack::text, salary_currency_code
		   FROM parsed_jobs WHERE id = 1`,
	).Scan(&roleDescription, &requiredLanguages, &locationUSStates, &locationCountries, &techStack, &salaryCurrencyCode); err != nil {
		t.Fatal(err)
	}
	if roleDescription.String != "Filled from duplicate" {
		t.Fatalf("expected role_description to be filled, got %q", roleDescription.String)
	}
	if requiredLanguages.String != "[\"English\"]" {
		t.Fatalf("expected required_languages to be filled, got %q", requiredLanguages.String)
	}
	if locationUSStates.String != "[\"NY\"]" {
		t.Fatalf("expected location_us_states to be filled, got %q", locationUSStates.String)
	}
	if locationCountries.String != "[\"United States\"]" {
		t.Fatalf("expected location_countries to be filled, got %q", locationCountries.String)
	}
	if techStack.String != "[\"Go\"]" {
		t.Fatalf("expected tech_stack to remain original, got %q", techStack.String)
	}
	if salaryCurrencyCode.String != "USD" {
		t.Fatalf("expected salary_currency_code to remain original, got %q", salaryCurrencyCode.String)
	}

	var parsedCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs`).Scan(&parsedCount); err != nil {
		t.Fatal(err)
	}
	if parsedCount != 1 {
		t.Fatalf("expected no new parsed rows for duplicate skip, got count=%d", parsedCount)
	}
}

func TestProcessPendingSameSourceExternalJobIDDuplicateSkipsNewParsedRow(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_same_source_external_job_id_duplicate"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'remoterocketship', 'https://remote.example/jobs/first', ?, true, false, true, 0, '{}'),
		        (2, 'remoterocketship', 'https://remote.example/jobs/second', ?, true, false, false, 0, '{}')`,
		now,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_companies (id, external_company_id, name, updated_at)
		 VALUES (77, 'company-77', 'Acme', ?)`,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, company_id, external_job_id, url, role_title, role_description, updated_at)
		 VALUES (1, 1, 77, 'rr-123', 'https://remote.example/jobs/shared', 'Backend Engineer', '', ?)`,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(map[string]any{
		"id":              "rr-123",
		"url":             "https://remote.example/jobs/shared",
		"roleTitle":       "Backend Engineer",
		"roleDescription": "Filled from same source duplicate",
		"company": map[string]any{
			"id":   "company-77",
			"name": "Acme",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `UPDATE raw_us_jobs SET raw_json = ? WHERE id = 2`, string(payload))
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"remoterocketship": {}}
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1 for same-source duplicate skip path, got %d", processed)
	}

	var isParsed, isSkippable bool
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_parsed, is_skippable FROM raw_us_jobs WHERE id = 2`).Scan(&isParsed, &isSkippable); err != nil {
		t.Fatal(err)
	}
	if !isParsed || !isSkippable {
		t.Fatalf("expected same-source duplicate raw row marked parsed+skippable, got is_parsed=%t is_skippable=%t", isParsed, isSkippable)
	}

	var roleDescription sql.NullString
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT role_description FROM parsed_jobs WHERE id = 1`).Scan(&roleDescription); err != nil {
		t.Fatal(err)
	}
	if roleDescription.String != "Filled from same source duplicate" {
		t.Fatalf("expected role_description to be merged from same-source duplicate, got %q", roleDescription.String)
	}

	var parsedCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs`).Scan(&parsedCount); err != nil {
		t.Fatal(err)
	}
	if parsedCount != 1 {
		t.Fatalf("expected no new parsed rows for same-source external_job_id duplicate, got count=%d", parsedCount)
	}
}

func TestProcessPendingSameSourceURLDuplicateSkipsNewParsedRow(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_same_source_url_duplicate"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'builtin', 'https://example.com/jobs/original', ?, true, false, true, 0, '{}'),
		        (2, 'builtin', 'https://example.com/jobs/new', ?, true, false, false, 0, '{}')`,
		now,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (id, raw_us_job_id, url, role_title, role_description, updated_at)
		 VALUES (1, 1, 'https://boards.example.com/jobs/shared?utm_source=alpha', 'Backend Engineer', '', ?)`,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(map[string]any{
		"url":             "https://boards.example.com/jobs/shared?utm_source=beta",
		"roleTitle":       "Backend Engineer",
		"roleDescription": "Filled from same source url duplicate",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `UPDATE raw_us_jobs SET raw_json = ? WHERE id = 2`, string(payload))
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1 for same-source url duplicate skip path, got %d", processed)
	}

	var isParsed, isSkippable bool
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT is_parsed, is_skippable FROM raw_us_jobs WHERE id = 2`).Scan(&isParsed, &isSkippable); err != nil {
		t.Fatal(err)
	}
	if !isParsed || !isSkippable {
		t.Fatalf("expected same-source url duplicate raw row marked parsed+skippable, got is_parsed=%t is_skippable=%t", isParsed, isSkippable)
	}

	var roleDescription sql.NullString
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT role_description FROM parsed_jobs WHERE id = 1`).Scan(&roleDescription); err != nil {
		t.Fatal(err)
	}
	if roleDescription.String != "Filled from same source url duplicate" {
		t.Fatalf("expected role_description to be merged from same-source url duplicate, got %q", roleDescription.String)
	}

	var parsedCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM parsed_jobs`).Scan(&parsedCount); err != nil {
		t.Fatal(err)
	}
	if parsedCount != 1 {
		t.Fatalf("expected no new parsed rows for same-source url duplicate, got count=%d", parsedCount)
	}
}

func TestProcessPendingUpsertMergesExistingParsedRowColumns(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "test_parsed_upsert_overwrite_existing"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (1, 'builtin', 'https://example.com/jobs/overwrite', ?, true, false, false, 0, '{}')`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (
		    raw_us_job_id, role_title, role_description, slug, salary_currency_code, role_title_brazil, location_us_states, tech_stack, updated_at
		 )
		 VALUES (1, 'Old Title', 'Old Description', 'old-slug', 'EUR', 'Titulo Antigo', '["CA"]', '["Ruby"]', ?)`,
		time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(map[string]any{
		"id":                     "ext-1",
		"url":                    "https://example.com/jobs/overwrite",
		"roleTitle":              "New Title",
		"roleDescription":        "New Description",
		"roleTitleBrazil":        "Novo Titulo",
		"slug":                   "new-slug",
		"categorizedJobTitle":    "Backend Engineer",
		"categorizedJobFunction": "Engineering",
		"locationUSStates":       []string{"NY"},
		"techStack":              []string{"Go", "Postgres"},
		"salaryRange": map[string]any{
			"currencyCode": "USD",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `UPDATE raw_us_jobs SET raw_json = ? WHERE id = 1`, string(payload))
	if err != nil {
		t.Fatal(err)
	}

	svc := New(Config{}, db)
	svc.EnabledSources = map[string]struct{}{"builtin": {}}
	processed, err := svc.ProcessPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("expected processed=1, got %d", processed)
	}

	var (
		roleTitle          sql.NullString
		roleDescription    sql.NullString
		slug               sql.NullString
		salaryCurrencyCode sql.NullString
		roleTitleBrazil    sql.NullString
		locationUSStates   sql.NullString
		techStack          sql.NullString
	)
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT role_title, role_description, slug, salary_currency_code, role_title_brazil, location_us_states::text, tech_stack::text
		   FROM parsed_jobs WHERE raw_us_job_id = 1`,
	).Scan(&roleTitle, &roleDescription, &slug, &salaryCurrencyCode, &roleTitleBrazil, &locationUSStates, &techStack); err != nil {
		t.Fatal(err)
	}
	if roleTitle.String != "Old Title" {
		t.Fatalf("expected role_title to be preserved, got %q", roleTitle.String)
	}
	if roleDescription.String != "Old Description" {
		t.Fatalf("expected role_description to be preserved, got %q", roleDescription.String)
	}
	if slug.String != "old-slug" {
		t.Fatalf("expected slug to be preserved, got %q", slug.String)
	}
	if salaryCurrencyCode.String != "EUR" {
		t.Fatalf("expected salary_currency_code to be preserved, got %q", salaryCurrencyCode.String)
	}
	if roleTitleBrazil.String != "Titulo Antigo" {
		t.Fatalf("expected role_title_brazil to be preserved, got %q", roleTitleBrazil.String)
	}
	if locationUSStates.String != "[\"CA\"]" {
		t.Fatalf("expected location_us_states to be preserved, got %q", locationUSStates.String)
	}
	var techValues []string
	if err := json.Unmarshal([]byte(techStack.String), &techValues); err != nil {
		t.Fatalf("failed to parse tech_stack json: %v", err)
	}
	if len(techValues) != 1 || techValues[0] != "Ruby" {
		t.Fatalf("expected existing tech_stack to be preserved, got %#v", techValues)
	}
}


