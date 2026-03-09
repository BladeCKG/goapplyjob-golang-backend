package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
)

func TestSourceOlderThanPostDateReturnsTrue(t *testing.T) {
	sourceCreatedAt := time.Date(2026, 2, 12, 9, 0, 0, 0, time.UTC)
	postDate := time.Date(2026, 2, 12, 10, 0, 0, 0, time.UTC)
	if !isSourceOlderThanPostDate(&sourceCreatedAt, &postDate) {
		t.Fatal("expected source to be older than post date")
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

	svc := New(db)
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
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (2, 'builtin', 'https://builtin.com/job/acme/200', ?, true, false, false, 0, ?)`, time.Date(2026, 2, 17, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), string(payload))
	if err != nil {
		t.Fatal(err)
	}

	svc := New(db)
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
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, updated_at) VALUES (1, 'Engineer', 'Engineer', 'Engineering', ?), (2, 'Consultant', 'Implementation Engineer', 'Engineering', ?)`, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(db)
	title, function, err := svc.findSimilarRemoteCategories(context.Background(), "Product Implementation Engineer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Implementation Engineer" || function != "Engineering" {
		t.Fatalf("expected specific category match, got %q / %q", title, function)
	}
}

func TestNormalizeRoleTitleForExactMatchHandlesCommonAbbreviations(t *testing.T) {
	got := normalizeRoleTitleForExactMatch("Senior SWE, Dev Ops")
	if got != "software engineer devops" {
		t.Fatalf("unexpected normalized title %q", got)
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
	svc := New(db)
	title, function, err := svc.findSimilarRemoteCategories(context.Background(), "Senior SWE Dev Ops", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "DevOps Engineer" || function != "Engineering" {
		t.Fatalf("expected exact normalized match, got %q / %q", title, function)
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
	svc := New(db)
	title, function, err := svc.findSimilarRemoteCategories(context.Background(), "Platform Engineer", []string{"Ruby"})
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
	svc := New(db)
	title, function, err := svc.findSimilarRemoteCategories(context.Background(), "Backend Engineer", []string{"Rust"})
	if err != nil {
		t.Fatal(err)
	}
	if title != "Backend Engineer" || function != "Engineering" {
		t.Fatalf("expected fallback category match, got %q / %q", title, function)
	}
}

func TestNormalizeEmploymentTypeValueCollapsesToFullTime(t *testing.T) {
	if got := normalizeEmploymentTypeValue("Full Time"); got != "full-time" {
		t.Fatalf("expected full-time, got %#v", got)
	}
	if got := normalizeEmploymentTypeValue("contractor"); got != "contract" {
		t.Fatalf("expected contract, got %#v", got)
	}
	if got := normalizeEmploymentTypeValue("intern"); got != "internship" {
		t.Fatalf("expected internship, got %#v", got)
	}
	if got := normalizeEmploymentTypeValue("temp"); got != "temporary" {
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
		"rr_company_1",
		"Old Name",
		"https://old.example",
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(db)
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
	companyID, err := svc.upsertCompanyFromPayload(context.Background(), payload, plugin, true)
	if err != nil {
		t.Fatal(err)
	}
	if companyID == nil {
		t.Fatal("expected company id")
	}

	var name, homePage string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT name, home_page_url FROM parsed_companies WHERE external_company_id = ?`, "rr_company_1").Scan(&name, &homePage); err != nil {
		t.Fatal(err)
	}
	if name != "New Name" || homePage != "https://new.example" {
		t.Fatalf("expected updated company fields, got name=%q home_page_url=%q", name, homePage)
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

	svc := New(db)
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

func TestFindDuplicateCrossSourceParsedJobByExternalID(t *testing.T) {
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
		`INSERT INTO parsed_jobs (raw_us_job_id, external_job_id, role_title, updated_at) VALUES (1, 'ext-123', 'Backend Engineer', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc := New(db)
	payload := map[string]any{
		"id":        "ext-123",
		"roleTitle": "Backend Engineer",
	}
	duplicateID, isDuplicate, err := svc.findDuplicateCrossSourceParsedJob(context.Background(), 2, "dailyremote", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate || duplicateID <= 0 {
		t.Fatalf("expected duplicate by external id, got duplicate=%v id=%d", isDuplicate, duplicateID)
	}
}
