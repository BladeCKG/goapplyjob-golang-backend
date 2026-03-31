package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
	"strings"
	"testing"
	"time"
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

func TestProcessPendingUpsertOverwritesExistingParsedRowColumns(t *testing.T) {
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
	if roleTitle.String != "New Title" {
		t.Fatalf("expected role_title to be overwritten, got %q", roleTitle.String)
	}
	if roleDescription.String != "New Description" {
		t.Fatalf("expected role_description to be overwritten, got %q", roleDescription.String)
	}
	if slug.String != "new-slug" {
		t.Fatalf("expected slug to be overwritten, got %q", slug.String)
	}
	if salaryCurrencyCode.String != "USD" {
		t.Fatalf("expected salary_currency_code to be overwritten, got %q", salaryCurrencyCode.String)
	}
	if roleTitleBrazil.String != "Novo Titulo" {
		t.Fatalf("expected role_title_brazil to be overwritten, got %q", roleTitleBrazil.String)
	}
	if locationUSStates.String != "[\"NY\"]" {
		t.Fatalf("expected location_us_states to be overwritten, got %q", locationUSStates.String)
	}
	var techValues []string
	if err := json.Unmarshal([]byte(techStack.String), &techValues); err != nil {
		t.Fatalf("failed to parse tech_stack json: %v", err)
	}
	if len(techValues) != 2 || techValues[0] != "Go" || techValues[1] != "PostgreSQL" {
		t.Fatalf("expected normalized tech_stack overwrite, got %#v", techValues)
	}
}
