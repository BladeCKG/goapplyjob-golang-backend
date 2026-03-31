package parsed

import (
	"context"
	"encoding/json"
	"fmt"
	"goapplyjob-golang-backend/internal/database"
	"testing"
	"time"
)

func insertSimilarCategoryCandidate(
	t *testing.T,
	db *database.DB,
	id int64,
	roleTitle string,
	categoryTitle string,
	categoryFunction string,
	techStack []string,
	updatedAt time.Time,
) {
	t.Helper()
	_, err := db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO raw_us_jobs (id, source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
		 VALUES (?, 'remoterocketship', ?, ?, true, false, true, 0, '{}')`,
		id,
		fmt.Sprintf("https://remote.example/jobs/%d", id),
		updatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
	var techStackJSON any
	if len(techStack) > 0 {
		body, marshalErr := json.Marshal(techStack)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		techStackJSON = string(body)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, role_title, categorized_job_title, categorized_job_function, tech_stack, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		id,
		roleTitle,
		categoryTitle,
		categoryFunction,
		techStackJSON,
		updatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFindSimilarRemoteCategoriesNoSignalReturnsEmpty(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_no_signal"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "" || function != "" {
		t.Fatalf("expected empty inference, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesPrefersOrderedCategoryTokens(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_ordered"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Manager Product", "Product Manager", "Product", nil, now)
	insertSimilarCategoryCandidate(t, db, 2, "Manager", "Manager", "Operations", nil, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Senior Product Manager", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Product Manager" || function != "Product" {
		t.Fatalf("expected Product Manager/Product, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesAcceptsOutOfOrderTokens(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_out_of_order"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Product Manager", "Product Manager", "Product", nil, now)

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Manager Product", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Product Manager" || function != "Product" {
		t.Fatalf("expected Product Manager/Product, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesIgnoresSeniorityAndEmploymentNoise(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_noise_tokens"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Backend Engineer", "Backend Engineer", "Engineering", nil, now)

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Senior II Full Time Backend Engineer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Backend Engineer" || function != "Engineering" {
		t.Fatalf("expected Backend Engineer/Engineering, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesScansBeyondFirstThousandCandidates(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_scan_window"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	for i := 0; i < 1000; i++ {
		id := int64(i + 1)
		insertSimilarCategoryCandidate(t, db, id, fmt.Sprintf("Engineer %d", i), "Engineer", "Engineering", nil, now.Add(-time.Duration(i)*time.Second))
	}
	insertSimilarCategoryCandidate(t, db, 2001, "Product Implementation Engineer", "Implementation Specialist", "Engineering", nil, now.Add(-time.Hour))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Senior Product Implementation Engineer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Implementation Specialist" || function != "Engineering" {
		t.Fatalf("expected scan to find Implementation Specialist/Engineering, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesPrefersSpecificAccountManagerOverGenericManager(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_specific_manager"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Manager", "Manager", "Operations", nil, now)
	insertSimilarCategoryCandidate(t, db, 2, "Account Manager", "Account Manager", "Sales", nil, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Senior Account Manager", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Account Manager" || function != "Sales" {
		t.Fatalf("expected Account Manager/Sales, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesUsesTechStackToBreakTie(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_tech_tiebreak"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Platform Engineer", "Software Engineer", "Engineering", []string{"Ruby", "PostgreSQL"}, now)
	insertSimilarCategoryCandidate(t, db, 2, "Platform Engineer", "Account Executive", "Sales", []string{"Java"}, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Platform Engineer", []string{"Ruby"})
	if err != nil {
		t.Fatal(err)
	}
	if title != "Software Engineer" || function != "Engineering" {
		t.Fatalf("expected Software Engineer/Engineering, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesFallsBackWhenTechStackFilteredSetEmpty(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_tech_fallback"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Backend Engineer", "Backend Engineer", "Engineering", []string{"Python"}, now)

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Backend Engineer", []string{"Rust"})
	if err != nil {
		t.Fatal(err)
	}
	if title != "Backend Engineer" || function != "Engineering" {
		t.Fatalf("expected fallback to unfiltered set, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesAvoidsGenericEngineerWhenSpecificExists(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_generic_vs_specific"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Engineer", "Engineer", "Engineering", nil, now)
	insertSimilarCategoryCandidate(t, db, 2, "Software Engineer", "Software Engineer", "Engineering", nil, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Senior Software Engineer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Software Engineer" || function != "Engineering" {
		t.Fatalf("expected Software Engineer/Engineering, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesUsesFunctionOverlapAsTieBreaker(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_function_overlap"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Account Manager", "Account Manager", "Operations", nil, now)
	insertSimilarCategoryCandidate(t, db, 2, "Account Manager", "Account Manager", "Sales", nil, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Account Manager Sales", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Account Manager" || function != "Sales" {
		t.Fatalf("expected Account Manager/Sales, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesPrefersImplementationEngineerOverEngineer(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_impl_over_generic"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Engineer", "Engineer", "Engineering", nil, now)
	insertSimilarCategoryCandidate(t, db, 2, "Consultant", "Implementation Specialist", "Engineering", nil, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Product Implementation Engineer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Implementation Specialist" || function != "Engineering" {
		t.Fatalf("expected Implementation Specialist/Engineering, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesExactTitlePathUsesSkillOverlapTieBreaker(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_exact_title_skill_tie"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Platform Engineer", "Software Engineer", "Engineering", []string{"Ruby", "PostgreSQL"}, now)
	insertSimilarCategoryCandidate(t, db, 2, "Platform Engineer", "Account Executive", "Sales", []string{"Java"}, now.Add(-time.Minute))
	insertSimilarCategoryCandidate(t, db, 3, "Platform Engineer", "Backend Engineer", "Engineering", []string{"Ruby"}, now.Add(-2*time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Platform Engineer", []string{"Ruby"})
	if err != nil {
		t.Fatal(err)
	}
	if title != "Software Engineer" || function != "Engineering" {
		t.Fatalf("expected exact-title best skill overlap winner, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesExactTitleSkipsGenericOneWordWhenSourceHasSpecificTokens(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_exact_title_skip_generic"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Platform Engineer", "Engineer", "Engineering", []string{"Ruby"}, now)
	insertSimilarCategoryCandidate(t, db, 2, "Platform Engineer", "Software Engineer", "Engineering", []string{"Ruby"}, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Senior Platform Engineer", []string{"Ruby"})
	if err != nil {
		t.Fatal(err)
	}
	if title != "Software Engineer" || function != "Engineering" {
		t.Fatalf("expected generic one-word category to be skipped, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesDoesNotScanBeyondMaxRows(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_scan_cap"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	for i := 0; i < 2000; i++ {
		id := int64(i + 1)
		insertSimilarCategoryCandidate(t, db, id, fmt.Sprintf("Generic Engineer %d", i), "Engineer", "Engineering", nil, now.Add(-time.Duration(i)*time.Second))
	}
	// Strong candidate intentionally placed beyond max scan limit.
	// Role title intentionally does not exactly match input to avoid exact-title fast path.
	insertSimilarCategoryCandidate(t, db, 3001, "Consultant", "Implementation Specialist", "Engineering", nil, now.Add(-3000*time.Second))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Product Implementation Engineer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title == "Implementation Engineer" && function == "Engineering" {
		t.Fatal("expected candidate beyond max scan rows to be ignored")
	}
}

func TestFindSimilarRemoteCategoriesConfidenceGateRejectsWeakOverlap(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_conf_reject"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Support Analyst", "Customer Support", "Operations", nil, now)
	insertSimilarCategoryCandidate(t, db, 2, "Finance Manager", "Manager", "Operations", nil, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Quantum Compiler Researcher", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Any" || function != "Any" {
		t.Fatalf("expected weak-overlap candidate to be rejected, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesDirectMatchWinsOverHigherRecency(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_direct_match_precedence"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	// More recent but weaker.
	insertSimilarCategoryCandidate(t, db, 1, "Manager", "Manager", "Operations", nil, now)
	// Slightly older but direct subset title+function match.
	insertSimilarCategoryCandidate(t, db, 2, "Something Else", "Product Manager", "Product", nil, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Senior Product Manager", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Product Manager" || function != "Product" {
		t.Fatalf("expected direct-match subset to win, got %q / %q", title, function)
	}
}

func TestFindSimilarRemoteCategoriesConfidenceGateAcceptsWithSpecificSignal(t *testing.T) {
	db, err := database.Open(testDatabaseURL(t, "simcat_conf_accept_specific"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	insertSimilarCategoryCandidate(t, db, 1, "Implementation Engineer", "Implementation Specialist", "Engineering", nil, now)
	insertSimilarCategoryCandidate(t, db, 2, "Engineer", "Engineer", "Engineering", nil, now.Add(-time.Minute))

	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), "Implementation Engineer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if title != "Implementation Specialist" || function != "Engineering" {
		t.Fatalf("expected specific-signal candidate to pass confidence gate, got %q / %q", title, function)
	}
}
