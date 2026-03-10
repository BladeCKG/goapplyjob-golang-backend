package app

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"

	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestJobsPublicAccess(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["is_preview"] != true {
		t.Fatalf("expected preview mode, got %#v", body["is_preview"])
	}
	if body["requires_upgrade"] != false {
		t.Fatalf("expected no upgrade requirement, got %#v", body["requires_upgrade"])
	}
}

func TestAuthAndJobsFlow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 1, "https://example.com/a", "Austin", "Texas", 120, 150, true, time.Date(2026, 2, 11, 0, 0, 0, 0, time.UTC))
	insertJob(t, db, 2, "https://example.com/b", "Seattle", "Washington", 80, 100, false, time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC))

	login := requestLoginCodePayload(t, router, "user@example.com")
	if login.DebugLink == "" {
		t.Fatal("expected debug link")
	}
	code := login.DebugCode
	cookie := verifyLoginCode(t, router, "user@example.com", code)

	meReq := httptest.NewRequest(http.MethodGet, "/auth/me", nil)
	meReq.AddCookie(cookie)
	meRec := httptest.NewRecorder()
	router.ServeHTTP(meRec, meReq)
	assertStatus(t, meRec.Code, http.StatusOK)
	var meBody map[string]any
	decodeBody(t, meRec.Body.Bytes(), &meBody)
	if meBody["email"].(string) != "user@example.com" {
		t.Fatalf("unexpected me payload %#v", meBody)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?sort_criteria=salary&job_title=Engineer&location=Texas&min_salary=100&seniority=senior&page=1&per_page=10", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one job, got %d", len(items))
	}
	if body["company_count"] != float64(0) {
		t.Fatalf("expected company_count=0, got %#v", body["company_count"])
	}
}

func TestMagicLinkAuthFlow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	login := requestLoginCodePayload(t, router, "magic@example.com")
	if login.DebugLink == "" {
		t.Fatal("expected debug link")
	}
	token := magicTokenFromLink(t, login.DebugLink)
	cookie := verifyLoginLink(t, router, token)

	meReq := httptest.NewRequest(http.MethodGet, "/auth/me", nil)
	meReq.AddCookie(cookie)
	meRec := httptest.NewRecorder()
	router.ServeHTTP(meRec, meReq)
	assertStatus(t, meRec.Code, http.StatusOK)

	var meBody map[string]any
	decodeBody(t, meRec.Body.Bytes(), &meBody)
	if meBody["email"] != "magic@example.com" {
		t.Fatalf("unexpected me payload %#v", meBody)
	}
}

func TestJobsPublicPreviewIsLimited(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_preview")
	cfg.AuthDebugReturnCode = true
	cfg.PublicJobsMaxPerPage = 3
	cfg.PublicJobsMaxTotal = 5
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	for idx := 0; idx < 7; idx++ {
		insertJob(t, db, idx+1, "https://example.com/"+strconv.Itoa(idx), "City-"+strconv.Itoa(idx), "State", float64(100+idx), float64(110+idx), idx%2 == 0, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))
	}

	req1 := httptest.NewRequest(http.MethodGet, "/jobs?per_page=100&page=1", nil)
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)
	assertStatus(t, rec1.Code, http.StatusOK)
	var page1 map[string]any
	decodeBody(t, rec1.Body.Bytes(), &page1)
	if int(page1["total"].(float64)) != 7 || int(page1["page"].(float64)) != 1 || int(page1["per_page"].(float64)) != 3 || len(page1["items"].([]any)) != 3 {
		t.Fatalf("unexpected preview page1 %#v", page1)
	}
	if page1["requires_login"] != true || page1["requires_upgrade"] != false {
		t.Fatalf("unexpected preview access flags %#v", page1)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/jobs?per_page=100&page=2", nil)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	assertStatus(t, rec2.Code, http.StatusOK)
	var page2 map[string]any
	decodeBody(t, rec2.Body.Bytes(), &page2)
	if int(page2["page"].(float64)) != 1 || len(page2["items"].([]any)) != 3 {
		t.Fatalf("unexpected preview page2 %#v", page2)
	}
}

func TestJobsSitemapEndpointNotPreviewLimited(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_jobs_sitemap_not_preview_limited")
	cfg.AuthDebugReturnCode = true
	cfg.PublicJobsMaxPerPage = 2
	cfg.PublicJobsMaxTotal = 2
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	for idx := 0; idx < 7; idx++ {
		insertJob(t, db, idx+9000, "https://example.com/sitemap-"+strconv.Itoa(idx), "City", "State", 100, 130, true, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))
	}
	companyID := insertCompany(t, db, "Sitemap Co")
	if _, err := db.SQL.ExecContext(context.Background(), `UPDATE parsed_jobs SET company_id = ? WHERE raw_us_job_id = ?`, companyID, 9006); err != nil {
		t.Fatal(err)
	}

	previewReq := httptest.NewRequest(http.MethodGet, "/jobs?page=1&per_page=100", nil)
	previewRec := httptest.NewRecorder()
	router.ServeHTTP(previewRec, previewReq)
	assertStatus(t, previewRec.Code, http.StatusOK)
	var previewPayload map[string]any
	decodeBody(t, previewRec.Body.Bytes(), &previewPayload)
	if len(previewPayload["items"].([]any)) != 2 || previewPayload["total"].(float64) != 7 {
		t.Fatalf("unexpected preview payload %#v", previewPayload)
	}

	sitemapReq := httptest.NewRequest(http.MethodGet, "/jobs/sitemap?page=1&per_page=3", nil)
	sitemapRec := httptest.NewRecorder()
	router.ServeHTTP(sitemapRec, sitemapReq)
	assertStatus(t, sitemapRec.Code, http.StatusOK)
	var sitemapPayload map[string]any
	decodeBody(t, sitemapRec.Body.Bytes(), &sitemapPayload)
	if sitemapPayload["total"].(float64) != 7 || len(sitemapPayload["items"].([]any)) != 3 {
		t.Fatalf("unexpected sitemap page1 payload %#v", sitemapPayload)
	}
	foundCompanyName := false
	for _, rawItem := range sitemapPayload["items"].([]any) {
		item := rawItem.(map[string]any)
		if item["company_name"] == "Sitemap Co" {
			foundCompanyName = true
			break
		}
	}
	if !foundCompanyName {
		t.Fatalf("expected company_name in sitemap payload %#v", sitemapPayload)
	}

	sitemapReq3 := httptest.NewRequest(http.MethodGet, "/jobs/sitemap?page=3&per_page=3", nil)
	sitemapRec3 := httptest.NewRecorder()
	router.ServeHTTP(sitemapRec3, sitemapReq3)
	assertStatus(t, sitemapRec3.Code, http.StatusOK)
	var sitemapPayload3 map[string]any
	decodeBody(t, sitemapRec3.Body.Bytes(), &sitemapPayload3)
	if len(sitemapPayload3["items"].([]any)) != 1 {
		t.Fatalf("unexpected sitemap page3 payload %#v", sitemapPayload3)
	}
}

func TestJobsRelatedCategoriesAreFunctionBased(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 9101, "Software Engineer", "Engineering", "Software Engineer")
	insertJobWithFunction(t, db, 9102, "Backend Engineer", "Engineering", "Backend Engineer")
	insertJobWithFunction(t, db, 9103, "DevOps Engineer", "Engineering", "DevOps Engineer")
	insertJobWithFunction(t, db, 9104, "Product Manager", "Product", "Product Manager")
	insertJobWithFunction(t, db, 9105, "Platform Engineer", "Engineering", "Platform Engineer")

	req := httptest.NewRequest(http.MethodGet, "/jobs/related-categories?category=Software+Engineer&limit=4", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	categories := map[string]struct{}{}
	for _, item := range items {
		row := item.(map[string]any)
		categories[row["category"].(string)] = struct{}{}
	}
	if items[0].(map[string]any)["category"] != "Software Engineer" {
		t.Fatalf("expected selected category to be first %#v", body)
	}
	if _, ok := categories["Backend Engineer"]; !ok {
		t.Fatalf("expected Backend Engineer in related categories %#v", body)
	}
	if _, ok := categories["DevOps Engineer"]; !ok {
		t.Fatalf("expected DevOps Engineer in related categories %#v", body)
	}
	if _, ok := categories["Product Manager"]; ok {
		t.Fatalf("did not expect Product Manager in related categories %#v", body)
	}
}

func TestJobsTopCategoriesRespectsLocationAndWindow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	now := time.Now().UTC()
	insertJobWithCreatedAt(t, db, 9201, "Software Engineer", "United States", now.Add(-2*time.Hour))
	insertJobWithCreatedAt(t, db, 9202, "Software Engineer", "United States", now.Add(-3*time.Hour))
	insertJobWithCreatedAt(t, db, 9203, "Data Engineer", "Canada", now.Add(-2*time.Hour))
	insertJobWithCreatedAt(t, db, 9204, "Legacy Role", "United States", now.Add(-40*24*time.Hour))

	req := httptest.NewRequest(http.MethodGet, "/jobs/top-categories?location=United+States&days=30&limit=5", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) == 0 {
		t.Fatalf("expected top categories payload %#v", body)
	}
	first := items[0].(map[string]any)
	if first["category"].(string) != "Software Engineer" {
		t.Fatalf("unexpected top category payload %#v", body)
	}
}

func TestJobsDefaultSortUsesCreatedAtSource(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (3001, 'https://example.com/old-updated', ?, true, false, true, 0, '{}')`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (3002, 'https://example.com/new-updated', ?, true, false, true, 0, '{}')`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, created_at_source, updated_at, url) VALUES (3001, 'Older Updated', ?, ?, 'https://jobs.example.com/old-updated')`, time.Date(2026, 2, 12, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), time.Date(2026, 2, 10, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, created_at_source, updated_at, url) VALUES (3002, 'Newer Updated', ?, ?, 'https://jobs.example.com/new-updated')`, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), time.Date(2026, 2, 11, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if items[0].(map[string]any)["categorized_job_title"].(string) != "Older Updated" || items[1].(map[string]any)["categorized_job_title"].(string) != "Newer Updated" {
		t.Fatalf("unexpected order %#v", items)
	}
}
func TestJobsFilterOptionsAnnualized(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()
	insertJobWithSalaryType(t, db, 1, "Hourly Role", 40, "hourly")
	insertJobWithSalaryType(t, db, 4, "Hourly Slash Role", 40, "$40/hr")
	insertJobWithSalaryType(t, db, 2, "Monthly Role", 6000, "monthly")
	insertJobWithSalaryType(t, db, 3, "Yearly Role", 70000, "yearly")
	for idx := 0; idx < 12; idx++ {
		insertJobWithSalaryType(t, db, 100+idx, "Dense Role", float64(80000+(idx*1000)), "yearly")
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs/filter-options", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	minSalaryOptions := body["min_salary_options"].([]any)
	if len(minSalaryOptions) != 28 || int(minSalaryOptions[0].(float64)) != 30000 || int(minSalaryOptions[len(minSalaryOptions)-1].(float64)) != 300000 {
		t.Fatalf("unexpected min salary options %#v", body)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/jobs?sort_criteria=salary", nil)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	assertStatus(t, rec2.Code, http.StatusOK)
	var body2 map[string]any
	decodeBody(t, rec2.Body.Bytes(), &body2)
	items := body2["items"].([]any)
	if len(items) == 0 {
		t.Fatalf("expected salary-sorted jobs")
	}

	code := requestLoginCode(t, router, "salary-check@example.com")
	cookie := verifyLoginCode(t, router, "salary-check@example.com", code)
	req3 := httptest.NewRequest(http.MethodGet, "/jobs?min_salary=80000&per_page=50", nil)
	req3.AddCookie(cookie)
	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, req3)
	assertStatus(t, rec3.Code, http.StatusOK)
	var body3 map[string]any
	decodeBody(t, rec3.Body.Bytes(), &body3)
	if len(body3["items"].([]any)) == 0 {
		t.Fatalf("expected min salary filter to return jobs, got %#v", body3)
	}
}

func TestJobsMinSalaryAnnualizesUSDWithSalaryType(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithSalaryType(t, db, 1901, "Monthly Mismatch Case", 15000, "monthly")

	code := requestLoginCode(t, router, "min-salary-check@example.com")
	cookie := verifyLoginCode(t, router, "min-salary-check@example.com", code)

	req := httptest.NewRequest(http.MethodGet, "/jobs?min_salary=170000&per_page=50", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if int(body["total"].(float64)) != 1 {
		t.Fatalf("expected annualized usd min-salary match %#v", body)
	}
	if body["items"].([]any)[0].(map[string]any)["categorized_job_title"] != "Monthly Mismatch Case" {
		t.Fatalf("unexpected item for min-salary annualized usd %#v", body)
	}
}

func TestJobsFilterOptionsIncludesHierarchyMetadata(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 201, "Data Engineer", "Engineering", "Data Engineer")
	insertJobWithFunction(t, db, 202, "Software Engineer", "Engineering", "Software Engineer")
	if _, err := db.SQL.ExecContext(context.Background(), `UPDATE parsed_jobs SET location_countries = ?, location_city = ?, location_us_states = ? WHERE raw_us_job_id = ?`, `["United States"]`, "Austin", `["Texas"]`, 3201); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `UPDATE parsed_jobs SET location_countries = ?, location_city = ?, location_us_states = ? WHERE raw_us_job_id = ?`, `["Canada"]`, "Toronto", `["Ontario"]`, 3202); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs/filter-options", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	jobCategoryParents := body["job_category_parents"].(map[string]any)
	if jobCategoryParents["Engineering"] != nil {
		t.Fatalf("expected Engineering root parent to be nil %#v", body)
	}
	if jobCategoryParents["Data Engineer"] != "Engineering" {
		t.Fatalf("missing Data Engineer parent metadata %#v", body)
	}
	locationParents := body["location_parents"].(map[string]any)
	texasParents := locationParents["Texas"].([]any)
	if len(texasParents) < 1 || texasParents[0] != "United States" {
		t.Fatalf("unexpected Texas parent metadata %#v", body)
	}
	if len(locationParents["United States"].([]any)) != 0 {
		t.Fatalf("expected country root to have no parents %#v", body)
	}
}

func TestJobsListLocationFilterSupportsStateWithCountryLabel(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 401, "https://example.com/state-1", "Austin", "Texas", 120, 160, true, time.Now().UTC())
	insertJob(t, db, 402, "https://example.com/state-2", "Seattle", "Washington", 120, 160, true, time.Now().UTC())

	req := httptest.NewRequest(http.MethodGet, "/jobs?location=Texas,+United+States", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["total"].(float64) != 1 {
		t.Fatalf("expected one location-matched job, got %#v", body)
	}
	if body["items"].([]any)[0].(map[string]any)["location_us_states"].([]any)[0] != "Texas" {
		t.Fatalf("unexpected location-matched item %#v", body)
	}
}

func TestJobDetailEndpoint(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()
	companyID := insertCompany(t, db, "Example Co")
	jobID := insertRichJob(t, db, companyID)

	req := httptest.NewRequest(http.MethodGet, "/job/"+strconv.Itoa(jobID), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["company_name"].(string) != "Example Co" || body["role_title"].(string) != "Staff Backend Engineer" || body["salary_type"].(string) != "hourly" || body["education_requirements_credential_category"].(string) != "bachelor" {
		t.Fatalf("unexpected detail payload %#v", body)
	}
}

func TestJobsListSupportsCSVFilters(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()
	insertCSVJob(t, db, 1, "Data Engineer", "United States", true, 100000, "yearly")
	insertCSVJob(t, db, 2, "Backend Engineer", "Canada", false, 60, "hourly")

	req := httptest.NewRequest(http.MethodGet, "/jobs?job_title=Data+Engineer,Backend+Engineer&location=United+States,Canada&seniority=mid,senior&min_salary=90000", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if int(body["total"].(float64)) != 2 || len(body["items"].([]any)) != 2 {
		t.Fatalf("unexpected csv filter payload %#v", body)
	}
}

func TestJobsPaginationStableWithCompanyJoin(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	code := requestLoginCode(t, router, "pagination@example.com")
	cookie := verifyLoginCode(t, router, "pagination@example.com", code)

	for idx := 0; idx < 25; idx++ {
		rawID := idx + 5000
		createdAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`, rawID, "https://example.com/pagination/"+strconv.Itoa(idx), createdAt.Format(time.RFC3339Nano))
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, created_at_source, url) VALUES (?, ?, ?, ?)`, rawID, "Role-"+strconv.Itoa(idx), createdAt.Format(time.RFC3339Nano), "https://jobs.example.com/pagination/"+strconv.Itoa(idx))
		if err != nil {
			t.Fatal(err)
		}
	}

	getPageIDs := func(page int) []int {
		req := httptest.NewRequest(http.MethodGet, "/jobs?per_page=10&page="+strconv.Itoa(page), nil)
		req.AddCookie(cookie)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		assertStatus(t, rec.Code, http.StatusOK)
		var body map[string]any
		decodeBody(t, rec.Body.Bytes(), &body)
		items := body["items"].([]any)
		out := make([]int, 0, len(items))
		for _, item := range items {
			out = append(out, int(item.(map[string]any)["id"].(float64)))
		}
		return out
	}

	page1 := getPageIDs(1)
	page2 := getPageIDs(2)
	page3 := getPageIDs(3)
	if len(page1) != 10 || len(page2) != 10 || len(page3) != 5 {
		t.Fatalf("unexpected page sizes p1=%d p2=%d p3=%d", len(page1), len(page2), len(page3))
	}
	if overlaps(page1, page2) || overlaps(page2, page3) || overlaps(page1, page3) {
		t.Fatalf("expected stable pagination without overlaps p1=%v p2=%v p3=%v", page1, page2, page3)
	}
}

func TestJobsTitleFilterMatchesExactFunction(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 61, "Platform Engineering", "backend", "Backend Lead")
	insertJobWithFunction(t, db, 62, "Data Science", "data", "ML Engineer")

	req := httptest.NewRequest(http.MethodGet, "/jobs?job_title=backend", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one function match, got %#v", body)
	}
	if items[0].(map[string]any)["categorized_job_title"].(string) != "Platform Engineering" {
		t.Fatalf("unexpected function match %#v", items[0])
	}
}

func TestJobsTitleFilterMatchesRoleTitleVariants(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 71, "Engineering Leadership", "platform", "backend")
	insertJobWithFunction(t, db, 72, "Infrastructure", "infra", "Senior Backend Platform Engineer")

	req := httptest.NewRequest(http.MethodGet, "/jobs?job_title=backend", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected fuzzy role-title matches like page-extract, got %#v", body)
	}
	seen := map[string]bool{}
	for _, item := range items {
		roleTitle, _ := item.(map[string]any)["role_title"].(string)
		seen[roleTitle] = true
	}
	if !seen["backend"] || !seen["Senior Backend Platform Engineer"] {
		t.Fatalf("unexpected fuzzy role-title matches %#v", body)
	}
}

func TestJobsTitleFilterIgnoresSpecialCharactersInRoleTitle(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 73, "Software Engineer", "frontend", "Frontend (React) Engineer")

	req := httptest.NewRequest(http.MethodGet, "/jobs?job_title=frontend+react+engineer", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one special character-tolerant match, got %#v", body)
	}
	if items[0].(map[string]any)["role_title"].(string) != "Frontend (React) Engineer" {
		t.Fatalf("unexpected special character role title match %#v", items[0])
	}
}

func TestJobsTitleFilterCombinesExactAndFreeTextMatches(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 74, "Blockchain Engineer", "Web3", "Web3 Smart Contract Engineer")
	insertJobWithFunction(t, db, 75, "Software Engineer", "Engineering", "Senior Golang Developer")

	req := httptest.NewRequest(http.MethodGet, "/jobs?job_title=web3,golang+developer", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected exact + free-text role matches, got %#v", body)
	}
	roles := map[string]struct{}{}
	for _, item := range items {
		roles[item.(map[string]any)["role_title"].(string)] = struct{}{}
	}
	if _, ok := roles["Web3 Smart Contract Engineer"]; !ok {
		t.Fatalf("missing exact function match %#v", body)
	}
	if _, ok := roles["Senior Golang Developer"]; !ok {
		t.Fatalf("missing free-text token match %#v", body)
	}
}

func TestJobsTechStackFilterAndOptions(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithTechStack(t, db, 81, "Platform Engineer", []string{"Go", "SQL"})
	insertJobWithTechStack(t, db, 82, "Frontend Engineer", []string{"TypeScript"})

	optionsReq := httptest.NewRequest(http.MethodGet, "/jobs/filter-options", nil)
	optionsRec := httptest.NewRecorder()
	router.ServeHTTP(optionsRec, optionsReq)
	assertStatus(t, optionsRec.Code, http.StatusOK)

	var optionsBody map[string]any
	decodeBody(t, optionsRec.Body.Bytes(), &optionsBody)
	techStacks := optionsBody["tech_stacks"].([]any)
	if len(techStacks) != 3 {
		t.Fatalf("unexpected tech stack options %#v", optionsBody)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?tech_stack=Go", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one tech stack match, got %#v", body)
	}
	firstItem := items[0].(map[string]any)
	if len(firstItem["tech_stack"].([]any)) != 2 {
		t.Fatalf("expected tech stack in job item, got %#v", firstItem)
	}
}

func TestJobsTechStackFilterSnakeCase(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithTechStack(t, db, 83, "Platform Engineer", []string{"Go", "SQL"})
	insertJobWithTechStack(t, db, 84, "Frontend Engineer", []string{"TypeScript"})

	req := httptest.NewRequest(http.MethodGet, "/jobs?tech_stack=Go", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one tech stack match, got %#v", body)
	}
}

func TestJobsPostDateFilterAndOptions(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertDatedJob(t, db, 91, time.Now().UTC().Add(-12*time.Hour))
	insertDatedJob(t, db, 92, time.Now().UTC().Add(-10*24*time.Hour))

	optionsReq := httptest.NewRequest(http.MethodGet, "/jobs/filter-options", nil)
	optionsRec := httptest.NewRecorder()
	router.ServeHTTP(optionsRec, optionsReq)
	assertStatus(t, optionsRec.Code, http.StatusOK)

	var optionsBody map[string]any
	decodeBody(t, optionsRec.Body.Bytes(), &optionsBody)
	if len(optionsBody["post_date_options"].([]any)) != 12 {
		t.Fatalf("unexpected post date options %#v", optionsBody)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?post_date=24_hours", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if len(body["items"].([]any)) != 1 {
		t.Fatalf("expected one recent job, got %#v", body)
	}
}

func TestJobsSupportsPostDateFromCutoff(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	now := time.Now().UTC()
	insertDatedJob(t, db, 911, now.Add(-72*time.Hour))
	insertDatedJob(t, db, 912, now.Add(-2*time.Hour))

	cutoff := now.Add(-24 * time.Hour).Format(time.RFC3339)

	req := httptest.NewRequest(http.MethodGet, "/jobs?post_date_from="+cutoff, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if len(body["items"].([]any)) != 1 {
		t.Fatalf("expected cutoff filter to return one job %#v", body)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/jobs/metrics?post_date_from="+cutoff, nil)
	metricsRec := httptest.NewRecorder()
	router.ServeHTTP(metricsRec, metricsReq)
	assertStatus(t, metricsRec.Code, http.StatusOK)
	var metricsBody map[string]any
	decodeBody(t, metricsRec.Body.Bytes(), &metricsBody)
	if metricsBody["jobs_today"].(float64) < 1 {
		t.Fatalf("expected metrics with cutoff to include recent job %#v", metricsBody)
	}
}

func TestJobsEmploymentTypeFilterAndOptions(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertEmploymentTypeJob(t, db, 101, "full-time")
	insertEmploymentTypeJob(t, db, 102, "contract")

	optionsReq := httptest.NewRequest(http.MethodGet, "/jobs/filter-options", nil)
	optionsRec := httptest.NewRecorder()
	router.ServeHTTP(optionsRec, optionsReq)
	assertStatus(t, optionsRec.Code, http.StatusOK)

	var optionsBody map[string]any
	decodeBody(t, optionsRec.Body.Bytes(), &optionsBody)
	if len(optionsBody["employment_types"].([]any)) != 2 {
		t.Fatalf("unexpected employment type options %#v", optionsBody)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?employment_type=contract", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["employment_type"].(string) != "contract" {
		t.Fatalf("unexpected employment type filter response %#v", body)
	}
}

func TestJobsStateAndCountryFiltersUseORAndNormalizeState(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 8401, "https://example.com/location-a", "Austin", "Texas", 100, 130, true, time.Now().UTC())
	insertJob(t, db, 8402, "https://example.com/location-b", "Toronto", "Ontario", 100, 130, true, time.Now().UTC())
	insertJob(t, db, 8403, "https://example.com/location-c", "Seattle", "Washington", 100, 130, true, time.Now().UTC())

	if _, err := db.SQL.ExecContext(
		context.Background(),
		`UPDATE parsed_jobs
		 SET location_city = 'Toronto',
		     location_us_states = '["Ontario"]',
		     location_countries = '["Canada"]'
		 WHERE raw_us_job_id = 8402`,
	); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?us_states=TX&countries=Canada&per_page=50", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["total"].(float64) != 2 {
		t.Fatalf("expected 2 location-matched jobs, got %#v", body)
	}

	items := body["items"].([]any)
	rawIDs := map[int]struct{}{}
	for _, item := range items {
		rawIDs[int(item.(map[string]any)["raw_us_job_id"].(float64))] = struct{}{}
	}
	if _, ok := rawIDs[8401]; !ok {
		t.Fatalf("expected Texas job in result %#v", body)
	}
	if _, ok := rawIDs[8402]; !ok {
		t.Fatalf("expected Canada job in result %#v", body)
	}
	if _, ok := rawIDs[8403]; ok {
		t.Fatalf("did not expect Washington/US-only job in result %#v", body)
	}
}

func TestJobsSeniorityFilter(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 8461, "https://example.com/exp-a", "Austin", "Texas", 100, 130, true, time.Now().UTC())
	insertJob(t, db, 8462, "https://example.com/exp-b", "Austin", "Texas", 100, 130, false, time.Now().UTC())

	req := httptest.NewRequest(http.MethodGet, "/jobs?seniority=senior&per_page=50", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one seniority match, got %#v", body)
	}
	if int(items[0].(map[string]any)["raw_us_job_id"].(float64)) != 8461 {
		t.Fatalf("unexpected seniority item %#v", body)
	}
}

func TestJobsTechStackFilterDoesNotFailOnNonJSONArrayData(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithTechStack(t, db, 8471, "Platform Engineer", []string{"Go"})
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`UPDATE parsed_jobs
		 SET tech_stack = '"Go"'
		 WHERE raw_us_job_id = 12471`,
	); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?techstack=go&per_page=50", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
}

func TestJobsFiltersCombineTechLocationAndSeniority(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithTechStack(t, db, 8451, "Platform Engineer", []string{"Go", "SQL"})
	insertJobWithTechStack(t, db, 8452, "Frontend Engineer", []string{"TypeScript"})
	if _, err := db.SQL.ExecContext(
		context.Background(),
		`UPDATE parsed_jobs
		 SET location_city = 'Austin',
		     location_us_states = '["Texas"]',
		     location_countries = '["United States"]',
		     is_senior = true
		 WHERE raw_us_job_id = 12451`,
	); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?tech_stack=Go&location=Texas&seniority=senior&per_page=50", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one combined-filter match, got %#v", body)
	}
	if int(items[0].(map[string]any)["raw_us_job_id"].(float64)) != 12451 {
		t.Fatalf("unexpected combined-filter item %#v", body)
	}
}

func TestJobsCategoryFunctionAndTitleFiltersCombineAsOR(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 8501, "Data Engineer", "Engineering", "Analytics Engineer")
	insertJobWithFunction(t, db, 8502, "Platform Engineer", "Platform", "SRE")
	insertJobWithFunction(t, db, 8503, "Product Designer", "Design", "UX Designer")

	req := httptest.NewRequest(http.MethodGet, "/jobs?job_categories=Data+Engineer&job_functions=Platform&job_titles=SRE&per_page=50", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["total"].(float64) != 2 {
		t.Fatalf("expected OR-combined filters to match 2 jobs, got %#v", body)
	}
}

func TestJobsJobTitlesFilterDoesNotMatchSingleTokenOnly(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 8701, "Product Manager", "Product", "Product Manager")
	insertJobWithFunction(t, db, 8702, "Product Designer", "Design", "Product Designer")
	insertJobWithFunction(t, db, 8703, "Engineering Manager", "Engineering", "Engineering Manager")

	req := httptest.NewRequest(http.MethodGet, "/jobs?job_titles=Product+Manager&per_page=50", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["total"].(float64) != 1 {
		t.Fatalf("expected exact title phrase filter to match only one job, got %#v", body)
	}
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one item, got %#v", body)
	}
	item := items[0].(map[string]any)
	if item["role_title"].(string) != "Product Manager" {
		t.Fatalf("unexpected item for job_titles filter %#v", item)
	}
}

func TestJobsJobTitlesFilterMatchesRoleTitleWhenAllTokensExist(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 8711, "Growth Lead", "Growth", "Senior Product Growth Manager")
	insertJobWithFunction(t, db, 8712, "Product Designer", "Design", "Senior Product Designer")
	insertJobWithFunction(t, db, 8713, "Engineering Manager", "Engineering", "Engineering Manager")

	req := httptest.NewRequest(http.MethodGet, "/jobs?job_titles=Product+Manager&per_page=50", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["total"].(float64) != 1 {
		t.Fatalf("expected token-group title filter to match one role_title row, got %#v", body)
	}
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one item, got %#v", body)
	}
	item := items[0].(map[string]any)
	if item["role_title"].(string) != "Senior Product Growth Manager" {
		t.Fatalf("unexpected item for token-group title filter %#v", item)
	}
}

func TestJobsUserJobActionNotAppliedExcludesAppliedAndHidden(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 8601, "https://example.com/not-applied-a", "Austin", "Texas", 100, 130, true, time.Now().UTC())
	insertJob(t, db, 8602, "https://example.com/not-applied-b", "Austin", "Texas", 100, 130, true, time.Now().UTC())
	insertJob(t, db, 8603, "https://example.com/not-applied-c", "Austin", "Texas", 100, 130, true, time.Now().UTC())

	code := requestLoginCode(t, router, "not-applied@example.com")
	cookie := verifyLoginCode(t, router, "not-applied@example.com", code)

	appliedBody, _ := json.Marshal(map[string]any{"is_applied": true})
	appliedReq := httptest.NewRequest(http.MethodPut, "/job-actions/1", bytes.NewReader(appliedBody))
	appliedReq.Header.Set("Content-Type", "application/json")
	appliedReq.AddCookie(cookie)
	appliedRec := httptest.NewRecorder()
	router.ServeHTTP(appliedRec, appliedReq)
	assertStatus(t, appliedRec.Code, http.StatusOK)

	hiddenBody, _ := json.Marshal(map[string]any{"is_hidden": true})
	hiddenReq := httptest.NewRequest(http.MethodPut, "/job-actions/2", bytes.NewReader(hiddenBody))
	hiddenReq.Header.Set("Content-Type", "application/json")
	hiddenReq.AddCookie(cookie)
	hiddenRec := httptest.NewRecorder()
	router.ServeHTTP(hiddenRec, hiddenReq)
	assertStatus(t, hiddenRec.Code, http.StatusOK)

	req := httptest.NewRequest(http.MethodGet, "/jobs?user_job_action=not_applied", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["total"].(float64) != 1 {
		t.Fatalf("expected only untouched jobs for not_applied filter, got %#v", body)
	}
	items := body["items"].([]any)
	if len(items) != 1 || int(items[0].(map[string]any)["id"].(float64)) != 3 {
		t.Fatalf("expected parsed job id 3 to remain, got %#v", body)
	}
}

func TestJobsMetricsCountsWithFilters(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	now := time.Now().UTC()
	insertJob(t, db, 8101, "https://example.com/metrics-a", "Austin", "Texas", 100, 130, true, now.Add(-20*time.Minute))
	insertJob(t, db, 8102, "https://example.com/metrics-b", "Austin", "Texas", 100, 130, true, now.Add(-3*time.Hour))
	insertJob(t, db, 8103, "https://example.com/metrics-c", "Austin", "Texas", 100, 130, true, now.Add(-25*time.Hour))

	req := httptest.NewRequest(http.MethodGet, "/jobs/metrics?job_title=Software+Engineer&location=Austin", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["jobs_today"].(float64) != 2 || body["jobs_last_hour"].(float64) != 1 {
		t.Fatalf("unexpected metrics payload %#v", body)
	}
}

func TestJobsMetricsUserActionHiddenFilter(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 8201, "https://example.com/metrics-hidden-a", "Austin", "Texas", 100, 130, true, time.Now().UTC().Add(-30*time.Minute))
	insertJob(t, db, 8202, "https://example.com/metrics-hidden-b", "Austin", "Texas", 100, 130, true, time.Now().UTC().Add(-20*time.Minute))

	code := requestLoginCode(t, router, "metrics-hidden@example.com")
	cookie := verifyLoginCode(t, router, "metrics-hidden@example.com", code)

	hideBody, _ := json.Marshal(map[string]any{"is_hidden": true})
	hideReq := httptest.NewRequest(http.MethodPut, "/job-actions/2", bytes.NewReader(hideBody))
	hideReq.Header.Set("Content-Type", "application/json")
	hideReq.AddCookie(cookie)
	hideRec := httptest.NewRecorder()
	router.ServeHTTP(hideRec, hideReq)
	assertStatus(t, hideRec.Code, http.StatusOK)

	defaultReq := httptest.NewRequest(http.MethodGet, "/jobs/metrics", nil)
	defaultReq.AddCookie(cookie)
	defaultRec := httptest.NewRecorder()
	router.ServeHTTP(defaultRec, defaultReq)
	assertStatus(t, defaultRec.Code, http.StatusOK)
	var defaultBody map[string]any
	decodeBody(t, defaultRec.Body.Bytes(), &defaultBody)
	if defaultBody["jobs_today"].(float64) != 1 {
		t.Fatalf("expected hidden jobs excluded in default metrics %#v", defaultBody)
	}

	hiddenReq := httptest.NewRequest(http.MethodGet, "/jobs/metrics?user_job_action=hidden", nil)
	hiddenReq.AddCookie(cookie)
	hiddenRec := httptest.NewRecorder()
	router.ServeHTTP(hiddenRec, hiddenReq)
	assertStatus(t, hiddenRec.Code, http.StatusOK)
	var hiddenBody map[string]any
	decodeBody(t, hiddenRec.Body.Bytes(), &hiddenBody)
	if hiddenBody["jobs_today"].(float64) != 1 {
		t.Fatalf("expected hidden-only metrics count %#v", hiddenBody)
	}
}

func TestClearSelectedJobActionBucket(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 8301, "https://example.com/clear-a", "Austin", "Texas", 100, 130, true, time.Now().UTC())
	insertJob(t, db, 8302, "https://example.com/clear-b", "Austin", "Texas", 100, 130, true, time.Now().UTC())
	insertJob(t, db, 8303, "https://example.com/clear-c", "Austin", "Texas", 100, 130, true, time.Now().UTC())

	code := requestLoginCode(t, router, "clear-user@example.com")
	cookie := verifyLoginCode(t, router, "clear-user@example.com", code)

	for _, pair := range []struct {
		jobID string
		body  string
	}{
		{"1", `{"is_saved":true}`},
		{"2", `{"is_saved":true}`},
		{"2", `{"is_applied":true}`},
		{"3", `{"is_applied":true}`},
		{"3", `{"is_hidden":true}`},
	} {
		req := httptest.NewRequest(http.MethodPut, "/job-actions/"+pair.jobID, strings.NewReader(pair.body))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(cookie)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		assertStatus(t, rec.Code, http.StatusOK)
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/job-actions/summary", nil)
	summaryReq.AddCookie(cookie)
	summaryRec := httptest.NewRecorder()
	router.ServeHTTP(summaryRec, summaryReq)
	assertStatus(t, summaryRec.Code, http.StatusOK)
	var before map[string]any
	decodeBody(t, summaryRec.Body.Bytes(), &before)
	if before["applied_count"] != float64(2) || before["saved_count"] != float64(2) || before["hidden_count"] != float64(1) {
		t.Fatalf("unexpected pre-clear summary %#v", before)
	}

	clearSavedReq := httptest.NewRequest(http.MethodPost, "/job-actions/clear?action=saved", nil)
	clearSavedReq.AddCookie(cookie)
	clearSavedRec := httptest.NewRecorder()
	router.ServeHTTP(clearSavedRec, clearSavedReq)
	assertStatus(t, clearSavedRec.Code, http.StatusOK)

	clearHiddenReq := httptest.NewRequest(http.MethodPost, "/job-actions/clear?action=hidden", nil)
	clearHiddenReq.AddCookie(cookie)
	clearHiddenRec := httptest.NewRecorder()
	router.ServeHTTP(clearHiddenRec, clearHiddenReq)
	assertStatus(t, clearHiddenRec.Code, http.StatusOK)

	clearAppliedReq := httptest.NewRequest(http.MethodPost, "/job-actions/clear?action=applied", nil)
	clearAppliedReq.AddCookie(cookie)
	clearAppliedRec := httptest.NewRecorder()
	router.ServeHTTP(clearAppliedRec, clearAppliedReq)
	assertStatus(t, clearAppliedRec.Code, http.StatusOK)

	finalSummaryReq := httptest.NewRequest(http.MethodGet, "/job-actions/summary", nil)
	finalSummaryReq.AddCookie(cookie)
	finalSummaryRec := httptest.NewRecorder()
	router.ServeHTTP(finalSummaryRec, finalSummaryReq)
	assertStatus(t, finalSummaryRec.Code, http.StatusOK)
	var after map[string]any
	decodeBody(t, finalSummaryRec.Body.Bytes(), &after)
	if after["applied_count"] != float64(0) || after["saved_count"] != float64(0) || after["hidden_count"] != float64(0) {
		t.Fatalf("unexpected post-clear summary %#v", after)
	}

	invalidReq := httptest.NewRequest(http.MethodPost, "/job-actions/clear?action=invalid", nil)
	invalidReq.AddCookie(cookie)
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)
	assertStatus(t, invalidRec.Code, http.StatusBadRequest)
}

func TestJobsListFiltersByCompanySlug(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	var acmeID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO parsed_companies (name, slug) VALUES ('Acme', 'acme') RETURNING id`).Scan(&acmeID); err != nil {
		t.Fatal(err)
	}
	var globexID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO parsed_companies (name, slug) VALUES ('Globex', 'globex') RETURNING id`).Scan(&globexID); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (91001, 'https://example.com/company-acme', ?, true, false, true, 0, '{}')`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (91002, 'https://example.com/company-globex', ?, true, false, true, 0, '{}')`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, company_id, role_title, created_at_source, url) VALUES (91001, ?, 'Backend Engineer', ?, 'https://jobs.example.com/company-acme')`, acmeID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, company_id, role_title, created_at_source, url) VALUES (91002, ?, 'Data Engineer', ?, 'https://jobs.example.com/company-globex')`, globexID, now); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?company=acme", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if int(body["total"].(float64)) != 1 {
		t.Fatalf("expected one company-filtered row %#v", body)
	}
	item := body["items"].([]any)[0].(map[string]any)
	if item["company_slug"] != "acme" || item["role_title"] != "Backend Engineer" {
		t.Fatalf("unexpected company-filtered item %#v", item)
	}
}

func TestCompanyProfileEndpointReturnsCompanyAndStats(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	industries, _ := json.Marshal([]string{"SaaS", "AI"})
	var companyID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO parsed_companies (name, slug, tagline, industry_specialities) VALUES ('Acme', 'acme', 'Builds tools', ?) RETURNING id`, string(industries)).Scan(&companyID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (92001, 'https://example.com/company-profile-job', ?, true, false, true, 0, '{}')`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, company_id, role_title, created_at_source, url) VALUES (92001, ?, 'Staff Engineer', ?, 'https://jobs.example.com/company-profile-job')`, companyID, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/companies/acme", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["slug"] != "acme" || body["name"] != "Acme" || body["tagline"] != "Builds tools" {
		t.Fatalf("unexpected company profile payload %#v", body)
	}
	if body["total_jobs"] != float64(1) {
		t.Fatalf("unexpected company profile stats %#v", body)
	}
	industryValues := body["industry_specialities"].([]any)
	if len(industryValues) != 2 || industryValues[0] != "SaaS" || industryValues[1] != "AI" {
		t.Fatalf("unexpected industry specialities %#v", body["industry_specialities"])
	}
}

func TestCompaniesSitemapEndpointListsCompanySlugs(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	var companyID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO parsed_companies (name, slug) VALUES ('Acme', 'acme') RETURNING id`).Scan(&companyID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (93001, 'https://example.com/company-sitemap-job', ?, true, false, true, 0, '{}')`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, company_id, role_title, created_at_source, url) VALUES (93001, ?, 'Engineer', ?, 'https://jobs.example.com/company-sitemap-job')`, companyID, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/companies/sitemap?page=1&per_page=10", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if int(body["total"].(float64)) != 1 {
		t.Fatalf("unexpected companies sitemap total %#v", body)
	}
	items := body["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["slug"] != "acme" {
		t.Fatalf("unexpected companies sitemap items %#v", body)
	}
}

func TestPricingFlow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	req := httptest.NewRequest(http.MethodGet, "/pricing/plans", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	code := requestLoginCode(t, router, "price-user@example.com")
	cookie := verifyLoginCode(t, router, "price-user@example.com", code)

	body, _ := json.Marshal(map[string]any{"plan_code": "monthly", "provider": "crypto", "payment_method": "crypto"})
	subscribeReq := httptest.NewRequest(http.MethodPost, "/pricing/subscribe", bytes.NewReader(body))
	subscribeReq.Header.Set("Content-Type", "application/json")
	subscribeReq.AddCookie(cookie)
	subscribeRec := httptest.NewRecorder()
	router.ServeHTTP(subscribeRec, subscribeReq)
	assertStatus(t, subscribeRec.Code, http.StatusOK)

	var payload map[string]any
	decodeBody(t, subscribeRec.Body.Bytes(), &payload)
	if payload["crypto_payment"] == nil {
		t.Fatalf("expected crypto payment payload %#v", payload)
	}
	cryptoPayment := payload["crypto_payment"].(map[string]any)
	if cryptoPayment["invoice_url"] == nil || cryptoPayment["invoice_url"] == "" {
		t.Fatalf("expected invoice_url in crypto payment payload %#v", cryptoPayment)
	}
	paymentID := int(payload["payment_id"].(float64))

	confirmReq := httptest.NewRequest(http.MethodPost, "/pricing/payments/"+strconv.Itoa(paymentID)+"/confirm", nil)
	confirmReq.AddCookie(cookie)
	confirmRec := httptest.NewRecorder()
	router.ServeHTTP(confirmRec, confirmReq)
	assertStatus(t, confirmRec.Code, http.StatusOK)
}

func TestConfirmPaymentDoesNotActivatePendingStripePayment(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	code := requestLoginCode(t, router, "pending-stripe@example.com")
	cookie := verifyLoginCode(t, router, "pending-stripe@example.com", code)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	var userID int64
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT id FROM auth_users WHERE email = ? LIMIT 1`, "pending-stripe@example.com").Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var planID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_plans (code, name, billing_cycle, duration_days, price_usd, is_active, created_at) VALUES (?, ?, ?, ?, ?, true, ?) RETURNING id`, "stripe-monthly", "Stripe Monthly", "monthly", 30, 10, now).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	var paymentID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_payments (user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, provider_payload, created_at) VALUES (?, ?, 'stripe', 'card', 'USD', 1000, 'pending', ?, '{}', ?) RETURNING id`, userID, planID, "cs_test_pending", now).Scan(&paymentID); err != nil {
		t.Fatal(err)
	}

	confirmReq := httptest.NewRequest(http.MethodPost, "/pricing/payments/"+strconv.FormatInt(paymentID, 10)+"/confirm", nil)
	confirmReq.AddCookie(cookie)
	confirmRec := httptest.NewRecorder()
	router.ServeHTTP(confirmRec, confirmReq)
	assertStatus(t, confirmRec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, confirmRec.Body.Bytes(), &body)
	if body["payment_status"] != "pending" {
		t.Fatalf("expected pending payment status, got %#v", body)
	}

	var status string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT status FROM pricing_payments WHERE id = ?`, paymentID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("expected DB payment status pending, got %q", status)
	}

	var subCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM user_subscriptions WHERE user_id = ?`, userID).Scan(&subCount); err != nil {
		t.Fatal(err)
	}
	if subCount != 1 {
		t.Fatalf("expected only default free subscription record, got %d", subCount)
	}
}

func TestPricingProvidersEndpointReportsEnabledMethods(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	_ = os.Setenv("STRIPE_SECRET_KEY", "sk_test_enabled")
	_ = os.Setenv("DODO_API_KEY", "dodo_key")
	_ = os.Setenv("DODO_PRODUCT_ID_WEEKLY", "prod_w")
	_ = os.Setenv("DODO_PRODUCT_ID_MONTHLY", "prod_m")
	_ = os.Setenv("DODO_PRODUCT_ID_QUARTERLY", "prod_q")
	_ = os.Setenv("DODO_PRODUCT_ID_YEARLY", "prod_y")
	t.Cleanup(func() {
		_ = os.Unsetenv("STRIPE_SECRET_KEY")
		_ = os.Unsetenv("DODO_API_KEY")
		_ = os.Unsetenv("DODO_PRODUCT_ID_WEEKLY")
		_ = os.Unsetenv("DODO_PRODUCT_ID_MONTHLY")
		_ = os.Unsetenv("DODO_PRODUCT_ID_QUARTERLY")
		_ = os.Unsetenv("DODO_PRODUCT_ID_YEARLY")
	})

	req := httptest.NewRequest(http.MethodGet, "/pricing/providers", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 3 {
		t.Fatalf("unexpected providers payload %#v", body)
	}
	var stripe, dodo, crypto map[string]any
	for _, item := range items {
		row := item.(map[string]any)
		if row["provider"] == "stripe" {
			stripe = row
		}
		if row["provider"] == "dodo" {
			dodo = row
		}
		if row["provider"] == "crypto" {
			crypto = row
		}
	}
	if stripe["enabled"] != true || len(stripe["payment_methods"].([]any)) == 0 {
		t.Fatalf("unexpected stripe provider %#v", stripe)
	}
	if dodo["enabled"] != true || len(dodo["payment_methods"].([]any)) == 0 {
		t.Fatalf("unexpected dodo provider %#v", dodo)
	}
	if crypto["enabled"] != true || len(crypto["payment_methods"].([]any)) == 0 {
		t.Fatalf("unexpected crypto provider %#v", crypto)
	}
}

func TestDefaultFreeSubscriptionAndUpgradePreview(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_upgrade_preview")
	cfg.AuthDebugReturnCode = true
	cfg.AuthEnableCodeLogin = true
	cfg.PublicJobsMaxPerPage = 3
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	for idx := 0; idx < 7; idx++ {
		insertJob(t, db, idx+100, "https://example.com/free-"+strconv.Itoa(idx), "City-"+strconv.Itoa(idx), "State", 100, 110, idx%2 == 0, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))
	}

	code := requestLoginCode(t, router, "upgrade-user@example.com")
	cookie := verifyLoginCode(t, router, "upgrade-user@example.com", code)

	var activeCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(1) FROM user_subscriptions WHERE ends_at > ?`, time.Now().UTC().Format(time.RFC3339Nano)).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 1 {
		t.Fatalf("expected one active free subscription, got %d", activeCount)
	}

	_, err = db.SQL.ExecContext(context.Background(), `UPDATE user_subscriptions SET ends_at = ?, is_active = false`, time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs?per_page=100&page=1", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["is_preview"] != true || body["requires_login"] != false || body["requires_upgrade"] != true {
		t.Fatalf("unexpected upgrade preview payload %#v", body)
	}
}

func TestSubscriptionStatusHandlesExpiredState(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	code := requestLoginCode(t, router, "expired-user@example.com")
	cookie := verifyLoginCode(t, router, "expired-user@example.com", code)

	_, err := db.SQL.ExecContext(context.Background(), `UPDATE user_subscriptions SET ends_at = ?, is_active = true`, time.Now().UTC().Add(-2*time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/pricing/subscription", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	if body["is_active"] != false || body["status"] != "expired" || int(body["days_left"].(float64)) != 0 {
		t.Fatalf("unexpected expired subscription payload %#v", body)
	}
}

func TestExpiredFreePlanIsNotRecreatedOnLogin(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	code := requestLoginCode(t, router, "expired-free@example.com")
	_ = verifyLoginCode(t, router, "expired-free@example.com", code)

	_, err := db.SQL.ExecContext(context.Background(), `UPDATE user_subscriptions SET ends_at = ?, is_active = false`, time.Now().UTC().Add(-2*time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}

	code = requestLoginCode(t, router, "expired-free@example.com")
	_ = verifyLoginCode(t, router, "expired-free@example.com", code)

	var subscriptionCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(1) FROM user_subscriptions`).Scan(&subscriptionCount); err != nil {
		t.Fatal(err)
	}
	if subscriptionCount != 1 {
		t.Fatalf("expected single subscription record after re-login, got %d", subscriptionCount)
	}
}

func TestCryptoWebhookRequiresSignatureAndActivatesSubscription(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_crypto_webhook_paid")
	cfg.AuthDebugReturnCode = true
	cfg.AuthEnableCodeLogin = true
	cfg.CryptoPaymentProvider = "nowpayments"
	cfg.NowPaymentsIPNSecret = "secret-token"
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	code := requestLoginCode(t, router, "crypto-user@example.com")
	cookie := verifyLoginCode(t, router, "crypto-user@example.com", code)

	body, _ := json.Marshal(map[string]any{"plan_code": "monthly", "provider": "crypto", "payment_method": "crypto"})
	subscribeReq := httptest.NewRequest(http.MethodPost, "/pricing/subscribe", bytes.NewReader(body))
	subscribeReq.Header.Set("Content-Type", "application/json")
	subscribeReq.AddCookie(cookie)
	subscribeRec := httptest.NewRecorder()
	router.ServeHTTP(subscribeRec, subscribeReq)
	assertStatus(t, subscribeRec.Code, http.StatusOK)
	var subscribeBody map[string]any
	decodeBody(t, subscribeRec.Body.Bytes(), &subscribeBody)
	paymentID := int(subscribeBody["payment_id"].(float64))

	webhookPayload := map[string]any{"order_id": strconv.Itoa(paymentID), "payment_id": "np_1", "payment_status": "finished"}
	rawPayload, _ := json.Marshal(webhookPayload)
	mac := hmac.New(sha512.New, []byte("secret-token"))
	_, _ = mac.Write(rawPayload)
	signature := fmt.Sprintf("%x", mac.Sum(nil))

	webhookReq := httptest.NewRequest(http.MethodPost, "/pricing/webhooks/crypto", bytes.NewReader(rawPayload))
	webhookReq.Header.Set("Content-Type", "application/json")
	webhookReq.Header.Set("x-nowpayments-sig", signature)
	webhookRec := httptest.NewRecorder()
	router.ServeHTTP(webhookRec, webhookReq)
	assertStatus(t, webhookRec.Code, http.StatusOK)
}

func TestOxaPayWebhookRequestShapeMarksPaymentPaid(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_oxapay_webhook_paid")
	cfg.CryptoPaymentProvider = "oxapay"
	cfg.OxaPayMerchantAPIKey = "secret-token"
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	var userID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO auth_users (email, created_at) VALUES (?, ?) RETURNING id`, "oxapay-user@example.com", now).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var planID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_plans (code, name, billing_cycle, duration_days, price_usd, is_active, created_at) VALUES (?, ?, ?, ?, ?, true, ?) RETURNING id`, "weekly", "Weekly", "weekly", 7, 3, now).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	var paymentID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_payments (user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, created_at) VALUES (?, ?, 'crypto', 'crypto', 'USD', 300, 'pending', ?, ?) RETURNING id`, userID, planID, "140013835", now).Scan(&paymentID); err != nil {
		t.Fatal(err)
	}

	bodyObj := map[string]any{
		"track_id":            "140013835",
		"status":              "Paid",
		"type":                "invoice",
		"module_name":         "goapplyjob",
		"amount":              3,
		"currency":            "USD",
		"value":               3.0440897036220003,
		"sent_value":          3.0440897036220003,
		"order_id":            strconv.FormatInt(paymentID, 10),
		"email":               "neverdreamagain9106@gmail.com",
		"note":                "",
		"fee_paid_by_payer":   1,
		"under_paid_coverage": 2.5,
		"description":         "GoApplyJob Weekly plan",
		"date":                1771269963,
		"txs": []map[string]any{{
			"status":                "confirmed",
			"tx_hash":               "sandbox",
			"sent_amount":           0.0015427329,
			"sent_value":            3.0440897036220003,
			"received_amount":       0.0015427329,
			"value":                 3.0440897036220003,
			"currency":              "ETH",
			"network":               "Ethereum Network",
			"sender_address":        "",
			"address":               "sandbox",
			"memo":                  "",
			"rate":                  1,
			"confirmations":         10,
			"auto_convert_amount":   0,
			"auto_convert_currency": "",
			"date":                  1771269981,
		}},
	}
	rawBody, _ := json.Marshal(bodyObj)
	mac := hmac.New(sha512.New, []byte("secret-token"))
	_, _ = mac.Write(rawBody)
	signature := fmt.Sprintf("%x", mac.Sum(nil))

	webhookReq := httptest.NewRequest(http.MethodPost, "/pricing/webhooks/crypto", bytes.NewReader(rawBody))
	webhookReq.Header.Set("Content-Type", "application/json")
	webhookReq.Header.Set("Hmac", signature)
	webhookReq.Header.Set("X-Request-Id", "69936f613eda0")
	webhookReq.Header.Set("X-Timestamp", "1771269985")
	webhookReq.Header.Set("X-Webhook-Version", "v1")
	webhookRec := httptest.NewRecorder()
	router.ServeHTTP(webhookRec, webhookReq)
	assertStatus(t, webhookRec.Code, http.StatusOK)

	var status string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT status FROM pricing_payments WHERE id = ?`, paymentID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "paid" {
		t.Fatalf("expected paid payment status, got %q", status)
	}
	var subscriptionCount int
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM user_subscriptions WHERE user_id = ?`, userID).Scan(&subscriptionCount); err != nil {
		t.Fatal(err)
	}
	if subscriptionCount != 1 {
		t.Fatalf("expected one user subscription, got %d", subscriptionCount)
	}
}

func TestPricingCryptoCurrenciesSupportsAmountFiltering(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_currency_filtering")
	cfg.NowPaymentsCurrencyCandidates = "btc,eth,usdttrc20"
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	req := httptest.NewRequest(http.MethodGet, "/pricing/crypto/currencies?amount_usd=10", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("unexpected currencies payload %#v", body)
	}
	first := items[0].(map[string]any)
	if _, ok := first["min_usd"]; !ok {
		t.Fatalf("expected min_usd field in payload %#v", first)
	}
}

func TestStripeWebhookRequiresSignatureWhenSecretConfigured(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	_ = os.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_test")
	t.Cleanup(func() { _ = os.Unsetenv("STRIPE_WEBHOOK_SECRET") })

	webhookReq := httptest.NewRequest(http.MethodPost, "/pricing/webhooks/stripe", bytes.NewReader([]byte(`{"type":"checkout.session.completed","data":{"object":{"metadata":{"payment_id":"1"}}}}`)))
	webhookReq.Header.Set("Content-Type", "application/json")
	webhookRec := httptest.NewRecorder()
	router.ServeHTTP(webhookRec, webhookReq)
	assertStatus(t, webhookRec.Code, http.StatusBadRequest)
}

func TestCancelStripeSubscriptionCallsProviderAndUpdatesPayload(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	stripeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/v1/subscriptions/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body := `{"id":"sub_test_123","status":"active","cancel_at_period_end":true,"current_period_end":1893456000,"canceled_at":null}`
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer stripeSrv.Close()

	_ = os.Setenv("STRIPE_SECRET_KEY", "sk_test")
	_ = os.Setenv("STRIPE_API_BASE_URL", stripeSrv.URL+"/v1")
	t.Cleanup(func() {
		_ = os.Unsetenv("STRIPE_SECRET_KEY")
		_ = os.Unsetenv("STRIPE_API_BASE_URL")
	})

	code := requestLoginCode(t, router, "cancel-stripe@example.com")
	cookie := verifyLoginCode(t, router, "cancel-stripe@example.com", code)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	var userID int64
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT id FROM auth_users WHERE email = ? LIMIT 1`, "cancel-stripe@example.com").Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var planID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_plans (code, name, billing_cycle, duration_days, price_usd, is_active, created_at) VALUES (?, ?, ?, ?, ?, true, ?) RETURNING id`, "stripe-weekly-cancel", "Stripe Weekly Cancel", "weekly", 7, 3, now).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	var paymentID int64
	payload := `{"stripe_subscription_id":"sub_test_123"}`
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_payments (user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_payload, created_at, paid_at) VALUES (?, ?, 'stripe', 'card', 'USD', 300, 'paid', ?, ?, ?) RETURNING id`, userID, planID, payload, now, now).Scan(&paymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `UPDATE user_subscriptions SET is_active = false WHERE user_id = ?`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO user_subscriptions (user_id, pricing_plan_id, starts_at, ends_at, is_active, created_at) VALUES (?, ?, ?, ?, true, ?)`,
		userID,
		planID,
		time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
		time.Now().UTC().Add(24*time.Hour).Format(time.RFC3339Nano),
		now,
	); err != nil {
		t.Fatal(err)
	}

	cancelReq := httptest.NewRequest(http.MethodPost, "/pricing/subscription/cancel", nil)
	cancelReq.AddCookie(cookie)
	cancelRec := httptest.NewRecorder()
	router.ServeHTTP(cancelRec, cancelReq)
	assertStatus(t, cancelRec.Code, http.StatusOK)

	var providerPayload []byte
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT provider_payload FROM pricing_payments WHERE id = ?`, paymentID).Scan(&providerPayload); err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal(providerPayload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored["stripe_cancel_at_period_end"] != true {
		t.Fatalf("expected stripe_cancel_at_period_end=true, got %#v", stored)
	}
}

func TestDodoWebhookPaidActivatesSubscription(t *testing.T) {
	_ = os.Setenv("DODO_API_KEY", "dodo_key")
	t.Cleanup(func() {
		_ = os.Unsetenv("DODO_API_KEY")
	})

	router, db := testRouter(t)
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	var userID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO auth_users (email, created_at) VALUES (?, ?) RETURNING id`, "dodo-user@example.com", now).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var planID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_plans (code, name, billing_cycle, duration_days, price_usd, is_active, created_at) VALUES (?, ?, ?, ?, ?, true, ?) RETURNING id`, "dodo-weekly", "Dodo Weekly", "weekly", 7, 3, now).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	var paymentID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_payments (user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, provider_payload, created_at) VALUES (?, ?, 'dodo', 'card', 'USD', 300, 'pending', ?, '{}', ?) RETURNING id`, userID, planID, "dodo_session_1", now).Scan(&paymentID); err != nil {
		t.Fatal(err)
	}

	webhookBody := map[string]any{
		"type": "payment.succeeded",
		"data": map[string]any{
			"object": map[string]any{
				"status": "succeeded",
				"metadata": map[string]any{
					"payment_id": strconv.FormatInt(paymentID, 10),
				},
			},
		},
	}
	raw, _ := json.Marshal(webhookBody)
	req := httptest.NewRequest(http.MethodPost, "/pricing/webhooks/dodo", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var status string
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT status FROM pricing_payments WHERE id = ?`, paymentID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "paid" {
		t.Fatalf("expected paid payment status, got %q", status)
	}
}

func TestCancelDodoSubscriptionUpdatesPayload(t *testing.T) {
	dodoSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/subscriptions/sub_dodo_123" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"active","cancel_at_next_billing_date":true,"next_billing_date":"2026-12-31T00:00:00Z"}`))
	}))
	defer dodoSrv.Close()

	router, db := testRouter(t)
	defer db.Close()

	_ = os.Setenv("DODO_API_KEY", "dodo_key")
	_ = os.Setenv("DODO_PRODUCT_ID_WEEKLY", "prod_w")
	_ = os.Setenv("DODO_PRODUCT_ID_MONTHLY", "prod_m")
	_ = os.Setenv("DODO_PRODUCT_ID_QUARTERLY", "prod_q")
	_ = os.Setenv("DODO_PRODUCT_ID_YEARLY", "prod_y")
	_ = os.Setenv("DODO_API_BASE_URL", dodoSrv.URL)
	t.Cleanup(func() {
		_ = os.Unsetenv("DODO_API_KEY")
		_ = os.Unsetenv("DODO_PRODUCT_ID_WEEKLY")
		_ = os.Unsetenv("DODO_PRODUCT_ID_MONTHLY")
		_ = os.Unsetenv("DODO_PRODUCT_ID_QUARTERLY")
		_ = os.Unsetenv("DODO_PRODUCT_ID_YEARLY")
		_ = os.Unsetenv("DODO_API_BASE_URL")
	})

	code := requestLoginCode(t, router, "cancel-dodo@example.com")
	cookie := verifyLoginCode(t, router, "cancel-dodo@example.com", code)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	var userID int64
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT id FROM auth_users WHERE email = ? LIMIT 1`, "cancel-dodo@example.com").Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var planID int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_plans (code, name, billing_cycle, duration_days, price_usd, is_active, created_at) VALUES (?, ?, ?, ?, ?, true, ?) RETURNING id`, "dodo-cancel-weekly", "Dodo Cancel Weekly", "weekly", 7, 3, now).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	var paymentID int64
	payload := `{"dodo_subscription_id":"sub_dodo_123"}`
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO pricing_payments (user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_payload, created_at, paid_at) VALUES (?, ?, 'dodo', 'card', 'USD', 300, 'paid', ?, ?, ?) RETURNING id`, userID, planID, payload, now, now).Scan(&paymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `UPDATE user_subscriptions SET is_active = false WHERE user_id = ?`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `INSERT INTO user_subscriptions (user_id, pricing_plan_id, starts_at, ends_at, is_active, created_at) VALUES (?, ?, ?, ?, true, ?)`,
		userID,
		planID,
		time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
		time.Now().UTC().Add(24*time.Hour).Format(time.RFC3339Nano),
		now,
	); err != nil {
		t.Fatal(err)
	}

	cancelReq := httptest.NewRequest(http.MethodPost, "/pricing/subscription/cancel", nil)
	cancelReq.AddCookie(cookie)
	cancelRec := httptest.NewRecorder()
	router.ServeHTTP(cancelRec, cancelReq)
	assertStatus(t, cancelRec.Code, http.StatusOK)

	var providerPayload []byte
	if err := db.SQL.QueryRowContext(context.Background(), `SELECT provider_payload FROM pricing_payments WHERE id = ?`, paymentID).Scan(&providerPayload); err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal(providerPayload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored["dodo_cancel_at_period_end"] != true {
		t.Fatalf("expected dodo_cancel_at_period_end=true, got %#v", stored)
	}
}

func TestPasswordSignupAndLoginFlow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()
	signupBody, _ := json.Marshal(map[string]string{"email": "pw-user@example.com", "password": "StrongPass123"})
	signupReq := httptest.NewRequest(http.MethodPost, "/auth/password/signup", bytes.NewReader(signupBody))
	signupReq.Header.Set("Content-Type", "application/json")
	signupRec := httptest.NewRecorder()
	router.ServeHTTP(signupRec, signupReq)
	assertStatus(t, signupRec.Code, http.StatusOK)

	meAfterSignupReq := httptest.NewRequest(http.MethodGet, "/auth/me", nil)
	meAfterSignupReq.AddCookie(signupRec.Result().Cookies()[0])
	meAfterSignupRec := httptest.NewRecorder()
	router.ServeHTTP(meAfterSignupRec, meAfterSignupReq)
	assertStatus(t, meAfterSignupRec.Code, http.StatusOK)
	var meAfterSignup map[string]any
	decodeBody(t, meAfterSignupRec.Body.Bytes(), &meAfterSignup)
	if meAfterSignup["email"].(string) != "pw-user@example.com" {
		t.Fatalf("unexpected me signup payload %#v", meAfterSignup)
	}

	cookie := signupRec.Result().Cookies()[0]
	logoutReq := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logoutReq.AddCookie(cookie)
	logoutRec := httptest.NewRecorder()
	router.ServeHTTP(logoutRec, logoutReq)
	assertStatus(t, logoutRec.Code, http.StatusOK)

	loginBody, _ := json.Marshal(map[string]string{"email": "pw-user@example.com", "password": "StrongPass123"})
	loginReq := httptest.NewRequest(http.MethodPost, "/auth/password/login", bytes.NewReader(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginRec := httptest.NewRecorder()
	router.ServeHTTP(loginRec, loginReq)
	assertStatus(t, loginRec.Code, http.StatusOK)

	loginCookie := loginRec.Result().Cookies()[0]
	badChangeBody, _ := json.Marshal(map[string]string{"current_password": "WrongPass123", "new_password": "NewStrongPass123"})
	badChangeReq := httptest.NewRequest(http.MethodPost, "/auth/password/change", bytes.NewReader(badChangeBody))
	badChangeReq.Header.Set("Content-Type", "application/json")
	badChangeReq.AddCookie(loginCookie)
	badChangeRec := httptest.NewRecorder()
	router.ServeHTTP(badChangeRec, badChangeReq)
	assertStatus(t, badChangeRec.Code, http.StatusUnauthorized)

	changeBody, _ := json.Marshal(map[string]string{"current_password": "StrongPass123", "new_password": "NewStrongPass123"})
	changeReq := httptest.NewRequest(http.MethodPost, "/auth/password/change", bytes.NewReader(changeBody))
	changeReq.Header.Set("Content-Type", "application/json")
	changeReq.AddCookie(loginCookie)
	changeRec := httptest.NewRecorder()
	router.ServeHTTP(changeRec, changeReq)
	assertStatus(t, changeRec.Code, http.StatusOK)

	logoutReq2 := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logoutReq2.AddCookie(loginCookie)
	logoutRec2 := httptest.NewRecorder()
	router.ServeHTTP(logoutRec2, logoutReq2)
	assertStatus(t, logoutRec2.Code, http.StatusOK)

	oldPasswordBody, _ := json.Marshal(map[string]string{"email": "pw-user@example.com", "password": "StrongPass123"})
	oldPasswordReq := httptest.NewRequest(http.MethodPost, "/auth/password/login", bytes.NewReader(oldPasswordBody))
	oldPasswordReq.Header.Set("Content-Type", "application/json")
	oldPasswordRec := httptest.NewRecorder()
	router.ServeHTTP(oldPasswordRec, oldPasswordReq)
	assertStatus(t, oldPasswordRec.Code, http.StatusUnauthorized)

	newPasswordBody, _ := json.Marshal(map[string]string{"email": "pw-user@example.com", "password": "NewStrongPass123"})
	newPasswordReq := httptest.NewRequest(http.MethodPost, "/auth/password/login", bytes.NewReader(newPasswordBody))
	newPasswordReq.Header.Set("Content-Type", "application/json")
	newPasswordRec := httptest.NewRecorder()
	router.ServeHTTP(newPasswordRec, newPasswordReq)
	assertStatus(t, newPasswordRec.Code, http.StatusOK)

	badBody, _ := json.Marshal(map[string]string{"email": "pw-user@example.com", "password": "WrongPass123"})
	badReq := httptest.NewRequest(http.MethodPost, "/auth/password/login", bytes.NewReader(badBody))
	badReq.Header.Set("Content-Type", "application/json")
	badRec := httptest.NewRecorder()
	router.ServeHTTP(badRec, badReq)
	assertStatus(t, badRec.Code, http.StatusUnauthorized)
}

func TestSupabaseGoogleLoginFlow(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_supabase_login")
	cfg.AuthEnableGoogleLogin = true
	cfg.SupabaseAnonKey = "anon-key"
	supabase := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth/v1/user" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer access-token" || r.Header.Get("apikey") != "anon-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"email": "google-user@example.com",
			"app_metadata": map[string]any{
				"provider": "google",
			},
		})
	}))
	defer supabase.Close()
	cfg.SupabaseURL = supabase.URL

	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	body, _ := json.Marshal(map[string]string{"access_token": "access-token"})
	req := httptest.NewRequest(http.MethodPost, "/auth/oauth/supabase/google", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatalf("expected auth cookie")
	}
	meReq := httptest.NewRequest(http.MethodGet, "/auth/me", nil)
	meReq.AddCookie(cookies[0])
	meRec := httptest.NewRecorder()
	router.ServeHTTP(meRec, meReq)
	assertStatus(t, meRec.Code, http.StatusOK)
}

func TestCodeLoginDisabled(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_code_login_disabled")
	cfg.AuthEnableCodeLogin = false
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	body, _ := json.Marshal(map[string]string{"email": "disabled@example.com"})
	req := httptest.NewRequest(http.MethodPost, "/auth/login/request-code", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusServiceUnavailable)
}

func TestPasswordSignupValidatesPasswordLength(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()
	body, _ := json.Marshal(map[string]string{"email": "short-pass@example.com", "password": "short"})
	req := httptest.NewRequest(http.MethodPost, "/auth/password/signup", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusBadRequest)
}

func TestEmployerPostingFlow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	code := requestLoginCode(t, router, "employer-owner@example.com")
	cookie := verifyLoginCode(t, router, "employer-owner@example.com", code)

	orgBody, _ := json.Marshal(map[string]any{"name": "Acme Hiring"})
	orgReq := httptest.NewRequest(http.MethodPost, "/employer/organizations", bytes.NewReader(orgBody))
	orgReq.Header.Set("Content-Type", "application/json")
	orgReq.AddCookie(cookie)
	orgRec := httptest.NewRecorder()
	router.ServeHTTP(orgRec, orgReq)
	assertStatus(t, orgRec.Code, http.StatusOK)
	var orgPayload map[string]any
	decodeBody(t, orgRec.Body.Bytes(), &orgPayload)
	orgID := int(orgPayload["id"].(float64))

	jobBody, _ := json.Marshal(map[string]any{
		"organization_id": orgID,
		"title":           "Senior Backend Engineer",
		"description":     "Build internal platform systems",
		"employment_type": "full-time",
		"location_type":   "remote",
		"apply_url":       "https://example.com/jobs/backend",
	})
	jobReq := httptest.NewRequest(http.MethodPost, "/employer/jobs", bytes.NewReader(jobBody))
	jobReq.Header.Set("Content-Type", "application/json")
	jobReq.AddCookie(cookie)
	jobRec := httptest.NewRecorder()
	router.ServeHTTP(jobRec, jobReq)
	assertStatus(t, jobRec.Code, http.StatusOK)
	var jobPayload map[string]any
	decodeBody(t, jobRec.Body.Bytes(), &jobPayload)
	jobID := int(jobPayload["id"].(float64))
	if jobPayload["posting_fee_status"].(string) != "unpaid" {
		t.Fatalf("expected unpaid posting fee status, got %#v", jobPayload["posting_fee_status"])
	}

	payReq := httptest.NewRequest(http.MethodPost, "/employer/jobs/"+strconv.Itoa(jobID)+"/pay", nil)
	payReq.AddCookie(cookie)
	payRec := httptest.NewRecorder()
	router.ServeHTTP(payRec, payReq)
	assertStatus(t, payRec.Code, http.StatusOK)

	publishReq := httptest.NewRequest(http.MethodPost, "/employer/jobs/"+strconv.Itoa(jobID)+"/publish", nil)
	publishReq.AddCookie(cookie)
	publishRec := httptest.NewRecorder()
	router.ServeHTTP(publishRec, publishReq)
	assertStatus(t, publishRec.Code, http.StatusOK)

	getReq := httptest.NewRequest(http.MethodGet, "/employer/jobs/"+strconv.Itoa(jobID), nil)
	getReq.AddCookie(cookie)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	assertStatus(t, getRec.Code, http.StatusOK)
	var getPayload map[string]any
	decodeBody(t, getRec.Body.Bytes(), &getPayload)
	if getPayload["status"].(string) != "published" || getPayload["posting_fee_status"].(string) != "paid" {
		t.Fatalf("unexpected employer job state %#v", getPayload)
	}
}

func TestJobActionsFlow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 7001, "https://example.com/action-1", "Austin", "Texas", 100, 130, true, time.Now().UTC())
	code := requestLoginCode(t, router, "actions@example.com")
	cookie := verifyLoginCode(t, router, "actions@example.com", code)

	updateBody, _ := json.Marshal(map[string]any{"is_saved": true, "is_hidden": false, "is_applied": true})
	updateReq := httptest.NewRequest(http.MethodPut, "/job-actions/1", bytes.NewReader(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq.AddCookie(cookie)
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	assertStatus(t, updateRec.Code, http.StatusOK)

	getReq := httptest.NewRequest(http.MethodGet, "/job-actions?job_ids=1,2", nil)
	getReq.AddCookie(cookie)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	assertStatus(t, getRec.Code, http.StatusOK)

	var payload map[string]any
	decodeBody(t, getRec.Body.Bytes(), &payload)
	items := payload["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one action item, got %#v", payload)
	}
	first := items[0].(map[string]any)
	if first["job_id"].(float64) != 1 || first["is_saved"] != true || first["is_hidden"] != false {
		t.Fatalf("unexpected action payload %#v", first)
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/job-actions/summary", nil)
	summaryReq.AddCookie(cookie)
	summaryRec := httptest.NewRecorder()
	router.ServeHTTP(summaryRec, summaryReq)
	assertStatus(t, summaryRec.Code, http.StatusOK)
	var summaryPayload map[string]any
	decodeBody(t, summaryRec.Body.Bytes(), &summaryPayload)
	if summaryPayload["applied_count"].(float64) != 1 || summaryPayload["saved_count"].(float64) != 1 || summaryPayload["hidden_count"].(float64) != 0 {
		t.Fatalf("unexpected summary payload %#v", summaryPayload)
	}

	hideBody, _ := json.Marshal(map[string]any{"is_hidden": true})
	hideReq := httptest.NewRequest(http.MethodPut, "/job-actions/1", bytes.NewReader(hideBody))
	hideReq.Header.Set("Content-Type", "application/json")
	hideReq.AddCookie(cookie)
	hideRec := httptest.NewRecorder()
	router.ServeHTTP(hideRec, hideReq)
	assertStatus(t, hideRec.Code, http.StatusOK)

	jobsDefaultReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	jobsDefaultReq.AddCookie(cookie)
	jobsDefaultRec := httptest.NewRecorder()
	router.ServeHTTP(jobsDefaultRec, jobsDefaultReq)
	assertStatus(t, jobsDefaultRec.Code, http.StatusOK)
	var jobsDefault map[string]any
	decodeBody(t, jobsDefaultRec.Body.Bytes(), &jobsDefault)
	if jobsDefault["total"].(float64) != 0 {
		t.Fatalf("expected hidden jobs excluded by default, got %#v", jobsDefault)
	}

	jobsHiddenReq := httptest.NewRequest(http.MethodGet, "/jobs?user_job_action=hidden", nil)
	jobsHiddenReq.AddCookie(cookie)
	jobsHiddenRec := httptest.NewRecorder()
	router.ServeHTTP(jobsHiddenRec, jobsHiddenReq)
	assertStatus(t, jobsHiddenRec.Code, http.StatusOK)
	var jobsHidden map[string]any
	decodeBody(t, jobsHiddenRec.Body.Bytes(), &jobsHidden)
	if jobsHidden["total"].(float64) != 1 {
		t.Fatalf("expected hidden-filtered listing to include hidden job, got %#v", jobsHidden)
	}
}

func testRouter(t *testing.T) (*gin.Engine, *database.DB) {
	t.Helper()
	cfg := config.Load()
	cfg.DatabaseURL = testDatabaseURL(t, "test_page_extract")
	cfg.AuthDebugReturnCode = true
	cfg.AuthEnableCodeLogin = true
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return NewRouter(cfg, db), db
}

func testDatabaseURL(t *testing.T, name string) string {
	t.Helper()
	baseURL := database.TestDatabaseBaseURL()
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL-backed tests")
	}
	schema := "test_" + strings.ReplaceAll(strings.ToLower(name), "-", "_") + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	adminDB, err := sql.Open("pgx", baseURL)
	if err != nil {
		t.Fatalf("open test postgres connection: %v", err)
	}
	defer adminDB.Close()
	if _, err := adminDB.ExecContext(context.Background(), `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`); err != nil {
		t.Fatalf("create test schema %q: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanupDB, openErr := sql.Open("pgx", baseURL)
		if openErr != nil {
			t.Logf("open cleanup postgres connection failed for schema %q: %v", schema, openErr)
			return
		}
		defer cleanupDB.Close()
		if _, err := cleanupDB.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`); err != nil {
			t.Logf("drop test schema %q failed: %v", schema, err)
		}
	})
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func insertJob(t *testing.T, db *database.DB, rawID int, rawURL, city, state string, salaryMin, salaryMax float64, isSenior bool, createdAt time.Time) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`, rawID, rawURL, createdAt.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(
		context.Background(),
		`INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, role_title, location_countries, location_city, location_us_states, salary_min_usd, salary_max_usd, is_senior, created_at_source, url) VALUES (?, 'Software Engineer', 'Software Engineer', '["United States"]', ?, ?, ?, ?, ?, ?, ?)`,
		rawID,
		city,
		`["`+state+`"]`,
		salaryMin,
		salaryMax,
		boolToInt(isSenior),
		createdAt.Format(time.RFC3339Nano),
		"https://jobs.example.com/"+strconv.Itoa(rawID),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func insertJobWithSalaryType(t *testing.T, db *database.DB, rawID int, category string, salaryMinUSD float64, salaryType string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`, rawID, "https://example.com/"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if salaryType == "yearly" {
		_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, salary_min_usd, salary_type, url) VALUES (?, ?, ?, ?, ?)`, rawID, category, salaryMinUSD, salaryType, "https://jobs.example.com/"+strconv.Itoa(rawID))
	} else {
		_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, salary_min, salary_type, url) VALUES (?, ?, ?, ?, ?)`, rawID, category, salaryMinUSD, salaryType, "https://jobs.example.com/"+strconv.Itoa(rawID))
	}
	if err != nil {
		t.Fatal(err)
	}
}

func insertCompany(t *testing.T, db *database.DB, name string) int64 {
	t.Helper()
	var id int64
	if err := db.SQL.QueryRowContext(context.Background(), `INSERT INTO parsed_companies (name, slug, tagline, profile_pic_url, home_page_url, linkedin_url, employee_range, founded_year, sponsors_h1b) VALUES (?, 'example-co', 'tagline', 'https://img', 'https://home', 'https://linkedin', '11-50', '2020', true) RETURNING id`, name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func insertRichJob(t *testing.T, db *database.DB, companyID int64) int {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (999, 'https://example.com/job-detail', ?, true, false, true, 0, '{}')`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	err = db.SQL.QueryRowContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, company_id, categorized_job_title, role_title, role_description, role_requirements, location_countries, location_city, salary_min_usd, salary_max_usd, salary_type, employment_type, education_requirements_credential_category, experience_requirements_months, experience_in_place_of_education, required_languages, tech_stack, benefits, url) VALUES (999, ?, 'Software Engineer', 'Staff Backend Engineer', 'Build distributed systems.', 'Python
FastAPI', '["United States"]', 'Austin', 150, 210, 'hourly', 'full-time', 'bachelor', 24, true, '["English"]', '["Go","SQL"]', 'Great benefits', 'https://jobs.example.com/detail') RETURNING id`, companyID).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return int(id)
}

func insertCSVJob(t *testing.T, db *database.DB, rawID int, title, region string, isMid bool, salaryMin float64, salaryType string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`, rawID+2000, "https://example.com/csv-"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if salaryType == "yearly" {
		_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, role_title, location_countries, is_mid_level, is_senior, salary_min_usd, salary_type, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, rawID+2000, title, title, `["`+region+`"]`, boolToInt(isMid), boolToInt(!isMid), salaryMin, salaryType, "https://jobs.example.com/csv-"+strconv.Itoa(rawID))
	} else {
		_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, role_title, location_countries, is_mid_level, is_senior, salary_min, salary_type, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, rawID+2000, title, title, `["`+region+`"]`, boolToInt(isMid), boolToInt(!isMid), salaryMin, salaryType, "https://jobs.example.com/csv-"+strconv.Itoa(rawID))
	}
	if err != nil {
		t.Fatal(err)
	}
}

func insertJobWithFunction(t *testing.T, db *database.DB, rawID int, categoryTitle, categoryFunction, roleTitle string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`, rawID+3000, "https://example.com/function-"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, categorized_job_function, role_title, created_at_source, url) VALUES (?, ?, ?, ?, ?, ?)`, rawID+3000, categoryTitle, categoryFunction, roleTitle, time.Now().UTC().Format(time.RFC3339Nano), "https://jobs.example.com/function-"+strconv.Itoa(rawID))
	if err != nil {
		t.Fatal(err)
	}
}

func insertJobWithTechStack(t *testing.T, db *database.DB, rawID int, roleTitle string, techStack []string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`, rawID+4000, "https://example.com/tech-"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	techStackJSON, err := json.Marshal(techStack)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, role_title, tech_stack, created_at_source, url) VALUES (?, 'Software Engineer', ?, ?, ?, ?)`, rawID+4000, roleTitle, string(techStackJSON), time.Now().UTC().Format(time.RFC3339Nano), "https://jobs.example.com/tech-"+strconv.Itoa(rawID))
	if err != nil {
		t.Fatal(err)
	}
}

func insertDatedJob(t *testing.T, db *database.DB, rawID int, createdAt time.Time) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`, rawID+5000, "https://example.com/date-"+strconv.Itoa(rawID), createdAt.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, role_title, created_at_source, url) VALUES (?, 'Software Engineer', 'Software Engineer', ?, ?)`, rawID+5000, createdAt.Format(time.RFC3339Nano), "https://jobs.example.com/date-"+strconv.Itoa(rawID))
	if err != nil {
		t.Fatal(err)
	}
}

func insertEmploymentTypeJob(t *testing.T, db *database.DB, rawID int, employmentType string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`, rawID+6000, "https://example.com/employment-"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, employment_type, created_at_source, url) VALUES (?, 'Software Engineer', ?, ?, ?)`, rawID+6000, employmentType, time.Now().UTC().Format(time.RFC3339Nano), "https://jobs.example.com/employment-"+strconv.Itoa(rawID))
	if err != nil {
		t.Fatal(err)
	}
}

func insertJobWithCreatedAt(t *testing.T, db *database.DB, rawID int, category, location string, createdAt time.Time) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, true, 0, '{}')`,
		rawID+7000, "https://example.com/top-"+strconv.Itoa(rawID), createdAt.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, role_title, location_countries, created_at_source, url) VALUES (?, ?, ?, ?, ?, ?)`,
		rawID+7000, category, category, `["`+location+`"]`, createdAt.Format(time.RFC3339Nano), "https://jobs.example.com/top-"+strconv.Itoa(rawID))
	if err != nil {
		t.Fatal(err)
	}
}

type loginCodePayload struct {
	DebugCode string
	DebugLink string
}

func requestLoginCode(t *testing.T, router http.Handler, email string) string {
	t.Helper()
	return requestLoginCodePayload(t, router, email).DebugCode
}

func requestLoginCodePayload(t *testing.T, router http.Handler, email string) loginCodePayload {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"email": email})
	req := httptest.NewRequest(http.MethodPost, "/auth/login/request-code", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	var payload map[string]any
	decodeBody(t, rec.Body.Bytes(), &payload)
	result := loginCodePayload{}
	if value, ok := payload["debug_code"].(string); ok {
		result.DebugCode = value
	}
	if value, ok := payload["debug_link"].(string); ok {
		result.DebugLink = value
	}
	return result
}

func verifyLoginCode(t *testing.T, router http.Handler, email, code string) *http.Cookie {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"email": email, "code": code})
	req := httptest.NewRequest(http.MethodPost, "/auth/login/verify-code", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	return rec.Result().Cookies()[0]
}

func verifyLoginLink(t *testing.T, router http.Handler, token string) *http.Cookie {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"token": token})
	req := httptest.NewRequest(http.MethodPost, "/auth/login/verify-link", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
	return rec.Result().Cookies()[0]
}

func magicTokenFromLink(t *testing.T, link string) string {
	t.Helper()
	parts := strings.SplitN(link, "token=", 2)
	if len(parts) != 2 || parts[1] == "" {
		t.Fatalf("missing token in debug link %q", link)
	}
	return parts[1]
}

func decodeBody(t *testing.T, body []byte, dest any) {
	t.Helper()
	if err := json.Unmarshal(body, dest); err != nil {
		t.Fatal(err)
	}
}

func assertStatus(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("status=%d want=%d", got, want)
	}
}

func boolToInt(value bool) bool {
	return value
}

func overlaps(left, right []int) bool {
	seen := map[int]struct{}{}
	for _, item := range left {
		seen[item] = struct{}{}
	}
	for _, item := range right {
		if _, ok := seen[item]; ok {
			return true
		}
	}
	return false
}
