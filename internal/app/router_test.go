package app

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"

	"github.com/gin-gonic/gin"
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
	cfg.DatabaseURL = "file:test_preview?mode=memory&cache=shared"
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
	cfg.DatabaseURL = "file:test_jobs_sitemap_not_preview_limited?mode=memory&cache=shared"
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
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (3001, 'https://example.com/old-updated', ?, 1, 0, 1, 0, '{}')`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (3002, 'https://example.com/new-updated', ?, 1, 0, 1, 0, '{}')`, time.Now().UTC().Format(time.RFC3339Nano))
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

func TestJobsFilterOptionsIncludesHierarchyMetadata(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJobWithFunction(t, db, 201, "Data Engineer", "Engineering", "Data Engineer")
	insertJobWithFunction(t, db, 202, "Software Engineer", "Engineering", "Software Engineer")
	if _, err := db.SQL.ExecContext(context.Background(), `UPDATE parsed_jobs SET location = ?, location_city = ?, location_us_states = ? WHERE raw_us_job_id = ?`, "United States", "Austin", `["Texas"]`, 3201); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(context.Background(), `UPDATE parsed_jobs SET location = ?, location_city = ?, location_us_states = ? WHERE raw_us_job_id = ?`, "Canada", "Toronto", `["Ontario"]`, 3202); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs/filter-options", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	jobCategories := body["job_categories"].([]any)
	if len(jobCategories) < 3 {
		t.Fatalf("expected category hierarchy options, got %#v", body)
	}
	jobCategoryQueryValues := body["job_category_query_values"].(map[string]any)
	jobCategoryParents := body["job_category_parents"].(map[string]any)
	if jobCategoryQueryValues["Engineering"] != "Engineering" {
		t.Fatalf("missing Engineering query value %#v", body)
	}
	if len(jobCategoryParents["Data Engineer"].([]any)) == 0 || jobCategoryParents["Data Engineer"].([]any)[0] != "Engineering" {
		t.Fatalf("missing Data Engineer parent metadata %#v", body)
	}

	locations := body["locations"].([]any)
	if len(locations) < 6 {
		t.Fatalf("expected expanded location options %#v", body)
	}
	locationQueryValues := body["location_query_values"].(map[string]any)
	locationParents := body["location_parents"].(map[string]any)
	if locationQueryValues["Austin"] != "Austin" {
		t.Fatalf("missing Austin query value %#v", body)
	}
	austinParents := locationParents["Austin"].([]any)
	if len(austinParents) < 2 || austinParents[0] != "Texas" || austinParents[1] != "United States" {
		t.Fatalf("unexpected Austin parent metadata %#v", body)
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
	if body["items"].([]any)[0].(map[string]any)["location_city"] != "Austin" {
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
		_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`, rawID, "https://example.com/pagination/"+strconv.Itoa(idx), createdAt.Format(time.RFC3339Nano))
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

	insertJobWithFunction(t, db, 61, "Platform Engineering", "backend", "Distributed Systems Lead")
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
		t.Fatalf("expected exact and partial role title matches, got %#v", body)
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

	req := httptest.NewRequest(http.MethodGet, "/jobs?tech_stack=go", nil)
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
	if len(optionsBody["post_date_options"].([]any)) != 6 {
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

func TestPricingProvidersEndpointReportsEnabledMethods(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	_ = os.Setenv("STRIPE_SECRET_KEY", "sk_test_enabled")
	t.Cleanup(func() { _ = os.Unsetenv("STRIPE_SECRET_KEY") })

	req := httptest.NewRequest(http.MethodGet, "/pricing/providers", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var body map[string]any
	decodeBody(t, rec.Body.Bytes(), &body)
	items := body["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("unexpected providers payload %#v", body)
	}
	var stripe, crypto map[string]any
	for _, item := range items {
		row := item.(map[string]any)
		if row["provider"] == "stripe" {
			stripe = row
		}
		if row["provider"] == "crypto" {
			crypto = row
		}
	}
	if stripe["enabled"] != true || len(stripe["payment_methods"].([]any)) == 0 {
		t.Fatalf("unexpected stripe provider %#v", stripe)
	}
	if crypto["enabled"] != true || len(crypto["payment_methods"].([]any)) == 0 {
		t.Fatalf("unexpected crypto provider %#v", crypto)
	}
}

func TestDefaultFreeSubscriptionAndUpgradePreview(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = "file:test_upgrade_preview?mode=memory&cache=shared"
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

	_, err = db.SQL.ExecContext(context.Background(), `UPDATE user_subscriptions SET ends_at = ?, is_active = 0`, time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano))
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

	_, err := db.SQL.ExecContext(context.Background(), `UPDATE user_subscriptions SET ends_at = ?, is_active = 1`, time.Now().UTC().Add(-2*time.Hour).Format(time.RFC3339Nano))
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

func TestCryptoWebhookRequiresSignatureAndActivatesSubscription(t *testing.T) {
	cfg := config.Load()
	cfg.DatabaseURL = "file:test_crypto_webhook_paid?mode=memory&cache=shared"
	cfg.AuthDebugReturnCode = true
	cfg.AuthEnableCodeLogin = true
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
	cfg.DatabaseURL = "file:test_oxapay_webhook_paid?mode=memory&cache=shared"
	cfg.CryptoPaymentProvider = "oxapay"
	cfg.OxaPayMerchantAPIKey = "secret-token"
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	router := NewRouter(cfg, db)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := db.SQL.ExecContext(context.Background(), `INSERT INTO auth_users (email, created_at) VALUES (?, ?)`, "oxapay-user@example.com", now)
	if err != nil {
		t.Fatal(err)
	}
	userID, _ := result.LastInsertId()
	result, err = db.SQL.ExecContext(context.Background(), `INSERT INTO pricing_plans (code, name, billing_cycle, duration_days, price_usd, is_active, created_at) VALUES (?, ?, ?, ?, ?, 1, ?)`, "weekly", "Weekly", "weekly", 7, 3, now)
	if err != nil {
		t.Fatal(err)
	}
	planID, _ := result.LastInsertId()
	result, err = db.SQL.ExecContext(context.Background(), `INSERT INTO pricing_payments (user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, created_at) VALUES (?, ?, 'crypto', 'crypto', 'USD', 300, 'pending', ?, ?)`, userID, planID, "140013835", now)
	if err != nil {
		t.Fatal(err)
	}
	paymentID, _ := result.LastInsertId()

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
	cfg.DatabaseURL = "file:test_currency_filtering?mode=memory&cache=shared"
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
	cfg.DatabaseURL = "file:test_supabase_login?mode=memory&cache=shared"
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
	cfg.DatabaseURL = "file:test_code_login_disabled?mode=memory&cache=shared"
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
	cfg.DatabaseURL = "file:test_page_extract?mode=memory&cache=shared"
	cfg.AuthDebugReturnCode = true
	cfg.AuthEnableCodeLogin = true
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return NewRouter(cfg, db), db
}

func insertJob(t *testing.T, db *database.DB, rawID int, rawURL, city, state string, salaryMin, salaryMax float64, isSenior bool, createdAt time.Time) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`, rawID, rawURL, createdAt.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, location, location_city, location_us_states, salary_min_usd, salary_max_usd, is_senior, created_at_source, url) VALUES (?, 'Software Engineer', 'United States', ?, ?, ?, ?, ?, ?, ?)`, rawID, city, `["`+state+`"]`, salaryMin, salaryMax, boolToInt(isSenior), createdAt.Format(time.RFC3339Nano), "https://jobs.example.com/"+strconv.Itoa(rawID))
	if err != nil {
		t.Fatal(err)
	}
}

func insertJobWithSalaryType(t *testing.T, db *database.DB, rawID int, category string, salaryMinUSD float64, salaryType string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`, rawID, "https://example.com/"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
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
	result, err := db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_companies (name, slug, tagline, profile_pic_url, home_page_url, linkedin_url, employee_range, founded_year, sponsors_h1b) VALUES (?, 'example-co', 'tagline', 'https://img', 'https://home', 'https://linkedin', '11-50', '2020', 1)`, name)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := result.LastInsertId()
	return id
}

func insertRichJob(t *testing.T, db *database.DB, companyID int64) int {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (999, 'https://example.com/job-detail', ?, 1, 0, 1, 0, '{}')`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, company_id, categorized_job_title, role_title, role_description, role_requirements, location, location_city, salary_min_usd, salary_max_usd, salary_type, employment_type, education_requirements_credential_category, experience_requirements_months, experience_in_place_of_education, required_languages, tech_stack, benefits, url) VALUES (999, ?, 'Software Engineer', 'Staff Backend Engineer', 'Build distributed systems.', 'Python
FastAPI', 'United States', 'Austin', 150, 210, 'hourly', 'full-time', 'bachelor', 24, 1, '["English"]', '["Go","SQL"]', 'Great benefits', 'https://jobs.example.com/detail')`, companyID)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := result.LastInsertId()
	return int(id)
}

func insertCSVJob(t *testing.T, db *database.DB, rawID int, title, region string, isMid bool, salaryMin float64, salaryType string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`, rawID+2000, "https://example.com/csv-"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if salaryType == "yearly" {
		_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, location, is_mid_level, is_senior, salary_min_usd, salary_type, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, rawID+2000, title, region, boolToInt(isMid), boolToInt(!isMid), salaryMin, salaryType, "https://jobs.example.com/csv-"+strconv.Itoa(rawID))
	} else {
		_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, location, is_mid_level, is_senior, salary_min, salary_type, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, rawID+2000, title, region, boolToInt(isMid), boolToInt(!isMid), salaryMin, salaryType, "https://jobs.example.com/csv-"+strconv.Itoa(rawID))
	}
	if err != nil {
		t.Fatal(err)
	}
}

func insertJobWithFunction(t *testing.T, db *database.DB, rawID int, categoryTitle, categoryFunction, roleTitle string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`, rawID+3000, "https://example.com/function-"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
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
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`, rawID+4000, "https://example.com/tech-"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
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
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`, rawID+5000, "https://example.com/date-"+strconv.Itoa(rawID), createdAt.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, created_at_source, url) VALUES (?, 'Software Engineer', ?, ?)`, rawID+5000, createdAt.Format(time.RFC3339Nano), "https://jobs.example.com/date-"+strconv.Itoa(rawID))
	if err != nil {
		t.Fatal(err)
	}
}

func insertEmploymentTypeJob(t *testing.T, db *database.DB, rawID int, employmentType string) {
	t.Helper()
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`, rawID+6000, "https://example.com/employment-"+strconv.Itoa(rawID), time.Now().UTC().Format(time.RFC3339Nano))
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
	_, err := db.SQL.ExecContext(context.Background(), `INSERT INTO raw_us_jobs (id, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, 1, 0, 1, 0, '{}')`,
		rawID+7000, "https://example.com/top-"+strconv.Itoa(rawID), createdAt.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, location, created_at_source, url) VALUES (?, ?, ?, ?, ?)`,
		rawID+7000, category, location, createdAt.Format(time.RFC3339Nano), "https://jobs.example.com/top-"+strconv.Itoa(rawID))
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

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
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
