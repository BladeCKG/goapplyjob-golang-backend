package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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
}

func TestAuthAndJobsFlow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	insertJob(t, db, 1, "https://example.com/a", "Austin", "Texas", 120, 150, true, time.Date(2026, 2, 11, 0, 0, 0, 0, time.UTC))
	insertJob(t, db, 2, "https://example.com/b", "Seattle", "Washington", 80, 100, false, time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC))

	code := requestLoginCode(t, router, "user@example.com")
	cookie := verifyLoginCode(t, router, "user@example.com", code)

	req := httptest.NewRequest(http.MethodGet, "/jobs?sort_criteria=salary&job_title=Engineer&region=Texas&min_salary=100&seniority=senior&page=1&per_page=10", nil)
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

func TestPricingFlow(t *testing.T) {
	router, db := testRouter(t)
	defer db.Close()

	req := httptest.NewRequest(http.MethodGet, "/pricing/plans", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	code := requestLoginCode(t, router, "price-user@example.com")
	cookie := verifyLoginCode(t, router, "price-user@example.com", code)

	body, _ := json.Marshal(map[string]any{
		"plan_code":      "monthly",
		"provider":       "crypto",
		"payment_method": "crypto",
	})
	subscribeReq := httptest.NewRequest(http.MethodPost, "/pricing/subscribe", bytes.NewReader(body))
	subscribeReq.Header.Set("Content-Type", "application/json")
	subscribeReq.AddCookie(cookie)
	subscribeRec := httptest.NewRecorder()
	router.ServeHTTP(subscribeRec, subscribeReq)
	assertStatus(t, subscribeRec.Code, http.StatusOK)

	var payload map[string]any
	decodeBody(t, subscribeRec.Body.Bytes(), &payload)
	paymentID := int(payload["payment_id"].(float64))

	confirmReq := httptest.NewRequest(http.MethodPost, "/pricing/payments/"+strconv.Itoa(paymentID)+"/confirm", nil)
	confirmReq.AddCookie(cookie)
	confirmRec := httptest.NewRecorder()
	router.ServeHTTP(confirmRec, confirmReq)
	assertStatus(t, confirmRec.Code, http.StatusOK)
}

func testRouter(t *testing.T) (*gin.Engine, *database.DB) {
	t.Helper()

	cfg := config.Load()
	cfg.DatabaseURL = "file:test_page_extract_init?mode=memory&cache=shared"
	cfg.AuthDebugReturnCode = true

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
	_, err = db.SQL.ExecContext(context.Background(), `INSERT INTO parsed_jobs (raw_us_job_id, categorized_job_title, location, location_city, location_us_states, salary_min_usd, salary_max_usd, is_senior, created_at_source, url) VALUES (?, 'Software Engineer', 'United States', ?, ?, ?, ?, ?, ?, ?)`,
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

func requestLoginCode(t *testing.T, router http.Handler, email string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"email": email})
	req := httptest.NewRequest(http.MethodPost, "/auth/login/request-code", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	var payload map[string]any
	decodeBody(t, rec.Body.Bytes(), &payload)
	return payload["debug_code"].(string)
}

func verifyLoginCode(t *testing.T, router http.Handler, email, code string) *http.Cookie {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"email": email, "code": code})
	req := httptest.NewRequest(http.MethodPost, "/auth/login/verify-code", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)

	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("expected auth cookie")
	}
	return cookies[0]
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
