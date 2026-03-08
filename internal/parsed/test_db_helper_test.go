package parsed

import (
	"context"
	"database/sql"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"goapplyjob-golang-backend/internal/database"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func requirePostgresTestDB(t *testing.T) {
	t.Helper()
	if !database.HasTestDatabaseURL() {
		t.Skip("TEST_DATABASE_URL is required for DB-backed tests")
	}
}

func testDatabaseURL(t *testing.T, schemaName string) string {
	t.Helper()
	requirePostgresTestDB(t)
	baseURL := database.TestDatabaseBaseURL()
	adminDB, err := sql.Open("pgx", baseURL)
	if err != nil {
		t.Fatalf("open test postgres connection: %v", err)
	}
	defer adminDB.Close()
	schema := "test_" + strings.ReplaceAll(strings.ToLower(schemaName), "-", "_") + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := adminDB.ExecContext(context.Background(), `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`); err != nil {
		t.Fatalf("create test schema %q: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanupDB, openErr := sql.Open("pgx", baseURL)
		if openErr != nil {
			return
		}
		defer cleanupDB.Close()
		_, _ = cleanupDB.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
	})
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	q := parsedURL.Query()
	q.Set("search_path", schema)
	parsedURL.RawQuery = q.Encode()
	return parsedURL.String()
}
