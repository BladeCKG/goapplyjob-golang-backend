package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"testing"
)

func TestDebugInferRawJob(t *testing.T) {
	debugInferRawJob(t, 413585)
}

func debugInferRawJob(t *testing.T, rawJobID int64) {
	t.Helper()

	_ = config.LoadDotEnvIfExists("c:/Users/aaa/Documents/dev/goapplyjob/goapplyjob-golang-backend/.env")
	dbURL := config.Getenv("DATABASE_URL", "")
	if dbURL == "" {
		t.Fatal("DATABASE_URL is empty")
	}

	db, err := database.Open(dbURL)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	defer db.Close()

	var rawJSON sql.NullString
	if err := db.SQL.QueryRowContext(
		context.Background(),
		`SELECT raw_json
		   FROM raw_us_jobs
		  WHERE id = ?`,
		rawJobID,
	).Scan(&rawJSON); err != nil {
		t.Fatalf("load raw job: %v", err)
	}
	if !rawJSON.Valid || rawJSON.String == "" {
		t.Fatal("raw job payload is empty")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(rawJSON.String), &payload); err != nil {
		t.Fatalf("parse raw payload: %v", err)
	}

	roleTitle := stringValue(payload["roleTitle"])
	techStack := normalizeTechStack(payload["techStack"])

	t.Logf("raw_job_id=%d role_title=%q tech_stack_len=%d", rawJobID, roleTitle, len(techStack))
	svc := New(Config{}, db)
	title, function, err := svc.findSimilarRemoteRoekctshipCategories(context.Background(), roleTitle, techStack)
	if err != nil {
		t.Fatalf("infer categories: %v", err)
	}
	t.Logf("inferred title=%q function=%q", title, function)
}
