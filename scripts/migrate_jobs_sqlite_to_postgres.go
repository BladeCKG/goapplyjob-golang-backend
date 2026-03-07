package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

const (
	defaultSourceSQLiteURL = "file:page_extract.db?_foreign_keys=on"
	defaultBatchSize       = 1000
)

var tableOrder = []string{"parsed_companies", "raw_us_jobs", "parsed_jobs"}

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	sourceURL := flag.String("source-sqlite-url", getenv("SOURCE_SQLITE_URL", defaultSourceSQLiteURL), "Source SQLite URL")
	targetURL := flag.String("target-database-url", getenv("TARGET_DATABASE_URL", getenv("DATABASE_URL", "")), "Target PostgreSQL URL")
	batchSize := flag.Int("batch-size", getenvInt("MIGRATE_BATCH_SIZE", defaultBatchSize), "Rows per batch")
	monthsBack := flag.Int("months-back", getenvInt("MIGRATE_MONTHS_BACK", 6), "Only migrate jobs within last N months")
	flag.Parse()

	if strings.TrimSpace(*targetURL) == "" {
		log.Fatal("missing target PostgreSQL URL")
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(*targetURL)), "postgres") {
		log.Fatalf("target database must be PostgreSQL, got %q", *targetURL)
	}

	sourceDB, err := sql.Open("sqlite", *sourceURL)
	if err != nil {
		log.Fatal(err)
	}
	defer sourceDB.Close()

	targetDB, err := sql.Open("pgx", normalizePostgresURL(*targetURL))
	if err != nil {
		log.Fatal(err)
	}
	defer targetDB.Close()

	cutoff := time.Now().UTC().AddDate(0, -*monthsBack, 0)
	log.Printf("[filter] post_date >= %s", cutoff.Format(time.RFC3339Nano))

	ctx := context.Background()
	total := 0
	for _, tableName := range tableOrder {
		copied, err := copyTable(ctx, sourceDB, targetDB, tableName, *batchSize, cutoff)
		if err != nil {
			log.Fatalf("copy %s: %v", tableName, err)
		}
		total += copied
		log.Printf("[done] %s: %d rows", tableName, copied)
	}
	log.Printf("[complete] migrated rows total=%d", total)
}

func copyTable(ctx context.Context, sourceDB, targetDB *sql.DB, tableName string, batchSize int, cutoff time.Time) (int, error) {
	sharedColumns, err := sharedTableColumns(ctx, sourceDB, targetDB, tableName)
	if err != nil {
		return 0, err
	}
	if len(sharedColumns) == 0 {
		return 0, nil
	}
	if batchSize < 1 {
		batchSize = 1
	}

	whereSQL := ""
	args := []any{}
	switch tableName {
	case "raw_us_jobs":
		whereSQL = "WHERE post_date >= ?"
		args = append(args, cutoff.Format(time.RFC3339Nano))
	case "parsed_jobs":
		whereSQL = "WHERE raw_us_job_id IN (SELECT id FROM raw_us_jobs WHERE post_date >= ?)"
		args = append(args, cutoff.Format(time.RFC3339Nano))
	case "parsed_companies":
		whereSQL = "WHERE id IN (SELECT DISTINCT company_id FROM parsed_jobs WHERE raw_us_job_id IN (SELECT id FROM raw_us_jobs WHERE post_date >= ?) AND company_id IS NOT NULL)"
		args = append(args, cutoff.Format(time.RFC3339Nano))
	}

	query := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY id ASC", strings.Join(sharedColumns, ", "), tableName, whereSQL)
	rows, err := sourceDB.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	total := 0
	buffer := make([]map[string]any, 0, batchSize)
	for rows.Next() {
		values := make([]any, len(sharedColumns))
		valuePtrs := make([]any, len(sharedColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return total, err
		}
		row := map[string]any{}
		for i, col := range sharedColumns {
			row[col] = normalizeSQLiteValue(values[i])
		}
		buffer = append(buffer, row)
		if len(buffer) >= batchSize {
			if err := upsertBatch(ctx, targetDB, tableName, sharedColumns, buffer); err != nil {
				return total, err
			}
			total += len(buffer)
			buffer = buffer[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return total, err
	}
	if len(buffer) > 0 {
		if err := upsertBatch(ctx, targetDB, tableName, sharedColumns, buffer); err != nil {
			return total, err
		}
		total += len(buffer)
	}
	return total, nil
}

func upsertBatch(ctx context.Context, db *sql.DB, tableName string, columns []string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	valueGroups := make([]string, 0, len(rows))
	args := make([]any, 0, len(rows)*len(columns))
	argPos := 1
	for _, row := range rows {
		placeholders := make([]string, 0, len(columns))
		for _, col := range columns {
			placeholders = append(placeholders, fmt.Sprintf("$%d", argPos))
			args = append(args, row[col])
			argPos++
		}
		valueGroups = append(valueGroups, "("+strings.Join(placeholders, ", ")+")")
	}
	updateCols := make([]string, 0, len(columns))
	for _, col := range columns {
		if col == "id" {
			continue
		}
		updateCols = append(updateCols, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
	}
	sqlText := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT (id) DO UPDATE SET %s",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(valueGroups, ", "),
		strings.Join(updateCols, ", "),
	)
	_, err := db.ExecContext(ctx, sqlText, args...)
	return err
}

func sharedTableColumns(ctx context.Context, sourceDB, targetDB *sql.DB, tableName string) ([]string, error) {
	sourceCols, err := sqliteColumns(ctx, sourceDB, tableName)
	if err != nil {
		return nil, err
	}
	targetCols, err := postgresColumns(ctx, targetDB, tableName)
	if err != nil {
		return nil, err
	}
	targetSet := map[string]struct{}{}
	for _, col := range targetCols {
		targetSet[col] = struct{}{}
	}
	shared := make([]string, 0, len(sourceCols))
	for _, col := range sourceCols {
		if _, ok := targetSet[col]; ok {
			shared = append(shared, col)
		}
	}
	sort.SliceStable(shared, func(i, j int) bool {
		if shared[i] == "id" {
			return true
		}
		if shared[j] == "id" {
			return false
		}
		return shared[i] < shared[j]
	})
	return shared, nil
}

func sqliteColumns(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+tableName+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func postgresColumns(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position`, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func normalizeSQLiteValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func normalizePostgresURL(raw string) string {
	value := strings.TrimSpace(strings.Trim(raw, `"'`))
	if strings.HasPrefix(strings.ToLower(value), "postgres://") {
		return "postgresql://" + strings.TrimPrefix(value, "postgres://")
	}
	return value
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := fmt.Sscanf(value, "%d", &fallback); parsed == 1 && err == nil {
			return fallback
		}
	}
	return fallback
}
