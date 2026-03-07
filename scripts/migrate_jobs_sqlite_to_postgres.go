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

var preferredTableOrder = []string{
	"parsed_companies",
	"raw_us_jobs",
	"parsed_jobs",
	"watcher_states",
	"watcher_payloads",
	"auth_users",
	"auth_password_credentials",
	"auth_verification_codes",
	"auth_sessions",
	"pricing_plans",
	"pricing_payments",
	"user_subscriptions",
}

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
	tableNames, err := commonTableNames(ctx, sourceDB, targetDB)
	if err != nil {
		log.Fatal(err)
	}
	total := 0
	for _, tableName := range tableNames {
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

	resumeFromID, err := targetMaxID(ctx, targetDB, tableName)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("SELECT %s FROM %s %s %s ORDER BY id ASC", strings.Join(sharedColumns, ", "), tableName, whereSQL, resumeWhereSQL(resumeFromID, whereSQL == ""))
	if resumeFromID > 0 {
		args = append(args, resumeFromID)
	}
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
	sqlText := ""
	switch tableName {
	case "raw_us_jobs":
		sqlText = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s ON CONFLICT (url) DO NOTHING",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(valueGroups, ", "),
		)
	case "parsed_jobs":
		sqlText = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s ON CONFLICT (raw_us_job_id) DO UPDATE SET %s",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(valueGroups, ", "),
			strings.Join(filterOut(updateCols, "raw_us_job_id = EXCLUDED.raw_us_job_id"), ", "),
		)
	default:
		sqlText = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s ON CONFLICT (id) DO UPDATE SET %s",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(valueGroups, ", "),
			strings.Join(updateCols, ", "),
		)
	}
	_, err := db.ExecContext(ctx, sqlText, args...)
	return err
}

func commonTableNames(ctx context.Context, sourceDB, targetDB *sql.DB) ([]string, error) {
	sourceTables, err := sqliteTableNames(ctx, sourceDB)
	if err != nil {
		return nil, err
	}
	targetTables, err := postgresTableNames(ctx, targetDB)
	if err != nil {
		return nil, err
	}
	targetSet := map[string]struct{}{}
	for _, name := range targetTables {
		targetSet[name] = struct{}{}
	}
	ordered := make([]string, 0, len(preferredTableOrder))
	used := map[string]struct{}{}
	for _, name := range preferredTableOrder {
		if contains(sourceTables, name) {
			if _, ok := targetSet[name]; ok {
				ordered = append(ordered, name)
				used[name] = struct{}{}
			}
		}
	}
	for _, name := range sourceTables {
		if _, ok := targetSet[name]; !ok {
			continue
		}
		if _, seen := used[name]; seen {
			continue
		}
		ordered = append(ordered, name)
	}
	return ordered, nil
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

func sqliteTableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
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

func postgresTableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name`)
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

func targetMaxID(ctx context.Context, db *sql.DB, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COALESCE(MAX(id), 0) FROM %s", tableName)
	var maxID int64
	if err := db.QueryRowContext(ctx, query).Scan(&maxID); err != nil {
		return 0, err
	}
	return maxID, nil
}

func resumeWhereSQL(resumeFromID int64, noWhere bool) string {
	if resumeFromID <= 0 {
		return ""
	}
	if noWhere {
		return "WHERE id > ?"
	}
	return "AND id > ?"
}

func normalizeSQLiteValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func filterOut(values []string, target string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			out = append(out, value)
		}
	}
	return out
}

func normalizePostgresURL(raw string) string {
	value := strings.TrimSpace(strings.Trim(raw, `"'`))
	if strings.HasPrefix(strings.ToLower(value), "postgres://") {
		return "postgresql://" + strings.TrimPrefix(value, "postgres://")
	}
	return value
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
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
