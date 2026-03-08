package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"goapplyjob-golang-backend/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type DB struct {
	SQL *SQLConn
	PGX *pgxpool.Pool
}

type SQLConn struct {
	inner *sql.DB
}

type Tx struct {
	inner *sql.Tx
}

var loadEnvOnce sync.Once

func loadEnvForTests() {
	loadEnvOnce.Do(func() {
		_ = config.LoadDotEnvIfExists(".env")
		if _, file, _, ok := runtime.Caller(0); ok {
			repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
			_ = config.LoadDotEnvIfExists(filepath.Join(repoRoot, ".env"))
		}
	})
}

func TestDatabaseBaseURL() string {
	loadEnvForTests()
	if value := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL")); value != "" {
		return value
	}
	return ""
}

func HasTestDatabaseURL() bool {
	return TestDatabaseBaseURL() != ""
}

func Open(dsn string) (*DB, error) {
	trimmed := strings.ToLower(strings.TrimSpace(dsn))
	if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
		return nil, fmt.Errorf("postgres DATABASE_URL is required (got %q)", dsn)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Minute)

	if err := runPostgresMigrations(context.Background(), dsn); err != nil {
		_ = db.Close()
		return nil, err
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		_ = db.Close()
		return nil, err
	}

	return &DB{SQL: &SQLConn{inner: db}, PGX: pool}, nil
}

func (db *DB) Close() error {
	if db.PGX != nil {
		db.PGX.Close()
	}
	return db.SQL.Close()
}

func (db *DB) Ping(ctx context.Context) bool {
	return db.SQL.PingContext(ctx) == nil
}

func rewritePlaceholders(query string) string {
	if !strings.Contains(query, "?") {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	argN := 1
	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		next := byte(0)
		if i+1 < len(query) {
			next = query[i+1]
		}
		if inLineComment {
			b.WriteByte(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			b.WriteByte(ch)
			if ch == '*' && next == '/' {
				b.WriteByte(next)
				i++
				inBlockComment = false
			}
			continue
		}
		if inSingle {
			b.WriteByte(ch)
			if ch == '\'' {
				if next == '\'' {
					b.WriteByte(next)
					i++
				} else {
					inSingle = false
				}
			}
			continue
		}
		if inDouble {
			b.WriteByte(ch)
			if ch == '"' {
				if next == '"' {
					b.WriteByte(next)
					i++
				} else {
					inDouble = false
				}
			}
			continue
		}
		if ch == '-' && next == '-' {
			b.WriteString("--")
			i++
			inLineComment = true
			continue
		}
		if ch == '/' && next == '*' {
			b.WriteString("/*")
			i++
			inBlockComment = true
			continue
		}
		if ch == '\'' {
			inSingle = true
			b.WriteByte(ch)
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteByte(ch)
			continue
		}
		if ch == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(argN))
			argN++
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func (c *SQLConn) PingContext(ctx context.Context) error {
	return c.inner.PingContext(ctx)
}

func (c *SQLConn) Close() error {
	return c.inner.Close()
}

func (c *SQLConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.inner.ExecContext(ctx, rewritePlaceholders(query), args...)
}

func (c *SQLConn) Exec(query string, args ...any) (sql.Result, error) {
	return c.inner.Exec(rewritePlaceholders(query), args...)
}

func (c *SQLConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.inner.QueryContext(ctx, rewritePlaceholders(query), args...)
}

func (c *SQLConn) Query(query string, args ...any) (*sql.Rows, error) {
	return c.inner.Query(rewritePlaceholders(query), args...)
}

func (c *SQLConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.inner.QueryRowContext(ctx, rewritePlaceholders(query), args...)
}

func (c *SQLConn) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return c.inner.PrepareContext(ctx, rewritePlaceholders(query))
}

func (c *SQLConn) Raw() *sql.DB {
	return c.inner
}

func (c *SQLConn) Begin() (*Tx, error) {
	tx, err := c.inner.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{inner: tx}, nil
}

func (c *SQLConn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.inner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{inner: tx}, nil
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.inner.ExecContext(ctx, rewritePlaceholders(query), args...)
}

func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.inner.Exec(rewritePlaceholders(query), args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.inner.QueryContext(ctx, rewritePlaceholders(query), args...)
}

func (tx *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.inner.Query(rewritePlaceholders(query), args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.inner.QueryRowContext(ctx, rewritePlaceholders(query), args...)
}

func (tx *Tx) QueryRow(query string, args ...any) *sql.Row {
	return tx.inner.QueryRow(rewritePlaceholders(query), args...)
}

func (tx *Tx) Commit() error {
	return tx.inner.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.inner.Rollback()
}
