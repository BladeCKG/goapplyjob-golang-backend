package database

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func runPostgresMigrations(_ context.Context, dsn string) error {
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open postgres for migrations: %w", err)
	}
	defer conn.Close()

	driver, err := postgres.WithInstance(conn, &postgres.Config{
		MigrationsTable: migrationTableName(dsn),
	})
	if err != nil {
		return fmt.Errorf("create postgres migration driver: %w", err)
	}
	sourceDriver, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded migration source: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", driver)
	if err != nil {
		return fmt.Errorf("create migrate instance: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply postgres migrations: %w", err)
	}
	return nil
}

func migrationTableName(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "schema_migrations"
	}
	rawSearchPath := strings.TrimSpace(parsed.Query().Get("search_path"))
	if rawSearchPath == "" {
		return "schema_migrations"
	}
	firstSchema := strings.TrimSpace(strings.Split(rawSearchPath, ",")[0])
	if firstSchema == "" {
		return "schema_migrations"
	}
	sum := sha1.Sum([]byte(strings.ToLower(firstSchema)))
	suffix := hex.EncodeToString(sum[:4])
	return "schema_migrations_" + suffix
}
