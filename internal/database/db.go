package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type DB struct {
	SQL *sql.DB
}

func Open(dsn string) (*DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(time.Minute)
	wrapped := &DB{SQL: db}
	if err := wrapped.Migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return wrapped, nil
}

func (db *DB) Close() error {
	return db.SQL.Close()
}

func (db *DB) Ping(ctx context.Context) bool {
	return db.SQL.PingContext(ctx) == nil
}

func (db *DB) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS raw_us_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL UNIQUE,
            post_date TEXT NOT NULL,
            is_ready INTEGER NOT NULL DEFAULT 0,
            is_skippable INTEGER NOT NULL DEFAULT 0,
            is_parsed INTEGER NOT NULL DEFAULT 0,
            retry_count INTEGER NOT NULL DEFAULT 0,
            raw_json TEXT
        );`,
		`CREATE TABLE IF NOT EXISTS parsed_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            slug TEXT,
            tagline TEXT,
            profile_pic_url TEXT,
            home_page_url TEXT,
            linkedin_url TEXT,
            employee_range TEXT,
            founded_year TEXT,
            sponsors_h1b INTEGER
        );`,
		`CREATE TABLE IF NOT EXISTS parsed_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_us_job_id INTEGER NOT NULL UNIQUE,
            company_id INTEGER,
            created_at_source TEXT,
            url TEXT,
            categorized_job_title TEXT,
            role_title TEXT,
            role_description TEXT,
            role_requirements TEXT,
            benefits TEXT,
            job_description_summary TEXT,
            location TEXT,
            location_city TEXT,
            location_type TEXT,
            location_us_states TEXT,
            employment_type TEXT,
            salary_type TEXT,
            updated_at TEXT,
            salary_min REAL,
            salary_max REAL,
            salary_min_usd REAL,
            salary_max_usd REAL,
            is_entry_level INTEGER,
            is_junior INTEGER,
            is_mid_level INTEGER,
            is_senior INTEGER,
            is_lead INTEGER,
            education_requirements_credential_category TEXT,
            experience_requirements_months INTEGER,
            experience_in_place_of_education INTEGER,
            required_languages TEXT,
            tech_stack TEXT,
            FOREIGN KEY(raw_us_job_id) REFERENCES raw_us_jobs(id),
            FOREIGN KEY(company_id) REFERENCES parsed_companies(id)
        );`,
		`CREATE TABLE IF NOT EXISTS auth_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );`,
		`CREATE TABLE IF NOT EXISTS auth_verification_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            code_hash TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            consumed_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES auth_users(id)
        );`,
		`CREATE TABLE IF NOT EXISTS auth_password_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            password_salt TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES auth_users(id)
        );`,
		`CREATE TABLE IF NOT EXISTS auth_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            session_token_hash TEXT NOT NULL UNIQUE,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES auth_users(id)
        );`,
		`CREATE TABLE IF NOT EXISTS pricing_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            billing_cycle TEXT NOT NULL,
            duration_days INTEGER NOT NULL,
            price_usd INTEGER NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );`,
		`CREATE TABLE IF NOT EXISTS user_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            pricing_plan_id INTEGER NOT NULL,
            starts_at TEXT NOT NULL,
            ends_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES auth_users(id),
            FOREIGN KEY(pricing_plan_id) REFERENCES pricing_plans(id)
        );`,
		`CREATE TABLE IF NOT EXISTS pricing_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            pricing_plan_id INTEGER NOT NULL,
            provider TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            amount_minor INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            provider_checkout_id TEXT,
            checkout_url TEXT,
            provider_payload TEXT,
            paid_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES auth_users(id),
            FOREIGN KEY(pricing_plan_id) REFERENCES pricing_plans(id)
        );`,
	}
	for _, stmt := range stmts {
		if _, err := db.SQL.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate schema: %w", err)
		}
	}
	return nil
}
