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
            source TEXT NOT NULL DEFAULT 'remoterocketship',
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
            external_company_id TEXT,
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
            external_job_id TEXT,
            company_id INTEGER,
            created_at_source TEXT,
            url TEXT,
            categorized_job_title TEXT,
            categorized_job_function TEXT,
            role_title TEXT,
            role_description TEXT,
            role_requirements TEXT,
            benefits TEXT,
            job_description_summary TEXT,
            description_language TEXT,
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
		`CREATE TABLE IF NOT EXISTS user_job_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            parsed_job_id INTEGER NOT NULL,
            is_applied INTEGER NOT NULL DEFAULT 0,
            is_saved INTEGER NOT NULL DEFAULT 0,
            is_hidden INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, parsed_job_id),
            FOREIGN KEY(user_id) REFERENCES auth_users(id) ON DELETE CASCADE,
            FOREIGN KEY(parsed_job_id) REFERENCES parsed_jobs(id) ON DELETE CASCADE
        );`,
		`CREATE TABLE IF NOT EXISTS employer_organizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_by_user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(created_by_user_id) REFERENCES auth_users(id)
        );`,
		`CREATE TABLE IF NOT EXISTS employer_organization_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'recruiter',
            created_at TEXT NOT NULL,
            UNIQUE(organization_id, user_id),
            FOREIGN KEY(organization_id) REFERENCES employer_organizations(id),
            FOREIGN KEY(user_id) REFERENCES auth_users(id)
        );`,
		`CREATE TABLE IF NOT EXISTS employer_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            created_by_user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft',
            title TEXT,
            department TEXT,
            slug TEXT,
            description TEXT,
            requirements TEXT,
            benefits TEXT,
            employment_type TEXT,
            location_type TEXT,
            locations_json TEXT,
            seniority TEXT,
            tech_stack TEXT,
            apply_url TEXT,
            apply_email TEXT,
            salary_currency TEXT,
            salary_period TEXT,
            salary_min REAL,
            salary_max REAL,
            posting_fee_usd INTEGER NOT NULL DEFAULT 10,
            posting_fee_status TEXT NOT NULL DEFAULT 'unpaid',
            posting_fee_paid_at TEXT,
            moderation_notes TEXT,
            published_at TEXT,
            closed_at TEXT,
            expires_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(organization_id) REFERENCES employer_organizations(id),
            FOREIGN KEY(created_by_user_id) REFERENCES auth_users(id)
        );`,
		`CREATE TABLE IF NOT EXISTS employer_job_audit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employer_job_id INTEGER NOT NULL,
            actor_user_id INTEGER,
            event_type TEXT NOT NULL,
            detail_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(employer_job_id) REFERENCES employer_jobs(id),
            FOREIGN KEY(actor_user_id) REFERENCES auth_users(id)
        );`,
		`CREATE TABLE IF NOT EXISTS watcher_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL UNIQUE,
            source_url TEXT,
            sample_hash TEXT,
            first_lastmod TEXT,
            state_json TEXT,
            updated_at TEXT NOT NULL
        );`,
		`CREATE TABLE IF NOT EXISTS watcher_payloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'remoterocketship',
            source_url TEXT NOT NULL,
            payload_type TEXT NOT NULL,
            body_text TEXT NOT NULL,
            consumed_at TEXT,
            created_at TEXT NOT NULL
        );`,
	}
	for _, stmt := range stmts {
		if _, err := db.SQL.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate schema: %w", err)
		}
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_parsed_jobs_salary_max_usd ON parsed_jobs (salary_max_usd)`); err != nil {
		return fmt.Errorf("create parsed_jobs salary_max_usd index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_user_job_actions_user_id ON user_job_actions (user_id)`); err != nil {
		return fmt.Errorf("create user_job_actions user_id index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_user_job_actions_parsed_job_id ON user_job_actions (parsed_job_id)`); err != nil {
		return fmt.Errorf("create user_job_actions parsed_job_id index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_user_job_actions_updated_at ON user_job_actions (updated_at)`); err != nil {
		return fmt.Errorf("create user_job_actions updated_at index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `DROP INDEX IF EXISTS uq_user_job_action`); err != nil {
		return fmt.Errorf("drop legacy user_job_actions unique index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS uq_user_job_action_new ON user_job_actions (user_id, parsed_job_id)`); err != nil {
		return fmt.Errorf("create user_job_actions unique index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_organizations_name ON employer_organizations (name)`); err != nil {
		return fmt.Errorf("create employer_organizations name index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_organization_members_org ON employer_organization_members (organization_id)`); err != nil {
		return fmt.Errorf("create employer_organization_members organization_id index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_organization_members_user ON employer_organization_members (user_id)`); err != nil {
		return fmt.Errorf("create employer_organization_members user_id index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_jobs_status ON employer_jobs (status)`); err != nil {
		return fmt.Errorf("create employer_jobs status index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_jobs_slug ON employer_jobs (slug)`); err != nil {
		return fmt.Errorf("create employer_jobs slug index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_jobs_published_at ON employer_jobs (published_at)`); err != nil {
		return fmt.Errorf("create employer_jobs published_at index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_jobs_expires_at ON employer_jobs (expires_at)`); err != nil {
		return fmt.Errorf("create employer_jobs expires_at index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_jobs_posting_fee_status ON employer_jobs (posting_fee_status)`); err != nil {
		return fmt.Errorf("create employer_jobs posting_fee_status index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_job_audit_events_job ON employer_job_audit_events (employer_job_id)`); err != nil {
		return fmt.Errorf("create employer_job_audit_events employer_job_id index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_job_audit_events_actor ON employer_job_audit_events (actor_user_id)`); err != nil {
		return fmt.Errorf("create employer_job_audit_events actor_user_id index: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_employer_job_audit_events_event_type ON employer_job_audit_events (event_type)`); err != nil {
		return fmt.Errorf("create employer_job_audit_events event_type index: %w", err)
	}
	if err := db.ensureColumn(ctx, "parsed_jobs", "categorized_job_function", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "parsed_jobs", "location", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "parsed_jobs", "description_language", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "parsed_jobs", "external_job_id", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "parsed_companies", "external_company_id", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "parsed_companies", "founded_year", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "employer_jobs", "posting_fee_usd", "INTEGER NOT NULL DEFAULT 10"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "employer_jobs", "posting_fee_status", "TEXT NOT NULL DEFAULT 'unpaid'"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "employer_jobs", "posting_fee_paid_at", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "raw_us_jobs", "source", "TEXT NOT NULL DEFAULT 'remoterocketship'"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "watcher_payloads", "source", "TEXT NOT NULL DEFAULT 'remoterocketship'"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "watcher_states", "source", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "watcher_states", "source_url", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "watcher_states", "sample_hash", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "watcher_states", "first_lastmod", "TEXT"); err != nil {
		return err
	}
	if err := db.ensureColumn(ctx, "watcher_states", "state_json", "TEXT"); err != nil {
		return err
	}
	if _, err := db.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET source = COALESCE(NULLIF(source, ''), 'remoterocketship')`); err != nil {
		return fmt.Errorf("backfill raw_us_jobs source: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `UPDATE watcher_payloads SET source = COALESCE(NULLIF(source, ''), 'remoterocketship')`); err != nil {
		return fmt.Errorf("backfill watcher_payloads source: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `UPDATE watcher_states SET source = COALESCE(NULLIF(source, ''), 'remoterocketship') WHERE source IS NULL OR source = ''`); err != nil {
		return fmt.Errorf("backfill watcher_states source: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `UPDATE watcher_states
		SET state_json = COALESCE(
			state_json,
			json_object(
				'source_url', source_url,
				'sample_hash', sample_hash,
				'first_lastmod', first_lastmod
			)
		)`); err != nil {
		return fmt.Errorf("backfill watcher_states state_json: %w", err)
	}
	return nil
}

func (db *DB) ensureColumn(ctx context.Context, tableName, columnName, columnType string) error {
	rows, err := db.SQL.QueryContext(ctx, `PRAGMA table_info(`+tableName+`)`)
	if err != nil {
		return fmt.Errorf("inspect %s columns: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("scan %s columns: %w", tableName, err)
		}
		if name == columnName {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s columns: %w", tableName, err)
	}
	if _, err := db.SQL.ExecContext(ctx, `ALTER TABLE `+tableName+` ADD COLUMN `+columnName+` `+columnType); err != nil {
		return fmt.Errorf("add %s.%s: %w", tableName, columnName, err)
	}
	return nil
}
