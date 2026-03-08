CREATE TABLE IF NOT EXISTS raw_us_jobs (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'remoterocketship',
    url TEXT NOT NULL UNIQUE,
    post_date TEXT NOT NULL,
    is_ready INTEGER NOT NULL DEFAULT 0,
    is_skippable INTEGER NOT NULL DEFAULT 0,
    is_parsed INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS parsed_companies (
    id BIGSERIAL PRIMARY KEY,
    external_company_id TEXT,
    name TEXT,
    slug TEXT,
    tagline TEXT,
    profile_pic_url TEXT,
    home_page_url TEXT,
    linkedin_url TEXT,
    employee_range TEXT,
    founded_year TEXT,
    sponsors_h1b INTEGER,
    industry_specialities TEXT
);

CREATE TABLE IF NOT EXISTS parsed_jobs (
    id BIGSERIAL PRIMARY KEY,
    raw_us_job_id BIGINT NOT NULL UNIQUE REFERENCES raw_us_jobs(id),
    external_job_id TEXT,
    company_id BIGINT REFERENCES parsed_companies(id) ON DELETE CASCADE,
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
    location_countries TEXT,
    employment_type TEXT,
    salary_type TEXT,
    updated_at TEXT,
    salary_min DOUBLE PRECISION,
    salary_max DOUBLE PRECISION,
    salary_min_usd DOUBLE PRECISION,
    salary_max_usd DOUBLE PRECISION,
    is_entry_level INTEGER,
    is_junior INTEGER,
    is_mid_level INTEGER,
    is_senior INTEGER,
    is_lead INTEGER,
    education_requirements_credential_category TEXT,
    experience_requirements_months INTEGER,
    experience_in_place_of_education INTEGER,
    required_languages TEXT,
    tech_stack TEXT
);

CREATE TABLE IF NOT EXISTS auth_users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_verification_codes (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES auth_users(id),
    code_hash TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_password_credentials (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL UNIQUE REFERENCES auth_users(id),
    password_salt TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES auth_users(id),
    session_token_hash TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pricing_plans (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    billing_cycle TEXT NOT NULL,
    duration_days INTEGER NOT NULL,
    price_usd INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_subscriptions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES auth_users(id),
    pricing_plan_id BIGINT NOT NULL REFERENCES pricing_plans(id),
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pricing_payments (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES auth_users(id),
    pricing_plan_id BIGINT NOT NULL REFERENCES pricing_plans(id),
    provider TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    amount_minor INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    provider_checkout_id TEXT,
    checkout_url TEXT,
    provider_payload TEXT,
    paid_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_job_actions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,
    parsed_job_id BIGINT NOT NULL REFERENCES parsed_jobs(id) ON DELETE CASCADE,
    is_applied INTEGER NOT NULL DEFAULT 0,
    is_saved INTEGER NOT NULL DEFAULT 0,
    is_hidden INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(user_id, parsed_job_id)
);

CREATE TABLE IF NOT EXISTS employer_organizations (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_by_user_id BIGINT NOT NULL REFERENCES auth_users(id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS employer_organization_members (
    id BIGSERIAL PRIMARY KEY,
    organization_id BIGINT NOT NULL REFERENCES employer_organizations(id),
    user_id BIGINT NOT NULL REFERENCES auth_users(id),
    role TEXT NOT NULL DEFAULT 'recruiter',
    created_at TEXT NOT NULL,
    UNIQUE(organization_id, user_id)
);

CREATE TABLE IF NOT EXISTS employer_jobs (
    id BIGSERIAL PRIMARY KEY,
    organization_id BIGINT NOT NULL REFERENCES employer_organizations(id),
    created_by_user_id BIGINT NOT NULL REFERENCES auth_users(id),
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
    salary_min DOUBLE PRECISION,
    salary_max DOUBLE PRECISION,
    posting_fee_usd INTEGER NOT NULL DEFAULT 10,
    posting_fee_status TEXT NOT NULL DEFAULT 'unpaid',
    posting_fee_paid_at TEXT,
    moderation_notes TEXT,
    published_at TEXT,
    closed_at TEXT,
    expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS employer_job_audit_events (
    id BIGSERIAL PRIMARY KEY,
    employer_job_id BIGINT NOT NULL REFERENCES employer_jobs(id),
    actor_user_id BIGINT REFERENCES auth_users(id),
    event_type TEXT NOT NULL,
    detail_json TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watcher_states (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL UNIQUE,
    source_url TEXT,
    sample_hash TEXT,
    first_lastmod TEXT,
    state_json TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watcher_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'remoterocketship',
    source_url TEXT NOT NULL,
    payload_type TEXT NOT NULL,
    body_text TEXT NOT NULL,
    consumed_at TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_parsed_jobs_salary_max_usd ON parsed_jobs (salary_max_usd);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_location_us_states_gin ON parsed_jobs USING GIN ((location_us_states::jsonb));
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_location_countries_gin ON parsed_jobs USING GIN ((location_countries::jsonb));
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_tech_stack_gin ON parsed_jobs USING GIN ((tech_stack::jsonb));

CREATE INDEX IF NOT EXISTS idx_user_job_actions_user_id ON user_job_actions (user_id);
CREATE INDEX IF NOT EXISTS idx_user_job_actions_parsed_job_id ON user_job_actions (parsed_job_id);
CREATE INDEX IF NOT EXISTS idx_user_job_actions_updated_at ON user_job_actions (updated_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_job_actions_user_id_parsed_job_id_unique ON user_job_actions (user_id, parsed_job_id);

CREATE INDEX IF NOT EXISTS idx_employer_organizations_name ON employer_organizations (name);
CREATE INDEX IF NOT EXISTS idx_employer_organization_members_org ON employer_organization_members (organization_id);
CREATE INDEX IF NOT EXISTS idx_employer_organization_members_user ON employer_organization_members (user_id);
CREATE INDEX IF NOT EXISTS idx_employer_jobs_status ON employer_jobs (status);
CREATE INDEX IF NOT EXISTS idx_employer_jobs_slug ON employer_jobs (slug);
CREATE INDEX IF NOT EXISTS idx_employer_jobs_published_at ON employer_jobs (published_at);
CREATE INDEX IF NOT EXISTS idx_employer_jobs_expires_at ON employer_jobs (expires_at);
CREATE INDEX IF NOT EXISTS idx_employer_jobs_posting_fee_status ON employer_jobs (posting_fee_status);
CREATE INDEX IF NOT EXISTS idx_employer_job_audit_events_job ON employer_job_audit_events (employer_job_id);
CREATE INDEX IF NOT EXISTS idx_employer_job_audit_events_actor ON employer_job_audit_events (actor_user_id);
CREATE INDEX IF NOT EXISTS idx_employer_job_audit_events_event_type ON employer_job_audit_events (event_type);
