CREATE TABLE IF NOT EXISTS auth_users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(320) NOT NULL UNIQUE,
    last_seen_at TIMESTAMPTZ,
    last_job_filters_json JSON,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS parsed_companies (
    id SERIAL PRIMARY KEY,
    external_company_id TEXT UNIQUE,
    name TEXT,
    slug VARCHAR(512),
    tagline TEXT,
    founded_year TEXT,
    home_page_url TEXT,
    linkedin_url TEXT,
    sponsors_h1b BOOLEAN,
    sponsors_uk_skilled_worker_visa BOOLEAN,
    employee_range VARCHAR(128),
    profile_pic_url TEXT,
    tagline_brazil TEXT,
    tagline_france TEXT,
    tagline_germany TEXT,
    chatgpt_description TEXT,
    linkedin_description TEXT,
    chatgpt_description_brazil TEXT,
    chatgpt_description_france TEXT,
    chatgpt_description_germany TEXT,
    linkedin_description_brazil TEXT,
    linkedin_description_france TEXT,
    linkedin_description_germany TEXT,
    funding_data JSON,
    chatgpt_industries JSON,
    industry_specialities JSON,
    industry_specialities_brazil JSON,
    industry_specialities_france JSON,
    industry_specialities_germany JSON,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pricing_plans (
    id SERIAL PRIMARY KEY,
    code VARCHAR(32) NOT NULL UNIQUE,
    name VARCHAR(64) NOT NULL,
    billing_cycle VARCHAR(32) NOT NULL,
    duration_days INTEGER NOT NULL,
    price_usd INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_us_jobs (
    id SERIAL PRIMARY KEY,
    source VARCHAR(64) NOT NULL DEFAULT 'remoterocketship',
    url TEXT NOT NULL UNIQUE,
    post_date TIMESTAMPTZ NOT NULL,
    is_ready BOOLEAN NOT NULL DEFAULT false,
    is_skippable BOOLEAN NOT NULL DEFAULT false,
    is_parsed BOOLEAN NOT NULL DEFAULT false,
    retry_count INTEGER NOT NULL DEFAULT 0,
    extra_json TEXT,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS watcher_events (
    id SERIAL PRIMARY KEY,
    source VARCHAR(32) NOT NULL,
    detail TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS watcher_payloads (
    id SERIAL PRIMARY KEY,
    source VARCHAR(64) NOT NULL DEFAULT 'remoterocketship',
    source_url TEXT NOT NULL,
    payload_type VARCHAR(32) NOT NULL,
    body_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS watcher_states (
    id SERIAL PRIMARY KEY,
    source VARCHAR(64) NOT NULL UNIQUE DEFAULT 'remoterocketship',
    state_json JSON,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS auth_password_credentials (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL UNIQUE REFERENCES auth_users(id),
    password_salt VARCHAR(128) NOT NULL,
    password_hash VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES auth_users(id),
    session_token_hash VARCHAR(128) NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_verification_codes (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES auth_users(id),
    code_hash VARCHAR(128) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS parsed_jobs (
    id SERIAL PRIMARY KEY,
    raw_us_job_id INTEGER NOT NULL UNIQUE REFERENCES raw_us_jobs(id),
    company_id INTEGER REFERENCES parsed_companies(id) ON DELETE CASCADE,
    external_job_id TEXT,
    created_at_source TIMESTAMPTZ,
    valid_until_date TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    description_language TEXT,
    role_title TEXT,
    role_description TEXT,
    role_requirements TEXT,
    benefits TEXT,
    job_description_summary TEXT,
    two_line_job_description_summary TEXT,
    role_title_brazil TEXT,
    role_description_brazil TEXT,
    role_requirements_brazil TEXT,
    benefits_brazil TEXT,
    slug_brazil VARCHAR(512),
    job_description_summary_brazil TEXT,
    two_line_job_description_summary_brazil TEXT,
    role_title_france TEXT,
    role_description_france TEXT,
    role_requirements_france TEXT,
    benefits_france TEXT,
    slug_france VARCHAR(512),
    job_description_summary_france TEXT,
    two_line_job_description_summary_france TEXT,
    role_title_germany TEXT,
    role_description_germany TEXT,
    role_requirements_germany TEXT,
    benefits_germany TEXT,
    slug_germany VARCHAR(512),
    job_description_summary_germany TEXT,
    two_line_job_description_summary_germany TEXT,
    url TEXT,
    slug VARCHAR(512),
    employment_type VARCHAR(64),
    location_type VARCHAR(64),
    location_city TEXT,
    categorized_job_title VARCHAR(256),
    categorized_job_function VARCHAR(256),
    education_requirements_credential_category TEXT,
    experience_in_place_of_education BOOLEAN,
    experience_requirements_months INTEGER,
    is_on_linkedin BOOLEAN,
    is_promoted BOOLEAN,
    is_entry_level BOOLEAN,
    is_junior BOOLEAN,
    is_mid_level BOOLEAN,
    is_senior BOOLEAN,
    is_lead BOOLEAN,
    required_languages JSON,
    location_us_states JSONB,
    location_countries JSONB,
    tech_stack JSONB,
    salary_min FLOAT,
    salary_max FLOAT,
    salary_type VARCHAR(64),
    salary_currency_code VARCHAR(16),
    salary_currency_symbol VARCHAR(16),
    salary_min_usd FLOAT,
    salary_max_usd FLOAT,
    salary_human_text TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pricing_payments (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES auth_users(id),
    pricing_plan_id INTEGER NOT NULL REFERENCES pricing_plans(id),
    provider VARCHAR(32) NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    currency VARCHAR(16) NOT NULL DEFAULT 'USD',
    amount_minor INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    provider_checkout_id VARCHAR(256),
    checkout_url TEXT,
    provider_payload JSON,
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS user_subscriptions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES auth_users(id),
    pricing_plan_id INTEGER NOT NULL REFERENCES pricing_plans(id),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS employer_organizations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    created_by_user_id INTEGER NOT NULL REFERENCES auth_users(id),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS employer_organization_members (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER NOT NULL REFERENCES employer_organizations(id),
    user_id INTEGER NOT NULL REFERENCES auth_users(id),
    role VARCHAR(32) NOT NULL DEFAULT 'recruiter',
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(organization_id, user_id)
);

CREATE TABLE IF NOT EXISTS employer_jobs (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER NOT NULL REFERENCES employer_organizations(id),
    created_by_user_id INTEGER NOT NULL REFERENCES auth_users(id),
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    title VARCHAR(256),
    department VARCHAR(128),
    slug VARCHAR(512),
    description TEXT,
    requirements TEXT,
    benefits TEXT,
    employment_type VARCHAR(64),
    location_type VARCHAR(64),
    locations_json JSON,
    seniority VARCHAR(64),
    tech_stack JSON,
    apply_url VARCHAR(2048),
    apply_email VARCHAR(320),
    salary_currency VARCHAR(16),
    salary_period VARCHAR(32),
    salary_min FLOAT,
    salary_max FLOAT,
    posting_fee_usd INTEGER NOT NULL DEFAULT 10,
    posting_fee_status VARCHAR(32) NOT NULL DEFAULT 'unpaid',
    posting_fee_paid_at TIMESTAMPTZ,
    moderation_notes TEXT,
    published_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS employer_job_audit_events (
    id SERIAL PRIMARY KEY,
    employer_job_id INTEGER NOT NULL REFERENCES employer_jobs(id),
    actor_user_id INTEGER REFERENCES auth_users(id),
    event_type VARCHAR(64) NOT NULL,
    detail_json JSON,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS user_job_actions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,
    parsed_job_id INTEGER NOT NULL REFERENCES parsed_jobs(id) ON DELETE CASCADE,
    is_applied BOOLEAN NOT NULL DEFAULT false,
    is_saved BOOLEAN NOT NULL DEFAULT false,
    is_hidden BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE(user_id, parsed_job_id)
);

CREATE INDEX IF NOT EXISTS ix_auth_users_email ON auth_users (email);
CREATE INDEX IF NOT EXISTS ix_watcher_events_source ON watcher_events (source);
CREATE INDEX IF NOT EXISTS ix_watcher_payloads_source ON watcher_payloads (source);
CREATE INDEX IF NOT EXISTS ix_watcher_payloads_source_url ON watcher_payloads (source_url);
CREATE INDEX IF NOT EXISTS ix_watcher_payloads_payload_type ON watcher_payloads (payload_type);
CREATE INDEX IF NOT EXISTS ix_watcher_payloads_created_at ON watcher_payloads (created_at);
CREATE INDEX IF NOT EXISTS ix_watcher_payloads_consumed_at ON watcher_payloads (consumed_at);
CREATE INDEX IF NOT EXISTS ix_raw_us_jobs_source ON raw_us_jobs (source);
CREATE INDEX IF NOT EXISTS ix_auth_password_credentials_user_id ON auth_password_credentials (user_id);
CREATE INDEX IF NOT EXISTS ix_auth_sessions_session_token_hash ON auth_sessions (session_token_hash);
CREATE INDEX IF NOT EXISTS ix_auth_sessions_user_id ON auth_sessions (user_id);
CREATE INDEX IF NOT EXISTS ix_auth_verification_codes_user_id ON auth_verification_codes (user_id);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_company_id ON parsed_jobs (company_id);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_external_job_id ON parsed_jobs (external_job_id);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_created_at_source ON parsed_jobs (created_at_source);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_employment_type ON parsed_jobs (employment_type);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_location_city ON parsed_jobs (location_city);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_categorized_job_title ON parsed_jobs (categorized_job_title);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_categorized_job_function ON parsed_jobs (categorized_job_function);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_salary_min_usd ON parsed_jobs (salary_min_usd);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_salary_max_usd ON parsed_jobs (salary_max_usd);
CREATE INDEX IF NOT EXISTS ix_pricing_plans_code ON pricing_plans (code);
CREATE INDEX IF NOT EXISTS ix_user_subscriptions_user_id ON user_subscriptions (user_id);
CREATE INDEX IF NOT EXISTS ix_user_subscriptions_pricing_plan_id ON user_subscriptions (pricing_plan_id);
CREATE INDEX IF NOT EXISTS ix_pricing_payments_user_id ON pricing_payments (user_id);
CREATE INDEX IF NOT EXISTS ix_pricing_payments_pricing_plan_id ON pricing_payments (pricing_plan_id);
CREATE INDEX IF NOT EXISTS ix_pricing_payments_provider_checkout_id ON pricing_payments (provider_checkout_id);
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
CREATE INDEX IF NOT EXISTS idx_user_job_actions_user_id ON user_job_actions (user_id);
CREATE INDEX IF NOT EXISTS idx_user_job_actions_parsed_job_id ON user_job_actions (parsed_job_id);
CREATE INDEX IF NOT EXISTS idx_user_job_actions_updated_at ON user_job_actions (updated_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_job_actions_user_id_parsed_job_id_unique ON user_job_actions (user_id, parsed_job_id);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_location_us_states_gin ON parsed_jobs USING GIN (location_us_states);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_location_countries_gin ON parsed_jobs USING GIN (location_countries);
CREATE INDEX IF NOT EXISTS ix_parsed_jobs_tech_stack_gin ON parsed_jobs USING GIN (tech_stack);
