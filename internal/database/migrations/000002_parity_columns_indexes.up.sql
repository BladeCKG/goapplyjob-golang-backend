CREATE TABLE IF NOT EXISTS watcher_events (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(32),
    detail TEXT,
    created_at TIMESTAMPTZ
);

ALTER TABLE raw_us_jobs
    ADD COLUMN IF NOT EXISTS extra_json TEXT;

ALTER TABLE auth_users
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_job_filters_json JSONB;

ALTER TABLE parsed_companies
    ADD COLUMN IF NOT EXISTS sponsors_uk_skilled_worker_visa BOOLEAN,
    ADD COLUMN IF NOT EXISTS tagline_brazil TEXT,
    ADD COLUMN IF NOT EXISTS tagline_france TEXT,
    ADD COLUMN IF NOT EXISTS tagline_germany TEXT,
    ADD COLUMN IF NOT EXISTS chatgpt_description TEXT,
    ADD COLUMN IF NOT EXISTS linkedin_description TEXT,
    ADD COLUMN IF NOT EXISTS chatgpt_description_brazil TEXT,
    ADD COLUMN IF NOT EXISTS chatgpt_description_france TEXT,
    ADD COLUMN IF NOT EXISTS chatgpt_description_germany TEXT,
    ADD COLUMN IF NOT EXISTS linkedin_description_brazil TEXT,
    ADD COLUMN IF NOT EXISTS linkedin_description_france TEXT,
    ADD COLUMN IF NOT EXISTS linkedin_description_germany TEXT,
    ADD COLUMN IF NOT EXISTS funding_data JSONB,
    ADD COLUMN IF NOT EXISTS chatgpt_industries JSONB,
    ADD COLUMN IF NOT EXISTS industry_specialities_brazil JSONB,
    ADD COLUMN IF NOT EXISTS industry_specialities_france JSONB,
    ADD COLUMN IF NOT EXISTS industry_specialities_germany JSONB,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

ALTER TABLE parsed_jobs
    ADD COLUMN IF NOT EXISTS valid_until_date TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS date_deleted TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS two_line_job_description_summary TEXT,
    ADD COLUMN IF NOT EXISTS role_title_brazil TEXT,
    ADD COLUMN IF NOT EXISTS role_description_brazil TEXT,
    ADD COLUMN IF NOT EXISTS role_requirements_brazil TEXT,
    ADD COLUMN IF NOT EXISTS benefits_brazil TEXT,
    ADD COLUMN IF NOT EXISTS slug_brazil VARCHAR(512),
    ADD COLUMN IF NOT EXISTS job_description_summary_brazil TEXT,
    ADD COLUMN IF NOT EXISTS two_line_job_description_summary_brazil TEXT,
    ADD COLUMN IF NOT EXISTS role_title_france TEXT,
    ADD COLUMN IF NOT EXISTS role_description_france TEXT,
    ADD COLUMN IF NOT EXISTS role_requirements_france TEXT,
    ADD COLUMN IF NOT EXISTS benefits_france TEXT,
    ADD COLUMN IF NOT EXISTS slug_france VARCHAR(512),
    ADD COLUMN IF NOT EXISTS job_description_summary_france TEXT,
    ADD COLUMN IF NOT EXISTS two_line_job_description_summary_france TEXT,
    ADD COLUMN IF NOT EXISTS role_title_germany TEXT,
    ADD COLUMN IF NOT EXISTS role_description_germany TEXT,
    ADD COLUMN IF NOT EXISTS role_requirements_germany TEXT,
    ADD COLUMN IF NOT EXISTS benefits_germany TEXT,
    ADD COLUMN IF NOT EXISTS slug_germany VARCHAR(512),
    ADD COLUMN IF NOT EXISTS job_description_summary_germany TEXT,
    ADD COLUMN IF NOT EXISTS two_line_job_description_summary_germany TEXT,
    ADD COLUMN IF NOT EXISTS slug VARCHAR(512),
    ADD COLUMN IF NOT EXISTS is_on_linkedin BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_promoted BOOLEAN,
    ADD COLUMN IF NOT EXISTS salary_currency_code VARCHAR(16),
    ADD COLUMN IF NOT EXISTS salary_currency_symbol VARCHAR(16),
    ADD COLUMN IF NOT EXISTS salary_human_text TEXT;

CREATE INDEX IF NOT EXISTS idx_watcher_events_source ON watcher_events (source);
CREATE INDEX IF NOT EXISTS idx_watcher_payloads_source ON watcher_payloads (source);
CREATE INDEX IF NOT EXISTS idx_watcher_payloads_source_url ON watcher_payloads (source_url);
CREATE INDEX IF NOT EXISTS idx_watcher_payloads_payload_type ON watcher_payloads (payload_type);
CREATE INDEX IF NOT EXISTS idx_watcher_payloads_created_at ON watcher_payloads (created_at);
CREATE INDEX IF NOT EXISTS idx_watcher_payloads_consumed_at ON watcher_payloads (consumed_at);
CREATE INDEX IF NOT EXISTS idx_raw_us_jobs_source ON raw_us_jobs (source);
CREATE INDEX IF NOT EXISTS idx_parsed_jobs_company_id ON parsed_jobs (company_id);
CREATE INDEX IF NOT EXISTS idx_parsed_jobs_external_job_id ON parsed_jobs (external_job_id);
CREATE INDEX IF NOT EXISTS idx_parsed_jobs_created_at_source ON parsed_jobs (created_at_source);
CREATE INDEX IF NOT EXISTS idx_parsed_jobs_employment_type ON parsed_jobs (employment_type);
CREATE INDEX IF NOT EXISTS idx_parsed_jobs_location_city ON parsed_jobs (location_city);
CREATE INDEX IF NOT EXISTS idx_parsed_jobs_categorized_job_title ON parsed_jobs (categorized_job_title);
CREATE INDEX IF NOT EXISTS idx_parsed_jobs_categorized_job_function ON parsed_jobs (categorized_job_function);
CREATE INDEX IF NOT EXISTS idx_parsed_jobs_salary_min_usd ON parsed_jobs (salary_min_usd);
CREATE UNIQUE INDEX IF NOT EXISTS idx_parsed_companies_external_company_id ON parsed_companies (external_company_id);
CREATE INDEX IF NOT EXISTS idx_auth_users_email ON auth_users (email);
CREATE INDEX IF NOT EXISTS idx_auth_users_last_seen_at ON auth_users (last_seen_at);
CREATE INDEX IF NOT EXISTS idx_auth_verification_codes_user_id ON auth_verification_codes (user_id);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_user_id ON auth_sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_session_token_hash ON auth_sessions (session_token_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_password_credentials_user_id ON auth_password_credentials (user_id);
CREATE INDEX IF NOT EXISTS idx_pricing_plans_code ON pricing_plans (code);
CREATE INDEX IF NOT EXISTS idx_user_subscriptions_user_id ON user_subscriptions (user_id);
CREATE INDEX IF NOT EXISTS idx_user_subscriptions_pricing_plan_id ON user_subscriptions (pricing_plan_id);
CREATE INDEX IF NOT EXISTS idx_pricing_payments_user_id ON pricing_payments (user_id);
CREATE INDEX IF NOT EXISTS idx_pricing_payments_pricing_plan_id ON pricing_payments (pricing_plan_id);
CREATE INDEX IF NOT EXISTS idx_pricing_payments_provider_checkout_id ON pricing_payments (provider_checkout_id);
CREATE INDEX IF NOT EXISTS idx_employer_organizations_created_by_user_id ON employer_organizations (created_by_user_id);
CREATE INDEX IF NOT EXISTS idx_employer_jobs_organization_id ON employer_jobs (organization_id);
CREATE INDEX IF NOT EXISTS idx_employer_jobs_created_by_user_id ON employer_jobs (created_by_user_id);
