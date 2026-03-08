DROP INDEX IF EXISTS idx_employer_jobs_created_by_user_id;
DROP INDEX IF EXISTS idx_employer_jobs_organization_id;
DROP INDEX IF EXISTS idx_employer_organizations_created_by_user_id;
DROP INDEX IF EXISTS idx_pricing_payments_provider_checkout_id;
DROP INDEX IF EXISTS idx_pricing_payments_pricing_plan_id;
DROP INDEX IF EXISTS idx_pricing_payments_user_id;
DROP INDEX IF EXISTS idx_user_subscriptions_pricing_plan_id;
DROP INDEX IF EXISTS idx_user_subscriptions_user_id;
DROP INDEX IF EXISTS idx_pricing_plans_code;
DROP INDEX IF EXISTS idx_auth_password_credentials_user_id;
DROP INDEX IF EXISTS idx_auth_sessions_session_token_hash;
DROP INDEX IF EXISTS idx_auth_sessions_user_id;
DROP INDEX IF EXISTS idx_auth_verification_codes_user_id;
DROP INDEX IF EXISTS idx_auth_users_last_seen_at;
DROP INDEX IF EXISTS idx_auth_users_email;
DROP INDEX IF EXISTS idx_parsed_companies_external_company_id;
DROP INDEX IF EXISTS idx_parsed_jobs_salary_min_usd;
DROP INDEX IF EXISTS idx_parsed_jobs_categorized_job_function;
DROP INDEX IF EXISTS idx_parsed_jobs_categorized_job_title;
DROP INDEX IF EXISTS idx_parsed_jobs_location_city;
DROP INDEX IF EXISTS idx_parsed_jobs_employment_type;
DROP INDEX IF EXISTS idx_parsed_jobs_created_at_source;
DROP INDEX IF EXISTS idx_parsed_jobs_external_job_id;
DROP INDEX IF EXISTS idx_parsed_jobs_company_id;
DROP INDEX IF EXISTS idx_raw_us_jobs_source;
DROP INDEX IF EXISTS idx_watcher_payloads_consumed_at;
DROP INDEX IF EXISTS idx_watcher_payloads_created_at;
DROP INDEX IF EXISTS idx_watcher_payloads_payload_type;
DROP INDEX IF EXISTS idx_watcher_payloads_source_url;
DROP INDEX IF EXISTS idx_watcher_payloads_source;
DROP INDEX IF EXISTS idx_watcher_events_source;

ALTER TABLE parsed_jobs
    DROP COLUMN IF EXISTS salary_human_text,
    DROP COLUMN IF EXISTS salary_currency_symbol,
    DROP COLUMN IF EXISTS salary_currency_code,
    DROP COLUMN IF EXISTS is_promoted,
    DROP COLUMN IF EXISTS is_on_linkedin,
    DROP COLUMN IF EXISTS slug,
    DROP COLUMN IF EXISTS two_line_job_description_summary_germany,
    DROP COLUMN IF EXISTS job_description_summary_germany,
    DROP COLUMN IF EXISTS slug_germany,
    DROP COLUMN IF EXISTS benefits_germany,
    DROP COLUMN IF EXISTS role_requirements_germany,
    DROP COLUMN IF EXISTS role_description_germany,
    DROP COLUMN IF EXISTS role_title_germany,
    DROP COLUMN IF EXISTS two_line_job_description_summary_france,
    DROP COLUMN IF EXISTS job_description_summary_france,
    DROP COLUMN IF EXISTS slug_france,
    DROP COLUMN IF EXISTS benefits_france,
    DROP COLUMN IF EXISTS role_requirements_france,
    DROP COLUMN IF EXISTS role_description_france,
    DROP COLUMN IF EXISTS role_title_france,
    DROP COLUMN IF EXISTS two_line_job_description_summary_brazil,
    DROP COLUMN IF EXISTS job_description_summary_brazil,
    DROP COLUMN IF EXISTS slug_brazil,
    DROP COLUMN IF EXISTS benefits_brazil,
    DROP COLUMN IF EXISTS role_requirements_brazil,
    DROP COLUMN IF EXISTS role_description_brazil,
    DROP COLUMN IF EXISTS role_title_brazil,
    DROP COLUMN IF EXISTS two_line_job_description_summary,
    DROP COLUMN IF EXISTS date_deleted,
    DROP COLUMN IF EXISTS valid_until_date;

ALTER TABLE parsed_companies
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS industry_specialities_germany,
    DROP COLUMN IF EXISTS industry_specialities_france,
    DROP COLUMN IF EXISTS industry_specialities_brazil,
    DROP COLUMN IF EXISTS chatgpt_industries,
    DROP COLUMN IF EXISTS funding_data,
    DROP COLUMN IF EXISTS linkedin_description_germany,
    DROP COLUMN IF EXISTS linkedin_description_france,
    DROP COLUMN IF EXISTS linkedin_description_brazil,
    DROP COLUMN IF EXISTS chatgpt_description_germany,
    DROP COLUMN IF EXISTS chatgpt_description_france,
    DROP COLUMN IF EXISTS chatgpt_description_brazil,
    DROP COLUMN IF EXISTS linkedin_description,
    DROP COLUMN IF EXISTS chatgpt_description,
    DROP COLUMN IF EXISTS tagline_germany,
    DROP COLUMN IF EXISTS tagline_france,
    DROP COLUMN IF EXISTS tagline_brazil,
    DROP COLUMN IF EXISTS sponsors_uk_skilled_worker_visa;

ALTER TABLE auth_users
    DROP COLUMN IF EXISTS last_job_filters_json,
    DROP COLUMN IF EXISTS last_seen_at;

ALTER TABLE raw_us_jobs
    DROP COLUMN IF EXISTS extra_json;

DROP TABLE IF EXISTS watcher_events;
