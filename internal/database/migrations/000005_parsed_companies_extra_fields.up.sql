ALTER TABLE parsed_companies
    ADD COLUMN IF NOT EXISTS careers_page_url TEXT,
    ADD COLUMN IF NOT EXISTS number_of_employees_on_linkedin INTEGER,
    ADD COLUMN IF NOT EXISTS total_funding_amount BIGINT,
    ADD COLUMN IF NOT EXISTS industries JSONB,
    ADD COLUMN IF NOT EXISTS hq_location TEXT;
