ALTER TABLE parsed_companies
    DROP COLUMN IF EXISTS hq_location,
    DROP COLUMN IF EXISTS industries,
    DROP COLUMN IF EXISTS total_funding_amount,
    DROP COLUMN IF EXISTS number_of_employees_on_linkedin,
    DROP COLUMN IF EXISTS careers_page_url;
