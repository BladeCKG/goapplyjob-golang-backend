ALTER TABLE parsed_jobs
    DROP CONSTRAINT IF EXISTS parsed_jobs_raw_us_job_id_fkey;

ALTER TABLE parsed_jobs
    ADD CONSTRAINT parsed_jobs_raw_us_job_id_fkey
    FOREIGN KEY (raw_us_job_id) REFERENCES raw_us_jobs(id);
