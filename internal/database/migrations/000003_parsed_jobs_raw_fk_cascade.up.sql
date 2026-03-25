DO $$
DECLARE
    fk_name text;
BEGIN
    SELECT c.conname
      INTO fk_name
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
     WHERE c.contype = 'f'
       AND n.nspname = current_schema()
       AND t.relname = 'parsed_jobs'
       AND a.attname = 'raw_us_job_id'
     LIMIT 1;

    IF fk_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE parsed_jobs DROP CONSTRAINT %I', fk_name);
    END IF;
END $$;

ALTER TABLE parsed_jobs
    ADD CONSTRAINT parsed_jobs_raw_us_job_id_fkey
    FOREIGN KEY (raw_us_job_id) REFERENCES raw_us_jobs(id) ON DELETE CASCADE;
