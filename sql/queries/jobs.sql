-- name: GetMaxParsedJobID :one
SELECT COALESCE(MAX(id), 0)::bigint AS max_id
FROM parsed_jobs;

-- name: ListDistinctJobCategoryFunctionPairs :many
SELECT DISTINCT categorized_job_title, categorized_job_function
FROM parsed_jobs
WHERE categorized_job_title IS NOT NULL
  AND categorized_job_function IS NOT NULL;

-- name: ListDistinctTechStackTexts :many
SELECT DISTINCT tech_stack
FROM parsed_jobs
WHERE tech_stack IS NOT NULL;

-- name: ListDistinctEmploymentTypes :many
SELECT DISTINCT employment_type
FROM parsed_jobs
WHERE employment_type IS NOT NULL;

-- name: ListDistinctLocationTypes :many
SELECT DISTINCT location_type
FROM parsed_jobs
WHERE location_type IS NOT NULL;

-- name: GetJobDetailByID :one
SELECT p.id, p.raw_us_job_id, c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b, p.categorized_job_title, p.categorized_job_function, p.role_title, p.location_city, p.location_type, p.location_us_states, p.location_countries, p.employment_type, p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_type, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead, p.updated_at, p.created_at_source, p.role_description, p.role_requirements, p.education_requirements_credential_category, p.experience_requirements_months, p.experience_in_place_of_education, p.required_languages, p.tech_stack, p.benefits, p.url
FROM parsed_jobs p
LEFT JOIN parsed_companies c ON c.id = p.company_id
WHERE p.id = $1
LIMIT 1;

-- name: GetActiveSubscriptionIDForUser :one
SELECT s.id
FROM user_subscriptions s
JOIN pricing_plans p ON p.id = s.pricing_plan_id
WHERE s.user_id = $1 AND s.ends_at > $2 AND p.is_active = true
ORDER BY s.ends_at DESC
LIMIT 1;

-- name: CountParsedJobs :one
SELECT COUNT(id)::bigint AS count
FROM parsed_jobs;

-- name: ListJobSitemapPage :many
SELECT p.id, p.role_title, p.categorized_job_title, c.name, p.created_at_source
FROM parsed_jobs p
LEFT JOIN parsed_companies c ON c.id = p.company_id
ORDER BY p.created_at_source DESC, p.id DESC
LIMIT $1 OFFSET $2;

-- name: CountCompaniesWithJobsForSitemap :one
SELECT COUNT(DISTINCT c.id)::bigint AS count
FROM parsed_companies c
JOIN parsed_jobs p ON p.company_id = c.id
WHERE c.slug IS NOT NULL AND trim(c.slug) != '';

-- name: ListCompanySitemapPage :many
SELECT c.slug, c.name, MAX(p.created_at_source)::timestamptz AS latest_job_posted_at
FROM parsed_companies c
JOIN parsed_jobs p ON p.company_id = c.id
WHERE c.slug IS NOT NULL AND trim(c.slug) != ''
GROUP BY c.id, c.slug, c.name
ORDER BY latest_job_posted_at DESC, c.id DESC
LIMIT $1 OFFSET $2;

-- name: GetCompanyProfileBySlug :one
SELECT id, slug, name, tagline, profile_pic_url, home_page_url, linkedin_url, employee_range, founded_year, sponsors_h1b, industry_specialities
FROM parsed_companies
WHERE lower(trim(COALESCE(slug, ''))) = $1
LIMIT 1;

-- name: GetCompanyProfileStats :one
SELECT COUNT(id)::bigint AS total_jobs, MAX(created_at_source)::timestamptz AS latest_job_posted_at
FROM parsed_jobs
WHERE company_id = $1;

-- name: GetTopFunctionByCategory :one
SELECT categorized_job_function
FROM parsed_jobs
WHERE categorized_job_title = $1
  AND categorized_job_function IS NOT NULL
  AND categorized_job_function != ''
GROUP BY categorized_job_function
ORDER BY COUNT(id) DESC, categorized_job_function ASC
LIMIT 1;

-- name: ListRelatedCategoriesByFunction :many
SELECT categorized_job_title, COUNT(id)::bigint AS score
FROM parsed_jobs
WHERE categorized_job_title IS NOT NULL
  AND categorized_job_title != ''
  AND categorized_job_function = $1
GROUP BY categorized_job_title
ORDER BY CASE WHEN categorized_job_title = $2 THEN 0 ELSE 1 END ASC,
         score DESC,
         categorized_job_title ASC
LIMIT $3;

-- name: ListTopCategories :many
SELECT categorized_job_title, COUNT(id)::bigint AS score
FROM parsed_jobs
WHERE categorized_job_title IS NOT NULL
  AND categorized_job_title != ''
  AND created_at_source IS NOT NULL
  AND created_at_source >= $1
  AND (
    NOT $2::boolean
    OR CAST(location_us_states AS text) ILIKE $3
    OR CAST(location_countries AS text) ILIKE $4
  )
GROUP BY categorized_job_title
ORDER BY score DESC, categorized_job_title ASC
LIMIT $5;

-- name: ListJobsForListing :many
SELECT p.id, p.raw_us_job_id, p.company_id, p.role_title, p.job_description_summary, c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b, p.categorized_job_title, p.categorized_job_function, p.location_city, p.location_type, p.location_us_states, p.location_countries, p.employment_type, p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_type, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead, p.tech_stack, p.updated_at, p.created_at_source, p.url
FROM parsed_jobs p
LEFT JOIN parsed_companies c ON c.id = p.company_id;

-- name: CountJobsForListingFiltered :one
SELECT
	COUNT(p.id)::bigint AS total,
	CASE
		WHEN sqlc.arg(company_filter)::text <> '' THEN CASE WHEN COUNT(p.id) > 0 THEN 1::bigint ELSE 0::bigint END
		ELSE COUNT(DISTINCT p.company_id)::bigint
	END AS company_count
FROM parsed_jobs p
WHERE
(
	NOT sqlc.arg(has_title_filters)::boolean
	OR p.categorized_job_title = ANY(sqlc.arg(job_categories)::text[])
	OR p.categorized_job_function = ANY(sqlc.arg(job_functions)::text[])
	OR lower(trim(COALESCE(p.categorized_job_title, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR lower(trim(COALESCE(p.categorized_job_function, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR lower(trim(COALESCE(p.role_title, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR p.categorized_job_title ILIKE ANY(sqlc.arg(title_like_patterns)::text[])
	OR p.role_title ILIKE ANY(sqlc.arg(title_like_patterns)::text[])
	OR EXISTS (
		SELECT 1
		FROM jsonb_array_elements(sqlc.arg(title_token_groups_json)::jsonb) AS grp(tokens)
		WHERE jsonb_array_length(grp.tokens) > 0
		  AND NOT EXISTS (
			SELECT 1
			FROM jsonb_array_elements_text(grp.tokens) AS tok(token)
			WHERE COALESCE(p.role_title, '') NOT ILIKE ('%%' || tok.token || '%%')
		  )
	)
)
AND (
	sqlc.arg(company_filter)::text = ''
	OR EXISTS (
		SELECT 1
		FROM parsed_companies c
		WHERE c.id = p.company_id
		AND (
			lower(trim(COALESCE(c.slug, ''))) = sqlc.arg(company_filter)::text
			OR lower(trim(COALESCE(c.name, ''))) = sqlc.arg(company_filter)::text
		)
	)
)
AND (
	NOT sqlc.arg(has_structured_location)::boolean
	OR EXISTS (
		SELECT 1
		FROM unnest(sqlc.arg(us_states)::text[]) AS s(state_name)
		WHERE CAST(p.location_us_states AS jsonb) @> to_jsonb(ARRAY[s.state_name])
	)
	OR EXISTS (
		SELECT 1
		FROM unnest(sqlc.arg(countries)::text[]) AS c(country_name)
		WHERE CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[c.country_name])
	)
)
AND (
	sqlc.arg(has_structured_location)::boolean
	OR cardinality(sqlc.arg(location_patterns)::text[]) = 0
	OR p.location_city ILIKE ANY(sqlc.arg(location_patterns)::text[])
	OR CAST(p.location_us_states AS text) ILIKE ANY(sqlc.arg(location_patterns)::text[])
	OR CAST(p.location_countries AS text) ILIKE ANY(sqlc.arg(location_patterns)::text[])
)
AND (
	cardinality(sqlc.arg(tech_stacks)::text[]) = 0
	OR EXISTS (
		SELECT 1
		FROM unnest(sqlc.arg(tech_stacks)::text[]) AS t(stack_name)
		WHERE CAST(p.tech_stack AS jsonb) @> to_jsonb(ARRAY[t.stack_name])
	)
)
AND (
	cardinality(sqlc.arg(location_types)::text[]) = 0
	OR p.location_type ILIKE ANY(sqlc.arg(location_types)::text[])
)
AND (
	cardinality(sqlc.arg(employment_types)::text[]) = 0
	OR p.employment_type ILIKE ANY(sqlc.arg(employment_types)::text[])
)
AND (
	NOT sqlc.arg(has_created_from)::boolean
	OR p.created_at_source >= sqlc.arg(created_from)::timestamptz
)
AND (
	NOT sqlc.arg(has_created_to)::boolean
	OR p.created_at_source < sqlc.arg(created_to)::timestamptz
)
AND (
	NOT sqlc.arg(has_min_salary)::boolean
	OR (
		(
			COALESCE(p.salary_max_usd, p.salary_min_usd) * CASE
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('yearly', 'year', 'annual') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%year%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annual%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annually%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per year%' THEN 1.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('monthly', 'month') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month-salary%' THEN 12.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('biweekly', 'bi-weekly') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%biweekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%bi-weekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per biweekly%' THEN 26.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('weekly', 'week') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%week%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per week%' THEN 52.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('daily', 'day') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per visit%' THEN 260.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('hourly', 'hour', 'hr') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%hour%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per hour%' THEN 2080.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%minute%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/min%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per min%' THEN 124800.0
				ELSE 1.0
			END
		) >= sqlc.arg(min_salary)::float8
		OR (
			COALESCE(p.salary_max, p.salary_min) * CASE
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('yearly', 'year', 'annual') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%year%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annual%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annually%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per year%' THEN 1.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('monthly', 'month') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month-salary%' THEN 12.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('biweekly', 'bi-weekly') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%biweekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%bi-weekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per biweekly%' THEN 26.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('weekly', 'week') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%week%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per week%' THEN 52.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('daily', 'day') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per visit%' THEN 260.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('hourly', 'hour', 'hr') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%hour%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per hour%' THEN 2080.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%minute%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/min%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per min%' THEN 124800.0
				ELSE 1.0
			END
		) >= sqlc.arg(min_salary)::float8
	)
)
AND (
	NOT sqlc.arg(has_seniority)::boolean
	OR (
		(sqlc.arg(seniority_entry)::boolean AND p.is_entry_level = true)
		OR (sqlc.arg(seniority_junior)::boolean AND p.is_junior = true)
		OR (sqlc.arg(seniority_mid)::boolean AND p.is_mid_level = true)
		OR (sqlc.arg(seniority_senior)::boolean AND p.is_senior = true)
		OR (sqlc.arg(seniority_lead)::boolean AND p.is_lead = true)
	)
)
AND (
	NOT sqlc.arg(has_user)::boolean
	OR (
		CASE sqlc.arg(user_action_filter)::text
			WHEN 'hidden' THEN EXISTS (
				SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
			)
			WHEN 'applied' THEN EXISTS (
				SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true
			)
			WHEN 'not_applied' THEN (
				NOT EXISTS (
					SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true
				)
				AND NOT EXISTS (
					SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
				)
			)
			WHEN 'saved' THEN EXISTS (
				SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_saved = true
			)
			ELSE NOT EXISTS (
				SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
			)
		END
	)
);

-- name: ListFilteredJobIDs :many
SELECT p.id
FROM parsed_jobs p
WHERE
(
	NOT sqlc.arg(has_title_filters)::boolean
	OR p.categorized_job_title = ANY(sqlc.arg(job_categories)::text[])
	OR p.categorized_job_function = ANY(sqlc.arg(job_functions)::text[])
	OR lower(trim(COALESCE(p.categorized_job_title, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR lower(trim(COALESCE(p.categorized_job_function, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR lower(trim(COALESCE(p.role_title, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR p.categorized_job_title ILIKE ANY(sqlc.arg(title_like_patterns)::text[])
	OR p.role_title ILIKE ANY(sqlc.arg(title_like_patterns)::text[])
	OR EXISTS (
		SELECT 1
		FROM jsonb_array_elements(sqlc.arg(title_token_groups_json)::jsonb) AS grp(tokens)
		WHERE jsonb_array_length(grp.tokens) > 0
		  AND NOT EXISTS (
			SELECT 1
			FROM jsonb_array_elements_text(grp.tokens) AS tok(token)
			WHERE COALESCE(p.role_title, '') NOT ILIKE ('%%' || tok.token || '%%')
		  )
	)
)
AND (
	sqlc.arg(company_filter)::text = ''
	OR EXISTS (
		SELECT 1
		FROM parsed_companies c
		WHERE c.id = p.company_id
		AND (
			lower(trim(COALESCE(c.slug, ''))) = sqlc.arg(company_filter)::text
			OR lower(trim(COALESCE(c.name, ''))) = sqlc.arg(company_filter)::text
		)
	)
)
AND (
	NOT sqlc.arg(has_structured_location)::boolean
	OR EXISTS (
		SELECT 1
		FROM unnest(sqlc.arg(us_states)::text[]) AS s(state_name)
		WHERE CAST(p.location_us_states AS jsonb) @> to_jsonb(ARRAY[s.state_name])
	)
	OR EXISTS (
		SELECT 1
		FROM unnest(sqlc.arg(countries)::text[]) AS c(country_name)
		WHERE CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[c.country_name])
	)
)
AND (
	sqlc.arg(has_structured_location)::boolean
	OR cardinality(sqlc.arg(location_patterns)::text[]) = 0
	OR p.location_city ILIKE ANY(sqlc.arg(location_patterns)::text[])
	OR CAST(p.location_us_states AS text) ILIKE ANY(sqlc.arg(location_patterns)::text[])
	OR CAST(p.location_countries AS text) ILIKE ANY(sqlc.arg(location_patterns)::text[])
)
AND (
	cardinality(sqlc.arg(tech_stacks)::text[]) = 0
	OR EXISTS (
		SELECT 1
		FROM unnest(sqlc.arg(tech_stacks)::text[]) AS t(stack_name)
		WHERE CAST(p.tech_stack AS jsonb) @> to_jsonb(ARRAY[t.stack_name])
	)
)
AND (
	cardinality(sqlc.arg(location_types)::text[]) = 0
	OR p.location_type ILIKE ANY(sqlc.arg(location_types)::text[])
)
AND (
	cardinality(sqlc.arg(employment_types)::text[]) = 0
	OR p.employment_type ILIKE ANY(sqlc.arg(employment_types)::text[])
)
AND (
	NOT sqlc.arg(has_created_from)::boolean
	OR p.created_at_source >= sqlc.arg(created_from)::timestamptz
)
AND (
	NOT sqlc.arg(has_created_to)::boolean
	OR p.created_at_source < sqlc.arg(created_to)::timestamptz
)
AND (
	NOT sqlc.arg(has_min_salary)::boolean
	OR (
		(
			COALESCE(p.salary_max_usd, p.salary_min_usd) * CASE
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('yearly', 'year', 'annual') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%year%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annual%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annually%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per year%' THEN 1.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('monthly', 'month') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month-salary%' THEN 12.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('biweekly', 'bi-weekly') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%biweekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%bi-weekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per biweekly%' THEN 26.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('weekly', 'week') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%week%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per week%' THEN 52.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('daily', 'day') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per visit%' THEN 260.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('hourly', 'hour', 'hr') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%hour%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per hour%' THEN 2080.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%minute%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/min%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per min%' THEN 124800.0
				ELSE 1.0
			END
		) >= sqlc.arg(min_salary)::float8
		OR (
			COALESCE(p.salary_max, p.salary_min) * CASE
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('yearly', 'year', 'annual') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%year%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annual%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annually%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per year%' THEN 1.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('monthly', 'month') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month-salary%' THEN 12.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('biweekly', 'bi-weekly') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%biweekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%bi-weekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per biweekly%' THEN 26.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('weekly', 'week') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%week%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per week%' THEN 52.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('daily', 'day') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per visit%' THEN 260.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('hourly', 'hour', 'hr') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%hour%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per hour%' THEN 2080.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%minute%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/min%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per min%' THEN 124800.0
				ELSE 1.0
			END
		) >= sqlc.arg(min_salary)::float8
	)
)
AND (
	NOT sqlc.arg(has_seniority)::boolean
	OR (
		(sqlc.arg(seniority_entry)::boolean AND p.is_entry_level = true)
		OR (sqlc.arg(seniority_junior)::boolean AND p.is_junior = true)
		OR (sqlc.arg(seniority_mid)::boolean AND p.is_mid_level = true)
		OR (sqlc.arg(seniority_senior)::boolean AND p.is_senior = true)
		OR (sqlc.arg(seniority_lead)::boolean AND p.is_lead = true)
	)
)
AND (
	NOT sqlc.arg(has_user)::boolean
	OR (
		CASE sqlc.arg(user_action_filter)::text
			WHEN 'hidden' THEN EXISTS (
				SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
			)
			WHEN 'applied' THEN EXISTS (
				SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true
			)
			WHEN 'not_applied' THEN (
				NOT EXISTS (
					SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true
				)
				AND NOT EXISTS (
					SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
				)
			)
			WHEN 'saved' THEN EXISTS (
				SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_saved = true
			)
			ELSE NOT EXISTS (
				SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
			)
		END
	)
)
ORDER BY
	CASE
		WHEN sqlc.arg(sort_salary)::boolean THEN (
			COALESCE(p.salary_max_usd, p.salary_min_usd) * CASE
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('yearly', 'year', 'annual') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%year%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annual%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annually%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per year%' THEN 1.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('monthly', 'month') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month-salary%' THEN 12.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('biweekly', 'bi-weekly') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%biweekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%bi-weekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per biweekly%' THEN 26.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('weekly', 'week') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%week%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per week%' THEN 52.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('daily', 'day') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per visit%' THEN 260.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('hourly', 'hour', 'hr') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%hour%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per hour%' THEN 2080.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%minute%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/min%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per min%' THEN 124800.0
				ELSE 1.0
			END
		)
	END DESC NULLS LAST,
	CASE
		WHEN sqlc.arg(sort_salary)::boolean THEN (
			COALESCE(p.salary_max, p.salary_min) * CASE
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('yearly', 'year', 'annual') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%year%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annual%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annually%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per year%' THEN 1.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('monthly', 'month') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month-salary%' THEN 12.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('biweekly', 'bi-weekly') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%biweekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%bi-weekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per biweekly%' THEN 26.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('weekly', 'week') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%week%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per week%' THEN 52.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('daily', 'day') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per visit%' THEN 260.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('hourly', 'hour', 'hr') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%hour%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per hour%' THEN 2080.0
				WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%minute%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/min%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per min%' THEN 124800.0
				ELSE 1.0
			END
		)
	END DESC NULLS LAST,
	p.created_at_source DESC,
	p.id DESC
LIMIT sqlc.arg(limit_rows) OFFSET sqlc.arg(offset_rows);

-- name: ListJobsByIDsInOrder :many
SELECT p.id, p.raw_us_job_id, p.role_title, p.job_description_summary, c.name, c.slug, c.tagline, c.profile_pic_url, c.home_page_url, c.linkedin_url, c.employee_range, c.founded_year, c.sponsors_h1b, p.categorized_job_title, p.location_city, p.location_type, p.location_us_states, p.location_countries, p.employment_type, p.salary_min, p.salary_max, p.salary_min_usd, p.salary_max_usd, p.salary_type, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead, p.tech_stack, p.updated_at, p.created_at_source, p.url
FROM parsed_jobs p
LEFT JOIN parsed_companies c ON c.id = p.company_id
WHERE p.id = ANY(sqlc.arg(ids)::bigint[])
ORDER BY array_position(sqlc.arg(ids)::bigint[], p.id);

-- name: GetJobsMetricsFiltered :one
WITH filtered AS (
	SELECT p.id, p.company_id, p.created_at_source
	FROM parsed_jobs p
	WHERE
	(
		NOT sqlc.arg(has_title_filters)::boolean
		OR p.categorized_job_title = ANY(sqlc.arg(job_categories)::text[])
		OR p.categorized_job_function = ANY(sqlc.arg(job_functions)::text[])
	OR lower(trim(COALESCE(p.categorized_job_title, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR lower(trim(COALESCE(p.categorized_job_function, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR lower(trim(COALESCE(p.role_title, ''))) = ANY(sqlc.arg(title_exact_terms)::text[])
	OR p.categorized_job_title ILIKE ANY(sqlc.arg(title_like_patterns)::text[])
	OR p.role_title ILIKE ANY(sqlc.arg(title_like_patterns)::text[])
	OR EXISTS (
		SELECT 1
		FROM jsonb_array_elements(sqlc.arg(title_token_groups_json)::jsonb) AS grp(tokens)
		WHERE jsonb_array_length(grp.tokens) > 0
		  AND NOT EXISTS (
			SELECT 1
			FROM jsonb_array_elements_text(grp.tokens) AS tok(token)
			WHERE COALESCE(p.role_title, '') NOT ILIKE ('%%' || tok.token || '%%')
		  )
	)
)
	AND (
		sqlc.arg(company_filter)::text = ''
		OR EXISTS (
			SELECT 1
			FROM parsed_companies c
			WHERE c.id = p.company_id
			AND (
				lower(trim(COALESCE(c.slug, ''))) = sqlc.arg(company_filter)::text
				OR lower(trim(COALESCE(c.name, ''))) = sqlc.arg(company_filter)::text
			)
		)
	)
	AND (
		NOT sqlc.arg(has_structured_location)::boolean
		OR EXISTS (
			SELECT 1
			FROM unnest(sqlc.arg(us_states)::text[]) AS s(state_name)
			WHERE CAST(p.location_us_states AS jsonb) @> to_jsonb(ARRAY[s.state_name])
		)
		OR EXISTS (
			SELECT 1
			FROM unnest(sqlc.arg(countries)::text[]) AS c(country_name)
			WHERE CAST(p.location_countries AS jsonb) @> to_jsonb(ARRAY[c.country_name])
		)
	)
	AND (
		sqlc.arg(has_structured_location)::boolean
		OR cardinality(sqlc.arg(location_patterns)::text[]) = 0
		OR p.location_city ILIKE ANY(sqlc.arg(location_patterns)::text[])
		OR CAST(p.location_us_states AS text) ILIKE ANY(sqlc.arg(location_patterns)::text[])
		OR CAST(p.location_countries AS text) ILIKE ANY(sqlc.arg(location_patterns)::text[])
	)
	AND (
		cardinality(sqlc.arg(tech_stacks)::text[]) = 0
		OR EXISTS (
			SELECT 1
			FROM unnest(sqlc.arg(tech_stacks)::text[]) AS t(stack_name)
			WHERE CAST(p.tech_stack AS jsonb) @> to_jsonb(ARRAY[t.stack_name])
		)
	)
	AND (
		cardinality(sqlc.arg(location_types)::text[]) = 0
		OR p.location_type ILIKE ANY(sqlc.arg(location_types)::text[])
	)
	AND (
		cardinality(sqlc.arg(employment_types)::text[]) = 0
		OR p.employment_type ILIKE ANY(sqlc.arg(employment_types)::text[])
	)
	AND (
		NOT sqlc.arg(has_created_from)::boolean
		OR p.created_at_source >= sqlc.arg(created_from)::timestamptz
	)
	AND (
		NOT sqlc.arg(has_created_to)::boolean
		OR p.created_at_source < sqlc.arg(created_to)::timestamptz
	)
	AND (
		NOT sqlc.arg(has_min_salary)::boolean
		OR (
			(
				COALESCE(p.salary_max_usd, p.salary_min_usd) * CASE
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('yearly', 'year', 'annual') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%year%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annual%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annually%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per year%' THEN 1.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('monthly', 'month') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month-salary%' THEN 12.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('biweekly', 'bi-weekly') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%biweekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%bi-weekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per biweekly%' THEN 26.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('weekly', 'week') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%week%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per week%' THEN 52.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('daily', 'day') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per visit%' THEN 260.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('hourly', 'hour', 'hr') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%hour%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per hour%' THEN 2080.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%minute%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/min%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per min%' THEN 124800.0
					ELSE 1.0
				END
			) >= sqlc.arg(min_salary)::float8
			OR (
				COALESCE(p.salary_max, p.salary_min) * CASE
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('yearly', 'year', 'annual') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%year%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annual%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%annually%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per year%' THEN 1.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('monthly', 'month') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% mo%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per month%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%month-salary%' THEN 12.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('biweekly', 'bi-weekly') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%biweekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%bi-weekly%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per biweekly%' THEN 26.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('weekly', 'week') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%week%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% wk%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per week%' THEN 52.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('daily', 'day') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per day%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per visit%' THEN 260.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) IN ('hourly', 'hour', 'hr') OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%hour%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '% hr%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per hour%' THEN 2080.0
					WHEN lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%minute%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%/min%' OR lower(trim(COALESCE(p.salary_type, 'yearly'))) LIKE '%per min%' THEN 124800.0
					ELSE 1.0
				END
			) >= sqlc.arg(min_salary)::float8
		)
	)
	AND (
		NOT sqlc.arg(has_seniority)::boolean
		OR (
			(sqlc.arg(seniority_entry)::boolean AND p.is_entry_level = true)
			OR (sqlc.arg(seniority_junior)::boolean AND p.is_junior = true)
			OR (sqlc.arg(seniority_mid)::boolean AND p.is_mid_level = true)
			OR (sqlc.arg(seniority_senior)::boolean AND p.is_senior = true)
			OR (sqlc.arg(seniority_lead)::boolean AND p.is_lead = true)
		)
	)
	AND (
		NOT sqlc.arg(has_user)::boolean
		OR (
			CASE sqlc.arg(user_action_filter)::text
				WHEN 'hidden' THEN EXISTS (
					SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
				)
				WHEN 'applied' THEN EXISTS (
					SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true
				)
				WHEN 'not_applied' THEN (
					NOT EXISTS (
						SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true
					)
					AND NOT EXISTS (
						SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
					)
				)
				WHEN 'saved' THEN EXISTS (
					SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_saved = true
				)
				ELSE NOT EXISTS (
					SELECT 1 FROM user_job_actions uja WHERE uja.user_id = sqlc.arg(user_id)::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true
				)
			END
		)
	)
)
SELECT
	COUNT(id) FILTER (WHERE created_at_source IS NOT NULL AND created_at_source >= sqlc.arg(today_cutoff)::timestamptz)::bigint AS jobs_today,
	COUNT(id) FILTER (WHERE created_at_source IS NOT NULL AND created_at_source >= sqlc.arg(last_hour_cutoff)::timestamptz)::bigint AS jobs_last_hour,
	COUNT(DISTINCT company_id) FILTER (WHERE company_id IS NOT NULL AND created_at_source IS NOT NULL AND created_at_source >= sqlc.arg(today_cutoff)::timestamptz)::bigint AS companies_hiring_now
FROM filtered;
