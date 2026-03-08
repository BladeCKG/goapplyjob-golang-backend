-- name: CreateEmployerOrganization :one
INSERT INTO employer_organizations (name, created_by_user_id, created_at)
VALUES ($1, $2, $3)
RETURNING id;

-- name: CreateEmployerOrganizationOwnerMembership :exec
INSERT INTO employer_organization_members (organization_id, user_id, role, created_at)
VALUES ($1, $2, 'owner', $3);

-- name: ListEmployerOrganizationsByUser :many
SELECT o.id, o.name, m.role
FROM employer_organizations o
JOIN employer_organization_members m ON m.organization_id = o.id
WHERE m.user_id = $1
ORDER BY o.created_at DESC, o.id DESC;

-- name: CreateEmployerJobDraft :one
INSERT INTO employer_jobs (
    organization_id, created_by_user_id, status, title, department, description, requirements, benefits,
    employment_type, location_type, locations_json, seniority, tech_stack, apply_url, apply_email,
    salary_currency, salary_period, salary_min, salary_max, posting_fee_usd, posting_fee_status, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15,
    $16, $17, $18, $19, $20, 'unpaid', $21, $22
) RETURNING id;

-- name: UpdateEmployerJobSlug :exec
UPDATE employer_jobs
SET slug = $1,
    updated_at = $2
WHERE id = $3;

-- name: ListEmployerJobIDsByUser :many
SELECT j.id
FROM employer_jobs j
JOIN employer_organization_members m ON m.organization_id = j.organization_id
WHERE m.user_id = $1
  AND ($2 = 0 OR j.organization_id = $2)
  AND ($3 = '' OR j.status = $3)
ORDER BY j.updated_at DESC, j.id DESC;

-- name: UpdateEmployerJobPatch :exec
UPDATE employer_jobs
SET title = COALESCE($1, title),
    department = COALESCE($2, department),
    description = COALESCE($3, description),
    requirements = COALESCE($4, requirements),
    benefits = COALESCE($5, benefits),
    employment_type = COALESCE($6, employment_type),
    location_type = COALESCE($7, location_type),
    locations_json = COALESCE($8, locations_json),
    seniority = COALESCE($9, seniority),
    tech_stack = COALESCE($10, tech_stack),
    apply_url = COALESCE($11, apply_url),
    apply_email = COALESCE($12, apply_email),
    salary_currency = COALESCE($13, salary_currency),
    salary_period = COALESCE($14, salary_period),
    salary_min = COALESCE($15, salary_min),
    salary_max = COALESCE($16, salary_max),
    status = $17,
    updated_at = $18
WHERE id = $19;

-- name: MarkEmployerJobPostingFeePaid :exec
UPDATE employer_jobs
SET posting_fee_status = 'paid',
    posting_fee_paid_at = $1,
    updated_at = $2
WHERE id = $3;

-- name: UpdateEmployerJobStatusPublished :exec
UPDATE employer_jobs
SET status = $1,
    published_at = COALESCE(published_at, $2),
    expires_at = COALESCE(expires_at, $3),
    closed_at = NULL,
    updated_at = $4
WHERE id = $5;

-- name: UpdateEmployerJobStatusClosed :exec
UPDATE employer_jobs
SET status = $1,
    closed_at = $2,
    updated_at = $3
WHERE id = $4;

-- name: UpdateEmployerJobStatusSimple :exec
UPDATE employer_jobs
SET status = $1,
    updated_at = $2
WHERE id = $3;

-- name: GetOwnerEmployerOrganizationByUser :one
SELECT organization_id, role
FROM employer_organization_members
WHERE user_id = $1
  AND role = 'owner'
ORDER BY id ASC
LIMIT 1;

-- name: GetEmployerOrganizationMemberRole :one
SELECT role
FROM employer_organization_members
WHERE organization_id = $1
  AND user_id = $2
LIMIT 1;

-- name: GetEmployerJobForMemberCheck :one
SELECT id, organization_id, status, posting_fee_status
FROM employer_jobs
WHERE id = $1
LIMIT 1;

-- name: InsertEmployerJobAuditEvent :exec
INSERT INTO employer_job_audit_events (employer_job_id, actor_user_id, event_type, detail_json, created_at)
VALUES ($1, $2, $3, $4, $5);

-- name: GetEmployerJobByID :one
SELECT id,
       organization_id,
       status,
       title,
       slug,
       department,
       description,
       requirements,
       benefits,
       employment_type,
       location_type,
       locations_json,
       seniority,
       tech_stack,
       apply_url,
       apply_email,
       salary_currency,
       salary_period,
       salary_min,
       salary_max,
       posting_fee_usd,
       posting_fee_status,
       posting_fee_paid_at,
       published_at,
       closed_at,
       expires_at,
       created_at,
       updated_at
FROM employer_jobs
WHERE id = $1
LIMIT 1;
