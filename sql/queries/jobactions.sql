-- name: GetUserJobActionsByJobIDs :many
SELECT parsed_job_id, is_applied, is_saved, is_hidden, updated_at
FROM user_job_actions
WHERE user_id = $1
  AND parsed_job_id = ANY($2::bigint[]);

-- name: CountParsedJobsByID :one
SELECT COUNT(1)
FROM parsed_jobs
WHERE id = $1;

-- name: GetUserJobActionByUserAndJob :one
SELECT parsed_job_id, is_applied, is_saved, is_hidden, updated_at
FROM user_job_actions
WHERE user_id = $1
  AND parsed_job_id = $2
LIMIT 1;

-- name: InsertUserJobActionDefaults :exec
INSERT INTO user_job_actions (user_id, parsed_job_id, is_applied, is_saved, is_hidden, updated_at, created_at)
VALUES ($1, $2, 0, 0, 0, $3, $4);

-- name: UpdateUserJobActionByUserAndJob :exec
UPDATE user_job_actions
SET is_applied = $1,
    is_saved = $2,
    is_hidden = $3,
    updated_at = $4
WHERE user_id = $5
  AND parsed_job_id = $6;

-- name: GetUserJobActionsSummary :one
SELECT COALESCE(SUM(is_applied), 0)::bigint,
       COALESCE(SUM(is_saved), 0)::bigint,
       COALESCE(SUM(is_hidden), 0)::bigint
FROM user_job_actions
WHERE user_id = $1;

-- name: ClearAppliedJobActionsByUser :execrows
UPDATE user_job_actions
SET is_applied = 0,
    updated_at = $1
WHERE user_id = $2
  AND is_applied = 1;

-- name: ClearSavedJobActionsByUser :execrows
UPDATE user_job_actions
SET is_saved = 0,
    updated_at = $1
WHERE user_id = $2
  AND is_saved = 1;

-- name: ClearHiddenJobActionsByUser :execrows
UPDATE user_job_actions
SET is_hidden = 0,
    updated_at = $1
WHERE user_id = $2
  AND is_hidden = 1;
