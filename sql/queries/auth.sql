-- name: GetCurrentUserBySession :one
SELECT u.id, u.email
FROM auth_sessions s
JOIN auth_users u ON u.id = s.user_id
WHERE s.session_token_hash = $1
  AND s.expires_at > $2
LIMIT 1;

-- name: GetAuthUserByEmail :one
SELECT id, email, created_at
FROM auth_users
WHERE email = $1
LIMIT 1;

-- name: CreateAuthUser :one
INSERT INTO auth_users (email, created_at)
VALUES ($1, $2)
RETURNING id;

-- name: ConsumeActiveVerificationCodesByUser :exec
UPDATE auth_verification_codes
SET consumed_at = $1
WHERE user_id = $2
  AND consumed_at IS NULL;

-- name: InsertVerificationCode :exec
INSERT INTO auth_verification_codes (user_id, code_hash, expires_at, created_at)
VALUES ($1, $2, $3, $4);

-- name: GetVerificationCodeIDByUser :one
SELECT id
FROM auth_verification_codes
WHERE user_id = $1
  AND code_hash = $2
  AND consumed_at IS NULL
  AND expires_at > $3
ORDER BY created_at DESC
LIMIT 1;

-- name: MarkVerificationCodeConsumed :exec
UPDATE auth_verification_codes
SET consumed_at = $1
WHERE id = $2;

-- name: GetMagicLinkVerificationCode :one
SELECT avc.id, u.id AS user_id
FROM auth_verification_codes avc
JOIN auth_users u ON u.id = avc.user_id
WHERE avc.code_hash = $1
  AND avc.consumed_at IS NULL
  AND avc.expires_at > $2
ORDER BY avc.created_at DESC
LIMIT 1;

-- name: CountPasswordCredentialsByUser :one
SELECT COUNT(1)
FROM auth_password_credentials
WHERE user_id = $1;

-- name: InsertPasswordCredential :exec
INSERT INTO auth_password_credentials (user_id, password_salt, password_hash, created_at)
VALUES ($1, $2, $3, $4);

-- name: GetPasswordCredentialByUser :one
SELECT password_salt, password_hash
FROM auth_password_credentials
WHERE user_id = $1
LIMIT 1;

-- name: UpdatePasswordCredentialByUser :exec
UPDATE auth_password_credentials
SET password_salt = $1, password_hash = $2
WHERE user_id = $3;

-- name: InsertAuthSession :exec
INSERT INTO auth_sessions (user_id, session_token_hash, expires_at, created_at)
VALUES ($1, $2, $3, $4);

-- name: DeleteAuthSessionByTokenHash :exec
DELETE FROM auth_sessions
WHERE session_token_hash = $1;

-- name: UpsertPricingPlanByCode :exec
INSERT INTO pricing_plans (code, name, billing_cycle, duration_days, price_usd, is_active, created_at)
VALUES ($1, $2, $3, $4, $5, 1, $6)
ON CONFLICT (code) DO UPDATE
SET name = EXCLUDED.name,
    billing_cycle = EXCLUDED.billing_cycle,
    duration_days = EXCLUDED.duration_days,
    price_usd = EXCLUDED.price_usd,
    is_active = 1;

-- name: CountUserSubscriptions :one
SELECT COUNT(1)
FROM user_subscriptions
WHERE user_id = $1;

-- name: GetActivePricingPlanIDByCode :one
SELECT id
FROM pricing_plans
WHERE code = $1
  AND is_active = 1
LIMIT 1;

-- name: InsertUserSubscription :exec
INSERT INTO user_subscriptions (user_id, pricing_plan_id, starts_at, ends_at, is_active, created_at)
VALUES ($1, $2, $3, $4, 1, $5);
