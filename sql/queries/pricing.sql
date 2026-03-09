-- name: ListActivePricingPlans :many
SELECT code, name, billing_cycle, duration_days, price_usd
FROM pricing_plans
WHERE is_active = true
ORDER BY price_usd ASC;

-- name: GetActivePricingPlanByCode :one
SELECT id, code, name, duration_days, price_usd
FROM pricing_plans
WHERE code = $1
  AND is_active = true
LIMIT 1;

-- name: CreatePaidInternalPayment :one
INSERT INTO pricing_payments
    (user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, checkout_url, provider_payload, paid_at, created_at)
VALUES ($1, $2, 'internal', 'free', 'USD', 0, 'paid', NULL, NULL, '{}', $3, $4)
RETURNING id;

-- name: CreatePendingPayment :one
INSERT INTO pricing_payments
    (user_id, pricing_plan_id, provider, payment_method, currency, amount_minor, status, provider_checkout_id, checkout_url, provider_payload, created_at)
VALUES ($1, $2, $3, $4, 'USD', $5, 'pending', NULL, NULL, '{}', $6)
RETURNING id;

-- name: UpdatePaymentCheckoutInfo :exec
UPDATE pricing_payments
SET provider_checkout_id = $1,
    checkout_url = $2,
    provider_payload = $3
WHERE id = $4;

-- name: GetPaymentForUser :one
SELECT p.id,
       p.user_id,
       p.pricing_plan_id,
       p.status,
       p.provider,
       p.provider_checkout_id,
       p.provider_payload,
       plan.duration_days
FROM pricing_payments p
JOIN pricing_plans plan ON plan.id = p.pricing_plan_id
WHERE p.id = $1
  AND p.user_id = $2
LIMIT 1;

-- name: UpdatePaymentPayloadByID :exec
UPDATE pricing_payments
SET provider_payload = $1
WHERE id = $2;

-- name: UpdatePaymentFailedByID :exec
UPDATE pricing_payments
SET status = 'failed'
WHERE id = $1;

-- name: GetPlanDurationByID :one
SELECT duration_days
FROM pricing_plans
WHERE id = $1
LIMIT 1;

-- name: DeactivateActiveSubscriptionsByUser :exec
UPDATE user_subscriptions
SET is_active = false
WHERE user_id = $1
  AND is_active = true;

-- name: CreateUserSubscriptionActive :exec
INSERT INTO user_subscriptions (user_id, pricing_plan_id, starts_at, ends_at, is_active, created_at)
VALUES ($1, $2, $3, $4, true, $5);

-- name: MarkPaymentPaidByID :exec
UPDATE pricing_payments
SET status = 'paid',
    paid_at = $1
WHERE id = $2;

-- name: GetPaymentByID :one
SELECT id, user_id, pricing_plan_id, status
FROM pricing_payments
WHERE id = $1
LIMIT 1;

-- name: GetPaymentByIDAndUser :one
SELECT id, user_id, pricing_plan_id, status
FROM pricing_payments
WHERE id = $1
  AND user_id = $2
LIMIT 1;

-- name: GetPaymentStatusViewByIDAndUser :one
SELECT p.id,
       plan.code,
       p.provider,
       p.payment_method,
       p.status,
       p.checkout_url,
       p.paid_at,
       p.provider_payload
FROM pricing_payments p
JOIN pricing_plans plan ON plan.id = p.pricing_plan_id
WHERE p.id = $1
  AND p.user_id = $2
LIMIT 1;

-- name: GetLatestSubscriptionWithPlanByUser :one
SELECT s.id, p.id, p.code, p.name, s.starts_at, s.ends_at
FROM user_subscriptions s
JOIN pricing_plans p ON p.id = s.pricing_plan_id
WHERE s.user_id = $1
  AND p.is_active = true
ORDER BY s.ends_at DESC
LIMIT 1;

-- name: DeactivateSubscriptionByID :exec
UPDATE user_subscriptions
SET is_active = false
WHERE id = $1
  AND is_active = true;

-- name: GetLatestPaidPaymentMetaByUserAndPlan :one
SELECT provider, payment_method, provider_payload
FROM pricing_payments
WHERE user_id = $1
  AND pricing_plan_id = $2
  AND status = 'paid'
ORDER BY paid_at DESC, created_at DESC
LIMIT 1;

-- name: GetCancelableCardPaymentByUser :one
SELECT p.id, p.provider, p.provider_payload
FROM pricing_payments p
JOIN user_subscriptions s ON s.pricing_plan_id = p.pricing_plan_id
WHERE s.user_id = $1
  AND s.is_active = true
  AND p.user_id = $2
  AND p.provider IN ('stripe', 'dodo')
  AND p.payment_method = 'card'
  AND p.status = 'paid'
ORDER BY p.paid_at DESC, p.created_at DESC
LIMIT 1;

-- name: GetPaymentForWebhookByPaymentID :one
SELECT pay.id, pay.user_id, pay.pricing_plan_id, plan.duration_days
FROM pricing_payments pay
JOIN pricing_plans plan ON plan.id = pay.pricing_plan_id
WHERE pay.id = $1
LIMIT 1;

-- name: GetPaymentForWebhookByCheckoutID :one
SELECT pay.id, pay.user_id, pay.pricing_plan_id, plan.duration_days
FROM pricing_payments pay
JOIN pricing_plans plan ON plan.id = pay.pricing_plan_id
WHERE pay.provider_checkout_id = $1
LIMIT 1;

-- name: ListRecentDodoCardPayments :many
SELECT pay.id,
       pay.user_id,
       pay.pricing_plan_id,
       plan.duration_days,
       pay.provider_payload
FROM pricing_payments pay
JOIN pricing_plans plan ON plan.id = pay.pricing_plan_id
WHERE pay.provider = 'dodo'
  AND pay.payment_method = 'card'
ORDER BY pay.paid_at DESC NULLS LAST, pay.created_at DESC
LIMIT $1;
