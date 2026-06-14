-- ============================================================================
-- KANIDA.AI Launch — Migration 0001 ROLLBACK: Billing (Razorpay)
-- Target dialect: PostgreSQL (Supabase)
-- Reverses backend/power_user/migrations/0001_billing.sql
--
-- WARNING — DESTRUCTIVE. This DROPs the two billing tables (and all rows in
-- them: subscriptions + the webhook audit/idempotency ledger) and removes the
-- three billing columns from power_user_users (losing billing_plan /
-- razorpay_customer_id / subscription_status for every user). Take a backup
-- before running.
--
-- IMPORTANT — google_sub is NOT restored to NOT NULL.
--   The forward migration relaxed `google_sub` to nullable so open email
--   signup (M5) could write synthetic subjects. By the time you roll back,
--   real rows may already contain NULL google_sub (or synthetic
--   'email:<sha256>' values that were never real Google subs). Re-adding a
--   NOT NULL constraint would FAIL on those NULLs and is therefore deliberately
--   omitted. Relaxing a NOT NULL is non-destructive and safe to leave in place;
--   if you truly need it back, first backfill/clean google_sub, then run
--   `ALTER TABLE power_user_users ALTER COLUMN google_sub SET NOT NULL;`
--   manually.
--
-- Idempotent: IF EXISTS guards make a second run a no-op.
-- ============================================================================

BEGIN;

-- ─── 1. Drop the two new tables ─────────────────────────────────────────────
-- Order is independent (no FKs between them); both FK only to power_user_users.
DROP TABLE IF EXISTS power_user_billing_events;
DROP TABLE IF EXISTS power_user_subscriptions;

-- ─── 2. Drop the billing CHECK constraint, then the three new columns ────────
-- Drop the constraint first (defensive; DROP COLUMN would cascade it anyway).
ALTER TABLE power_user_users
    DROP CONSTRAINT IF EXISTS power_user_users_billing_plan_check;

ALTER TABLE power_user_users DROP COLUMN IF EXISTS billing_plan;
ALTER TABLE power_user_users DROP COLUMN IF EXISTS razorpay_customer_id;
ALTER TABLE power_user_users DROP COLUMN IF EXISTS subscription_status;

-- ─── 3. google_sub NOT NULL is intentionally NOT restored — see header. ──────

COMMIT;
