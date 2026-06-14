-- ============================================================================
-- KANIDA.AI Launch — Migration 0001: Billing (Razorpay)
-- Target dialect: PostgreSQL (Supabase — the production App DB)
-- Module: M2 (DB)  |  CONTRACT §1, §2  |  MASTER_SPEC §3 M2
--
-- Purpose:
--   Add billing columns to power_user_users; create subscriptions + billing
--   events tables; relax google_sub to nullable (open email signup, M5);
--   backfill every existing user to billing_plan='founding' (INV1).
--
-- Properties:
--   * Additive only (INV7). The only ALTER to an existing column is the
--     NON-destructive `google_sub DROP NOT NULL`.
--   * Idempotent — safe to re-run. Uses IF NOT EXISTS / catalog guards / DO
--     blocks so a second run is a no-op.
--   * Does NOT touch anything under backend/falcon/ (INV2 — schema-only here).
--   * All timestamps TIMESTAMPTZ, written in IST (+05:30) by the app (INV4).
--
-- Run as a single transaction.
-- ============================================================================

BEGIN;

-- ─── 1. power_user_users: additive billing columns ──────────────────────────
-- CONTRACT §1.1. All defaulted/nullable so existing rows stay valid (INV7).

ALTER TABLE power_user_users
    ADD COLUMN IF NOT EXISTS billing_plan TEXT DEFAULT 'founding';

ALTER TABLE power_user_users
    ADD COLUMN IF NOT EXISTS razorpay_customer_id TEXT;

ALTER TABLE power_user_users
    ADD COLUMN IF NOT EXISTS subscription_status TEXT DEFAULT 'active';

-- CHECK constraint for billing_plan domain. Added via guarded DO block because
-- Postgres has no `ADD CONSTRAINT IF NOT EXISTS`; the catalog lookup makes the
-- whole migration idempotent.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'power_user_users_billing_plan_check'
    ) THEN
        ALTER TABLE power_user_users
            ADD CONSTRAINT power_user_users_billing_plan_check
            CHECK (billing_plan IN ('founding', 'comp', 'paid', 'blocked'));
    END IF;
END$$;

-- ─── 2. Relax google_sub to nullable ────────────────────────────────────────
-- Open email signup (M5) has no Google sub. Non-destructive: dropping NOT NULL
-- never invalidates an existing row. CONTRACT §1.1.
ALTER TABLE power_user_users
    ALTER COLUMN google_sub DROP NOT NULL;

-- ─── 3. Backfill existing users → founding/active (INV1) ────────────────────
-- New columns are DEFAULTed for rows inserted *after* this migration. For rows
-- that pre-existed the ADD COLUMN, Postgres already stamped them with the
-- column default ('founding'/'active'), but we set them explicitly so the
-- intent is auditable and so any NULLs (e.g. from a prior partial run that
-- added the column without a default) are repaired. Idempotent.
UPDATE power_user_users
   SET billing_plan = 'founding'
 WHERE billing_plan IS NULL;

UPDATE power_user_users
   SET subscription_status = 'active'
 WHERE subscription_status IS NULL;

-- Guarantee: every pre-existing row is 'founding' + 'active'. We scope this to
-- rows that have NO Razorpay linkage and NO non-default status, so re-running
-- after real paid users exist does NOT clobber them. On the very first run
-- (clean ported DB) all rows match and become founding.
UPDATE power_user_users
   SET billing_plan        = 'founding',
       subscription_status = 'active'
 WHERE razorpay_customer_id IS NULL
   AND billing_plan NOT IN ('comp', 'paid', 'blocked');

-- ─── 4. power_user_subscriptions (NEW) ──────────────────────────────────────
-- CONTRACT §1.2.
CREATE TABLE IF NOT EXISTS power_user_subscriptions (
    id                        BIGSERIAL    PRIMARY KEY,
    user_id                   BIGINT       NOT NULL
                              REFERENCES power_user_users(id),
    razorpay_subscription_id  TEXT         UNIQUE,
    plan_code                 TEXT         NOT NULL,      -- 'monthly_999' at launch
    status                    TEXT         NOT NULL,      -- Razorpay sub states
    current_start             TIMESTAMPTZ,
    current_end               TIMESTAMPTZ,               -- access valid until this
    created_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_pu_subs_user
    ON power_user_subscriptions(user_id);
CREATE INDEX IF NOT EXISTS ix_pu_subs_rzp_sub
    ON power_user_subscriptions(razorpay_subscription_id);

-- ─── 5. power_user_billing_events (NEW) ─────────────────────────────────────
-- CONTRACT §1.3. razorpay_event_id is the idempotency key — UNIQUE + NOT NULL
-- so duplicate webhook deliveries dedupe on insert.
CREATE TABLE IF NOT EXISTS power_user_billing_events (
    id                  BIGSERIAL    PRIMARY KEY,
    user_id             BIGINT
                        REFERENCES power_user_users(id),  -- nullable: event may arrive pre-link
    razorpay_event_id   TEXT         UNIQUE NOT NULL,      -- idempotency key
    event_type          TEXT         NOT NULL,             -- e.g. subscription.activated
    payload             JSONB        NOT NULL,             -- raw event
    received_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_pu_bill_events_user
    ON power_user_billing_events(user_id);
CREATE INDEX IF NOT EXISTS ix_pu_bill_events_type
    ON power_user_billing_events(event_type, received_at);

COMMIT;

-- ============================================================================
-- Verification (run manually post-migrate; not part of the transaction):
--   SELECT billing_plan, subscription_status, count(*)
--     FROM power_user_users GROUP BY 1, 2;   -- expect all founding/active
--   \d power_user_users                       -- google_sub now nullable
--   \d power_user_subscriptions
--   \d power_user_billing_events
-- ============================================================================
