# M2 — DB (Billing Migration) — Build Log

**Agent:** BuildAgent-M2
**Date:** 2026-06-13 (IST)
**Module:** M2 (DB schema migration)
**Spec:** MASTER_SPEC §3 M2 + §2 invariants · CONTRACT §1, §2, §6, §7
**Status:** Built — not executed (no Python/sqlite3 in this environment; validated by reading + CONTRACT matching).

---

## 1. Files changed

| File | Change | Owner (CONTRACT §6) |
|---|---|---|
| `backend/power_user/migrations/0001_billing.sql` | **NEW** — canonical PostgreSQL forward migration | M2 |
| `backend/power_user/migrations/0001_billing_rollback.sql` | **NEW** — Postgres rollback | M2 |
| `backend/power_user/db_schema.sql` | **APPENDED** — SQLite dev mirror (2 new tables + dialect/strategy docs) | M2 |
| `backend/power_user/db_init.py` | **EDITED** — guarded SQLite ALTERs + backfill; manifest entries for the 2 new tables + 4 indices | M2 (necessary support; see Risk R1) |

> `db_init.py` was not named in CONTRACT §6, but editing it is **required** to keep DEV booting (see §4 Dialect decisions / Risk R1). It is `power_user/` code (INV6-safe) and the change is purely additive.

---

## 2. Exact DDL produced

### 2.1 PostgreSQL (`0001_billing.sql`) — canonical / prod

`power_user_users` (additive, INV7):
```
ADD COLUMN IF NOT EXISTS billing_plan TEXT DEFAULT 'founding'
ADD COLUMN IF NOT EXISTS razorpay_customer_id TEXT
ADD COLUMN IF NOT EXISTS subscription_status TEXT DEFAULT 'active'
+ CHECK (billing_plan IN ('founding','comp','paid','blocked'))   -- guarded DO block (no ADD CONSTRAINT IF NOT EXISTS in PG)
ALTER COLUMN google_sub DROP NOT NULL                            -- non-destructive
```

Backfill (INV1):
```
UPDATE ... SET billing_plan='founding'        WHERE billing_plan IS NULL;
UPDATE ... SET subscription_status='active'   WHERE subscription_status IS NULL;
UPDATE ... SET billing_plan='founding', subscription_status='active'
  WHERE razorpay_customer_id IS NULL AND billing_plan NOT IN ('comp','paid','blocked');
```

`power_user_subscriptions` (CONTRACT §1.2):
```
id BIGSERIAL PK · user_id BIGINT NOT NULL FK→power_user_users(id)
razorpay_subscription_id TEXT UNIQUE · plan_code TEXT NOT NULL · status TEXT NOT NULL
current_start TIMESTAMPTZ · current_end TIMESTAMPTZ
created_at TIMESTAMPTZ NOT NULL DEFAULT now() · updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
INDEX ix_pu_subs_user(user_id) · ix_pu_subs_rzp_sub(razorpay_subscription_id)
```

`power_user_billing_events` (CONTRACT §1.3):
```
id BIGSERIAL PK · user_id BIGINT NULL FK→power_user_users(id)
razorpay_event_id TEXT UNIQUE NOT NULL       -- idempotency key
event_type TEXT NOT NULL · payload JSONB NOT NULL · received_at TIMESTAMPTZ NOT NULL DEFAULT now()
INDEX ix_pu_bill_events_user(user_id) · ix_pu_bill_events_type(event_type, received_at)
```

Whole migration wrapped in `BEGIN; ... COMMIT;`. Idempotent via `ADD COLUMN IF NOT EXISTS`, `CREATE TABLE/INDEX IF NOT EXISTS`, and a `pg_constraint` catalog guard for the CHECK.

### 2.2 Rollback (`0001_billing_rollback.sql`)
```
DROP TABLE IF EXISTS power_user_billing_events;
DROP TABLE IF EXISTS power_user_subscriptions;
ALTER TABLE power_user_users DROP CONSTRAINT IF EXISTS power_user_users_billing_plan_check;
DROP COLUMN IF EXISTS billing_plan / razorpay_customer_id / subscription_status;
```
**`google_sub NOT NULL` is intentionally NOT restored** — by rollback time rows may hold NULL or synthetic `email:<sha256>` subjects, so `SET NOT NULL` would fail. Documented in the file header with a manual-recovery note.

### 2.3 SQLite dev (`db_schema.sql` append + `db_init.py`)
- 2 new tables created with `CREATE TABLE IF NOT EXISTS` in the schema file: `BIGSERIAL→INTEGER PK AUTOINCREMENT`, `BIGINT→INTEGER`, `TIMESTAMPTZ→TEXT (IST ISO)`, `JSONB→TEXT`, `DEFAULT now()` dropped (app sets timestamps). FKs to `power_user_users(id)` (which is `INTEGER PK` — types align).
- The 3 `power_user_users` ADD COLUMNs are **NOT** in the schema file; they live in `db_init.py` as guarded `try/except sqlite3.OperationalError("duplicate column")` ALTERs, plus the founding/active backfill `UPDATE`s.

---

## 3. CONTRACT conformance

- Table/column/index names taken verbatim from CONTRACT §1.1/§1.2/§1.3. No invented names.
- `billing_plan` domain `(founding,comp,paid,blocked)` and plan semantics match §2.
- Idempotency key `razorpay_event_id UNIQUE NOT NULL` per §1.3.
- Backfill → all existing rows `founding`/`active` per §1.1 + INV1.
- Files match the §6 ownership rows for M2 (plus the documented `db_init.py` support edit).

---

## 4. Dev-vs-prod dialect decisions

1. **Two artifacts, one logical schema.** Postgres migration is canonical (prod App DB = Supabase). SQLite append exists only to keep local dev booting; types translated as listed in §2.3.
2. **CHECK constraint, prod only.** SQLite `ADD COLUMN` cannot attach a column-domain CHECK without a table rebuild; the `billing_plan` domain is enforced at the application layer in dev. Postgres gets the real CHECK.
3. **`now()` defaults, prod only.** SQLite has no `now()` default of the TIMESTAMPTZ kind we want; dev timestamp columns are plain `TEXT NOT NULL` and the app writes IST ISO strings (INV4), consistent with every other table in `db_schema.sql`.
4. **`google_sub` relaxation, prod only.** Postgres does the real `DROP NOT NULL`. SQLite cannot drop NOT NULL without a risky table rebuild, which the brief forbids — so dev keeps the NOT NULL and the open-signup path (M5) writes a synthetic `google_sub = 'email:' || sha256(lower(email))` so the constraint is never violated. Documented in a SQL comment in `db_schema.sql`.
5. **ADD COLUMN placement (critical).** `db_init.py` runs `db_schema.sql` through `sqlite3.executescript()` at **every boot**. SQLite has no `ADD COLUMN IF NOT EXISTS`; a bare ALTER in the script would throw `duplicate column name` on the 2nd boot and, inside `executescript`, abort the entire schema init → dev crash. So the ALTERs were moved into `db_init.py` as per-statement guarded try/except, mirroring the existing `portfolio_definitions.narrative_json` precedent (same file, lines ~106). This is the single most important correctness decision in this build.

---

## 5. Invariants

- **INV1** — backfill forces every pre-existing user to `founding`/`active` (both PG and SQLite paths).
- **INV2** — nothing under `backend/falcon/*` touched. Schema-only billing work.
- **INV7** — additive only. Sole existing-column change is `google_sub DROP NOT NULL` (non-destructive, allowed by spec). New columns all defaulted/nullable.
- **INV4** — dev timestamps are IST ISO TEXT, app-written; prod is TIMESTAMPTZ. No machine-clock inference.
- **INV6** — all changes in `power_user/`.

---

## 6. Assumptions & open risks (for the audit agent)

- **R1 — `db_init.py` edit is in scope?** CONTRACT §6 lists only the two migration files + the `db_schema.sql` append for M2. I additionally edited `db_init.py` because leaving bare ALTERs in the executescript body would crash dev on the 2nd boot (see §4.5). The edit is additive and confined to `power_user/`. **Audit: confirm this is acceptable, or have the orchestrator amend §6.**
- **R2 — backfill scope on a non-empty prod DB.** The 3rd PG backfill UPDATE is scoped `WHERE razorpay_customer_id IS NULL AND billing_plan NOT IN ('comp','paid','blocked')` so re-running after real customers exist won't revert them. On the **first** run against the freshly-ported DB (no billing data yet) every row matches → all become `founding`. **Audit: confirm the first run targets the ported DB before any signups, which is the M1→M2 ordering in the spec.**
- **R3 — partial-prior-run repair.** If a prior partial run added `billing_plan` *without* a default (not possible with this migration, but defensively), the `WHERE billing_plan IS NULL` UPDATEs repair it. No-op on clean runs.
- **R4 — synthetic `google_sub` is an M5 responsibility.** This migration only *enables* nullable google_sub (prod) / documents the synthetic strategy (dev). The actual `'email:'||sha256` write lives in M5's signup code. **Audit M5 later for the matching write; M2 only sets up the column.**
- **R5 — no FK `ON DELETE` specified.** CONTRACT §1.2/§1.3 don't specify cascade behavior; I used a plain FK (RESTRICT default) in both dialects. Matches the contract literally. Flag if cascade is wanted.
- **R6 — `statements_executed` counter** in `db_init.py` manifest is a rough heuristic (pre-existing) and slightly off after the append; it's informational only, not a correctness signal. Not changed.
- **R7 — could not execute.** No Python/sqlite3 in this env. Validated by reading + CONTRACT match only. **Audit should run `0001_billing.sql` against a copy of the ported DB and boot dev twice** to confirm idempotency + no `duplicate column` crash.
