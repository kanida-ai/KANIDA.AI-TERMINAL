# M2 — DB (Billing Migration) — AUDIT

**Agent:** AuditAgent-M2
**Date:** 2026-06-13 (IST)
**Scope audited:** CONTRACT §1/§2/§6/§7 · MASTER_SPEC "M2 — DB" + §2 invariants · M2-build-log.md
**Artifacts:**
- `backend/power_user/migrations/0001_billing.sql`
- `backend/power_user/migrations/0001_billing_rollback.sql`
- `backend/power_user/db_schema.sql` (appended §211–288)
- `backend/power_user/db_init.py` (edited §122–153, manifest §46–47, §77–80)

**Method:** Static read + CONTRACT diff + `git diff` scope check. Could not execute SQL (no Postgres/sqlite3 in env) — same constraint the build flagged (R7). Idempotency verified by tracing guard logic, not by running.

---

## 1. Contract fidelity (CONTRACT §1.1/§1.2/§1.3)

**Verdict: MATCH.** Every column/type/constraint/index is verbatim.

### §1.1 `power_user_users` additive columns — `0001_billing.sql:27–34`
| Contract | Built | OK |
|---|---|---|
| `billing_plan TEXT DEFAULT 'founding'` | `:28` `ADD COLUMN IF NOT EXISTS billing_plan TEXT DEFAULT 'founding'` | ✅ |
| `razorpay_customer_id TEXT NULL` | `:31` `ADD COLUMN IF NOT EXISTS razorpay_customer_id TEXT` (nullable, no default) | ✅ |
| `subscription_status TEXT DEFAULT 'active'` | `:34` | ✅ |
| CHECK domain `founding/comp/paid/blocked` | `:45–47` guarded `DO` block, exact 4-value list | ✅ |
| relax `google_sub` NOT NULL | `:54–55` `ALTER COLUMN google_sub DROP NOT NULL` | ✅ |

### §1.2 `power_user_subscriptions` — `0001_billing.sql:83–98`
All 9 columns present with correct types: `id BIGSERIAL PK` (`:84`), `user_id BIGINT NOT NULL REFERENCES power_user_users(id)` (`:85–86`), `razorpay_subscription_id TEXT UNIQUE` (`:87`), `plan_code TEXT NOT NULL` (`:88`), `status TEXT NOT NULL` (`:89`), `current_start/current_end TIMESTAMPTZ` (`:90–91`), `created_at/updated_at TIMESTAMPTZ NOT NULL DEFAULT now()` (`:92–93`). Both required indexes present: `ix_pu_subs_user(user_id)` `:95–96`, `ix_pu_subs_rzp_sub(razorpay_subscription_id)` `:97–98`. ✅

### §1.3 `power_user_billing_events` — `0001_billing.sql:103–115`
`id BIGSERIAL PK` (`:104`), `user_id BIGINT` nullable FK (`:105–106`), `razorpay_event_id TEXT UNIQUE NOT NULL` idempotency key (`:107`), `event_type TEXT NOT NULL` (`:108`), `payload JSONB NOT NULL` (`:109`), `received_at TIMESTAMPTZ NOT NULL DEFAULT now()` (`:110`). ✅
Contract specified no index on this table; build added two (`ix_pu_bill_events_user`, `ix_pu_bill_events_type`) — additive, sensible (user lookup + type/time audit scan), not a violation.

**No invented names.** Table/column/index names match CONTRACT exactly. Manifest in `db_init.py` (`:46–47`, `:77–80`) was extended to include the 2 new tables + 4 indices, so the boot self-check actually validates them.

---

## 2. Plan semantics & backfill (§2, INV1)

**Verdict: CORRECT, including the re-run safety the checklist demanded.**

Postgres backfill is 3 statements (`0001_billing.sql:63–79`):
1. `:63–65` `SET billing_plan='founding' WHERE billing_plan IS NULL` — repairs NULLs.
2. `:67–69` `SET subscription_status='active' WHERE subscription_status IS NULL`.
3. `:75–79` the guarantee UPDATE, **scoped** `WHERE razorpay_customer_id IS NULL AND billing_plan NOT IN ('comp','paid','blocked')`.

The scope on statement 3 is the key correctness point: a naive `SET billing_plan='founding'` on all rows would clobber legitimately-`comp`/`paid`/`blocked` users on any re-run. The `NOT IN (...)` guard plus the `razorpay_customer_id IS NULL` predicate means a re-run after real customers exist leaves them untouched. On the first run against the freshly-ported DB (no billing data, all NULL customer ids) every row matches → all become `founding`/`active`, satisfying INV1.

SQLite path mirrors this (`db_init.py:146–153`) but only runs statements 1+2 (the `WHERE ... IS NULL` form). That is itself re-run-safe (only touches NULLs), so the omission of the scoped statement 3 is harmless in dev — after the first backfill no row is NULL, and post-launch comp/paid/blocked rows are never NULL either. Logically equivalent outcome.

---

## 3. Idempotency

**Verdict: SAFE on both dialects.**

**Postgres (run twice):** Every DDL is guarded — `ADD COLUMN IF NOT EXISTS` (`:28,31,34`), `CREATE TABLE IF NOT EXISTS` (`:83,103`), `CREATE INDEX IF NOT EXISTS` (`:95,97,112,114`). The CHECK constraint is the one statement with no native `IF NOT EXISTS`; it is wrapped in a `pg_constraint` catalog lookup `DO` block (`:39–49`) keyed on `conname='power_user_users_billing_plan_check'` — second run finds it and skips. `ALTER COLUMN ... DROP NOT NULL` (`:55`) is naturally idempotent (no-op if already nullable). Backfill UPDATEs are NULL-scoped → no-op on second run. Whole thing in `BEGIN/COMMIT`. ✅

**SQLite (boot twice):** This is the genuinely dangerous path and the build handled it correctly. `db_schema.sql` runs through `executescript()` at every boot (`db_init.py:112`) — a bare `ALTER TABLE ... ADD COLUMN` in that file would throw `duplicate column name` on the 2nd boot and, because it is inside `executescript`, abort the entire schema init and crash dev. The build deliberately kept the 3 ADD COLUMNs **out** of `db_schema.sql` (documented at `db_schema.sql:236–256`) and moved them into `db_init.py:128–139` as per-statement `con.execute()` calls each wrapped in `try/except sqlite3.OperationalError` that re-raises only if `"duplicate column" not in str(e).lower()`. This mirrors the pre-existing `narrative_json` precedent (`db_init.py:115–120`). The 2 new tables use `CREATE TABLE IF NOT EXISTS` in the schema file (`db_schema.sql:259,277`), safe inside executescript. Second boot = clean no-op. ✅

Trace confirms: tables created by executescript → `power_user_users` already exists (CREATE IF NOT EXISTS) → ALTERs run against existing table → backfill → commit. Ordering is sound.

---

## 4. google_sub relaxation

**Verdict: CONFIRMED, non-destructive in both dialects.**

- **PG:** `ALTER COLUMN google_sub DROP NOT NULL` (`:54–55`). Dropping NOT NULL never invalidates an existing row — truly non-destructive.
- **SQLite:** No table rebuild attempted. The dev strategy is documented (`db_schema.sql:224–234`): the existing NOT NULL is kept, and the open-signup path (M5) will write synthetic `google_sub = 'email:' || sha256(lower(email))` so the constraint is never hit. This correctly avoids the risky create→copy→drop→rename rebuild. The actual synthetic write is an M5 responsibility (build R4) — M2 only sets up/documents it, which is the right boundary.

---

## 5. Invariants

**Verdict: ALL HELD.**

- **INV2 (Auto-Trade untouched):** `git diff` confirms nothing under `backend/falcon/` modified. Migration is schema-only. ✅
- **INV6 (power_user code stays in power_user/):** All 4 changed files are under `backend/power_user/`. ✅
- **INV7 (additive only):** `git diff HEAD` shows **zero deletion lines** in both modified files (`db_schema.sql` +78, `db_init.py` +42, both purely additive). The only existing-column change is `google_sub DROP NOT NULL`, which CONTRACT §1.1 explicitly mandates and is non-destructive. No column drops, no type changes, no narrowing. ✅
- **INV4 (IST):** TIMESTAMPTZ in prod; dev timestamp columns are TEXT app-written IST ISO (consistent with the rest of `db_schema.sql`); no machine-clock `now()` in dev. ✅
- **INV1:** covered in §2. ✅

---

## 6. Rollback

**Verdict: CLEAN.** `0001_billing_rollback.sql`:
- Drops both new tables (`:30–31`, `DROP TABLE IF EXISTS`, FK order irrelevant — neither references the other).
- Drops the CHECK constraint (`:35–36`) then the 3 columns (`:38–40`), all `IF EXISTS` → idempotent.
- `google_sub` NOT NULL deliberately **not** restored, with a clear header caveat (`:12–21`) explaining why (rows may already hold NULL/synthetic subjects so `SET NOT NULL` would fail) plus a manual-recovery recipe. Correct and honest. ✅

Note: rollback is Postgres-only. There is no SQLite rollback, but dev DBs are disposable/regenerable from schema, so this is acceptable and consistent with the dev-vs-prod split.

---

## 7. Security / correctness

- **SQL injection:** None. All migration DDL is static. The one string-built log line (`db_init.py:136` `_stmt.split("ADD COLUMN ")[1]`) splits a hardcoded literal for logging only — no user input, no injection surface.
- **Type/FK alignment:** PG `user_id BIGINT` → `power_user_users(id)`. The ported `power_user_users.id` originates as SQLite `INTEGER PK AUTOINCREMENT`; the M1 porting script must land it as `BIGINT`/`BIGSERIAL` in Supabase for the FK type to match. This is an **M1 dependency, not an M2 defect** — flagging for the orchestrator to confirm M1 ports `id` as a 64-bit integer. In dev, `INTEGER`↔`INTEGER` aligns. No mismatch within M2's own artifacts.
- **FK ON DELETE (CONTRACT silent):** Build chose plain FK = `NO ACTION`/`RESTRICT` default in both dialects (build R5). This is the *safe* choice for a billing audit ledger — you do not want deleting a user to silently vaporize their subscription history or webhook idempotency records. CONTRACT did not specify, so this is a defensible literal reading. **Flagged as a design choice, not a violation.** If the orchestrator later wants user-deletion to cascade, that is a follow-up amendment, not an M2 fix.
- **JSONB payload:** correct type in PG (`:109`), TEXT in dev (`:282`) — appropriate.
- **Idempotency key:** `razorpay_event_id UNIQUE NOT NULL` present in both dialects — webhook dedupe surface is correctly enforced at the DB level. ✅

---

## Minor (non-blocking, not RED)

1. **`statements_executed` manifest counter** (`db_init.py:181`) is a rough `;`/`--` heuristic and is now slightly off after the append. Build flagged it (R6); it is informational only, not a correctness signal. Leave or fix at leisure.
2. **SQLite backfill omits the scoped statement-3** equivalent (`db_init.py:146–153` only does the two `IS NULL` UPDATEs). Harmless (proven in §2) but a one-line comment noting "statement 3 unnecessary in dev because NULL-scoped 1+2 already cover it" would aid parity reading.
3. **`db_init.py` edit not in CONTRACT §6** (build R1). It is additive, confined to `power_user/`, and necessary to keep dev booting. Recommend the orchestrator amend §6 to list `db_init.py` under M2 (the CONTRACT already has a parenthetical note anticipating this) — bookkeeping, not a code problem.
4. **No SQLite rollback** — acceptable (dev DB regenerable), noted for completeness.

---

## VERDICT: GREEN

Matches CONTRACT §1/§2 exactly, honors INV1/INV2/INV4/INV6/INV7, idempotent on both dialects, rollback clean with documented google_sub caveat, no injection/type/FK defect within M2's scope. Safe to proceed to M3.

**One cross-module flag for the orchestrator (not an M2 blocker):** confirm M1's porting script lands `power_user_users.id` as a 64-bit integer (`BIGINT`/`BIGSERIAL`) in Supabase so the `BIGINT user_id` FKs in the two new tables type-match. M2's own DDL is internally consistent; this is purely an M1↔M2 handoff check.
