# M1 — Infra — Audit Report

**Agent:** AuditAgent-M1
**Date:** 2026-06-13 (IST)
**Scope:** Audit of BuildAgent-M1 Infra artifacts (read-only on code). Verifies the
legacy-coupling map, R1, porting-script correctness, invariants, runbook
completeness, and the requirements/Dockerfile edits.

**VERDICT: GREEN**

The artifacts are correct, the coupling map is independently confirmed accurate,
R1 is real and properly flagged + gated, and no invariant is breached. Items
below are advisory (carry into M1/M2 fix work), not blockers.

---

## 1. Legacy-coupling map — INDEPENDENTLY CONFIRMED ACCURATE

Verified by grepping `kanida_quant.db`, `kite_tokens`, `falcon_auth_log`,
`POWER_DB_PATH`, `KANIDA_DB_PATH` across the repo and reading the code paths.

### `kite_tokens` — lives in legacy `kanida_quant.db` (must be folded in) ✅ CONFIRMED
- `backend/services/kite_auth.py:54-56` — `DB_PATH = os.environ.get("KANIDA_DB_PATH", …/kanida_quant.db)`.
- `kite_auth.py:79-82` `_conn()` → **raw `sqlite3.connect(DB_PATH)`** (NOT `db.get_conn()`).
- Write: `kite_auth.py:104-107` (`_save_token_to_db` delete+insert today).
- Read: `kite_auth.py:120` (`_load_token_from_db`) — reached by `get_access_token()`
  (line 168) on **every live-tier request** and by Auto-Trade.
- `backend/main.py:41` sets `KANIDA_DB_PATH` default → `kanida_quant.db`.

### `falcon_auth_log` — ALREADY in the App DB (`kanida_universe.db`) ✅ CONFIRMED
The build agent's headline correction to the spec is **correct**. The spec
assumed both tables were in `kanida_quant.db`; only `kite_tokens` is.
- Write path: `scripts/auth_worker.py:150,169` and `:133` pass **`POWER_DB_PATH`**
  to `zerodha_auto_auth.log_attempt()` / `today_already_succeeded()`.
- `backend/power_user/config.py:31-41` `_resolve_power_db_path()` resolves
  `POWER_DB_PATH` to **`kanida_universe.db`** (App DB) and explicitly comments
  (lines 28-30) that it must NOT fall through to `KANIDA_DB_PATH` (the legacy DB).
- Read path: `auth_status.get_status()` is called with `POWER_DB_PATH` at
  `admin_router.py:227,452` and `picks_router.py:272`.
- Schema: `backend/power_user/db_schema.sql:136` creates `falcon_auth_log`;
  `db_init.py:32` lists it in `EXPECTED_TABLES` → created on Postgres at app boot.
- Stale docstrings (`auth_status.py:5`, `zerodha_auto_auth.py:13`,
  `config/AUTH_BOT_SETUP.md`, `architecture.md:96`) still *say*
  `kanida_quant.db.falcon_auth_log`, but the **running code uses `POWER_DB_PATH`**.
  The build log calls this out; the porting script still defensively folds the
  legacy copy in with `ON CONFLICT DO NOTHING` (`migrate_to_supabase.py:149-159`).

**Conclusion:** the coupling map (build-log §2a-2d) is accurate. The single live
coupling that retiring `kanida_quant.db` breaks is **`kite_tokens` only**, exactly
as claimed.

---

## 2. R1 (HIGH) — VALID, REAL, and properly gated

**Confirmed real.** `kite_auth.py` reaches `kite_tokens` through its own
`sqlite3.connect(DB_PATH)` (`:80`), keyed off `KANIDA_DB_PATH` — it does **not**
use the Postgres-aware `backend/db.py:get_conn()` (`db.py:45` `IS_POSTGRES`).
Therefore, even with `DATABASE_URL` set on a Postgres-only host:
- `auth_worker.py --force` → `_save_token_to_db` → SQLite auto-creates a fresh
  empty `kanida_quant.db` on the host and writes the token THERE, not Postgres.
- `get_access_token()` (live-tier read) then reads from that SQLite file →
  Postgres `kite_tokens` stays empty → live tier breaks.

`falcon_auth_log` is **not** affected (it already routes via `POWER_DB_PATH`),
matching the build log's distinction.

**Runbook gating is ADEQUATE.** RUNBOOK §6 (⚠️ callout, lines 175-180) and §7.4
(lines 198-202) explicitly require, on the host, a fresh `kite_tokens` row **in
Postgres** with today's `token_date` AND a `success` row in `falcon_auth_log`
**in Postgres**, then `token_valid:true` from the admin endpoint. If R1 is unfixed
the token lands in SQLite and §7.4 FAILS — the gate cannot be passed by a broken
deploy. §8 (retire) is correctly gated on §7.4+§7.5 passing. This closes the
"broken deploy looks green" hole.

**INV2 flag is valid and important.** `kite_auth` is consumed by 7 Auto-Trade
files (`backend/falcon/trade/services/position_monitor.py`,
`premarket_deployer.py`, `eod_orchestrator.py`, `kite_ticker.py`,
`trade/routers/trade_router.py`, `preflight.py`, `integration_smoke.py`) via
`get_kite_client()` / `get_access_token()`. The R1 fix is in-scope for M1/M2
(these files are under `backend/services` + `backend/power_user`, NOT
`backend/falcon/*`), but **must preserve the `kite_auth` public API** so
Auto-Trade is undisturbed (INV2). The build agent correctly left it as a flagged
change rather than editing blind. **Advisory to the M1/M2 fix agent:** route only
the internal `_conn()`/`_ensure_table()` through `db.get_conn()` and add a
`kite_tokens` DDL to the Postgres schema; do not change function signatures.

---

## 3. Porting script — STATICALLY CORRECT

`scripts/migrate_to_supabase.py` reviewed in full.

- **Right tables ported.** Cross-checked the manifest (lines 93-155) against every
  App-DB table referenced by `backend/power_user/*` SQL and `db_init.py`
  `EXPECTED_TABLES`. All runtime tables are present: `power_user_*` (incl.
  `_watchlists`, `_subscriptions`, `_billing_events`), the 6 `portfolio_*`,
  `falcon_signals_live`/`_signal_runs`/`_features`/`_pattern_*`/`_sectors`,
  `ohlc_daily`/`_weekly`, `universe_master`, `falcon_live_decisions`,
  `falcon_replay_cache`, and the `falcon_trade_*`/`_position_*`/`_premarket_staging`
  operator state.
- **Legacy fold-in is `kite_tokens` + `falcon_auth_log` only** (lines 149-152).
- **14G R&D warehouse skipped** — `POWER_RND_DB_PATH` / `universe_engine/data/db`
  is never referenced. `falcon_outcomes` (the 827k R&D table read at request time
  via `POWER_RND_DB_PATH`) is correctly NOT ported — it stays on machine per
  MASTER_SPEC §1.5 until the M2 precompute. Correct.
- **Idempotent** — every INSERT `ON CONFLICT DO NOTHING` (line 302); offset paging
  resumes after partial port.
- **Chunked** — 1000-row batches (line 354), `ORDER BY rowid` stable paging,
  progress print for >50k tables; bounds memory on the 573M DB.
- **Schema-drift safe (INV7)** — loads the column intersection (lines 286-291), so
  M2 billing columns present on PG but absent on SQLite don't break the load.
- **FK load order correct** — `power_user_users` before `_subscriptions`/`_billing_events`;
  `portfolio_definitions` before positions/history.
- No embedded credentials; `DATABASE_URL` from env only (INV5).
- `--apply-schema` target `backend/power_user/migrations/0001_billing.sql`
  **exists** (verified) so the convenience hook won't no-op silently.

**No table missed; none wrongly included. No correctness bug found.**

Advisory (non-blocking): (a) `--truncate` issues `TRUNCATE … CASCADE` per-table
inside the load loop; because the manifest is parent-before-child, a CASCADE on a
parent truncates not-yet-loaded children (harmless, they load next) — but a
re-run with `--tables` restricted to a child only could be surprising. Document
that `--truncate` is whole-manifest only. (b) `kite_tokens` has no checked-in
Postgres DDL (only `kite_auth._ensure_table()`, which is SQLite and won't run
against PG) — the porting script will **skip** `kite_tokens` if the table is
absent in Postgres (line 265-267). This is the same gap R1 surfaces; the M1/M2
fix must add `kite_tokens` to the Postgres schema. Build log §2d + RUNBOOK §7.4
both already flag this.

---

## 4. Invariants — ALL HONORED

- **INV2 (Auto-Trade untouched):** `git status` shows **no `backend/falcon/*`
  file modified**. `kite_auth.py`, `zerodha_auto_auth.py`, `auth_status.py` are
  **unmodified** (R1 left flagged, not changed). Confirmed. The only changed
  backend files are `db_init.py` + `db_schema.sql` — those are **M2-owned**
  billing artifacts (CONTRACT §6), not M1, and out of M1 audit scope.
- **INV5 (no secrets):** grep of `deploy/`, `RUNBOOK_deploy.md`,
  `migrate_to_supabase.py` for hardcoded secrets returned clean. All creds are
  env-driven; RUNBOOK uses `openssl rand`/placeholders only.
- **INV6 (power_user code not moved):** no `power_user/` source relocated; M1
  touched only `scripts/`, `deploy/`, `docs/launch/`, `requirements.txt`,
  `Dockerfile`. Confirmed.
- **INV7 (additive):** porting script uses column-intersection + `ON CONFLICT`;
  no destructive ALTERs in M1 artifacts.

---

## 5. Runbook completeness — PROVES THE ACCEPTANCE CRITERIA

RUNBOOK §7 maps 1:1 to MASTER_SPEC §M1 acceptance:
- Off-laptop reachable (§7.1, laptop closed + curl).
- `/power/today` from Postgres (§7.2 — checks rows in Supabase
  `falcon_signals_live` AND that `db_url()` is `postgres://`, not a SQLite path —
  this defeats a false-green where the app silently fell back to SQLite).
- Pipeline writes to Postgres (§7.3 — new `falcon_signals_live`/`_signal_runs`
  rows for today).
- Token refresh headless (§7.4 — exit 0 + `success` in `falcon_auth_log` **in
  Postgres** + fresh `kite_tokens` **in Postgres**; gates R1).
- No code reads `kanida_quant.db` in a request path (§7.5 — grep host logs;
  correctly scopes the surviving legacy `/api/*` routers as out-of-product).
- Laptop-closed full trading day (§7.6).

**No verification gap that would hide a broken deploy.** The two ways a broken
deploy could look green are both closed: silent SQLite fallback (caught by §7.2's
`db_url()` check) and token landing in SQLite not Postgres (caught by §7.4's
"in Postgres" requirement). Retire-the-file (§8) is gated behind these.

---

## 6. requirements.txt / Dockerfile — JUSTIFIED AND CORRECT

- `requirements.txt` adds `playwright>=1.44.0` + `pyotp>=2.9.0`. Justified:
  `backend/services/zerodha_auto_auth.py` + `scripts/auth_worker.py` import them;
  absent from the prior file → clean host would fail with `BROWSER_LAUNCH_FAILED`.
- `Dockerfile` adds `RUN playwright install --with-deps chromium` after
  `pip install`. Correct ordering and the right incantation for headless Chromium
  + Linux shared libs. Matches RUNBOOK §6 and `deploy/Procfile`'s `auth_refresh`
  process. R2 (headless-never-run-on-Linux) is honestly carried; §7.4 `--force`
  is the proof gate.

---

## Independent confirmation summary (for the orchestrator)

- **Coupling map: CONFIRMED accurate.** `kite_tokens` = legacy `kanida_quant.db`
  (fold in + retire). `falcon_auth_log` = **already App DB** via `POWER_DB_PATH`
  (`config.py:31-41`), not legacy — build agent's spec correction is right.
- **R1: CONFIRMED real + correctly characterized + adequately gated** by
  RUNBOOK §7.4. Fix is in-scope (non-falcon files) but must preserve `kite_auth`'s
  public API to keep INV2 (7 Auto-Trade consumers).
- **Porting script: no correctness bug, no missing/extra table.**
- **Invariants INV2/INV5/INV6/INV7: intact** in M1 artifacts.
- **No verification gap** that hides a broken deploy.

**VERDICT: GREEN** — no must-fix items. Carry into M1/M2 fix work: (1) implement
R1 (route `kite_auth`/`auth_status` internal connections through `db.get_conn()`
+ add `kite_tokens` Postgres DDL, API-preserving), (2) update the stale
`kanida_quant.db.falcon_auth_log` docstrings to `kanida_universe.db`,
(3) document `--truncate` as whole-manifest only.
