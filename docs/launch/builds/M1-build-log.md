# M1 — Infra — Build Log

**Agent:** BuildAgent-M1
**Date:** 2026-06-13 (IST)
**Scope:** Infra artifacts + deploy runbook for moving the backend off the laptop
onto a hosted host with Supabase Postgres as the App DB. No live cloud
provisioning (no credentials, no Python installed) — those are operator handoffs.

---

## 1. Files written / changed

| File | Change |
|---|---|
| `scripts/migrate_to_supabase.py` | **Rewritten** to port the App-DB tables from `kanida_universe.db` (PROD) and fold in `kite_tokens` + `falcon_auth_log` from `kanida_quant.db`. (Old version targeted the legacy `kanida_quant.db` R&D schema + a non-existent `db/migrations/0001_initial.sql`.) |
| `deploy/README.md` | **New** — host config index, 3-process model, image prerequisites, what stays on the laptop. References root Dockerfile (not duplicated). |
| `deploy/Procfile` | **New** — process defs for VPS/Heroku-style hosts (web + 4 cron jobs + auth refresh). |
| `docs/launch/RUNBOOK_deploy.md` | **New** — full operator runbook: provision Supabase, set env, port data, deploy, host-side token refresh, verification checklist, risks. |
| `requirements.txt` | **Edited** — added `playwright` + `pyotp` (the headless auth path imports them; they were missing → would fail on a clean host). |
| `Dockerfile` | **Edited** — added `playwright install --with-deps chromium` so headless Chromium + libs exist on Linux. |
| `docs/launch/builds/M1-build-log.md` | **New** — this file. |

INV2 honored: **no `backend/falcon/*` file was modified.** INV5 honored: **no
secrets in any file** — the porting script reads `DATABASE_URL` from env only.
INV6 honored: no power_user code moved.

---

## 2. Legacy-coupling map (the key finding)

**Search terms:** `kanida_quant.db`, `KANIDA_DB_PATH`, `kite_tokens`,
`falcon_auth_log`.

### 2a. The headline correction

The spec assumed BOTH `kite_tokens` AND `falcon_auth_log` still live in
`kanida_quant.db`. **Only `kite_tokens` does.** `falcon_auth_log` already lives
in the **App DB** (`kanida_universe.db`): it is created by
`backend/power_user/db_schema.sql`, registered in `power_user/db_init.py`, and
written by `auth_scheduler` / `auth_worker` via **`POWER_DB_PATH`** (→
`kanida_universe.db`), not the legacy DB. Several docstrings still *say*
`kanida_quant.db.falcon_auth_log`, but the running code passes `POWER_DB_PATH`.
The porting script still folds `falcon_auth_log` in defensively (ON CONFLICT
dedupes), but the live coupling to retire is **`kite_tokens` only**.

### 2b. LIVE table read/write paths (must be folded into the App DB)

| file:line | Table | R/W | In a request path? |
|---|---|---|---|
| `backend/services/kite_auth.py:88` | `kite_tokens` | W (CREATE) | scheduler/worker |
| `backend/services/kite_auth.py:104-107` | `kite_tokens` | W (delete+insert today) | token refresh |
| `backend/services/kite_auth.py:120` | `kite_tokens` | R (today's token) | **YES — every live-tier req** via `get_access_token()` |
| `backend/services/zerodha_auto_auth.py:389-413` | `kite_tokens` | W (via `_save_token_to_db`) | auth attempt |
| `universe_engine/scripts/refresh_kite_token.py:72` | `kite_tokens` | W (manual operator refresh) | laptop one-off |
| `backend/services/zerodha_auto_auth.py:426-437` | `falcon_auth_log` | W | passed `POWER_DB_PATH` → **App DB already** |
| `backend/services/zerodha_auto_auth.py:482` | `falcon_auth_log` | R (`today_already_succeeded`) | App DB |
| `backend/power_user/services/auth_status.py:71-83` | `falcon_auth_log` | R | App DB (admin widget + degradation banner) |
| `backend/power_user/services/auth_status.py:164` | `power_user_magic_links` | R | App DB |

> `kite_auth.py` DB path = `os.environ["KANIDA_DB_PATH"]`, which `backend/main.py`
> sets to `kanida_quant.db`. This is the ONE live coupling that retiring
> `kanida_quant.db` breaks unless the write/read is routed to the App DB. **This
> is module-owned by `power_user`/`services` (NOT `falcon/*`), so M1/M2 may fix
> it.** Flagged as Risk R1, gated by RUNBOOK §7.4. Left as a flagged change (not
> done blind) because it is the token path Auto-Trade depends on.

### 2c. LEGACY / out-of-product code that references `kanida_quant.db`

These are mounted under `/api/*` (NOT `/power/*`), read legacy tables
(`live_opportunities`, `pattern_library`, `trade_log`, …), and are **dropped /
retired with the file** per MASTER_SPEC §1.5. **Mapped, not modified.**

| file:line | Table(s) | R/W |
|---|---|---|
| `backend/routers/quant_router.py:30` | `pattern_library`, `live_opportunities` | R |
| `backend/routers/live_router.py:32` | `live_opportunities` | R |
| `backend/routers/backtest_router.py:20` | legacy backtest | R |
| `backend/routers/execution_router.py:24` | `execution_log` | R |
| `backend/routers/swing_router.py:28` | legacy swing | R |
| `backend/routers/universe_router.py:35` | `universe`, `instruments` | R |
| `backend/services/data_freshness.py:14` | legacy freshness probe | R |
| `backend/services/opportunity_explainer.py:15` | `live_opportunities` | R |
| `backend/services/order_service.py:27` | `trade_log` (orders router NOT mounted) | R/W |
| `backend/services/stock_context.py:15` | legacy context | R |
| `backend/main.py:39-43` | sets `KANIDA_DB_PATH` default → `kanida_quant.db` | env |
| `backend/db.py:28` | `KANIDA_DB_PATH` default | env |
| `backend/falcon/config.py:20` | `LEGACY_DB` = `kanida_quant.db` (read by `falcon/db.py:connect_legacy`) | INV2 — do not touch |
| `entrypoint.sh:4,6` + `Dockerfile:24` | bundles/seeds `kanida_quant.db` | deploy |
| data ingest + `_archive/**` + `scripts/*walk*/*oos*` | `ohlc_daily` etc. in legacy DB | offline/archived |

> `backend/falcon/db.py:connect_legacy()` opens `LEGACY_DB`. Grep shows **no
> live caller** uses it for `kite_tokens` (the live token path is `kite_auth.py`,
> outside falcon). It is INV2-protected regardless and was not touched.

### 2d. Where the live tables' schemas are defined

- `kite_tokens` — created on the fly by `kite_auth.py:_ensure_table()` (5 cols:
  `id, access_token, token_date, set_by, created_at`). No checked-in DDL beyond
  that. **M2/migration should add it to the Postgres schema** so the host has it
  before the first refresh (RUNBOOK §3 / §7.4).
- `falcon_auth_log` — `backend/power_user/db_schema.sql:136` (already App-DB,
  created by `db_init` on Postgres at boot).

---

## 3. What the porting script now covers

`scripts/migrate_to_supabase.py` (rewritten):

- **Two sources:** PROD App DB (`kanida_universe.db`, override `--app-db`) +
  legacy (`kanida_quant.db`, override `--legacy-db`).
- **App-DB manifest** (enumerated from real SQL in `power_user/*` + `falcon/*`):
  - Engine read-tables: `universe_master`, `falcon_sectors`, `ohlc_daily`,
    `ohlc_weekly`, `falcon_features`, `falcon_pattern_candidates`,
    `falcon_promoted_patterns`, `falcon_pattern_taxonomy`, `falcon_signals_live`,
    `falcon_signal_runs`, `falcon_notifications_out`, `falcon_top10_audit`,
    `falcon_kv_store`, `falcon_kv_trades`.
  - Power-User: `power_user_users`, `_invite_codes`, `_waitlist`, `_watchlists`,
    `_request_log`, `_push_subscriptions`, `_magic_links`, `_subscriptions`,
    `_billing_events`, `falcon_live_decisions`, `falcon_replay_cache`, and the 6
    `portfolio_*` persona tables.
  - Falcon trade/operator state: `falcon_engine_config`, `_trail_config`,
    `_trade_runs`, `_trade_orders`, `_trade_events`, `_position_state`,
    `_position_first_seen`, `_premarket_staging`, `_eod_runs`, `_job_runs`.
- **Legacy fold-in:** `kite_tokens` + `falcon_auth_log` only. Everything else in
  `kanida_quant.db` is intentionally NOT ported (dropped/retired).
- **Skips the 14G R&D warehouse** entirely (never references
  `universe_engine/data/db/*`).
- **Idempotent:** every INSERT `ON CONFLICT DO NOTHING`; re-run resumes safely.
  `--truncate` for a clean reload (CASCADE, children handled by load order).
- **Chunked:** default 1000 rows/batch, stable `ORDER BY rowid` paging, progress
  print for >50k-row tables (bounds memory on the 573M DB).
- **Robust to schema drift (INV7):** loads only the **intersection** of SQLite
  and Postgres columns per table, so M2 billing columns present on PG but absent
  on SQLite (and vice-versa) don't break the load. Skips any table absent in PG.
- **`--apply-schema`** convenience hook runs the M2 billing migration; the
  authoritative way to create the full schema is booting the app once (Option A).
- **`--dry-run`** prints both-side counts, writes nothing.

What changed vs. the prior script: it no longer targets the legacy R&D table set
or `db/migrations/0001_initial.sql`; it targets the App-DB manifest, adds the
legacy fold-in, column-intersection safety, `--truncate`, progress output, and
WITHOUT-ROWID handling.

---

## 4. OPERATOR-ONLY steps that still need live credentials (handoffs)

These are NOT failures — they require Supabase/host credentials this agent does
not have, and Python (not installed locally). Detailed in RUNBOOK_deploy.md.

1. **Provision Supabase** → obtain `DATABASE_URL` (RUNBOOK §1).
2. **Generate/collect secrets** — `POWER_JWT_SECRET` (`openssl rand -hex 32`),
   `KITE_*`, `ZERODHA_*`, `SITE_USER/PASS` (RUNBOOK §2).
3. **Create schema on Postgres** — boot the app once against the empty DB
   (RUNBOOK §3, Option A).
4. **Run the porting script** with `DATABASE_URL` set — dry-run then real
   (RUNBOOK §4). Requires `pip install psycopg2-binary` locally.
5. **Deploy to host** — set env (incl. `DATABASE_URL`, `POWER_JWT_SECRET`),
   build (now installs Chromium), start web + cron services (RUNBOOK §5).
6. **Configure the Zerodha token refresh host job** → `scripts/auth_worker.py`
   on a 30-min schedule; prove with `--force` (RUNBOOK §6).
7. **Run the verification checklist** (RUNBOOK §7) — especially §7.4 (headless
   token write lands in Postgres) which gates Risk R1.
8. **Retire `kanida_quant.db`** only after verification is green (RUNBOOK §8).

---

## 5. Risks carried into audit

- **R1 (HIGH)** — `kite_auth.py` / `auth_status.py` write/read `kite_tokens` +
  `falcon_auth_log` via raw `sqlite3` on `KANIDA_DB_PATH`/`POWER_DB_PATH`, NOT
  through `backend/db.py`/`DATABASE_URL`. On a Postgres-only host the token must
  land in Postgres or the live-tier read breaks. This code is `power_user`/
  `services` (NOT `falcon/*`), so it is in-scope for M1/M2 to fix — left flagged
  + gated by §7.4 rather than changed blind, because it is the Auto-Trade token
  path. **Recommend the DB/audit agent route these through `db.get_conn()` and
  add `kite_tokens` to the Postgres schema.**
- **R2 (MED)** — Playwright headless has never run on the target Linux image;
  first `--force` run is the proof.
- **R3 (MED)** — SQLite→PG type/CHECK differences on big tables (`ohlc_daily`);
  the port stops on the first failing batch by design.
- **R4 (LOW)** — porting script loads data only; tables missing in PG silently
  skip. Run the dry-run and confirm the table list first.
- **R5 (LOW)** — forgetting `POWER_JWT_SECRET` → users logged out on every
  restart. Mark it required in host config.
- **R6 (LOW)** — `entrypoint.sh` still defaults `DB_PATH` to `kanida_quant.db`
  and the Dockerfile still bundles it. Removing them is a §8 follow-up gated on a
  green audit (not done now, to avoid breaking the legacy operator surface
  mid-migration).
