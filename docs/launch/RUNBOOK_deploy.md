# Kanida.AI — M1 Infra Deploy Runbook

**Goal:** move the backend off the laptop onto always-on host (Railway/VPS) with
**Supabase Postgres** as the App DB; port the PROD `kanida_universe.db` into it;
fold in `kite_tokens` + `falcon_auth_log`; retire `kanida_quant.db`; confirm the
daily pipeline and headless Zerodha token refresh fire on the host.

This runbook is written for the **operator** (Pudhuraja). Steps that need live
credentials are flagged **🔑 OPERATOR**. Everything else is already in the repo.

> Invariants in force: INV2 (never touch `backend/falcon/*` logic), INV5 (no
> secrets in any file — all via host env), INV7 (additive migrations only).

---

## 0. Pre-flight (one-time, before you touch any cloud)

- [ ] Confirm the PROD App DB exists locally: `data/db/kanida_universe.db` (~573M).
- [ ] Confirm the legacy DB exists: `data/db/kanida_quant.db` (~83M) — source of
      `kite_tokens` + `falcon_auth_log` fold-in.
- [ ] Confirm the 14G R&D warehouse stays put: `universe_engine/data/db/kanida_universe.db`.
      **It is never ported and `POWER_RND_DB_PATH` is never set on the host.**
- [ ] `pip install psycopg2-binary playwright pyotp` locally if you'll run the
      porting script from the laptop (the script needs `psycopg2`).

---

## 1. 🔑 OPERATOR — Provision Supabase

1. Create a Supabase project (region closest to your users — `ap-south-1`/Mumbai
   for India). Choose a strong DB password.
2. Project → **Settings → Database → Connection string → URI**. Copy it. It looks
   like `postgresql://postgres:<PW>@db.<ref>.supabase.co:5432/postgres`.
   - For pooled/serverless use the **Session pooler** URI (port 6543) if the host
     opens many short connections; the migration script and backend both work
     with either. Prefer the direct 5432 URI for the one-time bulk port.
3. Hold this as `DATABASE_URL` for the next steps. **Do not commit it.**

---

## 2. 🔑 OPERATOR — Generate / collect the other secrets

Gather every value in [`docs/launch/ENV.md`](ENV.md). Minimum for M1:

| Var | How to get it |
|---|---|
| `DATABASE_URL` | from §1 |
| `POWER_JWT_SECRET` | `openssl rand -hex 32` — **CRITICAL**, currently random per boot (`backend/power_user/config.py:59`); must be fixed in prod or every restart logs all users out |
| `KITE_API_KEY` / `KITE_API_SECRET` | Zerodha Kite Connect app |
| `ZERODHA_USERNAME` / `ZERODHA_PASSWORD` / `ZERODHA_TOTP_SECRET` | broker login + TOTP seed (headless auth) |
| `SITE_USER` / `SITE_PASS` | choose — HTTP Basic gate on `/falcon/*` operator routes |

Billing/email vars (`RAZORPAY_*`, `EMAIL_*`) are M3/M8 — not required to bring
the backend up, but set them now if available.

> Note: `zerodha_auto_auth.py` reads `ZERODHA_TOTP_SECRET` (the seed), not a live
> code. ENV.md lists `ZERODHA_PIN` for completeness; the headless bot path uses
> the TOTP seed.

---

## 3. Create the App-DB schema in Postgres

The schema is created by the app's own idempotent initialisers — you do **not**
hand-write DDL. Two options:

**Option A (recommended): boot the backend once against the empty Supabase DB.**
At boot, `main.py` lifespan runs `power_user.db_init.init_power_user_schema()`,
`falcon.db_init.apply_extensions()`, and the M2 billing migration — all
`CREATE IF NOT EXISTS`, so they build the full App-DB schema on Postgres.
Do this in §5 right after setting `DATABASE_URL`; it is safe on an empty DB.

**Option B (offline): run the M2 migration via the porting script.**
```
DATABASE_URL=postgresql://... python scripts/migrate_to_supabase.py --apply-schema --dry-run
```
`--apply-schema` runs `backend/power_user/migrations/0001_billing.sql`. This only
covers the billing tables; the engine + falcon tables still need the app's
db_init (Option A) to exist. Prefer A.

> The engine read-tables (`falcon_signals_live`, `falcon_features`, `ohlc_daily`,
> …) are **created by the offline pipeline / app boot**, not by a checked-in
> Postgres DDL file. Booting the app once (Option A) is the reliable way to get
> every table to exist before the data port.

---

## 4. Port the data (SQLite → Postgres)

Run from the repo root, with `DATABASE_URL` pointing at Supabase.

1. **Dry run** — see counts on both sides, write nothing:
   ```
   DATABASE_URL=postgresql://... python scripts/migrate_to_supabase.py --dry-run
   ```
2. **Real port:**
   ```
   DATABASE_URL=postgresql://... python scripts/migrate_to_supabase.py
   ```
   - It loads, in order: engine read-tables → `power_user_*` → `falcon_trade_*`,
     all from `data/db/kanida_universe.db`; then folds in `kite_tokens` +
     `falcon_auth_log` from `data/db/kanida_quant.db`.
   - Chunked (1000 rows/batch by default) so the 573M PROD DB won't blow memory.
   - Idempotent: every INSERT is `ON CONFLICT DO NOTHING`. Re-running after a
     partial port resumes safely. For a guaranteed-clean reload: add `--truncate`.
   - Column-intersection: extra columns on either side are skipped, so the M2
     billing columns being present on PG (and absent on SQLite) does not break
     the load.
3. Confirm the printed Postgres counts match SQLite. Spot-check in the Supabase
   table editor: `power_user_users`, `falcon_signals_live`, `kite_tokens`.

> If `kanida_quant.db` is missing at port time, the fold-in is skipped with a
> warning and `kite_tokens` stays empty — the first host token refresh (§6)
> repopulates it. Not a failure, but verify §7 before retiring the legacy file.

---

## 5. 🔑 OPERATOR — Deploy the backend to the host

**Railway path (matches existing `railway.json` + `Dockerfile`):**

1. Push the branch / connect the repo to a Railway project.
2. In the **web service** env, set every var from §2 (especially `DATABASE_URL`
   and `POWER_JWT_SECRET`). **Do NOT set `POWER_RND_DB_PATH`** on the host — the
   R&D warehouse stays on the laptop.
3. The build runs the root `Dockerfile`, which now also runs
   `playwright install --with-deps chromium` (needed for §6). First build is
   slower because of the browser download — expected.
4. Deploy. `entrypoint.sh` starts uvicorn. With `DATABASE_URL` set,
   `backend/db.py` flips `IS_POSTGRES=True` automatically and the app uses
   Supabase. (On first boot it also creates any missing schema — Option A in §3.)
5. Add the **cron services** (daily pipeline) — one per job, each with
   `FALCON_JOB` set and the start command `/entrypoint_cron.sh`:
   - `daily_data_refresh` `30 11 * * 1-5`
   - `daily_features` `32 11 * * 1-5`
   - `daily_signals` `35 11 * * 1-5`
   - `weekly_remine` `30 12 * * 0`
   (times are UTC = the IST schedule in `deploy/README.md`.)

**VPS path:** use `deploy/Procfile` with a process manager (systemd/supervisor)
for `web`, and system cron for the pipeline jobs + auth refresh.

---

## 6. 🔑 OPERATOR — Configure the Zerodha token refresh as a host job

On the laptop this was a Windows Scheduled Task running
`scripts/auth_worker.py`. On the host it becomes a scheduled job running the
**same** script (a fresh short-lived process every fire — the design that fixed
the aged-process Playwright failure; see `scripts/auth_worker.py` header).

- **Command:** `cd /app/backend && python3 ../scripts/auth_worker.py`
  (or `python3 /app/scripts/auth_worker.py` — it self-adds `backend/` to path).
- **Schedule:** every 30 min during the IST trading window. The worker
  **self-gates** to weekday 06:00–16:30 IST and exits in <1s outside it, so a
  dumb UTC interval is fine. Simplest: run it every 30 min `*/30 * * * *` and let
  the worker decide; or restrict to `*/30 0-11 * * 1-5` UTC (≈05:30–17:00 IST).
- **Env:** the same `KITE_*` + `ZERODHA_*` + `DATABASE_URL` + `POWER_DB_PATH`
  as the web service. On the host, `POWER_DB_PATH`/`DATABASE_URL` decide where
  `kite_tokens` + `falcon_auth_log` are written — set `DATABASE_URL` so both the
  worker and the web service share the same Supabase token row.
- **Force a proof run** (bypasses the skip gate) to validate the headless path
  end-to-end before trusting the schedule:
  ```
  python3 scripts/auth_worker.py --force
  ```
  Exit 0 + a `success` row in `falcon_auth_log` = headless auth works on Linux.

> **Alternative:** the web service already starts an in-process
> `auth_scheduler` thread. On a stable always-on host (no sleep/wake) that thread
> can carry auth without a separate cron. The standalone worker is more robust
> (fresh process) and lets you `--force`-prove it; prefer the cron job. Running
> BOTH is harmless — the skip gate + `ON CONFLICT` dedupe prevents double work.

> ⚠️ `auth_worker.py` currently writes `kite_tokens` via
> `services/kite_auth.py`, which uses a **raw `sqlite3`** connection keyed off
> `KANIDA_DB_PATH` — it does NOT go through `backend/db.py` / `DATABASE_URL`.
> See §9 Risk R1: on a Postgres-only host this write path needs the token row to
> land in Postgres. Verify §7.4 explicitly; if the token does not appear in the
> Supabase `kite_tokens` table, escalate to the audit/DB agent before go-live.

---

## 7. VERIFICATION checklist (the acceptance gate)

Run these AFTER §5 + §6. All must pass before retiring `kanida_quant.db`.

1. **Backend reachable off-laptop.** Close the laptop. `curl https://<host>/healthz`
   (or the app root) returns 200 from the hosted URL. No 503.
2. **`/power/today` loads from Postgres.** Hit `GET /api/power/picks/today`
   (authenticated) — returns today's picks. Cross-check the same rows exist in
   the Supabase `falcon_signals_live` table. Backend log shows
   `db_url()` = `postgres://…` (not a local SQLite path).
3. **Daily pipeline writes to Postgres.** Trigger `daily_signals` (run the cron
   service once, or `python3 -m falcon.jobs.daily_signals` on the host). Confirm
   new rows land in Supabase `falcon_signals_live` / `falcon_signal_runs` for
   today's date.
4. **Token refresh works HEADLESS on Linux.** Run `auth_worker.py --force` on the
   host (§6). Confirm: exit 0, a `success` row in `falcon_auth_log` **in
   Postgres**, AND a fresh row in `kite_tokens` **in Postgres** with today's
   `token_date`. Then hit the admin auth-status endpoint — `token_valid: true`.
   (This is the highest-risk check — see R1.)
5. **No code still reads `kanida_quant.db` in a request path.** With the host on
   Postgres, exercise `/power/*` and the live-tier routes; grep the host logs for
   any open of a `kanida_quant.db` path. Expected: none from `/power/*`. (The
   legacy `/api/quant`, `/api/live`, `/api/backtest`, `/api/universe`,
   `/api/swing` routes still reference it — see the coupling map; they are
   out-of-product legacy and are addressed by retiring the file, not by M1 code
   changes.)
6. **Laptop can be closed without a 503.** Leave the host running, laptop shut,
   for one full trading day. Daily pipeline + token refresh both fire on the host.

---

## 8. Retire `kanida_quant.db`

Only after §7.4 + §7.5 pass:

- [ ] `kite_tokens` + `falcon_auth_log` confirmed present and updating in Postgres.
- [ ] No `/power/*` request opens `kanida_quant.db` in host logs.
- [ ] Remove the legacy DB from the deploy image (drop the bundle copy in the
      `Dockerfile` + the `entrypoint.sh` seed of `kanida_quant.db`). **This is a
      follow-up edit gated on the audit — do not do it until verification is
      green**, because `entrypoint.sh` still defaults `DB_PATH` to it.
- [ ] Delete the 5 stale worktree copies under `.claude/worktrees/**/kanida_quant.db`.

> The legacy `/api/quant|live|backtest|execution|swing|universe` routers still
> open `kanida_quant.db`. They are NOT part of the Stage-1 product and are not
> gated by the paywall. Retiring the file means those endpoints return empty /
> error — acceptable per MASTER_SPEC §1.5 (`live_opportunities` is dropped).
> If any must survive, that is a separate decision, not an M1 deliverable.

---

## 9. Risks carried into audit

- **R1 (HIGH) — token write path bypasses `DATABASE_URL`.**
  `services/kite_auth.py` + `services/auth_status.py` + `zerodha_auto_auth.py`
  use raw `sqlite3` against `KANIDA_DB_PATH`/`POWER_DB_PATH`, not the
  Postgres-aware `backend/db.py`. On a Postgres-only host these writes/reads need
  to target Postgres. **`falcon_auth_log` already works** because the scheduler
  passes `POWER_DB_PATH` and the app sets that to the App DB — but on the host
  `POWER_DB_PATH` is a SQLite path unless overridden, while the product reads via
  `DATABASE_URL`. **The token + auth-log read/write code is `kite_auth` /
  `auth_status` (NOT `backend/falcon/*`), so M1 may modify it** to route through
  `backend/db.py`. This was left as a flagged change for the audit/DB agent
  rather than done blind, because it touches the live token path that Auto-Trade
  depends on. **§7.4 is the gate that catches it.**
- **R2 (MED) — Playwright headless on the host.** New `requirements.txt` +
  `Dockerfile` lines add Chromium, but this has never run on the target Linux
  image. First `--force` run (§7.4) is the proof. If it fails with
  `BROWSER_LAUNCH_FAILED`, the `--with-deps` install or a missing font/lib is the
  cause.
- **R3 (MED) — SQLite→PG dialect on big tables.** `ohlc_daily` is large and has a
  `source` CHECK; the porting script filters `yfinance` rows. Other tables may
  carry SQLite-only types; the port adapts `bytes→text` only. Watch the port log
  for the first failing batch (it stops on error, by design).
- **R4 (LOW) — schema must pre-exist.** The porting script loads data only; it
  skips any table not present in Postgres. If §3 Option A wasn't done, tables
  silently skip. Always run §4 step 1 (dry-run) and confirm the table list.
- **R5 (LOW) — `POWER_JWT_SECRET` not set.** If the operator forgets it, the app
  boots with a random per-process key and every restart logs users out. Make it
  a required env in the host service config.

---

## Appendix — env var quick reference (host web service)

Required to come up on Postgres:
```
DATABASE_URL=postgresql://…           # Supabase
POWER_JWT_SECRET=<openssl rand -hex 32>
KITE_API_KEY=…  KITE_API_SECRET=…
ZERODHA_USERNAME=…  ZERODHA_PASSWORD=…  ZERODHA_TOTP_SECRET=…
SITE_USER=…  SITE_PASS=…
# DO NOT SET on host:  POWER_RND_DB_PATH  (R&D warehouse is laptop-only)
```
Full manifest + secret/non-secret flags: [`docs/launch/ENV.md`](ENV.md).
