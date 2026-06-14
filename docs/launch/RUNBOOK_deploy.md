# Kanida.AI — Phase 1 Deploy Runbook (Path A: SQLite on a cloud volume)

**Goal:** get the product **off the laptop** and **charging**, with the **least code risk**.
App + the whole 573M SQLite DB move to an always-on host with a **persistent volume**.
**No Supabase / no Postgres in Phase 1** — that's Phase 4 (see CLOUD_ARCHITECTURE.md).

This is written for the **operator**. Steps needing live credentials/accounts are flagged **🔑 OPERATOR**.

> Source of truth: [CLOUD_ARCHITECTURE.md](CLOUD_ARCHITECTURE.md). Invariants: INV2 (never touch
> `backend/falcon/*` execution), INV5 (no secrets in files — host env only).
>
> **Why SQLite, not Supabase, now:** the code talks to SQLite directly everywhere; switching to
> Postgres is a multi-module refactor (Phase 4), not a deploy step. Supabase account is ready for
> when that scale trigger hits (multiple instances OR managed PITR backups). The Postgres-first
> version of this runbook is preserved in git history (commit `af5dc38`) + `builds/M1-build-log.md`.

---

## 0. Pre-flight (local, one-time)

- [ ] Confirm the app DB exists: `data/db/kanida_universe.db` (~573M). **This whole file ships — no pruning in Phase 1.** It already holds the multi-year `falcon_features` (back to 2021) the Historical Evidence panel needs, so shipping it whole keeps every request path working.
- [ ] **Make all request paths self-sufficient on the host** — three DISTINCT mechanisms, verified against source:
  - **`/power/today` Historical Evidence** reads `falcon_outcomes` from **`POWER_RND_DB_PATH`** ([falcon_top20_router.py:94](../../backend/power_user/routers/falcon_top20_router.py)). → merge `falcon_outcomes` (~827k) into the app DB **and** set `POWER_RND_DB_PATH` = the app DB path.
  - **`/power/personas/*` (ALL personas)** call `load_full_patterns()` which reads **`falcon_promoted_patterns` + `falcon_pattern_candidates`** ([persona_engine_core.py:69-74](../../backend/power_user/services/persona_engine_core.py:69)). The path resolver **IGNORES `POWER_RND_DB_PATH`** — it builds its own and falls back to **`kanida_universe_rnd.db` next to the app DB** ([persona_simulator.py:62-71](../../backend/power_user/services/persona_simulator.py:62)). Missing → **every persona endpoint fails** (uncaught, [persona_simulator.py:408](../../backend/power_user/services/persona_simulator.py:408)). → ship a **SMALL sidecar `kanida_universe_rnd.db`** next to the app DB containing just those 2 pattern tables (KB-MB, NOT the 14G; copying the tables into the app DB does NOT work — the code won't look there).
  - **patient-trader (P2) intraday filter** reads `intraday_mining.db` via a **hardcoded relative path with no env and no fallback** ([persona_simulator.py:74-79](../../backend/power_user/services/persona_simulator.py:74)). Its call is **caught** ([persona_simulator.py:433-434](../../backend/power_user/services/persona_simulator.py:433)), so on the host **P2 runs DEGRADED (no intraday gate), it does NOT crash.** Full P2 accuracy needs a small code change (add an env/fallback for the intraday path) — **decide consciously**: accept degraded P2 for Phase 1, or do the code change. (`§9`)
- [ ] The 14G research warehouse **stays on the laptop**. It is never uploaded. The persona sidecar is a *small extract*, not the warehouse.

---

## 1. 🔑 OPERATOR — Pick a host with a persistent volume

You need a compute host (Supabase does NOT run your Python app). Recommended, in order of least friction:

| Host | Why |
|---|---|
| **Railway** (recommended) | Matches the existing `railway.json` + `Dockerfile`. Has **volumes**. Cron services for the daily jobs. Easiest path. |
| Fly.io | Volumes + always-on; a bit more ops. |
| VPS (Hetzner/DigitalOcean) | Cheapest for always-on + big disk; you manage everything (`deploy/Procfile` + systemd + cron). |

Create the host project and **attach a persistent volume** (≥2 GB; the DB is ~0.6 GB and grows slowly). Mount it at a stable path, e.g. `/data`.

---

## 2. 🔑 OPERATOR — Collect secrets (host env)

Set these in the host's environment (never in git). Full manifest: [ENV.md](ENV.md).

| Var | Value |
|---|---|
| `POWER_DB_PATH` | the volume path to the app DB, e.g. `/data/kanida_universe.db` |
| `FALCON_DB_PATH` | same as `POWER_DB_PATH` (one file is the app DB) |
| `POWER_RND_DB_PATH` | **same app DB path** — used ONLY by `/power/today` Historical Evidence (`falcon_outcomes`). **Personas IGNORE this var** (they need the `kanida_universe_rnd.db` sidecar — see §0). |
| `POWER_JWT_SECRET` | `openssl rand -hex 32` — **CRITICAL**; if unset, every restart logs all users out |
| `KITE_API_KEY` / `KITE_API_SECRET` | Zerodha Kite Connect app |
| `ZERODHA_USERNAME` / `ZERODHA_PASSWORD` / `ZERODHA_TOTP_SECRET` | broker login + TOTP seed (headless auth) |
| `SITE_USER` / `SITE_PASS` | HTTP Basic gate on `/falcon/*` operator routes |
| `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET` / `RAZORPAY_WEBHOOK_SECRET` / `RAZORPAY_PLAN_MONTHLY` | billing (₹999/mo) |
| `EMAIL_PROVIDER` / `RESEND_API_KEY` / `EMAIL_FROM` | transactional email |
| `FALCON_PUBLISH_SECRET` | the laptop→cloud publish secret (set the SAME value on the laptop) |

> **DO NOT set `DATABASE_URL`** — that flips the app to Postgres mode. Phase 1 is SQLite.

---

## 3. Put the DB on the volume (once)

Get `data/db/kanida_universe.db` (with `falcon_outcomes` merged in, §0) onto the volume at `POWER_DB_PATH`:
- **Railway:** upload via the volume's shell/one-off, or seed-on-first-boot: have `entrypoint.sh` copy the image-baked DB to the volume **only if the volume copy is absent** (so redeploys never overwrite live data). Confirm the seed-if-absent guard before first deploy.
- **VPS:** `scp` the file to the mounted volume path.

After this, the volume holds the live DB; the app reads/writes it there and it **survives redeploys/restarts**.

---

## 4. 🔑 OPERATOR — Deploy the app + daily jobs

**Railway (matches `railway.json` + `Dockerfile`):**
1. Connect the repo/branch. Set all §2 env vars on the **web service**.
2. Build runs the root `Dockerfile` (incl. `playwright install --with-deps chromium` for headless auth — first build is slower, expected).
3. Deploy. `entrypoint.sh` starts uvicorn. With no `DATABASE_URL`, `backend/db.py` stays SQLite, pointed at `POWER_DB_PATH` on the volume.
4. Add **cron services** (one per job, `/entrypoint_cron.sh` with `FALCON_JOB` set) — times in UTC = the IST schedule in `deploy/README.md`:
   - `daily_data_refresh` `30 11 * * 1-5`
   - `daily_features` `32 11 * * 1-5`
   - `daily_signals` `35 11 * * 1-5`
   - (weekly mining stays on the **laptop**, not here)

**VPS:** `deploy/Procfile` + systemd for `web`; system cron for the daily jobs.

---

## 5. 🔑 OPERATOR — Zerodha token refresh as a host job

The daily jobs need a valid Kite token. On the host, run the SAME `scripts/auth_worker.py` as a scheduled job (fresh process each fire — the design that fixed the aged-Playwright failure).
- **Command:** `python3 /app/scripts/auth_worker.py`
- **Schedule:** `*/30 * * * *` — the worker self-gates to the IST trading window and exits in <1s outside it.
- **Env:** same as the web service. On SQLite, the token is written to the volume DB at `POWER_DB_PATH` — **no Postgres token-path fix needed** (that's why Path A avoids the R1/C2 work entirely).
- **Prove it:** `python3 /app/scripts/auth_worker.py --force` → exit 0 + a `success` row in `falcon_auth_log` + a fresh `kite_tokens` row = headless auth works on Linux.

---

## 6. 🔑 OPERATOR — Wire the laptop → cloud weekly publish

So the cloud keeps getting fresh patterns after the move:
1. On the **host**: `FALCON_PUBLISH_SECRET` is set (§2). The endpoint `POST /api/falcon/publish/intelligence` is live and **fails closed** until it is.
2. On the **laptop**: set `FALCON_PUBLISH_SECRET` (same value) + `FALCON_PUBLISH_URL=https://<host>`.
3. After the weekly mining, run: `python3 scripts/publish_to_cloud.py` (use `--dry-run` first to see the bundle). It POSTs the promoted patterns + candidates + taxonomy up; the cloud imports them atomically.
4. (Optional) schedule it weekly on the laptop after `weekly_remine`.

---

## 7. VERIFICATION — Phase 1 is DONE only when ALL pass (with the laptop OFF)

1. **Boots from the volume.** Restart the host service → data survives; no fresh empty DB created. Logs show the DB path = the volume, SQLite (not Postgres).
2. **Daily jobs run in cloud.** Trigger `daily_signals` on the host → new rows for today in `falcon_signals_live` / `falcon_signal_runs` (in the volume DB).
3. **Billing works end-to-end.** Test-mode Razorpay → webhook → a user flips to `billing_plan='paid'`.
4. **Auth/session/token work.** Login persists across a restart; `auth_worker.py --force` succeeds headless; admin auth-status shows `token_valid: true`.
5. **`/power/today` AND `/power/personas/*` are laptop-independent.** **Power the laptop OFF.** (a) `/power/today` loads signals + Historical Evidence (needs `falcon_outcomes` in the app DB + `POWER_RND_DB_PATH` set to it). (b) **Every** persona at `/power/personas/{slug}` loads (needs the `kanida_universe_rnd.db` sidecar — §0). (c) patient-trader loads but may log "P2 intraday filter NOT wired" — expected/degraded unless the §9 code change is done. Grep host logs: no successful open of a path under `universe_engine/` (the 14G).
6. **Backups + restore tested.** Take a volume snapshot; restore it to a scratch instance; app boots from it.
7. **Publish is authenticated + atomic.** From the laptop, `publish_to_cloud.py` succeeds with the secret and is rejected without it; a forced-failure mid-import leaves the cloud DB unchanged.

---

## 8. Backups (don't skip — you're holding paying-user + token data)

SQLite-on-volume means **you** own backups (the trade-off vs managed Postgres):
- Schedule a **daily volume snapshot** (Railway/Fly/VPS provider feature), retain ≥7.
- Additionally, a nightly `sqlite3 .backup` copy of `kanida_universe.db` to object storage (S3/R2) is cheap insurance.
- **Test a restore** (verification §6) — an untested backup is not a backup.

---

## 9. Conscious Phase-1 decisions / what it does NOT do
- **P2 (patient-trader) intraday filter:** OFF on the host by default — `intraday_mining.db` resolves via a hardcoded relative path with no env/fallback ([persona_simulator.py:74-79](../../backend/power_user/services/persona_simulator.py:74)) and is caught when absent ([:433-434](../../backend/power_user/services/persona_simulator.py:433)), so P2 runs *degraded but stable*. **To enable it:** a small code change to give `_resolve_intraday_db_path()` an env (`POWER_INTRADAY_DB_PATH`) + a next-to-app-DB fallback, then ship the 30M `intraday_mining.db` sidecar. Decide before launch; not a crash risk either way.
- **No pruning** of OHLC/features (Phase 3) — ship the DB whole.
- **No precomputed evidence/persona summary tables** (Phase 2) — the shipped multi-year DB serves them directly.
- **No Postgres / Supabase** (Phase 4) — revisit when you need multiple app instances or managed PITR backups. Account is ready; the porting script + migration are on the shelf.

---

## Appendix — host web-service env quick reference (Phase 1 / SQLite)
```
POWER_DB_PATH=/data/kanida_universe.db
FALCON_DB_PATH=/data/kanida_universe.db
POWER_RND_DB_PATH=/data/kanida_universe.db        # /power/today evidence ONLY; personas ignore this
# Personas also need a small sidecar:  /data/kanida_universe_rnd.db  (2 pattern tables — see §0)
# Optional for full P2 accuracy:        /data/.../intraday_mining.db  (needs §9 code change)
POWER_JWT_SECRET=<openssl rand -hex 32>
KITE_API_KEY=…  KITE_API_SECRET=…
ZERODHA_USERNAME=…  ZERODHA_PASSWORD=…  ZERODHA_TOTP_SECRET=…
SITE_USER=…  SITE_PASS=…
RAZORPAY_KEY_ID=…  RAZORPAY_KEY_SECRET=…  RAZORPAY_WEBHOOK_SECRET=…  RAZORPAY_PLAN_MONTHLY=…
EMAIL_PROVIDER=resend  RESEND_API_KEY=…  EMAIL_FROM=…
FALCON_PUBLISH_SECRET=<shared with laptop>
# DO NOT SET:  DATABASE_URL  (would switch to Postgres — that's Phase 4)
```
Full manifest + secret flags: [ENV.md](ENV.md).
