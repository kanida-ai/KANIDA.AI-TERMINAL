# P1BUNDLE — Cloud DB bundle builder — build log

**Agent:** BuildAgent-P1BUNDLE
**Date:** 2026-06-14 (IST)
**Deliverable:** `scripts/build_cloud_bundle.py` + RUNBOOK §3 update + this log.
**Goal:** one stdlib-only script that produces the volume-ready DB bundle for the
Phase-1 SQLite-on-a-volume deploy, so every `/power` request path is self-sufficient
on the host and nothing reaches the 14 GB research warehouse at request time.

---

## STANDARD compliance — every claim re-verified against source

I read the exact source lines before relying on them. Citations below are
`file:line` and were confirmed by reading, NOT trusted from the runbook/architecture docs.

### Source DB locations (confirmed on disk)
- App DB: `data/db/kanida_universe.db` — **600,285,184 B (~573 MB)**
- R&D DB: `universe_engine/data/db/kanida_universe.db` — **14,092,242,944 B (~14 GB)** → confirms it must stay off-cloud; we extract only small tables.
- Intraday: `universe_engine/data/db/intraday_mining.db` — **30,461,952 B (~30 MB)**

---

## VERIFIED table list (what goes in the bundle, and why)

### File 1 — `kanida_universe.db` (app DB copy + FULL `falcon_outcomes` merged)

**Table merged in (FULL, from R&D, replacing the sparse local copy): `falcon_outcomes`.**

Why / where read — the `/power/today` Historical Evidence path:

1. Router opens the RND connection at **`POWER_RND_DB_PATH`** (NOT the research DB on host):
   - `backend/power_user/routers/falcon_top20_router.py:30` — `from ..config import POWER_RND_DB_PATH`
   - `backend/power_user/routers/falcon_top20_router.py:94-96` —
     `rnd_con = sqlite3.connect(f"file:{POWER_RND_DB_PATH}?mode=ro", uri=True, timeout=10.0)`
   - passed into `build_falcon_top20(..., rnd_con=rnd_con, ...)` at `:101-110`.

2. The explainer reads `falcon_outcomes` from that `rnd_con`:
   - `backend/power_user/services/falcon_top20_explainer.py:1103-1108` —
     `SELECT trade_date, ret_20d, hit_10pc_20d, mae_20d, mfe_20d FROM falcon_outcomes WHERE symbol=? AND trade_date IN (...)`
   - `backend/power_user/services/falcon_top20_explainer.py:1223-1226` (`_stock_lifetime_baseline`) —
     `SELECT AVG(hit_10pc_20d)*100 FROM falcon_outcomes WHERE symbol=? AND hit_10pc_20d IS NOT NULL`
   - `backend/power_user/services/falcon_top20_explainer.py:1242-1245` (`_load_outcomes`, present in code path) —
     `SELECT trade_date, ret_5d, ret_10d, ret_20d, ret_30d, hit_10pc_20d, hit_15pc_20d, failure_10d FROM falcon_outcomes ...`

   → Columns needed across these queries: `symbol, trade_date, ret_5d, ret_10d, ret_20d,
   ret_30d, hit_10pc_20d, hit_15pc_20d, mae_20d, mfe_20d, failure_10d`. The builder copies the
   table **verbatim** (replays the source `CREATE TABLE`), so ALL columns are preserved — no
   risk of dropping a column a query needs.

**Decision:** on the host `POWER_RND_DB_PATH` = the app DB path (RUNBOOK §0/§2, line 23 + line 52,
appendix line 144). So `falcon_outcomes` MUST live inside `kanida_universe.db`. The app DB may
already carry a SPARSE `falcon_outcomes`; the builder DROPs it and replaces it with the FULL
R&D copy (`build_cloud_bundle.py: build_app_db`).

> Note: `prod_con` (the app DB) is also read by the explainer for `falcon_signals_live`,
> `falcon_pattern_taxonomy`, `falcon_sectors`, `universe_master`, `falcon_features`, `ohlc_daily`
> (e.g. `:211-214, :405-410, :432-436, :530-538, :1023-1028, :649-674`). These already live in the
> app DB and ship as-is inside the whole-file copy — no extra action needed. The ONLY table the
> app DB is missing in full is `falcon_outcomes`, which is exactly what the merge fixes.

### File 2 — `kanida_universe_rnd.db` (persona sidecar — 2 tables only)

**Tables (verbatim from R&D): `falcon_promoted_patterns`, `falcon_pattern_candidates`.**

Why / where read — ALL persona endpoints (`/power/personas/*`):

1. `backend/power_user/services/persona_engine_core.py:61-83` — `load_full_patterns(rnd_db_path)`:
   ```sql
   SELECT c.mined_year, c.rule_json, p.avg_oos_year_lift_pp
     FROM falcon_promoted_patterns p
     JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
    WHERE p.classification IN ('universal','regime_dependent')
   ```
   (the exact JOIN is at `:69-74`). These are the only two tables personas read from the RND DB.

2. The RND path resolver IGNORES `POWER_RND_DB_PATH` and falls back to the sidecar filename
   next to the app DB:
   - `backend/power_user/services/persona_simulator.py:62-71` — `_resolve_rnd_db_path()` builds
     `…/universe_engine/data/db/kanida_universe.db` (absent on host) then falls back to
     `Path(config.POWER_DB_PATH).parent / "kanida_universe_rnd.db"` (`:67`). It never reads
     `POWER_RND_DB_PATH`. Confirmed: no reference to `POWER_RND_DB_PATH` anywhere in this file.
   - Call site `:408-409` — `rnd_db = _resolve_rnd_db_path(); all_pats = load_full_patterns(rnd_db)`.
     This is NOT wrapped in try/except → if the sidecar is missing, **every persona endpoint
     fails** (FileNotFoundError raised by `_resolve_rnd_db_path` at `:70`).

**Decision:** ship a SMALL `kanida_universe_rnd.db` sidecar (these 2 tables only) and place it
NEXT TO the app DB on the volume. Copying the tables into the app DB does NOT help — the code
looks only at the sidecar filename. Builder VACUUMs the sidecar so it is genuinely small.

### File 3 — `intraday_mining.db` (OPTIONAL, `--with-intraday`)

**Table: `intraday_dataset_v2` (whole-file copy when requested).**

Why / where read — patient-trader (P2) intraday filter:

1. `backend/power_user/services/persona_simulator.py:74-79` — `_resolve_intraday_db_path()` uses a
   HARDCODED relative path (`…/universe_engine/data/db/intraday_mining.db`), **no env, no fallback**,
   and raises `FileNotFoundError` if absent (`:77-78`).
2. `backend/power_user/services/persona_simulator.py:98-103` — reads
   `SELECT symbol, entry_date, today_first_15_ret_pct, today_first_15_vol_pct_of_yest FROM intraday_dataset_v2`.
3. Call site `:428-434` is CAUGHT: `except FileNotFoundError as e: log.warning("P2 intraday filter
   NOT wired …")`. So on the host P2 runs **DEGRADED (no intraday gate) but does NOT crash**.

**Decision:** intraday is OPTIONAL for Phase 1. `--with-intraday` copies the file, but the host
will NOT actually use it until a code change adds an env (`POWER_INTRADAY_DB_PATH`) + a
next-to-app-DB fallback to `_resolve_intraday_db_path()` (RUNBOOK §9). The script documents this
in its end-of-run host-placement notes.

---

## What the script does (`scripts/build_cloud_bundle.py`)

1. **`kanida_universe.db`** — `shutil.copy2` the app DB to the output, open the COPY, DROP any
   existing `falcon_outcomes`, replay the R&D `CREATE TABLE falcon_outcomes` from `sqlite_master`,
   bulk-INSERT all rows (placeholder count derived from `PRAGMA table_info` — no hand-written
   column list), replay non-auto indexes, assert dst row-count == src row-count.
2. **`kanida_universe_rnd.db`** — fresh sidecar; for each of the 2 persona tables, replay
   `CREATE` + bulk-INSERT + indexes, assert row counts, then `VACUUM`.
3. **`intraday_mining.db`** — only with `--with-intraday`; verbatim whole-file `shutil.copy2`,
   then a read-only row-count check on `intraday_dataset_v2`.

### Safety / idempotency measures
- **Sources never mutated:** R&D + intraday opened `file:…?mode=ro` (read-only URI). App DB is
  copied to the output FIRST, then only the COPY is written.
- **Schema-preserving:** every table recreated by replaying its source `CREATE` statement; row
  inserts use an auto-sized `?`-placeholder list from the real column count → cannot drift.
- **Idempotent:** output files overwritten; merged table DROPped before recreate; sidecar rebuilt
  fresh each run. Re-running yields the same bundle.
- **Verified:** row counts asserted after each copy; `--verify` re-opens the produced bundle and
  asserts each required table exists with > 0 rows; manifest prints per-table row counts + sizes.
- **stdlib only:** `sqlite3, shutil, argparse, pathlib, os/sys/time`. No external deps.

### CLI
`--app-db --rnd-db --intraday-db --out-dir --with-intraday --dry-run --verify`
(`--dry-run` and `--verify` are mutually exclusive; neither = build). Defaults resolve from the
repo root computed off `__file__`, so it runs from anywhere.

---

## Host placement (printed by the script at end-of-run)
- `kanida_universe.db` → `POWER_DB_PATH` (e.g. `/data/kanida_universe.db`). Set
  `FALCON_DB_PATH` = `POWER_RND_DB_PATH` = the SAME path.
- `kanida_universe_rnd.db` → SAME directory as the app DB (e.g. `/data/kanida_universe_rnd.db`).
  Personas ignore `POWER_RND_DB_PATH`; missing sidecar = all personas fail.
- `intraday_mining.db` → optional; host won't read it without the §9 code change.

---

## Constraints honored
- **INV2:** did not touch `backend/falcon/*` execution (read-only verification only).
- **INV5:** no secrets in the script or docs.
- **INV6:** script lives in `scripts/`.
- Did NOT git commit.

## Deviations / notes for the audit agent
- The script copies `falcon_outcomes` **verbatim (all columns)** rather than only the 11 columns
  the queries name. This is deliberate (the spec says "copied verbatim … schema + rows" / "replay
  each table's CREATE statement") and removes any risk of a future query referencing a column the
  copy dropped. Cost: a few extra columns of data; `falcon_outcomes` is ~827k rows so the size
  delta is small relative to the 573 MB app DB.
- `_load_outcomes` (`falcon_top20_explainer.py:1232-1248`) is defined and references additional
  outcome columns (`ret_5d, ret_10d, ret_30d, hit_15pc_20d, failure_10d`); the verbatim copy
  already covers these. (It is a helper; the live Bucket-2 path uses the `:1103` query.)
- Could not run Python in this environment (none installed). The script was built by careful
  source reading; `--dry-run` (inspects sources read-only, writes nothing) and `--verify` are the
  operator's on-machine validation gates.

## Risks for the audit agent to check on the operator's machine
1. **Disk:** the output `kanida_universe.db` is a full copy of the ~573 MB app DB + the merged
   `falcon_outcomes`; ensure `deploy/cloud-bundle/` has room.
2. **`falcon_outcomes` schema parity:** if the R&D `falcon_outcomes` ever lacks a column a query
   names, the verbatim copy preserves whatever R&D has — verify R&D has `ret_20d, hit_10pc_20d,
   mae_20d, mfe_20d` (it is the canonical source, so it should).
3. **Persona classification filter:** `load_full_patterns` filters
   `classification IN ('universal','regime_dependent')` on `falcon_promoted_patterns`. The builder
   copies the FULL tables (no filter), which is correct — filtering happens at query time. Just
   confirm both tables carry rows with those classifications (else personas return empty).
4. **min_fires=10 gate (falcon-top-10):** `falcon_top20_router`/explainer enforce min_fires from
   `PERSONA_CONFIGS`; this is config, not a bundle concern — flagged only so the audit agent knows
   the bundle does not affect it.
