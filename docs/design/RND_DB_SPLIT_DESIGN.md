# R&D-DB Serve/Execution-Time Dependency — Split & Publish Design

**Status:** DESIGN (read-only investigation). No code changed, no backend restarted, no `config/.env` touched.
**Prod HEAD at investigation:** `27709ec` (verified `git rev-parse HEAD`).
**Author context:** AutoTrade Sessions engineer. Must-fix before the single-server cloud (EFS) cutover — Doc 1 §4.2.
**Scope:** the production SERVING + LIVE-EXECUTION paths that open the ~36 GB R&D database at request/execution time.

---

## 0. TL;DR / Headline

The "36 GB R&D DB" (`universe_engine/data/db/kanida_universe.db`, measured **38,131,961,856 bytes ≈ 38.1 GB** on 2026-07-17) is **two datasets fused in one file**: the research/mining panel **and** the live market-data sink the pollers write. Three runtime legs reach into it:

| Leg | Where | Live-critical? | Right fix | Difficulty |
|-----|-------|----------------|-----------|------------|
| **1. Explainer evidence** | `power_user/routers/falcon_top20_router.py:94,214` → `falcon_top20_explainer.py:1145,1266` reads `falcon_outcomes` | No (serve-time, read-only) | **Artifact-publish + read-flag** — artifact already built (A1) | **Low** |
| **2. Persona / co-trade sim** | `persona_simulator.py:466` + `cotrade_sim.py:409` → `persona_engine_core.load_full_patterns():67` reads `falcon_promoted_patterns ⋈ falcon_pattern_candidates` (unfiltered) | No (serve-time) | **Artifact-publish (unfiltered patterns) + read-flag** | **Low** |
| **3. Live market-data sink** | `autotrade/execution/worked_order.py:395,880` + `autotrade/strategies/tesla_short_engine.py:106` read `ohlc_1min` / `mkt_orderflow_1min` / `mkt_reference` **at execution/signal time** | **YES (real money)** | **Live-sink SPLIT** (poller write-target + exec read-source move together) + a small `ohlc_1min` **profile artifact** | **High — the real work** |

**Legs 1 & 2 are straightforward, additive, default-off, parity-testable on the laptop NOW.** **Leg 3 is the real work** — but it can be done **without a live-execution safety gap** (both execution consumers degrade *fail-safe* when the sink is absent/thin, and the cutover happens in a market-closed window with zero concurrent writer/reader). The honest catch is an **EFS/topology constraint**: the sink is written by laptop Task-Scheduler pollers and read by execution — a cloud replica cannot share a laptop-local SQLite file, so **the pollers must be co-located with execution** (same box / same EFS mount). See §6.

---

## 1. Leg 1 — Explainer serve-time evidence (`falcon_outcomes`)

### 1.1 Evidence (file:line)
- `power_user/routers/falcon_top20_router.py:94` (`/today/falcon-top-20`) and `:214` (`/today/falcon-explain`) open R&D **per request**, read-only:
  ```py
  rnd_con = sqlite3.connect(f"file:{POWER_RND_DB_PATH}?mode=ro", uri=True, timeout=10.0)
  ```
  `POWER_RND_DB_PATH` is imported from `power_user/config.py` (router line 30).
- The R&D connection is consumed by `falcon_top20_explainer.build_falcon_top20(...)` → `_build_bucket2` (`:766`) → two reads of `falcon_outcomes`:
  - `falcon_top20_explainer.py:1145` — `SELECT trade_date, ret_20d, hit_10pc_20d, mae_20d, mfe_20d FROM falcon_outcomes WHERE symbol=? AND trade_date IN (<fire dates>)`. Fire dates come from replaying rules over **PROD** `falcon_features` bounded by `_BUCKET2_LOOKBACK_START = "2021-01-01"` (`:179`, read at `:1069`).
  - `falcon_top20_explainer.py:1266` — `_stock_lifetime_baseline`: `SELECT AVG(hit_10pc_20d)*100 FROM falcon_outcomes WHERE symbol=? AND hit_10pc_20d IS NOT NULL` — **ALL-TIME, no date filter** (the subtle one).
- `_load_outcomes` (`:1274`) is **dead code** (no callers) — not published.

Cached 10 min per key (`_cache_get`), but a cold key opens R&D. The R&D dependency is real and serve-time.

### 1.2 Fix: artifact-publish + read-flag (A1 — publisher DONE)
`scripts/publish_outcomes_evidence.py` already produces the artifact and is verified:
- **`falcon_outcomes_evidence`** — the 6-col evidence table, bounded `trade_date >= 2021-01-01`.
- **`falcon_outcome_baseline`** — per-symbol ALL-TIME `lifetime_hit_pct` + `n_rows_all_time`, **precomputed over full R&D history** (the docstring correctly flags that recomputing the baseline over only the 2021+ subset would silently shrink the denominator).
- **`falcon_artifact_manifest`** — version/checksum/cutoff/source-commit.
- Sizing (measured in the script's docstring): 827,379 R&D rows → **~53.8 MB artifact**, exact parity.
- Guards: `_assert_cutoff_matches_explainer()` fails the build if `_BUCKET2_LOOKBACK_START` drifts; `--verify-parity` compares both queries per-symbol for exact equality.

**Remaining work (the only remaining part of Leg 1):** wire the explainer to READ the artifact instead of R&D, behind a **default-off flag**:
1. Add `FALCON_SERVE_EVIDENCE_ARTIFACT` (path) — unset = current R&D behavior (byte-identical).
2. In `build_falcon_top20` / `_build_bucket2` / `_stock_lifetime_baseline`, when the flag is set, open the artifact (`falcon_outcomes_evidence` for the `IN (...)` lookup; read `lifetime_hit_pct` directly from `falcon_outcome_baseline` instead of `AVG(...)`).
3. **Shadow mode** (`FALCON_SERVE_EVIDENCE_SHADOW=true`): compute BOTH (R&D and artifact), serve R&D, log any payload diff. Flip to artifact-only after N clean days.

### 1.3 Parity gate
Publisher already ships `--verify-parity` (per-symbol exact equality on both queries). Serve-path gate = shadow-compare the **fully-built explainer payload** (not just the raw rows) between R&D-read and artifact-read for every served symbol over ≥1 trading week; zero diffs → flip the flag.

---

## 2. Leg 2 — Persona / co-trade simulator (unfiltered pattern set)

### 2.1 Evidence (file:line)
- `persona_simulator._simulate_persona_uncached` (`:465-466`):
  ```py
  rnd_db = _resolve_rnd_db_path()      # persona_simulator.py:63 → universe_engine/data/db/kanida_universe.db
  all_pats = load_full_patterns(rnd_db)
  ```
  (The task-cited `:408` is the warm-cache return; the actual R&D open is `:466`.)
- `cotrade_sim.py:408-409` does the identical `_resolve_rnd_db_path()` + `load_full_patterns(rnd_db)`.
- `persona_engine_core.load_full_patterns():67-74` opens R&D and runs:
  ```sql
  SELECT c.mined_year, c.rule_json, p.avg_oos_year_lift_pp
    FROM falcon_promoted_patterns p
    JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
   WHERE p.classification IN ('universal','regime_dependent')
  ```
  then drops `in_drawdown_bounce` rules in Python. **No mining-year cutoff** — this is the **deliberately-unfiltered** set (task-measured 1826 R&D rows).
- Every other loader in the sim already reads **PROD** (`load_panel`, `load_all_bars`, `trading_days`, `build_sector_map` all take `PROD_DB` — `persona_engine_core.py:86,109,127,140`). R&D is touched **only** for the pattern catalog. Also reads `intraday_mining.db` (`_resolve_intraday_db_path`) for P2's 9:30 filter — a *third* research DB, same class of problem, flag it.

### 2.2 Why prod's existing published patterns are NOT enough
`scripts/publish_patterns.py` ALREADY copies `falcon_promoted_patterns` + `falcon_pattern_candidates` R&D→PROD — **but with a mining-window filter** (`mined_year >= current_year - mining_window_years`, default 4). That filter is exactly why PROD has the **task-measured 834 rows** vs R&D's 1826. The live signal engine (`signal_runner`) *wants* the filtered set; the sim *wants* the unfiltered set. So the sim **cannot** just repoint at PROD's existing tables — results would change.

### 2.3 Fix: artifact-publish (unfiltered) + read-flag
Publish the **unfiltered** sim pattern set as its own production artifact (mirrors A1):
- New script `scripts/publish_sim_patterns.py` (or a mode of `publish_patterns.py`): read R&D with `load_full_patterns`'s exact query (classification filter, **no** mining cutoff), write a small artifact table `falcon_sim_patterns(mined_year, rule_json, avg_oos_year_lift_pp)` + manifest. This is ~1826 rows of rule JSON — **a few MB**, trivially on EFS.
- Add `FALCON_SIM_PATTERNS_ARTIFACT` (path); `load_full_patterns` prefers the artifact when set, else current R&D path (default-off). Do the same for `intraday_dataset_v2` (P2 filter) → `falcon_sim_intraday_artifact`.
- `in_drawdown_bounce` filtering stays in Python (unchanged), so parity is exact.

### 2.4 Parity gate
Shadow-compare `simulate_persona(slug, force=True)` output (yearly returns, trade list, summary) between R&D-read and artifact-read for **every** persona slug + the co-trade sim. Byte-equal summaries → flip. (The sim is deterministic given the same pattern set + PROD panel, so parity should be exact, not statistical.)

---

## 3. Leg 3 — Live-execution market-data sink (the real work)

### 3.1 Evidence (file:line) — reads AT EXECUTION/SIGNAL TIME

**A) `autotrade/execution/worked_order.py`** — the worked/pacing order engine (used by every live worked entry/exit; `make_vwap_sizer` called from `session.py:3431` and `monitoring/exit_poller.py:674`). Default DB = `_DEFAULT_UNIVERSE_DB` (`:63-65` → `parents[3]/universe_engine/data/db/kanida_universe.db`, i.e. the 38 GB R&D file). Two reads:
- `load_intraday_profile` (`:373`, connect `:395`): `SELECT ... FROM ohlc_1min WHERE symbol=? ...` — builds the 75-bucket normalized **intraday volume profile** from the last 20 distinct days of **`ohlc_1min`** (the *deep* 1-min history). Powers the VWAP child-sizer shape.
- `recent_interval_volume` (`:868`, connect `:880`): `SELECT volume FROM mkt_orderflow_1min WHERE symbol=? AND segment=? ORDER BY bar_time DESC LIMIT ?` — the **live** recent-volume participation signal.
- **Fail-safe:** both are `try/except → None` on any error / missing DB; the caller then falls back to flat-POV / the TWAP floor. **A missing sink degrades pacing, it does not break the order.** (Verified: `:426-427`, `:893-895`, and the docstring at `:44`.)

**B) `autotrade/strategies/tesla_short_engine.py`** — the order-flow SHORT signal engine. Default DB = `DEFAULT_DB_PATH` (`:56-57`, same 38 GB file). `connect_db_readonly` (`:103`) read-only. Reads `mkt_orderflow_1min` (CASH candidates + NIFTY `segment='FUT'` context: `:114,154,197,238,591,674`) and `mkt_reference` (sector context). Per-session override exists: `config.tesla_signal_db_path` (`config.py:1387`, used `session.py:4345`) → passed as `db_path` to `compute_signals` (`session.py:4362,4367`). **Fail-safe:** no data → no A++/A+++ candidates → **no signal → no trade** (fail-*silent*, not a broken exit). Tesla is decision-only (`:26-28`); it never places/exits.

### 3.2 The dataset is two things in one file — verified table map
Measured (2026-07-17, R&D DB, read-only):

| Table | Rows (max rowid) | Span | Writer | Read by execution? |
|-------|------|------|--------|--------------------|
| `ohlc_1min` | ~95.7 M | deep history | **research** `universe_engine/engine/data_fetch.py:181,191` (NOT a live poller) | **YES** — `worked_order.load_intraday_profile` (profile shape) |
| `mkt_orderflow_1min` | ~1.95 M | 2026-07-08 → 07-16 (~9 live days) | `scripts/mkt_poller.py:122` (Task `KanidaOrderFlowPoller`) | **YES** — `worked_order.recent_interval_volume` + Tesla |
| `mkt_reference` | 742 | rebuilt daily | `scripts/mkt_reference.py:49` | **YES** — Tesla sector context |
| `mkt_ohlc_1min` | ~125 M | 2026-07-06 → 07-16 | `scripts/mkt_backfill_ohlc.py:101` (Task `KanidaMktBackfill`, 16:30 IST) | **No** |
| `mkt_trades_1min` | ~1.73 M | live | `scripts/mkt_tick_capture.py` (Task `KanidaTickCapture`) | **No** |
| `mkt_control` | small | — | `mkt_poller.py:44` | poller-internal |
| `fo_orderflow_1min` | empty | — | `scripts/orderflow_poller.py:128` (Rupeezy Vortex) | No (F&O, not yet live) |

**Key correction to the task's framing:** the execution path reads a **mix** — the *live-poller* table `mkt_orderflow_1min` (small, ~1.95 M/9 days) **and** the *research-fetch* table `ohlc_1min` (huge, ~95.7 M, written by `data_fetch.py`, **not** by a live poller). The live-critical, poller-written surface is small: `mkt_orderflow_1min` + `mkt_reference` (+ `mkt_control`). The `ohlc_1min` dependency is deep-history and, crucially, **optional/fail-safe**.

### 3.3 The full write→read map (what must move together)

**Writers (Task Scheduler, laptop, self-gated 09:15–15:30 IST):**
- `KanidaOrderFlowPoller` → `mkt_poller.py` → writes `mkt_orderflow_1min`, `mkt_control`. **Also READS `ohlc_1min` (distinct symbol) + `fo_stock_master` to build its universe** (`mkt_poller.py:61-63`).
- `mkt_reference.py` → writes `mkt_reference`. **READS `falcon_sectors` + `fo_stock_master`** (`:39-40`).
- `KanidaMktBackfill` → `mkt_backfill_ohlc.py` → writes `mkt_ohlc_1min` (reads `ohlc_1min` for universe).
- `KanidaTickCapture` → `mkt_tick_capture.py` → writes `mkt_trades_1min`.

**Readers (live execution):** `worked_order` (`ohlc_1min`, `mkt_orderflow_1min`), Tesla (`mkt_orderflow_1min`, `mkt_reference`).

**The atomic-move set** = `{mkt_orderflow_1min, mkt_reference, mkt_control}` (poller writes ↔ exec reads) **plus** the poller/reference **universe-seed reads** (`ohlc_1min` symbol list, `fo_stock_master`, `falcon_sectors`) **plus** the exec `ohlc_1min` profile read. If you flip the poller's DB target to a fresh empty sink, `SELECT DISTINCT symbol FROM ohlc_1min` and `fo_stock_master` return empty → **no universe → poller writes nothing**. So the sink must carry (a copy of) the seed tables too, or the poller must keep reading the research DB for its seed.

### 3.4 Fix: split the live sink into a production DB — recommended shape

Create a new **production market-data sink DB**, e.g. `data/db/kanida_mktsink.db` (co-located with the app / on EFS), containing:
- **Live tables the pollers write & exec reads:** `mkt_orderflow_1min`, `mkt_reference`, `mkt_control`. (Optionally `mkt_ohlc_1min`, `mkt_trades_1min` for completeness / future use.)
- **A small universe seed** the pollers need: a `symbol` list (materialized from R&D `SELECT DISTINCT symbol FROM ohlc_1min`), `fo_stock_master`, `falcon_sectors`. These are small and change slowly — publish nightly (A1-style) into the sink. This removes the poller's read-dependency on the research file.
- **The `ohlc_1min` deep-history dependency → a precomputed profile artifact**, NOT a 95.7 M-row copy. `load_intraday_profile` only needs a normalized **75-bucket-per-symbol histogram**. Precompute it nightly from R&D `ohlc_1min` → tiny table `mkt_intraday_profile(symbol, buckets_json, n_days, as_of)` and teach `load_intraday_profile` to prefer it (fail-safe unchanged). This is the elegant part: the biggest, deepest read collapses to a per-symbol vector — exactly the A1 pattern.

**Read-source lever (additive, default-off):**
- Add env `FALCON_MKT_SINK_DB`. When set, it overrides `worked_order._DEFAULT_UNIVERSE_DB` (`:63`) and `tesla_short_engine.DEFAULT_DB_PATH` (`:57`). Unset = current 38 GB path = **byte-identical** to today. (Tesla additionally already honors per-session `config.tesla_signal_db_path`.)
- Point the same env at the pollers' `DB` constant (`mkt_poller.py:21`, `mkt_reference.py`, etc.) so **write-target and read-source share ONE resolution**.

Ranking (simplicity + safety): **(i)** profile-artifact for `ohlc_1min` (pure A1, zero live risk) → **(ii)** seed-publish for the poller universe → **(iii)** the live-table split + env cutover (the only step touching the live write path).

### 3.5 Cutover WITHOUT a live-execution gap

The pollers self-gate to 09:15–15:30 IST and write **only** during market hours; no intraday session (worked_order / Tesla) runs between sessions. So a market-closed boundary (any evening after 15:30, or a weekend) has **zero concurrent writer and zero concurrent live reader** on these tables → a clean file-level cutover is possible **without dual-write**.

Cutover procedure (closed window):
1. Build the new sink: copy the last ≥`personality_window_days` (default 5, use ~10 for margin) trading days of `mkt_orderflow_1min` + current `mkt_reference` + `mkt_control` + the universe seed + the `ohlc_1min` profile artifact into `kanida_mktsink.db`.
2. Set `FALCON_MKT_SINK_DB=…/kanida_mktsink.db` (one line) so pollers **and** execution resolve to it together.
3. Restart the pollers (next 30-min trigger) + the backend. Next 09:15 session writes+reads the new sink.
4. Leave the R&D file untouched (research keeps its own `ohlc_1min`/mkt tables; no data lost).

**Why there is no *safety* gap even if seeding is imperfect:** both consumers degrade **fail-safe** —
- `worked_order`: missing/thin sink → `None` → flat-POV / TWAP-floor pacing. Orders still place & complete; only fill *quality* is slightly worse. (`:426`, `:889`.)
- Tesla: missing/thin sink → no candidates → **no new SHORT signal** → no trade. It never breaks an exit (decision-only, `:26`).
So the worst case of a bad cutover is *degraded pacing + delayed signals for a few days*, not a leaked order, a naked leg, or a missed kill. The **kill switch and per-position GTT do not read the sink at all** — they run off live LTP/broker state, so basket safety is unaffected by this split.

**Mitigation for the degradation window:** pre-seed ≥10 days of `mkt_orderflow_1min` + build the profile artifact before the flip, so day-1 on the new sink already has Tesla's rolling window + the volume profiles warm.

### 3.6 Parity gate (leg 3)
1. **Offline sizing parity:** with the new sink seeded, run `make_vwap_sizer` / `load_intraday_profile` / `recent_interval_volume` for a basket of symbols against BOTH DBs → assert identical child-qty sequences (or documented, bounded differences only where the sink's shorter history is intended).
2. **Tesla signal parity:** run `compute_signals(db_path=sink)` vs `db_path=R&D` for the same as-of poll → identical A++/A+++ set (the existing Tesla parity test framework covers this shape).
3. **Paper shadow:** run a paper (`dry_run`) tesla_short + worked-order session pointed at the sink, real prices, over ≥1 live session; compare executed sizing/signals to an R&D-pointed paper run. Zero-material-diff → flip live.

---

## 4. Interaction with the EFS single-replica cloud deploy (FLAG)

The near-term cloud target is **one Fargate replica, EFS-mounted SQLite, single writer** (per `cloud_migration_handoff`). Splitting the sink creates a **SECOND production DB** that BOTH the pollers (writers) and execution (reader) need on shared storage:

- **Constraint:** SQLite is a local-file DB. You **cannot** have laptop Task-Scheduler pollers writing a file that a cloud Fargate task reads — there is no shared filesystem across those machines, and SQLite has no network transport. So the sink split forces a decision:
  - **(Recommended) Move the pollers into the cloud, co-located with execution**, both mounting the sink on the **same EFS volume**. The pollers become a scheduled sidecar (EventBridge → a small task, or an in-container scheduler) on the one instance. Then sink writes + exec reads share one EFS file, single writer preserved.
  - **(Alternative, near-term)** Keep execution + pollers on **one box** (the laptop today, or one cloud instance) so the sink is a plain local file both touch. This is what today's laptop deploy already is — the split is then purely "carve the sink out of the 38 GB file", no topology change, and the cloud move happens later as a unit.
- **`.dockerignore` already excludes the 36 GB R&D DB** (`deploy/.dockerignore`, per handoff) — good; the sink split is what makes that exclusion *safe for execution*, because today execution still needs the excluded file. After the split, the container ships/mounts only the small sink + prod DB; the R&D panel stays in the batch environment.
- **Legs 1 & 2 artifacts** (53.8 MB evidence + a few-MB pattern set + profile artifact) are all EFS-friendly and are what let the single replica serve without the 38 GB file at all.

**Bottom line for the migration:** legs 1 & 2 fully remove serve-time R&D reads with tiny artifacts (do these first, laptop-testable now). Leg 3 removes the execution-time R&D read but **couples the pollers and execution onto one storage domain** — plan the poller relocation as part of the same cloud step, or keep them co-located on one box until then.

---

## 5. Phased build plan

**Phase A — Leg 1 (low risk, laptop NOW):**
- A1 publisher is DONE + parity-verified. Build the read-flag in the explainer (`FALCON_SERVE_EVIDENCE_ARTIFACT` + `_SHADOW`), default-off, additive. Shadow-compare served payloads ≥1 week → flip.

**Phase B — Leg 2 (low risk, laptop NOW):**
- Write `publish_sim_patterns.py` (unfiltered `falcon_sim_patterns` + manifest) and the P2 `intraday_dataset_v2` artifact. Add `FALCON_SIM_PATTERNS_ARTIFACT` read-flag in `load_full_patterns` (default-off). Shadow-compare every persona + co-trade sim output → flip.

**Phase C — Leg 3.a (low risk, laptop NOW):**
- Precompute the `ohlc_1min` **volume-profile artifact** + the **universe seed** (symbols / `fo_stock_master` / `falcon_sectors`) into a standalone sink DB. Pure read-of-R&D + write-of-artifact; changes no live behavior. Add the `FALCON_MKT_SINK_DB` env resolver to `worked_order` + `tesla_short_engine` + the pollers, **default-off** (unset = today). Parity-test sizing/signals offline against both DBs.

**Phase D — Leg 3.b (careful, market-closed window, operator sign-off):**
- Seed ≥10 live days into the sink, flip `FALCON_MKT_SINK_DB`, restart pollers + backend in a closed window. Paper-shadow ≥1 session, then run live. Rollback = unset the env + restart (reverts to the 38 GB file instantly; nothing was destroyed).

**Phase E — cloud:** co-locate pollers with execution on the single EFS replica (or keep one-box) per §4; ship only the small artifacts + sink + prod DB.

---

## 6. Assumptions & honest caveats

- **Verified:** all file:line reads above (read the exact source at `27709ec`); DB file size 38.1 GB; table row-estimates via `MAX(rowid)`; `mkt_orderflow_1min` span 2026-07-08→07-16; `mkt_reference`=742 rows; A1 publisher exists + documents parity; `publish_patterns.py` applies the mining-window filter (source of the 834-vs-1826 gap).
- **Unverified (needs a live run to confirm):** exact `--verify-parity` PASS on the *current* R&D DB (the script asserts it; I did not execute it — read-only, and it scans 800k rows). Exact `mkt_orderflow_1min`/`ohlc_1min` full COUNTs (used `MAX(rowid)` estimates; a full COUNT on the 38 GB file was too slow to run read-only). The Task-Scheduler task **names** are taken from the `.bat` headers (`KanidaOrderFlowPoller`, `KanidaMktBackfill`, `KanidaTickCapture`); `schtasks` query returned nothing under git-bash (likely a shell/quoting issue), so treat the exact registered names/triggers as *unverified* — confirm with `schtasks /query` in PowerShell before touching them.
- **Assumption:** `worked_order`'s `ohlc_1min` read is the *only* execution dependency on the 95.7 M-row deep table, and it is fail-safe (both hold true in the code read). If a future strategy adds a hard (non-fail-safe) `ohlc_1min` read, the profile-artifact approach must be revisited.
- **Assumption:** no intraday AutoTrade session runs outside 09:15–15:30, so the closed-window cutover has no concurrent live reader. Positional/overnight strategies do NOT use the sink (they use LTP/kill/GTT), so this holds — but re-verify before the flip if a new overnight strategy that reads the sink ships.
- **Not a gap, stated plainly:** the sink split can be done **without a live-execution safety gap** because both consumers fail-safe and the kill switch / GTT never read the sink. The only real *operational* cost of an imperfect cutover is a few days of degraded fill-pacing + delayed Tesla signals, mitigated by pre-seeding. The genuine hard part is the **EFS/poller-topology coupling** (§4), not the data move itself.
