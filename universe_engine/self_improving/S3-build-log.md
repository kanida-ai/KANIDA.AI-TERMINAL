# Step 3 Build Log — Pattern Suitability Classification

**Date:** 2026-06-15 (IST) · **Agent:** BuildAgent-S3 · **Worktree:** `C:\Users\SPS\Desktop\kanida-dev` (branch `feat/self-improving-engine`)
**Scope:** Create `universe_engine/self_improving/classify_patterns.py` — sector attribution on `falcon_baseline_trades` (Part A), per-pattern classification into `falcon_pattern_taxonomy` new columns (Part B), and `falcon_pattern_health_report.xlsx` (Part C). RND DB only. No commits. No existing/shared engine code modified (INV2). **Not executed** (no Python in env — validated by careful reading). Provides `--dry-run`.

## File created
| File | Purpose |
|---|---|
| `classify_patterns.py` | Step-3 classifier. Reuses persona loaders read-only; ports `sim_sweep.build_sector_indices` math; writes Part A (`UPDATE falcon_baseline_trades`) + Part B (`UPDATE falcon_pattern_taxonomy`) in one transaction; emits the health report. CLI: `--rnd-db` (required), `--prod-db`, `--out`, `--dry-run`. |

## HFCL discipline (the non-negotiable rule) — enforcement points
`HFCL_MIN_N = 18` resolved occurrences is required before ANY flag is asserted. Every place a conclusion could be drawn is gated:
1. **`big_loser_risk`** — set to `1`/`0` only when `n >= 18` AND `big_loser_rate` AND `EV` are both available; below 18 → `NULL` (not `0`-as-fact). The positive flag additionally requires `big_loser_rate >= 0.30` **AND** `EV (mean net_ret_pct) <= 0` — i.e. big-loser-heavy *and* not positive-EV across ALL occurrences.
2. **`quality_flag`** — `n < 18` → `'INSUFFICIENT_DATA'`. STOCK_SPECIFIC_ALPHA / SECTOR_FOLLOWER only assessed at `n >= 18`.
3. **`pattern_maturity`** — `n < 18` → `'insufficient_data'`. The lowest maturity cutoff is deliberately the HFCL gate.
4. **Persona suitability (`swing_suitable`)** — only set when `n >= 18` AND `EV` known (`1` if EV>0 else `0`); below 18 → `NULL`.
5. **All OTHER persona-suitability flags** (positional/longterm/btst/intraday/short/index/fo_only) → `NULL` always, because **zero trades exist for those personas** (Step 2 only produced `falcon_top10`). Documented "no data for persona"; **not fabricated to 0**.
6. **"Resolved" denominator** — a pattern occurrence counts only if its parent trade is **closed** (`falcon_baseline_trades.net_ret_pct IS NOT NULL`). Open-at-end trades (Step-2 NULLs them) are excluded from every aggregate, so n is the true resolved count.

`win_rate` and `EV` are computed across **all** resolved occurrences (never one example), and the report prints them per pattern: "n_resolved, win_rate_pct, expected_value_pct" — the spec's required "Setup X appeared N times … EV +x%" framing.

## Part A — sector attribution (`falcon_baseline_trades`)
**Method.** Build an equal-weight, daily-rebalanced **chained sector index** (base 100) per sector — **ported verbatim** from `sim_sweep.build_sector_indices` (engine worktree `infallible-mcclintock-225f46/sim_sweep.py:115-140`). For each day, the index multiplies by `(1 + mean of member daily returns)`; on days with no member return the level carries forward. Source: `falcon_sectors` (symbol→sector) + `ohlc_daily.close`, from **PROD DB** (read-only) — the same tables `build_sector_map` uses.

> `sim_sweep` is **not on this branch** (it lives in a separate engine worktree). Rather than import across worktrees (fragile, and would pull leverage/sweep machinery), its ~20-line **pure** function is re-implemented as `_build_sector_indices` with identical math. INV2 (no shared-code modification) is honoured — nothing in the engine tree is touched.

**Per trade** (closed trades only): `sector_ret_same_period` = index return over the trade's own `[entry_date, exit_date]` window (same endpoints the stock leg uses); `stock_vs_sector` = `net_ret_pct − sector_ret`; `move_type` via `_classify_move_type`; `sector_tailwind` = `1 if sector_ret > 0 else 0`. **UPDATE in place** (additive — these columns were Step-2 NULLs).

**move_type definitions** (spec vocabulary):
| Condition | move_type |
|---|---|
| stock up, sector up, stock ≥ sector | `STOCK_LED` |
| stock up, sector up, stock < sector | `SECTOR_DRIVEN` |
| stock up, sector down | `SECTOR_HEADWIND` (won vs falling sector = stock alpha) |
| stock down, sector down | `SECTOR_DRAGGED` |
| stock down, sector up | `STOCK_WEAKNESS` |

**Left NULL (documented).** A trade's sector fields stay NULL when: it is open-at-end (no closed window); it has no sector in `falcon_sectors`; its sector has **< `SECTOR_MIN_PEERS = 3`** distinct member symbols in the OHLC window (index too thin to trust — `sim_sweep` had no such guard; added per the Step-3 instruction); or an endpoint date falls outside the index's covered range. The run prints the count in each NULL bucket.

## Part B — per-pattern classification (`falcon_pattern_taxonomy`, RND)
Patterns classified = every `pattern_id` present in `falcon_pattern_contributions` (persona=`falcon_top10`) with ≥1 **resolved** occurrence. Aggregated by joining contributions → baseline_trades on `trade_id`.

> **Part A → Part B ordering fix:** the sector-split win-rates need Part A's `move_type`/`sector_tailwind`, but those columns are not written to the DB until the end of the run (and never in `--dry-run`). So Part B does **not** read them from the DB — it consumes Part A's **in-memory** `{trade_id: {move_type, sector_tailwind}}` map. This makes Part B correct on a *fresh* run and identical between dry-run and apply.

### Columns POPULATED (and how)
| Column | Source / rule |
|---|---|
| `n_resolved_trades_total` | count of resolved occurrences |
| `n_resolved_trades_60d` | resolved occurrences with `signal_date >= max(signal_date) − 60 calendar days` |
| `typical_peak_day` | mean `peak_day_during_hold` over occurrences having one |
| `early_peak_rate` | rate `peak_day <= 2` |
| `late_peak_rate` | rate `peak_day >= 5` |
| `peak_sustained_rate` | rate `peak_sustained == 1` |
| `win_rate_sector_tailwind` | win-rate (%) over occurrences with `sector_tailwind == 1` |
| `win_rate_sector_headwind` | win-rate (%) over `sector_tailwind == 0` |
| `win_rate_sector_neutral` | win-rate (%) over **unattributed** occurrences (`sector_tailwind is None`) — there is no "flat sector" bucket; 'neutral' honestly = 'no attribution' (documented) |
| `big_winner_rate_by_persona` | JSON `{"falcon_top10": rate}` (only persona with data) |
| `big_loser_rate_by_persona` | JSON `{"falcon_top10": rate}` |
| `big_loser_risk` | HFCL-gated (see above): `1`/`0` only at n≥18 with EV check; else NULL |
| `quality_flag` | spec Phase-2 Update-1 (see thresholds); n<18 → INSUFFICIENT_DATA; no clean signature → NULL |
| `pattern_maturity` | n cutoffs (see thresholds) |
| `swing_suitable` | n≥18 & EV>0 → 1, n≥18 & EV≤0 → 0, else NULL (falcon_top10 = the 7-day EOD swing data) |
| `last_classification_date` | today (IST, computed via explicit Asia/Kolkata tz — per always-use-IST rule) |
| `classification_version` | `1` (bumped from Step-1 default 0) |

### Thresholds (exact, single source = constants in the script)
- **HFCL:** `HFCL_MIN_N = 18`.
- **pattern_maturity:** `insufficient_data` (n<18) · `emerging` (18–49) · `established` (50–149) · `stable` (≥150). Rationale: lowest bound == HFCL gate so "insufficient_data" ≡ "below HFCL"; 50/150 give meaningful "enough to trust" / "deep history" tiers.
- **quality_flag:**
  - `STOCK_SPECIFIC_ALPHA` ⟸ `win_rate_sector_headwind >= 50%` **AND** `n_headwind >= 10`.
  - `SECTOR_FOLLOWER` ⟸ `win_rate_sector_tailwind >= 60%` **AND** `win_rate_sector_headwind < 40%`.
  - `INSUFFICIENT_DATA` ⟸ `n < 18`.
  - else (n≥18, no clean signature) ⟸ `NULL` (REGIME_SPECIFIC **not** emitted — see deferred).
- **big_loser_risk:** `1` ⟸ `n>=18` AND `big_loser_rate >= 0.30` AND `EV <= 0`; `0` ⟸ n≥18 but condition unmet; `NULL` ⟸ n<18.
- **peak buckets:** early `peak_day<=2`, late `peak_day>=5`.
- **sector index thin guard:** `SECTOR_MIN_PEERS = 3`. **60-day window:** `RECENT_WINDOW_DAYS = 60` calendar days.

### Columns DEFERRED — left NULL, NOT fabricated (with reasons)
| Column(s) | Why deferred |
|---|---|
| `signal_validity_next_day_pct`, `avg_signal_decay_days`, `signal_best_entry_window` | **Step 4** (signal validity). Needs next-open/intraday re-evaluation of each signal. |
| `intraday_detectable`, `intraday_detection_accuracy`, `intraday_false_positive_rate` | **Step 4** (1-min data, from 2024-05-13 only). |
| `realized_lift_60d`, `weight_multiplier`, `previous_multiplier`, `status` | **Phase 2 / Step 6** (weekly self-improvement). Step-1 default `weight_multiplier=1.0` preserved (not overwritten). |
| `regime_best_fit`, `sector_regime_fit` | No market/sector **regime tags** exist on these trades — Step-2 left `market_regime_on_signal_date` / `sector_regime_on_signal_date` NULL, and the pattern tables have no regime column. We do **not** invent a regime; left NULL + this note. (This is also why `quality_flag` never emits `REGIME_SPECIFIC`.) |
| `typical_peak_interval` | Intraday-interval concept (persona='intraday'); N/A for EOD Falcon Top 10. |
| `positional/longterm/btst/intraday/short/index_suitable`, `fo_only` | **No trades for those personas** (only `falcon_top10` exists). Cannot assess suitability without ≥18 resolved trades for THAT persona → NULL ("no data for persona"). |

## Part C — `falcon_pattern_health_report.xlsx`
One row per classified pattern, columns: `pattern_id, mined_years, quality_flag, pattern_maturity, n_resolved, n_resolved_60d, win_rate_pct, expected_value_pct, big_winner_rate, big_loser_rate, big_loser_risk, typical_peak_day, peak_sustained_rate, early_peak_rate, late_peak_rate, win_rate_sector_tailwind, win_rate_sector_headwind, win_rate_sector_neutral, swing_suitable`. Uses **openpyxl**; if the import fails it writes `falcon_pattern_health_report.csv` instead and prints a note. Output dir via `--out` (default `universe_engine/self_improving/out/`).

## Bucket counts (maturity / quality)
**Computed at runtime, not hardcoded.** The run prints `maturity_buckets`, `quality_flag buckets`, `n_big_loser_risk`, `n_swing_suitable`. Exact counts cannot be produced in this env (no Python). The audit agent should capture them from a `--dry-run`. *Expectation* given the Step-2 totals (3,008 baseline trades / 214,916 contributions over ~5.4 years): contributions are dense, so many patterns will clear n≥18 — but per-pattern n varies widely, so a substantial tail will be `insufficient_data`. Verify at dry-run.

## Idempotency & safety
- **RND only.** Part A `UPDATE falcon_baseline_trades`; Part B `UPDATE falcon_pattern_taxonomy WHERE pattern_id = ?`. **No PROD write.** PROD DB opened read-only (OHLC + falcon_sectors for the index).
- **Single transaction** (`BEGIN` … `commit`/`rollback`). Both parts in one txn; on exception → rollback, nothing applied.
- **Additive / re-runnable.** Every run recomputes and **overwrites** only this step's owned columns in place (`_TAXONOMY_WRITE_COLS`). No DROP/rename/retype; no new rows; other personas' baseline rows untouched; taxonomy rows for patterns we don't classify are untouched.
- **Column-presence guard.** Writes only taxonomy columns that actually exist (`PRAGMA table_info`), so a RND DB where the Step-1 ALTERs weren't applied degrades gracefully (warns, skips the absent columns / Part B). Missing `falcon_pattern_taxonomy` → Part B skipped, Part A + report still run.
- **`pattern_id` not in taxonomy** → `UPDATE` affects 0 rows; counted as `n_taxonomy_missing` and reported (classification still appears in the report). This happens when a pattern fired in trades but has no taxonomy row.
- **`--dry-run`** runs Parts A+B and the full report compute, prints the bucket summary + an 8-row report preview, and writes **nothing** (no DB, no file).

## Risks / notes for the audit agent
1. **Not executed** (no Python). Run `--dry-run` first; confirm: Part A NULL buckets are sane (most closed trades attributed; open-at-end + thin-sector counts explained), maturity/quality buckets look reasonable, and `big_loser_risk=1` count is small (HFCL should make it rare).
2. **`win_rate_sector_*` are PERCENTS (0–100)** to match the spec's quality thresholds (50/60/40). The big_winner/loser/peak *rates* are **fractions (0–1)** as in the report. Keep this in mind when reading the columns.
3. **`win_rate_sector_neutral` = unattributed**, not "flat sector". The binary tailwind split has no flat bucket. If a true neutral band is wanted later, define it on `|sector_ret| < ε` — left out deliberately so the STOCK_SPECIFIC_ALPHA / SECTOR_FOLLOWER tests use a clean up/down split.
4. **Sector index = current `falcon_sectors` membership applied retroactively** (same known caveat the spec flags for the universe). Equal-weight, no survivorship correction beyond what `falcon_sectors` carries.
5. **`oos_lift_at_mining` comparison NOT done here** — that drives Phase-2 multipliers (Step 6), out of scope. `realized_lift_60d` left NULL.
6. **`swing_suitable` semantics:** treated as "is this pattern profitable on the Falcon Top-10 7-day EOD swing horizon" (EV>0 over ≥18 resolved). If the auditor prefers `swing_suitable` to stay NULL until a *dedicated* swing persona runs, flip the one block in `classify_one_pattern`. Documented as a judgement call.
7. **PROD-DB choice:** the index uses `--prod-db` (default persona `PROD_DB`). Confirm it's the same DB Step 2 used for OHLC, so entry/exit dates resolve to index levels (they share `ohlc_daily.trade_date`).
8. **net-vs-gross in `stock_vs_sector`:** the stock leg is `net_ret_pct` (after 5bps×2 slippage + 30bps round-trip), the sector leg is a gross price index. `stock_vs_sector` therefore measures realized-stock-return vs passive-sector-benchmark (a deliberate, defensible choice — it is what the trade actually earned over the sector). `move_type` and `sector_tailwind` are unaffected (signs unchanged at this slippage scale, except a hair near zero). Documented, not a bug.
