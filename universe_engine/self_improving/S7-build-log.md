# S7 build log — Auto-Trade Readiness (Table 14) + the 3 remaining Excel outputs

**Goal.** Step 7 of the Self-Improving Engine: classify auto-trade readiness
(spec Table 14) at BOTH the pattern level and the pick level, prove (or honestly
disprove) that the AUTO_TRADE gate concentrates winners, and emit the three
remaining workbooks. Pure classification + aggregation + export over Steps 0-6
data — **NO walk-forward re-simulation**.

**File added:** `universe_engine/self_improving/build_autotrade_readiness.py`.

Nothing run, nothing committed. RND-only writes; PROD read-only (and this step
needs no OHLC, so PROD is never even opened). Additive / idempotent. No PROD /
shared-engine-code mutation (INV2). **Cannot run python in this environment** —
validated by reading, mirroring the sibling steps; `--dry-run` provided.

---

## 1. Conventions mirrored from Steps 3-6

- utf-8 stdout/stderr reconfigure; backend import root (`<repo>/backend` on
  `sys.path`, import `power_user.services.persona_simulator`).
- `--rnd-db` (optional, falls back to `_resolve_rnd_db_path()`), `--prod-db`
  (default `PROD_DB`), `--dry-run`, `--out` (default `out/v7`).
- openpyxl with `.csv` fallback; `_round` for numeric cells; bold header rows.
- IST date via explicit `timezone(timedelta(hours=5, minutes=30))`.
- Idempotency: taxonomy + baseline decisions are **UPDATE-in-place by primary
  key** (recompute + overwrite → re-run = same result). Schema ALTERs are
  PRAGMA-guarded (added only if absent). Single `BEGIN…commit` for the two
  UPDATE writes; rollback on any error.

## 2. Table 14 → VERIFIED-column mapping (the single source of truth)

Precedence = MOST-RESTRICTIVE-WINS: evaluate AVOID → WATCHLIST → MANUAL_REVIEW;
none ⇒ AUTO_TRADE.

| Table 14 criterion | Column used | Level |
|---|---|---|
| multiplier | latest-week `falcon_pattern_weekly_state.weight_multiplier` (MAX(week_ending) **per pattern**) → fallback `taxonomy.weight_multiplier` → 1.0 | pattern |
| multiplier | mean of fired patterns' latest-week multiplier → fallback `baseline_trades.pattern_weight_multiplier` → 1.0 | pick |
| quality_flag / maturity / big_loser_risk / signal_valid_at_open (=`signal_validity_next_day_pct`) / intraday_false_positive_rate | `falcon_pattern_taxonomy` | pattern; pick = **WORST of fired patterns** |
| recent INIT_STOP | pattern's last 2 RESOLVED appearances (contributions⋈closed baseline_trades, signal_date desc); 2+ ⇒ AVOID, 1 ⇒ MANUAL | pattern; pick = same on **stock+pattern** history |
| constitutional violation | 2+ INIT_STOP in last 60d (Rule #6) ⇒ AVOID | both |
| HFCL | maturity=insufficient_data OR n_resolved<18 ⇒ WATCHLIST | both |
| repeater_type | `baseline_trades.repeater_type` (FRESH/HEALTHY_REPEATER ok; STALE⇒MANUAL; PERSISTENCE_TRAP⇒WATCHLIST) | **pick only** |
| sector headwind | `sector_regime_on_signal_date` → else `move_type∈{SECTOR_HEADWIND,SECTOR_DRAGGED,STOCK_WEAKNESS}` → else `sector_tailwind==0` | **pick only** |
| signal_valid_at_open (pick) | `signal_still_valid_at_open` (0 ⇒ <60 ⇒ WATCHLIST) | **pick only** |
| capitulation% | regime/capitulation tags absent on these patterns | **DEFERRED — never fabricated** |

## 3. What could NOT be evaluated at pattern level (stated, not faked)

`repeater_type`, current sector headwind/regime, and capitulation% describe a
specific signal DAY, not a pattern's lifetime. At the pattern level they are
treated as not-triggered and the `autotrade_block_reason` explicitly says
*"(repeater_type / current sector-regime / capitulation% are evaluated at signal
time (pick level)).”* At the PICK level the first two ARE evaluated.

## 4. VERIFIED-BY-READING caveat on pick-time columns (Verify-before-asserting)

The Step-7 brief asserted `falcon_baseline_trades` "ALREADY HAS" pick-level
inputs `repeater_type`, `sector_regime_on_signal_date`,
`signal_still_valid_at_open`, `pattern_weight_multiplier`. Reading the build logs
shows otherwise:

- **S2-build-log** explicitly lists `repeater_type`, `signal_still_valid_at_open`,
  `sector_regime_on_signal_date`, `autotrade_*` as **deferred to later steps**
  ("repeater step", "falcon_signal_validity step", "regime step"). `grep` over
  `*.py` finds **no step that writes `repeater_type` or
  `signal_still_valid_at_open` into baseline_trades** — they exist as SCHEMA
  columns only (NULL). `pattern_weight_multiplier` is likewise unwritten.
- **S2/S3 logs** confirm `sector_regime_on_signal_date` is NULL (no regime tags).
- **Populated** (verified via the writer column lists / Part A): `move_type`,
  `sector_tailwind` (Step 3 Part A), `net_pnl`, `net_ret_pct`, `exit_reason`,
  `engine_rank`, `top_3_pattern_ids` (Step 2).

**Handling:** every possibly-NULL pick-time column is read defensively (column
existence PRAGMA-checked; NULL = "unknown / not triggered", **named as such in
the block_reason**, never invented). The sector-headwind check therefore falls
back NULL-regime → move_type → sector_tailwind (the last IS populated), so most
picks still get a real headwind verdict. `repeater_type`/`signal_still_valid_at_open`
NULL on every pick today ⇒ those two clauses are inert until their upstream steps
run; the reason string flags "repeater_type NULL / signal_still_valid_at_open
NULL" so the auditor sees exactly why. If/when those columns get populated the
classification activates with zero code change.

## 5. The validation GATE (Constitutional #14 — honest)

`build_pick_validation` groups CLOSED picks (net_ret_pct or net_pnl NOT NULL) by
`autotrade_decision` → n, %picks, win-rate, avg net_ret_pct, summed net_pnl, %
of total winners captured; plus an `ALL_TOP10` reference row. Verdict =
"CONCENTRATES winners" only if AUTO_TRADE WR > full-set WR **and** avg-return
lift ≥ 0; otherwise it reports honestly that the gate is a RISK filter, not a
return amplifier. Printed to stdout AND written to the `pick_validation` tab.

> NOTE (in-sample): the gate is computed on the same managed-portfolio trades the
> rules were calibrated against; it measures whether the rules *select* the
> better trades, not OOS edge. Stated in the workbook note.

## 6. Early-exit study (deliverable 5)

Per pattern, join `falcon_signal_day_study` (persona `falcon_top10_daily`) ↔
`falcon_pattern_contributions` (persona `falcon_top10`) on **(signal_date,
symbol)** — distinct personas, same coordinates (matches the brief). Average
`d{1..7,8,10,15,20,30,45,60}_close_ret` + `post_hold_peak_high_ret`. `avg_peak_day`,
`expected_value_at_hold_end`, `expected_value_at_peak` reused from Step 5's
`falcon_big_winner_loser_study`. **optimal_hold_day** = the offset maximizing avg
cumulative close_ret (close_ret IS the cumulative-from-entry return at that day).
HFCL: a recommendation is emitted only when n_occ≥18; else `INSUFFICIENT_DATA`.
One row per pattern + an `ALL_PATTERNS` summary row (avg by offset). The
EV(peak)−EV(hold-end) gap quantifies the "engine exits too early" finding.

## 7. Weekly review (deliverable 6)

`MAX(week_ending)` row(s) from `falcon_weekly_review_log` for persona
`falcon_top10` (falls back to global latest, and matches `persona IS NULL` rows
too, since Step-6 may write persona on the row). Tab `latest_review` = the log
counts/arrays/`summary_text`/`review_status`; tab `changed_patterns` = that
week's `falcon_pattern_weekly_state` rows with `status_change=1` (old→new
multiplier + reason).

## 8. pattern_id TEXT-vs-INTEGER bridge

The taxonomy PK is TEXT in some DBs (publish test fixture) but the self-improving
steps treat pattern_id as INTEGER (contributions/weekly_state). All cross-table
lookups go through `_lookup` / `_pid_variants`, which try the raw value, its
`int()`, and its `str()` — so joins resolve regardless of the stored type.

## 9. Idempotency / safety summary

- ALTER: only if absent (PRAGMA-checked) — safe to re-run.
- taxonomy.autotrade_decision/block_reason: UPDATE by pattern_id (overwrite).
- baseline_trades.autotrade_decision/block_reason: UPDATE by id (overwrite).
- `--dry-run`: computes + prints everything (incl. the gate table), `con.rollback()`,
  writes nothing (no DB, no Excel).
- PROD never opened.

## 10. Deviations / open risks

- **Pick-time NULL columns** (§4): `repeater_type`, `signal_still_valid_at_open`,
  `pattern_weight_multiplier`, `sector_regime_on_signal_date` are NULL today;
  their clauses are inert and the reason string says so. Not a code bug — upstream
  data gap. The pick multiplier therefore comes from the fired-patterns' weekly
  multipliers (primary method), which IS populated.
- **capitulation%**: deferred (no regime tags) — never emitted, documented.
- **In-sample gate** (§5): noted in the workbook; not an OOS claim.
- Could not run python/sqlite3 to dry-run; logic validated by reading against the
  confirmed schemas in `schema_self_improving.sql` / `schema_signal_day_study.sql`
  / `taxonomy_columns.sql` and the sibling builders. Orchestrator will dry-run +
  audit + apply + commit.

---

## POST-BUILD (orchestrator) — RED→GREEN fixes + honest validation

The first audit returned **RED**: a NULL required-positive AUTO_TRADE criterion was
treated as "pass" (`rep_ok = ... or repeater is None`), over-granting AUTO_TRADE on a
real-money gate. Verified independently that `repeater_type`, `signal_still_valid_at_open`,
`pattern_weight_multiplier`, `sector_regime_on_signal_date` are **0% populated** on
baseline_trades (the earlier "VERIFIED populated" brief claim was wrong — columns exist,
never written). Fixes applied by the orchestrator (re-audited GREEN):

1. **NULL semantics (the RED fix).** `classify_pick` now splits required positives into
   `auto_failed` (definitive) + `auto_unconfirmed` (unknown); AUTO_TRADE is granted ONLY when
   avoid/watch/manual/auto_failed/auto_unconfirmed are ALL empty. A NULL repeater / unknown
   sector / unknown signal_valid → MANUAL_REVIEW, never AUTO_TRADE. Same principle added to
   `classify_pattern` (NULL sig_valid or intraday_fp → MANUAL, was "NA>=60"→AUTO).
2. **signal_valid sourced from the POPULATED table.** `falcon_signal_validity.valid_at_next_open`
   keyed (signal_date,symbol) (99.3% coverage), since baseline_trades.signal_still_valid_at_open
   is always NULL. 1→pass, 0→WATCHLIST, None→unconfirmed.
3. **Dominant-pattern aggregation (supersedes "WORST-of-fired" in §2/§4 above — those are STALE).**
   Worst-of/min-across-fired was pathological at pick level (a pick firing many patterns almost
   always has one thin/noisy co-pattern → it AVOIDed 99% then WATCHLISTed 74%). Pick-level
   quality_flag / pattern_maturity / intraday_fp / n_resolved now come from the **DOMINANT fired
   pattern** (highest oos_lift via `_load_oos_lift`). EXCEPTION — genuine risk gates stay any-of/
   worst-of: **big_loser_risk** (any-of), **recent INIT_STOP** + **constitutional** (worst across
   stock+pattern history). Risk gates are NOT dominant-masked (audit-confirmed).
4. **Validation made honest + meaningful.** Added TRADEABLE_ex_AVOID and AUTO_TRADE_PENDING_REPEATER
   cohorts + a `gate_removes_losers` verdict (AVOID WR < ALL WR AND tradeable WR lift > 0).
5. Fixed a latent crash: `'%,.0f'` → `'%.0f'` in print_pick_validation.

### Honest result (in-sample, applied to out/v7)
- Per-pattern: AUTO_TRADE 48 / MANUAL_REVIEW 39 / WATCHLIST 1255 / AVOID 601 (of 1943).
- Per-pick: AUTO_TRADE **0** / MANUAL 496 / WATCHLIST 1873 / AVOID 639. (0 AUTO_TRADE because
  repeater was never recorded historically — a data gap, not a strategy result; the live system
  records repeater at signal time.)
- **Gate as a RISK FILTER (the real, deployable finding):** AVOID = 624 closed picks, WR **46.3%** /
  avg **+1.34%** vs the full top-10 set's 70.5% / +5.22%. Removing AVOID → tradeable set 2290 picks,
  WR **77.0%** (+6.6pp) / avg **+6.27%** (+1.06pp). **The gate works as a risk filter** — it isolates
  the worst ~21% of picks (constitutional #14). Reported in-sample, not dressed up.
- Early-exit study: 869 patterns, 702 with an optimal-hold rec (n≥18); ALL-pattern avg close peaks
  at **D+60 (+13.58%)** vs the 7-day hold — quantifies the early-exit give-back (Steps 2-5 finding).
- Weekly review: latest week 2026-06-21.

NOTE: §2 mapping table row "pick = WORST of fired patterns" and §4 "NULL-as-not-triggered" are
SUPERSEDED by items 1+3 here. Risk gates remain worst-of; signal-quality attrs are dominant-pattern;
NULL required criteria force MANUAL_REVIEW (not pass).
