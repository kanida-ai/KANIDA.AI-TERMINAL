# S6 — Phase 2 Weekly Self-Improving Layer (build log)

**File:** `universe_engine/self_improving/build_self_improvement.py`
**Spec:** "Phase 2 — Weekly Self-Improving Layer" (the 5 updates + Improved Score
Formula) + "Constitutional Rules" (#1–#11 in scope here).
**Step in build sequence:** 6 (gate = generate `falcon_self_improvement_comparison.xlsx`).
**Nature:** RE-RUN of the locked Falcon Top 10 walk-forward with the ONLY change
being the per-signal-date RANKING (improved_score). All trade mechanics LOCKED.
**CANNOT run Python in env** — validated by reading; `--dry-run` provided.

| Item | Value |
| --- | --- |
| Script | `build_self_improvement.py` |
| Writes tables | `falcon_pattern_weekly_state`, `falcon_weekly_review_log` (RND DB only) |
| Reads | `falcon_promoted_patterns` + `falcon_pattern_candidates` (patterns), PROD `falcon_features`/`ohlc_daily`/`falcon_sectors` (panel/bars/sectors), `falcon_baseline_trades` (GATE comparison only) |
| Report | `falcon_self_improvement_comparison.xlsx` (`--out`; orchestrator passes `out/v6`) |
| CLI | `--rnd-db` (opt, resolver fallback), `--prod-db` (opt), `--years`, `--dry-run`, `--out` (default `out/v6`) |
| Reuse | `persona_engine_core.simulate_year` **verbatim** (the locked exit), `compute_year_signals` / `eligible_patterns_for_year` / `rule_mask` / loaders; `PERSONA_CONFIGS["falcon-top-10"].run_cfg`; `_resolve_rnd_db_path` / `PROD_DB` from `persona_simulator` |

## The one change vs Phase-1 baseline
Phase-1 (`build_baseline.py`) ranks each day's signals by `avg_lift`
(simulate_year sorts candidates by `s["score"]`, and for falcon-top-10 score is
overwritten to `avg_lift`). Phase-2 overwrites `s["score"]` with:

```
improved_score = avg_lift × pattern_weight_mult × sector_mult × repeater_mult
```

then groups by `signal_date` and calls `simulate_year` **unchanged**. The exit
rules (−7% init stop, +12% trail trigger, 10-day Donchian floor, 7-TD max hold,
₹50k/trade, skip_already_held, 5bps slip, 30bps round-trip, ₹5L/yr reset,
cash-bounded, integer shares) are NEVER touched — Constitutional Rule #7.

## Reuse of simulate_year (file:line)
- `persona_engine_core.simulate_year` — `backend/power_user/services/persona_engine_core.py:311`.
  Called once per calendar year at `build_self_improvement.py` run-loop
  (`r = simulate_year(dict(sigs_by_sd), bars, year_td, run_cfg, cash_start)`).
- `compute_year_signals` (core:181), `eligible_patterns_for_year` (core:169),
  `rule_mask` (core:154), loaders (core:61/86/109/127/140) — all read-only reuse.
- run_cfg = `PERSONA_CONFIGS["falcon-top-10"]["run_cfg"]`
  (`persona_simulator.py:318`): top_n=10, fixed_per_trade=50_000, hold_days=7,
  init_stop=−0.07, trail_trigger=0.12, trail_lookback=10, sort_key="avg_lift",
  min_fires=10, group_by_signal_date=True.

## NO-LOOKAHEAD CAUSALITY MODEL (the #1 audit item)
**Week boundary.** Every signal_date D maps to `W(D) = Monday of D's ISO week`
(`_week_boundary` = `D − weekday(D) days`). The multiplier for any signal in
that week may use ONLY ledger trades with `exit_date < W` — trades fully closed
*before* that Monday (Constitutional Rule #9 "weekly only, no intra-week ranking
change").

**Enforced in code** in the `ImprovedLedger` causal slices — every one filters
`exit_date < W` (and `>= W − lookback`):
- `pattern_trades_60d(pid, W)` → U1
- `sector_trades_30d(sector, W)` → U2
- `symbol_trades_60d(symbol, W)` → U3
- `update5_bigwinner(pid, W)` → U5 (filters `exit_date < W`)
- `init_stop_cap_active(symbol, W)` → Rule #6

**Year-atomic simulate_year — the conservative simplification.** simulate_year
is inherently year-scoped (₹5L reset, single cash pool) and the "reuse the
locked engine / do not re-implement the exit" constraint forbids slicing the
year into per-week calls. So a year's weekly multipliers are all frozen from the
ledger **as of the START of year Y** (= 2021..Y-1 improved trades only). This is
**strictly no-lookahead — stronger than required**:
- A week in year Y can never see ANY same-year trade. (It couldn't anyway: a
  7-day-hold trade entered in week K of Y resolves *inside* the same
  `simulate_year(Y)` call, so its exit_date is unknowable until the call
  returns.)
- The ledger is appended with year Y's resolved trades **after**
  `simulate_year(Y)` returns (`_append_year_to_ledger`), so they influence Y+1
  onward only.
- **Cost:** within-year week→week feedback is deferred to next-year granularity;
  cross-year feedback is fully live. 2021 (empty pre-Y ledger) → all multipliers
  default 1.0 → improved ≈ baseline (the spec SANITY check) holds exactly.
- **Path to true intra-year weekly feedback (documented, not built):** a custom
  driver threading cash + open positions across per-week simulate_year calls —
  requires re-implementing the year reset OUTSIDE simulate_year, which the
  no-re-implement-exit constraint disallows for this step.

**Lookahead audit surfaces checked (all clean):**
1. Multiplier slices — all gated `exit_date < W`. ✔
2. Streaks (`consecutive_days_in_top50`) — `_advance_streaks` folds only
   signal-days STRICTLY BEFORE the current signal_date (cursor monotonic over
   chronological `compute_year_signals` output). ✔
3. `oos_lift` — a static mining-time constant (`avg_oos_year_lift_pp`), no
   forward data. ✔
4. Constitutional caps — derived from `symbol_trades_60d(W)` (pre-W only). ✔
5. Comparison/GATE — reads `falcon_baseline_trades` only AFTER the whole sim;
   never feeds multipliers. ✔

## The self-consistent improved ledger loop
Multipliers MUST come from the IMPROVED sim's own resolved trades (the avg_lift
baseline trades are a *different* trade set). Cannot pre-resolve then rank:
ranking in week W depends on trades resolved before W, themselves produced by
the improved ranking earlier. So one forward pass interleaves:
1. **Freeze** week multipliers from the ledger (`update1..3`, cached per (W,pid)
   / per W).
2. **Re-score** each signal → `improved_score`; group by signal_date.
3. **simulate_year(Y)** (locked) → closed + open trades.
4. **Append** year Y's CLOSED trades to the ledger (`_append_year_to_ledger`) →
   eligible for future weeks only.
`falcon_baseline_trades` is read ONLY for the GATE.

`_fired_pids` recovery: simulate_year rebuilds candidate dicts with a fixed key
set (drops our `_fired_pids`), so closed-trade pattern attribution is recovered
by `(symbol, signal_date)` from the rescored signals
(`fired_by_key`), then re-derived patterns are exactly the same fire-set the
sim's `n_fires` counted (`rule_mask` against the signal's own panel row — same
machinery as `build_baseline.derive_contributions_for_trade`).

## The 5 weekly updates — formulas + clamps + regime memory
`oos_lift` (a.k.a. `oos_lift_at_mining`) per pattern = `avg_oos_year_lift_pp`
(`_load_oos_lift`, from `falcon_promoted_patterns`; identical to the value
build_baseline summed and stored as `falcon_pattern_contributions.lift_pp`).

### U1 — Pattern realized lift (60d) — `update1_pattern`
`trades_60d` = ledger trades where this pattern fired, `exit_date ∈ [W−60d, W)`.
- **n < 18 → REGIME MEMORY:** keep the pattern's PREVIOUS multiplier (carried in
  `led.prev_pattern_mult`, default 1.0; **never reset to 1.0**), status `Keep`,
  status_change=0. (Constitutional Rule #11.)
- else `realized = mean(net_ret_pct)`:
  `realized > oos×1.20 → 1.2 Strengthen` · `realized < oos×0.80 → 0.7 Demote` ·
  else `1.0 Keep`.
- **Sector-quality override** (WIRED BUT INERT IN-PASS — see "Deferred"):
  headwind WR≥50 & n_headwind≥10 → `STOCK_SPECIFIC_ALPHA`, `mult=min(mult×1.1,1.5)`;
  elif tailwind WR≥60 & headwind WR<40 → `SECTOR_FOLLOWER` (no mult change; flag
  used for the later auto-trade block).
- **Clamp → [0.5, 1.5].** prev_pattern_mult updated AFTER recording (chronological
  carry within the year).

### U2 — Sector realized edge (30d) — `update2_sectors`
Per sector with ≥18 resolved trades in `[W−30d, W)`: `edge = mean(net_ret_pct)`;
rank sectors by edge → **top-3 → 1.2**, **bottom-5 → 0.85**, else 1.0. Sectors
with <18 (or unranked) → 1.0 at lookup. **Clamp → [0.75, 1.30].**

### U3 — Repeater quality (rolling) — `update3_repeater`
`prior_60d` = ledger trades on the symbol, `exit_date ∈ [W−60d, W)`. `avg =
mean(net_ret_pct)`; `consec = consecutive_days_in_top50` (causal streak);
`early_exit_count = #trades with peak_ret>15%` (proxy — see "Deferred").
- len<2 → FRESH 1.0
- consec≥3 & avg<0 → PERSISTENCE_TRAP 0.80
- avg>5 & len≥3 & early_exit≥2 → EXTENDED_TREND 1.10
- avg>3 & len≥2 → HEALTHY_REPEATER 1.15
- avg<1 → STALE 0.90
- else → (HEALTHY_REPEATER) 1.0

**Clamp → [0.7, 1.20].** Per-signal pattern multiplier = **mean** of the
signal's fired-pattern multipliers (a signal is a confluence; documented choice).

### U4 — Weekly review log — `_new_review`/`_accumulate_review`/`_finalize_review`
One `falcon_weekly_review_log` row per week: patterns_promoted/demoted,
quality_flag_changes, constitutional_violations (rank-cap events),
summary_text. `human_approved` defaults FALSE (0); `review_status` defaults
PENDING. (Constitutional Rule #16 — human gate.)

### U5 — Big-winner/loser refresh — `update5_bigwinner` + `_apply_u5_to_states`
Per week, for n≥18 patterns recompute `big_winner_rate` / `big_loser_rate` / EV
from ledger trades with `exit_date < W` and store on the weekly_state row.
Mirrors `build_bigwinner_study` (Step 5) HFCL aggregation — the per-week
snapshot; flags reuse the stored Table-8 big_winner/big_loser per-trade flags.
U5 is a STUDY refresh only — it does NOT feed ranking, so it may legitimately use
year-Y resolved trades for year-Y week boundaries (still `exit_date < W`, hence
causally clean for the boundary; no lookahead into ranking).

## Improved Score Formula
`improved_score = avg_lift × pattern_mult × sector_mult × repeater_mult`
(spec exactly). Written into `s["score"]`; simulate_year ranks by it unchanged.

## Constitutional ranking caps — applied vs deferred
- **Rule #6 (2+ INIT_STOP in 60d → rank cap 8): APPLIED.** Derivable from ledger
  `exit_reason`. Enforced WITHOUT mutating simulate_year (Rule #7): a capped
  pick's improved_score is damped (`_apply_rank_cap_to_score`, cap-8 → ×0.45)
  so it cannot occupy a top-7 slot among non-capped peers. **Approximation note:**
  this is a score-penalty proxy for an exact positional cap; the exact cap would
  require a post-ranking re-sort that the locked engine does not expose. The
  event is logged to `constitutional_violations`.
- **Rule #5 (capitulation>40% → rank cap 7): DEFERRED — NOT YET AVAILABLE.**
  Regime / capitulation tags do not exist on these patterns (build_baseline and
  classify_patterns leave `regime` NULL; the `falcon_pattern_taxonomy`
  plain-English regime tags are not joined in this step). `capitulation_share`
  returns None → the cap is a no-op. The hook is wired (just feed it a
  `{pid: is_capitulation}` map) so the cap activates with zero structural change
  once tags land.
- Rules #1/#2 (no lookahead / no pattern leakage) — enforced by the causality
  model + `eligible_patterns_for_year`. #3/#10 (18-min / HFCL) — enforced in
  U1/U5. #4 (bounds) — clamps on every multiplier. #11 (regime memory) — U1
  carry. #7 (locked rules) — simulate_year untouched. #8 (skip_already_held) —
  inherited from run_cfg/simulate_year. #9 (weekly only) — week-boundary freeze.

## The comparison (THE GATE)
`falcon_self_improvement_comparison.xlsx` (tab `baseline_vs_improved`): per-year
**baseline_return_pct** (from `falcon_baseline_trades`: Σ closed net_pnl / ₹5L
× 100 — like-for-like with the improved sim's own year_ret; the canonical locked
headline lives in `build_baseline_summary.xlsx`), **improved_return_pct** (this
sim), **delta_pp**, baseline/improved n_closed + win_rate, and an OVERALL(sum)
row. Honest: where improved ≤ baseline it is shown. SANITY printed: 2021
improved ≈ baseline (empty pre-W ledger → multipliers 1.0). openpyxl with a
`.csv` fallback if openpyxl is missing.

## Deferred / proxied (documented, never faked)
- **Sector attribution in-pass** (`move_type` / `sector_tailwind`) — NULL on the
  improved ledger (classify_patterns Step 3 attributes `falcon_baseline_trades`,
  not this ledger; porting the sector-index build into the forward pass is out of
  scope). Consequence: U1's STOCK_SPECIFIC_ALPHA / SECTOR_FOLLOWER override and
  the weekly_state `win_rate_sector_tailwind/headwind` columns are inert/NULL.
  Logic is fully implemented and activates the moment the ledger carries them.
- **`early_exit_flag`** (post-hold high >+15% in D+8..D+60) — true source is
  `falcon_post_exit_tracking` (a later incremental step). Proxied in U3's
  EXTENDED_TREND test by `peak_ret_during_hold > 15%` (intra-hold peak), the
  closest in-pass signal. Documented.
- **Rule #5 capitulation cap** — deferred (no regime tags), see above.

## Constraints honored
- **NO LOOKAHEAD** — every multiplier/cap slice gated `exit_date < W`; streaks
  causal; simulate_year year-atomic (stronger-than-required).
- **Multiplier bounds** clamped (#4). **Regime memory** (#11) — U1 carry.
- **Trade rules LOCKED** — simulate_year reused verbatim; caps are score-based,
  never engine mutations (#7).
- **HFCL** n≥18 in U1/U5 (#3/#10).
- **Additive / idempotent** — `_write` DELETEs this persona's rows then INSERTs
  in ONE transaction (re-run with same data = same output). RND-DB-only.
  No PROD / shared-engine-code mutation (INV2).
- **`--dry-run`** computes + prints comparison + would-write notice; writes
  nothing (DB or Excel).

## Validation done (read-only; Python not run in env)
- All `run()`-referenced helpers defined (grepped). simulate_year candidate-dict
  key set inspected → `_fired_pids` recovery added (it does NOT survive the call).
- `_WS_COLS` / `_RL_COLS` ⊆ `schema_self_improving.sql` columns for the two
  target tables; JSON-array columns dumped via `json.dumps`.
- `_load_oos_lift` query columns (`pattern_id`, `avg_oos_year_lift_pp`) confirmed
  present on `falcon_promoted_patterns` (same table build_baseline joins).
- Week boundary round-trips: `week_ending = Monday+6` (Sunday);
  `_apply_u5_to_states` recovers Monday via `−6 days`.

---

## POST-BUILD (orchestrator) — GATE BUG CAUGHT + FIXED + HONEST RESULT

After the audit (GREEN), the orchestrator ran the dry-run and scrutinized the gate.

### Bug: apples-to-oranges comparison (would have over-stated improvement by 3.6×)
First dry-run reported **+68.52pp** overall. Tell-tale that this was wrong: 2022
improved == baseline with IDENTICAL n_closed=317 (re-ranking changed nothing — too
little prior ledger), yet the gate showed Δ +8.26 — impossible if the trades are
identical. Root cause: `_load_baseline_returns` computed the baseline as **closed-only**
`sum(net_pnl)/₹5L` from `falcon_baseline_trades`, while the improved sim reports
**equity-basis** returns (incl. open-MTM). The two columns were on different bases.

### Fix
The GATE now runs an in-loop **baseline-EQUITY pass** through the SAME `simulate_year`
(score = avg_lift, multipliers ≡ 1.0). Both columns are now equity-basis from one engine,
so Δpp is PURELY the re-ranking effect. The baseline pass reproduces the locked Step-2
numbers exactly (2021 +5.35 / 2022 +76.36 / 2023 +564.47 / 2024 +469.05 / 2025 +346.64),
self-validating the basis. `_load_baseline_returns` marked DEPRECATED/UNUSED.
(Edits: `build_self_improvement.py` — baseline_year_summaries accumulator + in-loop
baseline pass + GATE wiring + note text.)

### Honest result (equity vs equity, same engine)
| Year | Baseline | Improved | Δpp |
|---|---|---|---|
| 2021 | +5.35 | +5.35 | 0.00 (sanity ✓) |
| 2022 | +76.36 | +76.36 | 0.00 |
| 2023 | +564.47 | +552.00 | −12.47 |
| 2024 | +469.05 | +483.41 | +14.36 |
| 2025 | +346.64 | +370.88 | +24.24 |
| 2026 (partial) | +88.62 | +81.56 | −7.06 |
| OVERALL(sum) | +1550.49 | +1569.57 | **+19.07** |

**Verdict:** re-ranking is marginal + regime-dependent (~+19pp / 5.5yr ≈ +1.2% relative);
helped 2024–25, hurt 2023 + 2026-partial. Matches `engine_p1_5yr_ranker_verdict`: avg_lift
selection is already near-optimal; the real levers are exit timing + entry promptness.

### Applied
RND: falcon_pattern_weekly_state 79,854 rows + falcon_weekly_review_log 233 rows
(persona='falcon_top10'). The 80,605→79,854 gap = 751 year-boundary ISO-week collisions
collapsed by UNIQUE(week_ending,pattern_id,persona); 0 stored dups; benign (diagnostic table,
not read by the sim). Excel: out/v6/falcon_self_improvement_comparison.xlsx.
