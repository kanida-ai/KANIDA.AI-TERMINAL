# S6 Audit — build_self_improvement.py (AuditAgent-S6)

**Target:** `universe_engine/self_improving/build_self_improvement.py` (+ `S6-build-log.md`)
**Scope:** Phase-2 weekly self-improving re-rank of the locked Falcon Top 10 walk-forward.
**Mode:** READ-ONLY on code. Only this file written. Nothing run/committed.

## VERDICT: GREEN

No lookahead, sim reused verbatim, all clamps + regime-memory present, inert parts honestly NULL/no-op (not faked), RND-only idempotent writes, `--dry-run` is a true no-write gate. The annual-freeze simplification and the deferred/inert components are documented limitations, not defects.

---

## 1. NO LOOKAHEAD — CONFIRMED

Every multiplier/cap input is gated `exit_date < W` (W = Monday of the signal's ISO week, `_week_boundary` line 298-301):

- `pattern_trades_60d` line 390-394 → `lo <= exit_date < W` (U1)
- `sector_trades_30d` line 396-400 → `lo <= exit_date < W` (U2, and the inline gather in `update2_sectors` line 477)
- `symbol_trades_60d` line 402-406 → `lo <= exit_date < W` (U3 + Rule #6 cap via `init_stop_cap_active` line 549-553)
- `update5_bigwinner` line 527-528 → `exit_date < W` (U5; study-only, never feeds ranking)
- Streaks: `_advance_streaks` line 809-831 folds only signal-days `< before_sd` via a monotonic cursor — no end-of-year leakage.

**The decisive structural guarantee:** the ledger is appended with year Y's CLOSED trades only AFTER `simulate_year(Y)` returns (`_append_year_to_ledger` call at line 747, definition 834-882). The forward loop runs years ascending (line 624). Therefore year Y's ranking is frozen from the ledger as it stood at the START of Y = 2021..Y−1 improved trades ONLY. There is no code path where a year-Y (or future) trade/outcome reaches year-Y ranking. This is *stronger* than weekly no-lookahead (it is annual-grain feedback) — a deliberate, documented conservative simplification (file header lines 62-89; build log lines 60-79).

**2021 sanity invariant — CONFIRMED.** First year ⇒ empty pre-W ledger. U1: n<18 ⇒ returns `prev` = default 1.0 (line 422-426). U2: no sector ≥18 ⇒ returns `{}` ⇒ 1.0 at lookup (line 480-481, 691). U3: n<2 ⇒ FRESH 1.0 (line 503-504). Caps inactive. So `improved = avg_lift × 1 × 1 × 1 = avg_lift` (line 696) = the baseline ranking key. improved 2021 == baseline 2021 holds exactly.

`falcon_baseline_trades` is read ONLY in the GATE (`_load_baseline_returns` line 757, 1013-1047), after the whole sim — never for multipliers.

## 2. SIM MECHANICS PARITY — CONFIRMED (apples-to-apples)

`simulate_year` is called once per year, verbatim (line 723), the ONLY engine call. No exit/portfolio/cash logic is re-implemented anywhere in the build. The single difference vs baseline is `s["score"] = improved` (line 706); `simulate_year` ranks candidates by `c["score"]` (engine core lines 451-457). Run context (`_build_run_context` line 308-334) mirrors `simulate_persona` exactly: same loaders, same `sim_start/sim_end/cash_per_year` from `PERSONA_CONFIGS["falcon-top-10"]`, same `run_cfg` (top_n=10, fixed_per_trade=50_000, hold_days=7, init_stop=−0.07, trail_trigger=0.12, trail_lookback=10, min_fires=10, group_by_signal_date=True — confirmed persona_simulator.py lines 291-330). ₹5L/yr reset is inherent to the per-year `cash_start` arg. Locked exits (INIT_STOP/TRAIL_GIVEBACK/TARGET/TIME_STOP, slip, fees, integer shares, cash bound, skip_held) live entirely in engine core lines 384-525 and are untouched.

`_fired_pids` recovery (line 745-746) correctly handles that `simulate_year` rebuilds candidate dicts with a fixed key set (engine core line 370-374, 519-523) that drops the custom key — attribution recovered by (symbol, signal_date).

## 3. MULTIPLIER FORMULAS + CLAMPS + REGIME MEMORY — CONFIRMED

- **U1** (`update1_pattern` 413-467): n<18 ⇒ keep PREVIOUS multiplier, never reset (line 422-426, carry in `prev_pattern_mult` default 1.0). Thresholds oos×1.20→1.2 / oos×0.80→0.7 / else 1.0 (line 429-434). Clamp [0.5,1.5] line 465. prev updated AFTER record (line 961). HFCL n≥18 enforced.
- **U2** (`update2_sectors` 470-495): n<18→1.0; top-3→1.2, bottom-5→0.85, else 1.0; clamp [0.75,1.30] line 494.
- **U3** (`update3_repeater` 498-520): len<2 FRESH 1.0; consec≥3 & avg<0 TRAP 0.80; avg>5 & len≥3 & early≥2 EXTENDED 1.10; avg>3 & len≥2 HEALTHY 1.15; avg<1 STALE 0.90; else 1.0; clamp [0.7,1.20] line 520.
- Bound constants exactly match spec (lines 229-267).

## 4. INERT PARTS — HONESTLY HANDLED (not faked)

| Component | State | Evidence |
| --- | --- | --- |
| U1 sector-quality override (STOCK_SPECIFIC_ALPHA / SECTOR_FOLLOWER) | **INERT** | `move_type`/`sector_tailwind` set to `None` on every ledger trade (line 879-880). With all-None, `n_head==n_tail==0` ⇒ neither branch fires (line 448-463); base U1 mult passes through. Documented file lines 439-446, build log 125-128/201-207. |
| weekly_state `win_rate_sector_tailwind/headwind` | **NULL** | Hard-set None (line 946-947). |
| U3 `early_exit_count` | **PROXY (active)** | Proxied by intra-hold `peak_ret > 15%` (line 507-508), real source `falcon_post_exit_tracking` deferred. Documented line 140-142, build log 208-211. |
| Rule #5 capitulation cap | **DEFERRED no-op** | `capitulation_share` returns None (line 556-562) ⇒ cap never added (line 986-988). Hook wired. Documented file 162-166, build log 178-184. |
| **U2 sector-edge multiplier itself** | **ACTIVE** | Needs only symbol→sector (`sector_map`, line 691) + `net_ret_pct` (both present on the ledger). Genuinely live from 2022 onward once a sector accrues ≥18 resolved trades. |

**Net: what is ACTIVE this pass** = U1 pattern realized-lift re-weight (Strengthen/Demote/Keep + regime memory), U2 sector edge, U3 repeater (FRESH/TRAP/EXTENDED/HEALTHY/STALE with peak-proxy), Rule #6 INIT-STOP cap, U4 review log, U5 big-winner/loser study annotations. **INERT/deferred** = U1 sector-quality override, the two sector-WR weekly_state columns, Rule #5 capitulation cap. The core three-multiplier re-ranking is fully active for 2022+; 2021 is identity by construction.

## 5. CONSTITUTIONAL — CONFIRMED

- Bounds clamped on all three multipliers (#4). 
- Rule #6 (2+ INIT_STOP/60d) APPLIED as a documented score-penalty proxy: `_cap_rank_for` (979-989) + `_apply_rank_cap_to_score` (992-1006) damp the score (cap-8 → ×0.45) WITHOUT mutating `simulate_year` (#7 preserved). Honestly flagged as an approximation of an exact positional cap (the engine exposes no post-rank re-sort). Logged to `constitutional_violations`.
- Rule #5 deferred + documented (no fake).
- Trade rules locked — only `s["score"]` changes.

## 6. IDEMPOTENT + SAFE — CONFIRMED

`_write` (1174-1203): single `BEGIN`/commit txn, `DELETE ... WHERE persona=?` then INSERT for both tables, rollback on error. weekly_state uses `INSERT OR REPLACE` against `UNIQUE (week_ending, pattern_id, persona)` — re-run with same data = same rows. RND DB only; PROD opened read-only for OHLC/features. No PROD/shared-engine mutation. `--dry-run` (line 764-767) writes nothing to DB or Excel — a genuine safe gate. Comparison reads baseline from `falcon_baseline_trades` (1013-1047). All `_WS_COLS`/`_RL_COLS` exist in `schema_self_improving.sql` (tables 7 & 8, lines 290-342); JSON-array columns dumped via `json.dumps` (1193-1194).

## Required confirmations
- **(a) No lookahead:** year Y ranking uses only ≤Y−1 resolved trades (ledger appended post-`simulate_year(Y)`); 2021 == baseline (all multipliers 1.0). CONFIRMED.
- **(b) Sim reused verbatim:** single `simulate_year` call, only `s["score"]` differs — apples-to-apples. CONFIRMED.
- **(c) Active vs inert:** ACTIVE = U1 pattern weight (+regime memory), U2 sector edge, U3 repeater (peak-proxy), Rule #6 cap, U4 log, U5 study. INERT/deferred = U1 sector-quality override, sector-WR columns, Rule #5 capitulation cap.
- **--dry-run is the safe gate** (no DB/Excel writes).

## Notes (limitations, NOT must-fix)
1. Annual freeze grain: within-year week→week feedback is deferred to next-year (documented trade-off of reusing the year-atomic locked engine). Cross-year feedback fully live.
2. `_load_baseline_returns` uses closed-only Σnet_pnl/₹5L (open-MTM≈0 at year tails) — explicitly a like-for-like vs the improved sim's own year_ret, with `build_baseline_summary.xlsx` named as the canonical headline. Honest, documented.
