# S2B audit — per-signal-day efficacy study (`build_signal_day_study.py`)

**Auditor:** AuditAgent-S2B (read-only on code; this file is the only write).
**Targets:** `universe_engine/self_improving/build_signal_day_study.py`,
`schema_signal_day_study.sql`, `S2B-build-log.md`.
**Source of truth:** `backend/power_user/services/persona_engine_core.py`
(`simulate_year`), `persona_simulator.py` (`PERSONA_CONFIGS["falcon-top-10"]`,
selection :451-467), `build_baseline.py` (`falcon_baseline_trades` writer).

---

## VERDICT: GREEN

Exit math is identical to the engine; the entry-bar offset is correct; no cash/
skip constraint leaks in; post-exit values are never imputed; the parity check is
sound and can catch a mechanics bug; no PROD/shared-code mutation. One non-
blocking operational caveat on the `--dry-run` gate (below) the orchestrator must
honor.

---

## 1. Exit mechanics — IDENTICAL to the engine (verified line-for-line)

`simulate_independent_pick` (build:197-308) vs `simulate_year` entry (core:483-523)
and exit walk (core:396-432):

| property | engine | study | match |
|---|---|---|---|
| entry px `open*mult*(1+SLIP)`, mult=1 | core:491 | build:217 | ✓ |
| integer shares `floor(₹50k/ep)` | core:495 | build:219 | ✓ |
| `actual_deployed = shares*ep` | core:500 | build:222 | ✓ |
| `time_exit_idx = min(bsi+hold-1, len-1)` | core:517 | build:226 | ✓ |
| `cur_ret = close/ep-1`; high_water = max | core:396-398 | build:242-244 | ✓ |
| `init_stop_lvl = ep*(1+init_stop)` | core:400 | build:246 | ✓ |
| trail arm `hw>=trail_trigger`; window `[max(0,bi-lb+1):bi+1]`; `max(ep,min low)`; `max(init,trail)` | core:403-408 | build:249-254 | ✓ |
| priority SL→TARGET→TIME; SL `min(stop,open)` (gap-down); TARGET `max(tgt,open)`; TIME `close` | core:415-423 | build:260-268 | ✓ |
| `exit_px=raw*(1-SLIP)`; gross/fees/net; hold=`bar-entry+1` | core:426-432 | build:271-275 | ✓ |

`SLIP` and `FEE` are **imported** from `persona_engine_core` (build:115-116), not
redeclared — 5bps/30bps can never drift. `TRAIL_LOOKBACK`, `init_stop`, `target`,
`hold_days`, `trail_trigger`, `fixed_per_trade` are all read from `run_cfg`
(`PERSONA_CONFIGS["falcon-top-10"]`: fixed_per_trade=50_000, hold=7,
init_stop=-0.07, trail_trigger=0.12, trail_lookback=10, target=None) — nothing
hardcoded. `target=None` collapses priority to SL→TIME, matching the engine for
this persona. **No exit-arithmetic divergence found.**

## 2. Entry-bar off-by-one — CORRECT

Study walk is `range(entry_bar_idx + 1, last_idx + 1)` (build:240) — the entry bar
is never exit-checked. This is right: in `simulate_year` the position is added to
`open_pos` in step 2 of the entry day (core:519-525), AFTER step 1's exit loop
already ran (core:387-439), so the entry-day bar is never evaluated; the first
exit check is the next td with a bar = `bars_sym[entry_bar_idx+1]`. Iterating the
symbol's own bars in order is equivalent to the engine iterating `td_list` and
skipping `d not in idx_map` (core:391-393). Since `time_exit_idx = bsi+6 >
entry_bar_idx`, starting at +1 never skips the TIME_STOP. **Offset is correct.**

## 3. Selection — unconstrained top-N/day, no skip, no cash (confirmed)

`_year_signals_by_signal_date` (build:164-184) reproduces sim:451-467 exactly:
`eligible_patterns_for_year(all_pats, Y)` → `compute_year_signals(...,
min_fires=run_cfg.min_fires)` (=10) → `if sort_key=="avg_lift": score=avg_lift`
→ group by signal_date. Then per signal_date, sort by score desc, take top-N
(build:851); every pick traded at ₹50k. **No `held_syms` skip, no cash gate.**
Parity to the baseline cohort holds because the daily persona's entry_date is
deterministic (next open) and the baseline runs `group_by_signal_date=True` (top-N
per signal_date, no accepted-count cap) — so for any (signal_date, symbol) the
baseline *entered*, the study contains the same pair with identical mechanics.
The study is a superset (it additionally keeps held/cash-skipped pairs). Correct.

## 4. Parity cross-check — SOUND, would catch a mechanics bug

`build_baseline.py` writes `falcon_baseline_trades` from the **real**
`simulate_year` (build_baseline:217), with `net_ret_pct = net_pnl/actual_deployed
*100` (build_baseline:457) and raw `exit_reason` (INIT_STOP/TRAIL_GIVEBACK/
TIME_STOP, un-mapped) and slipped `entry_price`/`exit_price` — identical
definitions to the study. `parity_cross_check` (build:589-627) compares every
shared CLOSED (signal_date, symbol) on entry_price/exit_price/net_ret_pct (rel
tol 1e-6) + exit_reason (exact); counts mismatches (must be 0). Any divergence in
entry px, share rounding, stop level, gap-down, slippage, fee, or the off-by-one
shifts entry/exit/net and trips a mismatch — the check is genuinely diagnostic.
Year-boundary is handled without false mismatch: a Dec pick whose exit is in Jan
is `OPEN_AT_BACKTEST_END_MTM` in the baseline year run → `exit_price=NULL` →
excluded by `_load_baseline_trades`' `exit_price IS NOT NULL` filter; the study
closes it in Jan but has no baseline counterpart to compare against. Open-at-edge
study rows (exit_price None) are skipped. Each shared key compared once.

## 5. Post-exit D+8..D+60 — no imputation, correct base

`compute_post_exit` (build:374-422): base = entry bar's raw open (`close/entry_px
-1`, same base as the journey). D+k uses `j=eidx+(k-1)`, set only when
`j<len(bars_sym)`, else stays `None` — **never imputed** (build:396-399).
`post_hold_high_ret` scans the FULL window `range(8,61)`, breaking at the data
edge, max close_ret (not just the 7 sampled offsets). `early_exit_flag = hi_ret >
15%`. `kept_running_d30 = d30 > net_ret_pct`, NULL if either NULL. Confirmed.

## 6. Safety — additive, RND-only, idempotent, dry-run inert

- Schema: single `CREATE TABLE IF NOT EXISTS falcon_signal_day_study` + two
  `CREATE INDEX IF NOT EXISTS`; does not touch `schema_self_improving.sql`.
- `_COLS` (66 cols) matches the schema's insertable columns **exactly** (order +
  names verified programmatically); `id` (AUTOINCREMENT) and `created_at`
  (DEFAULT) correctly omitted from the INSERT.
- `_write` (build:780-798): `BEGIN; DELETE WHERE persona='falcon_top10_daily';
  INSERT executemany; COMMIT` (rollback on error) — single transaction,
  idempotent, scoped to this study persona only (baseline/other personas
  untouched). RND DB only; PROD opened SELECT-only by the engine loaders.
- `--dry-run` returns at build:936-938 BEFORE `apply_schema`/`_write`/
  `write_excel` — no schema, no rows, no xlsx. Inert. Confirmed.
- No mutation of `persona_engine_core.py`, `persona_simulator.py`, or any shared
  engine code (reuse-by-import only). INV2 satisfied.

---

## Is `--dry-run` the safe gate? YES, with one required condition

`--dry-run` is safe to run (writes nothing) and is the correct gate. **But the
pass condition is `parity matched > 0 AND mismatches == 0`, NOT exit code 0.**

Reason: if `falcon_baseline_trades` (persona='falcon_top10') is absent or empty,
the cross-check is SKIPPED with a note and the run still returns 0 (build:912-915)
— a green exit that proves nothing about mechanics parity. In this worktree the
RND DB (`universe_engine/data/db/kanida_universe.db`) does not currently exist
(only `data/db/kanida_quant.db` is present), so a naive `--dry-run` here would
either FileNotFoundError on the resolver or SKIP the parity check. The
orchestrator must run dry-run against an RND DB that has the baseline table
populated and assert the printed `matched > 0` and `mismatches: 0`.

## Must-fix

None (GREEN). Non-blocking recommendation for the orchestrator only: treat
"PARITY CROSS-CHECK ... SKIPPED" as a NON-PASS for the gate; require a populated
`falcon_baseline_trades` and a printed `matched > 0, mismatches: 0`.

## Explicit confirmations

- Exit math is **identical** to `persona_engine_core.simulate_year`
  (entry, share rounding, init/trail stop, gap-down, priority, slippage, fee).
- Entry-bar offset is **correct** (walk starts at `entry_bar_idx + 1`).
- `--dry-run` is the **safe gate** (inert), with the parity pass condition
  `matched > 0 AND mismatches == 0` (not merely exit 0).
