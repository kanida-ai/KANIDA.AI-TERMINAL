# S5 Audit — build_bigwinner_study.py

**Auditor:** AuditAgent-S5 (read-only) · **Date (IST):** 2026-06-15
**Target:** `universe_engine/self_improving/build_bigwinner_study.py` (+ `S5-build-log.md`)
**Writes:** `falcon_big_winner_loser_study` (RND only)

## VERDICT: GREEN

No must-fix items. All six checks pass against the file and the schema; thresholds and
move_type literals cross-verified against `classify_patterns.py` and `build_baseline.py`.

---

## Check-by-check (cited)

### 1. HFCL discipline — ENFORCED (critical)
`build_bigwinner_study.py:393-403`:
```
if n >= HFCL_MIN_N and ev_hold_end is not None:
    big_winner_candidate = 1 if ev_hold_end > BIG_WINNER_EV_MIN else 0   # 0.0
    big_loser_risk = 1 if (big_loser_rate >= BIG_LOSER_RATE_MIN and ev_hold_end <= BIG_LOSER_EV_MAX) else 0
else:
    big_winner_candidate = None    # n<18 -> NULL, not 0
    big_loser_risk = None
```
- `HFCL_MIN_N = 18` (`:194`), `BIG_WINNER_EV_MIN = 0.0` (`:202`), `BIG_LOSER_RATE_MIN = 0.30` (`:205`), `BIG_LOSER_EV_MAX = 0.0` (`:206`).
- `big_winner_candidate=1` IFF n>=18 AND EV>0. `big_loser_risk=1` IFF n>=18 AND big_loser_rate>=0.30 AND EV<=0. Both NULL when n<18 (the `else` branch sets `None`, not `0`). When n>=18 but condition unmet → `0`. Correct 0-vs-NULL distinction. No single-example conclusion.
- Cross-check: `HFCL_MIN_N=18` and maturity cutoffs 18/50/150 match `classify_patterns.py:108,116-118` exactly.

### 2. EV formulas — span ALL occurrences
`:386` `ev_hold_end = _mean([o["net_ret_pct"] for o in occ])` — mean over `occ` (all resolved occurrences), realized at exit.
`:387-388` `ev_peak = _mean([o["peak_ret"] for o in occ if o["peak_ret"] is not None])` — mean over all occ (NULL-safe). Opportunity EV.
Neither is restricted to winners. The HFCL gate tests `ev_hold_end` (`:393`,`:397`). Confirmed.

### 3. Winner/loser sub-stats — correct subsets
- Winners subset `:330` `big_winner==1`; losers `:331` `big_loser==1`.
- Winner stats over `winners` only (`:344-356`): avg/median peak, avg peak_day, peak_sustained_rate, sector_tailwind_rate, stock_led_rate (`move_type=='STOCK_LED'`).
- Loser stats over `losers` only (`:359-383`): avg trough/trough_day, recovery_rate, stop_hit_rate, sector_headwind_rate (`SECTOR_HEADWIND`), stock_weakness_rate (`STOCK_WEAKNESS`).
- `stop_hit_rate` (`:373-376`) = count(`exit_reason=='INIT_STOP'`) / `n_bl` (= len(losers)). Correct denominator.
- `recovery_rate_after_trough` (`:365-371`) = fraction of losers with `net_ret_pct > trough_ret`, NULL `trough_ret` excluded from num and denom; definition documented `S5-build-log.md:77-86` ("ended > trough", spec left open, deliberate vs >=0). NULL-safe via `_rate(...) if l_recov_pairs else None`.
- `n_neutral` (`:334,338`) = direct "neither flag" count, not n−W−L (flags can co-occur). Partition caveat documented `:46-52` and `S5-build-log.md:66-75` — acceptable per audit scope.
- move_type literals (`STOCK_LED/SECTOR_HEADWIND/STOCK_WEAKNESS`) match `_classify_move_type` in `classify_patterns.py:243-249` and schema comment `schema_self_improving.sql:99`.

### 4. Source consistency — from JOINed bt row
`_load_pattern_occurrences` (`:273-294`) SELECTs journey/move_type/flags from `bt` (falcon_baseline_trades), JOIN on `bt.id = c.trade_id`, filtered `c.persona='falcon_top10' AND bt.net_ret_pct IS NOT NULL`. move_type/peak/trough/sustained/exit_reason/sector_tailwind/flags all from `bt` because `contributions.move_type` is NULL (documented `:260-267`, `S5-build-log.md:22-28`). Flags identical on both tables; read from bt for one source. Confirmed.

### 5. Idempotent + safe
- `_write` (`:606-633`): single `BEGIN`/`commit`, `DELETE WHERE persona='falcon_top10'` (`:619`) then `executemany INSERT` (`:624`); rollback on any exception (`:628-630`). RND-only connection (`:611`).
- Guard: aborts with rollback if table missing (`:614-618`) — "apply Step-1 schema first".
- Single `week_ending = MAX(signal_date)` (`:301,459`) → `UNIQUE(pattern_id, persona, week_ending)` honored; re-run = clean rebuild.
- `--dry-run` (`:841-850`) computes + prints, returns before `_write`/`write_report` — writes nothing.
- `_STUDY_COLS` (`:437-449`) = 26 columns, exact name+order match to `falcon_big_winner_loser_study` schema (`schema_self_improving.sql:257-282`, minus AUTOINCREMENT `id`). No column mismatch. INSERT uses `_STUDY_COLS` for both column list and placeholders (`:621-623`).
- No PROD/shared-code mutation: only import from backend is `_resolve_rnd_db_path` (read-only path helper, `:175-179`), used solely when `--rnd-db` omitted.

### 6. Deep-dives honest
`build_deep_dives` (`:579-592`) for HFCL/CARTRADE/IFCI lists every resolved occurrence from baseline (`:489-516`) + optional `falcon_signal_day_study` block (`:519-549`, returns None / "table absent" note if missing). `_symbol_aggregate_line` (`:552-576`) emits real "appeared N times, X winners, EV..." with explicit `n<18 INSUFFICIENT` vs `n>=18 eligible` tag; zero-count case (`:557-559`) states "appeared 0 times — NOTHING can be concluded". No fabrication; thin/zero shown honestly.

---

## Confirmations requested by orchestrator
- **HFCL enforced:** YES. Candidate/risk flags only set with n>=18 AND the EV check; n<18 → both NULL. Single-example conclusions impossible.
- **EVs span all occurrences:** YES. Both `expected_value_at_hold_end` and `expected_value_at_peak` are means over all resolved occurrences (`occ`), not winners-only.
- **`--dry-run` is the safe gate:** YES. Recommend running `--dry-run` first to read the three deep-dive `appeared N times` lines and the maturity/HFCL summary before APPLY (no Python in this env; counts knowable only at run time — `S5-build-log.md:138-145`).

## Notes (not RED)
- `n_neutral` is a "neither" count, not a 3-way partition — intentional, documented.
- `*_rate_winners/losers` cols are NULL if Step 3 (classify_patterns move_type backfill) hasn't run; Step 3 IS done. Counts/rates/EVs/HFCL flags unaffected.
