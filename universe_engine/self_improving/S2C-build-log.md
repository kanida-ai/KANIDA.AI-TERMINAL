# S2C build log — post-hold intraday high/low + circuit detection

**Goal.** Strengthen the per-signal-day study's POST-HOLD tracking (D+8..D+60) so
big post-exit spikes are explicit, not hidden behind close-only columns. The
study previously stored only `d{k}_close_ret` + a close-based `post_hold_high_ret`
for the post-hold window; intraday highs and upper-circuit days were invisible.

**Spot-check that motivated this (IFCI, entry 81.04, stopped out d3):** the
06-12 upper-circuit day (+19.94% single-day) and the 06-15 bar (intraday high
91.30 = +12.7% vs entry) are real in the OHLC, but the close-only columns stored
the +9.8% **close** for 06-15 and flagged no circuit day at all. The close-based
peak also understates the early-exit finding (intraday highs > the close-based
+24% peak).

**Scope:** ONLY `falcon_signal_day_study` (the table that already HAS post-hold
data) + its builder + its report. `build_baseline.py` / `falcon_baseline_trades`
are explicitly NOT touched (baseline post-hold is a separate later step).

**Files changed (dev worktree `kanida-dev`, branch `feat/self-improving-engine`):**
- `universe_engine/self_improving/schema_signal_day_study.sql` — new columns in
  the CREATE TABLE + a guarded ALTER block at the tail.
- `universe_engine/self_improving/build_signal_day_study.py` — compute the new
  columns, guarded-ALTER applier in `apply_schema`, report extensions.
- `universe_engine/self_improving/S2C-build-log.md` — this log.

Nothing run, nothing committed. RND-only. No PROD / shared-engine-code mutation
(INV2). Cannot run python — validated by reading.

---

## 1. Schema columns added (all NULL when the bar / move hasn't arrived — never imputed)

Added to `falcon_signal_day_study`, all additive:

- **per offset k ∈ {8,10,15,20,30,45,60}:** `d{k}_high_ret`, `d{k}_low_ret`
  (= high/entry_px−1 and low/entry_px−1 in %, at that bar). 14 columns.
- `post_hold_peak_high_ret` REAL — MAX(high/entry_px−1) over the AVAILABLE
  D+8..D+60 bars (the **true intraday post-exit peak**).
- `post_hold_peak_high_day` INTEGER — the D-offset (8..60) where that high occurred.
- `post_hold_trough_low_ret` REAL — MIN(low/entry_px−1) over D+8..D+60.
- `max_up_day_move_pct` REAL — largest single-day close/prev_close−1 (%) over the
  FULL path (entry bar → last available bar; covers circuit days during BOTH the
  hold and the post-hold).
- `max_up_day_date` TEXT — date of that max single-day up move.
- `hit_circuit_flag` INTEGER (0/1) — 1 if `max_up_day_move_pct >= 19.5`
  (≈20% upper circuit).

20 new columns total. **All close-based columns are UNCHANGED** for continuity:
`d{k}_close_ret`, `post_hold_high_ret`, `post_hold_peak_day`, `early_exit_flag`,
`kept_running_d30` keep their exact prior definitions and values.

Naming note: the post-hold offsets (8,10,15,20,30,45,60) never collide with the
intra-hold journey's `d1..d7_{high,low}_ret`.

### Additive + idempotent schema apply (PRAGMA-guarded ALTER)

`CREATE TABLE IF NOT EXISTS` only adds these columns on a **fresh** DB; it will
NOT add them to an existing S2B `falcon_signal_day_study`. SQLite has no
"ADD COLUMN IF NOT EXISTS", so:
- the new columns are listed in BOTH the CREATE TABLE (fresh DBs) AND a tail
  `ALTER TABLE … ADD COLUMN …;` block (existing DBs);
- `build_signal_day_study.apply_schema()` was reworked to mirror
  `apply_schema.py`'s taxonomy-column applier: `_split_schema_sql()` parses the
  DDL, separating the CREATE/INDEX block from the ALTER statements; the
  CREATE/INDEX block runs via `executescript` (IF NOT EXISTS → safe re-run); each
  ALTER is applied GUARDED — skipped if the column is already present
  (`PRAGMA table_info`), with a `duplicate column` `OperationalError` tolerated as
  a race fallback. Fresh DB → CREATE adds the cols, every ALTER is skipped.
  Existing DB → ALTERs add the missing cols. Re-run → no-op. Never drops/retypes.

---

## 2. High/low + post-hold-peak + max-up-day/circuit math (`compute_post_exit`)

Same `bars_sym` already loaded (no refetch); base = the entry bar's RAW open
(`entry_open`, identical base to the close columns and the intra-hold journey).

- **per-offset high/low:** in the existing `for k in POST_OFFSETS` loop, alongside
  the unchanged `d{k}_close_ret`, also set
  `d{k}_high_ret = (bar["high"]/entry_open − 1)*100` and
  `d{k}_low_ret  = (bar["low"]/entry_open − 1)*100`, only when the bar
  `eidx + (k−1)` exists (else NULL).
- **intraday post-hold peak/trough:** the existing single pass `for k in
  range(8, 61)` (which already computes the close-based `post_hold_high_ret`) now
  also tracks, on the SAME available-bar set, `post_hold_peak_high_ret` =
  max(bar high/entry_open−1) and its day `post_hold_peak_high_day`, and
  `post_hold_trough_low_ret` = min(bar low/entry_open−1). One pass, breaks at the
  first absent bar (same availability semantics as the close-based scan).
  `post_hold_peak_high_ret >= post_hold_high_ret` by construction (intraday high
  ≥ close), so it is generally the larger, truer post-exit peak.
- **max single-day up move + circuit:** a separate scan over the FULL path
  `for j in range(max(eidx,1), last_idx+1)`: `move = close[j]/close[j−1] − 1` (%),
  tracking the max and its date. `start = max(eidx, 1)` so the first iteration
  always has a `prev_close` (the bar before entry); the `eidx==0` guard is
  defensive only — the trail-padding bar load (`load_all_bars` from `2020-12-01`)
  makes a year-window entry at index 0 effectively impossible. `prev_close <= 0`
  rows are skipped. `hit_circuit_flag = 1 if max_up_day_move_pct >= 19.5 else 0`
  (NULL only if no comparable bar pair exists). This deliberately spans the hold
  AND post-hold so an upper-circuit day during the held period (e.g. IFCI 06-12
  +19.94%) is captured too.

All new values default to NULL at the top of the function and stay NULL when the
relevant bar/move is unavailable — never imputed.

`build_row` copies the 20 new keys from `compute_post_exit`'s dict; `_COLS`
appends them (after the close-based post-hold block, before
`prior_appearances_30d`) so the INSERT carries them.

---

## 3. Close-based columns PRESERVED + parity unaffected

- `d{k}_close_ret`, `post_hold_high_ret`, `post_hold_peak_day`,
  `early_exit_flag`, `kept_running_d30` are byte-for-byte unchanged (same code
  paths, same thresholds: `EARLY_EXIT_THRESHOLD_PCT = 15.0`).
- **No entry/exit/net_ret_pct or hold-journey change.** `simulate_independent_pick`
  (entry px, integer shares, high_water, init/trail stop, gap-down, priority
  SL→TARGET→TIME, slippage, fees), `compute_journey`, and `build_row`'s execution
  fields are untouched. The new columns are pure post-hoc OHLC reads on
  already-loaded bars.
- **Parity cross-check logic stays as-is.** `parity_cross_check` compares only
  `entry_price / exit_price / exit_reason / net_ret_pct` against
  `falcon_baseline_trades`; none of those are affected by the new columns, so it
  must still report **0 true mismatches**. The new columns are not part of any
  parity comparison.

---

## 4. Report extensions

- **"All Signal-Day Trades" sheet:** driven by `_COLS`, so it automatically
  gains the 20 new columns (`post_hold_peak_high_ret`, `post_hold_peak_high_day`,
  `post_hold_trough_low_ret`, `max_up_day_move_pct`, `max_up_day_date`,
  `hit_circuit_flag`, and the per-offset `d{k}_high_ret`/`d{k}_low_ret`).
- **"Per-Year Efficacy" + "Overall" sheets:** `compute_efficacy` now also returns
  `avg_post_hold_peak_high_ret` (mean of non-NULL `post_hold_peak_high_ret` over
  CLOSED rows — the intraday peak, expected ≥ the close-based
  `avg_post_hold_high_ret`) and `circuit_hit_rate` (% of CLOSED rows with
  `hit_circuit_flag == 1`, denominator = rows with a non-NULL `hit_circuit_flag`).
  Added to the Excel per-year + overall column lists, the CSV fallback, and the
  console overall print.

---

## 5. Idempotency / safety

- Additive only: no column dropped/renamed/retyped; write path unchanged
  (`DELETE WHERE persona='falcon_top10_daily'` then INSERT in one transaction).
- `apply_schema` is idempotent (CREATE IF NOT EXISTS + guarded ALTER) and is
  called at APPLY time (after the dry-run gate), so `--dry-run` still writes
  NOTHING (no schema, no rows, no Excel).
- RND-only; PROD opened read-only by the loaders. No mutation of
  `persona_engine_core.py`, `persona_simulator.py`, `build_baseline.py`,
  `schema_self_improving.sql`, or any shared engine code (INV2).
- `--out` already supported (orchestrator passes `out/v3`).

---

## 6. Risks for the audit agent

1. **`max_up_day_move_pct` first-comparison semantics.** The scan starts at the
   entry bar comparing to the bar BEFORE entry, so the move INTO the entry day is
   included. If the audit expects the window to start strictly AFTER entry, this
   is a one-bar definitional choice (documented in the function + this log). It
   does not affect any circuit day at/after entry (e.g. IFCI 06-12).
2. **Circuit threshold 19.5%.** `CIRCUIT_MOVE_PCT = 19.5` is a proxy for the 20%
   upper circuit band; stocks in 5%/10%/2% bands won't trip it (intended — the
   flag is specifically for ~20%-band moves). Lower-band circuits show up in
   `max_up_day_move_pct` as a value, just unflagged.
3. **`post_hold_peak_high_ret` base = entry open**, NOT exit price (same base as
   the close columns and journey, so D+8..D+60 stay directly comparable to
   d1..d7). Confirm the audit reads it as "from the customer's entry", not "from
   our exit".
4. **Availability semantics.** Per-offset high/low and the post-hold scans use the
   identical "bar must exist, else NULL/stop" rule as the existing close columns;
   open-at-data-edge picks (NULL exec) still get whatever post-hold bars exist,
   exactly as before.
5. **Existing-DB migration.** First APPLY on an existing S2B DB ADDs 20 columns
   (NULL on old rows until the persona is rebuilt). The write path DELETEs +
   re-INSERTs the whole `falcon_top10_daily` persona each run, so a normal rebuild
   populates the new columns for every row; partial/legacy rows of OTHER personas
   (none exist today) would retain NULLs.
