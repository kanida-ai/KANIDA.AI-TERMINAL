# S2C audit — post-hold intraday high/low + circuit columns

**Auditor:** AuditAgent-S2C (read-only). **Date:** 2026-06-15 IST.
**Target:** `universe_engine/self_improving/build_signal_day_study.py`,
`schema_signal_day_study.sql`, `S2C-build-log.md`.
**Method:** full `git diff HEAD` of both files (only 2 files changed, +253/-13),
line-by-line read of the builder, cross-check of the reused exit/entry arithmetic
against `backend/power_user/services/persona_engine_core.simulate_year`
(core:384-436 exit, 480-523 entry), and manual `_COLS` ↔ schema ↔ ALTER parity
count. Python is not installed in this environment, so the column count was
verified by hand (lists are fully explicit).

---

## VERDICT: GREEN

No must-fix items. The change is a clean additive post-hoc OHLC read. Entry/exit/
net/parity are byte-for-byte untouched; the new math has no look-ahead, no
cross-trade leakage, and no NULL imputation; the schema apply is idempotent and
non-destructive; `_COLS` matches the schema with no INSERT mismatch.

---

## 1. Parity + execution UNTOUCHED — CONFIRMED

The `git diff` shows **zero** changes to any execution code:
- `simulate_independent_pick` (build:216-327) — not in the diff. Entry arithmetic
  (build:232-246) and the per-day exit walk (build:259-306) are unchanged.
- `compute_journey` / d1..d7 (build:334-386) — not in the diff.
- The close-based post-hold cols inside `compute_post_exit`
  (`post_hold_high_ret`/`d{k}_close_ret`/`post_hold_peak_day`/`early_exit_flag`/
  `kept_running_d30`) — the diff only *interleaves* new assignments; the existing
  lines (build:441, 461-463, 470-471, 476, 501-503) are behaviorally identical
  (`bars_sym[j]` was refactored to a local `bar = bars_sym[j]`, same value).
- `parity_cross_check` (build:689-746) and `_load_baseline_trades` (build:650-678)
  — not in the diff. Still compares only entry_price / exit_price / exit_reason /
  net_ret_pct (build:719-724).

Re-verified against the engine source that the mirrored arithmetic is faithful
(not just unchanged):
- Exit walk build:259-306 == core:384-436: `cur_ret = close/ep-1`; high_water
  ratchet; `init_stop_lvl = ep*(1+init_stop)`; trail window
  `bars[max(0,bar_idx-lookback+1):bar_idx+1]`, `max(ep, min low)`;
  priority SL→TARGET→TIME; gap-down `min(stop_lvl, open)` / `max(target, open)`;
  `exit_px = raw*(1-SLIP)`; `fees = actual_deployed*FEE`; `net = gross-fees`.
- Entry build:232-246 == core:485-518: `ep = open*1.0*(1+SLIP)`,
  `shares = floor(allocated/ep)`, `actual_deployed = shares*ep`,
  `time_exit_idx = min(entry+hold-1, len-1)`. Cash gate + held_syms are
  intentionally stripped (documented design: independent replicated capital).

The new columns are pure post-hoc reads of already-loaded `bars_sym`; none feed
back into entry_px, exit_px, exit_reason, shares, fees, or net_ret_pct.

## 2. High/low + peak/trough math, no look-ahead — CONFIRMED

- `d{k}_high_ret`/`d{k}_low_ret` (build:442-443) = `bar.high|low / entry_open - 1`
  at `j = eidx+(k-1)`, written **only inside `if j < len(bars_sym)`** (build:439);
  absent bar ⇒ left at the NULL default (build:419-421). No imputation.
- `post_hold_peak_high_ret`/`_day` and `post_hold_trough_low_ret` (build:464-474)
  are computed in the SAME `for k in range(8, 61)` loop as the close-based scan,
  which `break`s at the first absent bar (build:458-459). So they read only
  existing D+8..D+60 bars — identical availability semantics to the unchanged
  close scan.
- `post_hold_peak_high_ret >= post_hold_high_ret` by construction: both scan the
  identical bar set in one pass; `bar.high >= bar.close` per bar ⇒ the running max
  of highs ≥ running max of closes. Holds.
- Base is `entry_open` (entry bar's raw open), the same base as the close cols and
  the d1..d7 journey — so post-hold offsets stay directly comparable. (Note: this
  is "from the customer's entry", not "from our exit" — intended, matches the
  close cols.)

## 3. max_up_day / circuit — CONFIRMED (no cross-trade leak)

- `max_up_day_move_pct` (build:485-499) scans `j in range(max(eidx,1), last_idx+1)`
  over **this symbol's own `bars_sym`** (`bars[sym]` for the single pick) — no
  other symbol's bars are reachable, so no cross-trade leakage.
- `move = bars_sym[j].close / bars_sym[j-1].close - 1` (%); tracks max + date.
  `start = max(eidx, 1)` guarantees `prev_close = bars_sym[j-1]` exists; the
  eidx==0 guard is benign (trail-padding load from 2020-12-01 makes a
  window-entry at index 0 effectively impossible). `prev_close <= 0` skipped
  (build:491-492). `hit_circuit_flag = 1 if move >= 19.5 else 0` (build:498-499);
  NULL only if no comparable pair.
- The "move into the entry bar is included" start (comparing the entry bar to the
  bar before entry) is a benign definitional choice — it is a backward-looking
  read of this symbol's own already-existing history, **not** a look-ahead into
  future bars or other symbols. Documented in the function (build:479-484), the
  build log (§6.1), and acceptable per the audit brief.

## 4. Idempotent additive schema — CONFIRMED

- CREATE TABLE / CREATE INDEX are `IF NOT EXISTS`; the 20 new cols are in the
  CREATE TABLE (fresh DBs) AND a tail ALTER block (existing DBs).
- `apply_schema` (build:954-980): runs the CREATE/INDEX block via `executescript`,
  then applies each parsed ALTER **PRAGMA-guarded** — skips cols already present
  (`_existing_columns` via `PRAGMA table_info`, build:968-971) and tolerates a
  `duplicate column` `OperationalError` as a race fallback (build:974-977).
- `_split_schema_sql` (build:915-951) strips `--` comments, separates ALTERs from
  CREATE/INDEX via the regex
  `ALTER\s+TABLE\s+falcon_signal_day_study\s+ADD\s+COLUMN\s+([A-Za-z_]\w*)`.
  All 20 ALTERs match; everything else (CREATE TABLE/INDEX) stays in the exec
  block. No DROP / RENAME / retype anywhere. Re-run = no-op; fresh DB = every
  ALTER skipped.
- `--dry-run` returns before `apply_schema`/`_write`/`write_excel` (build:1148-1150),
  so it writes nothing (no DB, no Excel) — unchanged by S2C.
- Write path (build:987-1005) unchanged: `DELETE WHERE persona='falcon_top10_daily'`
  then `executemany` INSERT in one BEGIN/commit transaction.

## 5. Report + _COLS / INSERT parity — CONFIRMED

- `_COLS` (build:613-643): manual count = **86 data columns** (17 core/exec + 28
  d1..d7 OHLC + 9 peak/trough/flags + 7 post-hold close + 4
  post_hold_high/peak_day/early_exit/kept_running + 14 S2C d{k}_high/low + 6 S2C
  peak_high/trough/max_up/circuit + 1 prior_appearances_30d). No duplicates.
- Schema CREATE TABLE has these same 86 data cols plus `id` (PK AUTOINCREMENT) and
  `created_at` (DEFAULT) — both correctly EXCLUDED from `_COLS`/INSERT, so the
  `INSERT (...) VALUES (?…)` placeholder count matches the column count. No
  column-count mismatch.
- The 20 S2C cols are identical across CREATE TABLE, the 20 ALTERs,
  `_S2C_ALTER_COLS`, `build_row` (build:601-608), and `_COLS` (build:633-641).
- `compute_efficacy` adds `avg_post_hold_peak_high_ret` (mean of non-NULL over
  closed rows) and `circuit_hit_rate` (denominator = rows with non-NULL
  hit_circuit_flag) — build:781-795. Both are threaded into the Per-Year + Overall
  Excel sheets, the CSV fallback, and the console overall print. All-Trades sheet
  is `_COLS`-driven, so it gains the 20 cols automatically.

---

## Explicit confirmations requested by the brief

**(a) Entry/exit/parity are UNTOUCHED.** `simulate_independent_pick`, the entry/
exit arithmetic, `compute_journey`, the existing close-based post-hold columns,
and `parity_cross_check` (still comparing only entry/exit/reason/net_ret_pct) are
byte-for-byte unchanged in the diff. The new columns never feed back into the
trade simulation.

**(b) New post-hold math has NO look-ahead and NO imputation.** Every new value
defaults to NULL and is written only when the underlying bar exists; the peak/
trough scans break at the first absent bar; the max-up-day scan reads only this
pick's own symbol bars (no cross-trade leak) and only backward/at-bar history
(no future read). `post_hold_peak_high_ret >= post_hold_high_ret` by construction.
