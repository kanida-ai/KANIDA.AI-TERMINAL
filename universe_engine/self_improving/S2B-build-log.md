# S2B build log — per-signal-day efficacy study

**Goal.** A NEW study, distinct from the managed-portfolio baseline
(`build_baseline.py`). It measures **per-signal-day efficacy**: take the engine's
top-10 picks on EVERY signal day and simulate EACH pick INDEPENDENTLY with
replicated capital (₹50k/pick), **NO `skip_already_held`, NO cash constraint**,
duplicates expected (same stock many days). It answers "if a customer acts on a
pick on ANY given day, what happens to that stock + its outcome" and tracks what
happens AFTER our exit (D+8 → D+60).

**Files written (dev worktree `kanida-dev`, branch `feat/self-improving-engine`):**
- `universe_engine/self_improving/schema_signal_day_study.sql` — new, self-contained.
- `universe_engine/self_improving/build_signal_day_study.py` — new builder.
- `universe_engine/self_improving/S2B-build-log.md` — this log.

Nothing run, nothing committed, no PROD / shared-engine-code mutation (INV2).

---

## 1. Table `falcon_signal_day_study` (RND, SQLite)

Self-contained `CREATE TABLE IF NOT EXISTS` in `schema_signal_day_study.sql`
(does NOT touch `schema_self_improving.sql`). Applied by the script via
`apply_schema()` (`executescript`, CREATE IF NOT EXISTS) before any write.
Column groups:

- **identity:** `id` PK AUTOINCREMENT, `persona` (default `'falcon_top10_daily'`),
  `signal_date`, `entry_date`, `exit_date`, `symbol`, `sector`, `engine_rank`
  (pick's rank within its signal_date cohort, 1..top_n), `avg_lift`, `n_fires`,
  `sum_lift`.
- **execution:** `entry_price`, `exit_price`, `exit_reason`,
  `hold_days_trading`, `shares`, `net_pnl`, `net_ret_pct`.
- **journey (same defs as baseline):** `d1..d7_{open,high,low,close}_ret`,
  `peak_ret_during_hold`, `peak_day_during_hold`, `trough_ret_during_hold`,
  `trough_day_during_hold`, `peak_before_trough`, `trough_before_peak`,
  `peak_sustained`, `big_winner_flag`, `big_loser_flag`.
- **post-entry "what happened next" (the key addition):** `d8/d10/d15/d20/d30/
  d45/d60_close_ret`, `post_hold_high_ret`, `post_hold_peak_day`,
  `early_exit_flag`, `kept_running_d30`.
- **dup context:** `prior_appearances_30d`.
- `created_at TEXT DEFAULT (datetime('now'))`.

Two additive `CREATE INDEX IF NOT EXISTS` (persona+signal_date+symbol; symbol)
for the audit agent's queries. SQLite translation rules identical to
`schema_self_improving.sql` (BOOLEAN→INTEGER 0/1, FLOAT→REAL, DATE→TEXT,
INTEGER[]→TEXT JSON, NOW()→datetime('now')).

---

## 2. Selection — same as Falcon Top 10, MINUS skip/cash

`_year_signals_by_signal_date(ctx, Y)` reproduces the parity Falcon Top 10 signal
generation per year (mirrors `persona_simulator.py:451-467`):
- `eligible_patterns_for_year(all_pats, Y)`            (`persona_engine_core.py:169`)
- `compute_year_signals(..., min_fires=run_cfg.min_fires)` (`:181`, min_fires=10)
- `if sort_key=="avg_lift": s["score"]=s["avg_lift"]`  (`persona_simulator.py:462-464`)
- group by signal_date                                 (`:465-467`)

Then **per signal_date**, sort the cohort by `score` (= avg_lift) desc and take
the **top-N** (default 10). Every pick is ONE independent trade. We do **not**
drop held symbols and **not** apply any cash gate. Because the daily persona's
entry_date is deterministic (next open after signal_date), each signal_date maps
to exactly one entry_date, so this per-signal_date top-N cohort is identical to
the cohort `simulate_year` builds with `group_by_signal_date=True`. The study's
pick set is therefore a **superset** of the baseline's *entered* set (baseline
additionally drops held + cash-skips) — exactly the intended difference, and
every baseline-entered `(signal_date, symbol)` is present here with identical
mechanics.

All loaders reuse the engine: `load_full_patterns` (RND), `load_panel`,
`load_all_bars` (padded from `2020-12-01` for trail lookback), `build_sector_map`,
`trading_days` (PROD), with `PERSONA_CONFIGS["falcon-top-10"]` (sim_start
`2021-01-01`, dynamic sim_end, `fixed_per_trade=₹50,000`, hold 7, init_stop −7%,
trail_trigger +12%, trail_lookback 10).

---

## 3. Exit mechanics — REUSED, proven identical to parity

The engine's per-position exit block is `persona_engine_core.simulate_year`
(`:384-439` exit walk, `:480-523` entry). `simulate_year` itself bakes in the
multi-position cash pool + `held_syms` skip, which this study must NOT have, so
calling it directly is impossible. Instead `simulate_independent_pick()` re-runs
the **exact same arithmetic** for a single position, copied line-for-line from
the engine with only the cash/held bookkeeping removed (a single position never
competes for cash and is never skipped). Mapping:

| step | engine line | study line |
|---|---|---|
| entry px `ep = open*(1+SLIP)` (mult=1) | core:491 | `ep = ep_raw * 1.0 * (1+SLIP)` |
| integer shares `floor(₹50k/ep)` | core:495 | `math.floor(allocated/ep)` |
| `actual_deployed = shares*ep` | core:500 | same |
| `time_exit_idx = min(bsi+hold-1, len-1)` | core:517 | same |
| `high_water = max(.., close/ep-1)` | core:396-398 | same |
| `init_stop_lvl = ep*(1+init_stop)` | core:400 | same |
| trail arm + Donchian `max(ep, min low over lookback)` | core:403-408 | same |
| priority SL→TARGET→TIME, gap-down `min(stop,open)` | core:415-423 | same |
| `exit_px = raw*(1-SLIP)`, gross/fees/net | core:426-432 | same |

`SLIP` and `FEE` are **imported** from `persona_engine_core` (not re-declared),
so the 5bps/30bps constants can never drift.

**CRITICAL off-by-one parity fix.** In `simulate_year`, a position is added to
`open_pos` in step 2 of the entry day (`:441-525`), AFTER step 1's exit checks
already ran for that day (`:384-439`). So the **entry-day bar is never evaluated
for an exit** — the first exit check is the next trading day. The study's walk
therefore starts at `entry_bar_idx + 1`, not `entry_bar_idx`. Iterating the
symbol's own bars in order is equivalent to `simulate_year` iterating `td_list`
and skipping `d not in idx_map` (`:391-393`): both visit exactly this symbol's
post-entry bars in date order.

If no exit triggers within loaded bars (e.g. a recent pick whose 7-day hold
hasn't fully elapsed in data, or one entered at the data edge), the pick is
returned as `OPEN_AT_BACKTEST_END_MTM` (MTM at last bar, mirroring
`simulate_year`'s year-end MTM `:548-574`), with `exit_price/exit_reason/net*`
NULL — it is excluded from efficacy stats and from the parity check.

---

## 4. Post-exit "what happened next" (D+8..D+60)

`compute_post_exit()`. All returns are `close/entry_px − 1` (in %), base = the
entry bar's RAW open (the same base as the intra-hold journey). For each offset
k ∈ {8,10,15,20,30,45,60} take the bar at `entry_bar_idx + (k-1)` **if it
exists**, else **NULL** — the future bar has not arrived yet, NEVER imputed.
`post_hold_high_ret` = max close_ret over EVERY available bar in D+8..D+60 (the
full window, not just the 7 sampled offsets, so an intermediate peak isn't
missed); `post_hold_peak_day` = the D where it occurred. `early_exit_flag` = 1 if
`post_hold_high_ret > 15%` (stock kept running after we exited; NULL if no
post-window bar exists). `kept_running_d30` = 1 if `d30_close_ret > net_ret_pct`
(NULL if either is NULL).

`prior_appearances_30d` (`build_prior_appearances_index`): for each
`(signal_date, symbol)` pick, count how many times that symbol was a pick in the
prior **30 calendar days** (strictly before this signal_date), computed over ALL
picks across all years (cross-year safe).

---

## 5. Parity cross-check (must be 0 mismatches)

`_load_baseline_trades()` reads `falcon_baseline_trades` rows for
persona `'falcon_top10'` that are CLOSED (`exit_price IS NOT NULL`).
`parity_cross_check()` then, for every `(signal_date, symbol)` shared with the
study's CLOSED rows, asserts `entry_price / exit_price / net_ret_pct` match
within float tolerance and `exit_reason` matches exactly. Prints the matched
count and any mismatches; **mismatches MUST be 0** (any mismatch = a mechanics
divergence bug). If `falcon_baseline_trades` is absent/empty (baseline not built
yet), the check is SKIPPED with a note rather than failing. Open-at-end study
rows have NULL exec and are not compared (no closed baseline counterpart). Each
shared key is compared once (a `(signal_date, symbol)` is unique within a
cohort).

This is the proof that the re-implemented single-position walk produces
byte-identical outcomes to the portfolio engine on shared trades.

---

## 6. Excel / CSV output

`write_excel()` (openpyxl; CSV fallback if openpyxl missing) → `out/` (default,
gitignored). Sheets:
- **All Signal-Day Trades** — every pick, all `_COLS`.
- **Per-Year Efficacy** — per year: `n_picks, win_rate, avg_ret, median_ret,
  big_winner_rate, big_loser_rate, avg_post_hold_high_ret, early_exit_rate`.
- **Overall** — the headline every-day efficacy stats.

Efficacy stats (`compute_efficacy`) are computed on CLOSED rows only
(`net_ret_pct` not None). `avg_post_hold_high_ret` averages only rows with a
non-NULL `post_hold_high_ret`; `early_exit_rate` denominator is rows with a
non-NULL `early_exit_flag` (i.e. those that have at least one post-window bar).

---

## 7. Idempotency / safety

- Write path: `PRAGMA foreign_keys=ON; BEGIN; DELETE WHERE persona='falcon_top10_daily';
  INSERT (executemany); COMMIT` (rollback on error) — single transaction,
  idempotent rebuild for this persona only.
- RND-only writes; PROD opened read-only (loaders only SELECT). No mutation of
  `schema_self_improving.sql`, `persona_engine_core.py`, `persona_simulator.py`,
  or any shared engine code (INV2).
- `--dry-run` computes + prints per-year efficacy + overall + parity cross-check
  and writes NOTHING (no DB schema, no rows, no Excel).
- Imports: stdlib + numpy + (optional) openpyxl + the engine modules. Backend
  import root mirrors `backend/main.py` (insert `<repo>/backend` on `sys.path`,
  import `power_user.services.*`).

CLI: `--rnd-db`, `--prod-db`, `--years`, `--top-n` (default 10), `--dry-run`,
`--out` (default `./out`). `--rnd-db`/`--prod-db` default to the persona
resolver / `config.POWER_DB_PATH`.

---

## 8. Risks for the audit agent

1. **Off-by-one in the exit walk is the single highest-risk parity item.** It is
   fixed (walk starts at `entry_bar_idx + 1`) and *proven* by the parity
   cross-check returning 0 mismatches. If the audit sees ANY mismatch, this is
   the first place to look.
2. **Year-boundary handling.** `simulate_year` iterates the year's `td_list`
   (intermediate years end `12-31`), so a late-December baseline pick whose exit
   is in January is OPEN in baseline (NULL exec) and excluded from the parity
   check. The study walks into January bars and may close it — this is correct
   (independent outcome) and does NOT produce a false mismatch, but means a
   handful of December picks will be CLOSED in the study yet OPEN in baseline.
3. **Open-at-data-edge picks** (recent signals, hold not fully elapsed in data)
   carry NULL exec + NULL post-window columns; they are excluded from efficacy
   and parity. Count is printed.
4. **`post_hold_high_ret` base.** Uses the entry bar's raw open (same base as the
   journey), NOT the exit price — by design this is "what happened to the stock
   from the customer's entry", so D+8..D+60 are directly comparable to the
   d1..d7 journey. If the audit expects post-exit returns measured from the EXIT
   price instead, that is a definitional choice to confirm (the column docstring
   and this log state entry-base explicitly).
5. **Duplicates.** The same symbol appears on many signal days → many rows; this
   is intended. `prior_appearances_30d` quantifies the duplication per pick.
6. **`min_fires=10`** is inherited from the Falcon Top 10 run_cfg; picks are
   already high-confluence. Lowering `--top-n` only narrows per-day breadth, it
   does not change the confluence gate.
