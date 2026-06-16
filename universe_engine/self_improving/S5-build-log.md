# S5 — Big Winner / Big Loser Study (build log)

**File:** `universe_engine/self_improving/build_bigwinner_study.py`
**Spec:** "Big Winners and Big Losers" (Table 8 per-persona thresholds; the HFCL rule)
+ `falcon_big_winner_loser_study` CREATE TABLE.
**Step in build sequence:** 5 (gate = generate `falcon_big_winner_loser_study.xlsx`).
**Nature:** pure AGGREGATION over existing resolved data — **no re-simulation**.
**CANNOT run Python in env** — validated by reading; `--dry-run` provided.

| Item | Value |
| --- | --- |
| Script | `build_bigwinner_study.py` |
| Writes table | `falcon_big_winner_loser_study` (RND DB only) |
| Report | `falcon_big_winner_loser_study.xlsx` (`--out`; orchestrator passes `out/v5`) |
| CLI | `--rnd-db` (optional, falls back to persona resolver), `--out`, `--dry-run` |
| Persona | `falcon_top10` only (the sole persona with resolved trades from Step 2) |
| Reuse | loader/DB/Excel patterns from `classify_patterns.py` + `build_signal_validity.py`; `_resolve_rnd_db_path` from `persona_simulator` (read-only import) |

## Data sources (RND, already populated — Step 2)
- `falcon_pattern_contributions` (persona='falcon_top10': `trade_id`, `pattern_id`)
  JOINed to `falcon_baseline_trades` on `bt.id = c.trade_id`.
- **All study inputs are read from the JOINed `bt` row, NOT from the contribution
  row.** Rationale: `build_baseline.py` writes `falcon_pattern_contributions.move_type
  = NULL` (sector attribution is filled later, on `falcon_baseline_trades.move_type`,
  by `classify_patterns.py` Part A). So `move_type` / `sector_tailwind` / `peak_*` /
  `trough_*` / `peak_sustained` / `exit_reason` / `net_ret_pct` come from `bt` — the
  single, post-classify source of truth. `big_winner_flag` / `big_loser_flag` are
  identical on both tables; read from `bt` for one consistent source.
- **Resolved occurrence** = a contribution row whose parent `bt` is CLOSED
  (`bt.net_ret_pct IS NOT NULL`). Open-at-end trades excluded from every aggregate
  (so `n_total_occurrences` is the HFCL denominator). Identical definition to
  `classify_patterns.py`. The same pattern contributes to many trades → many
  occurrences (this is exactly what the HFCL rule counts).

## Per-(pattern_id, persona) study columns — exact formulas
Let `OCC` = resolved occurrences; `W` = OCC with `big_winner_flag=1`; `L` = OCC with
`big_loser_flag=1`.

| Column | Formula |
| --- | --- |
| `n_total_occurrences` | `len(OCC)` |
| `n_big_winners` | `len(W)` |
| `n_big_losers` | `len(L)` |
| `n_neutral` | count of OCC with **NEITHER** flag (see note) |
| `big_winner_rate` | `n_big_winners / n_total_occurrences` |
| `big_loser_rate` | `n_big_losers / n_total_occurrences` |
| `avg_peak_ret_when_winner` | `mean(peak_ret_during_hold for w in W)` |
| `median_peak_ret_when_winner` | `median(peak_ret_during_hold for w in W)` |
| `avg_peak_day_when_winner` | `mean(peak_day_during_hold for w in W, non-NULL)` |
| `peak_sustained_rate` | `mean(peak_sustained==1 for w in W, non-NULL)` |
| `sector_tailwind_rate_winners` | `mean(sector_tailwind==1 for w in W, non-NULL)` |
| `stock_led_rate_winners` | `mean(move_type=='STOCK_LED' for w in W, non-NULL)` |
| `avg_trough_ret_when_loser` | `mean(trough_ret_during_hold for l in L)` |
| `avg_trough_day_when_loser` | `mean(trough_day_during_hold for l in L, non-NULL)` |
| `recovery_rate_after_trough` | see definition below |
| `stop_hit_rate` | `count(exit_reason=='INIT_STOP' in L) / len(L)` |
| `sector_headwind_rate_losers` | `mean(move_type=='SECTOR_HEADWIND' for l in L, non-NULL)` |
| `stock_weakness_rate_losers` | `mean(move_type=='STOCK_WEAKNESS' for l in L, non-NULL)` |
| `expected_value_at_hold_end` | `mean(net_ret_pct for o in OCC)` — realized EV at OUR exit |
| `expected_value_at_peak` | `mean(peak_ret_during_hold for o in OCC, non-NULL)` — opportunity EV |
| `pattern_maturity` | maturity cutoffs (below) on `n_total_occurrences` |
| `big_winner_candidate` | HFCL gate (below) |
| `big_loser_risk` | HFCL gate (below) |
| `week_ending` | `MAX(signal_date)` across all resolved occurrences (single snapshot) |

### `n_neutral` — why it's a direct "neither" count, not `n − W − L`
The big-winner and big-loser flags are **independent booleans** in Step 2
(`build_baseline.classify_journey`, lines 307–315): `big_winner = peak_ret > 12%`;
`big_loser = trough_ret < −7% OR exit_reason==INIT_STOP`. An occurrence can be
**BOTH** (spiked >12% then crashed <−7% on the same path). So `n − len(W) − len(L)`
would double-subtract such rows and under-count. `n_neutral` is therefore computed as
the count of occurrences with **neither** flag set. Consequence:
`n_big_winners + n_big_losers + n_neutral >= n_total_occurrences` (equality only when
no occurrence is both). Documented in code + here so an auditor doesn't read the three
counts as a partition.

### `recovery_rate_after_trough` — chosen definition (spec left it open)
The spec offered two candidate definitions ("ended > trough" OR "ended >= 0"). **We
use: among losers, the fraction whose realized exit ended STRICTLY ABOVE the trough —
`net_ret_pct > trough_ret_during_hold`.** NULL `trough_ret` rows excluded from both
numerator and denominator. Rationale: every closed trade exits at or after its trough,
so `net_ret_pct >= trough` almost always; `>` measures any genuine bounce off the low
(a loser that troughed −9% and exited −5% really did recover ground). We deliberately
do NOT use `>=0` because that conflates "recovered" with "ended profitable" and would
report ~0 for a deeply-stopped pattern even when it consistently bounces. Stated here
and in the column docstring.

### `expected_value_at_peak` vs `expected_value_at_hold_end`
- `expected_value_at_hold_end = mean(net_ret_pct)` over ALL OCC — what the trades
  ACTUALLY made at our exit. **This is the EV the HFCL gate tests.**
- `expected_value_at_peak = mean(peak_ret_during_hold)` over ALL OCC — the OPPORTUNITY
  EV (best the intra-hold path offered; an upper bound a perfect exit would capture).
- The gap `(peak EV − hold-end EV)` = give-back the exit rule left on the table;
  surfaced for exit-rule review (spec "if avg peak is D+2/D+3, flag for exit-rule
  review"). It is NOT used in any flag — informational only.

### `pattern_maturity` cutoffs (identical to `classify_patterns.py`)
`n < 18` → `insufficient_data` (== below HFCL) · `18 ≤ n < 50` → `emerging` ·
`50 ≤ n < 150` → `established` · `n ≥ 150` → `stable`.

## HFCL gate points (the statistical discipline — enforced in code)
- **`HFCL_MIN_N = 18`** (spec Statistical Discipline + Constitutional Rules #3 & #10).
- `big_winner_candidate = 1` **IFF** `n_total_occurrences >= 18` **AND**
  `expected_value_at_hold_end > 0` (positive EV across **ALL** occurrences, not just the
  winners). When `n >= 18` but EV ≤ 0 → `0`. When `n < 18` → **NULL** (cannot conclude;
  never 0-as-fact).
- `big_loser_risk = 1` **IFF** `n >= 18` **AND** `big_loser_rate >= 0.30` **AND**
  `expected_value_at_hold_end <= 0`. When `n >= 18` but condition unmet → `0`. When
  `n < 18` → **NULL**.
- The "0 vs NULL" distinction is deliberate everywhere: `0` = "has ≥18 trades, not
  flagged" (a real outcome); `NULL` = "insufficient data to say". Mirrors
  `classify_patterns.py`.
- Per-pattern winner/loser SUB-STATS (avg peak, sustained rate, etc.) are descriptive
  and computed at ANY n (NULL-safe over their available subset); only the two
  CANDIDATE/RISK booleans are HFCL-gated. This matches "compute across ALL occurrences"
  for the descriptives while gating only the actionable flags.

## Deep-dive design (HFCL / CARTRADE / IFCI) — the HFCL rule made concrete
The spec's point: the system must say *"Setup X appeared 47 times, 12 big winners, EV
+8.3%"* and **never** *"HFCL ran because of pattern X."* So the deep-dive tabs are
**symbol-level occurrence ledgers**, one tab per named SYMBOL:
- **Per-occurrence rows** from `falcon_baseline_trades` (persona=falcon_top10, closed),
  one row per signal day: `signal_date, entry/exit price, net_ret_pct, peak_ret &
  day, trough_ret & day, big_winner/big_loser flags, move_type, exit_reason`.
- **Aggregate footer line** demonstrating the rule: *"appeared N times, X big winners
  (Y%), Z big losers (W%), EV(hold-end)=+A%, EV(peak)=+B%"* with an explicit HFCL
  verdict tag (`n<18 → INSUFFICIENT to flag; do NOT conclude from this alone` vs
  `n>=18 → eligible to flag IF EV>0`).
- **If a symbol has few/zero occurrences we say so honestly** — the empty-case line
  reads *"appeared 0 times … Per the HFCL rule, NOTHING can be concluded."* That IS the
  lesson the named examples teach.
- **Additive second block** per symbol from `falcon_signal_day_study` (the
  unconstrained per-signal-day study, Step 2B) WHEN that table exists — it has MORE
  occurrences per symbol (no `skip_already_held` / cash cap), so it strengthens the
  "appeared N times" evidence. Clearly labelled `source=signal_day`; if the table is
  absent the tab notes "baseline ledger only" and proceeds.

### Occurrence counts — knowable only at run time (no Python in env)
Cannot enumerate the exact per-symbol counts here (cannot execute SQL). The script
prints them in both `--dry-run` and `APPLY`: for each of HFCL/CARTRADE/IFCI it logs
the baseline aggregate line (`appeared N times …`) and the signal_day occurrence
count. **Audit by running `--dry-run` first** and reading those three lines. Memory
context notes IFCI/CARTRADE/HFCL appear in the per-signal-day study (S2C build log
references IFCI's 06-15 bar), so non-zero counts are expected there; baseline counts
will be smaller due to `skip_already_held` + cash cap.

## Report tabs (`falcon_big_winner_loser_study.xlsx`)
1. **"Falcon Top 10"** — one row per pattern with all 26 study columns; header rows
   carry the Table-8 per-persona threshold note + the HFCL-rule note.
2. **"HFCL deep dive" / "CARTRADE deep dive" / "IFCI deep dive"** — per-symbol ledgers
   + aggregate line(s) as above (tab names truncated to Excel's 31-char limit).
3. **"HFCL Summary"** — the n≥18 + EV rule in full, plus the per-persona coverage note
   (only `falcon_top10` has data; other personas → no rows; thresholds never mixed,
   Constitutional Rule #20).
- **openpyxl with `.csv` fallback** (one study CSV + one per deep-dive symbol) so the
  step never hard-fails if openpyxl is missing — same pattern as the sibling steps.

## Idempotency & safety
- **Idempotent write:** single transaction — `DELETE FROM falcon_big_winner_loser_study
  WHERE persona='falcon_top10'`, then `INSERT` all rows. `week_ending` is a single
  value (MAX signal_date), so the `UNIQUE(pattern_id, persona, week_ending)` key is
  honored and a re-run on the same data is a clean rebuild (no dupes, no drift).
- **RND-only:** every connection is to `--rnd-db`. No PROD connection. The only PROD
  import is `_resolve_rnd_db_path` (read-only path helper). **No shared/engine code
  modified (INV2).** Additive — creates rows only in the Step-1 study table; touches no
  other table.
- **Guard:** if `falcon_big_winner_loser_study` is absent, the write aborts with a
  clear "apply Step-1 schema first" error (rolled back, nothing written). If there are
  zero resolved occurrences, the script reports and exits 0 without writing.
- **`--dry-run`** computes everything (study rows, deep-dive ledgers, aggregate lines)
  and prints a full summary + an 8-pattern preview; writes nothing (no DB row, no file).

## Risks / notes for audit
1. **`move_type` / `sector_tailwind` depend on `classify_patterns.py` (Step 3) having
   run.** If Step 3 has NOT run, `bt.move_type` / `bt.sector_tailwind` are NULL → the
   `*_rate_winners` / `*_rate_losers` / `sector_tailwind_rate_winners` columns come out
   NULL. The counts, rates, peak/trough stats, EVs, and HFCL flags are UNAFFECTED
   (they don't depend on sector attribution). Run order should be Step 2 → Step 3 →
   Step 5. Documented so a NULL sector column isn't read as a bug.
2. **`week_ending` is a backtest-tail snapshot, not a live week.** The genuine weekly
   refresh is Step 6 / Update 5. One `week_ending` per run by design.
3. **Descriptive sub-stats are reported at any n** (NULL-safe); only the two
   actionable booleans are HFCL-gated. An auditor wanting "EV across all occurrences"
   gets it for every pattern; the gate only blocks the FLAG, per spec.
4. **Big-winner & big-loser are not mutually exclusive** (see `n_neutral` note) — the
   three counts are not a strict partition. Intentional, matches Step-2 flag logic.
5. **Deep-dive symbol counts unknown until run** (no Python) — surfaced via `--dry-run`
   prints; the design handles zero/thin counts honestly.
