# Research rerun on corrected market data

Generated 2026-09-16 by W8. Inputs: the frozen clean snapshot in `db/market15.db`; code
`market_data/research_export.py` (new), `market_scanner/pattern_research/runner.py`,
`market_scanner/pattern_research/outcomes_cli.py`, `market_scanner/pattern_research/outcomes_summary.py`
(new: the reporting queries and the two-stage FDR refinement). Contract: `docs/DATA_PIPELINE_CONTRACT.md`.
What was corrected and what is labelled bad: `docs/pattern_research/DATA_REPAIR_REPORT.md`.

`db/kanida.db` and `db/market15.db` were opened **read-only** for every step. Nothing was
interpolated: a symbol, session or timeframe we cannot build is an explicit status, never a
manufactured candle.

**Headline.** 495 symbols, 107 patterns, 518,760 study cells, 1,001,880 outcome cells and 4,052,091
out-of-sample trades. At the engine's default resolution **0 cells and 0 condition buckets** survive
Benjamini-Hochberg — but that "0" was a **resolution limit, not a measurement**: the bootstrap
p-value is floored at 1/(replicates+1) and at 400 replicates no cell could have been declared a
discovery whatever the data said. A two-stage refinement at 2,000,000 replicates removes the floor
and gives the honest answer: **2 of 94,979 cells are discoveries at q<0.1 and at q<0.05** —
and both of them fail the engine's own baseline-coverage guard and are single-stock, single-year
phenomena. Everything else holds: the conditional forward return is indistinguishable from the
unconditional baseline, no timeframe reaches the break-even hit rate for a +2% / -1% barrier, the
uncorrected significant rate is exactly the chance rate (5.80% / 5.03%), and correcting the data did
not change the research picture (positive walk-forward share 41.92% -> 41.86%). Sections 3.2-3.3 and
4 carry the numbers.

---

## 1. The data this run used

| field | value |
|---|---|
| snapshot_id | `snap_20260916T055228_f4bab0e9` |
| checksum | `580af23f585159081c62760eaf1265a2a7afd498c34652df59e6c3b894dd7b19` |
| checksum re-verified by this run | **yes** — `MarketStore.verify_snapshot` recomputed it over all 27,867,377 rows and it matched |
| universe | NIFTY 500 minus quarantined symbols = **495 symbols** |
| provider / adjustment basis | kite / `kite-eod-adjusted` |
| snapshot first / last 15-minute bar | 2015-02-02 09:15:00 / 2026-09-15 15:15:00 |
| snapshot rows | 27,867,377 |
| research cutoff | 2026-09-15 15:30:00 (end of the last session that had actually closed) |

### 1.1 Quarantined symbols (excluded from the universe)

`AKI`, `GSPL`, `GUJGASLTD`, `HEG`, `HFCL`, `JBCHEPHARM`, `LTIM` — all `not_in_provider_instrument_list`,
verified against a refreshed Kite NSE instrument dump. They are absent, not zero-filled.

### 1.2 Rows excluded by label

Rows the repair labelled unusable stay in `candles_15m` (deleting them would be the silent edit the
store exists to prevent) and are dropped from the research snapshot by label.

| label | rows dropped from the snapshot (W7) | rows dropped again at export (W8) |
|---|---|---|
| `vendor_zero_print` | 3,152 | 0 |
| `vendor_bad_print` | 3 | 0 |
| `wrong_instrument` | 40,164 | 2,515 |
| `unresolved` | 2,323 | 0 |

The export re-applies the same label filter independently of the snapshot, so the zeros above are a
**confirmation** that the snapshot already excluded them, not an assumption. The 2,515 extra
`wrong_instrument` rows are explained in 1.3.

Per-symbol exclusion counts, first/last bar and row provenance: `market_scanner/output/history/m15clean-20260916_manifest.json`.

### 1.3 Reused instrument tokens — one rule beyond the snapshot's

The repair classified 10 symbols as `wrong_instrument` (a Kite token with an earlier life) and
excluded each symbol's bars up to *the vendor's own first daily bar*. For two of them the vendor's
**daily** series carries the reused token's block as well, so that boundary is not enough:

- `STARHEALTH` — daily bars from 2016-01-04 at a flat ~1000-1076, then a 1,390-day hole, then the real
  listing on 2021-12-10;
- `DELHIVERY` — daily bars from 2016-01-18 at ~5-11, then a 2,171-day hole, then 2022-05-24 at ~495.

So the export takes a symbol's usable start as
`max(vendor_first_daily_session, first session after the last gap of >= 180 calendar days)` and drops
every 15-minute **and** daily bar before it, under the same `wrong_instrument` label. 180 days is
declared in code before any return was looked at, and is applied **only** to the 10 symbols the
repair had already classified on independent evidence (FORCEMOT's genuine 110-day suspension is well
inside it). Effect: STARHEALTH 2,379 rows, DELHIVERY 136 rows; the other eight symbols were already
covered by the snapshot's own rule.

### 1.4 Aggregation

| timeframe | source | rule |
|---|---|---|
| 1H, 4H | `candles_15m` (snapshot rows only) | `market_data.aggregate` bucketed by the symbol's own `SessionCalendar`. 4H buckets are 09:15-13:15 and 13:15-session end. Under the CAS regime (contract 2A) a symbol's session ends 15:15 from 2026-08-03, so its last 1H bucket is 14:15-15:15 — **208 of the 495 symbols are on the CAS regime.** |
| 1D | the provider's own daily bars | never the intraday close: a CAS stock's official close is the 15:30-15:35 auction price, and pre-CAS it was a 30-minute VWAP. |
| 1W | Mon-Fri weeks built from that 1D series (`weekly_from_daily`) | |

Incomplete buckets are dropped, never padded. No bar is synthesised.

**Provider daily history.** `daily_bars` only holds what the live ingest backfilled — 2021-07-05
onward, 1,291 sessions. Using it alone would have cut 1D/1W history from ~13.5 years to 5.2 years.
The repair pass had already archived the vendor's **full** daily series (2013-01-01 onward, 1,530
sha256-verified payloads in `raw_archive`). The export therefore uses `daily_bars` wherever it has
the session and the verified archive before that. The two were compared on every overlapping
session: **1,290 overlapping sessions per symbol, 0 disagreements across all 495 symbols.**
697,269 daily bars came from the archive only.

**Calendar.** `db/market15_calendar.json` starts 2015-02-02 (where the legacy intraday history
starts), so the 2013-2015 daily bars would have had no session to sit in. The export back-extends the
calendar with the same rule the calendar itself uses — *a date the reference universe
(RELIANCE, INFY, TCS, HDFCBANK, ICICIBANK) traded is a session* — fed from the provider daily series:
**516 sessions added**, calendar span 2013-01-01 .. 2026-10-15, 3,415 sessions.
1,066 pre-2021 daily bars carried a regular 09:15-15:30 stamp on Muhurat evenings; they were
relabelled onto the session window the calendar observed (label only — no price or volume touched),
which removes 13 spurious 1D "gap" flags per large-cap symbol.

**368 fifteen-minute bars** across the whole universe fall on dates the calendar has no session for.
`market_data.aggregate` drops them rather than inventing a session; they are counted per symbol in
the manifest.

### 1.5 Export result and its verification

| measure | value |
|---|---|
| source run id | **`m15clean-20260916`** |
| symbols exported / errors | 495 / 0 |
| wall clock | 263 s (8 workers) |
| 15-minute rows read from the snapshot / used | 27,867,377 / 27,864,931 |
| provider daily rows used | 1,272,555 |
| candles written | 1H 7,767,651 · 4H 2,209,882 · 1D 1,271,957 · 1W 267,527 |
| every symbol's last candle | 2026-09-15 15:30:00 |

Verification (`market_scanner/output/history/m15clean-20260916_verification.json`), **passed**:

1. for RELIANCE, TITAN and INFY the exported 1H and 4H match a direct, independent
   `market_data.aggregate.aggregate` call bar for bar and field for field (20,014 / 5,728 ·
   20,015 / 5,729 · 19,936 / 5,708);
2. across **all 495 symbols and 11,517,017 candles**: 0 bars outside a session, 0 bars on a date with
   no known session, 0 bars that run past a CAS symbol's 15:15 session end.

### 1.6 Depth against the frozen research (same 495 symbols)

| timeframe | clean median / total | legacy median / total | total delta |
|---|---|---|---|
| 1H | 19,935 / 7,767,651 | 19,680 / 7,644,474 | +1.6% |
| 4H | 5,654 / 2,209,882 | 5,533 / 2,164,679 | +2.1% |
| 1D | 3,387 / 1,271,957 | 3,354 / 1,255,687 | +1.3% |
| 1W | 713 / 267,527 | 700 / 262,277 | +2.0% |

The clean history is the **same depth** as the frozen research, and the small surplus is the later end
date (2026-07-29 -> 2026-09-15, ~33 extra sessions). The difference between the two runs is therefore
the *content* of the candles, not how many there are.

---

## 2. Research run

```
python -m market_scanner.pattern_research.runner --workers 8          # prepare
python -m market_scanner.pattern_research.runner --execute-manifest .../4b33a5249562631524d6/manifest.json
```

| field | value |
|---|---|
| research run id | **`4b33a5249562631524d6`** |
| source run | `m15clean-20260916` (snapshot `snap_20260916T055228_f4bab0e9`) |
| modules | legacy, candlesticks, chart_patterns, price_action, harmonics |
| catalogue | **107 pattern ids, 262 directional study variants** |
| timeframes | 1H, 4H, 1D, 1W |
| symbols | 495 |
| study cells | 262 x 4 x 495 = **518,760** |
| workers | 8 |
| wall clock | **51 min 36 s** |
| symbols complete / errors | **495 / 0** |
| protocol | `expansion-v1-36m-train-6m-test-min20-40bps` |

### 2.1 Gate re-run before the research

`runner.prepare` refuses a full-universe run unless `independent_validation.json` passes and its code
hashes still match. It was re-run **on the clean data** and passed: 107 catalogue ids, 262 directional
variants, event contracts valid and prefix-causality equivalence (no future-dependent detector output)
on TITAN across all four timeframes.

That exposed a real defect, which was fixed: `validation.py` hashed *every* `pattern_research/*.py`
while `runner.code_files()` had been changed to exclude `outcomes*.py` from run identity, so the gate
could never be satisfied again. `validation.identity_hashes()` now derives the file set from
`runner.code_files()` instead of restating the rule. Both test suites stay green
(`market_scanner/pattern_research/tests` 97 passed, `market_data/tests` 148 passed).

### 2.2 Coverage, as the runner reports it

| status | clean run (495 symbols) | share | frozen run, same 495 symbols | share |
|---|---|---|---|---|
| `no_walkforward_trades` | 252,308 | 48.64% | 248,914 | 47.98% |
| `no_occurrences` | 160,323 | 30.91% | 164,457 | 31.70% |
| `insufficient_walkforward_history` | 33,922 | 6.54% | 34,933 | 6.73% |
| `small_walkforward_sample` | 25,733 | 4.96% | 24,958 | 4.81% |
| `tested_nonpositive` | 24,384 | 4.70% | 23,626 | 4.55% |
| `tested_positive` | **17,557** | **3.38%** | **17,055** | **3.29%** |
| `insufficient_history` | 4,533 | 0.87% | 4,817 | 0.93% |
| total | 518,760 | | 518,760 | |

Cells with >= 20 walk-forward trades: **41,941 clean vs 40,681 dirty**; of those, positive expectancy
**17,557 (41.9%) clean vs 17,055 (41.9%) dirty**.

Per timeframe (cells with >= 20 walk-forward trades / of which positive):

| timeframe | clean | legacy |
|---|---|---|
| 1H | 14,234 / 4,421 | 14,013 / 4,429 |
| 4H | 16,112 / 7,328 | 15,425 / 7,025 |
| 1D | 10,585 / 5,137 | 10,488 / 5,089 |
| 1W | 1,010 / 671 | 755 / 512 |

**Reading this honestly: correcting the data did not change the research picture.** The positive
share is identical to the second decimal. The bad rows the repair found were 0.4% of the store and
they were not what was producing the result.

---

## 3. Outcome-evidence pass

```
python -m market_scanner.pattern_research.outcomes_cli --run 4b33a5249562631524d6 \
    --snapshot-id snap_20260916T055228_f4bab0e9 --summary-only --workers 8
```

| field | value |
|---|---|
| outcome run id | **`0b89eff7c36c01b28af0`** |
| engine version | 1.2.0 |
| horizons | 1H 1..30 · 4H 1..20 · 1D 1..10 · 1W 1..8 |
| costs | 0.40% of entry notional, charged once at the measured horizon |
| bootstrap replicates | 400 |
| barrier grid | pct 1:1, **pct 2:1**, pct 3:1.5, atr 1:1, atr 2:1 — ties inside a bar count as the **stop** |
| bucket minimum sample | 30 |
| detector code | verified **by code hash** against the research manifest; no drift in the detection closure |
| protocol | `outcome-v1-36m-train-6m-test-min20-40bps` |
| symbols complete / errors | **495 / 0** |
| wall clock | **15,022 s (4 h 10 min)**, 8 workers |
| rows written | 1,001,880 cell outcomes · 3,807,177 condition buckets · 0 occurrence rows (`--summary-only`) |

### 3.1 Out-of-sample cells

| measure | value |
|---|---|
| outcome cells total | 1,001,880 |
| cells with at least one occurrence | 615,535 |
| cells with a tested out-of-sample sample | 64,472 |
| **cells with >= 20 out-of-sample trades** | **64,472** |
| of those, **positive** expectancy | **26,823 (41.60%)** |
| of those, BH discoveries after refinement (q<0.1 / q<0.05) | **2 / 2** |
| out-of-sample trades behind them | 4,052,091 |
| mean / median out-of-sample expectancy over those cells | **-0.151% / -0.139%** |

Selection status over all 1,001,880 cells: `insufficient` 770,617 · `insufficient_history` 127,512 ·
`small_out_of_sample_sample` 39,279 · `tested` 64,472.

Per timeframe. "stage 1" is the engine's own 400-replicate BH, which section 3.2 shows could not have
rejected anything; "refined" is the two-stage BH of section 3.2a, which could.

| timeframe | >= 20 OOS trades | positive | stage-1 q<0.1 / q<0.05 | refined q<0.1 / q<0.05 |
|---|---|---|---|---|
| 1H | 27,211 | 9,374 | 0 / 0 | **1** / **1** (CGPOWER) |
| 4H | 21,980 | 10,407 | 0 / 0 | **1** / **1** (TTML) |
| 1D | 14,083 | 6,222 | 0 / 0 | 0 / 0 |
| 1W | 1,198 | 820 | 0 / 0 | 0 / 0 |
| **all** | **64,472** | **26,823** | **0 / 0** | **2 / 2** |

### 3.2 Benjamini-Hochberg — stage 1 (the engine's default), and why it was not a measurement

| family | trials | q<0.1 | q<0.05 | smallest p | smallest q |
|---|---|---|---|---|---|
| cells (`cell_outcomes.oos_p_value`) | 94,979 | 0 | 0 | 0.002494 | 0.857 |
| condition buckets (`bucket_outcomes.p_value`) | 810,220 | 0 | 0 | 0.002494 | 0.293 |

Cells and buckets are never pooled; each is its own testing family, corrected once after every symbol
was committed.

**Those zeros were a resolution limit.** The reported p-value is
`max(student_t, stationary_bootstrap)` — deliberately conservative — and the stationary bootstrap
cannot return a value below `1/(replicates+1)`. At the default 400 replicates that floor is
**0.002494**, which is exactly the smallest p in both families. Benjamini-Hochberg at rank 1 needs
`p < 0.1/94,979 = 1.05e-6` (cells) or `1.2e-7` (buckets), so **no cell could have been declared a
discovery at 400 replicates whatever the data said**. 238 cells and 6,905 buckets sat on the floor;
a q<0.1 discovery would have needed >= 2,369 cells or >= 20,207 buckets there. Stage 1 alone
therefore proves nothing about the data, and the run was re-tested.

### 3.2a Two-stage FDR with adequate resolution (the measurement)

Protocol, with the threshold declared before any refined p-value was computed. The selection
protocol, the baseline and the barrier analysis are **unchanged**; only the resolution of the
significance test moves.

- **Stage 1**: the run's own screen, exactly as above.
- **Stage 2**: every cell whose *unfloored* Student-t p-value is below **p_t < 0.01** is re-tested
  with the same stationary block bootstrap (same null, same mean block of 4 bars, same add-one
  correction) at **R = 2,000,000** replicates, drawn in chunks of 20,000 with a sequential stop once
  1,000 replicate means have matched or beaten the observed one (at which point the p-value is
  already far above anything BH could select; `replicates_used` is reported per cell).
- **Then BH across the full 94,979-cell family**, using the refined p where one was computed and the
  stage-1 p everywhere else. That mixture is **conservative**: a stage-1 p can only be too *large*,
  which lowers a refined cell's rank and therefore *raises* its q-value. It can never manufacture a
  discovery.

| measure | value |
|---|---|
| candidates (p_t < 0.01) | **1,144** of 94,979 |
| replicates | **2,000,000** |
| resulting floor `1/(R+1)` | **5.0e-7** |
| BH rank-1 requirement | 1.05e-6 (q<0.1) · 5.26e-7 (q<0.05) |
| does the floor still bind at rank 1? | **no** at both levels — **0 ranks unreachable** |
| refined cells that still hit the 2e6 floor | 225 |
| **discoveries** | **2 at q<0.1 · 2 at q<0.05** (smallest q = **0.0475**) |
| wall clock | 916 s, 8 workers |

Command: `python -m market_scanner.pattern_research.outcomes_summary --run 0b89eff7c36c01b28af0 --refine`.
Nothing was written to `outcomes.sqlite3`; the refinement is a read-only recomputation from the
out-of-sample returns stored in each cell's `summary`, and it reproduces the stored 400-replicate
p-value exactly when run at R=400.

**The two discoveries, and why neither is a strategy.**

| # | cell | OOS n | OOS mean | win | p_t | p_boot (2e6) | q (refined) | baseline diff (CI95) | P(target first) | years / positive |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **CGPOWER** 1H CDLGAPSIDESIDEWHITE `canonical` long/setup | 41 | **+5.44%** | 82.9% | <1e-6 | 1.0e-6 | **0.0475** | **+7.69 pp** (+4.59, +11.22) | 71.4% | 8 / 7 |
| 2 | **TTML** 4H CDLGAPSIDESIDEWHITE `canonical_context` long/confirmed | 59 | **+4.85%** | 88.1% | <1e-6 | 5.0e-7 | **0.0475** | **+3.27 pp** (+2.45, +4.14) | 86.4% | 5 / 4 |

Both are genuine under the test. Both also fail the engine's own guard rail:

- **Both carry `baseline_window_mismatch = 1`.** The unconditional baseline covers only **70.0%**
  (CGPOWER) and **38.3%** (TTML) of the cell's occurrence window, against a declared minimum of 80%.
  Only 28 of the 1,144 candidates are flagged, so this is not a generic condition — both survivors
  are in the flagged minority, and their headline "conditional beats baseline by +7.7 / +3.3 pp" is
  therefore comparing partly different periods.
- **Both are one stock in one extraordinary year.** CGPOWER: 18 of its 42 all-history occurrences are
  in 2020 at +10.44% mean (the post-COVID recovery); TTML: **59 of 103 occurrences are in 2021**, the
  year the stock went up roughly tenfold, and it has only 5 distinct years of occurrences at all.
  The engine's concentration test (drop the best two calendar years) reads 0 for both, but that test
  cannot see that the occurrences themselves are concentrated in one regime.
- Both are small caps, and the effect is a single candlestick id (`CDLGAPSIDESIDEWHITE`) that fires
  91 and 103 times in eleven years.

Two single-stock cells out of 94,979, both with a flagged baseline comparison and both loaded into
one year, are a reason to look at those two names — not evidence of a pattern edge. They are reported
here because they are what the data says, not because they are tradeable.

Ranks 3-10 by refined q are **0.19 or worse** and are dominated by *negative*-expectancy cells
(JUBLFOOD -2.95% at q=0.19, TTML 1H `canonical_context` +6.34% at q=0.27, then BAYERCROP -1.98%,
BIOCON -1.84%, COCHINSHIP -10.88% on n=6, COALINDIA -0.90%, CYIENT -8.75% on n=7, EIDPARRY -2.21%,
all at q=0.797). A two-sided test flags an extreme negative mean as readily as a positive one, and
several of these have n < 15, where the stationary bootstrap degenerates (with six returns every
resample mean has the same sign, so the bootstrap p collapses to its floor). In those cells the
`max(student_t, bootstrap)` rule is what keeps the reported p honest, and it is the t-value that
binds.

**What remains floor-limited.** The condition-bucket family was **not** refined: its p-value tests a
different null (bucket mean vs unconditional entries in the same bucket) through
`bootstrap_difference_p_value`, and 810,220 trials would need R ≳ 1.6e7 per row. Its 0 discoveries
therefore remain a resolution limit, not a measurement, and are reported as such. The uncorrected
bucket rate (5.03%, section 3.3a) is the statement the bucket data actually supports.

### 3.3 The measurements that never depended on the floor

**(a) No excess of significant results anywhere in the universe.** Uncorrected, 5,512 of 94,979 cells
(**5.80%**) and 40,743 of 810,220 buckets (**5.03%**) have p<0.05. At a 5% test level pure noise
produces 5%. There is no reservoir of real effects hiding under the multiplicity correction.

**(b) The seven parametric candidates, and what the refined bootstrap did to each.** Before the
refinement, BH over the *Student-t p-value alone* (no floor, `outcomes_summary --crosscheck`) found
**7 of 94,979 cells at q<0.1 and 5 at q<0.05**, in three symbols. Refining each with 2,000,000
bootstrap replicates promotes two of them and demotes five. Full evidence per cell:

| cell | OOS n | OOS mean | win | baseline diff (CI95) | coverage | p_t | p_boot (2e6) | q refined | verdict |
|---|---|---|---|---|---|---|---|---|---|
| **CGPOWER** 1H CDLGAPSIDESIDEWHITE `canonical` L/setup | 41 | +5.44% | 82.9% | +7.69 (+4.59, +11.22) | **0.70 flagged** | <1e-6 | 1.0e-6 | **0.047** | **discovery**, baseline flagged |
| **TTML** 4H CDLGAPSIDESIDEWHITE `canonical_context` L/confirmed | 59 | +4.85% | 88.1% | +3.27 (+2.45, +4.14) | **0.38 flagged** | <1e-6 | 5.0e-7 | **0.047** | **discovery**, baseline flagged |
| JUBLFOOD 4H PA05 `inside_nr4_break_down` S/confirmed | 12 | **-2.95%** | 0.0% | +0.27 (-0.41, +0.86) | 0.99 | 6e-6 | 5.0e-7 (n=12, degenerate) | 0.190 | not a discovery |
| TTML 1H CDLGAPSIDESIDEWHITE `canonical_context` L/setup | 70 | +6.34% | 80.0% | +3.17 (+2.66, +3.58) | 0.69 | <1e-6 | 1.15e-5 | 0.273 | not a discovery |
| TTML 1H PA07 `rising` L/setup | 180 | +3.69% | 56.7% | +4.48 (+1.37, +7.35) | 0.99 | 1e-6 | 1.04e-4 | >0.797 | not a discovery |
| TTML 1H CDLGAPSIDESIDEWHITE `canonical` L/setup | 88 | +5.87% | 68.2% | +6.77 (+3.56, +10.30) | 0.80 | 5e-6 | 2.11e-4 | >0.797 | not a discovery |
| TTML 4H PA07 `rising` L/confirmed | 85 | +4.13% | 71.8% | +3.06 (+2.42, +3.67) | 0.74 | 2e-6 | 5.40e-4 | >0.797 | not a discovery |

("coverage" is `baseline_window_coverage`; the engine flags anything below 0.80.)

This is exactly the diagnosis the coordinator expected, with one correction: the block bootstrap does
**not** simply overturn the t-test. For the five demoted cells the refined bootstrap p is 20x to 500x
larger than the t p-value — fat tails, as predicted — and they fall out. For two cells the bootstrap
agrees with the t-test at full resolution and they become genuine BH discoveries.

Year by year, every one of the seven is concentrated in the 2020-2021 small-cap surge:

| cell | occurrences by year (mean net return) |
|---|---|
| CGPOWER 1H canonical | 2016:3 (+1.4%) · 2018:2 (-0.9%) · 2019:7 (+4.9%) · **2020:18 (+10.4%)** · 2021:6 (+6.3%) · 2022:2 · 2023:2 · 2024:2 |
| TTML 4H canonical_context | 2017:8 (+3.2%) · 2019:8 (-0.4%) · 2020:18 (+2.7%) · **2021:59 (+3.5%)** · 2022:10 (+2.6%) |
| TTML 1H canonical_context | 2015:2 · 2016:2 · 2017:12 · 2018:4 · 2019:14 · 2020:32 · **2021:70** · 2022:19 · 2023:2 |
| TTML 1H PA07 rising | 10 / 8 / 14 / 8 / 9 / 14 / **29** / 23 / 21 / 15 / 13 / 11 across 2015-2026, 10 of 12 years positive |
| JUBLFOOD 4H PA05 | 17-24 per year across 2015-2026, 4 of 12 years positive, all-history expectancy **-0.32%** |

CGPOWER puts 43% of its occurrences in 2020 and TTML's 4H cell puts 57% in 2021 — the year TTML rose
roughly tenfold. TTML's PA07 and JUBLFOOD cells, by contrast, are spread evenly across twelve years,
and those are precisely the two the refinement rejects. The all-history record of the two survivors
is strong (CGPOWER n=42, 73.8% wins, +6.93% expectancy, profit factor 6.7; TTML 4H n=103, 83.5%
wins, +2.92%, profit factor 4.6) but it is one stock each, in one regime each, with a baseline
comparison the engine itself marks unreliable.

**(c) Conditional forward return is the unconditional baseline.** Every cell is compared with
unconditional entries on **every eligible bar in the same window, at the same horizon and the same
cost model**, at the cell's stability horizon. Medians across cells:

| timeframe | cells compared | median unconditional | median conditional | median difference | cells with a positive difference | cells whose 95% CI excludes zero |
|---|---|---|---|---|---|---|
| 1H | 184,670 | -0.397% | -0.359% | **+0.025 pp** | 99,655 (54.0%) | 7,609 (4.1%) |
| 4H | 161,702 | -0.376% | -0.285% | **+0.068 pp** | 87,262 (54.0%) | 5,867 (3.6%) |
| 1D | 156,712 | -0.280% | -0.315% | **-0.014 pp** | 77,618 (49.5%) | 2,908 (1.9%) |
| 1W | 111,482 | -0.126% | -0.345% | **-0.185 pp** | 52,793 (47.4%) | 1,111 (1.0%) |

Both columns are negative at every timeframe: after 0.40% round-trip costs the average entry loses
money whether or not a pattern fired, and the pattern moves the median by hundredths of a percentage
point in either direction. The share of cells whose CI excludes zero (1.0-4.1%) is at or below the
5% a 95% interval produces by chance.

**(d) The barrier table does not reach break-even.** First touch of +2% target / -1% stop, ties
inside a bar counted as the **stop** (intrabar order is unknowable from OHLC, so the pessimistic
branch is the only honest one), weighted by occurrences:

| timeframe | cells | occurrences | P(target first) | P(stop first) | P(neither) | median bars to target | median bars to stop |
|---|---|---|---|---|---|---|---|
| 1H | 184,674 | 66,834,045 | **32.51%** | 65.76% | 1.18% | 4.5 | 3.0 |
| 4H | 161,754 | 17,407,822 | **32.51%** | 67.14% | 0.05% | 2.0 | 1.0 |
| 1D | 156,941 | 9,409,118 | **29.74%** | 70.13% | 0.07% | 1.0 | 1.0 |
| 1W | 111,759 | 1,925,529 | **19.10%** | 80.89% | 0.00% | 1.0 | 1.0 |

A 2:1 payoff needs **P(target first) > 33.33%** just to break even *before* costs. No timeframe
reaches it; 1D and 1W are far below. Median per-cell P(target first) tells the same story
(32.43 / 32.13 / 27.91 / 13.85%).

### 3.4 The strongest pre-multiplicity filter

Cells with >= 20 out-of-sample trades **and** positive expectancy **and** a bootstrap
conditional-minus-baseline CI above zero **and** an edge not concentrated in their best two calendar
years: **1,615 of 64,472 (2.5%)**. Even this filter is applied without multiplicity correction and
over 64,472 candidates it is what chance supplies; it is listed as the ceiling of the optimistic
reading, not as a result.

---

## 4. What survives correction, and what does not

**First, the distinction that decides how to read everything below.**

*"No edge is detectable"* and *"the test lacked the resolution to detect one"* are different claims,
and this run produced both. At the engine's default 400 bootstrap replicates the cells family could
not have produced a discovery at any data — that is a **resolution limit**. After refining to
2,000,000 replicates the floor no longer binds and the family gives a real answer — that is a
**measurement**. The condition-bucket family was not refined and stays a resolution limit. Each claim
below is labelled accordingly.

**Does not survive — measured, not floor-limited.**

1. **Essentially no pattern cell is a statistical discovery.** *(measurement)* With a bootstrap that
   can resolve BH rank 1, **2 of 94,979 cells** reach q<0.1 and q<0.05. Both are one stock each
   (CGPOWER 1H and TTML 4H, both the single candlestick id `CDLGAPSIDESIDEWHITE`, both long), both
   carry the engine's `baseline_window_mismatch` flag (baseline coverage 0.70 and 0.38 against a
   declared 0.80 minimum), and both load 43% / 57% of their occurrences into a single year (2020,
   2021). They are two idiosyncratic single-symbol cells, not a pattern effect, and nothing in this
   run supports trading them.
2. **No excess of significant results anywhere.** *(measurement)* Uncorrected, 5.80% of cells and
   5.03% of buckets have p<0.05 — the rate a 5% test produces on pure noise. There is no reservoir of
   real effects sitting under the correction.
3. **Conditioning on a pattern does not change the forward return.** *(measurement, independent of
   any p-value)* The median conditional and unconditional means differ by +0.025, +0.068, -0.014 and
   -0.185 percentage points at 1H/4H/1D/1W, and both are negative net of 0.40% costs.
4. **The +2%/-1% barrier is not reachable.** *(measurement, independent of any p-value)*
   P(target first) is 32.5 / 32.5 / 29.7 / 19.1%, below the 33.3% break-even at every timeframe,
   before costs.
5. **The correction did not change the conclusion.** *(measurement)* On the identical 495 symbols and
   identical 518,760-cell grid, `tested_positive` went 17,055 -> 17,557 and the positive share among
   cells with >= 20 walk-forward trades went 41.92% -> 41.86%. The bad rows were real and are now
   excluded, but they were roughly 0.4% of the store and they were not what produced the earlier
   picture.

**Not decided — the test lacked resolution.**

6. **The condition-bucket family.** *(resolution limit)* 0 of 810,220 at q<0.1 and q<0.05, but its
   p-values are floored at 0.002494 exactly as the cells were, and BH rank 1 there needs 1.2e-7.
   Refining it would need R ≳ 1.6e7 per row against a two-sample difference bootstrap. **Do not read
   its zero as evidence.** What the bucket data does support is item 2: its uncorrected rate is the
   chance rate. Volume, regime and quality buckets show no sign of rescuing any pattern, but that is
   a statement about the absence of an excess, not a corrected test.
7. **Cells below the stage-2 candidate threshold.** *(resolution limit, deliberately accepted)* Cells
   with p_t >= 0.01 kept their stage-1 p-value. Their floor can only inflate a p-value, so it can
   only *reduce* the discovery count — the mixture is conservative and cannot have manufactured the
   two discoveries in item 1 — but a cell whose true p is small and whose t-value happens to be weak
   would not have been found. 1,144 candidates were refined out of 94,979 trials.

**Does survive.**

1. **The data itself.** 495 symbols, 27.9 M snapshot rows, checksum re-verified, every labelled row
   excluded by label, 0 disagreements between the two provider-daily sources over 1,290 overlapping
   sessions per symbol, and 11,517,017 exported candles with 0 outside their session regime. The
   reruns are reproducible from `snap_20260916T055228_f4bab0e9` + `m15clean-20260916`.
2. **The pipeline's honesty properties.** Prefix-causality (no future-dependent detector output) was
   re-verified on the clean data across all four timeframes; entry is the next eligible open; costs
   are charged on every simulated trade; the baseline uses the same window, horizon and cost model;
   the barrier tie rule is pessimistic; the two testing families are corrected separately.
3. **One honest negative result**, which is a result: across 107 patterns, 262 directional variants,
   4 timeframes, 495 stocks and 4.05 M out-of-sample trades, chart-pattern occurrence alone carries
   no measurable forward edge after costs on this universe — with two single-symbol exceptions that
   fail the baseline-coverage guard.
4. **Two names worth a separate, properly designed look** — not a promotion: CGPOWER 1H and TTML 4H
   `CDLGAPSIDESIDEWHITE` long. Anything built on them must first fix what disqualifies them here: a
   baseline window that covers the occurrence window (>= 0.80), and occurrences that are not more
   than half in one calendar year. Both are small caps; TTML's 2021 is a tenfold move and cannot be
   assumed to repeat.

**What this report cannot claim.**

- There is **no full pre-correction outcome run to compare against**. The two outcome runs that exist
  on the dirty research run (`b11f9d44e3fb13a49cfd`, `aa179f21928fddf1c3eb`) cover 5 and 2 symbols.
  The before/after comparison in this report is therefore at the *research-coverage* level
  (section 2.2), which is exact, and not at the outcome level.
- The engine's own stored q-values (`cell_outcomes.oos_q_value`, `bucket_outcomes.q_value`) are still
  the 400-replicate, floor-limited ones; the refinement is a read-only recomputation and was
  deliberately **not** written back. Anyone quoting a q-value from the store must say which stage it
  came from. The durable fix is to raise `outcomes_cli --replicates` for the cells family; the bucket
  family needs a different approach (its difference bootstrap is two-sample and 8.5x larger).
- The two discoveries are at q = 0.0475, just inside q<0.05, and both sit at or next to the
  2,000,000-replicate floor, so their p-values are themselves resolution-limited from below — more
  replicates would lower their q, not raise it. The count of 2 is a floor on the count, not a ceiling;
  ranks 3 and beyond are at q >= 0.19 and are limited by their t-values, not by the bootstrap, so
  more replicates would not promote them.
- The stationary bootstrap degenerates at very small n: with 6-14 out-of-sample returns of one sign,
  every resample mean has that sign and the bootstrap p collapses to its floor. Several rank-3-to-10
  cells are in that regime. The `max(student_t, bootstrap)` rule is what keeps their reported p
  honest, and no such cell became a discovery.
- Index membership and sector labels attached to the cells are a **current** snapshot from
  `instrument_labels`, not point-in-time. They are grouping keys only and never entered a decision.
- Mixed adjustment basis remains, as the repair report warned: re-fetched windows sit on the vendor's
  current basis and windows that were not re-fetched keep the legacy basis, with 174,518 bars measured
  as a constant level shift and recorded at segment level in `corrections`. Returns are not re-based
  across that boundary here. The clean fix is a full-history re-fetch of the affected symbols.
- 368 fifteen-minute bars fall on dates the session calendar does not know and were dropped rather
  than placed in an invented session; 1,066 pre-2021 daily bars were relabelled onto their observed
  Muhurat session window (label only).

---

## 5. Reproducing this

```bash
PY=market_scanner/.venv/Scripts/python.exe   # run from the repo root

# 1. export the frozen histories from the clean snapshot
$PY -m market_data.research_export --snapshot snap_20260916T055228_f4bab0e9 \
      --source-run m15clean-20260916 --workers 8
$PY -m market_data.research_export --verify --source-run m15clean-20260916 \
      --verify-symbols RELIANCE TITAN INFY

# 2. research (point settings.active_run at the source run for `prepare` only, then restore it)
$PY -m market_scanner.pattern_research.validation          # the catalogue / causality gate
$PY -m market_scanner.pattern_research.runner --workers 8 --prepare-only
$PY -m market_scanner.pattern_research.runner --execute-manifest \
      market_scanner/output/expanded_research/4b33a5249562631524d6/manifest.json

# 3. outcomes
$PY -m market_scanner.pattern_research.outcomes_cli --run 4b33a5249562631524d6 \
      --snapshot-id snap_20260916T055228_f4bab0e9 --summary-only --workers 8

# 4. every number in sections 3 and 4 of this report, including the two-stage FDR
#    (read-only; ~15 min on 8 workers; writes nothing to outcomes.sqlite3)
$PY -m market_scanner.pattern_research.outcomes_summary --run 0b89eff7c36c01b28af0 \
      --refine --workers 8 \
      --write market_scanner/output/expanded_research/0b89eff7c36c01b28af0_summary.json
```

Artefacts: `market_scanner/output/history/m15clean-20260916{,_manifest.json,_calendar.json,_verification.json}`,
`market_scanner/output/expanded_research/4b33a5249562631524d6/` (12 GB),
`market_scanner/output/expanded_research/research.sqlite3` (run `4b33a5249562631524d6`),
`market_scanner/output/expanded_research/outcomes.sqlite3` (run `0b89eff7c36c01b28af0`),
`market_scanner/output/expanded_research/0b89eff7c36c01b28af0_summary.json`.

**Operational note on footprint.** `outcomes.sqlite3` is now **36 GB** for one universe-wide run:
`cell_outcomes` stores the full per-horizon cell dictionary in its `summary` column, so 1,001,880
cells average ~38 KB each. The run-level Benjamini-Hochberg post-pass rewrites every one of those
rows to clear and reassign q-values, which is why it takes minutes of pure I/O after the last symbol
commits. Anyone planning repeated universe-wide runs should budget the disk, and this is the obvious
place to trim if a future run needs to be cheaper.

The app's `settings.active_run` was switched to `m15clean-20260916` only for the duration of
`runner.prepare` and restored to `0e15f954dc432754` immediately afterwards; the research run's
identity carries the source run in its manifest, so nothing depends on that setting afterwards.
`db/kanida.db` and `db/market15.db` were never written.
