# Kanida.AI Quant Engine — Institutional Due-Diligence Memo

**Auditor:** Independent code/quant DD pass
**Date:** 2026-05-02
**Universe:** 148 NSE F&O equities, daily bars 2020-01-01 → 2026-04-28
**Trades audited:** 21,515 backtest entries (run_backtest.py output)
**Comparable:** NIFTY 50 buy-and-hold, same window
**Files audited:** `engine/backtest/run_backtest.py`, `engine/backtest/run_execution_analysis.py`, `engine/backtest/execution_engine.py`, `engine/jobs/run_learning.py`, `engine/outcome_first/learner.py`, `data/ingest/fetch_fno_kite.py`, `data/db/kanida_quant.db`

---

## 1. Verdict

**BROKEN — do not pitch in current form.**

The advertised "99.6% win rate, 1,893 trades, +5.19% avg P&L" headline is an **arithmetic tautology**, not a strategy result. The aggregate strategy, audited honestly, has a **30.2% win rate**, +0.10% gross daily mean, an annualised gross Sharpe of **1.81**, and a max drawdown of **−11.2%** before costs. After realistic NSE F&O round-trip costs (10–20 bps), Sharpe collapses to between **+0.02 and −1.77** and the strategy underperforms NIFTY buy-and-hold (which returned +10.3% with Sharpe 0.38).

There is also a structural data-snooping bias: patterns are mined on the same data they are tested on, with no train/test split. Within-sample, the "high_conviction" patterns claim 90% probability and realise 32%. This is not a capacity problem — it is a methodology problem.

The work shipped is real, the engineering is competent, the analytics layer is well-built. But the *evidence of edge* presented is not real edge. Two of the three "engine tiers" (Turbo and Super) are after-the-fact relabelings of trades that already won. There is no out-of-sample test in the repository.

---

## 2. Red flags found (severity, fix difficulty)

### 🔴 CRITICAL — kill-the-pitch issues

**RF-1. The 99.6% / Turbo+Super win rate is a tautology.** [`engine/backtest/run_backtest.py:59-71`]
`classify_bucket()` is *only* allowed to return `"turbo"` or `"super"` when `exit_reason == "tp"`. Every Turbo and Super trade is, by construction, a TP exit. DB confirms exactly that: Turbo n=1,894 (100.0% WR), Super n=2,190 (100.0% WR), Standard (TP exits + non-TP exits both allowed) = 17.7%, Trap = 0.0%. The headline number describes the *labelling rule*, not strategy edge. **Fix difficulty:** trivial — delete the bucket-WR claim from all marketing. Aggregate WR (the only number that matters) is 30.2%.

**RF-2. Patterns are mined and back-tested on the same data — no OOS split.** [`engine/jobs/run_learning.py:35-57`, `engine/outcome_first/learner.py:13-101`]
`load_ohlcv()` reads all bars 2020-01-01 → 2026-04-28. The learner labels each bar by whether `forward_window` future bars hit the `target_move`, mines patterns where `lift > floor` and `probability > target`, and writes them to `pattern_library`. The backtest then *replays the same patterns on the same data* (lines 152-159 of `run_backtest.py` filter to `BACKTEST_YEARS={"2024","2025","2026"}` but the patterns themselves were mined including those years). This is textbook data-snooping. **Realised vs claimed within-sample:** of 7,369 patterns with ≥5 trades, mean(realised − claimed probability) = −0.598. High-conviction tier claims 90% probability and realises 32% — a 58 percentage-point overfit. **Fix difficulty:** moderate. Train on 2020-2023, lock the pattern library, then backtest 2024-2026 only. The current "OOS" period 2026-Q1 has 2,906 trades at 32.9% WR.

**RF-3. After realistic costs the strategy underperforms NIFTY buy-and-hold.** Cost sensitivity (equal-weight, 1 trade per day per signal):

| Round-trip cost | Cumulative | Avg/trade | Sharpe (ann) | Max DD |
|---|---|---|---|---|
| 0 bps (paper) | +74.5% | +0.101% | +1.81 | −11.2% |
| 5 bps | +31.0% | +0.051% | +0.91 | −17.2% |
| **10 bps (cost floor)** | **−1.6%** | **+0.001%** | **+0.02** | **−25.4%** |
| **15 bps (realistic)** | **−26.1%** | **−0.049%** | **−0.88** | **−36.6%** |
| 20 bps (mid-cap reality) | −44.5% | −0.099% | −1.77 | −49.5% |
| NIFTY buy-and-hold | +10.3% | +0.021% | +0.38 | −15.8% |

NSE F&O round-trip economics: STT 6.25 bps on F&O sell + brokerage 3 bps + transaction 0.32 bps + GST + SEBI + stamp ~5-8 bps minimum, plus 1-10 bps slippage depending on liquidity. **A 10 bps round-trip is the floor; 15-20 bps is the realistic working assumption for this universe.** **Fix difficulty:** none — this is a data point, not a code bug.

**RF-4. Smart-entry P&L re-uses the blind-entry exit price.** [`engine/backtest/run_execution_analysis.py:212-227`]
`smart_pnl = pnl(result.entry_price, exit_price)` where `exit_price` was computed by `simulate_trade()` using TP/SL anchored to the **blind** entry. When the smart entry is more favourable (e.g. a long pullback entry below the open), this mechanically inflates P&L because the implicit reward keeps the same nominal exit while the entry is lower. The smart engine reports +0.62% avg / 45.0% WR vs blind +0.14% / 37.9%. The smart-vs-blind delta is partly a real selection effect, but the magnitude is exaggerated by this re-use. **Fix difficulty:** medium. Re-simulate TP/SL from the smart entry price intraday on the entry day, and re-walk forward bars from there.

### 🟠 HIGH — material methodology issues

**RF-5. Intrabar TP/SL ordering bias.** [`engine/backtest/run_backtest.py:84-100`]
On each daily bar, TP is checked before SL. When `low ≤ SL` AND `high ≥ TP` are both true on the exit bar, the simulator awards a TP win. **Empirically:** 80 trades had both TP and SL inside the exit bar's [low, high]; 79 of 80 (98.8%) were classified TP wins. Bias is small in magnitude (~0.4% of WR, ~80/21,507) because the daily ranges rarely span both TP and SL given a 2:1 RR ratio, but it is structurally wrong. **Fix difficulty:** trivial — assume worst-case (SL first) when both fall inside; or, better, fall back to intraday data on ambiguous bars.

**RF-6. Pattern multi-fire dedup picks the highest-`opportunity_score` candidate.** [`engine/backtest/run_backtest.py:198-201`]
`opportunity_score` is itself a function of historical hit rate. When multiple patterns fire on the same (stock, date, direction), the simulator picks the *historically best-performing* one. This is a hindsight selection layered on top of the in-sample mining. **Fix difficulty:** moderate. Either pick the first-detected pattern, the lowest pattern_id (deterministic), or take a weighted ensemble.

**RF-7. Universe is constructed today, then back-tested historically.** [`data/ingest/fetch_fno_kite.py:58-83`]
`FNO_TICKERS` is the *current* NSE F&O list. Bars are pulled 3 years back. Companies that were in F&O in 2024 but got delisted/dropped (e.g. ADANIPOWER de/inclusions, RBLBANK exits) are missing. Companies added to F&O in 2025-2026 are included from 2020. This is **survivorship bias in the universe construction**. Pattern mining on a survivor set inflates baseline returns. **Fix difficulty:** moderate. Pull the historical NSE F&O membership list (NSE publishes monthly inclusion/exclusion notices) and apply point-in-time membership at signal date.

**RF-8. Splits/bonuses are not adjusted; corp-action handling is reject-only.** [`data/ingest/fetch_fno_kite.py:135-149`, line 177]
`kite.historical_data(...)` is called without `continuous=True` or any adjustment flag — Kite returns **unadjusted** OHLC. `validate_row` rejects bars where `|close − prev_close|/prev_close > 25%`, but **59 daily moves greater than 20% remain in the dataset with `quality_flag='ok'`** (e.g. ABFRL 2025-05-22: −66.59%, INDUSINDBK 2025-03-11: −27.16%, IEX 2025-07-24: −29.58%, OFSS 2024-01-18: +30.11%, RECLTD 2024-06-04: −25.19%). ABFRL at −66.59% should have been rejected by the 25% rule and was not, meaning either the rule was added after ingest or it isn't being enforced on existing rows. The user's reported "VEDL −64.76% on 2026-04-30" is not in the DB (latest VEDL bar is 2026-04-27 close 742.50). The 59 unadjusted price discontinuities will:
  - trigger spurious SLs on long trades and spurious TPs on short trades around the action date,
  - corrupt feature atoms (volatility, MA position, breakout state) for ~30 trading days each side,
  - distort baseline probabilities at the stock level.
  **Fix difficulty:** moderate. Either (a) switch to a corporate-action-adjusted source (NSE's own bhavcopy with adjustment factors, or a vendor like Refinitiv/Bloomberg), or (b) implement a forward-adjustment pipeline that scales pre-action OHLCV by the action ratio.

### 🟡 MEDIUM — would not flunk DD on its own, but compounding

**RF-9. `trusted_probability` is poorly calibrated.** [`engine/outcome_first/learner.py:137`, `engine/outcome_first/trust.py`]
The 90%-claimed / 32%-realised gap on high_conviction patterns means whatever Bayesian/Wilson shrinkage is being applied is not aggressive enough relative to the multiple-testing burden of mining `combinations(atoms, ≤max_size)` patterns. With 100s of atoms per stock and combinatorial mining, multiple-comparisons inflation will produce many false-discovery patterns at any reasonable sample size. **Fix difficulty:** moderate. Apply Benjamini-Hochberg FDR or simply require `probability_ci_low > baseline × 1.3` instead of point estimates.

**RF-10. Cooldown is per-direction, not per-symbol.** [`engine/backtest/run_backtest.py:254-269`]
A long signal does not block a short signal in the same stock the same week. In live trading this means competing positions in the same name. Risk-management concern, not a backtest bias. **Fix difficulty:** trivial.

**RF-11. The "NIFTY weak" execution-engine veto is post-hoc tuned.** [`engine/backtest/execution_engine.py:21-24, 48-52`]
The doc-comments cite "data shows 45% win rate on volatile open days" and "50% win rate in BIG_GAP_UP that continued" as justification for converting NO_TRADE_VOLATILE into delayed entry. These are in-sample observations being used to build the in-sample backtest engine. Same data-snooping pathology as RF-2. **Fix difficulty:** moderate. Lock these thresholds on the train slice; do not retune them after seeing test results.

### 🟢 LOW — fine, document and move on

**RF-12. P&L is reported as arithmetic-sum cumulative ("9828.66%").** [`engine/backtest/run_backtest.py:380-410`]
Sum of per-trade percents is not a real return number. Compounded equity is +74.5% gross, −1.6% to −44.5% after realistic costs. **Fix difficulty:** trivial — replace `+sum()` with `prod(1+r)−1`.

**RF-13. Weekend/overnight gap logic is naïve.** [`engine/backtest/run_backtest.py:38-43`]
Calendar-minutes between signal close and entry open is reported in `notes`, but it is not used anywhere in entry/exit logic. Cosmetic.

---

## 3. Statistical evidence (real numbers)

### 3.1 Aggregate (in-sample, all 21,515 backtest trades)

|  | Value |
|---|---|
| Trades | 21,515 |
| TP exits | 6,490 (30.2%) |
| SL exits | 12,636 (58.7%) |
| Timeout | 2,381 (11.1%) |
| Mean P&L per trade | +0.147% |
| Median P&L per trade | (negative — most lose) |
| RR ratio | 2.0 (target) / 1.0 (stop) |
| Break-even WR @ 2:1 RR | 33.3% |
| Actual WR | 30.2% |

**The strategy loses money at its own claimed risk/reward before any costs.** The +0.147% headline survives only because TP wins are exactly 2× SL losses by construction (TP fills at +target_move, SL at −target_move/2), so 30.2% × +X − 58.7% × X/2 − 11.1% × small_drift = positive but nearly zero.

### 3.2 By year (in-sample throughout — patterns mined on full window)

| Year | n | WR | Avg P&L | Per-trade Sharpe |
|---|---|---|---|---|
| 2024 | 9,384 | 31.7% | +0.188% | 0.044 |
| 2025 | 9,225 | 27.7% | +0.039% | 0.010 |
| 2026 (partial) | 2,906 | 32.9% | +0.330% | 0.076 |

WR is stable across years at ~28-33%. There is **no train/test discrimination** because the learner had access to all years. A real OOS test (train 2020-2023, test 2024-2026) is not in the repo and was not performed.

### 3.3 Daily equity (gross, no costs)

|  | Strategy | NIFTY 50 |
|---|---|---|
| Trading days | 573 | 569 |
| Mean daily | +0.101% | +0.021% |
| Daily std | 0.887% | ~0.85% (typical) |
| Annualised Sharpe | **1.81** | 0.38 |
| Annualised Sortino | 3.02 | — |
| Max drawdown | −11.2% | −15.8% |
| Cumulative return | +74.5% | +10.3% |

Gross strategy looks excellent. **After 15 bps round-trip costs, strategy = −26.1% cum, Sharpe −0.88, MDD −36.6%.** NIFTY buy-and-hold dominates.

### 3.4 Pattern claimed vs realised probability (in-sample!)

| Pattern tier | Patterns matched (≥5 trades) | Claimed avg | Realised avg | Gap |
|---|---|---|---|---|
| All tiers | 7,369 | ~0.6 | ~0 (sum-diff) | −59.8 pp |
| **high_conviction** | **782** | **0.90** | **0.32** | **−58 pp** |

Even within the same dataset the patterns were mined on, they realise <40% of their claimed probability. This is overfitting on a dramatic scale.

---

## 4. Comparison vs NIFTY benchmark

For the period **2024-01-02 → 2026-04-24** (matched window):

|  | Strategy gross | Strategy @ 15 bps | NIFTY 50 |
|---|---|---|---|
| Cumulative | +74.5% | −26.1% | +10.3% |
| CAGR (≈2.4 yr) | ~26% | −12% | ~4% |
| Sharpe | 1.81 | −0.88 | 0.38 |
| Max DD | −11.2% | −36.6% | −15.8% |
| Hit ratio | 30.2% | 30.2% | n/a |

**Headline:** the gross paper number is good. The investable number is worse than holding NIFTY ETF. There is no version of this strategy at scale, with realistic execution friction, that beats NIFTY in this dataset.

---

## 5. Capacity estimate

Top-15 traded names ADV (60-day, INR Cr):

| Ticker | Trades | ADV (Cr) | 1% ADV cap |
|---|---|---|---|
| RELIANCE | 173 | 2,372 | 23.7 Cr |
| AXISBANK | 178 | 901 | 9.0 Cr |
| SUNPHARMA | 164 | 598 | 6.0 Cr |
| TRENT | 176 | 476 | 4.8 Cr |
| COFORGE | 167 | 452 | 4.5 Cr |
| SAIL | 168 | 389 | 3.9 Cr |
| CIPLA | 166 | 227 | 2.3 Cr |
| IRFC | 172 | 182 | 1.8 Cr |
| BANDHANBNK | 166 | 178 | 1.8 Cr |
| MCDOWELL-N | 164 | 145 | 1.4 Cr |
| IRCTC | 176 | 136 | 1.4 Cr |
| PAGEIND | 164 | 93 | 0.9 Cr |
| CROMPTON | 166 | 78 | 0.8 Cr |
| GRANULES | 178 | 58 | 0.6 Cr |
| ABFRL | 166 | 36 | 0.4 Cr |

At a strict 1%-of-ADV per-trade cap (the academic cap before slippage shows up as ≥10 bps on top of explicit costs), the strategy can deploy roughly **₹50-80 Cr (~$6-10M) total per signal cluster** across the universe. Push to 5% of ADV and slippage rises super-linearly — the very 10-15 bps that already kills the edge.

**Honest capacity ceiling: $10-20M of NAV before edge dies entirely.** That is below the minimum allocation any institutional LP, IB, or fund of funds would consider — they typically need $50M+ deployable to make a manager interesting.

---

## 6. Recommended fixes — ranked by importance

| # | Fix | Severity | Effort | Impact |
|---|---|---|---|---|
| 1 | Strip the "99.6% WR" claim. Replace headline with aggregate 30.2% WR. | 🔴 | 0.5 day | Restores honesty |
| 2 | Implement strict train/test split (train 2020-2023, freeze patterns, test 2024-2026 only). Rerun all metrics. | 🔴 | 2-3 days | Probably reveals OOS Sharpe ≈ 0 |
| 3 | Add cost model (≥10 bps round-trip per trade, configurable). Show net Sharpe & DD as the *primary* metrics; gross only as a footnote. | 🔴 | 0.5 day | Realistic numbers |
| 4 | Re-pull OHLCV from a corporate-action-adjusted source, or implement forward adjustment. | 🟠 | 2-4 days | Removes 59 fake price discontinuities |
| 5 | Re-simulate TP/SL from the smart entry price (don't reuse blind exit). | 🟠 | 1 day | Honest smart-entry attribution |
| 6 | Apply pessimistic-fill rule on intrabar TP/SL ambiguity. | 🟠 | 0.5 day | Removes ~80 fake wins |
| 7 | Reconstruct point-in-time NSE F&O membership history; apply at signal date. | 🟠 | 1-2 days | Removes survivorship bias |
| 8 | Tighten pattern-quality gate: require `probability_ci_low > 1.3 × baseline`, drop tier="high_conviction" thresholds, apply BH-FDR across patterns. | 🟠 | 1 day | Cuts library from ~thousands to ~dozens |
| 9 | Replace dedup by `opportunity_score` with deterministic tie-breaking. | 🟡 | 0.5 day | Removes hindsight selection |
| 10 | Switch cumulative reporting to compounded equity, not arithmetic sum. | 🟢 | 1 hr | Cosmetic but mandatory |

---

## 7. What to fix BEFORE pitching to a real LP / IB

A Tier-1 quant DD desk (GS QIS / MS QSI / Bloomberg quant / any allocator) will ask these in the first 30 minutes. **Today, the answers fail.**

1. **"Show me your train/test split. What's the OOS Sharpe?"**
   → No split exists. Required before any meeting. Target: OOS Sharpe ≥ 1.0 net of 15 bps.
2. **"What's your cost model and at what AUM does the edge die?"**
   → Currently zero cost assumed. Capacity ceiling honestly is $10-20M. Required: explicit cost curve, walk-away-AUM stated.
3. **"How is the universe constructed point-in-time?"**
   → Survivor-biased. Required: NSE F&O monthly membership reconstruction.
4. **"Walk me through one trade end-to-end. Why is the win rate 99.6%?"**
   → It isn't. The 99.6% is a label, not a result. Aggregate is 30.2%. Required: drop the 99.6% number entirely from every dashboard, deck, and one-pager.
5. **"How do you handle splits, bonuses, demergers?"**
   → 59 unadjusted >20% moves in the data. Required: corporate-action-adjusted price source.
6. **"What is your live track record?"**
   → None in this audit. Required: 6+ months of paper-traded live performance with actual fills, before any institutional pitch.

### What is good about the work

- The data pipeline (Kite ingest, SQLite schema, behavior-atom feature extraction) is clean engineering.
- The execution-decision engine (`execution_engine.py`) is thoughtfully constructed with sensible heuristics — it is the most defensible piece of the codebase.
- The pattern-atom decomposition is conceptually sound; the *mining* on top of it is what's broken, not the features.
- The notes-JSON metadata on every trade enables exactly this kind of forensic audit. That is unusual and good.

### Bottom line

The repo is a strong analytics platform sitting on top of an unproven strategy. As an institutional-grade quant pitch, it is not ready. The fixes above are 2-3 weeks of focused work, after which a re-pitch with honest OOS numbers is plausible — but expect aggregate Sharpe net of costs to come in somewhere between **0 and 0.6**, which is below institutional bars (≥1.0 net is the soft floor) but is at least credible.

The headline claim ("99.6% WR, 1,893 trades") **must be retired immediately**. Any sophisticated allocator will identify the bucketing tautology in 5 minutes and the meeting ends there.

— end of memo —
