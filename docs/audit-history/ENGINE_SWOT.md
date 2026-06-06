# Kanida Engines — Practitioner SWOT
**For:** the trader sitting in front of the screen, taking signals discretionarily
**Date:** 2026-05-02
**Audited:** Turbo / Super / Standard buckets in `trade_log` (21,515 backtest trades) + walk-forward replay on 2026 with patterns mined on 2020-2025 only.

---

## TL;DR — answers to your two questions

### "Is patterns-mined-on-the-same-data-as-tested actually a problem?"

**Yes, it is the right concern. But the empirical answer is more nuanced than I expected when I wrote the DD memo. Walk-forward results are NOT a disaster — they're roughly the same as in-sample on aggregate.**

| 2026 (76 trading days) | In-sample (patterns mined incl. 2026) | Out-of-sample (patterns mined on 2020-2025 only) |
|---|---|---|
| Trades emitted | 2,898 | **3,058** |
| Aggregate WR | 33.0% | **31.3%** |
| Avg P&L / trade | +0.330% | **+0.145%** |
| Sharpe (gross, ann) | +4.30 | **+4.86** |
| Max drawdown | −4.4% | −3.9% |
| Cum return (gross) | +18.8% | +23.6% |
| Cum return @ 15 bps cost | +6.0% | +10.3% |
| Cum return @ 20 bps cost | +2.1% | +6.2% |
| Turbo (post-hoc bucket) | n=312, 100% WR | n=332, 100% WR |
| Super | n=339, 100% WR | n=325, 100% WR |
| Standard | n=1,803, 17.0% WR | n=1,921, 15.6% WR |
| Trap | n=444, 0% WR | n=480, 0% WR |

The IS and OOS pattern *populations* differ (only 1,216 of ~3,000 signals overlap), but the *aggregate behaviour* is similar. The bucket distribution is virtually identical (~21% become Turbo+Super in both worlds). Aggregate WR drops 1.7 pp; avg P&L drops 18 bps.

**Conclusion: the engine is not catastrophically overfit — but the headline win-rate claim is still a tautology, and the per-trade edge is paper-thin.** Out-of-sample, you net +6 to +10% over a quarter at 15-20 bps round-trip, which compares to NIFTY's +10% over the same window with zero work.

### "I want to trade in front of the engine using Turbo/Super signals — is that viable?"

**Not as a primary edge. Three reasons:**

1. **Turbo and Super buckets are post-hoc labels** assigned by `classify_bucket()` ([run_backtest.py:59-71](engine/backtest/run_backtest.py:59)) using `exit_reason`, `mfe`, `mae`, and post-exit 5-bar stats. **You cannot know a signal is Turbo/Super at entry time.** The label is computed only after the trade closes. So "trade only Turbo signals" is unimplementable — there is no live-side equivalent.

2. **The closest live equivalent — the `tier` field — is broken.** Patterns tagged `high_conviction` (claimed 90% probability) realise 32% in-sample. `medium` (claimed 93%) realises 32%. `exploratory` (claimed 77%) realises 35% — *better* than high_conviction. The tier is reverse-correlated with edge.

3. **The base rate is the only honest forecast.** ~21% of all signals become Turbo+Super, regardless of which signal you pick. That number is stable across IS and OOS. So at any given trade, your prior is 21% chance of a clean fast win, ~10% chance of a mistimed trap, ~17% chance of a slow grind win, ~52% chance of a slow loss.

**What does actually predict a winner at signal time:** see Strengths #1 below. There is one filter — pattern `overlap` — that doubles your win rate from 29% to 61%. Use it.

---

## Strengths

### S1. Pattern-overlap filter is the one real, usable edge
[run_backtest.py:152-159](engine/backtest/run_backtest.py:152) computes `overlap = |pat_atoms ∩ live_atoms| / |pat_atoms|` for every signal. Cross-tab shows:

| Overlap range | Trades | WR |
|---|---|---|
| 0.65 - 0.70 (current min) | 16,137 | **29.1%** |
| 0.70 - 0.80 | 4,383 | 27.0% |
| 0.90 - 1.00 (near-exact match) | **995** | **61.2%** |

A 0.90+ overlap signal has a 61% raw WR with 2:1 RR — that's a real, tradeable edge (~+0.8% expected P&L per trade gross). 0.90+ matches are rare (~5% of all signals, or roughly 1-2 per day across the universe). **This is the actual signal you should be trading in front of.**

### S2. Direction symmetry — shorts work as well as longs
Long n=10,779 WR 28.6%; short n=10,736 WR 31.8%. The engine is not biased toward a directional regime, which is unusual for a 2024-2026 NSE backtest (markets generally rose). Shorts are slightly *better*, suggesting a real mean-reversion or breakdown component.

### S3. Walk-forward stability is genuinely OK
The OOS test (patterns mined on 2020-2025, tested on 2026) gives 31.3% WR vs in-sample 33.0% — only a ~2 pp degradation. For a pattern-mining system this is unusually stable. The engine *is* finding something real, even if the marketing framing is wrong.

### S4. Hold time is short and predictable
Turbo trades exit in 2.1 days (max 6); Super in 4.5 days (max 11); Standard in 6.1 days. You're not committed to long open exposure. That suits a trader-in-front-of-the-screen because risk is bounded.

### S5. Engineering quality
- Behavior-atom feature decomposition is clean.
- Trade metadata in `notes` JSON enables exactly this kind of forensic audit.
- Execution decision engine ([execution_engine.py](engine/backtest/execution_engine.py)) is thoughtfully constructed with separate gap/day-move/NIFTY-context heuristics.
- The codebase is reproducible.

---

## Weaknesses

### W1. The 99.6% Turbo/Super win rate is definitionally guaranteed
[run_backtest.py:60-67](engine/backtest/run_backtest.py:60) — `classify_bucket()` only returns `"turbo"` or `"super"` when `exit_reason == "tp"`. Every Turbo and Super trade is a TP win by construction. The 100% WR is a property of the labelling rule, not an edge. **Stop quoting this number to anyone, including yourself.**

### W2. Turbo/Super are post-hoc labels — not actionable at entry time
Bucket assignment requires `mfe`, `mae`, days_to_exit, and 5 bars *after* exit. None of these exist when you click Buy. There is no live-side filter that produces Turbo signals — it is mechanically impossible.

### W3. Live-side `tier` filter is miscalibrated by 50-60 percentage points

| Tier (live-visible) | Patterns | Claimed prob | Realised in-sample WR | Gap |
|---|---|---|---|---|
| high_conviction | 782 | 0.90 | **0.32** | −58 pp |
| medium | 6,406 | 0.93 | **0.32** | −61 pp |
| exploratory | 181 | 0.77 | **0.35** | −42 pp |

`high_conviction` actually has the *lowest* Turbo+Super conversion rate in the cross-tab (15%) compared to `exploratory` (23%) and `medium` (19%). **Treat the tier badge as decorative. Do not let a "high_conviction" tag inflate your position size.**

### W4. The aggregate edge is paper-thin and dies at realistic costs
Gross strategy on full universe blind: avg +0.10% per trade per day. NSE F&O round-trip is 10-20 bps minimum (STT 6.25 bps + brokerage + GST + slippage). At 15 bps, OOS Sharpe stays positive (+4.86 → ~3.5 net) but cumulative drops from +23.6% to +10.3% over a quarter. At 20 bps, +6.2%. Net of costs the engine barely beats NIFTY buy-and-hold over the same window.

### W5. Intrabar TP/SL ordering is biased toward winners
[run_backtest.py:84-100](engine/backtest/run_backtest.py:84) — TP is checked before SL on each daily bar. When both TP and SL fall inside [low, high] of the same daily bar, simulator awards a TP win. Empirically: 80 such bars across 21,507 trades, 79 awarded as wins. Small bias (~0.4% of WR) but structurally wrong.

### W6. Smart-entry P&L is inflated by reusing blind exit price
[run_execution_analysis.py:212-227](engine/backtest/run_execution_analysis.py:212) — `smart_pnl = pnl(smart_entry, exit_price)` where `exit_price` was derived from blind-entry TP/SL. When smart entry is more favourable, you mechanically inflate the smart-vs-blind delta. The "+0.62% smart vs +0.14% blind" difference partly reflects real selection but mostly reflects this accounting bug. Don't trust the Execution IQ improvement numbers.

### W7. Patterns are mined and tested on overlapping data
The OOS test softens but does not eliminate the concern. Live signals are matched to patterns whose probabilities were computed on bars *up to today*. Patterns that just achieved a "win" inside the past 5 days inflate their own probability for the next signal. Bias is small empirically (~2 pp WR) but real.

### W8. Universe is current F&O list, back-applied
[fetch_fno_kite.py:58-83](data/ingest/fetch_fno_kite.py:58) — `FNO_TICKERS` is today's NSE F&O membership pulled back 3 years. Stocks dropped from F&O (e.g. names that exited in 2024-2025) are missing; stocks added in 2025-2026 are present from 2020. Survivorship bias inflates baseline. Probably worth ~1-3 pp of the WR.

### W9. Splits and corporate actions are not adjusted
[fetch_fno_kite.py:177](data/ingest/fetch_fno_kite.py:177) — `kite.historical_data(...)` returns unadjusted prices. Bars with >25% moves get rejected, but **59 daily moves >20% remain in the dataset with `quality_flag='ok'`** (ABFRL −66.6% on 2025-05-22, INDUSINDBK −27.2%, IEX −29.6%, etc.). These are demergers/splits that:
  - corrupt features (volatility, MA, breakout-state) for ~30 days each side,
  - trigger spurious SL on long trades and spurious TP on short trades around action dates,
  - contaminate the pattern_library at the affected tickers.

**Avoid trading any name within 30 trading days of a known corporate action.**

### W10. Multi-pattern dedup picks the highest historical-edge pattern
[run_backtest.py:198-201](engine/backtest/run_backtest.py:198) — when multiple patterns fire on the same (stock, date, direction), keep the one with highest `opportunity_score`. Since `opportunity_score` is itself derived from in-sample win rate, this layers hindsight selection on top of pattern mining. Lift on dedup is small but adds noise.

### W11. Cumulative returns reported as arithmetic sum
[run_backtest.py:380-410](engine/backtest/run_backtest.py:380) — the "+9828.66%" headline is a sum of percent points across thousands of trades, not a compound return. Real compound on equal-weight 1-trade-per-day is +74.5% gross over 2.4 years. Cosmetic but misleading.

---

## Opportunities (low-effort fixes that would meaningfully improve your live trading)

### O1. **Add a live-visible `overlap` field and filter to >= 0.90.**
This is the single most valuable improvement. The data shows 61% WR at overlap≥0.90 vs 29% at the current 0.65 floor. ~5% of signals qualify, so you'd take 1-2 trades per day on the universe. With 2:1 RR and 61% WR, expected gross P&L per trade is ~+0.8%, comfortably above any realistic cost.

### O2. **Replace the live `tier` ranking with a "predicted Turbo/Super probability" model.**
Even though Turbo/Super itself is post-hoc, you can build a classifier at signal time: "given this pattern's feature vector and recent regime, what fraction of past instances of this exact pattern became Turbo/Super?" Use that as the live ranking. The current `tier` field is uncorrelated to outcome — anything would beat it.

### O3. **Show the user the pattern's bucket distribution next to each live signal.**
Display: "this exact pattern fired 47 times historically: 6 Turbo, 5 Super, 28 Standard, 8 Trap." That is the honest forecast. The user can then decide whether 11/47 = 23% Turbo+Super rate is worth taking.

### O4. **Add a 30-day corporate-action exclusion.**
NSE publishes a corporate-actions feed. Exclude any signal where the underlying had a split/bonus/demerger in the past 30 trading days. Removes a known source of spurious wins/losses.

### O5. **Add a cost-aware live signal score.**
Reject signals where `target_move < 3 × estimated_round_trip_cost`. With 15 bps round-trip and a 2:1 RR, smallest viable target is ~1%. Anything below that is gambling on noise.

### O6. **Forward-adjust the OHLCV.**
Replace the current `validate_row` reject-on-large-move logic with a proper forward-adjustment routine. Pull NSE's adjustment factors and scale pre-action OHLCV. This single change would cleanse 59 known data discontinuities.

### O7. **Walk-forward refresh of pattern_library every quarter.**
Run the learner on a rolling 12-24 month window, dropping signals from before. Avoids cumulative drift in pattern probability estimates.

---

## Threats

### T1. The visible bucket math gives you false confidence
You will look at the dashboard, see "Turbo: 100% WR, +5.7% avg" and feel like a god. Then you'll size up. Then you'll take a Standard or Trap trade thinking it's a Turbo. Then you'll lose 2.4% on a 5% target. **The dashboard label is not your edge.** Don't let it set your sizing.

### T2. Realistic execution costs eat the edge
At your size and broker, the round-trip is realistically 12-18 bps for liquid F&O underlyings, more for mid-caps. The OOS strategy net of 15 bps still beats zero, but a single bad slippage day or a wrong fill window can wipe a week. **Track your actual fills against the engine's assumed entry/exit prices.**

### T3. Liquidity at scale
Top-of-list names have ample ADV (RELIANCE 2,372 Cr, AXISBANK 901 Cr) but the strategy fires equally on smaller names: ABFRL (36 Cr ADV), GRANULES (58 Cr), CROMPTON (78 Cr). At any meaningful size you'll move these stocks against yourself. **Cap any single trade at 0.5% of the underlying's 20-day ADV.**

### T4. Regime change
2024 + 2025 + early 2026 is a specific NSE regime — moderate volatility, IT correction, banking strength. Mean-reversion edge in Standard signals (-0.55% avg) suggests the engine is already underperforming in a steady-trend environment. A genuine 2008/2020-style regime shift will break the pattern library faster than it can re-learn.

### T5. Splits and demergers slipping through
With 59 unadjusted >20% moves in the data and corporate-action handling reject-only (never adjusted), every quarter brings new contamination. **Check the price history for any name 30 days before you take its signal.** Anything that looks like a one-day cliff-edge is a corporate action — skip the signal.

### T6. The "AI" framing creates a credibility tail risk
The pattern miner is sound combinatorial mining + Bayesian shrinkage on probability — not "AI" in any modern sense. If you market this as ML/AI in front of an informed audience (broker, prop desk, sophisticated client), they will read the code and discount everything else you say. **Talk about it as a systematic technical-analysis screen with a probability calibration. That's what it is, and that's defensible.**

### T7. The 99.6% number is going to bite you
Anywhere you've put it (deck, website, trader chats) is a future "gotcha" moment. It's mechanically tautological — a 1-minute audit reveals it. Pull it down before someone smart sees it.

---

## Bottom line for live trading

**The engine is a usable idea-generation screen, not a high-conviction signal machine.** Treat it like a smart watchlist that ranks ~3,000 candidate setups per year. Apply your own filter on top — minimum: `overlap ≥ 0.90`, no recent corporate action, target_move ≥ 1%. You'll go from ~30 trades/day with 30% WR to ~1-2 trades/day with 60% WR.

For position sizing, anchor on the **base rate (21% Turbo+Super, 30% all-TP, 50%+ losers)** and the **2:1 RR reality** — not on the 99.6% headline. Expected value per trade after 15 bps: roughly **+5 to +15 bps gross**, sub-1 R per trade. That sets your max risk per name at conservative levels.

Two engineering fixes that pay back fastest if you get them done this month:
1. Live-display `overlap` and filter the UI to ≥ 0.90.
2. Show each live signal's pattern bucket distribution (`turbo / super / standard / trap`) instead of the broken `tier` field.

Everything else (corp-action adjustment, smart-entry exit re-simulation, walk-forward refresh) is good housekeeping but doesn't change the day-to-day decision quality nearly as much as those two.
