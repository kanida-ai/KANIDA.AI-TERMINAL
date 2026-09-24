# Intraday Pattern-Walk-Forward Proof — March 2026

**107 long-only TA patterns** tested on intraday bars from `kanida_universe.db` (143-stock NSE F&O universe).
Train: trailing 6 months ending 2026-02-01 (4-week embargo). Test: March 2026.
Per-stock 20d RS filter (top 33%) at signal day. ATR stop, RR=2:1, end-of-day forced exit. 30 bps RT cost.

Promotion rule: strategy passes (n ≥ 100, PF_net ≥ 1.10, WR after cost ≥ 38%) on **≥ 2 of 4 timeframes**.

## 1. Headline

| TF | Total trades | Strategies that PASS | Median PF net |
|---|---|---|---|
| **30min** | ~30k | **0** | 0.34 |
| **15min** | ~120k | **0** | 0.36 |
| **5min** | ~600k | **1** (PA momentum bull, PF=1.13) | 0.37 |
| **1min** | not run (interrupted; pattern walk-forward replaced by IAS analysis at 1m) | — | — |

**0 strategies promoted (need ≥ 2 of 4 TFs).** PA momentum bull was a single-TF island on 5m → classified as **rejected_overfit**.

## 2. Verdict

**Pattern walk-forward at intraday timeframes hit the same ceiling as daily.** PF_net distributions across 30m/15m/5m cluster around 0.34–0.37 — uniformly below the cost-adjusted breakeven of 1.0. Going to finer time resolution did NOT lift the floor.

Why: each trade pays the same 30 bps round-trip cost, but intraday targets are smaller in absolute terms. A 30 bps round-trip on a 0.5% target leaves much less room than on a 5% daily target. The cost burden is mathematically heavier at finer TFs.

The 1 outlier on 5m (PA momentum bull, PF=1.13) is a single-TF result on one test month. Under cross-TF consistency rules it's classified as overfit / not promoted.

## 3. So we're moving to a different signal class

The pattern-matching hypothesis ("textbook TA setups predict next-bar return at intraday TFs") is closed: **no.**

Next test (Part 2 of this report): **IAS — Institutional Accumulation Score** from the Kanida Intraday Lab. Different signal philosophy:
- Looks for *behavioral evidence of accumulation* (vol surge + price compression + VWAP cluster + absorption + late-day vol share)
- 9 weighted components → composite score 0–10
- Window fixed at 13:45–15:29 IST (last ~104 min of trading day)
- Analyzed at 4 different bar resolutions (1m / 5m / 15m / 30m)
- Statistical evidence via 5 causal tests: Welch t, Cochran-Armitage trend, Bonferroni multi-day, Granger F

If IAS shows causal evidence of next-day return where pattern-matching didn't, that's the path forward. If it doesn't, the data layer (price + volume only, retail cost) is genuinely exhausted at this universe and we pivot architecture (F&O OI signals, factor models, or stop trading the engine).

See **Part 2** below for the IAS multi-TF analysis.
