# Intraday Full Report — Pattern walk-forward + IAS causal analysis

## Part 1 — Pattern walk-forward (107 strategies × 30m/15m/5m)

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



---


## Part 2 — IAS causal analysis (Kanida Lab method, multi-TF)

# IAS Multi-Timeframe Causal Analysis

**Adapted from Kanida Intraday Lab** — IAS (Institutional Accumulation Score) computed at the SAME 13:45-15:29 IST window using 4 different bar resolutions.

- Window: **13:45 → 15:29 IST** (fixed across all TFs)
- IAS components scaled relative to bars-in-window per TF
- Signal threshold: IAS ≥ 5.5
- 'Up day' definition: next-day close-to-close ≥ 1.0%
- 5 statistical tests run per TF: Welch t-test base rates, dose-response trend, multi-day forward returns (Bonferroni), Granger causality

## Headline — IAS strength across timeframes

| TF | Panel rows | Signal days | No-sig days | Hit rate (signal) | Hit rate (no-sig) | Lift | t-stat | p-value | Cohen's d |
|---|---|---|---|---|---|---|---|---|---|
| 1min | 17,732 | 13120 | 4469 | 23.1% | 29.1% | 0.79 | -3.02 | 0.0025 | -0.06 |
| 5min | 17,732 | 13642 | 3947 | 23.4% | 29.0% | 0.80 | -3.29 | 9.95e-04 | -0.06 |
| 15min | 17,732 | 14650 | 2939 | 23.7% | 29.4% | 0.81 | -2.32 | 0.0205 | -0.05 |
| 30min | 17,732 | 15672 | 1917 | 24.0% | 29.6% | 0.81 | -1.47 | 0.1422 | -0.04 |

## Test 2 — IAS Dose-Response (5 quintiles)

Monotone increase in hit-rate across IAS quintiles = dose-response = causal evidence.
Cochran-Armitage trend test p-value reported per TF.

**1min** (trend p = 0.00e+00):

| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |
|---|---|---|---|---|
| Q1 | 1.71–5.25 | 3517 | 29.6% | +0.096% |
| Q2 | 5.25–6.05 | 3517 | 26.2% | +0.002% |
| Q3 | 6.05–6.77 | 3517 | 23.7% | -0.021% |
| Q4 | 6.77–7.57 | 3517 | 23.4% | -0.029% |
| Q5 | 7.57–9.2 | 3521 | 20.3% | -0.113% |

**5min** (trend p = 0.00e+00):

| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |
|---|---|---|---|---|
| Q1 | 1.7–5.39 | 3517 | 29.4% | +0.082% |
| Q2 | 5.39–6.19 | 3517 | 26.2% | +0.011% |
| Q3 | 6.19–6.9 | 3517 | 24.0% | -0.019% |
| Q4 | 6.9–7.68 | 3517 | 23.4% | -0.034% |
| Q5 | 7.68–9.39 | 3521 | 20.2% | -0.106% |

**15min** (trend p = 0.00e+00):

| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |
|---|---|---|---|---|
| Q1 | 1.77–5.67 | 3517 | 29.3% | +0.072% |
| Q2 | 5.67–6.47 | 3517 | 26.5% | +0.003% |
| Q3 | 6.47–7.17 | 3517 | 24.0% | -0.010% |
| Q4 | 7.17–7.92 | 3517 | 22.8% | -0.056% |
| Q5 | 7.92–9.52 | 3521 | 20.6% | -0.073% |

**30min** (trend p = 0.00e+00):

| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |
|---|---|---|---|---|
| Q1 | 2.02–6.0 | 3517 | 29.7% | +0.088% |
| Q2 | 6.0–6.76 | 3517 | 26.6% | +0.009% |
| Q3 | 6.76–7.4 | 3517 | 23.9% | -0.039% |
| Q4 | 7.4–8.07 | 3517 | 22.4% | -0.062% |
| Q5 | 8.07–9.69 | 3521 | 20.5% | -0.061% |

## Test 3 — Multi-day forward returns (D+1 to D+5, Bonferroni-corrected)

| TF | Day | n_sig | n_nosig | Mean signal | Mean control | t | p (raw) | p (Bonf) | Cohen's d |
|---|---|---|---|---|---|---|---|---|---|
| 1min | D+1 | 13120 | 4469 | -0.043% | +0.073% | -3.02 | 0.0025 | 0.0126 | -0.06 |
| 1min | D+2 | 13053 | 4393 | -0.095% | +0.217% | -5.88 | 0.00e+00 | 0.00e+00 | -0.12 |
| 1min | D+3 | 12951 | 4352 | -0.082% | +0.175% | -4.02 | 5.80e-05 | 2.90e-04 | -0.08 |
| 1min | D+4 | 12851 | 4309 | -0.096% | +0.185% | -3.93 | 8.50e-05 | 4.25e-04 | -0.08 |
| 1min | D+5 | 12781 | 4236 | -0.114% | +0.196% | -3.85 | 1.19e-04 | 5.95e-04 | -0.08 |
| 5min | D+1 | 13642 | 3947 | -0.041% | +0.084% | -3.29 | 9.95e-04 | 0.0050 | -0.06 |
| 5min | D+2 | 13567 | 3879 | -0.083% | +0.217% | -5.27 | 0.00e+00 | 0.00e+00 | -0.11 |
| 5min | D+3 | 13461 | 3842 | -0.075% | +0.185% | -3.83 | 1.27e-04 | 6.35e-04 | -0.08 |
| 5min | D+4 | 13356 | 3804 | -0.091% | +0.204% | -3.88 | 1.07e-04 | 5.35e-04 | -0.08 |
| 5min | D+5 | 13275 | 3742 | -0.107% | +0.213% | -3.76 | 1.70e-04 | 8.50e-04 | -0.08 |
| 15min | D+1 | 14650 | 2939 | -0.030% | +0.071% | -2.32 | 0.0205 | 0.1024 | -0.05 |
| 15min | D+2 | 14563 | 2883 | -0.059% | +0.200% | -3.88 | 1.05e-04 | 5.25e-04 | -0.10 |
| 15min | D+3 | 14441 | 2862 | -0.062% | +0.207% | -3.57 | 3.52e-04 | 0.0018 | -0.08 |
| 15min | D+4 | 14327 | 2833 | -0.073% | +0.214% | -3.28 | 0.0010 | 0.0052 | -0.08 |
| 15min | D+5 | 14226 | 2791 | -0.078% | +0.175% | -2.61 | 0.0091 | 0.0454 | -0.06 |
| 30min | D+1 | 15672 | 1917 | -0.022% | +0.056% | -1.47 | 0.1422 | 0.7111 | -0.04 |
| 30min | D+2 | 15569 | 1877 | -0.050% | +0.263% | -3.63 | 2.88e-04 | 0.0014 | -0.12 |
| 30min | D+3 | 15441 | 1862 | -0.051% | +0.262% | -3.28 | 0.0010 | 0.0052 | -0.10 |
| 30min | D+4 | 15317 | 1843 | -0.063% | +0.286% | -3.28 | 0.0010 | 0.0051 | -0.10 |
| 30min | D+5 | 15193 | 1824 | -0.076% | +0.291% | -3.08 | 0.0021 | 0.0105 | -0.09 |

## Test 4 — Granger causality (does IAS predict beyond lagged returns?)

OLS F-test: full model `fwd_d1 ~ const + lag1 + lag2 + IAS` vs restricted `fwd_d1 ~ const + lag1 + lag2`.
p < 0.05 means IAS adds significant predictive power beyond pure return autocorrelation.

| TF | n | F | df1 | df2 | p | IAS coefficient |
|---|---|---|---|---|---|---|
| 1min | 17303 | 15.7338 | 1 | 17299 | 7.30e-05 | -0.04547 |
| 5min | 17303 | 13.4828 | 1 | 17299 | 2.41e-04 | -0.04231 |
| 15min | 17303 | 7.8626 | 1 | 17299 | 0.0051 | -0.03292 |
| 30min | 17303 | 5.0994 | 1 | 17299 | 0.0239 | -0.02835 |

## Verdict — which TF gives the strongest causal evidence?

| TF | Tests passed (of 4) | Lift (Test 1) | Bonf-sig fwd days | Granger p |
|---|---|---|---|---|
| 1min | 4/4 | 0.79 | 5/5 | 7.30e-05 |
| 5min | 4/4 | 0.80 | 5/5 | 2.41e-04 |
| 15min | 4/4 | 0.81 | 4/5 | 0.0051 |
| 30min | 3/4 | 0.81 | 4/5 | 0.0239 |

**Reading the verdict:**
- 4/4 = strong causal evidence at this TF — worth deploying
- 2-3/4 = real but partial signal — investigate further
- 0-1/4 = no causal evidence — IAS doesn't predict at this resolution
