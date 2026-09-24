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
| 1min | 12,600 | 8300 | 4195 | 24.9% | 29.7% | 0.84 | -0.31 | 0.7565 | -0.01 |
| 5min | 12,600 | 8697 | 3798 | 25.2% | 29.5% | 0.86 | 0.31 | 0.7531 | 0.01 |
| 15min | 12,600 | 9571 | 2924 | 25.5% | 30.0% | 0.85 | 0.25 | 0.8020 | 0.01 |
| 30min | 12,600 | 10469 | 2026 | 25.9% | 29.6% | 0.88 | 1.05 | 0.2954 | 0.03 |

## Test 2 — IAS Dose-Response (5 quintiles)

Monotone increase in hit-rate across IAS quintiles = dose-response = causal evidence.
Cochran-Armitage trend test p-value reported per TF.

**1min** (trend p = 0.00e+00):

| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |
|---|---|---|---|---|
| Q1 | 1.84–4.92 | 2499 | 29.9% | -0.036% |
| Q2 | 4.92–5.75 | 2499 | 28.7% | +0.008% |
| Q3 | 5.75–6.46 | 2499 | 25.6% | -0.033% |
| Q4 | 6.46–7.3 | 2499 | 24.5% | -0.059% |
| Q5 | 7.3–9.25 | 2499 | 24.0% | +0.025% |

**5min** (trend p = 0.00e+00):

| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |
|---|---|---|---|---|
| Q1 | 1.83–5.04 | 2499 | 30.1% | -0.011% |
| Q2 | 5.04–5.87 | 2499 | 28.5% | -0.022% |
| Q3 | 5.87–6.58 | 2499 | 25.6% | -0.045% |
| Q4 | 6.58–7.4 | 2499 | 24.6% | -0.045% |
| Q5 | 7.4–9.29 | 2499 | 23.9% | +0.029% |

**15min** (trend p = 0.00e+00):

| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |
|---|---|---|---|---|
| Q1 | 1.87–5.33 | 2499 | 29.2% | -0.072% |
| Q2 | 5.33–6.18 | 2499 | 29.3% | +0.041% |
| Q3 | 6.18–6.88 | 2499 | 25.4% | -0.047% |
| Q4 | 6.88–7.66 | 2499 | 25.2% | -0.028% |
| Q5 | 7.66–9.49 | 2499 | 23.4% | +0.011% |

**30min** (trend p = 1.00e-06):

| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |
|---|---|---|---|---|
| Q1 | 2.15–5.69 | 2499 | 29.2% | -0.094% |
| Q2 | 5.69–6.48 | 2499 | 28.1% | +0.013% |
| Q3 | 6.48–7.12 | 2499 | 26.5% | -0.049% |
| Q4 | 7.12–7.83 | 2499 | 25.4% | +0.023% |
| Q5 | 7.83–9.68 | 2499 | 23.5% | +0.012% |

## Test 3 — Multi-day forward returns (D+1 to D+5, Bonferroni-corrected)

| TF | Day | n_sig | n_nosig | Mean signal | Mean control | t | p (raw) | p (Bonf) | Cohen's d |
|---|---|---|---|---|---|---|---|---|---|
| 1min | D+1 | 8300 | 4195 | -0.023% | -0.011% | -0.31 | 0.7565 | 1.0000 | -0.01 |
| 1min | D+2 | 8220 | 4170 | -0.037% | -0.033% | -0.08 | 0.9387 | 1.0000 | -0.00 |
| 1min | D+3 | 8137 | 4148 | -0.032% | -0.136% | 1.47 | 0.1405 | 0.7023 | 0.03 |
| 1min | D+4 | 8080 | 4100 | -0.064% | -0.167% | 1.24 | 0.2136 | 1.0000 | 0.03 |
| 1min | D+5 | 7998 | 4077 | -0.103% | -0.112% | 0.10 | 0.9190 | 1.0000 | 0.00 |
| 5min | D+1 | 8697 | 3798 | -0.015% | -0.028% | 0.31 | 0.7531 | 1.0000 | 0.01 |
| 5min | D+2 | 8617 | 3773 | -0.028% | -0.055% | 0.47 | 0.6372 | 1.0000 | 0.01 |
| 5min | D+3 | 8533 | 3752 | -0.037% | -0.134% | 1.32 | 0.1875 | 0.9376 | 0.03 |
| 5min | D+4 | 8475 | 3705 | -0.071% | -0.161% | 1.05 | 0.2924 | 1.0000 | 0.02 |
| 5min | D+5 | 8389 | 3686 | -0.110% | -0.098% | -0.12 | 0.9055 | 1.0000 | -0.00 |
| 15min | D+1 | 9571 | 2924 | -0.016% | -0.028% | 0.25 | 0.8020 | 1.0000 | 0.01 |
| 15min | D+2 | 9485 | 2905 | -0.047% | -0.001% | -0.70 | 0.4846 | 1.0000 | -0.02 |
| 15min | D+3 | 9397 | 2888 | -0.062% | -0.083% | 0.26 | 0.7962 | 1.0000 | 0.01 |
| 15min | D+4 | 9337 | 2843 | -0.098% | -0.101% | 0.03 | 0.9747 | 1.0000 | 0.00 |
| 15min | D+5 | 9243 | 2832 | -0.135% | -0.013% | -1.15 | 0.2491 | 1.0000 | -0.03 |
| 30min | D+1 | 10469 | 2026 | -0.009% | -0.069% | 1.05 | 0.2954 | 1.0000 | 0.03 |
| 30min | D+2 | 10380 | 2010 | -0.053% | +0.054% | -1.41 | 0.1596 | 0.7978 | -0.04 |
| 30min | D+3 | 10282 | 2003 | -0.092% | +0.063% | -1.64 | 0.1018 | 0.5091 | -0.04 |
| 30min | D+4 | 10215 | 1965 | -0.146% | +0.149% | -2.67 | 0.0077 | 0.0385 | -0.07 |
| 30min | D+5 | 10117 | 1958 | -0.189% | +0.319% | -4.13 | 3.60e-05 | 1.80e-04 | -0.11 |

## Test 4 — Granger causality (does IAS predict beyond lagged returns?)

OLS F-test: full model `fwd_d1 ~ const + lag1 + lag2 + IAS` vs restricted `fwd_d1 ~ const + lag1 + lag2`.
p < 0.05 means IAS adds significant predictive power beyond pure return autocorrelation.

| TF | n | F | df1 | df2 | p | IAS coefficient |
|---|---|---|---|---|---|---|
| 1min | 12285 | 0.6569 | 1 | 12281 | 0.4177 | 0.01111 |
| 5min | 12285 | 0.5847 | 1 | 12281 | 0.4445 | 0.01052 |
| 15min | 12285 | 0.837 | 1 | 12281 | 0.3603 | 0.01276 |
| 30min | 12285 | 1.2496 | 1 | 12281 | 0.2636 | 0.01661 |

## Verdict — which TF gives the strongest causal evidence?

| TF | Tests passed (of 4) | Lift (Test 1) | Bonf-sig fwd days | Granger p |
|---|---|---|---|---|
| 1min | 1/4 | 0.84 | 0/5 | 0.4177 |
| 5min | 1/4 | 0.86 | 0/5 | 0.4445 |
| 15min | 1/4 | 0.85 | 0/5 | 0.3603 |
| 30min | 2/4 | 0.88 | 2/5 | 0.2636 |

**Reading the verdict:**
- 4/4 = strong causal evidence at this TF — worth deploying
- 2-3/4 = real but partial signal — investigate further
- 0-1/4 = no causal evidence — IAS doesn't predict at this resolution
