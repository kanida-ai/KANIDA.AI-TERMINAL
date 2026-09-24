# Intraday Accumulation Causation Engine Findings

Generated: 2026-04-29

## What Was Built

A separate causation-analysis layer was created without changing the existing backend pipeline:

- Engine file: `scripts/intraday_accumulation_causation_engine.py`
- Data source: `C:\Users\SPS\Desktop\Kanida Intraday Lab\data\intraday.db`
- Output folder: `outputs\intraday_causation`

The engine tests whether prior intraday accumulation behavior increases the probability of next-day and multi-day upside.

## Dataset Used

- Market: NSE
- Intraday table: `ohlc_1min`
- Intraday rows read: 2,110,477
- Stock-days tested: 4,851
- Tickers tested: 147
- Date range tested: 2026-03-09 to 2026-04-28
- Accumulation signal days found: 105

The engine uses all available stock-days, not only winning stocks.

## How Accumulation Was Defined

The engine scores each stock-day using a 0-10 Institutional Accumulation Score, or IAS.

Inputs include:

- Late-day volume buildup
- Volume acceleration
- Same-minute 20-day volume comparison
- Price compression
- Small net price movement
- VWAP support
- Absorption behavior

A stock-day is treated as an accumulation signal when:

- IAS is at least 6.0
- Price is compressed
- Absorption behavior is present

## Causation Tests Added

### 1. Base Rate Comparison

Question tested:

Does upside happen more often after accumulation days than after non-accumulation days?

Result:

The current accumulation signal does not yet show a positive base-rate edge.

Key examples:

- Gap-up >= 0.8%: accumulation 20.0% vs baseline 21.85%
- First 60-minute rally >= 1.2%: accumulation 11.43% vs baseline 14.08%
- Next-day high >= 2%: accumulation 30.48% vs baseline 34.34%
- Day+1 close positive: accumulation 43.81% vs baseline 54.17%

Important finding:

Day+1 close-positive rate is significantly worse after the current accumulation signal, with p-value about 0.035.

### 2. IAS Dose-Response

Question tested:

As IAS rises, does hit rate also rise?

Result:

No clean monotonic dose-response is visible yet.

The highest IAS quintile does not produce the best next-day results. This means the current IAS formula is probably mixing true accumulation with other late-day volume events such as exhaustion, event risk, or noisy spikes.

### 3. Multi-Day Forward Returns

Question tested:

After accumulation, does the stock perform better from Day+1 through Day+5?

Result:

No statistically strong positive forward-return lift was found.

Key examples:

- Day+1 return: accumulation -0.115% vs baseline +0.176%
- Day+3 return: accumulation +0.385% vs baseline +0.422%
- Day+5 return: accumulation +0.813% vs baseline +0.808%

Day+5 is roughly equal to baseline, but not meaningfully better.

### 4. Granger-Style Lag Test

Question tested:

Does lagged IAS predict future return beyond past returns, daily range, and daily volume behavior?

Result:

IAS adds some predictive information for selected targets:

- Next-day gap: significant, p-value about 0.0188
- Day+1 return: significant, p-value near 0.0000
- Day+5 return: borderline significant, p-value about 0.0493

But the explained improvement is very small:

- R2 gain from IAS is about 0.11% for gap
- R2 gain from IAS is about 0.77% for Day+1 return
- R2 gain from IAS is about 0.09% for Day+5 return

This means IAS contains some information, but it is not yet strong enough alone to power a high-confidence production signal.

### 5. Statistical Significance

The engine outputs:

- Mean difference
- 95% confidence interval
- t-statistic
- p-value
- Significant/not significant flag

Current p-values use a normal approximation because the environment did not include scipy/statsmodels. This is acceptable for first-pass engine work, but production research should upgrade to scipy/statsmodels for exact t-tests, F-tests, and robust regression diagnostics.

## Current Verdict

Verdict: WEAK_CAUSAL_EVIDENCE

Plain English interpretation:

The current accumulation score has some predictive signal, but it does not yet prove that quiet accumulation is causing stronger upside. The base-rate and forward-return tests are not positive enough. The regression tests say IAS contains some information, but the effect is small.

## Where It Is Broken Or Incomplete

The current IAS is likely too broad.

It appears to capture several different behaviors:

- True institutional accumulation
- Late-day short covering
- Event/news anticipation
- Exhaustion volume
- Random volume spikes
- Index-wide market movement

Because these are mixed together, the high-IAS bucket does not consistently outperform.

## What Should Be Improved Next

The next version should separate true accumulation from false positives using:

- News/event exclusion
- Sector/index-relative return filter
- Delivery or order-flow proxy if available
- More strict price compression rules
- Avoiding stocks already extended before the accumulation window
- Separate long-only accumulation from reversal/mean-reversion volume
- Stock-specific thresholds instead of one global IAS threshold
- Rolling walk-forward validation
- Matched controls by stock, date, volatility, and prior return

## Files Produced

- `causation_stock_days.csv`: full stock-day level dataset with IAS and forward outcomes
- `causation_base_rates.csv`: P(up | accumulation) vs P(up | no accumulation)
- `causation_dose_response.csv`: IAS quintile hit-rate curve
- `causation_forward_returns.csv`: Day+1 to Day+5 return comparison
- `causation_granger.csv`: Granger-style regression tests
- `causation_report.txt`: plain text report
- `CAUSATION_ENGINE_FINDINGS.md`: this readable product/architecture summary

## Product Meaning

This is now a valid research engine layer for Intraday Accumulation Intelligence.

It should not yet be used as a direct trade signal. It should be used as a research layer to refine the signal until:

- Accumulation days outperform baseline across base-rate tests
- Higher IAS buckets show higher hit rates
- Day+1 through Day+5 forward returns improve versus baseline
- Regression tests stay significant out of sample
- False positives are reduced through event/noise filters

