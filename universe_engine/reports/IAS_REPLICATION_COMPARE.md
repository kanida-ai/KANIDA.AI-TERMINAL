# IAS Replication — Window A vs Window B

**Window A** (original): 2025-10-28 → 2026-04-30
**Window B** (replication): 2024-11-01 → 2025-04-30

Identical universe (143 NSE F&O stocks), identical 13:45–15:29 IST window, identical IAS formula, identical signal threshold (IAS ≥ 5.5), identical 'up day' threshold (next-day close-to-close ≥ 1.0%).

## Headline — does the IAS sign hold across windows?

| TF | Window | Panel rows | Hit (sig) | Hit (no-sig) | Lift | t-stat | p | Cohen's d |
|---|---|---|---|---|---|---|---|---|
| 1min | A | 17,732 | 23.1% | 29.1% | 0.79 | -3.02 | 0.0025 | -0.06 |
| 1min | B | 12,600 | 24.9% | 29.7% | 0.84 | -0.31 | 0.7565 | -0.01 |
| 5min | A | 17,732 | 23.4% | 29.0% | 0.80 | -3.29 | 9.95e-04 | -0.06 |
| 5min | B | 12,600 | 25.2% | 29.5% | 0.86 | 0.31 | 0.7531 | 0.01 |
| 15min | A | 17,732 | 23.7% | 29.4% | 0.81 | -2.32 | 0.0205 | -0.05 |
| 15min | B | 12,600 | 25.5% | 30.0% | 0.85 | 0.25 | 0.8020 | 0.01 |
| 30min | A | 17,732 | 24.0% | 29.6% | 0.81 | -1.47 | 0.1422 | -0.04 |
| 30min | B | 12,600 | 25.9% | 29.6% | 0.88 | 1.05 | 0.2954 | 0.03 |

## Sign of Granger IAS coefficient

| TF | A coef | A p | B coef | B p |
|---|---|---|---|---|
| 1min | -0.04547 | 7.30e-05 | 0.01111 | 0.4177 |
| 5min | -0.04231 | 2.41e-04 | 0.01052 | 0.4445 |
| 15min | -0.03292 | 0.0051 | 0.01276 | 0.3603 |
| 30min | -0.02835 | 0.0239 | 0.01661 | 0.2636 |

## Verdict guide

- **Lift < 1 in BOTH windows AND Granger coef negative in BOTH** → finding is robust across regimes; high IAS systematically predicts weakness on this universe.
- **Lift flips above 1 in Window B** → regime-conditional; the inversion only applies in 2025-26 conditions, not a fundamental property.
- **Mixed (some TFs hold, others flip)** → effect is real but fragile; not safe to use as a standalone gate without further conditioning.