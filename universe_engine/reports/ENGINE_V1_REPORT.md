# Engine V1 — VCP Breakout Setup Detector

- Universe: in_nifty200, eval window 2026-03-05 -> 2026-04-24
- Setup criteria: range contraction + volume dry-up + late-day vol surge + OI buildup (where data exists)
- Two entry methods compared: (A) blind MOO vs (B) two-stage buy-stop with 9:30 vol confirmation
- Outcome metrics: 1d/5d return, MFE/MAE, hit rate at +5/+10/+15%

## Frequency

- Total signals fired: **22**
- Unique signal days:  11
- Avg signals per day: 2.00
- Top signal days: 2026-03-27(6), 2026-03-20(3), 2026-03-16(3), 2026-03-11(2), 2026-03-25(2)
- Top firing symbols: IRCTC(4), ABB(2), ZYDUSLIFE(2), ALKEM(1), BERGEPAINT(1), CROMPTON(1), DIVISLAB(1), HCLTECH(1)

- Of 22 signals, **12** had OI gate evaluable; 10 fired on the 3-component subset (non-F&O or pre-OI window).

## Entry method A/B comparison

| Metric | (A) Blind MOO | (B) Two-stage buy-stop |
|---|---|---|
| Filled (of all signals) | 22 (100%) | 2 (9%) |
| Win rate, 1d | 22.7% | 50.0% |
| Win rate, 5d | 40.9% | 0.0% |
| Mean return, 1d | -0.41% | +2.09% |
| Median return, 1d | -0.72% | +2.09% |
| Mean return, 5d | -1.26% | -4.34% |
| Median return, 5d | -1.39% | -4.34% |
| Mean MFE (5d high) | +2.83% | +3.47% |
| Mean MAE (5d low) | -4.60% | -5.79% |
| Hit +5% (within 5d) | 18.2% | 50.0% |
| Hit +10% (within 5d) | 0.0% | 0.0% |
| Hit +15% (within 5d) | 0.0% | 0.0% |
| Best 5d return | +5.2% | -1.3% |
| Worst 5d return | -7.8% | -7.4% |

## With-OI subset only (12 signals)

| Metric | Blind MOO (with OI gate) |
|---|---|
| Win rate, 5d | 33.3% |
| Mean ret 5d  | -1.39% |
| Hit +5%      | 8.3% |
| Hit +10%     | 0.0% |
| Hit +15%     | 0.0% |

## Top 10 winners (by 5d return, method A)

| Symbol | Signal date | Entry | 5d ret | MFE 5d | OI gate |
|---|---|---|---|---|---|
| IRCTC | 2026-03-30 | 510.00 | +5.20% | +5.45% | - |
| ABB | 2026-03-27 | 5995.00 | +4.57% | +4.75% | OK |
| BERGEPAINT | 2026-03-27 | 412.75 | +4.14% | +5.39% | - |
| HCLTECH | 2026-03-16 | 1303.64 | +2.26% | +3.75% | OK |
| IRCTC | 2026-03-27 | 508.00 | +1.61% | +2.83% | - |
| ABB | 2026-03-11 | 6242.00 | +1.53% | +5.00% | OK |
| MRF | 2026-03-20 | 127515.00 | +0.77% | +2.65% | - |
| IDFCFIRSTB | 2026-03-27 | 60.92 | +0.44% | +0.95% | OK |
| UBL | 2026-03-05 | 1640.70 | +0.29% | +8.44% | - |
| CROMPTON | 2026-03-13 | 246.00 | -1.16% | +3.25% | OK |

## Top 10 losers (by 5d return, method A)

| Symbol | Signal date | Entry | 5d ret | MAE 5d | OI gate |
|---|---|---|---|---|---|
| TATACHEM | 2026-03-16 | 660.00 | -7.75% | -9.81% | - |
| ICICIGI | 2026-03-16 | 1850.50 | -6.93% | -7.81% | - |
| IRFC | 2026-03-20 | 93.50 | -6.71% | -6.95% | OK |
| TORNTPHARM | 2026-03-27 | 4222.90 | -4.06% | -8.33% | OK |
| ZYDUSLIFE | 2026-03-25 | 901.95 | -3.95% | -7.37% | OK |
| LICI | 2026-03-25 | 775.00 | -3.88% | -6.90% | OK |
| IRCTC | 2026-04-20 | 565.60 | -3.62% | -4.67% | - |
| ALKEM | 2026-03-11 | 5515.50 | -2.97% | -5.12% | - |
| SBICARD | 2026-03-17 | 693.70 | -2.91% | -6.29% | OK |
| IRCTC | 2026-03-23 | 520.05 | -1.70% | -5.27% | - |

## V1 verdict

- 5d win rate (Method A): **40.9%** vs ~50% naive coin-flip => edge -9.1pp
- Hit-rate at +5% in 5d: **18.2%**
- Method B fill rate: 9% (the rest were filtered out by 9:30 vol confirmation)
- Mean 5d return per signal (Method A): **-1.26%**

Read the table to decide V2 priorities: which gate to drop, which to tighten, whether OI confirmation actually helps.