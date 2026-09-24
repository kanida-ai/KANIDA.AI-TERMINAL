# Engine V3 P&L — Cash-Constrained Simulator

- Starting capital: ₹30.00 L
- Per-trade slug: ₹1.00 L
- Max concurrent positions: 30
- No margin / no borrowing — skip when cash < ₹1L OR open >= 30
- Hold: T+1 entry to T+5 close  ·  Costs: 30bps RT + 5bps slippage each side

## Headline

- **Starting capital:** ₹30.00 L
- **Ending equity:** ₹31.06 L
- **Total P&L:** ₹1.06 L
- **Return on starting capital:** +3.53%
- **Trades taken:** 82
- **Trades skipped:** 1  (insufficient cash: 1, max concurrent: 0)
- **Win rate:** 58.5%
- **Avg win:** ₹3,681  ·  **Avg loss:** -₹2,078  ·  **W/L ratio:** 1.77
- **Best trade:** ₹9,052  ·  **Worst trade:** -₹5,356
- **Max drawdown:** -₹49,340  (-1.63% of peak equity)
- **Max concurrent positions used:** 30 of 30
- **Avg deployed:** ₹9.94 L  ·  **Avg idle cash:** ₹20.10 L
- **Avg utilization:** 33.0%  ·  **Peak utilization:** 99.0%

## Monthly P&L (by exit month)

| Month | P&L |
|---|---|
| 2026-03 | -₹23,623 |
| 2026-04 | ₹1.30 L |

## Skipped trades (sample of first 20)

| Symbol | Signal date | Reason | Cash at skip | Open at skip |
|---|---|---|---|---|
| TRENT | 2026-04-17 | insufficient_cash | ₹30,635 | 30 |

**Skipped count by signal date** (only days with skips shown):

| Date | Skipped | Reason mix |
|---|---|---|
| 2026-04-17 | 1 | cash=1 |

## Top 10 winners

| Symbol | Entry | Exit | Patterns | P&L | Ret % |
|---|---|---|---|---|---|
| SIEMENS | 2026-04-15 | 2026-04-21 | C | ₹9,052 | +9.05% |
| ABB | 2026-04-17 | 2026-04-23 | B+D | ₹8,941 | +8.94% |
| PAGEIND | 2026-04-02 | 2026-04-09 | D | ₹8,800 | +8.80% |
| NESTLEIND | 2026-04-21 | 2026-04-27 | B | ₹8,767 | +8.77% |
| DEEPAKNTR | 2026-04-21 | 2026-04-27 | D | ₹8,250 | +8.25% |
| TORNTPOWER | 2026-04-20 | 2026-04-24 | C+D | ₹7,634 | +7.63% |
| PFC | 2026-04-15 | 2026-04-21 | B+D | ₹6,954 | +6.95% |
| AUBANK | 2026-04-20 | 2026-04-24 | C+D | ₹6,630 | +6.63% |
| PERSISTENT | 2026-03-27 | 2026-04-06 | D | ₹6,343 | +6.34% |
| AARTIIND | 2026-04-20 | 2026-04-24 | D | ₹5,840 | +5.84% |

## Bottom 10 losers

| Symbol | Entry | Exit | Patterns | P&L | Ret % |
|---|---|---|---|---|---|
| NTPC | 2026-03-13 | 2026-03-19 | D | -₹5,356 | -5.36% |
| GLENMARK | 2026-03-11 | 2026-03-17 | D | -₹4,883 | -4.88% |
| WIPRO | 2026-04-16 | 2026-04-22 | D | -₹4,612 | -4.61% |
| VEDL | 2026-03-11 | 2026-03-17 | C+D | -₹4,279 | -4.28% |
| POWERGRID | 2026-03-13 | 2026-03-19 | D | -₹4,268 | -4.27% |
| MPHASIS | 2026-04-16 | 2026-04-22 | D | -₹4,155 | -4.16% |
| LICI | 2026-04-16 | 2026-04-22 | B | -₹4,048 | -4.05% |
| ABB | 2026-03-13 | 2026-03-19 | B+C+D | -₹4,037 | -4.04% |
| INFY | 2026-04-08 | 2026-04-15 | D | -₹3,646 | -3.65% |
| GODREJCP | 2026-04-20 | 2026-04-24 | C+D | -₹3,084 | -3.08% |

## Daily equity timeline (every 3rd day shown)

| Date | Cash | Deployed | Unrealized | Equity | Open | Util % | DD % |
|---|---|---|---|---|---|---|---|
| 2026-03-06 | ₹26.00 L | ₹4.00 L | ₹4,946 | ₹30.05 L | 4 | 13.5% | +0.00% |
| 2026-03-11 | ₹17.00 L | ₹13.00 L | ₹20,807 | ₹30.21 L | 13 | 43.7% | +0.00% |
| 2026-03-16 | ₹22.05 L | ₹8.00 L | -₹27,191 | ₹29.78 L | 8 | 25.9% | -1.43% |
| 2026-03-19 | ₹29.76 L | ₹0 | ₹0 | ₹29.76 L | 0 | 0.0% | -1.47% |
| 2026-03-24 | ₹29.76 L | ₹0 | ₹0 | ₹29.76 L | 0 | 0.0% | -1.47% |
| 2026-03-30 | ₹26.76 L | ₹3.00 L | -₹4,909 | ₹29.71 L | 3 | 9.9% | -1.63% |
| 2026-04-06 | ₹26.82 L | ₹3.00 L | ₹2,183 | ₹29.85 L | 3 | 10.1% | -1.20% |
| 2026-04-09 | ₹15.89 L | ₹14.00 L | -₹6,127 | ₹29.83 L | 14 | 46.7% | -1.25% |
| 2026-04-15 | ₹7.94 L | ₹22.00 L | ₹32,524 | ₹30.27 L | 22 | 73.8% | +0.00% |
| 2026-04-20 | ₹30,635 | ₹30.00 L | ₹34,205 | ₹30.65 L | 30 | 99.0% | +0.00% |
| 2026-04-23 | ₹14.70 L | ₹16.00 L | ₹37,938 | ₹31.08 L | 16 | 52.7% | -0.21% |
| 2026-04-27 | ₹31.06 L | ₹0 | ₹0 | ₹31.06 L | 0 | 0.0% | -0.27% |