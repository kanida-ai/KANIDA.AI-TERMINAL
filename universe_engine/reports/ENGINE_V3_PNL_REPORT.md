# Engine V3 P&L Simulator — ₹1L per trade, no capital cap

- Strategy: V3 production (3 patterns OR'd + Method B 9:30 confirm)
- Per-trade slug: ₹1.00 L
- Capital: uncapped, scales with concurrent positions
- Hold rule: T+1 entry to T+5 close (5 trading days)
- Costs: 30 bps round-trip + 5 bps slippage each side

## Headline

- **Total P&L:** ₹1.08 L
- **N trades:** 83
- **Sum capital deployed (n × ₹1L):** ₹83.00 L
- **Peak capital deployed (max concurrent × ₹1L):** ₹31.00 L
- **ROI on sum deployed:** +1.30%
- **ROI on peak deployed:** +3.49%
- **Max drawdown:** -₹49,340  (-1.59% of peak deployed, -40.97% of peak equity)
- **Win rate:** 59.0%
- **Avg win:** ₹3,651  ·  **Avg loss:** -₹2,078  ·  **Win/Loss ratio:** 1.76
- **Best trade:** ₹9,052  ·  **Worst trade:** -₹5,356
- **Max concurrent positions:** 31
- **Capital utilization (avg/peak):** 32.5%
- **Avg return per trade:** +1.30%

## Monthly P&L (by exit month)

| Month | P&L |
|---|---|
| 2026-03 | -₹23,623 |
| 2026-04 | ₹1.32 L |

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

## Equity curve milestones (every 5th day)

| Date | n_open | Deployed | Cum realized | Unrealized | Equity | DD |
|---|---|---|---|---|---|---|
| 2026-03-06 | 4 | ₹4.00 L | ₹0 | ₹4,946 | ₹4,946 | ₹0 |
| 2026-03-13 | 11 | ₹11.00 L | ₹4,927 | -₹17,768 | -₹12,841 | -₹33,648 |
| 2026-03-20 | 0 | ₹0 | -₹23,623 | ₹0 | -₹23,623 | -₹44,431 |
| 2026-03-30 | 3 | ₹3.00 L | -₹23,623 | -₹4,909 | -₹28,533 | -₹49,340 |
| 2026-04-08 | 12 | ₹12.00 L | -₹19,482 | ₹2,928 | -₹16,554 | -₹37,362 |
| 2026-04-16 | 29 | ₹29.00 L | ₹2,173 | ₹40,603 | ₹42,775 | ₹0 |
| 2026-04-23 | 17 | ₹17.00 L | ₹69,892 | ₹39,542 | ₹1.09 L | -₹10,990 |
| 2026-04-27 | 0 | ₹0 | ₹1.08 L | ₹0 | ₹1.08 L | -₹12,150 |
