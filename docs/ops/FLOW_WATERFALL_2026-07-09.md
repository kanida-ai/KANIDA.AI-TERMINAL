# Flow Paper-Trade — Waterfall to 100% WR (2026-07-09)

**Dataset:** 742 v1 trades (config: hard -2.5 / arm +1.3 / floor +0.9 / give 0.6), across 2026-07-08 (trend day) + 2026-07-09 (chop day). Win = gross pnl>0.

**Overall WR: 59.6%** (442W / 300L). Best-achievable after the waterfall filters: **66.7%**.

> Honesty: 384 symbols, median 2 trades/symbol — per-symbol WR is thin; the robust signal is the 742-trade pooled/pattern level. Only 13 of 171 '100%' symbols have >=3 trades.

## 10 things winning trades did right (entry-knowable → the constitution)

| trait                               |   wr_with |   wr_without |   n |   pct_of_wins |   lift | why                                 | rule_type   |
|:------------------------------------|----------:|-------------:|----:|--------------:|-------:|:------------------------------------|:------------|
| Book strongly agrees (lean>0.55)    |      60.9 |         54.9 | 578 |          79.6 |    6   | resting book backs the push         | boost       |
| Strong thrust (>median)             |      59.4 |         59.7 | 372 |          50   |   -0.3 | bigger 1-min impulse                | boost       |
| Cash (not FUT)                      |      59.4 |         60   | 537 |          72.2 |   -0.6 | tighter spreads / cleaner fills     | boost       |
| Book lean + whale together          |      58.6 |         59.7 |  70 |           9.3 |   -1.1 | structure + big-player confluence   | boost       |
| Whale on the aggressing side        |      57.8 |         59.8 | 102 |          13.3 |   -2   | few big orders lifting, not a crowd | boost       |
| Morning entry (<12h)                |      55.4 |         62.9 | 332 |          41.6 |   -7.5 | trend intact, pre-chop              | boost       |
| High conviction (>=2)               |      51.6 |         62.4 | 192 |          22.4 |  -10.8 | book+whale/tick agree on direction  | boost       |
| ATP-aligned entry (not counter-ATP) |      57.6 |         71.4 | 637 |          83   |  -13.8 | trading with fair-value location    | hard-filter |
| Tick buy-confirmed                  |      48.1 |         62.1 | 133 |          14.5 |  -13.9 | real aggressor agrees               | boost       |
| Long side                           |      44.4 |         67.3 | 250 |          25.1 |  -22.9 | context/regime dependent            | context     |

## Waterfall to best-achievable WR

| step                     |   wr_before |   gap | root_cause                       | fix                                                    |   wr_after |   gap_after |   removed |   winners_lost |   losers_removed | scalable                                   |
|:-------------------------|------------:|------:|:---------------------------------|:-------------------------------------------------------|-----------:|------------:|----------:|---------------:|-----------------:|:-------------------------------------------|
| 0 — current (all trades) |        59.6 |  40.4 | —                                | —                                                      |       59.6 |        40.4 |         0 |              0 |                0 | —                                          |
| 1 — filter               |        59.6 |  40.4 | Weak book lean (<0.52 agreement) | drop trades matching: Weak book lean (<0.52 agreement) |       60.5 |        39.5 |        74 |             38 |               36 | scalable (70 symbols, both-day consistent) |
| 2 — filter               |        60.5 |  39.5 | Weak thrust (< median impulse)   | drop trades matching: Weak thrust (< median impulse)   |       60.6 |        39.4 |       341 |            206 |              135 | partial (252 symbols)                      |
| 3 — filter               |        60.6 |  39.4 | No whale on aggressing side      | drop trades matching: No whale on aggressing side      |       66.7 |        33.3 |       282 |            168 |              114 | partial (201 symbols)                      |

## Per-symbol waterfall (top by losses)

| symbol     |   n |   WR |   wins |   losses |   gap | issue1      | fix1               |   WR_after_fix1 | issue2      | fix2               |   WR_after_fix2 |   best_WR |
|:-----------|----:|-----:|-------:|---------:|------:|:------------|:-------------------|----------------:|:------------|:-------------------|----------------:|----------:|
| KALYANKJIL |  12 | 58.3 |      7 |        5 |  41.7 | afternoon   | filter afternoon   |             100 | no-whale    | filter no-whale    |             100 |       100 |
| PAGEIND    |   6 | 50   |      3 |        3 |  50   | no-whale    | filter no-whale    |              75 | low-conv    | filter low-conv    |              75 |        75 |
| ASHOKLEY   |   5 | 40   |      2 |        3 |  60   | low-conv    | filter low-conv    |              67 | no-whale    | filter no-whale    |              67 |        67 |
| CHOLAFIN   |   5 | 40   |      2 |        3 |  60   | low-conv    | filter low-conv    |              67 | no-whale    | filter no-whale    |              67 |        67 |
| PNBHOUSING |   5 | 40   |      2 |        3 |  60   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| GODREJCP   |   4 | 25   |      1 |        3 |  75   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| IEX        |   4 | 25   |      1 |        3 |  75   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| KEI        |   4 | 25   |      1 |        3 |  75   | no-whale    | filter no-whale    |             100 | low-conv    | filter low-conv    |             100 |       100 |
| OBEROIRLTY |   4 | 25   |      1 |        3 |  75   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| SIEMENS    |   4 | 25   |      1 |        3 |  75   | no-whale    | filter no-whale    |              50 | low-conv    | filter low-conv    |              50 |        50 |
| ADANIGREEN |   3 |  0   |      0 |        3 | 100   | low-conv    | filter low-conv    |               0 | no-whale    | filter no-whale    |               0 |         0 |
| BLS        |   3 |  0   |      0 |        3 | 100   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| EICHERMOT  |   3 |  0   |      0 |        3 | 100   | no-whale    | filter no-whale    |             100 | low-conv    | filter low-conv    |             100 |       100 |
| ETERNAL    |   3 |  0   |      0 |        3 | 100   | no-whale    | filter no-whale    |             100 | low-conv    | filter low-conv    |             100 |       100 |
| PGEL       |   3 |  0   |      0 |        3 | 100   | no-whale    | filter no-whale    |             100 | low-conv    | filter low-conv    |             100 |       100 |
| PRESTIGE   |   3 |  0   |      0 |        3 | 100   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| TITAN      |   3 |  0   |      0 |        3 | 100   | no-whale    | filter no-whale    |             100 | afternoon   | filter afternoon   |             100 |       100 |
| JUBLFOOD   |   6 | 66.7 |      4 |        2 |  33.3 | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| ABREL      |   5 | 60   |      3 |        2 |  40   | no-whale    | filter no-whale    |             100 | afternoon   | filter afternoon   |             100 |       100 |
| BHARATFORG |   4 | 50   |      2 |        2 |  50   | no-whale    | filter no-whale    |             100 | weak-thrust | filter weak-thrust |             100 |       100 |
| CAMS       |   4 | 50   |      2 |        2 |  50   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| DABUR      |   4 | 50   |      2 |        2 |  50   | no-whale    | filter no-whale    |             100 | weak-thrust | filter weak-thrust |             100 |       100 |
| GABRIEL    |   4 | 50   |      2 |        2 |  50   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| POONAWALLA |   4 | 50   |      2 |        2 |  50   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| PREMIERENE |   4 | 50   |      2 |        2 |  50   | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| UNOMINDA   |   4 | 50   |      2 |        2 |  50   | weak-thrust | filter weak-thrust |             100 | low-conv    | filter low-conv    |             100 |       100 |
| ABFRL      |   3 | 33.3 |      1 |        2 |  66.7 | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| ADANIPORTS |   3 | 33.3 |      1 |        2 |  66.7 | low-conv    | filter low-conv    |             100 | no-whale    | filter no-whale    |             100 |       100 |
| CAPLIPOINT |   3 | 33.3 |      1 |        2 |  66.7 | afternoon   | filter afternoon   |             100 | low-conv    | filter low-conv    |             100 |       100 |
| DELHIVERY  |   3 | 33.3 |      1 |        2 |  66.7 | low-conv    | filter low-conv    |              50 | no-whale    | filter no-whale    |              50 |        50 |

## Trailing / missed-potential

- **trail_winner_n**: 395
- **avg_missed_fav_pct**: 0.474
- **missed_rs**: 926591.0
- **pct_winners_kept_running**: 42.0
- **stopped_loser_n**: 85
- **pct_stops_recovered**: 54.1
- **pct_stops_justified**: 55.3
- **mfe_median**: 1.13
- **mfe_cross_symbol_std**: 0.74
- **mfe_p10**: 0.25
- **mfe_p90**: 2.18