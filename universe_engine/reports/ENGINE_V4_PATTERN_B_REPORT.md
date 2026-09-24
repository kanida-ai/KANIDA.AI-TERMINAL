# Engine V4 — Pattern B only, Method A (blind 9:15 MOO), EOD data only

- Hypothesis: Pattern B (Compression + strict OI) is the cleanest signal cluster.
- Operational pitch: needs only daily OHLC + futures OI. No intraday data, no 9:30 confirmation.
- Entry: market-on-open at 9:15 next day after EOD signal.
- Hold: T+5 close (5 trading days). Costs 30bps RT + 5bps slippage each side.

## Trade outcomes (no capital constraint)

| Variant | n | WR | Hit +5% | Hit +10% | Mean ret | Avg win | Avg loss | W/L | Best | Worst | Total P&L |
|---|---|---|---|---|---|---|---|---|---|---|---|
| B overlap (any B fire) | 141 | 51.1% | 9.9% | 3.5% | +0.74% | ₹3,518 | -₹2,155 | 1.63 | ₹14,714 | -₹8,304 | ₹1.05 L |
| B alone (only B, no C/D) | 72 | 56.9% | 11.1% | 5.6% | +1.18% | ₹3,754 | -₹2,216 | 1.69 | ₹14,714 | -₹4,766 | ₹85,215 |

## Cash-constrained simulation (B overlap, ₹30L start, 30-pos cap)

- Starting capital: ₹30.00 L
- Ending equity: **₹30.66 L**
- Total P&L: **₹66,112**
- Return on starting: **+2.20%**
- Trades taken: 79  ·  Skipped: 62 ({'insufficient_cash': 62, 'max_concurrent': 0})
- Win rate: 54.4%  ·  W/L: 1.53
- Best: ₹11,837  ·  Worst: -₹5,483
- Max DD: -₹34,300 (-1.14%)
- Max concurrent: 30 of 30
- Avg deployed: ₹10.19 L  ·  Avg idle: ₹19.80 L
- Avg util: 33.8%  ·  Peak util: 99.4%

**Monthly P&L:**

| Month | P&L |
|---|---|
| 2026-03 | -₹27,857 |
| 2026-04 | ₹93,969 |

## V3 production vs V4 (Pattern B only, Method A)

| Engine | Signals | WR 5d | Hit +5% | Hit +10% | Mean ret | ROI on ₹30L | DD% |
|---|---|---|---|---|---|---|---|
| V3 (3-pat OR + Method B) | 83 | 65.1% | 31.3% | 6.0% | +1.71% | +3.53% | -1.63% |
| **V4 (B-overlap, Method A)** | 79 | 54.4% | 9.9% | 3.5% | +0.74% | +2.20% | -1.14% |

## Top 10 winners (B-overlap, Method A)

| Symbol | Entry | Exit | Patterns | OI 5d | Sub3-rng | P&L | Ret |
|---|---|---|---|---|---|---|---|
| OFSS | 2026-04-21 | 2026-04-27 | B | +96% | 4/7 | ₹14,714 | +14.71% |
| OFSS | 2026-04-17 | 2026-04-23 | B | +92% | 5/7 | ₹12,371 | +12.37% |
| OFSS | 2026-04-13 | 2026-04-20 | B+C+D | +104% | 5/7 | ₹11,837 | +11.84% |
| OFSS | 2026-04-20 | 2026-04-24 | B | +74% | 5/7 | ₹11,141 | +11.14% |
| PERSISTENT | 2026-03-30 | 2026-04-07 | B | +145% | 4/7 | ₹10,530 | +10.53% |
| ABB | 2026-04-16 | 2026-04-22 | B | +134% | 4/7 | ₹9,970 | +9.97% |
| ABB | 2026-04-17 | 2026-04-23 | B+D | +111% | 4/7 | ₹9,699 | +9.70% |
| OFSS | 2026-04-10 | 2026-04-17 | B+C+D | +73% | 5/7 | ₹9,385 | +9.38% |
| NESTLEIND | 2026-04-21 | 2026-04-27 | B | +161% | 6/7 | ₹8,774 | +8.77% |
| OFSS | 2026-04-09 | 2026-04-16 | B | +52% | 4/7 | ₹7,772 | +7.77% |

## Bottom 10 losers (B-overlap, Method A)

| Symbol | Entry | Exit | Patterns | OI 5d | Sub3-rng | P&L | Ret |
|---|---|---|---|---|---|---|---|
| VEDL | 2026-04-20 | 2026-04-24 | B+D | +66% | 6/7 | -₹8,304 | -8.30% |
| VEDL | 2026-04-17 | 2026-04-23 | B+D | +43% | 5/7 | -₹6,430 | -6.43% |
| DIXON | 2026-04-20 | 2026-04-24 | B+D | +63% | 6/7 | -₹5,483 | -5.48% |
| TVSMOTOR | 2026-04-16 | 2026-04-22 | B | +88% | 6/7 | -₹4,766 | -4.77% |
| AUROPHARMA | 2026-03-13 | 2026-03-19 | B+D | +162% | 5/7 | -₹4,750 | -4.75% |
| SUNPHARMA | 2026-03-13 | 2026-03-19 | B | +140% | 7/7 | -₹4,655 | -4.66% |
| BEL | 2026-04-20 | 2026-04-24 | B+C+D | +71% | 6/7 | -₹4,465 | -4.46% |
| LICI | 2026-04-16 | 2026-04-22 | B | +116% | 5/7 | -₹4,048 | -4.05% |
| LICI | 2026-04-20 | 2026-04-24 | B | +146% | 5/7 | -₹3,935 | -3.94% |
| ABB | 2026-03-13 | 2026-03-19 | B+C+D | +59% | 4/7 | -₹3,898 | -3.90% |
