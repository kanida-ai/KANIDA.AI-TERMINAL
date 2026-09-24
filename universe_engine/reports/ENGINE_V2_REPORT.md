# Engine V2 — Multi-pattern Breakout Detector

- Universe: in_nifty200, eval window 2026-03-05 -> 2026-04-24
- Universal gate: close within 3% of 20d high
- 5 OR'd patterns: A=Bandhan smart-money | B=Compression+OI | C=Heavy dry-up |
  D=Strong-close imbalance | E=RS leadership
- Per-pattern close gates: A>=60%, C>=60%, D>=70%, B and E no gate

## Frequency

- Total signals fired: **574** (390 max single pattern)
- Unique signal days:  28
- Avg signals per day: 20.50
- Pattern fires (overlap allowed): D=390, E=199, B=141, C=121, A=2
- Top firing symbols: AUROPHARMA(19), LUPIN(15), ABB(12), LAURUSLABS(11), PERSISTENT(11), OFSS(10), SUNPHARMA(10), HCLTECH(9)

## ALL signals — entry method A vs B

| Metric | (A) Blind MOO | (B) Two-stage |
|---|---|---|
| Filled | 574 (100%) | 112 (20%) |
| WR 5d | 58.2% | 66.1% |
| Mean ret 5d | +0.95% | +1.86% |
| Hit +5% | 28.4% | 33.0% |
| Hit +10% | 5.4% | 7.1% |
| Hit +15% | 0.3% | 0.9% |
| Best 5d | +16.7% | +13.4% |
| Worst 5d | -12.9% | -7.6% |

## Per-pattern hit rate (Method A — blind MOO)

Each row is the subset where THAT pattern fired (overlap with other patterns allowed).

| Pattern | n | WR 5d | Hit +5% | Hit +10% | Hit +15% | Mean ret 5d | Best 5d | Worst 5d |
|---|---|---|---|---|---|---|---|---|
| A | 2 | 50.0% | 0.0% | 0.0% | 0.0% | -0.90% | +1.5% | -3.3% |
| B | 141 | 56.0% | 22.0% | 7.1% | 0.7% | +1.14% | +15.1% | -7.9% |
| C | 121 | 57.9% | 28.9% | 5.0% | 0.0% | +0.76% | +12.2% | -10.5% |
| D | 390 | 59.5% | 30.3% | 5.6% | 0.3% | +1.05% | +16.7% | -12.9% |
| E | 199 | 48.2% | 25.6% | 3.0% | 0.0% | +0.23% | +12.2% | -9.9% |

## Per-pattern, NON-overlapping subsets (purest pattern-isolation read)

Signals where ONLY this pattern fired (no co-firing).

| Pattern | n | WR 5d | Hit +5% | Hit +10% | Mean ret 5d |
|---|---|---|---|---|---|
| A | 0 | — | — | — | — |
| B | 41 | 68.3% | 26.8% | 7.3% | +1.86% |
| C | 28 | 67.9% | 25.0% | 3.6% | +0.84% |
| D | 188 | 63.8% | 33.0% | 6.4% | +1.39% |
| E | 90 | 47.8% | 25.6% | 4.4% | +0.15% |

## Top 15 winners (5d return, Method A)

| Symbol | Date | Patterns | Entry | 5d ret | MFE | Close | OI 5d |
|---|---|---|---|---|---|---|---|
| BHEL | 2026-04-10 | D | 279.00 | +16.71% | +19.64% | 80% | +58% |
| OFSS | 2026-04-20 | B | 8130.00 | +15.13% | +16.73% | 64% | +96% |
| BHEL | 2026-04-09 | D | 279.00 | +13.54% | +13.91% | 78% | -26% |
| OFSS | 2026-04-16 | BD | 7795.00 | +12.78% | +14.18% | 92% | +92% |
| OFSS | 2026-04-10 | BCD | 7200.50 | +12.25% | +13.53% | 86% | +104% |
| RECLTD | 2026-04-10 | E | 341.15 | +12.18% | +12.72% | 46% | +24% |
| NESTLEIND | 2026-04-16 | D | 1258.10 | +12.11% | +13.28% | 71% | +151% |
| OFSS | 2026-04-17 | BD | 8063.50 | +11.55% | +11.78% | 91% | +74% |
| NESTLEIND | 2026-04-15 | D | 1255.10 | +11.21% | +13.55% | 88% | +45% |
| SIEMENS | 2026-04-10 | CD | 3340.00 | +11.05% | +13.13% | 92% | +72% |
| PERSISTENT | 2026-03-27 | BE | 4855.00 | +10.94% | +12.02% | 36% | +145% |
| ADANIENT | 2026-04-10 | D | 2010.20 | +10.91% | +13.43% | 85% | +39% |
| TORNTPOWER | 2026-04-20 | E | 1600.00 | +10.78% | +14.01% | 64% | — |
| ADANIPORTS | 2026-04-10 | D | 1426.80 | +10.63% | +12.17% | 93% | +50% |
| ASIANPAINT | 2026-04-10 | D | 2279.90 | +10.39% | +10.70% | 85% | +23% |

## Bottom 15 losers (5d return, Method A)

| Symbol | Date | Patterns | Entry | 5d ret | MAE | Close | OI 5d |
|---|---|---|---|---|---|---|---|
| POLYCAB | 2026-03-05 | D | 8562.00 | -12.93% | -14.95% | 93% | +110% |
| BHARATFORG | 2026-03-06 | D | 1909.20 | -12.26% | -12.73% | 79% | +300% |
| MPHASIS | 2026-04-17 | D | 2472.00 | -11.93% | -13.83% | 91% | +406% |
| SBILIFE | 2026-04-17 | CD | 1976.30 | -10.54% | -10.84% | 77% | +166% |
| MCX | 2026-03-16 | CD | 2579.90 | -10.29% | -12.40% | 84% | +25% |
| LTIM | 2026-04-17 | D | 4753.60 | -9.91% | -10.80% | 81% | — |
| TECHM | 2026-04-17 | CDE | 1510.90 | -9.87% | -10.82% | 77% | +115% |
| MCX | 2026-03-17 | CD | 2680.00 | -9.74% | -15.67% | 92% | -1% |
| SBILIFE | 2026-04-20 | CD | 1974.40 | -8.05% | -10.91% | 78% | +302% |
| VEDL | 2026-04-17 | BDE | 782.95 | -7.91% | -9.62% | 80% | +66% |
| IEX | 2026-04-17 | E | 132.89 | -7.30% | -7.94% | 59% | +29% |
| HINDALCO | 2026-03-12 | D | 967.00 | -7.23% | -7.74% | 94% | +54% |
| TCS | 2026-04-17 | D | 2587.10 | -7.19% | -7.66% | 77% | +68% |
| MCDOWELL-N | 2026-03-10 | D | 1401.80 | -7.16% | -7.35% | 95% | — |
| TECHM | 2026-04-20 | E | 1501.00 | -6.99% | -10.23% | 23% | +80% |

## V2 verdict

- 5d win rate (all signals, Method A): **58.2%**
- Hit +5%: **28.4%**, Hit +10%: **5.4%**, Hit +15%: **0.3%**
- Best individual signal: +16.7% in 5d

**Pattern-level decision rule for V3:**
- Keep patterns where Hit +5% > 30% AND Mean ret 5d > +1%
- Drop patterns where WR 5d < 40%
- Investigate patterns with high WR but low hit-+10% (might be small-edge, slow-grind)