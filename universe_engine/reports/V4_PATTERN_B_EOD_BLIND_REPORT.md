# V4 Ablation - EOD Pattern B Blind 9:15

- Source: `universe_engine/reports/_engine_v3_signals.json`
- Test: Pattern B only from EOD daily+OI features, blind next-day open entry (`em_a`, treated as 9:15 open).
- Comparison: same Pattern B with 9:30 Method B confirmation and full V3 production Method B.

| Config | n | WR 5d | Hit +5% | Hit +10% | Mean 5d | Median 5d | Best | Worst |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Pattern B, blind 9:15 | 141 | 56.0% | 22.0% | 7.1% | +1.14% | +0.52% | +15.1% | -7.9% |
| Pattern B-only, blind 9:15 | 72 | 61.1% | 23.6% | 8.3% | +1.59% | +1.18% | +15.1% | -4.4% |
| Pattern B-overlap, blind 9:15 | 69 | 50.7% | 20.3% | 5.8% | +0.68% | +0.17% | +12.2% | -7.9% |
| Pattern B, 9:30 confirmed | 23 | 65.2% | 26.1% | 8.7% | +1.78% | +1.45% | +9.4% | -3.7% |
| Full V3, 9:30 confirmed | 83 | 65.1% | 31.3% | 6.0% | +1.71% | +1.45% | +9.5% | -5.0% |

## Pattern B combinations

| Combo | n | WR 5d | Hit +5% | Mean 5d |
|---|---:|---:|---:|---:|
| B | 72 | 61.1% | 23.6% | +1.59% |
| B+D | 52 | 55.8% | 21.2% | +0.61% |
| B+C+D | 11 | 36.4% | 27.3% | +1.27% |
| B+C | 6 | 33.3% | 0.0% | +0.21% |