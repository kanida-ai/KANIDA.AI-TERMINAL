# Falcon Live Signals — 2026-04-30

- Engine: **V7.1**
- Signal date: **2026-04-30**
- Eligible patterns (mined before 2026): 674
- Stocks evaluated: 330  ·  Qualifying (≥2 fires): 310
- Showing top 25 by aggregate score

## Buy candidates for next trading day

| Rank | Symbol | Sector | Close | n_fires | Score | Liquidity (60d avg) |
|---|---|---|---|---|---|---|
| 1 | **HFCL** | Telecommunication | ₹116.0 | 141 | 2196 | ₹325.56 Cr |
| 2 | **CHENNPETRO** | Oil Gas & Consumable Fuels | ₹1128.3 | 142 | 2177 | ₹357.60 Cr |
| 3 | **CEMPRO** | Construction | ₹815.2 | 139 | 2128 | ₹62.23 Cr |
| 4 | **SAPPHIRE** | Consumer Services | ₹205.6 | 120 | 1834 | ₹47.00 Cr |
| 5 | **HSCL** | Chemicals | ₹607.8 | 125 | 1833 | ₹308.03 Cr |
| 6 | **IKS** | Information Technology | ₹1654.7 | 117 | 1831 | ₹54.06 Cr |
| 7 | **GMDCLTD** | Metals & Mining | ₹736.1 | 132 | 1820 | ₹415.57 Cr |
| 8 | **COHANCE** | Healthcare | ₹483.2 | 119 | 1807 | ₹187.18 Cr |
| 9 | **GESHIP** | Services | ₹1576.8 | 123 | 1780 | ₹96.61 Cr |
| 10 | **DEVYANI** | Consumer Services | ₹124.7 | 109 | 1654 | ₹60.08 Cr |
| 11 | **IIFL** | Financial Services | ₹458.9 | 101 | 1606 | ₹100.57 Cr |
| 12 | **BLUEJET** | Healthcare | ₹444.6 | 101 | 1490 | ₹21.88 Cr |
| 13 | **ACUTAAS** | Healthcare | ₹2594.1 | 113 | 1468 | ₹136.14 Cr |
| 14 | **RKFORGE** | Automobile and Auto Components | ₹598.3 | 102 | 1457 | ₹34.34 Cr |
| 15 | **ELECON** | Capital Goods | ₹506.6 | 110 | 1388 | ₹39.88 Cr |
| 16 | **CONCORDBIO** | Healthcare | ₹1142.0 | 94 | 1385 | ₹28.97 Cr |
| 17 | **ERIS** | Healthcare | ₹1322.9 | 88 | 1353 | ₹14.53 Cr |
| 18 | **RBLBANK** | Financial Services | ₹336.6 | 88 | 1306 | ₹185.64 Cr |
| 19 | **ZYDUSWELL** | Fast Moving Consumer Goods | ₹508.6 | 108 | 1257 | ₹184.98 Cr |
| 20 | **AFCONS** | Construction | ₹339.7 | 102 | 1256 | ₹56.39 Cr |
| 21 | **INTELLECT** | Information Technology | ₹745.4 | 80 | 1097 | ₹36.59 Cr |
| 22 | **GODFRYPHLP** | Fast Moving Consumer Goods | ₹2251.0 | 83 | 1084 | ₹225.18 Cr |
| 23 | **SCI** | Services | ₹304.5 | 105 | 1082 | ₹269.96 Cr |
| 24 | **RRKABEL** | Capital Goods | ₹1571.0 | 82 | 1063 | ₹46.86 Cr |
| 25 | **APARINDS** | Capital Goods | ₹12331.0 | 97 | 1053 | ₹141.76 Cr |

## Sample pattern signatures (top 5 stocks)

### HFCL — Telecommunication

- **n_fires:** 141  ·  **score:** 2196
- **Sample of patterns that fired** (showing 5 of 141):

  - target=hit_15pc_20d, OOS lift=43.0pp
    rule: `roc_60<=70.25 AND weekly_close_loc>0.61 AND weekly_close_vs_sma20>36.75 AND weekly_range_pct>9.62`
  - target=hit_10pc_20d, OOS lift=39.43pp
    rule: `roc_60<=69.69 AND weekly_close_loc>0.70 AND weekly_close_vs_sma20>39.28 AND weekly_range_pct>5.14`
  - target=hit_10pc_20d, OOS lift=39.01pp
    rule: `atr_5_vs_20<=1.58 AND weekly_close_loc>0.49 AND weekly_range_pct>13.88`
  - target=hit_15pc_20d, OOS lift=38.86pp
    rule: `atr_20_pct>4.34 AND weekly_close_loc>0.77 AND weekly_range_pct>14.03`
  - target=hit_10pc_20d, OOS lift=34.3pp
    rule: `atr_20_pct>2.23 AND weekly_close_loc>0.55 AND weekly_range_pct>13.30`

### CHENNPETRO — Oil Gas & Consumable Fuels

- **n_fires:** 142  ·  **score:** 2177
- **Sample of patterns that fired** (showing 5 of 142):

  - target=hit_15pc_20d, OOS lift=43.1pp
    rule: `roc_5<=11.75 AND weekly_close_loc>0.54 AND weekly_range_pct>14.98`
  - target=hit_10pc_20d, OOS lift=39.01pp
    rule: `atr_5_vs_20<=1.58 AND weekly_close_loc>0.49 AND weekly_range_pct>13.88`
  - target=hit_15pc_20d, OOS lift=38.86pp
    rule: `atr_20_pct>4.34 AND weekly_close_loc>0.77 AND weekly_range_pct>14.03`
  - target=hit_10pc_20d, OOS lift=34.3pp
    rule: `atr_20_pct>2.23 AND weekly_close_loc>0.55 AND weekly_range_pct>13.30`
  - target=hit_15pc_20d, OOS lift=32.62pp
    rule: `atr_20_pct>3.43 AND roc_5<=7.12 AND weekly_close_loc>0.61 AND weekly_range_pct>11.72`

### CEMPRO — Construction

- **n_fires:** 139  ·  **score:** 2128
- **Sample of patterns that fired** (showing 5 of 139):

  - target=hit_10pc_20d, OOS lift=39.01pp
    rule: `atr_5_vs_20<=1.58 AND weekly_close_loc>0.49 AND weekly_range_pct>13.88`
  - target=hit_10pc_20d, OOS lift=34.3pp
    rule: `atr_20_pct>2.23 AND weekly_close_loc>0.55 AND weekly_range_pct>13.30`
  - target=hit_10pc_20d, OOS lift=31.92pp
    rule: `roc_60<=31.97 AND weekly_close_loc>0.59 AND weekly_close_vs_sma20>8.87 AND weekly_range_pct>9.47`
  - target=hit_10pc_20d, OOS lift=30.52pp
    rule: `dist_sma_20>9.12 AND weekly_close_loc>0.48 AND weekly_range_pct>16.49`
  - target=hit_10pc_20d, OOS lift=29.93pp
    rule: `atr_20_pct>2.57 AND dist_high_252>-52.57 AND weekly_close_loc>0.47 AND weekly_range_pct>12.32`

### SAPPHIRE — Consumer Services

- **n_fires:** 120  ·  **score:** 1834
- **Sample of patterns that fired** (showing 5 of 120):

  - target=hit_10pc_20d, OOS lift=39.01pp
    rule: `atr_5_vs_20<=1.58 AND weekly_close_loc>0.49 AND weekly_range_pct>13.88`
  - target=hit_15pc_20d, OOS lift=38.86pp
    rule: `atr_20_pct>4.34 AND weekly_close_loc>0.77 AND weekly_range_pct>14.03`
  - target=hit_10pc_20d, OOS lift=34.3pp
    rule: `atr_20_pct>2.23 AND weekly_close_loc>0.55 AND weekly_range_pct>13.30`
  - target=hit_10pc_20d, OOS lift=30.52pp
    rule: `dist_sma_20>9.12 AND weekly_close_loc>0.48 AND weekly_range_pct>16.49`
  - target=hit_10pc_20d, OOS lift=29.93pp
    rule: `atr_20_pct>2.57 AND dist_high_252>-52.57 AND weekly_close_loc>0.47 AND weekly_range_pct>12.32`

### HSCL — Chemicals

- **n_fires:** 125  ·  **score:** 1833
- **Sample of patterns that fired** (showing 5 of 125):

  - target=hit_10pc_20d, OOS lift=31.92pp
    rule: `roc_60<=31.97 AND weekly_close_loc>0.59 AND weekly_close_vs_sma20>8.87 AND weekly_range_pct>9.47`
  - target=hit_10pc_20d, OOS lift=29.93pp
    rule: `atr_20_pct>2.57 AND dist_high_252>-52.57 AND weekly_close_loc>0.47 AND weekly_range_pct>12.32`
  - target=hit_10pc_20d, OOS lift=28.43pp
    rule: `rs_sector_20d>-6.57 AND weekly_close_loc>0.55 AND weekly_close_vs_sma20>9.15 AND weekly_range_pct>9.62`
  - target=hit_10pc_20d, OOS lift=27.96pp
    rule: `atr_20_pct>3.31 AND dist_sma_200>-0.92 AND weekly_close_loc>0.78 AND weekly_close_vs_sma20>7.63`
  - target=hit_10pc_20d, OOS lift=27.69pp
    rule: `atr_20_pct>2.56 AND roc_20>29.31 AND weekly_close_loc>0.71 AND weekly_range_pct>7.89`

## Execution checklist

Next trading day (T+1):
- Place market-on-open buy orders for the symbols above (₹1L per name)
- Hold for 20 trading days OR until trailing-stop triggered (10-day low after +10% HW)
- Initial stop: -7% from avg entry (intraday low)
- If you see a gap-down >7% on entry day, accept the loss (no chasing)
- Skip a candidate if liquidity (60d avg) < ₹5 Cr/day
- Skip if the stock has any corp action ±5 days from today
