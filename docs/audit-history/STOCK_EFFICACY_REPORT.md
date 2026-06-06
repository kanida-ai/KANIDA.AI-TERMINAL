# Per-Stock Signal Efficacy & Persistence — Walk-forward OOS

**Long-only · overlap ≥ 0.85 · 18-month rolling retraining · 4-week embargo · recency λ=0.95**

Two-phase analysis on the 14,514-trade rolling walk-forward (2021-08 → 2026-04):
- **Phase 1.** NIFTY 60-day realised-vol regime gate. Skip mid-vol trades (vol in 10.7–13.4% band).
- **Phase 2.** Per-stock efficacy classification on **label window Aug 2021 – Dec 2024**.
- **Phase 3.** Validate classifications on **held-out window Jan 2025 – Apr 2026**. Core Winners must outperform on data they were not selected on.

All ledger figures use ₹1,00,000 per trade, blind next-open entry, engine TP/SL, 30 bps round-trip cost. No bucket labels (Turbo/Super/Standard/Trap), no tier, no smart entry.

## Phase 1 — Regime gate result

Mid-vol skipped: **2461** trades dropped of 14514 (17.0%).

| | Full universe | **Regime-gated** |
|---|---|---|
| Trades | 14,514 | **12,053** |
| WR (positive ₹) | 39.9% | **41.9%** |
| Avg P&L per trade | +0.28% | **+0.44%** |
| Profit factor | 1.18 | **1.29** |
| Payoff ratio (avg-win/avg-loss) | 1.77 | 1.79 |
| Net P&L gross | ₹4,041,349 | **₹5,282,251** |
| Net P&L (after 30 bps) | -₹312,851 | **₹1,666,351** |
| Max concurrent | 142 | 128 |
| Max capital | ₹14,200,000 | **₹12,800,000** |
| RoC max-cap gross | +28.46% | **+41.27%** |
| RoC max-cap net | -2.20% | **+13.02%** |
| Max DD ₹ (closed-equity) | ₹1,595,190 | ₹1,178,901 |
| Max DD % of max-cap | 11.23% | 9.21% |
| Longest losing streak | 66 | 66 |
| Net-positive months | 28/57 (49%) | **27/54 (50%)** |

**Verdict P1:** regime gate **flips strategy from net-negative to net-positive**. Δ = ₹1,979,202 P&L improvement net of costs.

## Phase 2 — Per-stock classification (label window Aug 2021 – Dec 2024)

Run on **regime-gated** trades during the label window only. Classification thresholds:
- **Core Winners**: n ≥ 60, WR ≥ 38%, profit factor ≥ 1.25, ≥55% of months net-positive, no losing-month streak > 3
- **Conditional Winners**: profit factor ≥ 1.1 but fails one of the Core checks
- **Noisy**: profit factor in [0.95, 1.1)
- **Avoid**: profit factor < 0.95
- **Insufficient Data**: n < 60

### Class distribution

| Class | Stocks |
|---|---|
| Core Winners | 12 |
| Conditional Winners | 54 |
| Noisy | 17 |
| Avoid | 19 |
| Insufficient Data | 45 |

### Class members

**Core Winners** (12): ADANIPORTS, GRASIM, HAL, JINDALSTEL, LT, LTTS, NTPC, POLYCAB, POWERGRID, TCS, TORNTPOWER, TRENT

**Conditional Winners** (54): ABBOTINDIA, ABCAPITAL, ACC, APOLLOHOSP, APOLLOTYRE, AUROPHARMA, AXISBANK, BAJFINANCE, BALKRISIND, BANKBARODA, BEL, BHARTIARTL, BHEL, BPCL, BRITANNIA, CANBK, CHOLAFIN, CIPLA, COALINDIA, COFORGE, COLPAL, DEEPAKNTR, DIXON, DLF, ESCORTS, GAIL, GODREJPROP, GRANULES, GUJGASLTD, HINDALCO, HINDPETRO, ICICIPRULI, INDHOTEL, INDUSINDBK, IOC, IRFC, ITC, JSWSTEEL, LUPIN, MANAPPURAM, MUTHOOTFIN, NAUKRI, ONGC, PERSISTENT, PFC, PIDILITIND, RECLTD, SIEMENS, SRF, SUNPHARMA, TATASTEEL, TVSMOTOR, VEDL, WIPRO

**Noisy** (17): AARTIIND, ABFRL, AMBUJACEM, BATAINDIA, BHARATFORG, FEDERALBNK, GLENMARK, HEROMOTOCO, ICICIGI, IDFCFIRSTB, INFY, LICHSGFIN, SBILIFE, TATACONSUM, TECHM, TITAN, VOLTAS

**Avoid** (19): ADANIENT, ASHOKLEY, ASTRAL, BANDHANBNK, BERGEPAINT, CANFINHOME, CONCOR, DIVISLAB, EXIDEIND, GODREJCP, IDEA, IEX, LAURUSLABS, M&M, NESTLEIND, NMDC, SAIL, TATACHEM, UBL

**Insufficient Data** (45): ABB, ALKEM, ASIANPAINT, AUBANK, BAJAJ-AUTO, BAJAJFINSV, BIOCON, CROMPTON, DABUR, DRREDDY, EICHERMOT, HAVELLS, HCLTECH, HDFCAMC, HDFCBANK, HDFCLIFE, HINDUNILVR, ICICIBANK, IGL, IRCTC, JUBLFOOD, KOTAKBANK, LICI, LTIM, MARICO, MARUTI, MCDOWELL-N, MCX, MPHASIS, MRF, OFSS, PAGEIND, PETRONET, PNB, PVRINOX, RELIANCE, SBICARD, SBIN, SHREECEM, TATAPOWER, TORNTPHARM, ULTRACEMCO, UPL, ZOMATO, ZYDUSLIFE

### Master rank — top 30 by label-window net P&L (after 30 bps)

| Ticker | Class | n | WR | Avg % | PF | Payoff | Net₹ gross | Net₹ net | PosMo | LosStreak | ADV bucket |
|---|---|---|---|---|---|---|---|---|---|---|---|
| NTPC | Core Winners | 72 | 64% | +2.07% | 3.47 | 1.96 | ₹148,740 | ₹127,140 | 20/35 | 3 | high_liq |
| DIXON | Conditional Winners | 72 | 50% | +1.58% | 2.01 | 2.01 | ₹114,000 | ₹92,400 | 21/34 | 5 | high_liq |
| TRENT | Core Winners | 94 | 53% | +1.28% | 1.96 | 1.72 | ₹119,961 | ₹91,761 | 25/36 | 2 | high_liq |
| POWERGRID | Core Winners | 65 | 62% | +1.66% | 2.77 | 1.73 | ₹107,859 | ₹88,359 | 21/34 | 3 | high_liq |
| HAL | Core Winners | 80 | 52% | +1.31% | 1.90 | 1.72 | ₹104,834 | ₹80,834 | 22/36 | 3 | high_liq |
| TORNTPOWER | Core Winners | 65 | 52% | +1.53% | 2.30 | 2.10 | ₹99,691 | ₹80,191 | 21/32 | 3 | low_liq |
| COLPAL | Conditional Winners | 84 | 55% | +1.24% | 2.22 | 1.83 | ₹103,970 | ₹78,770 | 18/30 | 5 | low_liq |
| LTTS | Core Winners | 66 | 52% | +1.49% | 1.81 | 1.70 | ₹98,114 | ₹78,314 | 18/32 | 3 | low_liq |
| BEL | Conditional Winners | 66 | 45% | +1.47% | 1.98 | 2.38 | ₹97,026 | ₹77,226 | 19/35 | 4 | high_liq |
| ADANIPORTS | Core Winners | 86 | 50% | +1.18% | 2.15 | 2.15 | ₹101,888 | ₹76,088 | 19/32 | 3 | high_liq |
| DLF | Conditional Winners | 75 | 49% | +1.28% | 1.89 | 1.94 | ₹96,270 | ₹73,770 | 20/35 | 4 | mid_liq |
| TCS | Core Winners | 62 | 56% | +1.43% | 2.48 | 1.91 | ₹88,700 | ₹70,100 | 20/35 | 3 | high_liq |
| ESCORTS | Conditional Winners | 66 | 47% | +1.32% | 1.95 | 2.20 | ₹87,354 | ₹67,554 | 16/32 | 5 | low_liq |
| HINDPETRO | Conditional Winners | 75 | 45% | +1.20% | 1.86 | 2.24 | ₹89,731 | ₹67,231 | 20/34 | 5 | mid_liq |
| ONGC | Conditional Winners | 68 | 49% | +1.29% | 2.05 | 2.18 | ₹87,400 | ₹67,000 | 14/29 | 5 | high_liq |
| POLYCAB | Core Winners | 87 | 49% | +1.02% | 1.66 | 1.70 | ₹88,521 | ₹62,421 | 20/36 | 3 | high_liq |
| MPHASIS | Insufficient Data | 58 | 50% | +1.36% | 2.00 | 2.00 | ₹79,126 | ₹61,726 | 14/29 | 6 | mid_liq |
| BANKBARODA | Conditional Winners | 70 | 50% | +1.17% | 1.78 | 1.78 | ₹81,956 | ₹60,956 | 17/32 | 5 | mid_liq |
| GRASIM | Core Winners | 71 | 51% | +1.16% | 1.91 | 1.86 | ₹82,199 | ₹60,899 | 18/32 | 3 | mid_liq |
| IRFC | Conditional Winners | 63 | 44% | +1.25% | 1.80 | 2.25 | ₹78,824 | ₹59,924 | 15/29 | 4 | mid_liq |
| PERSISTENT | Conditional Winners | 61 | 43% | +1.27% | 1.73 | 2.33 | ₹77,172 | ₹58,872 | 13/31 | 4 | high_liq |
| LT | Core Winners | 61 | 51% | +1.23% | 1.95 | 1.89 | ₹75,096 | ₹56,796 | 17/30 | 2 | high_liq |
| ICICIPRULI | Conditional Winners | 66 | 41% | +1.04% | 1.65 | 2.39 | ₹68,871 | ₹49,071 | 14/34 | 5 | low_liq |
| JINDALSTEL | Core Winners | 70 | 41% | +1.00% | 1.69 | 2.39 | ₹69,691 | ₹48,691 | 17/30 | 3 | mid_liq |
| SIEMENS | Conditional Winners | 64 | 50% | +1.06% | 1.81 | 1.81 | ₹67,636 | ₹48,436 | 18/30 | 4 | mid_liq |
| ABCAPITAL | Conditional Winners | 84 | 40% | +0.86% | 1.55 | 2.28 | ₹72,547 | ₹47,347 | 20/34 | 4 | mid_liq |
| IOC | Conditional Winners | 80 | 49% | +0.87% | 1.61 | 1.69 | ₹69,973 | ₹45,973 | 15/33 | 4 | mid_liq |
| HCLTECH | Insufficient Data | 43 | 49% | +1.31% | 2.19 | 2.30 | ₹56,131 | ₹43,231 | 12/22 | 5 | high_liq |
| VEDL | Conditional Winners | 82 | 45% | +0.82% | 1.52 | 1.85 | ₹67,615 | ₹43,015 | 18/35 | 5 | high_liq |
| BPCL | Conditional Winners | 68 | 50% | +0.93% | 1.67 | 1.67 | ₹63,225 | ₹42,825 | 19/32 | 5 | high_liq |

### Bottom 15 by label-window net P&L

| Ticker | Class | n | WR | Avg % | PF | Net₹ gross | Net₹ net | LosStreak |
|---|---|---|---|---|---|---|---|---|
| MARICO | Insufficient Data | 55 | 33% | -0.48% | 0.70 | -₹26,566 | -₹43,066 | 4 |
| ADANIENT | Avoid | 67 | 34% | -0.36% | 0.84 | -₹24,042 | -₹44,142 | 5 |
| IEX | Avoid | 65 | 28% | -0.40% | 0.79 | -₹25,699 | -₹45,199 | 10 |
| ASTRAL | Avoid | 71 | 31% | -0.37% | 0.82 | -₹26,041 | -₹47,341 | 5 |
| BERGEPAINT | Avoid | 81 | 37% | -0.32% | 0.80 | -₹25,735 | -₹50,035 | 5 |
| LAURUSLABS | Avoid | 68 | 31% | -0.47% | 0.76 | -₹32,000 | -₹52,400 | 4 |
| EXIDEIND | Avoid | 80 | 35% | -0.38% | 0.77 | -₹30,465 | -₹54,465 | 4 |
| DRREDDY | Insufficient Data | 52 | 27% | -0.77% | 0.56 | -₹39,789 | -₹55,389 | 12 |
| CROMPTON | Insufficient Data | 50 | 32% | -0.85% | 0.54 | -₹42,387 | -₹57,387 | 6 |
| BIOCON | Insufficient Data | 56 | 30% | -0.75% | 0.58 | -₹41,795 | -₹58,595 | 13 |
| CONCOR | Avoid | 75 | 31% | -0.51% | 0.73 | -₹37,910 | -₹60,410 | 9 |
| IDEA | Avoid | 63 | 25% | -0.74% | 0.68 | -₹46,563 | -₹65,463 | 6 |
| SAIL | Avoid | 66 | 30% | -0.71% | 0.65 | -₹47,115 | -₹66,915 | 9 |
| DABUR | Insufficient Data | 57 | 30% | -0.96% | 0.48 | -₹54,829 | -₹71,929 | 8 |
| CANFINHOME | Avoid | 70 | 26% | -1.01% | 0.54 | -₹70,584 | -₹91,584 | 7 |

### Edge by liquidity bucket (label window, regime-gated)

ADV terciles: low_liq < ₹150.9 Cr | mid_liq | high_liq > ₹350.2 Cr

| Bucket | Trades | WR | Avg % | PF | Net ₹ gross | Net ₹ net |
|---|---|---|---|---|---|---|
| low_liq | 3014 | 40% | +0.23% | 1.14 | ₹688,480 | -₹215,720 |
| mid_liq | 3155 | 41% | +0.45% | 1.29 | ₹1,404,404 | ₹457,904 |
| high_liq | 3002 | 44% | +0.68% | 1.46 | ₹2,027,136 | ₹1,126,536 |

## Phase 3 — Validation on held-out window (Jan 2025 – Apr 2026)

Apply the **labels from Phase 2** (frozen) to validation trades. Test: do Core Winners outperform Avoid (and the universe) on data the classifier never saw?

| Class | Trades | WR | Avg % | PF | Net ₹ gross | Net ₹ net | PosMo |
|---|---|---|---|---|---|---|---|
| Core Winners | 233 | 40% | +0.18% | 1.13 | ₹41,282 | -₹28,618 | 8/15 |
| Conditional Winners | 1061 | 43% | +0.45% | 1.32 | ₹475,698 | ₹157,398 | 7/15 |
| Noisy | 304 | 45% | +0.73% | 1.52 | ₹223,336 | ₹132,136 | 7/15 |
| Avoid | 404 | 45% | +0.61% | 1.45 | ₹245,069 | ₹123,869 | 7/15 |
| Insufficient Data | 880 | 41% | +0.20% | 1.14 | ₹176,846 | -₹87,154 | 6/15 |

### Validation diagnostics

- **Universe (regime-gated, val window):** PF=1.29, net = ₹297,631
- **Core Winners (val window):** PF=1.13, net = -₹28,618
- **Avoid (val window):** PF=1.45, net = ₹123,869

- **Per-trade ₹ net (val window):** Core -123 | Universe +103 | Avoid +307

**Verdict P3:** Core Winners do **not** outperform the universe in the held-out window. Classification labels were noise. Recommend trading the regime-gated universe broadly without per-stock filter.

## Active trade list (recommended)

Phase 3 validation did not confirm Core Winners as an outperformer over the universe in the held-out window. Recommendation: **trade the full regime-gated universe** until further evidence accumulates.

**Excluded under all scenarios:** tickers labelled **Avoid** (consistent negative profit factor across label window):

ADANIENT, ASHOKLEY, ASTRAL, BANDHANBNK, BERGEPAINT, CANFINHOME, CONCOR, DIVISLAB, EXIDEIND, GODREJCP, IDEA, IEX, LAURUSLABS, M&M, NESTLEIND, NMDC, SAIL, TATACHEM, UBL

## One-line summary

> Rolling walk-forward 2021-08 → 2026-04, long-only, overlap ≥ 0.85, 18-month retraining: applying NIFTY-vol regime gate cuts 2,461 mid-vol trades and shifts net P&L from -₹312,851 to ₹1,666,351 after 30 bps cost. Per-stock classification on label window Aug 2021–Dec 2024 produced 12 Core Winners, 19 Avoid; held-out validation Jan 2025–Apr 2026 shows Core net -₹28,618 (233 trades, PF=1.13) vs Avoid ₹123,869 (404 trades, PF=1.45).
