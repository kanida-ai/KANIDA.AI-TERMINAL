# Kanida Universe Engine — V1

Universe-level systematic long-only signal engine for NSE cash equity.
Built on lessons from the main Quant Intelligence Engine audit.

## What's different from the main engine

| | Main engine (audited) | This engine |
|---|---|---|
| Pattern source | Mined atom-combinations per stock | 100 hand-coded long technical strategies |
| Pattern fitting | Per-stock | **Universe-level** (pooled across 200 stocks) |
| Train/test | Walk-forward (after fixes) | Walk-forward built in from day 1 |
| Cost in efficacy | Reported separately | **Baked into "blessed" qualification** |
| Direction | Long + short | **Long only** (cash equity) |
| Data source | Zerodha Kite | Zerodha Kite |
| Universe | Hard-coded F&O list | **Index-membership CSV (Nifty 50/100/200/500)** |
| Entry modes | Blind only | **Blind + Smart** (intraday filter) |

## Two pipelines

**Pipeline A — Build (run monthly):**
Scans 100 strategies × full universe × 18-month rolling train window with monthly slide. Identifies "blessed" strategies that have universe-level positive expectancy after costs. Identifies "Avoid" stocks that consistently lose. Outputs: `blessed_strategies.json`, `avoid_stocks.json`, `WALKFWD_REPORT.md`.

**Pipeline B — Live (run daily after close):**
Uses Pipeline A's outputs. Scans today's bar across the universe. Logs tomorrow's signals to `live_signals` table.

## Folder layout

```
universe_engine/
├── README.md              ← this file
├── config/
│   ├── .env               ← Kite credentials (KITE_API_KEY, etc.)
│   ├── universe_master.csv  ← editable: 200 NSE stocks + index flags + sectors
│   └── strategies_config.json ← thresholds, params
├── data/db/
│   └── kanida_universe.db ← new DB, schema in init_db.py
├── engine/
│   ├── strategies.py      ← 100 long-only patterns + helpers
│   ├── simulator.py       ← TP/SL/day-12 cap, cost-adjusted, blind + smart entry
│   ├── regime.py          ← NIFTY 60d vol bucket
│   ├── walkforward.py     ← Pipeline A driver (multi-worker)
│   └── signal_engine.py   ← Pipeline B driver
├── scripts/
│   ├── setup.py           ← init DB + bootstrap OHLC from main engine
│   ├── run_smoke_test.py  ← single-month sanity check (~5 min)
│   ├── run_pipeline_a.py  ← full walk-forward (Pipeline A, ~1.5 hr)
│   └── run_pipeline_b.py  ← daily live signals
└── reports/               ← report outputs land here
```

## V1 trading rules (locked)

- **Universe:** stocks in selected index (default Nifty 200) AND active AND not in Avoid list AND ADV ≥ ₹100 Cr
- **Signal:** ≥1 blessed strategy fires on today's closed daily bar
- **Regime gate:** skip if NIFTY 60d realised vol is in mid-vol band (10.7%–13.4%)
- **Entry:** Blind = market BUY at 9:15. Smart = wait until 9:30, enter only if price ≥ 9:15 open AND price not >+3% above 9:15 open AND volume confirms (otherwise skip)
- **Stop-loss:** 1.5 × ATR_14 below entry, capped at 8%
- **Target:** 1.5 × stop distance above entry
- **Time exit:** day 12 hard exit
- **Position size:** risk 1% of capital per trade
- **Max concurrent:** 10 positions
- **Round-trip cost:** 30 bps applied at simulation and reporting

## Run sequence

```bash
# 1. Init DB + bootstrap OHLC from main engine (one-time)
python scripts/setup.py

# 2. Smoke test (single month, ~5 min)
python scripts/run_smoke_test.py --month 2026-03

# 3. Full walk-forward (~1.5 hr)
python scripts/run_pipeline_a.py --start 2021-08 --end 2026-04

# 4. Daily live signals
python scripts/run_pipeline_b.py
```
