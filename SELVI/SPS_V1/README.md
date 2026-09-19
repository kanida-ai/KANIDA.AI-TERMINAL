# SPS_V1 — Baseline Intraday Profitability Engine

Part of the **SELVI** research suite. Answers, per stock and per side (long/short):

> *"If I mechanically enter at 09:15 every day and hold, what normally happens
> minute-by-minute until EOD?"* → **When to enter? How long to stay? How to manage?**

## Run
```bash
python engine.py            # add PYTHONIOENCODING=utf-8 on Windows consoles
```
Reads `../../db/kanida.db` (`ohlc_1min`). Writes to `./outputs/`.

## Method (leak-free)
- **Entry** = OPEN of the 09:15 one-minute candle (first tradable print; no same-bar leak).
- **Marks** = CLOSE at each forward minute offset; **MAE/MFE** = intrabar high/low.
- **Forward-only**: every statistic is measured *after* entry; nothing future selects days/entries.
- **Costs**: round-trip `RT_COST_PCT` (default 0.10% = STT+exchange+brokerage+slippage), applied to every round trip. Gross **and** net reported.
- **Sizing**: fixed `CAPITAL` per (stock, side); `qty = floor(CAPITAL/entry)`.
- **In-sample** = 2022–2025 (the requested window). **Out-of-sample** = Jan-2026→latest, held out to check the conclusions survive.

## Config (top of `engine.py`)
`STOCKS`, `SIDES`, `CAPITAL=30000`, `RT_COST_PCT=0.0010`, `IS_START/IS_END`, `OOS_START/OOS_END`, `ENTRY_SCAN`, `HOLD_MARKS`.

## Outputs (`outputs/`)
- `report.md` — the human summary (answers the 3 questions).
- `<STOCK>_<SIDE>_summary.json` — full stats (baseline, hold curve, MAE/MFE percentiles, entry scan, OOS).
- `<STOCK>_<SIDE>_drift.csv` — mean/median net return & %positive by minute-offset (in-sample).
- `<STOCK>_<SIDE>.png` — drift curve + cumulative-₹ equity.

## Interpreting
- `net_mean_ret` > 0 **and** OOS sign matches → a real (if small) edge. Otherwise the blind time-entry has **no tradeable edge** — expected for unconditional baselines; a real edge needs *conditioning* (regime/setup/signal), which later SELVI engines add.
