# Kanida.AI — Persona Expansion Engine

Two **additive** self-learning persona engines layered on the existing Falcon
infrastructure. Nothing here modifies the Falcon Top-10 engine, tier classifier,
tier self-learning loop, the auto-trade path, or the portal frontend.

* **F&O Trader** — next-day **Long Futures Top 10** + **Short Futures Top 10**,
  measured by overlap with the next day's actual top/bottom movers (full
  open→close return) across the F&O universe.
* **Long-Term Investor** — **Top 10** long-only Nifty-500 picks, measured by overlap
  with the actual 4-week / 8-week forward Top-10 gainers.

## Architecture (spec §1)

```
stock agents  →  sector agents  →  persona agents
(per-symbol      (per-sector       (objective-specific
 features)        momentum/regime)  ranking → Top 10)
```

Realised as a scoring composition:
* **Stock agent** — point-in-time feature row per symbol/day (`features.py`,
  persisted to `persona_signal_features`).
* **Sector agent** — per-date sector momentum / rotation blended in as a centred
  rank adjustment (`model.sector_momentum`).
* **Persona agent** — applies the persona weight vector + market regime, emits the
  ranked Top-10 list(s) (`personas.py`).

## Closed self-learning loop (spec §2.7, §4.1)

`PREDICT → MEASURE → ANALYSE → LEARN → APPROVE → DEPLOY → REPORT`, run causally in
`engine.run_walkforward`:

* **PREDICT** with the weights active as of date T (no future data).
* **MEASURE** once T's outcome is known: rank the universe by realised forward
  return → actual Top-10 → overlap / hits / misses.
* **LEARN** every 5 trading days: re-estimate feature weights from a *trailing*
  window only (rank-IC), and mine discrete threshold rules (`learn.py`) with the
  full scaling-discipline record.
* **APPROVE / DEPLOY** — every learned weight vector / rule is written to
  `learning_proposals` (`human_approved = 0`). Live deployment requires human
  approval (constitutional P4/P10). The research walk-forward adopts learned
  weights *within the simulation* to measure self-improvement — it does not touch
  live trading.
* **REPORT** — plain-English daily F&O review + weekly LT review (`review.py`).

## Data reality (audited 2026-06-22)

* **RND DB**: `universe_engine/data/db/kanida_universe.db` (resolved in `db.py`).
* OHLC daily 2016–2026; **2020–2021 thin (~138 symbols)** → 2021 is warm-up only,
  the real walk-forward is **2022→2026** (409–505 symbols).
* Reuses nothing destructively; computes its own features from `ohlc_daily`.
* **Data gaps (honest):** futures **OI** exists only for ~2 months (2026), and
  **PCR / IV are absent entirely**. The spec's OI/PCR/IV features (§2.4/§2.5) are
  therefore **live-only enhancements** and are *not* used in the 2022–2026
  walk-forward — that model runs on price / volume / relative-strength features.
* **F&O membership** is fetched live from Kite at runtime (`universe.py`); the
  walk-forward uses the current membership as a proxy for historical dates
  (point-in-time F&O membership history is not stored) — a documented backtest
  simplification.

## Run

```bash
PY="C:/Users/SPS/Anaconda3/python.exe"   # the only interpreter with sklearn+kiteconnect
export PYTHONPATH=.
$PY -m persona_engine.run init-schema
$PY -m persona_engine.run build-features --start 2021-01-01
$PY -m persona_engine.run walkforward   --start 2022-01-01
$PY -m persona_engine.run learn         --week-ending 2026-06-22
$PY -m persona_engine.run reviews
$PY -m persona_engine.run status
```

## Read-only access for the portal

`access.py` exposes read-only getters (`fo_predictions`, `fo_daily_review`,
`fo_performance`, `lt_predictions`, `lt_performance`, `active_rules`,
`pending_proposals`). No UI is built here.

## Tables (all additive, SQLite)

`fo_daily_predictions`, `fo_prediction_outcomes`, `lt_daily_predictions`,
`lt_prediction_outcomes`, `fo_miss_analysis`, `lt_miss_analysis`,
`learning_proposals`, `model_weight_history`, `persona_model_weights`,
`learning_review_log`, `persona_signal_features`, `fo_daily_review`,
`lt_weekly_review`, `fo_universe_membership`, `persona_run_log`.
