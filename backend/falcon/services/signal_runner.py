"""Signal runner — production-runtime version.

Vectorized rule evaluation over today's feature snapshot. Persists the top-N
picks to falcon_signals_live and queues a notification.

This is what the daily_signals cron job calls.
"""
import json
import sqlite3
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np

from ..config import FALCON_VERSION, MIN_FIRES_DEFAULT, TOP_N_DEFAULT
from ..db import falcon_conn
from .pattern_loader import load_promoted_patterns


# Feature column order — must match falcon_features schema.
FEATURE_COLS = [
    "range_pct","close_loc","gap_pct","body_pct","upper_wick_pct","lower_wick_pct",
    "dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20","slope_sma_50",
    "rsi_14","roc_5","roc_20","roc_60",
    "vol_vs_20d","vol_5d_vs_20d","n_sub_75v_7d","n_sub_75v_20d",
    "atr_20_pct","atr_5_vs_20","n_sub_2_5_range_7d","n_sub_3_range_7d",
    "n_higher_lows_5d","n_higher_highs_5d",
    "dist_high_10","dist_high_20","dist_high_60","dist_high_120","dist_high_252",
    "weekly_close_vs_sma20","weekly_breakout_20w","weekly_range_pct","weekly_close_loc",
    "rs_sector_20d","rs_sector_60d","rs_market_20d","rs_market_60d",
]
FEATURE_IDX = {c: i for i, c in enumerate(FEATURE_COLS)}


def _rule_mask(rule: List[Tuple[str, str, float]], X: np.ndarray) -> np.ndarray:
    mask = np.ones(X.shape[0], dtype=bool)
    for f, op, th in rule:
        idx = FEATURE_IDX.get(f)
        if idx is None: return np.zeros(X.shape[0], dtype=bool)
        col = X[:, idx]
        if op == "<=": mask &= (col <= th) & ~np.isnan(col)
        else:          mask &= (col > th)  & ~np.isnan(col)
    return mask


def generate_signals_for_date(
    signal_date: Optional[str] = None,
    top_n: int = TOP_N_DEFAULT,
    min_fires: int = MIN_FIRES_DEFAULT,
) -> Dict:
    """Generate signals for a specific date (defaults to latest in falcon_features)
    and persist top-N to falcon_signals_live.

    Returns summary dict for cron logging."""
    with falcon_conn() as con:
        if signal_date is None:
            row = con.execute(
                "SELECT MAX(trade_date) FROM falcon_features"
            ).fetchone()
            signal_date = row[0]
        if not signal_date:
            return {"error": "no feature data available", "n_picks": 0}

        signal_year = int(signal_date[:4])

        # Load latest features for the signal date
        feat_select = ", ".join(FEATURE_COLS)
        rows = con.execute(f"""
            SELECT symbol, {feat_select} FROM falcon_features
            WHERE trade_date = ?
        """, (signal_date,)).fetchall()
        if not rows:
            return {"error": f"no features for {signal_date}", "n_picks": 0}

        # Sector + close + liquidity in one shot
        sectors = {r["symbol"]: r["sector"] for r in
                    con.execute("SELECT symbol, sector FROM falcon_sectors")}
        closes = {r["symbol"]: r["close"] for r in con.execute(
            "SELECT symbol, close FROM ohlc_daily WHERE trade_date = ?", (signal_date,))}
        liq = {r[0]: r[1] for r in con.execute(f"""
            SELECT symbol, AVG(close*volume) FROM ohlc_daily
            WHERE trade_date BETWEEN date(?, '-60 days') AND ?
            GROUP BY symbol""", (signal_date, signal_date)).fetchall()}

    # Load patterns (V7.1 default — drops drawdown_bounce)
    patterns = load_promoted_patterns()
    eligible = [p for p in patterns if int(p["mined_year"]) < signal_year]
    if not eligible:
        return {"error": f"no patterns mined before {signal_year}", "n_picks": 0}

    # Build today's feature matrix
    syms = [r["symbol"] for r in rows]
    n_today = len(syms)
    X = np.full((n_today, len(FEATURE_COLS)), np.nan, dtype=np.float64)
    for i, r in enumerate(rows):
        X[i] = [r[c] if r[c] is not None else np.nan for c in FEATURE_COLS]

    # Evaluate each pattern; aggregate fire count + score per stock
    fire_count = np.zeros(n_today, dtype=np.int32)
    score = np.zeros(n_today, dtype=np.float64)
    fired_per_stock: List[List[Dict]] = [[] for _ in range(n_today)]
    for p in eligible:
        m = _rule_mask(p["rule"], X)
        if not m.any(): continue
        fire_count += m.astype(np.int32)
        score += m.astype(np.float64) * p["oos_lift"]
        for i in np.where(m)[0]:
            if len(fired_per_stock[i]) < 5:
                fired_per_stock[i].append({
                    "pattern_id": int(p["pattern_id"]),
                    "target":     p["target"],
                    "oos_lift":   round(float(p["oos_lift"]), 2),
                    "rule":       " AND ".join(f"{f}{op}{th:.2f}" for f, op, th in p["rule"]),
                })

    # Build candidate list
    cands = []
    for i in range(n_today):
        if fire_count[i] < min_fires: continue
        cands.append({
            "symbol":            syms[i],
            "sector":            sectors.get(syms[i]),
            "n_fires":           int(fire_count[i]),
            "score":             float(score[i]),
            "close_at_signal":   closes.get(syms[i]),
            "avg_value_60d":     liq.get(syms[i]),
            "fired_pattern_ids": [s["pattern_id"] for s in fired_per_stock[i]],
            "sample_rules":      fired_per_stock[i],
        })
    cands.sort(key=lambda c: -c["score"])
    top = cands[:top_n]

    # Compute target entry date = next trading day.
    # Primary: query ohlc_daily for the next existing bar (handles holidays).
    # Fallback: tomorrow's OHLC bar doesn't exist yet at signal-gen time (we
    # emit signals at 16:35 IST EOD, before tomorrow's bar lands at 15:30 IST+1d),
    # so the SELECT MIN returns NULL — silently dropping entry_date breaks the
    # pre-market deployer's `WHERE entry_date = ?` lookup → NO_SIGNALS_FOR_DATE.
    # Compute next weekday in Python as the fallback (holiday-unaware but
    # correct 4 days out of 5).
    from datetime import datetime as _dt, timedelta as _td
    with falcon_conn() as con:
        next_row = con.execute(
            "SELECT MIN(trade_date) FROM ohlc_daily WHERE trade_date > ?", (signal_date,)
        ).fetchone()
        entry_date = next_row[0] if next_row and next_row[0] else None
        if entry_date is None:
            sd_obj = _dt.strptime(signal_date, '%Y-%m-%d').date()
            ed_obj = sd_obj + _td(days=1)
            while ed_obj.weekday() >= 5:           # Sat=5, Sun=6
                ed_obj += _td(days=1)
            entry_date = ed_obj.isoformat()

        # Persist
        con.execute("DELETE FROM falcon_signals_live WHERE signal_date = ? AND engine_version = ?",
                     (signal_date, FALCON_VERSION))
        for rank, c in enumerate(top, 1):
            con.execute("""
                INSERT INTO falcon_signals_live
                  (signal_date, entry_date, rank, symbol, sector, n_fires, score,
                   close_at_signal, avg_value_60d, fired_pattern_ids, sample_rules,
                   engine_version)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (signal_date, entry_date, rank, c["symbol"], c["sector"],
                   c["n_fires"], c["score"], c["close_at_signal"], c["avg_value_60d"],
                   json.dumps(c["fired_pattern_ids"]),
                   json.dumps(c["sample_rules"]),
                   FALCON_VERSION))
        con.commit()

    return {
        "signal_date":     signal_date,
        "entry_date":      entry_date,
        "engine_version":  FALCON_VERSION,
        "n_eligible_patterns": len(eligible),
        "n_stocks_evaluated":  n_today,
        "n_qualifying":    len(cands),
        "n_picks":         len(top),
        "top":             top,
    }
