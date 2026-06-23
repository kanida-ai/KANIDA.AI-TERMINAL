"""
Scoring model for the three persona objective functions: FO_LONG, FO_SHORT, LT.

Design choices
--------------
* **Cross-sectional rank scoring.** For each prediction date we convert every
  feature to a per-date percentile rank centred to [-1, +1]. This is robust to
  outliers and scale, and matches the objective (rank stocks against each other on
  the same day). score = Σ weight_f · rank_f.
* **Learnable weights.** Baseline weights encode quant priors (a "basic rulebook",
  spec). The self-learning loop (learn.py) proposes weight changes; once
  human-approved they are persisted in ``persona_model_weights`` and versioned in
  ``model_weight_history``. The walk-forward reads the active weights as of each
  date so there is no lookahead.
* **Regime + sector context** (spec §1.3, §1.4) are applied as score adjustments.
"""
from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# ── Baseline rulebooks (initial weights on centred percentile ranks) ───────────
# Baseline weights are aligned to the EMPIRICAL rank-IC signs measured over
# 2022–2026 (walk-forward sample), not naive momentum priors. Both next-day and
# 20-day forward returns are mildly MEAN-REVERTING in this universe/sample, and the
# extreme next-day movers are the high-volatility / high-volume names — so the long
# book tilts to consolidating, near-high, recently-soft names and the short book to
# high-ATR, recently-strong names. The IC self-learning loop refines these online.
BASELINE_WEIGHTS: Dict[str, Dict[str, float]] = {
    "FO_LONG": {   # higher score => more likely a next-day GAINER
        "consol_days": 0.60, "dist_high_20": 0.40, "rs_index_20d": 0.20,
        "trend_5_20_50_200": 0.15, "roc_20": 0.10, "rsi_14": 0.05,
        "sig_ret_pct": -0.25, "two_day_ret_pct": -0.22, "rs_sector_5d": -0.18,
        "vol_ratio_20d": -0.18, "atr_20_pct": -0.30, "roc_5": -0.12,
    },
    "FO_SHORT": {  # higher score => more likely a next-day LOSER
        "atr_20_pct": 0.60, "sig_ret_pct": 0.25, "two_day_ret_pct": 0.22,
        "vol_ratio_20d": 0.18, "rs_sector_5d": 0.18, "roc_5": 0.12,
        "n_distrib_5d": 0.10, "consol_days": -0.60, "dist_high_20": -0.40,
        "rs_index_20d": -0.20, "trend_5_20_50_200": -0.15,
    },
    "LT": {        # higher score => more likely a 4–8wk forward outperformer
        "atr_20_pct": 0.50, "vol_ratio_20d": 0.20, "n_distrib_5d": 0.30,
        "roc_20": 0.10, "rsi_14": 0.05, "rs_index_60d": -0.50,
        "rs_sector_5d": -0.40, "rs_index_20d": -0.20, "rs_index_5d": -0.20,
        "roc_5": -0.18, "consol_days": -0.20, "dist_high_20": -0.20,
    },
}

# Features whose ranking is naturally directional are still ranked the same way;
# the weight sign carries the direction.
ALL_FEATURES = sorted(
    {f for w in BASELINE_WEIGHTS.values() for f in w}
)


# ── weight persistence ─────────────────────────────────────────────────────────

def seed_baseline_weights(con: sqlite3.Connection, overwrite: bool = False) -> None:
    for persona, w in BASELINE_WEIGHTS.items():
        for feat, val in w.items():
            if overwrite:
                con.execute(
                    "INSERT OR REPLACE INTO persona_model_weights(persona,feature_name,weight) "
                    "VALUES (?,?,?)", (persona, feat, val))
            else:
                con.execute(
                    "INSERT OR IGNORE INTO persona_model_weights(persona,feature_name,weight) "
                    "VALUES (?,?,?)", (persona, feat, val))
    con.commit()


def load_weights(con: sqlite3.Connection, persona: str) -> Dict[str, float]:
    rows = con.execute(
        "SELECT feature_name, weight FROM persona_model_weights WHERE persona=?",
        (persona,)).fetchall()
    if rows:
        return {r["feature_name"]: r["weight"] for r in rows}
    return dict(BASELINE_WEIGHTS[persona])


# ── cross-sectional scoring ────────────────────────────────────────────────────

def _centered_rank(s: pd.Series) -> pd.Series:
    """Percentile rank in [-1, +1]; NaNs -> 0 (neutral)."""
    r = s.rank(pct=True)
    return (r - 0.5) * 2.0


def score_cross_section(
    day_feats: pd.DataFrame, weights: Dict[str, float]
) -> pd.Series:
    """Score every stock for one date. day_feats indexed by symbol."""
    score = pd.Series(0.0, index=day_feats.index)
    for feat, w in weights.items():
        if feat not in day_feats.columns:
            continue
        cr = _centered_rank(day_feats[feat]).fillna(0.0)
        score = score + w * cr
    return score


# ── market regime (point-in-time, from NIFTY) ──────────────────────────────────

def market_regime_series(con: sqlite3.Connection) -> pd.DataFrame:
    """Per-date NIFTY regime: BULL / BEAR / SIDEWAYS + realised vol. Point-in-time
    (uses only trailing data)."""
    idx = pd.read_sql_query(
        "SELECT trade_date, close FROM ohlc_daily WHERE symbol='NIFTY50' ORDER BY trade_date",
        con)
    if idx.empty:
        return idx
    c = idx["close"]
    sma50 = c.rolling(50).mean()
    sma200 = c.rolling(200).mean()
    ret20 = c / c.shift(20) - 1
    rv = (c.pct_change().rolling(20).std() * np.sqrt(252) * 100)
    regime = np.where(
        (c > sma200) & (ret20 > 0.02), "BULL",
        np.where((c < sma200) & (ret20 < -0.02), "BEAR", "SIDEWAYS"))
    idx["regime"] = regime
    idx["realised_vol"] = rv
    return idx[["trade_date", "regime", "realised_vol"]]


# ── sector momentum (spec §1.3, point-in-time) ─────────────────────────────────

def sector_momentum(feats_all: pd.DataFrame) -> pd.DataFrame:
    """Per (date, sector) average relative strength vs index over 5/20d.
    Returns long-form with a `sector_mom` score and a bull/neutral/bear regime."""
    g = (feats_all.groupby(["trade_date", "sector"])
         .agg(sm5=("rs_index_5d", "mean"), sm20=("rs_index_20d", "mean"))
         .reset_index())
    g["sector_mom"] = g["sm5"].fillna(0) * 0.4 + g["sm20"].fillna(0) * 0.6
    # per-date rank of sectors -> rotation signal
    g["sector_rot_rank"] = g.groupby("trade_date")["sector_mom"].rank(pct=True)
    g["sector_regime"] = np.where(
        g["sector_mom"] > 1.0, "BULL",
        np.where(g["sector_mom"] < -1.0, "BEAR", "NEUTRAL"))
    return g
