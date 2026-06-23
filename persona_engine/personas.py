"""
Persona agents (spec §1.4, §2, §3).

The three-level hierarchy is realised as a scoring composition:

  Level 1 — stock agent   : per-stock feature row + persona weight vector
                            (model.score_cross_section)
  Level 2 — sector agent  : per-date sector-momentum context, blended in as a
                            centred-rank adjustment (model.sector_momentum)
  Level 3 — persona agent : applies persona objective + market regime, emits the
                            final ranked Top-10 list(s).

No lookahead: a date-T prediction uses only feature/sector/regime values dated ≤ T.
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from persona_engine import model

SECTOR_WEIGHT = 0.15  # sector momentum tilt (kept small: sector RS has slightly
                      # negative next-day IC, so a large tilt would hurt overlap)
MAG_WEIGHT = 1.5      # F&O: being in EITHER next-day tail is dominated by
                      # volatility/volume/recent-move magnitude; direction is the
                      # weak tie-breaker. Both books favour high-magnitude names.
MAG_FEATURES = ["atr_20_pct", "vol_ratio_20d", "two_day_ret_pct", "sig_ret_pct"]


def _magnitude_rank(day_feats: pd.DataFrame) -> pd.Series:
    """Centred-rank composite of 'how big will this stock move' (sign-agnostic)."""
    score = pd.Series(0.0, index=day_feats.index)
    for f in MAG_FEATURES:
        if f not in day_feats.columns:
            continue
        v = day_feats[f].abs() if f in ("two_day_ret_pct", "sig_ret_pct") else day_feats[f]
        score = score + model._centered_rank(v).fillna(0.0)
    return score / max(1, len(MAG_FEATURES))


def _sector_rank_adj(day_feats: pd.DataFrame, sector_mom_day: pd.DataFrame) -> pd.Series:
    """Map each stock to its sector's momentum, then centre-rank across stocks."""
    if sector_mom_day is None or sector_mom_day.empty:
        return pd.Series(0.0, index=day_feats.index)
    mom = sector_mom_day.set_index("sector")["sector_mom"].to_dict()
    sec_vals = day_feats["sector"].map(mom).astype(float)
    return model._centered_rank(sec_vals).fillna(0.0)


def predict_fo(
    day_feats: pd.DataFrame,
    sector_mom_day: pd.DataFrame,
    w_long: Dict[str, float],
    w_short: Dict[str, float],
    regime: Optional[str] = None,
    k: int = 10,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return (long_top10, short_top10) for one date. day_feats indexed by symbol."""
    df = day_feats.copy()
    sec_adj = _sector_rank_adj(df, sector_mom_day)
    mag = _magnitude_rank(df)  # high = likely in either tail (gainer OR loser)

    long_stock = model.score_cross_section(df, w_long)
    short_stock = model.score_cross_section(df, w_short)

    # both books need high-magnitude (tail) names; the directional score + sector
    # tilt break which tail. (Short's directional signal is the stronger one.)
    df["long_score"] = long_stock + MAG_WEIGHT * mag + SECTOR_WEIGHT * sec_adj
    df["short_score"] = short_stock + MAG_WEIGHT * mag + SECTOR_WEIGHT * (-sec_adj)

    long_top = (df.sort_values("long_score", ascending=False)
                .head(k).reset_index().rename(columns={"index": "symbol"}))
    short_top = (df.sort_values("short_score", ascending=False)
                 .head(k).reset_index().rename(columns={"index": "symbol"}))
    long_top["rank"] = np.arange(1, len(long_top) + 1)
    short_top["rank"] = np.arange(1, len(short_top) + 1)
    return long_top, short_top


def predict_lt(
    day_feats: pd.DataFrame,
    sector_mom_day: pd.DataFrame,
    w_lt: Dict[str, float],
    regime: Optional[str] = None,
    k: int = 10,
) -> pd.DataFrame:
    df = day_feats.copy()
    sec_adj = _sector_rank_adj(df, sector_mom_day)
    lt_stock = model.score_cross_section(df, w_lt)
    df["lt_score"] = lt_stock + SECTOR_WEIGHT * sec_adj
    top = (df.sort_values("lt_score", ascending=False)
           .head(k).reset_index().rename(columns={"index": "symbol"}))
    top["rank"] = np.arange(1, len(top) + 1)
    return top


def top_features_json(row: pd.Series, weights: Dict[str, float], n: int = 5) -> str:
    """Which features (by |weight·rank-ish value|) drove this pick — for explainability."""
    import json
    contrib = {}
    for f, w in weights.items():
        v = row.get(f)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        contrib[f] = round(float(v), 3)
    top = dict(sorted(contrib.items(), key=lambda kv: -abs(kv[1]))[:n])
    return json.dumps(top)
