"""Falcon V7.1 feature columns + sanity filters.

Ported from `universe_engine/.../sim_sweep.py` so the Power User portal doesn't
depend on a path inside a worktree. These are the 38 columns the engine writes
to `falcon_features` and references in pattern rules.

Keep in sync with the engine's feature pipeline. If the engine adds/removes a
feature, this list MUST be updated; otherwise pattern matching silently fails
(rule references a feature → `feat_vals.get(f)` returns None → `rule_fires`
returns False → the pattern won't fire even if it should).
"""
from __future__ import annotations

from typing import List, Tuple

FEATURE_COLS: List[str] = [
    # OHLC structure
    "range_pct", "close_loc", "gap_pct", "body_pct", "upper_wick_pct", "lower_wick_pct",
    # Moving-average distance + slope
    "dist_sma_20", "dist_sma_50", "dist_sma_200", "slope_sma_20", "slope_sma_50",
    # Momentum
    "rsi_14", "roc_5", "roc_20", "roc_60",
    # Volume
    "vol_vs_20d", "vol_5d_vs_20d", "n_sub_75v_7d", "n_sub_75v_20d",
    # Volatility / compression
    "atr_20_pct", "atr_5_vs_20", "n_sub_2_5_range_7d", "n_sub_3_range_7d",
    # Trend structure
    "n_higher_lows_5d", "n_higher_highs_5d",
    # Distance to N-day high
    "dist_high_10", "dist_high_20", "dist_high_60", "dist_high_120", "dist_high_252",
    # Weekly structure
    "weekly_close_vs_sma20", "weekly_breakout_20w", "weekly_range_pct", "weekly_close_loc",
    # Relative strength
    "rs_sector_20d", "rs_sector_60d", "rs_market_20d", "rs_market_60d",
]

FEATURE_IDX = {c: i for i, c in enumerate(FEATURE_COLS)}


def in_drawdown_bounce(rule: List[Tuple[str, str, float]]) -> bool:
    """True if `rule` is a 'bouncing-from-deep-drawdown' setup we exclude.

    The engine mined some patterns where `dist_high_252 <= -X` (way below
    52-week high) is the dominant condition — these are mean-reversion plays
    on beaten-down stocks. We filter them out of the explainer/live-tier flow
    because the operator's strategy is momentum continuation, not mean-reversion.

    Threshold: dist_high_252 (or _120) ≤ -10% → drawdown-bounce → skip.
    """
    for f, op, th in rule:
        if f in ("dist_high_252", "dist_high_120") and op == "<=" and th < -10:
            return True
    return False
