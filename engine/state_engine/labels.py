"""Outcome labelling.

This is the ONLY module permitted to look forward in time. Everything it produces
is prefixed `y_` so leakage checks can assert that no `y_` column ever reaches
the feature matrix.

Trading assumption baked in: the signal is generated after the close of day T
(state frozen at 15:31) and the position is entered at the OPEN of day T+1
(09:15). Outcomes are therefore measured from next_open, not from close_T.
That is what makes the backtest executable rather than theoretical.

Honest limitation of daily bars: a daily bar tells you the high and the low but
not which came FIRST. So "reached +1% before -0.5%" is not resolvable from daily
data alone. Three label modes make that explicit rather than hiding it:

  conservative -- hit requires MFE >= target AND MAE never breaching -stop.
                  Understates the true hit rate. Use this as the default.
  optimistic   -- hit requires MFE >= target only. Overstates it.
                  The truth lies between the two; if your edge only exists in
                  'optimistic', you do not have an edge.
  close        -- entry-to-close return >= target. Fully resolvable, no ordering
                  ambiguity, but ignores the intraday path.

If you supply intraday bars, replace `add_labels` with a proper first-touch
resolver; the rest of the engine is unchanged.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import Config


def add_labels(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    d = df.copy()
    g = d.groupby("symbol", sort=False)
    H = cfg.horizon_days

    entry = g["open"].shift(-1)                      # T+1 open

    # forward window T+1 .. T+H, computed by reversing then rolling (never leaks
    # backwards into the feature matrix because results are y_-prefixed)
    def fwd(col: str, how: str) -> pd.Series:
        rev = d.iloc[::-1]
        r = rev.groupby("symbol", sort=False)[col].rolling(H, min_periods=H)
        out = getattr(r, how)().reset_index(level=0, drop=True).iloc[::-1]
        return out.groupby(d["symbol"], sort=False).shift(-1)

    fwd_high = fwd("high", "max")
    fwd_low = fwd("low", "min")
    exit_close = g["close"].shift(-H)

    up_exc = fwd_high / entry - 1.0        # best case for a LONG
    dn_exc = fwd_low / entry - 1.0         # worst case for a LONG
    close_ret = exit_close / entry - 1.0

    d["y_entry"] = entry
    d["y_up_excursion"] = up_exc           # kept unsigned for the baseline profiler
    d["y_dn_excursion"] = dn_exc

    if cfg.side == "short":
        # favourable = price falling. Flip so every downstream module reads
        # "mfe = move in my favour, mae = move against me" regardless of side.
        d["y_mfe"] = -dn_exc
        d["y_mae"] = -up_exc
        d["y_ret"] = -close_ret
    elif cfg.side == "long":
        d["y_mfe"] = up_exc
        d["y_mae"] = dn_exc
        d["y_ret"] = close_ret
    else:
        raise ValueError(f"side must be 'long' or 'short', got {cfg.side!r}")

    if cfg.structure == "hybrid":
        # one barrier only, so there is no same-bar ambiguity: the label is exact
        # even on daily bars. Stopped out at -stop, otherwise out at the close.
        stopped = d["y_mae"] <= -cfg.stop_pct
        pnl = np.where(stopped, -cfg.stop_pct, d["y_ret"]) - cfg.cost_pct
        d["y_pnl"] = np.where(d["y_mfe"].isna(), np.nan, pnl)
        d["y_hit"] = (d["y_pnl"] > 0).astype(float)
        d.loc[d["y_mfe"].isna(), "y_hit"] = np.nan
        d["y_expectancy"] = d["y_pnl"]
        d["y_hit_down"] = np.nan
        return d

    if cfg.label_mode == "conservative":
        hit = (d["y_mfe"] >= cfg.target_pct) & (d["y_mae"] > -cfg.stop_pct)
    elif cfg.label_mode == "optimistic":
        hit = d["y_mfe"] >= cfg.target_pct
    elif cfg.label_mode == "close":
        hit = d["y_ret"] >= cfg.target_pct
    else:
        raise ValueError(f"unknown label_mode {cfg.label_mode!r}")

    d["y_hit"] = hit.astype(float)
    d.loc[d["y_mfe"].isna(), "y_hit"] = np.nan

    # the opposite side's outcome, reported alongside so you can see both
    d["y_hit_down"] = ((d["y_mae"] <= -cfg.target_pct) & (d["y_mfe"] < cfg.stop_pct)).astype(float)
    d.loc[d["y_mae"].isna(), "y_hit_down"] = np.nan

    # expectancy of one trade under the stated rules, used for ranking
    d["y_expectancy"] = np.where(
        d["y_hit"] == 1, cfg.target_pct,
        np.where(d["y_mae"] <= -cfg.stop_pct, -cfg.stop_pct, d["y_ret"])
    ) - cfg.cost_pct
    d.loc[d["y_hit"].isna(), "y_expectancy"] = np.nan

    return d


def label_columns(d: pd.DataFrame) -> list[str]:
    return [c for c in d.columns if c.startswith("y_")]
