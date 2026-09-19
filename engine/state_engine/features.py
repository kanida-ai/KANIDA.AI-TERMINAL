"""Feature factory.

Design rule (non-negotiable): every value on row t is computable from information
available at the CLOSE of day t. No `.shift(-n)`. No centered windows. No
cross-sectional statistic that includes a future date.

The factory is a grammar, not a list:
      feature = OPERATOR( BASE , WINDOW )
plus optional pairwise interactions between survivors.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import Config

EPS = 1e-12


# --------------------------------------------------------------------------- #
# primitives                                                                    #
# --------------------------------------------------------------------------- #
def add_primitives(df: pd.DataFrame) -> pd.DataFrame:
    """Raw, parameter-light facts about day t. These are the atoms of the grammar."""
    d = df.copy()
    g = d.groupby("symbol", sort=False)

    prev_close = g["close"].shift(1)
    d["ret1"] = d["close"] / prev_close - 1.0
    d["gap"] = d["open"] / prev_close - 1.0
    d["range_pct"] = (d["high"] - d["low"]) / d["close"].replace(0, np.nan)
    d["clv"] = ((d["close"] - d["low"]) / (d["high"] - d["low"]).replace(0, np.nan))
    d["body_pct"] = (d["close"] - d["open"]) / d["open"].replace(0, np.nan)

    tr = pd.concat([
        d["high"] - d["low"],
        (d["high"] - prev_close).abs(),
        (d["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    d["tr_pct"] = tr / d["close"].replace(0, np.nan)

    d["dollar_vol"] = d["close"] * d["volume"]
    # typical-price VWAP proxy (true VWAP needs intraday data; swap it in if you have it)
    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    d["vwap_proxy"] = tp
    d["vwap_dist"] = d["close"] / tp.replace(0, np.nan) - 1.0

    if "mkt_ret" in d.columns:
        d["rel_str"] = d["ret1"] - d["mkt_ret"]
        d["rel_str_sec"] = d["ret1"] - d.get("sec_ret", 0.0)
    else:
        d["rel_str"] = d["ret1"]
        d["rel_str_sec"] = d["ret1"]

    return d


# --------------------------------------------------------------------------- #
# operators (all causal, all grouped by symbol)                                 #
# --------------------------------------------------------------------------- #
def _roll(s: pd.Series, by: pd.Series, w: int, how: str) -> pd.Series:
    r = s.groupby(by, sort=False).rolling(w, min_periods=w)
    return getattr(r, how)().reset_index(level=0, drop=True)


def _slope(s: pd.Series, by: pd.Series, w: int) -> pd.Series:
    """OLS slope of the last w points against time, vectorised via fixed weights.

    Convolution is O(n) per symbol, unlike rolling().apply() which is O(n*w) in Python.
    """
    t = np.arange(w, dtype=float)
    t -= t.mean()
    wts = (t / (t @ t))[::-1]      # np.convolve reverses the kernel

    out = pd.Series(np.nan, index=s.index, dtype=float)
    for _, idx in s.groupby(by, sort=False).groups.items():
        arr = s.loc[idx].to_numpy(dtype=float)
        if len(arr) < w:
            continue
        vals = np.convolve(np.nan_to_num(arr, nan=0.0), wts, mode="valid")
        res = np.full(len(arr), np.nan)
        res[w - 1:] = vals
        # invalidate windows that contained a NaN
        res[pd.Series(arr).rolling(w).count().to_numpy() < w] = np.nan
        out.loc[idx] = res
    return out


def _apply_op(d: pd.DataFrame, base: str, op: str, w: int) -> pd.Series | None:
    if base not in d.columns:
        return None
    s = d[base].astype(float)
    by = d["symbol"]

    if op == "z":
        m = _roll(s, by, w, "mean")
        sd = _roll(s, by, w, "std")
        return (s - m) / (sd + EPS)

    if op == "pctrank":
        return (s.groupby(by, sort=False)
                 .rolling(w, min_periods=w)
                 .rank(pct=True)
                 .reset_index(level=0, drop=True))

    if op == "slope":
        return _slope(s, by, w)

    if op == "ratio":
        m = _roll(s, by, w, "mean")
        return (s - m) / (m.abs() + EPS)

    if op == "accel":
        m_fast = _roll(s, by, max(2, w // 2), "mean")
        m_slow = _roll(s, by, w, "mean")
        return (m_fast - m_slow) / (m_slow.abs() + EPS)

    if op == "distmax":
        mx = _roll(s, by, w, "max")
        return s / (mx + EPS) - 1.0

    if op == "distmin":
        mn = _roll(s, by, w, "min")
        return s / (mn + EPS) - 1.0

    return None


# --------------------------------------------------------------------------- #
# factory                                                                       #
# --------------------------------------------------------------------------- #
def build_features(df: pd.DataFrame, cfg: Config) -> tuple[pd.DataFrame, list[str]]:
    """Expand the grammar into hundreds of candidate features."""
    d = add_primitives(df)
    names: list[str] = []
    new_cols: dict[str, pd.Series] = {}

    for base in cfg.bases:
        for op in cfg.operators:
            for w in cfg.windows:
                # a couple of combinations are degenerate; skip them
                if base in ("ret1", "gap", "rel_str") and op in ("distmax", "distmin"):
                    continue
                col = _apply_op(d, base, op, w)
                if col is None:
                    continue
                name = f"f_{base}__{op}{w}"
                new_cols[name] = col.replace([np.inf, -np.inf], np.nan)
                names.append(name)

    # a few hand-added cross-sectional primitives (still causal)
    if "rel_str_sec" in d.columns:
        new_cols["f_rel_str_sec__raw"] = d["rel_str_sec"]
        names.append("f_rel_str_sec__raw")
    new_cols["f_clv__raw"] = d["clv"]
    names.append("f_clv__raw")

    d = pd.concat([d, pd.DataFrame(new_cols, index=d.index)], axis=1)
    return d, names


def add_interactions(d: pd.DataFrame, feats: list[str], top: list[str]) -> tuple[pd.DataFrame, list[str]]:
    """Pairwise products of a small set of already-promising features."""
    new_cols, names = {}, []
    for i in range(len(top)):
        for j in range(i + 1, len(top)):
            a, b = top[i], top[j]
            name = f"x_{a}__X__{b}"
            new_cols[name] = d[a] * d[b]
            names.append(name)
    if new_cols:
        d = pd.concat([d, pd.DataFrame(new_cols, index=d.index)], axis=1)
    return d, feats + names
