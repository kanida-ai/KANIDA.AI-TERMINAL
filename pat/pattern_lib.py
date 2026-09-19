"""Generic patterns -> stock-specific patterns, point-in-time.

THE PROBLEM WITH THE WORKBOOK AS WRITTEN
---------------------------------------
Every one of the 4,794 rules carries ABSOLUTE thresholds:

    atr_20_pct <= 3.7177 AND weekly_close_loc <= 0.7898

For ADANIENT, whose average daily range is about 3.4%, an ATR of 3.7% is an
ordinary day. For HDFCBANK it is an extraordinary one. The same number selects
"calm" on one stock and "chaotic" on another, so applied uniformly the rule is
not one pattern -- it is a different pattern on every stock, and mostly the
wrong one.

HOW THE THRESHOLD IS ADAPTED
----------------------------
A threshold is translated into WHERE IT SITS in a distribution, and then back
into that stock's own units:

  1. During a burn-in window, pool the variable across the reference universe
     and find the percentile the generic threshold occupies. `atr_20_pct <= 3.72`
     might be the 55th percentile of ATR across all stocks.

  2. For each stock, on each date, take the 55th percentile of THAT STOCK'S OWN
     history up to that date. For ADANIENT that might be 3.9%; for HDFCBANK 1.4%.

  3. The rule now reads "ATR in the calmer 55% of this stock's own recent
     behaviour" -- which is one pattern, expressed correctly on every stock.

WHY THIS IS POINT-IN-TIME AND NOT A CONVENIENCE
-----------------------------------------------
The per-stock percentile is EXPANDING: on 3 March 2023 it uses that stock's
history up to 2 March 2023 and nothing after. The burn-in percentile is fixed
once, from the burn-in window only, and never revisited. A rule evaluated on any
date could have been written that morning with the data then available.

This matters more than it sounds. A fixed percentile computed over the whole
sample would encode 2026's volatility into a 2022 decision, and every backtest
built on it would be worthless in a way that is almost impossible to see.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

EPS = 1e-12

# variables the workbook actually uses, grouped by what they need
DAILY_VARS = [
    "atr_20_pct", "atr_5_vs_20", "range_pct", "gap_pct", "close_loc",
    "body_pct", "upper_wick_pct", "lower_wick_pct",
    "dist_high_10", "dist_high_20", "dist_high_60", "dist_high_120",
    "dist_high_252", "dist_sma_20", "dist_sma_50", "dist_sma_200",
    "slope_sma_20", "slope_sma_50", "roc_5", "roc_20", "roc_60", "rsi_14",
    "vol_vs_20d", "vol_5d_vs_20d", "n_sub_75v_7d", "n_sub_75v_20d",
    "n_sub_3_range_7d", "n_sub_2_5_range_7d",
    "n_higher_highs_5d", "n_higher_lows_5d",
]
WEEKLY_VARS = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20",
               "weekly_breakout_20w"]
RELATIVE_VARS = ["rs_market_20d", "rs_market_60d", "rs_sector_20d", "rs_sector_60d"]
ALL_VARS = DAILY_VARS + WEEKLY_VARS + RELATIVE_VARS


def _rsi(c: pd.Series, n: int = 14) -> pd.Series:
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    return 100 - 100 / (1 + up / (dn + EPS))


def compute_vars(d: pd.DataFrame, mkt: pd.Series | None = None,
                 sec: pd.Series | None = None) -> pd.DataFrame:
    """All 38 workbook variables from daily OHLCV. Every one backward-looking."""
    o, h, l, c, v = d["open"], d["high"], d["low"], d["close"], d["volume"]
    f = pd.DataFrame(index=d.index)

    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr20 = tr.rolling(20, min_periods=10).mean()
    atr5 = tr.rolling(5, min_periods=3).mean()
    f["atr_20_pct"] = atr20 / c * 100
    f["atr_5_vs_20"] = atr5 / (atr20 + EPS)

    rng = (h - l)
    f["range_pct"] = rng / c * 100
    f["gap_pct"] = (o / prev_c - 1) * 100
    f["close_loc"] = (c - l) / (rng + EPS)
    f["body_pct"] = (c - o).abs() / (rng + EPS)
    f["upper_wick_pct"] = (h - np.maximum(o, c)) / (rng + EPS)
    f["lower_wick_pct"] = (np.minimum(o, c) - l) / (rng + EPS)

    for w in (10, 20, 60, 120, 252):
        f[f"dist_high_{w}"] = (c / h.rolling(w, min_periods=w // 2).max() - 1) * 100
    for w in (20, 50, 200):
        sma = c.rolling(w, min_periods=w // 2).mean()
        f[f"dist_sma_{w}"] = (c / sma - 1) * 100
        if w in (20, 50):
            f[f"slope_sma_{w}"] = (sma / sma.shift(5) - 1) * 100
    for w in (5, 20, 60):
        f[f"roc_{w}"] = (c / c.shift(w) - 1) * 100
    f["rsi_14"] = _rsi(c)

    v20 = v.rolling(20, min_periods=10).mean()
    f["vol_vs_20d"] = v / (v20 + EPS)
    f["vol_5d_vs_20d"] = v.rolling(5, min_periods=3).mean() / (v20 + EPS)
    sub75 = (v < 0.75 * v20).astype(float)
    f["n_sub_75v_7d"] = sub75.rolling(7, min_periods=4).sum()
    f["n_sub_75v_20d"] = sub75.rolling(20, min_periods=10).sum()
    r3 = (f["range_pct"] < 3.0).astype(float)
    r25 = (f["range_pct"] < 2.5).astype(float)
    f["n_sub_3_range_7d"] = r3.rolling(7, min_periods=4).sum()
    f["n_sub_2_5_range_7d"] = r25.rolling(7, min_periods=4).sum()
    f["n_higher_highs_5d"] = (h > h.shift(1)).astype(float).rolling(5, min_periods=3).sum()
    f["n_higher_lows_5d"] = (l > l.shift(1)).astype(float).rolling(5, min_periods=3).sum()

    # ---- weekly: only COMPLETED weeks, carried forward -----------------------
    wk = d.set_index("date").resample("W-FRI").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()
    wsma = wk["close"].rolling(20, min_periods=8).mean()
    wf = pd.DataFrame({
        "weekly_close_loc": (wk["close"] - wk["low"]) / (wk["high"] - wk["low"] + EPS),
        "weekly_range_pct": (wk["high"] - wk["low"]) / wk["close"] * 100,
        "weekly_close_vs_sma20": (wk["close"] / wsma - 1) * 100,
        "weekly_breakout_20w": (wk["close"] >= wk["high"].rolling(
            20, min_periods=8).max().shift(1)).astype(float),
    })
    # a week's values are known only AFTER it closes, so shift then merge back
    wf = wf.shift(1)
    wf = wf.reset_index().rename(columns={wf.index.name or "index": "date"})
    wf["date"] = pd.to_datetime(wf["date"])
    merged = pd.merge_asof(
        d[["date"]].sort_values("date").reset_index(drop=True),
        wf.sort_values("date").reset_index(drop=True),
        on="date", direction="backward")
    for cname in WEEKLY_VARS:
        f[cname] = merged[cname].to_numpy()

    # ---- relative strength ---------------------------------------------------
    for tag, ref in (("market", mkt), ("sector", sec)):
        for w in (20, 60):
            if ref is None:
                f[f"rs_{tag}_{w}d"] = np.nan
            else:
                rr = ref.reindex(d["date"].to_numpy()).to_numpy()
                stock = (c / c.shift(w) - 1) * 100
                bench = pd.Series(rr, index=d.index)
                bench = (bench / bench.shift(w) - 1) * 100
                f[f"rs_{tag}_{w}d"] = stock - bench
    f["date"] = d["date"].to_numpy()
    return f


# --------------------------------------------------------------------------- #
class ThresholdAdapter:
    """Turns absolute workbook thresholds into stock-specific, point-in-time ones."""

    def __init__(self, burnin_frames: list[pd.DataFrame], min_hist: int = 250):
        """burnin_frames: variable frames from the reference universe, burn-in only."""
        pool = pd.concat(burnin_frames, ignore_index=True)
        # The reference must be POOLED ACROSS STOCKS. A single stock's burn-in
        # cannot say where 3.7177 sits "in general" -- it can only say where it
        # sits on that stock, which is the circularity the whole adaptation is
        # meant to break. One stock's 123 rows also fail the sample floor, so
        # every rule was rejected and nothing fired at all.
        self.ref = {v: np.sort(pool[v].dropna().to_numpy())
                    for v in ALL_VARS if v in pool.columns
                    and pool[v].notna().sum() >= 300}
        self.n_ref = len(pool)
        self.min_hist = min_hist

    def pct_of(self, var: str, thr: float) -> float | None:
        """Where does this generic threshold sit in the reference distribution?"""
        a = self.ref.get(var)
        if a is None or len(a) < 300:
            return None
        return float(np.searchsorted(a, thr) / len(a))

    def stock_series(self, f: pd.DataFrame, var: str, pct: float) -> np.ndarray:
        """That percentile of THIS stock's own history, expanding, point-in-time.

        Uses only rows strictly before each date. The shift is what makes it
        honest -- without it, today's value helps set the threshold today.
        """
        s = f[var]
        return (s.shift(1)
                 .expanding(min_periods=self.min_hist)
                 .quantile(pct)
                 .to_numpy())


def parse_rules(xlsx: str, sheet: str = "All_Candidates_4794") -> pd.DataFrame:
    d = pd.read_excel(xlsx, sheet_name=sheet)
    d["conds"] = d["rule_json"].apply(json.loads)
    d["n_cond"] = d["conds"].apply(len)
    d["vars"] = d["conds"].apply(lambda r: sorted({c[0] for c in r}))
    return d[["pattern_id", "rule_text", "conds", "n_cond", "vars"]]


def evaluate_rule(f: pd.DataFrame, conds, adapter: ThresholdAdapter,
                  cache: dict) -> np.ndarray | None:
    """Boolean mask: does this stock-adapted rule hold on each date?"""
    mask = None
    for var, op, thr in conds:
        if var not in f.columns:
            return None
        key = (var, round(float(thr), 6))
        if key not in cache:
            p = adapter.pct_of(var, float(thr))
            if p is None:
                return None
            cache[key] = adapter.stock_series(f, var, p)
        lvl = cache[key]
        val = f[var].to_numpy(float)
        m = (val <= lvl) if op == "<=" else (val > lvl)
        m &= np.isfinite(lvl) & np.isfinite(val)
        mask = m if mask is None else (mask & m)
    return mask
