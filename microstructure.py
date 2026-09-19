"""MICROSTRUCTURE ATOM — facts from the 1-minute tape, never averages.

A daily bar is four numbers. A session is a sequence of regimes: accumulation,
markup, distribution, markdown. Averaging across it destroys exactly the
information that distinguishes them.

So this atom does not compute rolling means over the day. It extracts EVENTS --
timestamped moments where something specific happened -- and describes the state
around each one. A hypothesis is then "when event X fires, does side S over
horizon H make money?", which is falsifiable in a way that "the 20-day mean is
high" never was.

THE VOCABULARY IT STARTS WITH
-----------------------------
These encode the behaviours you described, made measurable:

  dryup          heavy selling on volume, then volume collapses while price
                 stops making new lows -- the supply exhausting
  squeeze        realised volatility compressing into a low percentile of its
                 own recent history, range narrowing bar over bar
  pressure       consecutive bars closing in the lower part of their own range
                 on above-median volume -- sustained pushing
  atp_diverge    average traded price rising while price falls, or the reverse:
                 size going the opposite way to the print
  vwap_reclaim   price crossing back above VWAP after time spent below
  or_break       opening-range boundary broken, with the direction
  vol_spike_narrow  large volume in a narrow bar: absorption
  new_low_stall  a new session low, then N bars without another

CRITICAL: every event is computed from bars strictly BEFORE the decision bar.
The event fires at bar t; the trade enters at the OPEN of bar t+1. Nothing about
bar t+1 or later touches the event definition. The truncation test in the
validation atom verifies this and will fail the build if it is violated.

The vocabulary is a starting point, not a boundary. The hypothesis atom composes
these into conjunctions, and new primitives can be added here without touching
anything downstream.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

EPS = 1e-12


def _pct_rank(s: pd.Series, window: int) -> pd.Series:
    """Where does the current value sit in its own recent history? Causal."""
    return s.rolling(window, min_periods=max(window // 3, 5)).apply(
        lambda a: (a[:-1] < a[-1]).mean() if len(a) > 1 else np.nan, raw=True)


def session_features(g: pd.DataFrame) -> pd.DataFrame:
    """Per-bar state within one session. Everything backward-looking."""
    d = g.sort_values("ts").reset_index(drop=True).copy()
    o, h, l, c, v = d["open"], d["high"], d["low"], d["close"], d["volume"]

    d["bar"] = np.arange(len(d))
    d["minute"] = d["bar"]                      # 1-min bars
    sess_open = float(o.iloc[0])
    d["ret_from_open"] = c / sess_open - 1.0

    # where the bar closed inside its own range
    rng = (h - l).replace(0, np.nan)
    d["clv"] = (c - l) / rng
    d["bar_ret"] = c.pct_change().fillna(0.0)
    d["range_pct"] = rng / c

    # running VWAP and ATP (turnover / volume proxy)
    tp = (h + l + c) / 3.0
    d["vwap"] = (tp * v).cumsum() / (v.cumsum() + EPS)
    d["vwap_dist"] = c / d["vwap"] - 1.0
    d["atp"] = (tp * v).cumsum() / (v.cumsum() + EPS)
    d["atp_slope"] = d["atp"].diff(5)

    # volume and volatility in percentile terms, within the session so far
    d["vol_pr"] = _pct_rank(v, 30)
    d["rv"] = d["bar_ret"].rolling(10, min_periods=5).std()
    d["rv_pr"] = _pct_rank(d["rv"], 60)
    d["rng_pr"] = _pct_rank(d["range_pct"], 30)

    d["run_low"] = l.cummin()
    d["run_high"] = h.cummax()
    d["bars_since_new_low"] = (
        (l <= d["run_low"] + EPS).astype(int)
        .groupby((l <= d["run_low"] + EPS).cumsum()).cumcount())
    d["bars_since_new_high"] = (
        (h >= d["run_high"] - EPS).astype(int)
        .groupby((h >= d["run_high"] - EPS).cumsum()).cumcount())

    # opening range, first 15 minutes
    orh = h.iloc[:15].max() if len(d) > 15 else np.nan
    orl = l.iloc[:15].min() if len(d) > 15 else np.nan
    d["or_high"], d["or_low"] = orh, orl
    return d


def extract_events(d: pd.DataFrame) -> pd.DataFrame:
    """Boolean event flags, each true at the bar where the condition completes."""
    e = pd.DataFrame(index=d.index)
    warm = d["bar"] >= 20                       # no events before enough history

    # --- your example 1: pushed down on volume, then dry-up -------------------
    heavy_down = ((d["clv"] < 0.35) & (d["vol_pr"] > 0.70)).rolling(
        10, min_periods=3).sum()
    e["dryup_long"] = warm & (heavy_down >= 3) & (d["vol_pr"] < 0.30) \
        & (d["bars_since_new_low"] >= 5)
    heavy_up = ((d["clv"] > 0.65) & (d["vol_pr"] > 0.70)).rolling(
        10, min_periods=3).sum()
    e["dryup_short"] = warm & (heavy_up >= 3) & (d["vol_pr"] < 0.30) \
        & (d["bars_since_new_high"] >= 5)

    # --- your example 2: volatility contraction ------------------------------
    e["squeeze"] = warm & (d["rv_pr"] < 0.20) & (d["rng_pr"] < 0.25)
    e["squeeze_above_vwap"] = e["squeeze"] & (d["vwap_dist"] > 0)
    e["squeeze_below_vwap"] = e["squeeze"] & (d["vwap_dist"] < 0)

    # --- your example 3: sustained pressure ----------------------------------
    low_close = (d["clv"] < 0.30) & (d["vol_pr"] > 0.50)
    high_close = (d["clv"] > 0.70) & (d["vol_pr"] > 0.50)
    e["pressure_down_3"] = warm & (low_close.rolling(3, min_periods=3).sum() >= 3)
    e["pressure_up_3"] = warm & (high_close.rolling(3, min_periods=3).sum() >= 3)
    e["pressure_down_5"] = warm & (low_close.rolling(5, min_periods=5).sum() >= 4)
    e["pressure_up_5"] = warm & (high_close.rolling(5, min_periods=5).sum() >= 4)

    # --- size going the other way to the print -------------------------------
    px_dn = d["close"].diff(5) < 0
    px_up = d["close"].diff(5) > 0
    e["atp_diverge_bull"] = warm & px_dn & (d["atp_slope"] > 0)
    e["atp_diverge_bear"] = warm & px_up & (d["atp_slope"] < 0)

    # --- VWAP reclaim / loss --------------------------------------------------
    below = d["vwap_dist"] < 0
    e["vwap_reclaim"] = warm & (~below) & below.shift(1).fillna(False) \
        & (below.rolling(15, min_periods=5).sum() >= 8)
    e["vwap_lose"] = warm & below & (~below.shift(1).fillna(False)) \
        & ((~below).rolling(15, min_periods=5).sum() >= 8)

    # --- opening range ---------------------------------------------------------
    e["or_break_up"] = warm & (d["close"] > d["or_high"]) \
        & (d["close"].shift(1) <= d["or_high"])
    e["or_break_dn"] = warm & (d["close"] < d["or_low"]) \
        & (d["close"].shift(1) >= d["or_low"])

    # --- absorption: big size, small range -------------------------------------
    e["absorb"] = warm & (d["vol_pr"] > 0.85) & (d["rng_pr"] < 0.35)
    e["absorb_low"] = e["absorb"] & (d["ret_from_open"] < 0)
    e["absorb_high"] = e["absorb"] & (d["ret_from_open"] > 0)

    # --- exhaustion: new extreme then stall ------------------------------------
    e["low_stall"] = warm & (d["bars_since_new_low"].between(8, 20))
    e["high_stall"] = warm & (d["bars_since_new_high"].between(8, 20))

    return e.fillna(False).astype(bool)


TIME_BUCKETS = [("open30", 0, 30), ("mid_morning", 30, 90),
                ("midday", 90, 240), ("afternoon", 240, 330),
                ("last30", 330, 375)]


def build_event_panel(bars: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """One row per bar: event flags, session context, and the forward anchors.

    Forward anchors (next bar's open, session close, session index) are computed
    here so the hypothesis atom never has to touch raw bars again -- which keeps
    the causality boundary in exactly one place.
    """
    out = []
    for sess, g in bars.groupby("session", sort=True):
        if len(g) < 60:
            continue
        d = session_features(g)
        e = extract_events(d)
        row = pd.concat([d[["ts", "bar", "minute", "open", "high", "low", "close",
                            "ret_from_open", "clv", "vwap_dist", "vol_pr",
                            "rv_pr", "rng_pr"]], e], axis=1)
        row["session"] = sess
        # entry is the NEXT bar's open; nothing later leaks into the event
        row["next_open"] = d["open"].shift(-1)
        row["sess_close"] = float(d["close"].iloc[-1])
        row["bars_left"] = len(d) - 1 - d["bar"]
        for name, a, b in TIME_BUCKETS:
            row[f"t_{name}"] = (d["minute"] >= a) & (d["minute"] < b)
        out.append(row)
    if not out:
        raise RuntimeError("no sessions with enough bars")
    p = pd.concat(out, ignore_index=True)
    if verbose:
        ev = [c for c in p.columns if p[c].dtype == bool and not c.startswith("t_")]
        print(f"    event panel: {len(p):,} bars, {p['session'].nunique()} sessions, "
              f"{len(ev)} event types")
        fired = {c: int(p[c].sum()) for c in ev}
        top = sorted(fired.items(), key=lambda x: -x[1])[:6]
        print(f"    most frequent: " +
              ", ".join(f"{k}={v:,}" for k, v in top))
    return p


def event_names(panel: pd.DataFrame) -> list[str]:
    return [c for c in panel.columns
            if panel[c].dtype == bool and not c.startswith("t_")]


def time_names(panel: pd.DataFrame) -> list[str]:
    return [c for c in panel.columns if c.startswith("t_")]
