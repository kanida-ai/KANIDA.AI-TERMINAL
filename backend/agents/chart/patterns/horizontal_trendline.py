"""
Chart Agent · Pattern: Horizontal Trendline (Breakout-Retest + Volume), daily.

PORTED VERBATIM from the R&D reference (Documents/Kanida_Falcon):
  - scripts/chart_agent.py          : detect_horizontal_breakout_retest, _pivot_highs, PARAMS
  - scripts/chart_agent_screener.py : _levels() deterministic clustering, classify() live-stage

Every threshold in PARAMS is preserved bit-for-bit (v3 §5.5). The logic is strictly point-in-time
(v3 §0/§2/§3): pivots are confirmed only L bars later; level clustering reads only pivots in
[k-level_window, k-L]; the flat-top test reads only closes[min(touch)..k]; the retest/failed scan
reads only bars < k. Entry is always the next open (entry_idx = signal_idx + 1).

Two point-in-time surfaces:
  * detect(df, as_of_idx)      -> the LIVE stage at as_of_idx (screener classify()) as an occurrence.
  * historical_events(df, ...) -> resolved BREAKOUT->RETEST occurrences (detect_horizontal_breakout_retest),
                                  used by evidence.py for the "what happened next" statistics.
"""
from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd

from .base import PatternDetector, PatternOccurrence
from . import registry

# --- Detector parameters (v3 §5.5) — copied EXACTLY from R&D chart_agent.PARAMS -----------------
PARAMS = dict(L=5, level_window=120, min_touches=2, tol=0.01, buffer=0.002,
              vol_mult=1.3, retest_vol_mult=1.0, vol_win=20, retest_max=15, retest_tol=0.012)

# v3 §5.4 [SPEC — fix] retest depth-floor: low[d] >= R*(1 - retest_depth_max). Default None keeps
# the BUILT behaviour byte-identical (we do NOT silently change the detector). Set to e.g. 0.02 via
# Loop-4 governance to enable. When enabled it only makes RETEST *stricter* (rejects deep craters).
PARAMS["retest_depth_max"] = None

APPROACH_BAND = 0.02   # within 2% below the level = "approaching" (screener constant)

PATTERN_ID = "horizontal_trendline"


# --------------------------------------------------------------------- pivots / levels (ported)
def _pivots(high: np.ndarray, L: int) -> np.ndarray:
    """Indices of strict local highs over +/-L (confirmed at i+L). Ported from screener._pivots."""
    n = len(high)
    p = np.zeros(n, bool)
    for i in range(L, n - L):
        if high[i] == high[i - L:i + L + 1].max():
            p[i] = True
    return np.where(p)[0]


def _levels(h, c, pv, k, P=PARAMS):
    """Flat-top resistance candidates as-of bar k (highest first). PORTED from screener._levels:
    >=min_touches pivot highs within tol, no daily close above the level between first touch and k.
    Point-in-time: only pivots in [k-level_window, k-L] and closes[min(ti)..k]."""
    win = [i for i in pv if k - P["level_window"] <= i <= k - P["L"]]
    if len(win) < P["min_touches"]:
        return []
    prices = np.array([h[i] for i in win])
    out = []
    used = set()
    for cand in np.sort(prices)[::-1]:
        m = np.abs(prices - cand) <= P["tol"] * cand
        if m.sum() < P["min_touches"]:
            continue
        lvl = float(prices[m].mean())
        if round(lvl, 1) in used:
            continue
        used.add(round(lvl, 1))
        ti = [win[j] for j in range(len(win)) if m[j]]
        if c[min(ti):k].max() > lvl * (1 + P["buffer"]):   # flat top must be unbroken through k
            continue
        out.append((lvl, ti))
    return out


def classify(o, h, l, c, v, avg, pv, di, P=PARAMS):
    """Live-stage classifier for day di — PORTED from screener.classify().
    Returns (stage, level, touches, dist_pct, volx) or None. Strictly point-in-time (reads <= di)."""
    a = avg[di]
    if not np.isfinite(a) or a <= 0:
        return None
    cands = _levels(h, c, pv, di, P)
    # 1) BREAKOUT today — first close above, on volume
    for lvl, ti in cands:
        if c[di - 1] <= lvl * (1 + P["buffer"]) < c[di] and v[di] > P["vol_mult"] * a:
            return ("BREAKOUT", lvl, ti, (c[di] / lvl - 1) * 100, v[di] / a)
    # 2) APPROACHING — nearest flat top just above price, not yet broken
    for lvl, ti in cands:
        if lvl * (1 - APPROACH_BAND) <= c[di] <= lvl * (1 + P["buffer"]):
            return ("APPROACHING", lvl, ti, (c[di] / lvl - 1) * 100, v[di] / a)
    # 3) RETEST / FAILED — was there a breakout in the last retest_max sessions?
    for b in range(di - 1, max(P["L"], di - P["retest_max"] - 1), -1):
        for lvl, ti in _levels(h, c, pv, b, P):
            if c[b - 1] <= lvl * (1 + P["buffer"]) < c[b] and v[b] > P["vol_mult"] * avg[b]:
                depth_ok = (P["retest_depth_max"] is None
                            or l[di] >= lvl * (1 - P["retest_depth_max"]))   # v3 §5.4 [SPEC] floor
                if (l[di] <= lvl * (1 + P["retest_tol"]) and c[di] >= lvl * (1 - P["retest_tol"])
                        and c[di] > c[di - 1] and c[di] > lvl and v[di] > P["retest_vol_mult"] * a
                        and depth_ok):
                    return ("RETEST", lvl, ti, (c[di] / lvl - 1) * 100, v[di] / a)
                if c[di] < lvl * (1 - P["buffer"]):
                    return ("FAILED", lvl, ti, (c[di] / lvl - 1) * 100, v[di] / a)
                break
    return None


# ----------------------------------------------- full-history breakout->retest events (ported)
class _Event:
    """Lightweight stand-in for the R&D PatternEvent — exposes .entry_idx / .signal_idx / .level
    so evidence.pattern_evidence (also ported) works unchanged."""
    __slots__ = ("signal_idx", "entry_idx", "breakout_idx", "retest_idx", "level", "touches")

    def __init__(self, signal_idx, entry_idx, breakout_idx, retest_idx, level, touches):
        self.signal_idx = signal_idx
        self.entry_idx = entry_idx
        self.breakout_idx = breakout_idx
        self.retest_idx = retest_idx
        self.level = level
        self.touches = touches


def detect_horizontal_breakout_retest(df: pd.DataFrame, L=5, level_window=120, min_touches=2,
                                      tol=0.01, buffer=0.002, vol_mult=1.3, retest_vol_mult=1.0,
                                      vol_win=20, retest_max=15, retest_tol=0.012,
                                      retest_depth_max=None) -> list:
    """PORTED from chart_agent.detect_horizontal_breakout_retest. A horizontal resistance touched
    >=2x, a VOLUME breakout above it, then a retest that reclaims the level and turns up. Entry =
    next open. Point-in-time: every read is <= the bar being evaluated."""
    o, h, l, c, v = (df[k].values for k in ["open", "high", "low", "close", "volume"])
    n = len(c)
    piv = np.zeros(n, bool)
    for i in range(L, n - L):
        if h[i] == h[i - L:i + L + 1].max():
            piv[i] = True
    avgvol = pd.Series(v).rolling(vol_win).mean().values
    events: list = []
    b = level_window
    while b < n:
        pv = [i for i in range(max(0, b - level_window), b - L) if piv[i]]
        if len(pv) < min_touches or not np.isfinite(avgvol[b]):
            b += 1; continue
        prices = np.array([h[i] for i in pv])
        R = None; touch_idx = []
        for cand in np.sort(prices)[::-1]:
            m = np.abs(prices - cand) <= tol * cand
            if m.sum() < min_touches:
                continue
            lvl = float(prices[m].mean())
            if not (c[b - 1] <= lvl * (1 + buffer) < c[b]):     # today is the first close above
                continue
            ti = [pv[k] for k in range(len(pv)) if m[k]]
            if c[min(ti):b].max() > lvl * (1 + buffer):         # clean flat top
                continue
            R = lvl; touch_idx = ti; break
        if R is None or v[b] <= vol_mult * avgvol[b]:
            b += 1; continue
        fired = None
        for d in range(b + 1, min(b + 1 + retest_max, n - 1)):
            touched = l[d] <= R * (1 + retest_tol)
            holds = c[d] >= R * (1 - retest_tol)
            confirm = c[d] > c[d - 1] and c[d] > R and np.isfinite(avgvol[d]) and v[d] > retest_vol_mult * avgvol[d]
            depth_ok = (retest_depth_max is None or l[d] >= R * (1 - retest_depth_max))   # v3 §5.4 [SPEC]
            if touched and holds and confirm and depth_ok:
                fired = d; break
        if fired is not None and fired + 1 < n:
            events.append(_Event(signal_idx=fired, entry_idx=fired + 1, breakout_idx=b,
                                 retest_idx=fired, level=float(R), touches=touch_idx))
            b = fired + 2
        else:
            b += 1
    return events


# -------------------------------------------------------------------------- detector class
class HorizontalTrendlineDetector(PatternDetector):
    pattern_id = PATTERN_ID
    name = "Horizontal Trendline · Breakout-Retest + Volume"
    status = "built"

    def __init__(self, params: Optional[dict] = None):
        self.P = dict(PARAMS)
        if params:
            self.P.update(params)

    def detect(self, df: pd.DataFrame, as_of_idx: Optional[int] = None) -> list[PatternOccurrence]:
        """LIVE stage at as_of_idx via the ported screener classify(). Strictly point-in-time:
        arrays are sliced to <= as_of_idx before anything is computed, so no future bar is readable.
        Returns [] when there is no clean stage, or too little history."""
        P = self.P
        n_full = len(df)
        if n_full == 0:
            return []
        k = (n_full - 1) if as_of_idx is None else int(as_of_idx)
        if k < P["level_window"] + P["L"] + 5 or k >= n_full:
            return []
        w = df.iloc[:k + 1]                                     # <= as_of_idx only
        o, h, l, c, v = (w[x].to_numpy(float) for x in ["open", "high", "low", "close", "volume"])
        avg = pd.Series(v).rolling(P["vol_win"]).mean().to_numpy()
        pv = _pivots(h, P["L"])
        di = len(c) - 1
        r = classify(o, h, l, c, v, avg, pv, di, P)
        if not r:
            return []
        stage, lvl, ti, dist, volx = r
        sym = str(df.attrs.get("symbol", ""))
        occ = PatternOccurrence(
            pattern=self.pattern_id, stock=sym, stage=stage,
            signal_idx=di, entry_idx=di + 1, level=round(float(lvl), 4),
            touches=[int(t) for t in ti], direction="long", timeframe="daily",
            signature={"pattern": self.pattern_id, "timeframe": "daily", "direction": "long",
                       "stage": stage, "touches": len(ti)},
            context={"distance_to_level_pct": round(float(dist), 3), "volume_x": round(float(volx), 2),
                     "as_of_date": str(df.index[di].date()) if hasattr(df.index[di], "date") else None},
        )
        return [occ]

    def historical_events(self, df: pd.DataFrame, as_of_idx: Optional[int] = None) -> list:
        """Resolved BREAKOUT->RETEST events (ported detector) for the evidence base. When as_of_idx
        is given, keep only occurrences whose T+10 has printed by then (entry_idx+9 <= as_of_idx),
        i.e. the leak-proof as-of rule (v3 §3/§7.1)."""
        if as_of_idx is not None:
            k = int(as_of_idx)
            # Detect on the AS-OF SLICE so leak-freedom is STRUCTURAL, not empirical (auditor #1):
            # the detector's retest-scan window depends on n, so on the full frame a post-as_of
            # retest could in principle reorder pre-as_of events. Slicing removes that possibility.
            evs = detect_horizontal_breakout_retest(df.iloc[:k + 1], **self.P)
            return [e for e in evs if e.entry_idx + 9 <= k]
        return detect_horizontal_breakout_retest(df, **self.P)


registry.register(HorizontalTrendlineDetector())
