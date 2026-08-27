"""
Chart Agent · Pattern: Cup & Handle (daily). BULLISH / long only.  [BUILT-basic]

WAVE B. A rounded U base between two similar-height rims, then a shallow handle near the right rim,
then a breakout above the rim on volume. The geometry is bespoke (rounding + handle), but every step
is DETERMINISTIC (np.polyfit least-squares; fixed candidate ordering; frozen governed thresholds) so
two engineers reproduce identical results, and strictly POINT-IN-TIME.

DETERMINISTIC GEOMETRY (architect's Wave B spec):
  1. Two rims — two pivot highs (reuse ``_geometry.pivot_highs``; eligible index <= k-L) within
     ``rim_tol`` of each other. rim = the LOWER of the two (the breakout reference).
  2. Rounded U base between the rims — least-squares quadratic on the closes between the rims:
     require positive curvature (opens upward), R^2 >= ``cup_r2_min``, a PARABOLA-vs-TENT test
     (``quad_ssr / tent_ssr < tent_ratio_max`` where the tent is the best symmetric V centred on the
     base) that rejects wide symmetric V bottoms and blocky staircases which a bare R^2 floor lets
     through, depth in [``cup_depth_min``, ``cup_depth_max``] off the rim, and the minimum near the
     middle (|argmin_frac - 0.5| <= ``symmetry_frac``). Together these genuinely reject V bottoms.
  3. Handle — a shallow pullback after the right rim: depth <= ``handle_depth_max`` * cup_depth,
     duration <= ``handle_max_bars``, staying in the upper half of the cup (low >= cup_mid) and NOT
     breaking the rim early. A handle is REQUIRED for cup_and_handle.
  4. Rim breakout — bullish live-stage (same convention as horizontal): close[di-1] <= rim*(1+buffer)
     < close[di] AND v[di] > vol_mult*avg[di]. direction="long", level = rim (frozen scalar).

POINT-IN-TIME (LAW): detect on the as-of slice df.iloc[:k+1]; pivots <= k-L; the quadratic fit +
handle scan read only bars < k for structure; the breakout is tested on close[k-1]->close[k]; entry =
signal+1; historical_events keep only entry+9 <= k. Never read a bar > k.

SPEC (honestly labelled, not fabricated): rim-symmetry robustness (a single global-min-based
symmetry test), volume-profile confirmation of the base, and OOS calibration of every knob below
remain SPEC-until-OOS (Loop-4). All precedents are small-N -> honest WATCH.
"""
from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd

from .base import PatternDetector, PatternOccurrence
from . import registry
from ._geometry import pivot_highs
from .horizontal_trendline import PARAMS as _HP, APPROACH_BAND

PATTERN_ID = "cup_and_handle"

# Reused horizontal knobs + NEW governed cup knobs (frozen; SPEC-until-OOS Loop-4).
CUP_PARAMS = dict(
    L=_HP["L"], level_window=_HP["level_window"], buffer=_HP["buffer"],
    vol_mult=_HP["vol_mult"], vol_win=_HP["vol_win"], retest_max=_HP["retest_max"],
    rim_tol=0.05,            # |right-left|/left rim-height tolerance
    cup_r2_min=0.60,         # quadratic fit-quality floor (rounded, not V)
    tent_ratio_max=0.50,     # quad_ssr / tent_ssr must be < this: a PARABOLA beats a symmetric TENT
                             # (V/staircase) badly (ratio«1); a V/tent does not (ratio>=~1) -> rejected
    cup_depth_min=0.08,      # cup depth off the rim, min
    cup_depth_max=0.50,      # cup depth off the rim, max
    handle_depth_max=0.33,   # handle retrace as a FRACTION of cup depth
    handle_depth_min=0.01,   # a handle must be a GENUINE pullback (>=1% off rim), not a flat drift
    handle_max_bars=20,      # handle duration cap
    symmetry_frac=0.25,      # |argmin_frac - 0.5| <= this (min near the middle)
    min_cup_bars=20,         # min rim-to-rim width
    max_cup_bars=130,        # max rim-to-rim width (<= level_window)
)


# --------------------------------------------------------------------------- deterministic geometry
def _quad_fit(y: np.ndarray):
    """Least-squares quadratic on y (x = 0..len-1). Returns (curvature a, R^2, SSR). Deterministic."""
    x = np.arange(len(y), dtype=float)
    a, b, cc = np.polyfit(x, y, 2)
    yhat = a * x * x + b * x + cc
    ss_res = float(((y - yhat) ** 2).sum())
    ybar = float(y.mean())
    ss_tot = float(((y - ybar) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return float(a), float(r2), ss_res


def _tent_ssr(y: np.ndarray) -> float:
    """Least-squares SYMMETRIC TENT (V) fit: y = a*|i - center| + b, vertex fixed at the geometric
    CENTER (deterministic; the center cleanly separates U from V/staircase — argmin does not). Returns
    the tent residual SSR. A true parabola fits far better than this tent (quad_ssr«tent_ssr); a
    linear V or a blocky staircase does not."""
    n = len(y)
    x = np.arange(n, dtype=float)
    d = np.abs(x - (n - 1) / 2.0)
    A = np.vstack([d, np.ones_like(d)]).T
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    yhat = A @ coef
    return float(((y - yhat) ** 2).sum())


def _candidate_cups(h, c, pvh, di, P) -> list:
    """All valid CUPS (rims + rounded base) as-of di, WITHOUT the handle — deterministically ordered
    (most-recent right rim, then best R^2, then widest). Point-in-time: rims in [di-level_window,
    di-L]; the base fit reads closes[Lr:Rr+1], all <= di-L < di."""
    rims = [int(i) for i in pvh if di - P["level_window"] <= i <= di - P["L"]]
    out = []
    for ri in range(len(rims)):
        Rr = rims[ri]
        hr = float(h[Rr])
        for li in range(ri):
            Lr = rims[li]
            w = Rr - Lr
            if w < P["min_cup_bars"] or w > P["max_cup_bars"]:
                continue
            hl = float(h[Lr])
            if hl <= 0 or hr <= 0 or abs(hr - hl) / hl > P["rim_tol"]:
                continue
            rim = min(hl, hr)
            y = c[Lr:Rr + 1].astype(float)
            a, r2, qssr = _quad_fit(y)
            if a <= 0 or r2 < P["cup_r2_min"]:
                continue
            # PARABOLA-vs-TENT roundedness discriminator: a rounded U beats the best symmetric tent
            # (V/staircase) badly; a V or staircase does not. Rejects the wide-V / staircase that
            # cup_r2_min alone lets through (auditor Item 3). PIT: reads only base closes <= di-L.
            tssr = _tent_ssr(y)
            tent_ratio = qssr / tssr if tssr > 1e-12 else float("inf")
            if not (tent_ratio < P["tent_ratio_max"]):
                continue
            cb = float(y.min())
            depth = (rim - cb) / rim
            if depth < P["cup_depth_min"] or depth > P["cup_depth_max"]:
                continue
            amin = int(np.argmin(y))
            if abs(amin / w - 0.5) > P["symmetry_frac"]:
                continue
            out.append(dict(Lr=Lr, Rr=Rr, rim=float(rim), depth=float(depth), curv=a, r2=r2,
                            tent_ratio=float(tent_ratio), cup_bottom=cb, cup_bottom_idx=Lr + amin,
                            cup_mid=float(rim * (1 - depth / 2)), symmetry=abs(amin / w - 0.5)))
    out.sort(key=lambda d: (-d["Rr"], -d["r2"], d["Lr"]))
    return out


def _handle_ok(h, l, cup, hend, P) -> Optional[dict]:
    """Validate a handle over bars (Rr, hend]: shallow (<= handle_depth_max*cup_depth), short
    (<= handle_max_bars), no premature rim break, low >= cup_mid. Returns handle metrics or None."""
    Rr, rim = cup["Rr"], cup["rim"]
    if hend <= Rr or (hend - Rr) > P["handle_max_bars"]:
        return None
    wl = l[Rr + 1:hend + 1]
    wh = h[Rr + 1:hend + 1]
    if wl.size == 0:
        return None
    if float(wh.max()) > rim * (1 + P["buffer"]):        # rim already breached inside the handle
        return None
    hlow = float(wl.min())
    if hlow < cup["cup_mid"]:                            # undercuts the cup mid -> not a handle
        return None
    hdepth = (rim - hlow) / rim
    if hdepth < P["handle_depth_min"]:                   # a flat drift is not a pullback -> not a handle
        return None
    if hdepth > P["handle_depth_max"] * cup["depth"]:
        return None
    return dict(low=hlow, depth=float(hdepth), bars=int(hend - Rr))


def _breakout_at(h, l, c, v, avg, pvh, b, P) -> Optional[dict]:
    """The deterministic cup+handle whose rim is BROKEN at bar b (handle ends b-1, first close above
    the rim on volume), or None. Reused by the live BREAKOUT stage and historical_events."""
    if b - 1 < 0:
        return None
    ab = avg[b]
    if not np.isfinite(ab) or ab <= 0:
        return None
    for cup in _candidate_cups(h, c, pvh, b, P):
        hm = _handle_ok(h, l, cup, b - 1, P)
        if hm is None:
            continue
        rim = cup["rim"]
        if c[b - 1] <= rim * (1 + P["buffer"]) < c[b] and v[b] > P["vol_mult"] * ab:
            return dict(cup=cup, handle=hm, rim=rim)
    return None


def _geometry_dict(cup: dict, handle: dict) -> dict:
    return {
        "left_rim": cup["Lr"], "right_rim": cup["Rr"], "rim": round(cup["rim"], 4),
        "cup_depth_pct": round(cup["depth"] * 100, 2), "curvature": cup["curv"],
        "r2": round(cup["r2"], 4), "tent_ratio": round(cup["tent_ratio"], 4),
        "cup_bottom_idx": cup["cup_bottom_idx"],
        "cup_mid": round(cup["cup_mid"], 4), "symmetry": round(cup["symmetry"], 4),
        "handle_bars": handle["bars"], "handle_depth_pct": round(handle["depth"] * 100, 2),
    }


def _classify(o, h, l, c, v, avg, pvh, di, P):
    """Live stage at di: BREAKOUT | APPROACHING (in-handle) | FAILED, or None. Long only."""
    a = avg[di]
    if not np.isfinite(a) or a <= 0:
        return None
    buffer = P["buffer"]

    # 1) BREAKOUT today (handle ends di-1) — reuse the shared breakout finder
    brk = _breakout_at(h, l, c, v, avg, pvh, di, P)
    if brk is not None:
        rim = brk["rim"]
        return ("BREAKOUT", rim, [brk["cup"]["Lr"], brk["cup"]["Rr"]],
                (c[di] / rim - 1) * 100, v[di] / a, "long", _geometry_dict(brk["cup"], brk["handle"]))

    # 2) APPROACHING — a valid cup+handle formed through di, price sitting just under the rim
    for cup in _candidate_cups(h, c, pvh, di, P):
        hm = _handle_ok(h, l, cup, di, P)
        if hm is None:
            continue
        rim = cup["rim"]
        if rim * (1 - APPROACH_BAND) <= c[di] <= rim * (1 + buffer):
            return ("APPROACHING", rim, [cup["Lr"], cup["Rr"]],
                    (c[di] / rim - 1) * 100, v[di] / a, "long", _geometry_dict(cup, hm))

    # 3) FAILED — a recent cup breakout that has since lost the rim (gated to down bars for cost)
    if c[di] < c[di - 1]:
        for b in range(di - 1, max(P["L"], di - P["retest_max"] - 1), -1):
            brk = _breakout_at(h, l, c, v, avg, pvh, b, P)
            if brk is not None:
                rim = brk["rim"]
                if c[di] < rim * (1 - buffer):
                    return ("FAILED", rim, [brk["cup"]["Lr"], brk["cup"]["Rr"]],
                            (c[di] / rim - 1) * 100, v[di] / a, "long",
                            _geometry_dict(brk["cup"], brk["handle"]))
                break
    return None


# --------------------------------------------------------------------------- historical events
class _CupEvent:
    __slots__ = ("signal_idx", "entry_idx", "breakout_idx", "level", "touches", "direction")

    def __init__(self, signal_idx, entry_idx, level, touches):
        self.signal_idx = signal_idx
        self.entry_idx = entry_idx
        self.breakout_idx = signal_idx
        self.level = level
        self.touches = touches
        self.direction = "long"


def _cup_events(w: pd.DataFrame, P) -> list:
    o, h, l, c, v = (w[k].to_numpy(float) for k in ["open", "high", "low", "close", "volume"])
    n = len(c)
    if n < P["level_window"] + 2:
        return []
    avg = pd.Series(v).rolling(P["vol_win"]).mean().to_numpy()
    pvh = pivot_highs(h, P["L"])
    events, b = [], P["level_window"]
    while b < n - 1:
        brk = _breakout_at(h, l, c, v, avg, pvh, b, P)
        if brk is None:
            b += 1
            continue
        events.append(_CupEvent(signal_idx=b, entry_idx=b + 1, level=float(brk["rim"]),
                                touches=[brk["cup"]["Lr"], brk["cup"]["Rr"]]))
        b += 1                                          # next bar can't be a fresh "first close above"
    return events


# --------------------------------------------------------------------------- detector class
class CupAndHandleDetector(PatternDetector):
    pattern_id = PATTERN_ID
    name = "Cup & Handle (rounded base + handle, breakout)"
    status = "built"
    direction = "long"
    P = CUP_PARAMS

    def detect(self, df, as_of_idx: Optional[int] = None) -> list:
        n = len(df)
        if n == 0:
            return []
        k = (n - 1) if as_of_idx is None else int(as_of_idx)
        if k < self.P["level_window"] + self.P["L"] + 5 or k >= n:
            return []
        w = df.iloc[:k + 1]
        o, h, l, c, v = (w[x].to_numpy(float) for x in ["open", "high", "low", "close", "volume"])
        avg = pd.Series(v).rolling(self.P["vol_win"]).mean().to_numpy()
        pvh = pivot_highs(h, self.P["L"])
        di = len(c) - 1
        r = _classify(o, h, l, c, v, avg, pvh, di, self.P)
        if not r:
            return []
        stage, rim, touches, dist, volx, direction, geometry = r
        sym = str(df.attrs.get("symbol", ""))
        occ = PatternOccurrence(
            pattern=self.pattern_id, stock=sym, stage=stage,
            signal_idx=di, entry_idx=di + 1, level=round(float(rim), 4),
            geometry=geometry, touches=[int(t) for t in touches], direction="long", timeframe="daily",
            signature={"pattern": self.pattern_id, "timeframe": "daily", "direction": "long",
                       "stage": stage, "touches": len(touches), "breakout_side": "upper"},
            context={"distance_to_level_pct": round(float(dist), 3), "volume_x": round(float(volx), 2),
                     "as_of_date": str(df.index[di].date()) if hasattr(df.index[di], "date") else None},
        )
        return [occ]

    def historical_events(self, df, as_of_idx: Optional[int] = None,
                          direction: Optional[str] = None) -> list:
        """Resolved cup breakout occurrences (long). Detect on the as-of slice (structural
        leak-freedom), keep only those whose T+10 has printed (entry+9 <= as_of). ``direction`` is
        accepted for the uniform detector contract but the cup is long-only."""
        if as_of_idx is not None:
            k = int(as_of_idx)
            return [e for e in _cup_events(df.iloc[:k + 1], self.P) if e.entry_idx + 9 <= k]
        return _cup_events(df, self.P)


registry.register(CupAndHandleDetector())
