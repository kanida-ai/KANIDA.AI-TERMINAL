"""
Chart Agent · SHARED SLOPED-GEOMETRY ENGINE (triangles / wedges / rectangle / channel).

Pure functions over ALREADY-SLICED arrays. Point-in-time BY CONSTRUCTION: a pivot at i is unknown
until i+L (eligible pivots live only in [k-level_window, k-L], never k); the containment scan reads
close[min(anchor):k] EXCLUSIVE of k; the breakout is tested on close[k-1]->close[k]; nothing ever
evaluates a line against high/low/close[x] for x>k (apex_x may be >k — that is a PROJECTED
intersection that reads no bar). Line fitting is TWO-ANCHOR + CONTAINMENT (deterministic), never OLS;
r2 is a REPORTED diagnostic only, never the selector.

Everything mirrors ``horizontal_trendline.classify`` bar-for-bar (same PARAMS, APPROACH_BAND,
retest_tol/retest_max/retest_vol_mult, buffer, vol_mult), with the flat level replaced by a sloped
line line(x)=m*(x-a)+p[a] and the inequalities flipped for bearish breaks.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import numpy as np
import pandas as pd

from .base import PatternDetector, PatternOccurrence
from .horizontal_trendline import PARAMS as _HP, APPROACH_BAND

# --- Geometry PARAMS = horizontal PARAMS + governed sloped knobs (frozen; SPEC-until-OOS Loop-4) ---
GEOM_PARAMS = dict(_HP)
GEOM_PARAMS.update(
    flat_eps=5e-4,          # |slope_pph| <= flat_eps => a line counts as FLAT (else up/down)
    parallel_eps=0.15,      # |mu-ml| <= parallel_eps*mean(|slopes|) => the two lines are PARALLEL
    apex_max_frac=0.75,     # triangles/wedges valid only while progress-to-apex <= this
    min_pattern_bars=15,    # a pattern base must span at least this many bars (earliest anchor..k)
    breakdown_vol_mult=_HP["vol_mult"],   # bearish-break volume gate (SPEC knob; default = vol_mult)
    min_contraction=0.20,   # apex patterns (wedges/triangles) must have NARROWED >= this fraction of
                            # the overlap-window width by k; a barely-narrowing pair is a CHANNEL, not
                            # a wedge — the wedge-vs-channel tightness knob (frozen; SPEC-until-OOS)
)

# Which taxonomy labels have a converging apex (expiry gate applies); rectangle/channel do not.
APEX_LABELS = {"ascending_triangle", "descending_triangle", "symmetrical_triangle",
               "rising_wedge", "falling_wedge"}


# ------------------------------------------------------------------------------- pivots
def pivot_highs(high: np.ndarray, L: int) -> np.ndarray:
    """Local highs over +/-L, confirmed at i+L — SAME convention as horizontal._pivots (== max)."""
    n = len(high)
    p = np.zeros(n, bool)
    for i in range(L, n - L):
        if high[i] == high[i - L:i + L + 1].max():
            p[i] = True
    return np.where(p)[0]


def pivot_lows(low: np.ndarray, L: int) -> np.ndarray:
    """Mirror of pivot_highs — local lows over +/-L, confirmed at i+L."""
    n = len(low)
    p = np.zeros(n, bool)
    for i in range(L, n - L):
        if low[i] == low[i - L:i + L + 1].min():
            p[i] = True
    return np.where(p)[0]


# ------------------------------------------------------------------------------- line fit
@dataclass
class Line:
    m: float               # slope, price per bar
    intercept: float       # value at x=0 (= anchor_price - m*a)
    a: int                 # first anchor index
    b: int                 # second anchor index
    anchor_price: float    # price at anchor a
    touches: list = field(default_factory=list)
    r2: Optional[float] = None

    def val(self, x) -> float:
        return self.m * (x - self.a) + self.anchor_price

    @property
    def slope_pph(self) -> float:
        return self.m / self.anchor_price if self.anchor_price else 0.0


def _r2(y: np.ndarray, yhat: np.ndarray) -> Optional[float]:
    if y.size < 2:
        return None
    ss_res = float(((y - yhat) ** 2).sum())
    ybar = float(y.mean())
    ss_tot = float(((y - ybar) ** 2).sum())
    return round(1 - ss_res / ss_tot, 4) if ss_tot > 0 else None


def fit_line(pivot_idx: np.ndarray, prices: np.ndarray, closes: np.ndarray,
             side: str, k: int, P: dict) -> Optional[Line]:
    """Two-anchor + containment line as-of bar k. ``side`` in {'upper','lower'}. Deterministic
    selection: most touches -> smallest SSR to touching pivots -> smallest a -> smallest b. PIT: only
    pivots in [k-level_window, k-L]; the flat/clean scan reads close[min(touch):k] EXCLUSIVE of k."""
    tol, buffer = P["tol"], P["buffer"]
    elig = np.array([int(i) for i in pivot_idx
                     if k - P["level_window"] <= i <= k - P["L"]], dtype=int)
    if elig.size < P["min_touches"]:
        return None
    pv = prices[elig].astype(float)
    best_key = None
    best: Optional[Line] = None
    for ai in range(elig.size):
        a = int(elig[ai]); pa = float(prices[a])
        for bi in range(ai + 1, elig.size):
            b = int(elig[bi]); pb = float(prices[b])
            if b == a:
                continue
            m = (pb - pa) / (b - a)
            yv = m * (elig - a) + pa                      # line value at every eligible pivot
            if np.any(yv <= 0):
                continue
            if side == "upper":
                if not np.all(pv <= yv + tol * yv):       # no pivot high pokes above (beyond tol)
                    continue
            else:
                if not np.all(pv >= yv - tol * yv):       # no pivot low pokes below (beyond tol)
                    continue
            tmask = np.abs(pv - yv) <= tol * yv
            if int(tmask.sum()) < P["min_touches"]:
                continue
            touches = [int(x) for x in elig[tmask]]
            start = min(touches)                          # == min(a, first_touch); a is always a touch
            xs = np.arange(start, k)                      # EXCLUSIVE of k
            if xs.size:
                yline = m * (xs - a) + pa
                cseg = closes[start:k].astype(float)
                if side == "upper":
                    if np.any(cseg > yline * (1 + buffer)):   # a close broke above before k -> not clean
                        continue
                else:
                    if np.any(cseg < yline * (1 - buffer)):
                        continue
            ssr = float(((pv[tmask] - yv[tmask]) ** 2).sum())
            key = (int(tmask.sum()), -ssr, -a, -b)        # max() gives the deterministic winner
            if best_key is None or key > best_key:
                best_key = key
                best = Line(m=float(m), intercept=float(pa - m * a), a=a, b=b,
                            anchor_price=pa, touches=touches, r2=_r2(pv[tmask], yv[tmask]))
    return best


# ------------------------------------------------------------------------------- fit + taxonomy
@dataclass
class GeomFit:
    upper: Optional[Line] = None
    lower: Optional[Line] = None
    apex_x: Optional[float] = None
    converging: bool = False
    parallel: bool = False
    base_start: Optional[int] = None
    contraction: float = 0.0     # fraction the channel has narrowed base->k (== progress to apex)
    label: Optional[str] = None


def _sign(slope_pph: float, flat_eps: float) -> str:
    if slope_pph > flat_eps:
        return "up"
    if slope_pph < -flat_eps:
        return "down"
    return "flat"


def classify_taxonomy(fit: GeomFit, P: dict) -> Optional[str]:
    """ORDERED decision tree, first match wins — this ordering is how rectangle/channel/symmetrical
    ambiguity resolves."""
    if fit.upper is None or fit.lower is None:
        return None
    su = _sign(fit.upper.slope_pph, P["flat_eps"])
    sl = _sign(fit.lower.slope_pph, P["flat_eps"])
    conv, par = fit.converging, fit.parallel
    if su == "flat" and sl == "flat" and par:
        return "rectangle"
    if su == "flat" and sl == "up" and conv:
        return "ascending_triangle"
    if su == "down" and sl == "flat" and conv:
        return "descending_triangle"
    if su == "up" and sl == "up" and conv:
        return "rising_wedge"
    if su == "down" and sl == "down" and conv:
        return "falling_wedge"
    if su != "flat" and sl != "flat" and su != sl and conv:
        return "symmetrical_triangle"
    if ((su == "up" and sl == "up") or (su == "down" and sl == "down")) and par and not conv:
        return "channel"
    return None


def fit_as_of(h, l, c, pvh, pvl, k: int, P: dict) -> GeomFit:
    """Fit upper (highs) + lower (lows) lines as-of bar k and classify. PIT: all reads <= k."""
    upper = fit_line(pvh, h, c, "upper", k, P)
    lower = fit_line(pvl, l, c, "lower", k, P)
    fit = GeomFit(upper=upper, lower=lower)
    if upper is None or lower is None:
        return fit
    fit.base_start = min(upper.a, lower.a)
    if (k - fit.base_start) < P["min_pattern_bars"]:
        return fit                                        # too short a base -> label stays None
    den = upper.m - lower.m
    apex = None if abs(den) < 1e-12 else (lower.intercept - upper.intercept) / den
    fit.apex_x = apex
    # Contraction = how much the gap between the two lines has NARROWED from the base to k (equals
    # progress toward the apex for a true wedge/triangle). A pair whose lines barely close is a
    # CHANNEL, not a wedge — so "converging" now requires a real minimum narrowing, not just an apex
    # that exists somewhere far ahead. This is the wedge-vs-channel tightness fix.
    # Measure over the OVERLAP window [max(upper.a, lower.a) .. k] where BOTH lines are interpolated
    # within their own anchor span. Using min(anchors) would back-extrapolate the steeper line and
    # inflate the width (a staggered-anchor artifact) — the overlap window is the honest narrowing.
    ov = max(upper.a, lower.a)
    w_ov = upper.val(ov) - lower.val(ov)
    w_k = upper.val(k) - lower.val(k)
    fit.contraction = (1.0 - w_k / w_ov) if w_ov > 0 else 0.0
    fit.converging = ((den < 0) and (apex is not None) and (apex > k)
                      and fit.contraction >= P["min_contraction"])
    mean_abs = (abs(upper.m) + abs(lower.m)) / 2.0
    both_flat = (_sign(upper.slope_pph, P["flat_eps"]) == "flat"
                 and _sign(lower.slope_pph, P["flat_eps"]) == "flat")
    fit.parallel = both_flat or (mean_abs > 0 and abs(den) <= P["parallel_eps"] * mean_abs)
    fit.label = classify_taxonomy(fit, P)
    return fit


def _apex_ok(fit: GeomFit, k: int, P: dict) -> bool:
    """Apex/expiry gate: apex patterns are valid only while k<apex_x AND progress<=apex_max_frac."""
    if fit.label not in APEX_LABELS:
        return True
    if fit.apex_x is None or fit.base_start is None or fit.apex_x <= fit.base_start:
        return False
    if not (k < fit.apex_x):
        return False
    progress = (k - fit.base_start) / (fit.apex_x - fit.base_start)
    return 0.0 <= progress <= P["apex_max_frac"]


def _geometry_dict(fit: GeomFit, line: Line, breakout_side: str) -> dict:
    return {
        "upper_slope": round(fit.upper.slope_pph, 6) if fit.upper else None,
        "lower_slope": round(fit.lower.slope_pph, 6) if fit.lower else None,
        "anchors": {"upper": [fit.upper.a, fit.upper.b] if fit.upper else None,
                    "lower": [fit.lower.a, fit.lower.b] if fit.lower else None},
        "apex_idx": round(float(fit.apex_x), 1) if fit.apex_x is not None else None,
        "r2s": {"upper": fit.upper.r2 if fit.upper else None,
                "lower": fit.lower.r2 if fit.lower else None},
        "breakout_side": breakout_side,
    }


# ------------------------------------------------------------------------------- live context (cached)
def _live_context(df: pd.DataFrame, k: int, P: dict) -> Optional[dict]:
    """Build (and memoize on df.attrs) the as-of-k arrays + pivots + di-fit, so all 7 sloped detectors
    reuse ONE fit per (symbol, k). Purely a speed cache — no state leaks across bars/symbols."""
    cache = df.attrs.setdefault("_slctx_cache", {})
    if k in cache:
        return cache[k]
    w = df.iloc[:k + 1]
    if len(w) < P["level_window"] + P["L"] + 5:
        cache[k] = None
        return None
    o, h, l, c, v = (w[x].to_numpy(float) for x in ["open", "high", "low", "close", "volume"])
    avg = pd.Series(v).rolling(P["vol_win"]).mean().to_numpy()
    pvh, pvl = pivot_highs(h, P["L"]), pivot_lows(l, P["L"])
    di = len(c) - 1
    fit = fit_as_of(h, l, c, pvh, pvl, di, P)
    # ``fits`` memoizes fit_as_of(...b) so all 7 sloped detectors share the di-fit AND the retest
    # re-fits (they scan the SAME recent bars). PIT-safe: a fit as-of b reads only bars <= b even
    # though it runs on the di-length arrays (pivots restricted to <= b-L; close scan stops at b).
    ctx = dict(o=o, h=h, l=l, c=c, v=v, avg=avg, pvh=pvh, pvl=pvl, di=di, fit=fit, fits={di: fit})
    cache[k] = ctx
    return ctx


def _fit_at(ctx: dict, b: int, P: dict) -> GeomFit:
    fits = ctx["fits"]
    f = fits.get(b)
    if f is None:
        f = fit_as_of(ctx["h"], ctx["l"], ctx["c"], ctx["pvh"], ctx["pvl"], b, P)
        fits[b] = f
    return f


# ------------------------------------------------------------------------------- live-stage classifier
def sloped_live_stage(df: pd.DataFrame, k: int, P: dict, breakout_side: str, pattern_id: str):
    """Live stage for a sloped pattern at bar k. Returns
    (stage, level, touches, dist_pct, volx, direction, geometry) or None. Mirrors horizontal.classify
    branch-for-branch; BULLISH break tests the upper line, BEARISH the lower, EITHER tries upper then
    lower (whichever breaks first sets direction+level)."""
    ctx = _live_context(df, k, P)
    if ctx is None:
        return None
    h, l, c, v, avg, pvh, pvl, di, fit = (ctx[x] for x in
                                          ("h", "l", "c", "v", "avg", "pvh", "pvl", "di", "fit"))
    a = avg[di]
    if not np.isfinite(a) or a <= 0:
        return None
    tol, buffer = P["tol"], P["buffer"]

    # 1/2) BREAKOUT + APPROACHING on the AS-OF-di fit (requires taxonomy match + not expired)
    if fit.label == pattern_id and _apex_ok(fit, di, P):
        want_up = breakout_side in ("upper", "either")
        want_dn = breakout_side in ("lower", "either")
        lu = fit.upper.val(di) if want_up else None
        ll = fit.lower.val(di) if want_dn else None
        # BREAKOUT (bullish upper first, then bearish lower)
        if want_up and lu > 0 and c[di - 1] <= lu * (1 + buffer) < c[di] and v[di] > P["vol_mult"] * a:
            return ("BREAKOUT", float(lu), fit.upper.touches, (c[di] / lu - 1) * 100, v[di] / a,
                    "long", _geometry_dict(fit, fit.upper, "upper"))
        if want_dn and ll > 0 and c[di - 1] >= ll * (1 - buffer) > c[di] and v[di] > P["breakdown_vol_mult"] * a:
            return ("BREAKOUT", float(ll), fit.lower.touches, (c[di] / ll - 1) * 100, v[di] / a,
                    "short", _geometry_dict(fit, fit.lower, "lower"))
        # APPROACHING (nearest un-broken line just beyond price)
        if want_up and lu > 0 and lu * (1 - APPROACH_BAND) <= c[di] <= lu * (1 + buffer):
            return ("APPROACHING", float(lu), fit.upper.touches, (c[di] / lu - 1) * 100, v[di] / a,
                    "long", _geometry_dict(fit, fit.upper, "upper"))
        if want_dn and ll > 0 and ll * (1 - buffer) <= c[di] <= ll * (1 + APPROACH_BAND):
            return ("APPROACHING", float(ll), fit.lower.touches, (c[di] / ll - 1) * 100, v[di] / a,
                    "short", _geometry_dict(fit, fit.lower, "lower"))

    # 3) RETEST / FAILED — re-fit at historical bars b (the breakout line is projected forward to di)
    for b in range(di - 1, max(P["L"], di - P["retest_max"] - 1), -1):
        fb = _fit_at(ctx, b, P)
        if fb.label != pattern_id or not _apex_ok(fb, b, P) or not np.isfinite(avg[b]):
            continue
        # bullish breakout at b on the upper line?
        if breakout_side in ("upper", "either"):
            lub = fb.upper.val(b)
            if lub > 0 and c[b - 1] <= lub * (1 + buffer) < c[b] and v[b] > P["vol_mult"] * avg[b]:
                R = fb.upper.val(di)
                if (l[di] <= R * (1 + P["retest_tol"]) and c[di] >= R * (1 - P["retest_tol"])
                        and c[di] > c[di - 1] and c[di] > R and v[di] > P["retest_vol_mult"] * a):
                    return ("RETEST", float(R), fb.upper.touches, (c[di] / R - 1) * 100, v[di] / a,
                            "long", _geometry_dict(fb, fb.upper, "upper"))
                if c[di] < R * (1 - buffer):
                    return ("FAILED", float(R), fb.upper.touches, (c[di] / R - 1) * 100, v[di] / a,
                            "long", _geometry_dict(fb, fb.upper, "upper"))
                continue
        # bearish breakdown at b on the lower line?
        if breakout_side in ("lower", "either"):
            llb = fb.lower.val(b)
            if llb > 0 and c[b - 1] >= llb * (1 - buffer) > c[b] and v[b] > P["breakdown_vol_mult"] * avg[b]:
                R = fb.lower.val(di)
                if (h[di] >= R * (1 - P["retest_tol"]) and c[di] <= R * (1 + P["retest_tol"])
                        and c[di] < c[di - 1] and c[di] < R and v[di] > P["retest_vol_mult"] * a):
                    return ("RETEST", float(R), fb.lower.touches, (c[di] / R - 1) * 100, v[di] / a,
                            "short", _geometry_dict(fb, fb.lower, "lower"))
                if c[di] > R * (1 + buffer):
                    return ("FAILED", float(R), fb.lower.touches, (c[di] / R - 1) * 100, v[di] / a,
                            "short", _geometry_dict(fb, fb.lower, "lower"))
                continue
    return None


# ------------------------------------------------------------------------------- historical events
class SlopedEvent:
    """Completed sloped BREAKOUT->RETEST occurrence for the evidence base (mirrors horizontal._Event,
    plus .direction). Level is the FROZEN scalar = broken line value at the breakout bar."""
    __slots__ = ("signal_idx", "entry_idx", "breakout_idx", "retest_idx", "level", "touches", "direction")

    def __init__(self, signal_idx, entry_idx, breakout_idx, retest_idx, level, touches, direction):
        self.signal_idx = signal_idx
        self.entry_idx = entry_idx
        self.breakout_idx = breakout_idx
        self.retest_idx = retest_idx
        self.level = level
        self.touches = touches
        self.direction = direction


def _sloped_events(w: pd.DataFrame, P: dict, breakout_side: str, pattern_id: str,
                   direction: Optional[str]) -> list:
    """Full-frame completed events (mirror of detect_horizontal_breakout_retest, sloped + directional).
    ``direction`` (when not None) filters EITHER-side patterns to one side so evidence is homogeneous."""
    o, h, l, c, v = (w[k].to_numpy(float) for k in ["open", "high", "low", "close", "volume"])
    n = len(c)
    if n < P["level_window"] + 2:
        return []
    avg = pd.Series(v).rolling(P["vol_win"]).mean().to_numpy()
    pvh, pvl = pivot_highs(h, P["L"]), pivot_lows(l, P["L"])
    events: list = []
    b = P["level_window"]
    while b < n - 1:
        fit = fit_as_of(h, l, c, pvh, pvl, b, P)
        if fit.label != pattern_id or not _apex_ok(fit, b, P) or not np.isfinite(avg[b]):
            b += 1
            continue
        brk_dir = None
        line = None
        if breakout_side in ("upper", "either"):
            lu = fit.upper.val(b)
            if lu > 0 and c[b - 1] <= lu * (1 + P["buffer"]) < c[b] and v[b] > P["vol_mult"] * avg[b]:
                brk_dir, line = "long", fit.upper
        if brk_dir is None and breakout_side in ("lower", "either"):
            ll = fit.lower.val(b)
            if ll > 0 and c[b - 1] >= ll * (1 - P["buffer"]) > c[b] and v[b] > P["breakdown_vol_mult"] * avg[b]:
                brk_dir, line = "short", fit.lower
        if brk_dir is None or (direction is not None and brk_dir != direction):
            b += 1
            continue
        fired = None
        for d in range(b + 1, min(b + 1 + P["retest_max"], n - 1)):
            R = line.val(d)
            if R <= 0:
                continue
            if brk_dir == "long":
                touched = l[d] <= R * (1 + P["retest_tol"])
                holds = c[d] >= R * (1 - P["retest_tol"])
                confirm = (c[d] > c[d - 1] and c[d] > R and np.isfinite(avg[d])
                           and v[d] > P["retest_vol_mult"] * avg[d])
            else:
                touched = h[d] >= R * (1 - P["retest_tol"])
                holds = c[d] <= R * (1 + P["retest_tol"])
                confirm = (c[d] < c[d - 1] and c[d] < R and np.isfinite(avg[d])
                           and v[d] > P["retest_vol_mult"] * avg[d])
            if touched and holds and confirm:
                fired = d
                break
        if fired is not None and fired + 1 < n:
            events.append(SlopedEvent(signal_idx=fired, entry_idx=fired + 1, breakout_idx=b,
                                      retest_idx=fired, level=float(line.val(b)),
                                      touches=list(line.touches), direction=brk_dir))
            b = fired + 2
        else:
            b += 1
    return events


def sloped_historical_events(df: pd.DataFrame, P: dict, breakout_side: str, pattern_id: str,
                             direction: Optional[str] = None, as_of_idx: Optional[int] = None) -> list:
    """Resolved sloped events for the evidence base. When as_of_idx is given, detect on the AS-OF
    SLICE (structural leak-freedom) and keep only occurrences whose T+10 has printed (entry+9<=k)."""
    if as_of_idx is not None:
        k = int(as_of_idx)
        evs = _sloped_events(df.iloc[:k + 1], P, breakout_side, pattern_id, direction)
        return [e for e in evs if e.entry_idx + 9 <= k]
    return _sloped_events(df, P, breakout_side, pattern_id, direction)


# ------------------------------------------------------------------------------- shared detector base
class SlopedDetector(PatternDetector):
    """Thin shared base for the 7 sloped patterns. A subclass sets pattern_id/name/direction/
    breakout_side; detect() and historical_events() are inherited (the geometry engine does the work).
    ``direction`` is the pattern's fixed trade side ('long'/'short') or None for EITHER-side patterns
    (symmetrical/rectangle/channel), where the live break sets the direction."""
    status = "built"
    breakout_side = "either"     # 'upper' | 'lower' | 'either'
    direction: Optional[str] = None
    P = GEOM_PARAMS

    def detect(self, df, as_of_idx: Optional[int] = None) -> list:
        n = len(df)
        if n == 0:
            return []
        k = (n - 1) if as_of_idx is None else int(as_of_idx)
        if k < self.P["level_window"] + self.P["L"] + 5 or k >= n:
            return []
        r = sloped_live_stage(df, k, self.P, self.breakout_side, self.pattern_id)
        if not r:
            return []
        stage, level, touches, dist, volx, direction, geometry = r
        sym = str(df.attrs.get("symbol", ""))
        occ = PatternOccurrence(
            pattern=self.pattern_id, stock=sym, stage=stage,
            signal_idx=k, entry_idx=k + 1, level=round(float(level), 4),
            geometry=geometry, touches=[int(t) for t in touches], direction=direction,
            timeframe="daily",
            signature={"pattern": self.pattern_id, "timeframe": "daily", "direction": direction,
                       "stage": stage, "touches": len(touches),
                       "breakout_side": self.breakout_side},
            context={"distance_to_level_pct": round(float(dist), 3), "volume_x": round(float(volx), 2),
                     "as_of_date": str(df.index[k].date()) if hasattr(df.index[k], "date") else None},
        )
        return [occ]

    def historical_events(self, df, as_of_idx: Optional[int] = None, direction: Optional[str] = None) -> list:
        """Resolved same-direction events for the evidence base. For fixed-direction patterns the
        direction is self.direction; for EITHER patterns the caller (decide) passes the occurrence's
        direction so the precedents are homogeneous."""
        d = direction if direction is not None else self.direction
        return sloped_historical_events(df, self.P, self.breakout_side, self.pattern_id,
                                        direction=d, as_of_idx=as_of_idx)
