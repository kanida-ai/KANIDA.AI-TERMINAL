"""Shared, causal helpers for the expanded pattern detectors (Claude Code owned).

Contract (docs/pattern_research/CLAUDE_HANDOFF.md):
    specifications() -> list[dict]
    detect(bars, timeframe) -> {(pattern_id, variant, side): [event, ...]}

Point-in-time rules applied everywhere in this package:
- Bar ``i`` is usable only once it has closed; a value computed "at i" uses bars <= i.
- ``gap`` on bar ``i`` is the existing data-quality flag: a discontinuity between
  bar ``i-1`` and bar ``i``.  Nothing may span it (segments), and nothing is filled.
- ATR at ``i`` is the "prior ATR": mean true range of the 20 bars *before* ``i``,
  all inside the same clean segment, floored at 0.1% of close[i] (legacy floor).
- A radius-3 pivot at ``p`` is a strict unique extreme of bars ``p-3..p+3`` in one
  segment and becomes available at ``p+3``.  Callers must never use it earlier.
- Prior trend for a formation starting at ``s``: least-squares fit of closes
  ``s-20..s-1``; fitted change > +1 ATR[s] is ``up``, < -1 ATR[s] ``down``, else
  ``neutral``; ``None`` when the window is not clean/available.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import math

import numpy as np

DEFINITION_VERSION = "1.0.0"
PIVOT_RADIUS = 3
ATR_PERIOD = 20
ATR_FLOOR_FRACTION = 0.001
TREND_LOOKBACK = 20
TREND_ATR_MULT = 1.0
BREAKOUT_BUFFER_ATR = 0.12
CANDLE_CONFIRM_WINDOW = 3
EQUAL_TOL_ATR = 0.5
CHART_SETUP_EXPIRY = 10
FAMILIES = ("chart", "candlestick", "price_action", "harmonic")
STATES = ("setup", "confirmed")
SIDES = ("long", "short")
NOMINAL_MINUTES = {"1H": 60, "4H": 240}
_STATE_ORDER = {"setup": 0, "confirmed": 1}


# --------------------------------------------------------------------------- bars
@dataclass
class Series:
    """Array view of closed bars.  Build with :func:`prepare`."""
    timeframe: str
    n: int
    time: list
    end: list
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    v: np.ndarray
    gap: np.ndarray        # bool, existing quality flag (discontinuity before bar i)
    valid: np.ndarray      # bool, finite positive consistent OHLC, volume >= 0
    seg_id: np.ndarray     # int, constant across a clean contiguous run
    seg_start: np.ndarray  # int, first index of bar i's segment
    stub: np.ndarray       # bool, intraday bar shorter than the nominal duration
    tr: np.ndarray
    atr: np.ndarray        # nan where unavailable
    _pivots: dict = field(default_factory=dict, repr=False)
    _trend: np.ndarray | None = field(default=None, repr=False)

    # ---- windows -------------------------------------------------------------
    def clean(self, a: int, b: int) -> bool:
        """True when bars a..b (inclusive) are valid and in one gap-free segment."""
        if a < 0 or b >= self.n or a > b:
            return False
        return bool(self.seg_id[a] == self.seg_id[b] and self.valid[a] and self.valid[b])

    def atr_at(self, i: int) -> float | None:
        if i < 0 or i >= self.n:
            return None
        x = self.atr[i]
        return None if not math.isfinite(x) else float(x)

    def has_stub(self, a: int, b: int) -> bool:
        return bool(self.stub[max(a, 0):b + 1].any())

    # ---- pivots --------------------------------------------------------------
    def pivots(self, high: bool = True) -> np.ndarray:
        """All radius-3 pivot positions (strict unique extreme, one clean segment).

        Availability of pivot ``p`` is ``p + PIVOT_RADIUS``; use
        :meth:`pivots_available` to slice causally.
        """
        key = bool(high)
        if key not in self._pivots:
            self._pivots[key] = _pivot_positions(self.h if high else self.l, high, self.seg_id, self.valid)
        return self._pivots[key]

    def pivots_available(self, i: int, high: bool = True, since: int = 0) -> np.ndarray:
        """Pivots ``p`` with ``since <= p`` and ``p + 3 <= i`` (known at close of i)."""
        p = self.pivots(high)
        lo = np.searchsorted(p, since, side="left")
        hi = np.searchsorted(p, i - PIVOT_RADIUS, side="right")
        return p[lo:hi]

    # ---- trend ---------------------------------------------------------------
    def trend_change(self) -> np.ndarray:
        """Fitted 20-close change ending at ``s-1`` for every start ``s`` (nan if unclean)."""
        if self._trend is None:
            self._trend = _trend_change(self.c, self.seg_id, self.valid)
        return self._trend

    def trend(self, start: int) -> str | None:
        if start < TREND_LOOKBACK or start >= self.n:
            return None
        change = self.trend_change()[start]
        atr = self.atr[start]
        if not (math.isfinite(change) and math.isfinite(atr)):
            return None
        if change > TREND_ATR_MULT * atr:
            return "up"
        if change < -TREND_ATR_MULT * atr:
            return "down"
        return "neutral"


def prepare(bars: list[dict], timeframe: str) -> Series:
    n = len(bars)
    get = lambda k: np.array([_num(b.get(k)) for b in bars], dtype=float) if n else np.empty(0)
    o, h, l, c, v = (get(k) for k in ("open", "high", "low", "close", "volume"))
    time = [str(b.get("time", "")) for b in bars]
    end = [str(b.get("end", "")) for b in bars]
    gap = np.array([bool(b.get("gap", False)) for b in bars], dtype=bool) if n else np.empty(0, bool)
    with np.errstate(invalid="ignore"):
        valid = (np.isfinite(o) & np.isfinite(h) & np.isfinite(l) & np.isfinite(c) & np.isfinite(v)
                 & (np.minimum.reduce([o, h, l, c]) > 0 if n else np.empty(0, bool)) & (v >= 0)
                 & (h >= np.maximum.reduce([o, c, l]) if n else np.empty(0, bool))
                 & (l <= np.minimum(o, c) if n else np.empty(0, bool)))
    # A segment break sits before any gap-flagged bar and around any invalid bar.
    brk = np.zeros(n, dtype=bool)
    if n:
        brk[1:] = gap[1:] | ~valid[1:] | ~valid[:-1]
    seg_id = np.cumsum(brk).astype(np.int64)
    idx = np.arange(n)
    starts = np.where(np.r_[True, brk[1:]] if n else np.empty(0, bool), idx, 0)
    seg_start = np.maximum.accumulate(starts) if n else np.empty(0, np.int64)
    stub = np.zeros(n, dtype=bool)
    nominal = NOMINAL_MINUTES.get(timeframe)
    if nominal:
        for i in range(n):
            minutes = _minutes(time[i], end[i])
            stub[i] = minutes is not None and minutes < nominal
    # True range never uses a close across a segment break.
    tr = np.where(valid, h - l, np.nan)
    if n > 1:
        prev_c = np.r_[np.nan, c[:-1]]
        same = np.r_[False, ~brk[1:]]
        cross = np.maximum(np.abs(h - prev_c), np.abs(l - prev_c))
        tr = np.where(same & valid, np.maximum(h - l, np.where(same, cross, 0.0)), tr)
    atr = np.full(n, np.nan)
    if n > ATR_PERIOD:
        filled = np.where(np.isfinite(tr), tr, 0.0)
        cs = np.r_[0.0, np.cumsum(filled)]
        i = np.arange(ATR_PERIOD, n)
        mean = (cs[i] - cs[i - ATR_PERIOD]) / ATR_PERIOD
        ok = (i - ATR_PERIOD >= seg_start[i]) & valid[i]
        atr[i] = np.where(ok, np.maximum(mean, c[i] * ATR_FLOOR_FRACTION), np.nan)
    return Series(timeframe, n, time, end, o, h, l, c, v, gap, valid, seg_id, seg_start, stub, tr, atr)


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def _minutes(start: str, stop: str):
    try:
        return (datetime.fromisoformat(stop) - datetime.fromisoformat(start)).total_seconds() / 60
    except (TypeError, ValueError):
        return None


def _pivot_positions(values, high, seg_id, valid):
    r = PIVOT_RADIUS
    w = 2 * r + 1
    if len(values) < w:
        return np.empty(0, dtype=np.int64)
    win = np.lib.stride_tricks.sliding_window_view(values, w)
    center = win[:, r]
    extreme = win.max(axis=1) if high else win.min(axis=1)
    unique = (win == center[:, None]).sum(axis=1) == 1
    sw = np.lib.stride_tricks.sliding_window_view(seg_id, w)
    vw = np.lib.stride_tricks.sliding_window_view(valid, w)
    same = (sw[:, 0] == sw[:, -1]) & vw.all(axis=1)
    with np.errstate(invalid="ignore"):
        ok = (center == extreme) & unique & same
    return (np.flatnonzero(ok) + r).astype(np.int64)


def _trend_change(c, seg_id, valid):
    n = len(c)
    m = TREND_LOOKBACK
    out = np.full(n, np.nan)
    if n <= m:
        return out
    x = np.arange(m, dtype=float)
    weights = (x - x.mean()) / np.sum((x - x.mean()) ** 2)
    win = np.lib.stride_tricks.sliding_window_view(c, m)[:-1]          # closes s-20..s-1 for s=m..n-1
    slope = np.add.reduce(win * weights, axis=1)                        # row-wise, prefix invariant
    starts = np.arange(m, n)
    ok = (seg_id[starts - m] == seg_id[starts]) & valid[starts]
    vw = np.lib.stride_tricks.sliding_window_view(valid, m)[:-1].all(axis=1)
    out[starts] = np.where(ok & vw, slope * (m - 1), np.nan)
    return out


# ------------------------------------------------------------------- confirmation
def confirm_close_beyond(s: Series, detected: int, high_level: float, low_level: float,
                         bullish: bool, window: int = CANDLE_CONFIRM_WINDOW,
                         buffer_atr: float = BREAKOUT_BUFFER_ATR, atr: float | None = None) -> int | None:
    """First bar j in (detected, detected+window] closing beyond the structure by
    buffer*ATR(detected).  Stops at a segment break (expiry, no confirmation)."""
    atr = s.atr_at(detected) if atr is None else atr
    if atr is None:
        return None
    level = high_level + buffer_atr * atr if bullish else low_level - buffer_atr * atr
    for j in range(detected + 1, min(detected + window, s.n - 1) + 1):
        if not s.clean(detected, j):
            return None
        if (s.c[j] > level) if bullish else (s.c[j] < level):
            return j
    return None


# ------------------------------------------------------------------------ events
def make_event(s: Series, *, signal_index: int, episode: int, state: str, direction: str,
               formation_start: int, detected_index: int, atr: float | None = None,
               score: float = 1.0, confirmed_index: int | None = None, geometry=None,
               alias_group: str | None = None, library_value=None, quality_tags=None) -> dict:
    if state not in STATES:
        raise ValueError(f"bad state {state}")
    if not (0 <= formation_start <= detected_index <= signal_index < s.n):
        raise ValueError("event indices must satisfy formation_start <= detected <= signal < n")
    if atr is None:
        atr = s.atr_at(signal_index)  # point-in-time ATR at the actionable bar
    if atr is None or not math.isfinite(atr):
        raise ValueError("event requires a finite prior ATR")
    tags = sorted(set(quality_tags or ()))
    if s.has_stub(formation_start, signal_index) and "session_stub" not in tags:
        tags.append("session_stub")
        tags.sort()
    event = {
        "signal_index": int(signal_index), "episode": int(episode), "state": state,
        "atr": _finite(atr), "score": _finite(score), "direction": direction,
        "pattern_start": s.time[formation_start], "formation_start_index": int(formation_start),
        "detected_index": int(detected_index),
    }
    if confirmed_index is not None:
        event["confirmed_index"] = int(confirmed_index)
    if geometry is not None:
        event["geometry"] = json_safe(geometry)
    if alias_group is not None:
        event["alias_group"] = str(alias_group)
    if library_value is not None:
        event["library_value"] = json_safe(library_value)
    if tags:
        event["quality_tags"] = tags
    return event


def _finite(x) -> float:
    x = float(x)
    if not math.isfinite(x):
        raise ValueError("non-finite value in event")
    return round(x, 8)


def json_safe(value):
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return _finite(value)
    if value is None or isinstance(value, str):
        return value
    raise TypeError(f"not JSON-safe: {type(value)}")


class Collector:
    """Accumulates events per key; rejects repeated (episode, state) pairs."""
    def __init__(self, keys=()):
        self.events = {tuple(k): [] for k in keys}
        self._seen = set()

    def add(self, key, event) -> bool:
        key = tuple(key)
        marker = (key, event["episode"], event["state"])
        if marker in self._seen:
            return False
        self._seen.add(marker)
        self.events.setdefault(key, []).append(event)
        return True

    def result(self) -> dict:
        return {k: sorted(v, key=lambda e: (e["signal_index"], e["episode"], _STATE_ORDER[e["state"]]))
                for k, v in self.events.items()}


# ------------------------------------------------------------------ specifications
def spec(pattern_id: str, variant: str, side: str, name: str, family: str, states, lookback: int,
         definition: str, **extra) -> dict:
    if side not in SIDES or family not in FAMILIES or not set(states) <= set(STATES):
        raise ValueError(f"bad spec {pattern_id}/{variant}/{side}")
    out = {"pattern_id": pattern_id, "variant": variant, "side": side, "name": name, "family": family,
           "definition_version": extra.pop("definition_version", DEFINITION_VERSION),
           "states": list(states), "lookback": int(lookback), "definition": definition}
    out.update(json_safe(extra))
    return out


def spec_keys(specs) -> list[tuple]:
    return [(d["pattern_id"], d["variant"], d["side"]) for d in specs]


def empty_result(specs) -> Collector:
    return Collector(spec_keys(specs))
