"""Step 1 -- diagnose (READ-ONLY) -- contract section 3.1.

Sweeps the NIFTY 500 universe in ``db/kanida.db`` and compares what we hold
**against itself**:

===========================  ==================================================
check                        what it compares
===========================  ==================================================
``intraday_low_below_daily``   15m low  vs the separately stored daily low
``intraday_high_above_daily``  15m high vs the separately stored daily high
``low_below_half_body``        15m low  vs its own open/close (coarse screen)
``high_above_double_body``     15m high vs its own open/close (coarse screen)
``ohlc_inconsistent``          low <= min(o,c), high >= max(o,c), high >= low
``non_positive_price``         finite, strictly positive prices
``zero_volume_with_move``      volume == 0 while high > low
``intrabucket_discontinuity``  a 5-minute source row against its own neighbours
``off_grid_source_bar``        a source row not on the 5-minute session grid
``session_bar_count``          this symbol's bar count vs the universe's mode
``missing_intraday_session``   a daily bar with no intraday bars
``orphan_intraday_session``    intraday bars with no daily bar
``cross_tf_close``             15m-aggregated session close vs the daily close
``cross_tf_range``             15m-aggregated session range vs the daily range
===========================  ==================================================

Every candidate is emitted **with the actual rows**, not just a count.  Nothing
is written to ``kanida.db``; nothing is repaired here.

The 15m-bar checks are delegated to W2's ``market_data.validate`` when it is
importable, so there is one set of rules and not two.  ``--engine fallback``
(or W2 being absent) runs the equivalent local implementation and the output
records which engine produced it, so a report can never silently claim W2's
rules ran when they did not.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from market_data.repair import (
    ARTIFACTS,
    KANIDA_DB,
    INVISIBLE_TO_ACCOUNT,
    KNOWN_SUSPENSIONS,
    read_only_conn,
    universe,
    flagged_symbols,
)

# ---------------------------------------------------------------------------
# tunables -- coarse *screening* thresholds, not certified error rules
# ---------------------------------------------------------------------------

DAILY_LOW_FACTOR = 0.8        # intraday low  < 0.8 * daily low
DAILY_HIGH_FACTOR = 1.2       # intraday high > 1.2 * daily high
HALF_BODY_FACTOR = 0.5
DOUBLE_BODY_FACTOR = 2.0
NEIGHBOUR_LOW_FACTOR = 0.80   # 5m low  < 0.80 * local median close
NEIGHBOUR_HIGH_FACTOR = 1.25  # 5m high > 1.25 * local median close
CROSS_TF_RANGE_TOL = 0.01     # 1%
EPS = 1e-9

# -- adjustment basis ---------------------------------------------------------
# ``ohlc_daily`` and ``ohlc_5min`` in kanida.db were backfilled at different
# times, so for a symbol with a corporate action between the two backfills the
# two tables sit on DIFFERENT adjustment bases before the ex-date.  Comparing
# them raw produces thousands of false "intraday low below daily low" flags
# (measured: RELIANCE 2015-2019 daily/intraday = 1.0095 = the 2020-05-13 rights
# adjustment; FEDERALBNK Feb-Jun 2015 = 4.01).  So every intraday/daily
# comparison is done on a per-session SCALE estimated from open and close --
# the two fields a single spurious extreme cannot move -- and the scale series
# itself is reported as evidence.
BASIS_TOL = 0.002             # |scale - 1| below this = same basis
SCALE_STABILITY_TOL = 0.01    # open-ratio vs close-ratio may differ by 1%
CLOSE_AUCTION_TOL = 0.025     # NSE's close is a 15:00-15:30 WAP, not the LTP
MIN_SEGMENT_SESSIONS = 5      # a basis segment shorter than this is noise

SOURCE_MINUTES = 5            # kanida.db's finest widely populated intraday table
SESSION_OPEN_MIN = 9 * 60 + 15
SESSION_CLOSE_MIN = 15 * 60 + 30
EXPECTED_15M_BARS = (SESSION_CLOSE_MIN - SESSION_OPEN_MIN) // 15   # 25

#: checks whose findings mean "this window's prices are disputed" and therefore
#: need re-fetching from the provider.
PRICE_DISPUTE_CHECKS = frozenset({
    "intraday_low_below_daily",
    "intraday_high_above_daily",
    "low_below_half_body",
    "high_above_double_body",
    "ohlc_inconsistent",
    "non_positive_price",
    "intrabucket_discontinuity",
    "cross_tf_close_divergence",
    "cross_tf_range",
    "session_scale_unstable",
    # W2 codes, lowercased by _run_w2_validation.  Deliberately excluded:
    #   cross_tf_close_diff  -- informational. Before 2026-08-03 the official
    #       close was a 30-minute VWAP, after it the closing-auction price, so
    #       the last continuous close never has to match the daily close.
    #   cross_tf_volume_gap  -- for a CAS stock the shortfall IS the auction
    #       volume.
    #   session_truncated / session_bar_count -- a session-shape statement,
    #       answered by the regime, not by re-fetching prices.
    "intraday_below_daily_low",
    "intraday_above_daily_high",
    "cross_tf_range_break",
    "cross_tf_range_shortfall",
    "ohlc_order",
    "non_finite",
    "non_positive",
    "duplicate_bar",
})

#: findings that describe a *basis* difference, not a disputed price.  They are
#: reported and re-fetched as a whole-history problem, not window by window.
BASIS_CHECKS = frozenset({"adjustment_basis_segment"})

MAX_EVIDENCE_PER_CHECK = 50   # worst-first; counts are always complete


# ---------------------------------------------------------------------------
# records
# ---------------------------------------------------------------------------


@dataclass
class Finding:
    symbol: str
    timeframe: str
    check: str
    bar_start: str
    severity: float
    evidence: dict

    def as_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, default=str)


@dataclass
class SymbolResult:
    symbol: str
    bars_15m: int = 0
    source_rows: int = 0
    daily_rows: int = 0
    sessions: int = 0
    first_bar: Optional[str] = None
    last_bar: Optional[str] = None
    first_daily: Optional[str] = None
    last_daily: Optional[str] = None
    counts: dict = field(default_factory=dict)
    evidence: dict = field(default_factory=dict)      # check -> worst findings
    session_bar_counts: dict = field(default_factory=dict)   # 'YYYY-MM-DD' -> n
    basis_segments: list = field(default_factory=list)
    dispute_dates: list = field(default_factory=list)
    is_fno: Optional[int] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# 15m aggregation (prefers W2's aggregate.to_15m; identical maths either way)
# ---------------------------------------------------------------------------


def _aggregate_15m_local(rows: Sequence[tuple]) -> list[dict]:
    """5-minute rows -> 15-minute bars on the session grid.  No synthetic bars."""
    buckets: dict[str, dict] = {}
    for bt, o, h, l, c, v in rows:
        day, hms = bt[:10], bt[11:]
        try:
            minute = int(hms[0:2]) * 60 + int(hms[3:5])
        except ValueError:
            continue
        offset = minute - SESSION_OPEN_MIN
        bucket_min = SESSION_OPEN_MIN + (offset // 15) * 15 if offset >= 0 else minute - (minute % 15)
        key = f"{day} {bucket_min // 60:02d}:{bucket_min % 60:02d}:00"
        b = buckets.get(key)
        if b is None:
            buckets[key] = {
                "bar_start": key, "session": day, "open": o, "high": h, "low": l,
                "close": c, "volume": int(v or 0), "n": 1, "first": bt, "last": bt,
            }
            continue
        if bt < b["first"]:
            b["first"], b["open"] = bt, o
        if bt > b["last"]:
            b["last"], b["close"] = bt, c
        if h is not None and (b["high"] is None or h > b["high"]):
            b["high"] = h
        if l is not None and (b["low"] is None or l < b["low"]):
            b["low"] = l
        b["volume"] += int(v or 0)
        b["n"] += 1
    return [buckets[k] for k in sorted(buckets)]


def aggregate_15m(rows: Sequence[tuple], *, engine: str = "auto"):
    """Return ``(dict_bars, w2_bar_objects_or_None, engine_used)``."""
    if engine in ("auto", "w2"):
        try:
            from market_data.aggregate import to_15m  # type: ignore

            bars = to_15m(rows, SOURCE_MINUTES)
            out = []
            for b in bars:
                start = b.bar_start if isinstance(b.bar_start, str) else b.bar_start.isoformat(sep=" ")
                out.append({
                    "bar_start": start, "session": start[:10], "open": b.open,
                    "high": b.high, "low": b.low, "close": b.close,
                    "volume": int(b.volume or 0), "n": getattr(b, "constituents", 0),
                    "first": start, "last": start,
                })
            return out, list(bars), "market_data.aggregate.to_15m"
        except Exception:
            if engine == "w2":
                raise
    return _aggregate_15m_local(rows), None, "repair.fallback"


# ---------------------------------------------------------------------------
# bar-level checks (prefers W2's validate; falls back to the same rules)
# ---------------------------------------------------------------------------


def _w2_bar_checks(bar: dict, daily: Optional[dict]):
    """Yield ``(check, severity, extra)`` from W2's validate module, if it has
    a per-bar entry point.  Returns ``None`` when W2 offers nothing usable."""
    from market_data import validate as V  # type: ignore

    for name in ("check_bar", "validate_bar", "bar_findings", "check_candle"):
        fn = getattr(V, name, None)
        if callable(fn):
            try:
                res = fn(bar, daily) if daily is not None else fn(bar)
            except TypeError:
                res = fn(bar)
            out = []
            for item in res or ():
                if isinstance(item, str):
                    out.append((item, 1.0, {}))
                elif isinstance(item, (tuple, list)):
                    code = str(item[0])
                    sev = float(item[1]) if len(item) > 1 else 1.0
                    extra = item[2] if len(item) > 2 and isinstance(item[2], dict) else {}
                    out.append((code, sev, extra))
                elif isinstance(item, dict):
                    out.append((str(item.get("code")), float(item.get("severity", 1.0)),
                                item.get("evidence", {}) or {}))
                else:
                    out.append((str(getattr(item, "code", item)),
                                float(getattr(item, "severity", 1.0)),
                                dict(getattr(item, "evidence", {}) or {})))
            return out
    return None


def bar_checks(bar: dict, daily: Optional[dict]) -> list[tuple[str, float, dict]]:
    """The local implementation of contract section 2's row rules."""
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    vol = bar.get("volume") or 0
    out: list[tuple[str, float, dict]] = []
    vals = (o, h, l, c)
    if any(v is None or not math.isfinite(v) or v <= 0 for v in vals):
        out.append(("non_positive_price", float("inf"), {}))
        return out
    if l > min(o, c) + EPS or h < max(o, c) - EPS or h < l - EPS:
        out.append(("ohlc_inconsistent",
                    max(l / min(o, c), max(o, c) / h) if min(o, c) and h else 1.0, {}))
    if l < HALF_BODY_FACTOR * min(o, c):
        out.append(("low_below_half_body", min(o, c) / l, {}))
    if h > DOUBLE_BODY_FACTOR * max(o, c):
        out.append(("high_above_double_body", h / max(o, c), {}))
    if vol == 0 and h > l + EPS:
        out.append(("zero_volume_with_move", (h - l) / l, {}))
    if daily:
        # ``scale`` maps intraday prices onto the daily table's adjustment
        # basis; 1.0 when the two agree.  Injected by the caller.
        scale = float(daily.get("_scale") or 1.0)
        dl, dh = daily.get("low"), daily.get("high")
        if dl and l * scale < DAILY_LOW_FACTOR * dl:
            out.append(("intraday_low_below_daily", dl / (l * scale),
                        {"daily": daily, "basis_scale": scale}))
        if dh and h * scale > DAILY_HIGH_FACTOR * dh:
            out.append(("intraday_high_above_daily", (h * scale) / dh,
                        {"daily": daily, "basis_scale": scale}))
    return out


#: W2 finding codes whose verdict depends on the adjustment basis.  For these we
#: recompute the comparison on a common basis and record whether the finding
#: survives; the finding itself is never suppressed.
_BASIS_SENSITIVE = {
    "INTRADAY_BELOW_DAILY_LOW": ("low", "low", "below"),
    "INTRADAY_ABOVE_DAILY_HIGH": ("high", "high", "above"),
    "CROSS_TF_DISAGREEMENT": (None, None, "cross"),
}


def symbol_calendar(calendar, symbol: str, w2_bars, is_fno=None):
    """W2's per-symbol session calendar, with the CAS regime derived from the
    bars themselves (contract section 2A).

    The regime is W2's ``RegimeBook.from_observed_last_bars``, not a second
    implementation here: ``instrument_labels.is_fno`` goes in only as the
    cross-check it is meant to be, never as an override.
    """
    try:
        from market_data.calendar import RegimeBook  # type: ignore
    except Exception:
        return calendar
    last_by_day: dict = {}
    for b in w2_bars:
        d = b.bar_start.date()
        if d not in last_by_day or b.bar_start > last_by_day[d]:
            last_by_day[d] = b.bar_start
    book = RegimeBook.from_observed_last_bars(symbol, last_by_day, calendar,
                                              is_fno=is_fno)
    return calendar.for_symbol(symbol, book)


def _run_w2_validation(symbol, w2_bars, daily, segment_scale, calendar_path, emit,
                       is_fno=None) -> str:
    """Run ``market_data.validate.validate_symbol`` and funnel it into ``emit``.

    Returns the engine id actually used, so the report can state it.
    """
    from market_data.validate import DEFAULT_CONFIG, validate_symbol  # type: ignore
    from market_data.calendar import SessionCalendar  # type: ignore

    calendar = symbol_calendar(SessionCalendar.load(calendar_path), symbol, w2_bars,
                               is_fno=is_fno)
    daily_by_date = {
        date.fromisoformat(k): {kk: vv for kk, vv in v.items() if kk != "bar_time"}
        for k, v in daily.items()
    }
    findings = validate_symbol(symbol, w2_bars, calendar, daily_by_date, DEFAULT_CONFIG)
    for f in findings:
        ev = dict(f.evidence or {})
        ev["message"] = f.message
        ev["severity"] = f.severity
        ev["w2_code"] = f.code
        sev_num = 1.0
        session_key = (f.bar_start or "")[:10]
        if f.code in _BASIS_SENSITIVE and session_key:
            sc = segment_scale.get(session_key, 1.0)
            ev["basis_scale"] = sc
            kind = _BASIS_SENSITIVE[f.code][2]
            if kind == "below":
                lo, dl = ev.get("intraday_low"), ev.get("daily_low")
                if lo and dl:
                    sev_num = dl / (lo * sc)
                    ev["explained_by_basis"] = sev_num <= 1 / DAILY_LOW_FACTOR
            elif kind == "above":
                hi, dh = ev.get("intraday_high"), ev.get("daily_high")
                if hi and dh:
                    sev_num = (hi * sc) / dh
                    ev["explained_by_basis"] = sev_num <= DAILY_HIGH_FACTOR
            else:
                ours, theirs = ev.get("aggregated"), ev.get("daily")
                if ours and theirs:
                    sev_num = abs((ours * sc) / theirs - 1.0)
                    ev["explained_by_basis"] = sev_num <= DEFAULT_CONFIG.cross_tf_tolerance
        else:
            sev_num = float(ev.get("deviation") or ev.get("ratio") or 1.0)
        emit(f.code.lower(), f.bar_start or "", abs(sev_num), ev)
    return "market_data.validate.validate_symbol"


# ---------------------------------------------------------------------------
# per-session adjustment-basis scale
# ---------------------------------------------------------------------------


def session_scale(agg: dict, daily: dict) -> tuple[Optional[float], Optional[float]]:
    """Estimate ``daily_price / intraday_price`` for one session.

    Uses **open and close only**.  A single spurious extreme (PIIND's 65.35)
    moves the low or the high but cannot move both the open and the close, so a
    scale built from open/close stays trustworthy exactly where we need it.

    Returns ``(scale, instability)`` where ``instability`` is the relative
    disagreement between the open-ratio and the close-ratio.  A big instability
    means "no single scale explains this session" -- that is a real
    contradiction, not a basis difference.
    """
    ratios = []
    for key in ("open", "close"):
        a, d = agg.get(key), daily.get(key)
        if a and d and a > 0 and d > 0:
            ratios.append(d / a)
    if not ratios:
        return None, None
    if len(ratios) == 1:
        return ratios[0], None
    lo, hi = min(ratios), max(ratios)
    return statistics.median(ratios), (hi - lo) / lo


SMOOTH_WINDOW = 21            # sessions, centred rolling median
BASIS_JUMP_TOL = 0.004        # a real basis break steps the smoothed scale;
                              # the closing-auction wobble, smoothed over 21
                              # sessions, stays an order of magnitude below this


def _rolling_median(values: Sequence[float], window: int) -> list[float]:
    half = window // 2
    n = len(values)
    return [statistics.median(values[max(0, i - half):min(n, i + half + 1)]) for i in range(n)]


def _rolling_mean(values: Sequence[float], window: int) -> list[float]:
    half = window // 2
    n = len(values)
    return [statistics.fmean(values[max(0, i - half):min(n, i + half + 1)]) for i in range(n)]


def _smooth(values: Sequence[float], window: int) -> list[float]:
    """Median first (kills outliers), then mean (kills the alternating wobble).

    A rolling median alone reproduces an alternating input as an alternating
    output, which would put a segment boundary between every pair of sessions.
    """
    return _rolling_mean(_rolling_median(values, window), window)


def basis_segments(scale_by_session: dict[str, float], *,
                   tol: float = BASIS_TOL,
                   jump_tol: float = BASIS_JUMP_TOL,
                   min_sessions: int = MIN_SEGMENT_SESSIONS,
                   ) -> tuple[list[dict], dict[str, float]]:
    """Collapse the per-session scale series into constant-scale segments.

    The raw per-session scale is noisy (NSE's closing auction moves it a few
    tenths of a percent), so it is smoothed with a centred rolling median
    before segmentation; a genuine adjustment-basis break is a *step* of at
    least ``jump_tol``.  A segment whose median scale differs from 1.0 by more
    than ``tol`` is a divergence between ``ohlc_daily`` and ``ohlc_5min``, and
    its boundary date is the corporate action that one table was re-fetched
    across and the other was not.

    Returns ``(segments, scale_for_session)`` -- the second is the *segment*
    scale, which is what every intraday/daily comparison uses; a single bad
    session can no longer set its own correction factor.
    """
    days = sorted(scale_by_session)
    if not days:
        return [], {}
    raw = [scale_by_session[d] for d in days]
    smooth = _smooth(raw, SMOOTH_WINDOW)

    # Compare each smoothed value with the *running median of the segment so
    # far*, not with its immediate predecessor: a predecessor comparison turns
    # an alternating wobble into a boundary at every step.
    bounds = [0]
    seg_vals = [smooth[0]]
    ref = smooth[0]
    for i in range(1, len(days)):
        if ref > 0 and abs(smooth[i] / ref - 1.0) > jump_tol:
            bounds.append(i)
            seg_vals = [smooth[i]]
            ref = smooth[i]
        else:
            seg_vals.append(smooth[i])
            ref = statistics.median(seg_vals[-250:])
    bounds.append(len(days))

    segs: list[dict] = []
    for a, b in zip(bounds, bounds[1:]):
        vals = raw[a:b]
        if not vals:
            continue
        med = statistics.median(vals)
        if segs and abs(med / segs[-1]["median_scale"] - 1.0) <= tol:
            prev = segs[-1]
            merged = raw[prev["_a"]:b]
            prev.update(end=days[b - 1], sessions=b - prev["_a"],
                        median_scale=statistics.median(merged),
                        min_scale=min(merged), max_scale=max(merged), _b=b)
            continue
        segs.append({"start": days[a], "end": days[b - 1], "sessions": b - a,
                     "median_scale": med, "min_scale": min(vals), "max_scale": max(vals),
                     "_a": a, "_b": b})

    scale_for_session: dict[str, float] = {}
    out: list[dict] = []
    for s in segs:
        a, b = s.pop("_a"), s.pop("_b")
        s["median_scale"] = round(s["median_scale"], 6)
        s["min_scale"] = round(s["min_scale"], 6)
        s["max_scale"] = round(s["max_scale"], 6)
        s["same_basis"] = abs(s["median_scale"] - 1.0) <= tol
        # A credible basis shift is a *constant* factor.  A segment whose scale
        # wanders (PIIND 2018-12: 5.08 .. 10.10) is corrupt data wearing a
        # basis shift's clothes, and must not be scaled away.
        s["spread"] = round(s["max_scale"] / s["min_scale"] - 1.0, 6) if s["min_scale"] else None
        s["reliable"] = (s["sessions"] >= min_sessions
                         and s["spread"] is not None and s["spread"] <= 0.05)
        out.append(s)
        # A short segment is not trusted to redefine the basis: fall back to
        # 1.0 so we surface the disagreement rather than scaling it away.
        eff = s["median_scale"] if s["reliable"] else 1.0
        for d in days[a:b]:
            scale_for_session[d] = eff
    return out, scale_for_session


# ---------------------------------------------------------------------------
# source-row (5-minute) discontinuity -- the check that would have caught PIIND
# ---------------------------------------------------------------------------


def intrabucket_discontinuities(rows: Sequence[tuple], window: int = 2) -> list[tuple[str, float, dict]]:
    """Flag a source row whose price contradicts its own immediate neighbours.

    PIIND's signature: ``... 1191.30, 66.00, 1191.30 ...``.  The row is
    internally consistent (``low <= open,close <= high``) and positive, so the
    existing row rules pass it.  Comparing against the *local median close* of
    the surrounding rows inside the same session catches it without needing an
    external source.
    """
    out: list[tuple[str, float, dict]] = []
    n = len(rows)
    if n < 2 * window + 1:
        return out
    sessions: dict[str, list[int]] = defaultdict(list)
    for i, r in enumerate(rows):
        sessions[r[0][:10]].append(i)
    for _day, idxs in sessions.items():
        m = len(idxs)
        if m < 2 * window + 1:
            continue
        closes = [rows[i][4] for i in idxs]
        # The session median is the robust anchor.  The local-window median
        # alone is not enough: next to a bad row its own neighbours inherit a
        # poisoned median and get flagged too.  A row must contradict BOTH.
        session_ref = statistics.median([c for c in closes if c and c > 0] or [0])
        for pos in range(m):
            lo_i = max(0, pos - window)
            hi_i = min(m, pos + window + 1)
            neigh = [closes[j] for j in range(lo_i, hi_i) if j != pos and closes[j]]
            if len(neigh) < 2:
                continue
            ref = statistics.median(neigh)
            if not ref or ref <= 0:
                continue
            i = idxs[pos]
            bt, o, h, l, c, v = rows[i]
            if l is None or h is None or l <= 0:
                continue
            sev = 0.0
            if l < NEIGHBOUR_LOW_FACTOR * ref and (
                    not session_ref or l < NEIGHBOUR_LOW_FACTOR * session_ref):
                sev = max(sev, ref / l)
            if h > NEIGHBOUR_HIGH_FACTOR * ref and (
                    not session_ref or h > NEIGHBOUR_HIGH_FACTOR * session_ref):
                sev = max(sev, h / ref)
            if sev:
                out.append((
                    "intrabucket_discontinuity", sev,
                    {
                        "source_row": {"bar_time": bt, "open": o, "high": h, "low": l,
                                       "close": c, "volume": v},
                        "neighbour_median_close": ref,
                        "session_median_close": session_ref,
                        "neighbours": [
                            {"bar_time": rows[idxs[j]][0], "close": closes[j]}
                            for j in range(lo_i, hi_i) if j != pos
                        ],
                        "source_timeframe": f"{SOURCE_MINUTES}minute",
                    },
                ))
    return out


# ---------------------------------------------------------------------------
# per-symbol sweep
# ---------------------------------------------------------------------------


def diagnose_symbol(symbol: str, *, db: Path = KANIDA_DB, engine: str = "auto",
                    since: Optional[str] = None,
                    calendar_path: Optional[str] = None) -> SymbolResult:
    res = SymbolResult(symbol=symbol)
    try:
        con = read_only_conn(db)
    except Exception as exc:
        res.error = f"db open failed: {exc}"
        return res
    try:
        where = "WHERE symbol=?"
        args: list[Any] = [symbol]
        if since:
            where += " AND bar_time>=?"
            args.append(since)
        rows = con.execute(
            f"SELECT bar_time,open,high,low,close,volume FROM ohlc_5min {where} ORDER BY bar_time",
            args,
        ).fetchall()
        rows = [tuple(r) for r in rows]
        daily_rows = con.execute(
            f"SELECT bar_time,open,high,low,close,volume FROM ohlc_daily {where} ORDER BY bar_time",
            args,
        ).fetchall()
        lab = con.execute("SELECT is_fno FROM instrument_labels WHERE symbol=?",
                          (symbol,)).fetchone()
        is_fno_flag = int(lab[0]) if lab and lab[0] is not None else None
    except Exception as exc:
        res.error = f"query failed: {exc}"
        return res
    finally:
        con.close()

    res.source_rows = len(rows)
    res.daily_rows = len(daily_rows)
    res.is_fno = is_fno_flag
    daily = {
        r["bar_time"][:10]: {"bar_time": r["bar_time"], "open": r["open"], "high": r["high"],
                             "low": r["low"], "close": r["close"], "volume": r["volume"]}
        for r in daily_rows
    }
    if daily:
        res.first_daily = min(daily)
        res.last_daily = max(daily)
    if not rows:
        return res

    counts: Counter = Counter()
    evidence: dict[str, list[Finding]] = defaultdict(list)
    dispute_dates: set[str] = set()

    def emit(check: str, bar_start: str, severity: float, ev: dict) -> None:
        counts[check] += 1
        # A finding the adjustment basis fully explains is still reported --
        # never suppressed -- but it is not a *disputed price*, so it does not
        # drag a window into the re-fetch plan.
        basis_explained = bool((ev or {}).get("explained_by_basis"))
        if basis_explained:
            counts[check + ":basis_explained"] += 1
        if check in PRICE_DISPUTE_CHECKS and not basis_explained:
            dispute_dates.add(bar_start[:10])
        bucket = evidence[check]
        bucket.append(Finding(symbol, "15minute", check, bar_start, float(severity), ev))
        if len(bucket) > MAX_EVIDENCE_PER_CHECK * 4:
            bucket.sort(key=lambda f: f.severity, reverse=True)
            del bucket[MAX_EVIDENCE_PER_CHECK:]

    # -- source grid ---------------------------------------------------------
    for bt, o, h, l, c, v in rows:
        try:
            minute = int(bt[11:13]) * 60 + int(bt[14:16])
        except (ValueError, IndexError):
            emit("off_grid_source_bar", bt, 1.0, {"reason": "unparseable timestamp"})
            continue
        row_ev = {"source_row": {"bar_time": bt, "open": o, "high": h, "low": l,
                                 "close": c, "volume": v},
                  "session_window": "09:15:00-15:30:00",
                  "grid_minutes": SOURCE_MINUTES}
        if minute < SESSION_OPEN_MIN or minute >= SESSION_CLOSE_MIN:
            # Muhurat / special sessions land here.  They are reclassified in
            # run() once we can see how many symbols share the same date.
            emit("outside_regular_session", bt, 1.0, row_ev)
        elif (minute - SESSION_OPEN_MIN) % SOURCE_MINUTES:
            emit("off_grid_source_bar", bt, 1.0, row_ev)

    for check, sev, ev in intrabucket_discontinuities(rows):
        emit(check, ev["source_row"]["bar_time"], sev, ev)

    # -- 15m bars ------------------------------------------------------------
    bars, w2_bars, agg_engine = aggregate_15m(rows, engine=engine)
    res.bars_15m = len(bars)
    res.first_bar = bars[0]["bar_start"] if bars else None
    res.last_bar = bars[-1]["bar_start"] if bars else None

    # pass 1: sessions (needed before any intraday/daily comparison so the
    # adjustment-basis scale is known)
    per_session: dict[str, dict] = {}
    for b in bars:
        s = per_session.get(b["session"])
        if s is None:
            per_session[b["session"]] = {"n": 1, "open": b["open"], "high": b["high"],
                                         "low": b["low"], "close": b["close"],
                                         "volume": b["volume"], "constituents": b["n"]}
        else:
            s["n"] += 1
            s["high"] = max(s["high"], b["high"])
            s["low"] = min(s["low"], b["low"])
            s["close"] = b["close"]
            s["volume"] += b["volume"]
            s["constituents"] += b["n"]

    res.sessions = len(per_session)
    res.session_bar_counts = {d: s["n"] for d, s in per_session.items()}

    scale_by_session: dict[str, float] = {}
    for day, s in per_session.items():
        d = daily.get(day)
        if d is None:
            emit("orphan_intraday_session", f"{day} 09:15:00", 1.0,
                 {"intraday_session": s, "daily": None})
            continue
        sc, instability = session_scale(s, d)
        if sc is None or sc <= 0:
            continue
        scale_by_session[day] = sc
        if instability is not None and instability > CLOSE_AUCTION_TOL:
            # No single scale reconciles this session's open and close: the two
            # tables disagree about the *shape* of the day, not just its level.
            emit("cross_tf_close_divergence", f"{day} 09:15:00", instability,
                 {"aggregated_session": s, "daily": d,
                  "open_ratio": d["open"] / s["open"] if s["open"] else None,
                  "close_ratio": d["close"] / s["close"] if s["close"] else None,
                  "instability": instability,
                  "note": "NSE's daily close is a 15:00-15:30 WAP, so a small "
                          "close-only difference is expected; this exceeds "
                          f"{CLOSE_AUCTION_TOL:.1%}"})

    segments, segment_scale = basis_segments(scale_by_session)
    res.basis_segments = segments
    for seg in segments:
        if not seg["same_basis"] and seg["reliable"]:
            emit("adjustment_basis_segment", f"{seg['start']} 09:15:00",
                 abs(seg["median_scale"] - 1.0),
                 {"segment": seg,
                  "meaning": "ohlc_daily and ohlc_5min are on different "
                             "adjustment bases across this span "
                             "(daily / intraday = median_scale)"})

    # pass 2: the contract's section-2 checks.  These are W2's, not a second
    # set -- ``market_data.validate.validate_symbol`` owns the rules and the
    # thresholds; we only annotate its findings with the adjustment basis so a
    # basis difference is not reported as a price error.
    checks_engine = "repair.fallback"
    if engine in ("auto", "w2") and w2_bars:
        try:
            checks_engine = _run_w2_validation(
                symbol, w2_bars, daily, segment_scale, calendar_path, emit,
                is_fno=is_fno_flag)
        except Exception as exc:
            if engine == "w2":
                raise
            res.error = f"validate_symbol failed, fell back: {type(exc).__name__}: {exc}"
    if checks_engine == "repair.fallback":
        for b in bars:
            d = daily.get(b["session"])
            if d is not None:
                d = dict(d)
                d["_scale"] = segment_scale.get(b["session"], 1.0)
            for check, sev, ev in bar_checks(b, d):
                payload = {"bar": {k: b[k] for k in ("bar_start", "open", "high", "low",
                                                     "close", "volume", "n")}}
                payload.update(ev or {})
                if d and "daily" not in payload:
                    payload["daily"] = d
                emit(check, b["bar_start"], sev, payload)
        for day, s in per_session.items():
            d = daily.get(day)
            if d is None or not d.get("high") or not d.get("low"):
                continue
            sc = segment_scale.get(day, 1.0)
            if s["high"] * sc > d["high"] * (1 + CROSS_TF_RANGE_TOL) or \
                    s["low"] * sc < d["low"] * (1 - CROSS_TF_RANGE_TOL):
                emit("cross_tf_range", f"{day} 09:15:00",
                     max((s["high"] * sc) / d["high"],
                         d["low"] / (s["low"] * sc) if s["low"] else 1.0),
                     {"aggregated_session": s, "daily": d, "basis_scale": sc})

    if bars:
        lo_day, hi_day = bars[0]["session"], bars[-1]["session"]
        suspensions = KNOWN_SUSPENSIONS.get(symbol, [])
        for day, d in daily.items():
            if day < lo_day or day > hi_day or day in per_session:
                continue
            note = next((n for a, b_, n in suspensions if a <= day <= b_), None)
            emit("missing_intraday_session", f"{day} 09:15:00", 1.0,
                 {"daily": d, "known_suspension": note})

    res.counts = dict(counts)
    res.evidence = {
        k: [asdict(f) for f in sorted(v, key=lambda f: f.severity, reverse=True)[:MAX_EVIDENCE_PER_CHECK]]
        for k, v in evidence.items()
    }
    res.dispute_dates = sorted(dispute_dates)
    res.counts["_aggregate_engine"] = agg_engine  # type: ignore[assignment]
    res.counts["_checks_engine"] = checks_engine  # type: ignore[assignment]
    return res


def _worker(args) -> dict:
    symbol, db, engine, since, calendar_path = args
    try:
        return asdict(diagnose_symbol(symbol, db=Path(db), engine=engine, since=since,
                                      calendar_path=calendar_path))
    except Exception as exc:  # pragma: no cover - worker safety net
        return asdict(SymbolResult(symbol=symbol, error=f"{type(exc).__name__}: {exc}"))


# ---------------------------------------------------------------------------
# dispute windows -> the re-fetch plan's input
# ---------------------------------------------------------------------------


def merge_windows(days: Iterable[str], pad_days: int = 5,
                  join_gap_days: int = 90) -> list[tuple[str, str]]:
    """Merge dispute dates into padded, non-overlapping ``(start, end)`` ranges.

    ``join_gap_days`` is deliberately generous: a 200-day provider request
    costs exactly as much as a 5-day one, so joining nearby windows buys wider
    verification for fewer requests.
    """
    ds = sorted({date.fromisoformat(d) for d in days})
    if not ds:
        return []
    out: list[list[date]] = []
    for d in ds:
        lo, hi = d - timedelta(days=pad_days), d + timedelta(days=pad_days)
        if out and lo <= out[-1][1] + timedelta(days=join_gap_days):
            out[-1][1] = max(out[-1][1], hi)
        else:
            out.append([lo, hi])
    return [(a.isoformat(), b.isoformat()) for a, b in out]


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def run(symbols: Optional[Sequence[str]] = None, *, db: Path = KANIDA_DB,
        workers: int = 8, engine: str = "auto", since: Optional[str] = None,
        out_dir: Path = ARTIFACTS / "diagnose", progress: bool = True) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()

    if symbols is None:
        con = read_only_conn(db)
        try:
            symbols = [s.symbol for s in universe(con)]
        finally:
            con.close()
    symbols = list(symbols)

    # One session calendar for the whole sweep, derived read-only from the
    # legacy DB's always-traded reference symbols (W2 owns the derivation, and
    # it knows the Muhurat sessions).  Built once so 500 workers do not each
    # rescan, and cached so a rerun is instant.
    calendar_path = out_dir / "session_calendar.json"
    if not calendar_path.exists():
        from market_data.calendar import SessionCalendar  # type: ignore
        SessionCalendar.from_kanida_db(db, cache_path=calendar_path)

    results: dict[str, dict] = {}
    tasks = [(s, str(db), engine, since, str(calendar_path)) for s in symbols]
    if workers <= 1:
        for i, t in enumerate(tasks, 1):
            r = _worker(t)
            results[r["symbol"]] = r
            if progress and (i % 25 == 0 or i == len(tasks)):
                print(f"[diagnose] {i}/{len(tasks)} {r['symbol']}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_worker, t): t[0] for t in tasks}
            for i, fut in enumerate(as_completed(futs), 1):
                r = fut.result()
                results[r["symbol"]] = r
                if progress and (i % 25 == 0 or i == len(tasks)):
                    print(f"[diagnose] {i}/{len(tasks)} {r['symbol']}", flush=True)

    # -- Muhurat / special sessions are not a defect -------------------------
    # A bar outside 09:15-15:30 is only suspicious if it is this symbol's alone.
    # When most of the universe traded at the same unusual hour it is a special
    # session (Diwali Muhurat), and the calendar -- not the data -- is at fault.
    outside_dates: Counter = Counter()
    for r in results.values():
        for f in (r.get("evidence") or {}).get("outside_regular_session", []):
            outside_dates[f["bar_start"][:10]] += 1
    special_sessions = sorted(d for d, n in outside_dates.items() if n >= 20)
    for r in results.values():
        kept = [f for f in (r.get("evidence") or {}).get("outside_regular_session", [])
                if f["bar_start"][:10] not in special_sessions]
        if (r.get("evidence") or {}).get("outside_regular_session") is not None:
            if kept:
                r["evidence"]["outside_regular_session"] = kept
            else:
                r["evidence"].pop("outside_regular_session", None)
                (r.get("counts") or {}).pop("outside_regular_session", None)

    # -- session bar-count anomalies, judged against the universe's own mode --
    per_date: dict[str, Counter] = defaultdict(Counter)
    for r in results.values():
        for d, n in (r.get("session_bar_counts") or {}).items():
            per_date[d][n] += 1
    modal = {d: c.most_common(1)[0][0] for d, c in per_date.items()}
    breadth = {d: sum(c.values()) for d, c in per_date.items()}

    for r in results.values():
        bad = []
        for d, n in (r.get("session_bar_counts") or {}).items():
            exp = modal.get(d)
            # only judge a date at least 20 symbols traded, so a thin day is not
            # mistaken for a per-symbol defect
            if exp is None or breadth.get(d, 0) < 20 or n == exp:
                continue
            bad.append({"session": d, "bars": n, "universe_modal_bars": exp,
                        "symbols_on_date": breadth[d]})
        if bad:
            r.setdefault("counts", {})["session_bar_count"] = len(bad)
            r.setdefault("evidence", {})["session_bar_count"] = [
                {"symbol": r["symbol"], "timeframe": "15minute", "check": "session_bar_count",
                 "bar_start": f"{b['session']} 09:15:00", "severity": abs(b["bars"] - b["universe_modal_bars"]),
                 "evidence": b}
                for b in sorted(bad, key=lambda b: abs(b["bars"] - b["universe_modal_bars"]),
                                reverse=True)[:MAX_EVIDENCE_PER_CHECK]
            ]

    # -- write artifacts -----------------------------------------------------
    findings_path = out_dir / "findings.jsonl"
    with findings_path.open("w", encoding="utf-8") as fh:
        for sym in sorted(results):
            for check, items in (results[sym].get("evidence") or {}).items():
                for f in items:
                    fh.write(json.dumps(f, sort_keys=True, default=str) + "\n")

    totals: Counter = Counter()
    for r in results.values():
        for k, v in (r.get("counts") or {}).items():
            if not k.startswith("_") and isinstance(v, int):
                totals[k] += v

    coverage = {
        sym: {"first_bar": r.get("first_bar"), "last_bar": r.get("last_bar"),
              "bars_15m": r.get("bars_15m"), "source_rows": r.get("source_rows"),
              "sessions": r.get("sessions"), "first_daily": r.get("first_daily"),
              "last_daily": r.get("last_daily"), "error": r.get("error")}
        for sym, r in sorted(results.items())
    }
    disputed = {
        sym: merge_windows(r.get("dispute_dates") or [])
        for sym, r in sorted(results.items()) if r.get("dispute_dates")
    }
    basis = {
        sym: [s for s in (r.get("basis_segments") or [])
              if not s["same_basis"] and s.get("reliable")]
        for sym, r in sorted(results.items())
        if any(not s["same_basis"] and s.get("reliable")
               for s in (r.get("basis_segments") or []))
    }

    engines = Counter()
    for r in results.values():
        engines[(r.get("counts") or {}).get("_checks_engine", "n/a")] += 1

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "database": str(db),
        "read_only": True,
        "universe_size": len(symbols),
        "elapsed_seconds": round(time.time() - started, 1),
        "thresholds": {
            "daily_low_factor": DAILY_LOW_FACTOR, "daily_high_factor": DAILY_HIGH_FACTOR,
            "half_body_factor": HALF_BODY_FACTOR, "double_body_factor": DOUBLE_BODY_FACTOR,
            "neighbour_low_factor": NEIGHBOUR_LOW_FACTOR,
            "neighbour_high_factor": NEIGHBOUR_HIGH_FACTOR,
            "cross_tf_range_tol": CROSS_TF_RANGE_TOL,
            "basis_tol": BASIS_TOL, "close_auction_tol": CLOSE_AUCTION_TOL,
        },
        "checks_engine_by_symbol_count": dict(engines),
        "special_sessions_detected": special_sessions,
        "symbols_with_adjustment_basis_break": sorted(basis),
        "totals": dict(totals),
        "symbols_with_findings": sorted(
            s for s, r in results.items()
            if any(not k.startswith("_") for k in (r.get("counts") or {}))
        ),
        "symbols_with_no_intraday_data": sorted(
            s for s, r in results.items() if not r.get("source_rows")
        ),
        "errors": {s: r["error"] for s, r in results.items() if r.get("error")},
        "per_symbol": {
            s: {k: v for k, v in (r.get("counts") or {}).items()}
            for s, r in sorted(results.items()) if r.get("counts")
        },
    }

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    (out_dir / "coverage.json").write_text(json.dumps(coverage, indent=2, default=str), encoding="utf-8")
    (out_dir / "disputed_windows.json").write_text(json.dumps(disputed, indent=2), encoding="utf-8")
    (out_dir / "basis_segments.json").write_text(json.dumps(basis, indent=2), encoding="utf-8")
    (out_dir / "results.json").write_text(
        json.dumps({s: {k: v for k, v in r.items() if k != "session_bar_counts"}
                    for s, r in results.items()}, indent=2, default=str),
        encoding="utf-8")

    if progress:
        print(json.dumps({"elapsed_s": summary["elapsed_seconds"],
                          "totals": summary["totals"],
                          "symbols_with_findings": len(summary["symbols_with_findings"])},
                         indent=2), flush=True)
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="read-only data diagnosis over NIFTY 500")
    ap.add_argument("--symbols", nargs="*", help="limit to these symbols")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 2))
    ap.add_argument("--engine", choices=("auto", "w2", "fallback"), default="auto")
    ap.add_argument("--since", help="only examine bars at/after this timestamp")
    ap.add_argument("--out", default=str(ARTIFACTS / "diagnose"))
    a = ap.parse_args(argv)
    run(a.symbols, workers=a.workers, engine=a.engine, since=a.since, out_dir=Path(a.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
