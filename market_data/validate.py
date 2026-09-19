"""Row-level validation -- the checks the old pipeline did not have.

Contract: docs/DATA_PIPELINE_CONTRACT.md section 2, list items 1-5.

Why this module exists
----------------------
`market_scanner/data.py:125` only checked that a row was finite, positive and
internally ordered, and its discontinuity check compared the *aggregated*
candle's open against the previous aggregated close. The PIIND 2019-09-05
11:22 row (O/H/L/C ~ 65.35 sitting between ~1192 rows) satisfies every one of
those checks, so an ~94% intrabucket price break propagated into the frozen 1H
and 4H history with ``gap=false`` and produced a fake 50.9% short return.

The checks here work **on the source rows inside a bucket** and **against the
separately stored daily range**, which is what would have caught it.

Guarantees
----------
* Nothing is dropped and nothing is invented. Checks only produce `Finding`s
  and set `quality_flags` on the rows.
* Every finding carries structured `evidence` (the actual numbers) so a flag
  can be argued with.
"""

from __future__ import annotations

import json
import math
import statistics
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime
from typing import Iterable, Mapping, Sequence

from market_data.aggregate import SQLITE_MAX_INT, Bar, aggregate, bar_from_raw
from market_data.calendar import CAS_START, SessionCalendar, SymbolCalendar

SEVERITY_ERROR = "error"
SEVERITY_WARN = "warn"
SEVERITY_INFO = "info"

_SEVERITY_RANK = {SEVERITY_INFO: 0, SEVERITY_WARN: 1, SEVERITY_ERROR: 2}

# Codes that describe a whole session rather than one row. Their `bar_start` is
# the session open (so the finding is locatable), but the flag belongs on every
# row of that session, not just the first one.
SESSION_LEVEL_CODES = {"SESSION_BAR_COUNT", "SESSION_TRUNCATED",
                       "SESSION_EXCESS_BARS", "UNKNOWN_SESSION_DAY"}

# code -> quality flag written onto candles_15m.quality_flags
FLAG_BY_CODE = {
    "NON_FINITE": "non_finite",
    "NON_POSITIVE": "non_positive",
    "OHLC_ORDER": "ohlc_order",
    "NEGATIVE_VOLUME": "negative_volume",
    "VOLUME_OUT_OF_RANGE": "volume_out_of_range",
    "DUPLICATE_BAR": "duplicate",
    "INTRABUCKET_DISCONTINUITY": "intrabucket_discontinuity",
    "INTRADAY_BELOW_DAILY_LOW": "intraday_vs_daily",
    "INTRADAY_ABOVE_DAILY_HIGH": "intraday_vs_daily",
    "DAILY_RECONCILE_MISSING": "no_daily_reference",
    "SESSION_BAR_COUNT": "short_session",
    "SESSION_TRUNCATED": "session_truncated",
    "SESSION_EXCESS_BARS": "excess_bars",
    "OFF_GRID_BAR": "off_grid",
    "BAR_OUTSIDE_SESSION": "outside_session",
    "UNKNOWN_SESSION_DAY": "unknown_session_day",
    "ZERO_VOLUME_PRICE_MOVE": "zero_volume_move",
    "CROSS_TF_RANGE_BREAK": "cross_tf_range",
    "CROSS_TF_RANGE_SHORTFALL": "cross_tf_range",
    "CROSS_TF_VOLUME_GAP": "cross_tf_volume",
    "CROSS_TF_CLOSE_DIFF": "cross_tf_close_note",
    "REGIME_NOT_APPLIED": "regime_not_applied",
}


@dataclass(frozen=True)
class ValidationConfig:
    """Thresholds. Deliberately explicit so a run can record what it used."""

    # 1. intrabucket discontinuity, vs the median close of the bucket
    intrabucket_bucket: str = "session"   # 'session' | '1H'
    intrabucket_warn: float = 0.25
    intrabucket_error: float = 0.40
    intrabucket_min_rows: int = 5
    # 2. intraday vs daily reconciliation (contract: 0.8x low / 1.2x high)
    daily_low_factor: float = 0.80
    daily_high_factor: float = 1.20
    # 4. session bar count
    session_count_tolerance: int = 0      # bars missing before we warn
    # 5. cross-timeframe agreement with the provider's own daily bars.
    #    Compare RANGE CONTAINMENT and VOLUME, not closes: NSE's official daily
    #    close is the VWAP of the last 30 minutes, not the last traded price,
    #    so a 0.1-0.5% close difference is normal on a complete session and is
    #    reported as `info` only.
    cross_tf_range_tolerance: float = 0.02
    cross_tf_range_error: float = 0.10
    cross_tf_volume_tolerance: float = 0.15
    cross_tf_close_info: float = 0.01
    zero_volume_move_min: float = 0.0     # any move on zero volume is flagged


DEFAULT_CONFIG = ValidationConfig()


@dataclass(frozen=True)
class Finding:
    code: str
    severity: str
    symbol: str
    timeframe: str
    bar_start: str | None
    message: str
    evidence: dict = field(default_factory=dict)

    @property
    def flag(self) -> str:
        return FLAG_BY_CODE.get(self.code, self.code.lower())

    def to_row(self) -> dict:
        d = asdict(self)
        d["evidence"] = json.dumps(d["evidence"], default=str, sort_keys=True)
        return d


def _finite(x) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def _key(bar: Bar) -> str:
    return bar.bar_start.isoformat(sep=" ")


# ---------------------------------------------------------------------------
# 3. ordering / positivity  (contract item 3)
# ---------------------------------------------------------------------------
def check_ordering(symbol: str, bars: Sequence[Bar],
                   timeframe: str = "15m") -> list[Finding]:
    out: list[Finding] = []
    for b in bars:
        vals = {"open": b.open, "high": b.high, "low": b.low, "close": b.close}
        bad = {k: v for k, v in vals.items() if not _finite(v)}
        if bad:
            out.append(Finding("NON_FINITE", SEVERITY_ERROR, symbol, timeframe,
                               _key(b), f"non-finite price fields: {sorted(bad)}",
                               {"values": {k: str(v) for k, v in vals.items()}}))
            continue
        if min(vals.values()) <= 0:
            out.append(Finding("NON_POSITIVE", SEVERITY_ERROR, symbol, timeframe,
                               _key(b), "non-positive price", {"values": vals}))
        if b.high < max(b.open, b.close, b.low) or b.low > min(b.open, b.close):
            out.append(Finding("OHLC_ORDER", SEVERITY_ERROR, symbol, timeframe,
                               _key(b),
                               "OHLC ordering violated (need low<=min(o,c), high>=max(o,c))",
                               {"values": vals}))
        if not _finite(b.volume) or b.volume < 0:
            out.append(Finding("NEGATIVE_VOLUME", SEVERITY_ERROR, symbol, timeframe,
                               _key(b), "negative or non-finite volume",
                               {"volume": b.volume}))
        elif b.volume > SQLITE_MAX_INT:
            # A real case in db/kanida.db: a legacy volume larger than a signed
            # 64-bit integer. The row is still stored (nothing is dropped) but
            # the volume cannot be held exactly by SQLite, so it is flagged and
            # the exact value is preserved here as evidence.
            out.append(Finding("VOLUME_OUT_OF_RANGE", SEVERITY_ERROR, symbol,
                               timeframe, _key(b),
                               f"volume {b.volume} exceeds the signed 64-bit "
                               f"range and cannot be stored exactly",
                               {"volume": str(b.volume),
                                "sqlite_max_int": SQLITE_MAX_INT,
                                "bar": b.as_dict()}))
    return out


# ---------------------------------------------------------------------------
# 4a. duplicates  (contract item 4)
# ---------------------------------------------------------------------------
def check_duplicates(symbol: str, bars: Sequence[Bar],
                     timeframe: str = "15m") -> list[Finding]:
    seen: dict[datetime, Bar] = {}
    out: list[Finding] = []
    for b in bars:
        prev = seen.get(b.bar_start)
        if prev is None:
            seen[b.bar_start] = b
            continue
        identical = (prev.open, prev.high, prev.low, prev.close, prev.volume) == \
                    (b.open, b.high, b.low, b.close, b.volume)
        out.append(Finding(
            "DUPLICATE_BAR", SEVERITY_ERROR if not identical else SEVERITY_WARN,
            symbol, timeframe, _key(b),
            "duplicate bar_start" + ("" if identical else " with differing OHLCV"),
            {"first": prev.as_dict(), "second": b.as_dict()}))
    return out


# ---------------------------------------------------------------------------
# 1. intrabucket discontinuity  (contract item 1) -- the PIIND check
# ---------------------------------------------------------------------------
def _bucket_key(bar: Bar, mode: str) -> tuple:
    if mode == "1H":
        return (bar.bar_start.date(), bar.bar_start.hour)
    return (bar.bar_start.date(),)


def check_intrabucket_discontinuity(
        symbol: str, bars: Sequence[Bar], timeframe: str = "15m",
        config: ValidationConfig = DEFAULT_CONFIG) -> list[Finding]:
    """Flag a row whose prices deviate from its bucket's median close.

    This is the check whose absence let PIIND through: it compares *source
    rows inside a bucket* with each other, not the aggregated candle's open
    with the previous aggregated close.
    """
    buckets: dict[tuple, list[Bar]] = {}
    for b in bars:
        buckets.setdefault(_bucket_key(b, config.intrabucket_bucket), []).append(b)

    out: list[Finding] = []
    for key, group in sorted(buckets.items()):
        if len(group) < config.intrabucket_min_rows:
            continue
        closes = [b.close for b in group if _finite(b.close) and b.close > 0]
        if len(closes) < config.intrabucket_min_rows:
            continue
        median = statistics.median(closes)
        if median <= 0:
            continue
        for b in group:
            fields = {"open": b.open, "high": b.high, "low": b.low, "close": b.close}
            devs = {k: abs(v / median - 1.0) for k, v in fields.items()
                    if _finite(v) and v > 0}
            if not devs:
                continue
            worst_field = max(devs, key=devs.get)
            worst = devs[worst_field]
            if worst < config.intrabucket_warn:
                continue
            severity = SEVERITY_ERROR if worst >= config.intrabucket_error else SEVERITY_WARN
            out.append(Finding(
                "INTRABUCKET_DISCONTINUITY", severity, symbol, timeframe, _key(b),
                f"{worst_field}={fields[worst_field]:.4f} deviates "
                f"{worst * 100:.2f}% from the {config.intrabucket_bucket} median "
                f"close {median:.4f}",
                {"bucket": str(key), "bucket_median_close": median,
                 "field": worst_field, "value": fields[worst_field],
                 "deviation": worst, "bucket_rows": len(group),
                 "bar": b.as_dict()}))
    return out


# ---------------------------------------------------------------------------
# 2. intraday vs daily reconciliation  (contract item 2)
# ---------------------------------------------------------------------------
def check_intraday_vs_daily(
        symbol: str, bars: Sequence[Bar],
        daily: Mapping[date, Mapping[str, float]],
        timeframe: str = "15m",
        config: ValidationConfig = DEFAULT_CONFIG) -> list[Finding]:
    """intraday low >= 0.8 x daily low, intraday high <= 1.2 x daily high.

    `daily` maps a session date to {'open','high','low','close','volume'} from
    the *separately stored* daily series, same adjustment basis.
    """
    out: list[Finding] = []
    missing: set[date] = set()
    for b in bars:
        d = b.bar_start.date()
        ref = daily.get(d)
        if ref is None:
            missing.add(d)
            continue
        dl, dh = ref.get("low"), ref.get("high")
        if not (_finite(dl) and _finite(dh)) or dl <= 0 or dh <= 0:
            continue
        if _finite(b.low) and b.low > 0 and b.low < config.daily_low_factor * dl:
            out.append(Finding(
                "INTRADAY_BELOW_DAILY_LOW", SEVERITY_ERROR, symbol, timeframe,
                _key(b),
                f"intraday low {b.low:.4f} < {config.daily_low_factor:g} x daily low {dl:.4f}",
                {"intraday_low": b.low, "daily_low": dl,
                 "ratio": b.low / dl, "session": d.isoformat(),
                 "daily": dict(ref), "bar": b.as_dict()}))
        if _finite(b.high) and b.high > config.daily_high_factor * dh:
            out.append(Finding(
                "INTRADAY_ABOVE_DAILY_HIGH", SEVERITY_ERROR, symbol, timeframe,
                _key(b),
                f"intraday high {b.high:.4f} > {config.daily_high_factor:g} x daily high {dh:.4f}",
                {"intraday_high": b.high, "daily_high": dh,
                 "ratio": b.high / dh, "session": d.isoformat(),
                 "daily": dict(ref), "bar": b.as_dict()}))
    if missing:
        out.append(Finding(
            "DAILY_RECONCILE_MISSING", SEVERITY_WARN, symbol, timeframe, None,
            f"{len(missing)} session(s) had no daily reference bar to reconcile against",
            {"sessions": sorted(d.isoformat() for d in missing)[:50],
             "session_count": len(missing)}))
    return out


# ---------------------------------------------------------------------------
# 4b. session bar counts / grid alignment  (contract item 4)
# ---------------------------------------------------------------------------
def check_session_bars(symbol: str, bars: Sequence[Bar],
                       calendar: SessionCalendar, timeframe: str = "15m",
                       config: ValidationConfig = DEFAULT_CONFIG) -> list[Finding]:
    out: list[Finding] = []
    by_day: dict[date, list[Bar]] = {}
    for b in bars:
        by_day.setdefault(b.bar_start.date(), []).append(b)

    for day in sorted(by_day):
        group = by_day[day]
        session = calendar.session(day)
        if session is None:
            out.append(Finding(
                "UNKNOWN_SESSION_DAY", SEVERITY_WARN, symbol, timeframe,
                group[0].bar_start.isoformat(sep=" "),
                f"{len(group)} bar(s) on {day} which the calendar has no session for "
                f"(holiday, or a session the reference universe did not observe)",
                {"day": day.isoformat(), "bars": len(group)}))
            continue
        grid_list = session.bar_starts()
        grid = set(grid_list)
        stamps = {b.bar_start for b in group}
        off_grid = sorted(stamps - grid)
        for stamp in off_grid:
            code = ("BAR_OUTSIDE_SESSION"
                    if not (session.start <= stamp < session.end) else "OFF_GRID_BAR")
            out.append(Finding(
                code, SEVERITY_ERROR, symbol, timeframe, stamp.isoformat(sep=" "),
                f"bar at {stamp} is not on the {session.kind} session grid "
                f"{session.start:%H:%M}-{session.end:%H:%M}",
                {"session_start": session.start.isoformat(sep=" "),
                 "session_end": session.end.isoformat(sep=" ")}))
        present = len(stamps & grid)
        if present < session.expected_bars - config.session_count_tolerance:
            # A clean *suffix* of missing bars is a truncated session, not a
            # hole: Kite intraday history has stopped at 15:00 instead of 15:15
            # since 2026-08-03 while its daily bars stay complete.
            tail = 0
            while tail < len(grid_list) and grid_list[len(grid_list) - 1 - tail] not in stamps:
                tail += 1
            truncated = tail > 0 and present == session.expected_bars - tail
            out.append(Finding(
                "SESSION_TRUNCATED" if truncated else "SESSION_BAR_COUNT",
                SEVERITY_WARN, symbol, timeframe,
                session.start.isoformat(sep=" "),
                (f"session truncated: last {tail} bar(s) missing, data stops at "
                 f"{grid_list[len(grid_list) - 1 - tail]:%H:%M} instead of "
                 f"{grid_list[-1]:%H:%M} on {day}") if truncated else
                (f"{present}/{session.expected_bars} expected 15m bars on "
                 f"{day} ({session.kind})"),
                {"day": day.isoformat(), "present": present,
                 "expected": session.expected_bars, "kind": session.kind,
                 "missing_tail_bars": tail,
                 "truncated_from": (grid_list[len(grid_list) - tail].isoformat(sep=" ")
                                    if truncated else None),
                 "missing": [s.isoformat(sep=" ") for s in sorted(grid - stamps)][:30]}))
        if len(group) > session.expected_bars:
            out.append(Finding(
                "SESSION_EXCESS_BARS", SEVERITY_ERROR, symbol, timeframe,
                session.start.isoformat(sep=" "),
                f"{len(group)} bars on {day} exceeds the expected "
                f"{session.expected_bars}",
                {"day": day.isoformat(), "rows": len(group),
                 "expected": session.expected_bars}))
    return out


# ---------------------------------------------------------------------------
# 4c. zero-volume prints with price movement  (contract item 4)
# ---------------------------------------------------------------------------
def check_zero_volume_moves(symbol: str, bars: Sequence[Bar],
                            timeframe: str = "15m",
                            config: ValidationConfig = DEFAULT_CONFIG) -> list[Finding]:
    out: list[Finding] = []
    for b in bars:
        if b.volume != 0:
            continue
        if not all(_finite(v) for v in (b.open, b.high, b.low, b.close)):
            continue
        move = max(b.high - b.low, abs(b.close - b.open))
        if move > config.zero_volume_move_min:
            out.append(Finding(
                "ZERO_VOLUME_PRICE_MOVE", SEVERITY_WARN, symbol, timeframe, _key(b),
                f"zero volume but price moved {move:.4f}", {"bar": b.as_dict()}))
    return out


# ---------------------------------------------------------------------------
# 5. cross-timeframe agreement with the provider's own daily bars
# ---------------------------------------------------------------------------
def check_cross_timeframe(symbol: str, bars: Sequence[Bar],
                          daily: Mapping[date, Mapping[str, float]],
                          calendar: SessionCalendar,
                          config: ValidationConfig = DEFAULT_CONFIG) -> list[Finding]:
    """Aggregate 15m -> 1D and compare with the separately stored daily bar.

    What is compared, and why:

    * **range containment** -- the session's intraday extremes must sit inside
      the provider's daily high/low (and must not fall far short of them).
      This is the real cross-timeframe signal.
    * **volume** -- the summed intraday volume should match the daily volume.
      Skipped on an incomplete/truncated session, where a shortfall is expected
      and already reported as SESSION_TRUNCATED.
    * **close** -- reported as `info` only, and **not at all for a CAS
      session**. NSE's official daily close is the **VWAP of the last 30
      minutes**, not the last traded price, so a 0.1-0.5% difference from the
      last intraday close is normal on a complete session and is NOT a data
      error. (Measured: RELIANCE 2026-07-31 last intraday close 1305.00 vs
      daily close 1307.80 on a complete session.) From 2026-08-03 a CAS stock's
      official close is an auction price that is not in the intraday series at
      all (contract 2A), so comparing them is meaningless. Open is not compared
      either: the daily open can carry the pre-open auction print.
    """
    out: list[Finding] = []
    for b in aggregate(bars, "1D", calendar):
        ref = daily.get(b.bar_start.date())
        if ref is None:
            continue
        session = calendar.session(b.bar_start.date())
        is_cas = bool(session) and not session.official_close_in_series
        session_day = b.bar_start.date().isoformat()
        dh, dl = ref.get("high"), ref.get("low")
        base = {"session": session_day, "complete": b.candle_complete,
                "regime": session.regime if session else "unknown",
                "quality_flags": b.quality_flags}
        if _finite(dh) and _finite(dl) and dh > 0 and dl > 0:
            tol = config.cross_tf_range_tolerance
            if _finite(b.high) and b.high > dh * (1 + tol):
                dev = b.high / dh - 1.0
                out.append(Finding(
                    "CROSS_TF_RANGE_BREAK",
                    SEVERITY_ERROR if dev > config.cross_tf_range_error else SEVERITY_WARN,
                    symbol, "1D", _key(b),
                    f"intraday high {b.high:.4f} breaks above the daily high "
                    f"{dh:.4f} by {dev * 100:.2f}%",
                    {**base, "side": "high", "aggregated": b.high, "daily": dh,
                     "deviation": dev}))
            if _finite(b.low) and b.low > 0 and b.low < dl * (1 - tol):
                dev = 1.0 - b.low / dl
                out.append(Finding(
                    "CROSS_TF_RANGE_BREAK",
                    SEVERITY_ERROR if dev > config.cross_tf_range_error else SEVERITY_WARN,
                    symbol, "1D", _key(b),
                    f"intraday low {b.low:.4f} breaks below the daily low "
                    f"{dl:.4f} by {dev * 100:.2f}%",
                    {**base, "side": "low", "aggregated": b.low, "daily": dl,
                     "deviation": dev}))
            if b.candle_complete and _finite(b.high) and _finite(b.low) and b.low > 0:
                short_high = b.high < dh * (1 - tol)
                short_low = b.low > dl * (1 + tol)
                if short_high or short_low:
                    out.append(Finding(
                        "CROSS_TF_RANGE_SHORTFALL", SEVERITY_WARN, symbol, "1D",
                        _key(b),
                        f"intraday range {b.low:.4f}-{b.high:.4f} does not reach the "
                        f"daily range {dl:.4f}-{dh:.4f} on a session the calendar "
                        f"considers complete",
                        {**base, "intraday_high": b.high, "intraday_low": b.low,
                         "daily_high": dh, "daily_low": dl}))
        dv = ref.get("volume")
        if b.candle_complete and _finite(dv) and dv and dv > 0:
            dev = abs(b.volume / dv - 1.0)
            if dev > config.cross_tf_volume_tolerance:
                out.append(Finding(
                    "CROSS_TF_VOLUME_GAP", SEVERITY_WARN, symbol, "1D", _key(b),
                    f"summed intraday volume {b.volume:,} differs from the daily "
                    f"volume {int(dv):,} by {dev * 100:.2f}%",
                    {**base, "aggregated_volume": b.volume, "daily_volume": dv,
                     "deviation": dev}))
        dc = ref.get("close")
        if (b.candle_complete and not is_cas
                and _finite(dc) and dc and dc > 0 and _finite(b.close)):
            dev = abs(b.close / dc - 1.0)
            if dev > config.cross_tf_close_info:
                out.append(Finding(
                    "CROSS_TF_CLOSE_DIFF", SEVERITY_INFO, symbol, "1D", _key(b),
                    f"last intraday close {b.close:.4f} vs daily close {dc:.4f} "
                    f"({dev * 100:.2f}%). NSE's daily close is the last-30-minute "
                    f"VWAP, so this alone is not a data error",
                    {**base, "aggregated_close": b.close, "daily_close": dc,
                     "deviation": dev}))
    return out


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------
def validate_symbol(symbol: str, bars: Sequence, calendar: SessionCalendar,
                    daily: Mapping[date, Mapping[str, float]] | None = None,
                    config: ValidationConfig = DEFAULT_CONFIG,
                    timeframe: str = "15m") -> list[Finding]:
    """Run every contract-section-2 check over one symbol's 15m bars."""
    norm = [bar_from_raw(b) for b in bars]
    norm.sort(key=lambda b: b.bar_start)
    findings: list[Finding] = []
    # Guardrail for contract 2A: past 2026-08-03 the expected bar count is per
    # symbol, so validating with a market-wide calendar would flag every CAS
    # stock as a short session. Say so instead of producing false findings.
    if norm and norm[-1].bar_start.date() >= CAS_START and \
            not isinstance(calendar, SymbolCalendar):
        findings.append(Finding(
            "REGIME_NOT_APPLIED", SEVERITY_WARN, symbol, timeframe, None,
            "bars run past the CAS start (2026-08-03) but no per-symbol session "
            "regime was supplied; pass calendar.for_symbol(symbol, regime_book)",
            {"cas_start": CAS_START.isoformat(),
             "last_bar": norm[-1].bar_start.isoformat(sep=" ")}))
    findings += check_ordering(symbol, norm, timeframe)
    findings += check_duplicates(symbol, norm, timeframe)
    findings += check_intrabucket_discontinuity(symbol, norm, timeframe, config)
    findings += check_session_bars(symbol, norm, calendar, timeframe, config)
    findings += check_zero_volume_moves(symbol, norm, timeframe, config)
    if daily:
        findings += check_intraday_vs_daily(symbol, norm, daily, timeframe, config)
        findings += check_cross_timeframe(symbol, norm, daily, calendar, config)
    findings.sort(key=lambda f: (f.bar_start or "", f.code))
    return findings


def apply_quality_flags(bars: Sequence, findings: Iterable[Finding],
                        timeframe: str = "15m") -> list[Bar]:
    """Return the same bars with `quality_flags` set. Never drops or invents.

    Only findings raised **at this timeframe** flag a row. A finding without a
    `bar_start` (symbol-level notes) and a finding raised at another timeframe
    (e.g. CROSS_TF_DISAGREEMENT, which is a 1D-level statement whose bar_start
    happens to coincide with the session's first 15m bar) are still reported
    and stored, but they do not mislabel an individual 15-minute row.
    """
    norm = [bar_from_raw(b) for b in bars]
    by_key: dict[str, set[str]] = {}
    by_day: dict[str, set[str]] = {}
    for f in findings:
        if not f.bar_start or f.timeframe != timeframe:
            continue
        if f.code in SESSION_LEVEL_CODES and f.evidence.get("day"):
            by_day.setdefault(f.evidence["day"], set()).add(f.flag)
        else:
            by_key.setdefault(f.bar_start, set()).add(f.flag)
    out: list[Bar] = []
    for b in norm:
        merged = set(filter(None, b.quality_flags.split(",")))
        merged |= by_key.get(_key(b), set())
        merged |= by_day.get(b.bar_start.date().isoformat(), set())
        out.append(replace(b, quality_flags=",".join(sorted(merged))))
    return out


def summarise(findings: Iterable[Finding]) -> dict:
    """Counts by code and by severity, plus the worst severity seen."""
    findings = list(findings)
    by_code: dict[str, int] = {}
    by_sev: dict[str, int] = {}
    for f in findings:
        by_code[f.code] = by_code.get(f.code, 0) + 1
        by_sev[f.severity] = by_sev.get(f.severity, 0) + 1
    worst = max((f.severity for f in findings),
                key=lambda s: _SEVERITY_RANK.get(s, 0), default=None)
    return {"total": len(findings), "by_code": dict(sorted(by_code.items())),
            "by_severity": dict(sorted(by_sev.items())), "worst_severity": worst}
