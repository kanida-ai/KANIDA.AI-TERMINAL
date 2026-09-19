"""Normalized market-data types shared by every provider (contract §1/§2).

Everything here is provider-agnostic. A provider's only job is to turn its
vendor payload into `RawCandle` rows that obey the conventions below; the
store / aggregation / validation layers (W2/W3) never see vendor shapes.

Conventions that do not bend
---------------------------
* **All datetimes are timezone-aware IST** (`Asia/Kolkata`, UTC+05:30, no DST).
  A naive datetime handed to a provider is interpreted as IST.
* **`bar_start` is inclusive, `bar_end` is exclusive.**  A 15-minute bar that
  covers 09:15:00–09:29:59 has `bar_start=09:15` and `bar_end=09:30`.
* **Daily bars are session-aligned**, not midnight-aligned: a `day` bar for
  2026-09-15 has `bar_start=2026-09-15 09:15 IST` and
  `bar_end=2026-09-15 15:30 IST`.  Use `session_date()` to get the trade date.
  (Kite returns daily bars stamped at midnight IST; `KiteProvider` normalizes
  them to the session window so `bar_start < bar_end` and cross-timeframe
  reconciliation in contract §2.5 compares like with like.)
* **Nothing is ever fabricated.**  A window with no trading returns an empty
  list.  A provider never interpolates, forward-fills or invents a bar.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, Optional, Sequence

# ── time / session constants ─────────────────────────────────────────────────

IST = timezone(timedelta(hours=5, minutes=30), "IST")

NSE_SESSION_OPEN = time(9, 15)
NSE_SESSION_CLOSE = time(15, 30)

#: canonical timeframe id -> bar length in seconds.  ``day`` is the session
#: length (09:15–15:30 = 6h15m); it is a *bucket*, not a fixed-width grid step.
TIMEFRAME_SECONDS: dict[str, int] = {
    "15minute": 15 * 60,
    "30minute": 30 * 60,
    "60minute": 60 * 60,
    "day": int((15 * 3600 + 30 * 60) - (9 * 3600 + 15 * 60)),
}

#: The base grid every intraday timeframe must align to (contract decision 1).
BASE_GRID_SECONDS = 15 * 60


# ── errors ───────────────────────────────────────────────────────────────────

class ProviderError(RuntimeError):
    """Any provider-side failure that is not a credential problem.

    ``retryable`` tells the caller whether backing off could help.
    """

    def __init__(self, message: str, *, provider_id: str = "", retryable: bool = False):
        super().__init__(message)
        self.provider_id = provider_id
        self.retryable = retryable


class TokenError(ProviderError):
    """Credential / session failure (Kite ``TokenException``, HTTP 401/403).

    NEVER carries a token value.  Messages are limited to the provider id and
    the vendor's own error class so nothing secret reaches a log file.
    """

    def __init__(self, message: str, *, provider_id: str = ""):
        super().__init__(message, provider_id=provider_id, retryable=False)


class RateLimitError(ProviderError):
    """Vendor said "too many requests". Always retryable."""

    def __init__(self, message: str, *, provider_id: str = "", retry_after: float = 1.0):
        super().__init__(message, provider_id=provider_id, retryable=True)
        self.retry_after = retry_after


# ── quality flags (bitmask; validation layer owns the semantics) ─────────────

class QualityFlag:
    """Bit values for ``RawCandle.quality_flags``.

    The provider only sets flags it can know at fetch time.  Everything
    statistical (bucket-median outliers, intraday/daily reconciliation) is set
    later by ``market_data/validate.py`` (W2/W3) — this is just the shared
    vocabulary so both sides use the same bits.
    """

    NONE = 0
    OHLC_INCONSISTENT = 1 << 0   # low > min(open, close) or high < max(open, close)
    NON_POSITIVE_PRICE = 1 << 1  # a price <= 0 or non-finite
    ZERO_VOLUME = 1 << 2         # volume == 0
    OFF_GRID = 1 << 3            # bar_start not on the 15-minute session grid
    OUTSIDE_SESSION = 1 << 4     # bar falls outside 09:15–15:30 of a trading day
    PARTIAL_BAR = 1 << 5         # provider returned an in-progress bar
    VENDOR_SUSPECT = 1 << 6      # vendor itself flagged the row
    SESSION_TRUNCATED = 1 << 7   # session missing trailing bars vs the 25-bar grid


# ── instrument ───────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class Instrument:
    """A tradable instrument as the provider knows it.

    ``instrument_id`` is the *stable* key used by the store.  For Kite it is the
    numeric ``instrument_token`` rendered as a string; for a REST vendor it is
    whatever id that vendor guarantees is stable across symbol renames.  It is
    intentionally a string so a vendor swap cannot silently change the column
    type in ``candles_15m``.
    """

    instrument_id: str
    symbol: str
    exchange: str = "NSE"
    name: str = ""
    segment: str = ""
    instrument_type: str = "EQ"
    isin: str = ""
    lot_size: int = 1
    tick_size: float = 0.05
    expiry: Optional[date] = None
    vendor_id: str = ""

    @property
    def key(self) -> tuple[str, str]:
        return (self.exchange, self.symbol)


# ── candle ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class RawCandle:
    """One normalized OHLCV bar exactly as contract §2 names the columns.

    ``raw`` means *as the vendor served it, normalized in shape only*: no
    aggregation, no repair, no adjustment applied by us.  ``adjustment_basis_id``
    records which adjustment basis the vendor's prices are already on.
    """

    instrument_id: str
    symbol: str
    exchange: str
    timeframe: str
    bar_start: datetime            # inclusive, tz-aware IST
    bar_end: datetime              # exclusive, tz-aware IST
    open: float
    high: float
    low: float
    close: float
    volume: int
    candle_complete: bool
    quality_flags: int = QualityFlag.NONE
    adjustment_basis_id: str = ""
    vendor_id: str = ""
    vendor_revision: str = ""
    fetched_at: Optional[datetime] = None   # tz-aware UTC

    # -- derived helpers -----------------------------------------------------

    @property
    def session_date(self) -> date:
        """The trade date this bar belongs to."""
        return self.bar_start.astimezone(IST).date()

    @property
    def duration_seconds(self) -> int:
        return int((self.bar_end - self.bar_start).total_seconds())

    def with_flags(self, flags: int) -> "RawCandle":
        return replace(self, quality_flags=self.quality_flags | flags)

    def as_row(self) -> dict:
        """Dict keyed exactly like the ``candles_15m`` columns W2 writes."""
        return {
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timeframe": self.timeframe,
            "bar_start": self.bar_start.isoformat(),
            "bar_end": self.bar_end.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "candle_complete": int(self.candle_complete),
            "quality_flags": self.quality_flags,
            "adjustment_basis_id": self.adjustment_basis_id,
            "vendor_id": self.vendor_id,
            "vendor_revision": self.vendor_revision,
            "fetched_at": self.fetched_at.isoformat() if self.fetched_at else None,
        }


# ── session / grid helpers (shared by providers and the conformance suite) ───

def to_ist(dt: datetime) -> datetime:
    """Return ``dt`` in IST. A naive datetime is *interpreted* as IST."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def session_open(day: date) -> datetime:
    return datetime.combine(day, NSE_SESSION_OPEN, tzinfo=IST)


def session_close(day: date) -> datetime:
    return datetime.combine(day, NSE_SESSION_CLOSE, tzinfo=IST)


def is_weekday(day: date) -> bool:
    return day.weekday() < 5


def bars_per_session(timeframe: str) -> int:
    """How many complete bars a full NSE session contains (25 for 15minute)."""
    step = TIMEFRAME_SECONDS[timeframe]
    span = TIMEFRAME_SECONDS["day"]
    if timeframe == "day":
        return 1
    return span // step


def session_grid(day: date, timeframe: str) -> list[datetime]:
    """Every ``bar_start`` of a full session for ``timeframe``."""
    if timeframe == "day":
        return [session_open(day)]
    step = timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
    out, cur, close = [], session_open(day), session_close(day)
    while cur + step <= close:
        out.append(cur)
        cur += step
    return out


def bar_end_for(bar_start: datetime, timeframe: str) -> datetime:
    """Exclusive end of the bar starting at ``bar_start``."""
    if timeframe == "day":
        return session_close(to_ist(bar_start).date())
    return bar_start + timedelta(seconds=TIMEFRAME_SECONDS[timeframe])


def is_on_grid(bar_start: datetime, timeframe: str) -> bool:
    """True if ``bar_start`` sits on the session's 15-minute grid."""
    ist = to_ist(bar_start)
    open_dt = session_open(ist.date())
    offset = (ist - open_dt).total_seconds()
    if offset < 0:
        return False
    step = TIMEFRAME_SECONDS[timeframe] if timeframe != "day" else 0
    if timeframe == "day":
        return offset == 0
    if offset % BASE_GRID_SECONDS != 0:
        return False
    return offset % step == 0


def in_session(bar_start: datetime, timeframe: str) -> bool:
    """True if the whole bar lies inside 09:15–15:30 of a weekday."""
    ist = to_ist(bar_start)
    if not is_weekday(ist.date()):
        return False
    return session_open(ist.date()) <= ist and bar_end_for(ist, timeframe) <= session_close(ist.date())


def trading_days(start: date, end: date, holidays: Iterable[date] = ()) -> list[date]:
    """Weekdays in ``[start, end]`` minus the supplied holiday set.

    The provider layer deliberately knows *only* weekends plus an optional
    holiday file — the authoritative session calendar lives with the
    aggregation layer (contract §2).  A missing holiday therefore shows up as
    "no rows for that day", never as an invented bar.
    """
    hol = set(holidays)
    out, cur = [], start
    while cur <= end:
        if is_weekday(cur) and cur not in hol:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def basic_quality_flags(o: float, h: float, l: float, c: float, volume: int) -> int:
    """Flags a provider can set from the row alone (contract §2 checks 3 & 4)."""
    flags = QualityFlag.NONE
    vals = (o, h, l, c)
    if any((not math.isfinite(v)) or v <= 0 for v in vals):
        flags |= QualityFlag.NON_POSITIVE_PRICE
    else:
        if l > min(o, c) or h < max(o, c) or h < l:
            flags |= QualityFlag.OHLC_INCONSISTENT
    if volume == 0:
        flags |= QualityFlag.ZERO_VOLUME
    return flags


def sort_unique(candles: Sequence[RawCandle]) -> list[RawCandle]:
    """Sort by ``bar_start`` and drop exact duplicate starts (last wins).

    Used when stitching chunked requests whose windows touch at the seam.
    """
    by_start: dict[datetime, RawCandle] = {}
    for c in candles:
        by_start[c.bar_start] = c
    return [by_start[k] for k in sorted(by_start)]


__all__ = [
    "IST",
    "NSE_SESSION_OPEN",
    "NSE_SESSION_CLOSE",
    "TIMEFRAME_SECONDS",
    "BASE_GRID_SECONDS",
    "ProviderError",
    "TokenError",
    "RateLimitError",
    "QualityFlag",
    "Instrument",
    "RawCandle",
    "to_ist",
    "utc_now",
    "session_open",
    "session_close",
    "is_weekday",
    "bars_per_session",
    "session_grid",
    "bar_end_for",
    "is_on_grid",
    "in_session",
    "trading_days",
    "basic_quality_flags",
    "sort_unique",
]
