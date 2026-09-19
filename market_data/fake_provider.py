"""Deterministic fixture provider — the reference implementation for tests.

`FakeProvider` is what "correct" looks like: perfect 15-minute grid alignment,
exact session boundaries, no duplicates, honest `candle_complete`, and an empty
list (never a synthetic bar) for a day it has no data for.  Every other
provider is measured against it by `tests/test_conformance.py`.

It is deterministic: the same (symbol, timeframe, window) always produces the
same bars, on any machine, with no network.  Prices come from a seeded
hash-based random walk, so they are plausible but obviously synthetic —
`vendor_id='fake'` and `adjustment_basis_id='fake'` make that impossible to
mistake for real data downstream.

`missing_days` and `holidays` let a test say "this symbol genuinely has no data
here" so the conformance suite can prove the gap is *reported*, not filled.
"""
from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional

from .provider import BaseProvider, register_provider
from .ratelimit import get_limiter
from .types import (
    Instrument,
    QualityFlag,
    RawCandle,
    basic_quality_flags,
    bar_end_for,
    session_close,
    session_grid,
    session_open,
    to_ist,
    utc_now,
)

DEFAULT_SYMBOLS = ("RELIANCE", "TCS", "INFY", "HDFCBANK", "FORCEMOT")


def _rand01(*parts) -> float:
    """Deterministic pseudo-random in [0,1) from the given parts."""
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(h[:8], "big") / 2**64


class FakeProvider(BaseProvider):
    provider_id = "fake"
    delay_seconds = 0
    rate_limit_per_second = 1000.0
    max_days_per_request = {"15minute": 30, "30minute": 30, "60minute": 60, "day": 365}
    supported_timeframes = ("15minute", "30minute", "60minute", "day")

    def __init__(
        self,
        symbols: Iterable[str] = DEFAULT_SYMBOLS,
        *,
        delay_seconds: int = 0,
        missing_days: Iterable[date] = (),
        holidays: Iterable[date] = (),
        base_price: float = 1000.0,
        now: Optional[datetime] = None,
    ):
        self.symbols = list(symbols)
        self.delay_seconds = delay_seconds
        self.missing_days = frozenset(missing_days)
        self.holidays = frozenset(holidays)
        self.base_price = base_price
        #: freeze "now" so tests are reproducible; None = real clock.
        self.frozen_now = now
        self.limiter = get_limiter("fake", self.rate_limit_per_second, capacity=1000.0)
        self.requests_made = 0
        self._instruments = [
            Instrument(
                instrument_id=str(700000 + i),
                symbol=s,
                exchange="NSE",
                name=f"{s} FAKE LTD",
                segment="NSE",
                vendor_id="fake",
            )
            for i, s in enumerate(self.symbols)
        ]

    # -- interface -------------------------------------------------------

    def instruments(self) -> List[Instrument]:
        return list(self._instruments)

    def _now(self) -> datetime:
        return self.frozen_now or utc_now()

    def _resolve(self, symbol: str) -> Instrument:
        for i in self._instruments:
            if i.symbol == symbol:
                return i
        from .types import ProviderError

        raise ProviderError(f"unknown fake symbol {symbol!r}", provider_id="fake")

    def _fetch_window(self, symbol: str, timeframe: str, start_day: date, end_day: date):
        self.limiter.acquire()
        self.requests_made += 1
        inst = self._resolve(symbol)
        fetched_at = utc_now()
        cutoff = self._now().timestamp() - self.delay_seconds
        out: List[RawCandle] = []
        day = start_day
        while day <= end_day:
            if day.weekday() < 5 and day not in self.holidays and day not in self.missing_days:
                out.extend(self._day_bars(inst, timeframe, day, cutoff, fetched_at))
            day += timedelta(days=1)
        return out

    # -- synthesis -------------------------------------------------------

    def _day_bars(self, inst, timeframe, day, cutoff, fetched_at) -> List[RawCandle]:
        bars: List[RawCandle] = []
        if timeframe == "day":
            starts = [session_open(day)]
        else:
            starts = session_grid(day, timeframe)
        level = self.base_price * (0.8 + 0.4 * _rand01(inst.symbol, day.isoformat()))
        for start in starts:
            r = _rand01(inst.symbol, timeframe, start.isoformat())
            r2 = _rand01(inst.symbol, timeframe, start.isoformat(), "b")
            o = round(level * (1 + (r - 0.5) * 0.01), 2)
            c = round(o * (1 + (r2 - 0.5) * 0.01), 2)
            h = round(max(o, c) * (1 + r * 0.002), 2)
            l = round(min(o, c) * (1 - r2 * 0.002), 2)
            vol = int(1000 + r * 100000)
            level = c
            end = bar_end_for(start, timeframe)
            complete = end.timestamp() <= cutoff
            if end.timestamp() > self._now().timestamp():
                continue  # the future does not exist — never fabricate it
            flags = basic_quality_flags(o, h, l, c, vol)
            if not complete:
                flags |= QualityFlag.PARTIAL_BAR
            bars.append(
                RawCandle(
                    instrument_id=inst.instrument_id,
                    symbol=inst.symbol,
                    exchange=inst.exchange,
                    timeframe=timeframe,
                    bar_start=start,
                    bar_end=end,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    volume=vol,
                    candle_complete=complete,
                    quality_flags=flags,
                    adjustment_basis_id="fake",
                    vendor_id="fake",
                    vendor_revision="1",
                    fetched_at=fetched_at,
                )
            )
        return bars


register_provider("fake", lambda **kw: FakeProvider(**kw))

__all__ = ["FakeProvider", "DEFAULT_SYMBOLS"]
