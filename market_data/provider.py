"""The vendor-swappable provider interface (contract §1).

Swapping vendors is a **config change**::

    MARKET_DATA_PROVIDER=kite       # today  (delay_seconds=0)
    MARKET_DATA_PROVIDER=vendor15   # later  (delay_seconds=900)
    MARKET_DATA_PROVIDER=fake       # tests

Nothing downstream imports a concrete provider class; everything calls
``market_data.get_provider()``.  Any object that satisfies
``MarketDataProvider`` and passes ``assert_provider_conformance`` is a legal
drop-in.
"""
from __future__ import annotations

import importlib
import os
import threading
from datetime import datetime
from typing import Callable, Dict, List, Optional, Protocol, runtime_checkable

from .types import (
    IST,
    Instrument,
    ProviderError,
    RawCandle,
    bar_end_for,
    session_close,
    session_open,
    to_ist,
    trading_days,
)

__all__ = [
    "MarketDataProvider",
    "BaseProvider",
    "register_provider",
    "available_providers",
    "get_provider",
    "clear_provider_cache",
    "DEFAULT_PROVIDER_ENV",
]

DEFAULT_PROVIDER_ENV = "MARKET_DATA_PROVIDER"


# ── the interface ────────────────────────────────────────────────────────────

@runtime_checkable
class MarketDataProvider(Protocol):
    """Contract §1.  Implementations must be safe to call from many threads."""

    provider_id: str
    #: 0 for a real-time/historical source, 900 for the 15-minute-delayed vendor.
    delay_seconds: int
    #: timeframe -> maximum calendar days a single request may span.
    max_days_per_request: Dict[str, int]
    #: global requests/second allowed for the *key*, not per worker.
    rate_limit_per_second: float

    def instruments(self) -> List[Instrument]: ...

    def candles(
        self, symbol: str, timeframe: str, start: datetime, end: datetime
    ) -> List[RawCandle]: ...

    def latest_completed_bar(self, timeframe: str, now: datetime) -> datetime: ...


# ── shared base (optional, but every in-tree provider uses it) ───────────────

class BaseProvider:
    """Behaviour every provider shares, so it cannot drift between vendors.

    Subclasses supply ``provider_id``, ``delay_seconds``,
    ``max_days_per_request``, ``rate_limit_per_second``, ``instruments()`` and
    ``_fetch_window()``.  The base class owns:

    * ``latest_completed_bar`` — session-aware and delay-aware, identical maths
      for every vendor (a delayed vendor is "delayed by design", not "stale");
    * chunking a requested range into per-request windows;
    * the holiday hints used only to skip pointless requests.
    """

    provider_id: str = "base"
    delay_seconds: int = 0
    max_days_per_request: Dict[str, int] = {}
    rate_limit_per_second: float = 1.0
    supported_timeframes: tuple[str, ...] = ("15minute", "day")

    #: optional holiday hints.  Purely an optimisation: a missing holiday costs
    #: one empty request, never a fabricated bar.
    holidays: frozenset = frozenset()

    # -- session maths -------------------------------------------------------

    def latest_completed_bar(self, timeframe: str, now: datetime) -> datetime:
        """``bar_start`` of the newest bar that is complete *and* published.

        A bar counts as available when ``bar_end + delay_seconds <= now``.
        Raises ``ProviderError`` if no session has completed a bar yet within a
        two-week look-back (which would mean the calendar is wrong, not that
        the market is quiet).
        """
        self._check_timeframe(timeframe)
        now_ist = to_ist(now)
        cutoff = now_ist.timestamp() - self.delay_seconds
        day = now_ist.date()
        for _ in range(14):
            if day.weekday() < 5 and day not in self.holidays:
                start = self._last_bar_start_on(day, timeframe, cutoff)
                if start is not None:
                    return start
            day = day.fromordinal(day.toordinal() - 1)
        raise ProviderError(
            f"{self.provider_id}: no completed {timeframe} bar in the 14 days before "
            f"{now_ist.isoformat()}",
            provider_id=self.provider_id,
        )

    def _last_bar_start_on(self, day, timeframe: str, cutoff_ts: float) -> Optional[datetime]:
        from .types import session_grid

        for start in reversed(session_grid(day, timeframe)):
            if bar_end_for(start, timeframe).timestamp() <= cutoff_ts:
                return start
        return None

    # -- range handling ------------------------------------------------------

    def _check_timeframe(self, timeframe: str) -> None:
        if timeframe not in self.supported_timeframes:
            raise ProviderError(
                f"{self.provider_id}: unsupported timeframe {timeframe!r} "
                f"(supported: {', '.join(self.supported_timeframes)})",
                provider_id=self.provider_id,
            )

    def chunk_days(self, timeframe: str, start: datetime, end: datetime):
        """Yield ``(chunk_start_date, chunk_end_date)`` honouring the vendor cap."""
        cap = int(self.max_days_per_request.get(timeframe, 60))
        s, e = to_ist(start).date(), to_ist(end).date()
        cur = s
        while cur <= e:
            stop = min(cur.fromordinal(cur.toordinal() + cap - 1), e)
            yield cur, stop
            cur = stop.fromordinal(stop.toordinal() + 1)

    def trading_days_in(self, start: datetime, end: datetime):
        return trading_days(to_ist(start).date(), to_ist(end).date(), self.holidays)

    # -- the piece every vendor must write -----------------------------------

    def _fetch_window(self, symbol: str, timeframe: str, start_day, end_day) -> List[RawCandle]:
        raise NotImplementedError

    def candles(
        self, symbol: str, timeframe: str, start: datetime, end: datetime
    ) -> List[RawCandle]:
        """Fetch ``[start, end]`` (inclusive days), chunked, stitched, sorted.

        Returns only bars whose ``bar_start`` lies in the requested window.  An
        empty list means "the vendor has nothing here", never "assume flat".
        """
        from .types import sort_unique

        self._check_timeframe(timeframe)
        s_ist, e_ist = to_ist(start), to_ist(end)
        if e_ist < s_ist:
            return []
        out: List[RawCandle] = []
        for cs, ce in self.chunk_days(timeframe, s_ist, e_ist):
            out.extend(self._fetch_window(symbol, timeframe, cs, ce))
        lo = s_ist
        hi = e_ist
        # A caller passing plain dates (midnight) means "include that whole day".
        if hi.time() == hi.min.time():
            hi = session_close(hi.date())
        if lo.time() == lo.min.time():
            lo = session_open(lo.date())
        return [c for c in sort_unique(out) if lo <= c.bar_start <= hi]


# ── registry ─────────────────────────────────────────────────────────────────

_FACTORIES: Dict[str, Callable[..., MarketDataProvider]] = {}
#: providers shipped in this package, imported lazily so that (for example) a
#: machine without ``kiteconnect`` can still use ``fake``/``vendor15``.
_BUILTIN_MODULES = {
    "kite": ".kite_provider",
    "vendor15": ".vendor15_provider",
    "gdf": ".gdf_provider",
    "fake": ".fake_provider",
}
_CACHE: Dict[str, MarketDataProvider] = {}
_LOCK = threading.Lock()


def register_provider(provider_id: str, factory: Callable[..., MarketDataProvider]):
    """Register a provider factory.  Usable as a decorator on the class."""
    _FACTORIES[provider_id] = factory
    return factory


def available_providers() -> List[str]:
    return sorted(set(_FACTORIES) | set(_BUILTIN_MODULES))


def _ensure_loaded(provider_id: str) -> None:
    if provider_id in _FACTORIES:
        return
    mod = _BUILTIN_MODULES.get(provider_id)
    if mod is None:
        raise ProviderError(
            f"unknown provider {provider_id!r}; available: {', '.join(available_providers())}"
        )
    importlib.import_module(mod, package=__package__)
    if provider_id not in _FACTORIES:
        raise ProviderError(f"module for provider {provider_id!r} did not register itself")


def get_provider(provider_id: Optional[str] = None, *, fresh: bool = False, **kwargs):
    """Return the configured provider.

    ``provider_id`` defaults to ``$MARKET_DATA_PROVIDER`` and then to ``kite``.
    Instances are cached per id so the rate limiter, the instrument cache and
    the auth state are shared by every caller in the process.
    """
    pid = (provider_id or os.environ.get(DEFAULT_PROVIDER_ENV) or "kite").strip().lower()
    if fresh or kwargs:
        _ensure_loaded(pid)
        return _FACTORIES[pid](**kwargs)
    with _LOCK:
        inst = _CACHE.get(pid)
        if inst is None:
            _ensure_loaded(pid)
            inst = _FACTORIES[pid]()
            _CACHE[pid] = inst
        return inst


def clear_provider_cache() -> None:
    with _LOCK:
        _CACHE.clear()
