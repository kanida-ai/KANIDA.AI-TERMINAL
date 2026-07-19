"""Small read-only helpers shared by the monitors (time, market hours)."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Optional

IST = timezone(timedelta(hours=5, minutes=30))

_MARKET_OPEN = time(9, 15)
_MARKET_CLOSE = time(15, 30)


def now_ist(now: Optional[datetime] = None) -> datetime:
    return now or datetime.now(IST)


def in_market_hours(now: Optional[datetime] = None) -> bool:
    """True on a weekday within NSE cash session (09:15–15:30 IST). NSE holidays
    are not modelled here (best-effort; a holiday reads as in-hours, which only
    makes the freshness monitors STRICTER, never laxer)."""
    n = now_ist(now)
    if n.weekday() >= 5:
        return False
    return _MARKET_OPEN <= n.time() <= _MARKET_CLOSE


def age_seconds(iso_ts: Optional[str], now: Optional[datetime] = None
                ) -> Optional[float]:
    """Age in seconds of an ISO-IST timestamp, or None if unparseable/absent.
    A naive timestamp is assumed IST (what every writer in this codebase emits)."""
    if not iso_ts:
        return None
    try:
        d = datetime.fromisoformat(str(iso_ts))
    except (ValueError, TypeError):
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=IST)
    return (now_ist(now) - d).total_seconds()
