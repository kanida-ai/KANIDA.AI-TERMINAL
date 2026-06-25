"""Mark-to-market price sourcing for autotrade session positions.

Resolution order for a position's LTP:
  1. A live broker LTP (KiteTicker WS / broker REST) — passed in by the caller.
  2. The latest daily close from ohlc_daily for that symbol — so a PRE-OPEN mark
     equals (approximately) the entry, NOT a bogus stale/unrelated value that
     would show a phantom -7% the moment a session starts.

Never returns a stale unrelated value: if neither source has a price we return
None and the caller leaves the existing mark untouched (does not fabricate one).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.pricing")


def latest_close(symbol: str) -> Optional[float]:
    """Latest daily close from ohlc_daily for `symbol`, or None."""
    try:
        with falcon_conn() as con:
            row = con.execute(
                """SELECT close FROM ohlc_daily
                   WHERE symbol=? ORDER BY trade_date DESC LIMIT 1""",
                (symbol,),
            ).fetchone()
    except Exception as e:  # table may not exist in some test DBs
        log.debug("latest_close lookup failed for %s: %s", symbol, e)
        return None
    if not row:
        return None
    val = row[0]
    return float(val) if val is not None else None


def resolve_ltp(symbol: str, broker: Any = None,
                fallback_entry: Optional[float] = None) -> Optional[float]:
    """Sane mark-to-market price for `symbol`.

    1) broker.get_ltp(symbol) if a broker is supplied and returns a value.
    2) latest daily close from ohlc_daily.
    3) fallback_entry (the position's avg_price) so a pre-open mark ~= entry
       rather than nothing.
    Returns None only when all three are unavailable.
    """
    if broker is not None:
        try:
            ltp = broker.get_ltp(symbol)
        except Exception as e:
            log.debug("broker.get_ltp failed for %s: %s", symbol, e)
            ltp = None
        if ltp is not None:
            return float(ltp)

    close = latest_close(symbol)
    if close is not None:
        return close

    if fallback_entry is not None:
        return float(fallback_entry)
    return None


def resolve_brokers_ltp(symbol: str, brokers: Dict[str, Any],
                        broker_profile: Optional[str] = None,
                        fallback_entry: Optional[float] = None) -> Optional[float]:
    """Same as resolve_ltp but picks the owning broker from a {profile: client}
    map (falling back to any broker), then ohlc close, then entry."""
    broker = None
    if brokers:
        broker = brokers.get(broker_profile) or next(iter(brokers.values()), None)
    return resolve_ltp(symbol, broker=broker, fallback_entry=fallback_entry)
