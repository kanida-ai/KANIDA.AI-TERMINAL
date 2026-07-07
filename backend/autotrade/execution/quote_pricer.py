"""Quote-driven marketable-LIMIT pricer — pure, no I/O, fully unit-testable.

The AutoTrade entry/exit hot path prices raw MARKET orders off LTP only. A
MARKET buy on a stock locked at its UPPER circuit is rejected by the exchange
("outside circuit limits") — the 2026-07-06 CEMPRO incident. This module reads a
single symbol's live order book (bid/ask/circuit, fetched ONCE per fire by the
caller via broker.get_quotes) and returns a marketable-LIMIT price capped inside
the exchange circuit band.

PHILOSOPHY (operator directive): the execution layer ALWAYS PLACES an order and
MAXIMISES FILL SPEED. It NEVER skips an entry/exit and never hands the decision
back. This function therefore has NO skip outcome:
  * When there is a usable price → return a marketable-LIMIT that crosses the
    spread (fills as fast as a MARKET order for liquid names) and is CAPPED at
    the circuit band. A stock LOCKED at its circuit becomes a LIMIT exactly AT
    the circuit price → a valid, ACCEPTED, QUEUED order that fills the instant
    the lock breaks (no rejection, no skip).
  * When there is NO usable price at all (quote None/empty AND no ltp) → signal
    a MARKET fallback (fallback_market=True). The caller then places the existing
    MARKET order. Still an order — never a skip.

`plan_marketable_order` is pure: it does NO network I/O. All quote reads happen
in the caller (one batched broker.get_quotes for the whole basket).

DECISION TABLE (all prices tick-rounded TOWARD the inside of the circuit band):
  BUY  price = min( (ask or ltp) * (1 + buffer), upper_circuit )  [floor to tick]
  SELL price = max( (bid or ltp) * (1 - buffer), lower_circuit )  [ceil  to tick]
  no usable price (quote None/empty AND no ltp) → fallback_market=True (MARKET)

A stale quote is STILL USED (never skip on staleness). The circuit band is the
ONLY cap. There is no price-deviation skip, no circuit skip, no stale skip.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional


def _floor_to_tick(price: float, tick: float) -> float:
    """Largest multiple of tick that is <= price (keeps a BUY under the cap)."""
    if tick is None or tick <= 0:
        tick = 0.05
    return round(math.floor(price / tick + 1e-9) * tick, 2)


def _ceil_to_tick(price: float, tick: float) -> float:
    """Smallest multiple of tick that is >= price (keeps a SELL above the floor)."""
    if tick is None or tick <= 0:
        tick = 0.05
    return round(math.ceil(price / tick - 1e-9) * tick, 2)


def _market_fallback(reason: str) -> Dict[str, Any]:
    return {"ok": False, "fallback_market": True, "order_type": "MARKET",
            "price": None, "reason": reason}


def plan_marketable_order(side: str, symbol: str, qty: int,
                          quote: Optional[Dict[str, Any]], tick: float,
                          cfg, *, ltp_fallback: Optional[float] = None
                          ) -> Dict[str, Any]:
    """Plan a marketable-LIMIT order for ONE symbol from its live book.

    ALWAYS yields a placeable order — either a LIMIT (ok=True) or a MARKET
    fallback (fallback_market=True). NEVER a skip.

    Args:
        side  : "BUY" (long entry / short cover) or "SELL" (long exit / short
                entry). Only the price DIRECTION depends on this.
        symbol: for the reason string only.
        qty   : intended quantity (carried for the caller; not used in pricing).
        quote : ONE symbol's dict from broker.get_quotes:
                {ltp, bid, ask, upper_circuit, lower_circuit, ts} — or None.
        tick  : the instrument's tick size (from mtf_eligibility.get_tick_size).
        cfg   : TradingSessionConfig — reads marketable_buffer_pct only.
        ltp_fallback : an LTP the caller already has (e.g. from broker.get_ltp),
                used when the quote has no usable price. When BOTH are absent the
                result is a MARKET fallback.

    Returns dict {ok, order_type, price, reason, fallback_market}:
        ok=True            → {order_type:"LIMIT", price:<in-band tick-rounded>}
        fallback_market=True → {order_type:"MARKET", price:None}  (no usable price)
    """
    s = (side or "").upper()
    if s not in ("BUY", "SELL"):
        # Unknown side → don't guess a direction; MARKET fallback (still places).
        return _market_fallback(f"{symbol}: invalid side {side!r} - market fallback")

    buffer_pct = float(getattr(cfg, "marketable_buffer_pct", 0.003))
    if tick is None or tick <= 0:
        tick = 0.05

    q = quote or {}
    ltp = q.get("ltp")
    if ltp is None or ltp <= 0:
        ltp = ltp_fallback if (ltp_fallback and ltp_fallback > 0) else None
    bid = q.get("bid")
    ask = q.get("ask")
    upper = q.get("upper_circuit")
    lower = q.get("lower_circuit")

    if s == "BUY":
        # Best available reference to cross the spread: the ask, else the LTP.
        base = ask if (ask and ask > 0) else ltp
        if base is None or base <= 0:
            # No price data at all — MARKET fallback (still an order).
            return _market_fallback(f"{symbol}: no usable BUY price - market fallback")
        raw = base * (1.0 + buffer_pct)
        if upper is not None and upper > 0:
            raw = min(raw, upper)          # CAP at the circuit (only cap)
        price = _floor_to_tick(float(raw), tick)   # stay <= upper
        # A tick-floor of a tiny price can land at 0 → nudge to one tick.
        if price <= 0:
            price = round(tick, 2)
        # If the circuit cap itself rounded below the reference (locked-up name),
        # price is exactly AT the circuit — a valid queued LIMIT. Nothing to skip.
    else:  # SELL
        base = bid if (bid and bid > 0) else ltp
        if base is None or base <= 0:
            return _market_fallback(f"{symbol}: no usable SELL price - market fallback")
        raw = base * (1.0 - buffer_pct)
        if lower is not None and lower > 0:
            raw = max(raw, lower)          # FLOOR at the circuit (only cap)
        price = _ceil_to_tick(float(raw), tick)    # stay >= lower
        if price <= 0:
            price = round(tick, 2)

    return {"ok": True, "fallback_market": False, "order_type": "LIMIT",
            "price": price,
            "reason": f"{s} marketable-limit @ {price}"}
