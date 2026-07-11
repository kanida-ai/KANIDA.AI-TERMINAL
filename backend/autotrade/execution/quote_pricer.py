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
  EXIT (entry=False, controlled marketable-LIMIT):
    BUY  price = min( (ask or ltp) * (1 + buffer), upper_circuit )  [floor to tick]
    SELL price = max( (bid or ltp) * (1 - buffer), lower_circuit )  [ceil  to tick]
  ENTRY (entry=True, guarantee-the-fill):
    normal book (ask present, LTP inside band) → MARKET order (fills at the touch,
      margined on LTP — no limit-price margin inflation)
    LOCKED at the circuit (no ask, or LTP at/beyond the band) → LIMIT queued AT
      the circuit (a valid, ACCEPTED order that fills when the lock breaks)
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


def _market_fallback(reason: str, degraded_quote: bool = False) -> Dict[str, Any]:
    return {"ok": False, "fallback_market": True, "order_type": "MARKET",
            "price": None, "reason": reason, "degraded_quote": degraded_quote}


def exit_circuit_locked_reason(quote: Optional[Dict[str, Any]],
                               direction: str = "long") -> Optional[str]:
    """SPRINT CLUSTER 5 ITEM 4 — DISTINCT 'stranded at circuit' signal for EXITS.

    A needed exit can be UNFILLABLE because the stock is LOCKED at the ADVERSE
    circuit — a long-exit SELL into a LOWER-circuit lock, or a short-cover BUY
    into an UPPER-circuit lock. In that case there is no counterparty, so the
    exit LIMIT/MARKET sits unfilled and the generic exit-retry loop would spin
    forever emitting EXIT_FAILED. Returns the string "circuit_locked" when the
    book proves such a lock, else None. Pure (no I/O)."""
    if not quote:
        return None
    ltp = quote.get("ltp")
    bid = quote.get("bid")
    ask = quote.get("ask")
    upper = quote.get("upper_circuit")
    lower = quote.get("lower_circuit")
    if str(direction).lower() == "short":
        # short cover = BUY; stranded when locked at the UPPER circuit
        # (no offer to buy from AND the last price is at/above the band).
        locked = (ask is None or ask <= 0) and (
            upper is not None and upper > 0 and ltp is not None and ltp >= upper)
        return "circuit_locked" if locked else None
    # long exit = SELL; stranded when locked at the LOWER circuit
    # (no bid to sell into AND the last price is at/below the band).
    locked = (bid is None or bid <= 0) and (
        lower is not None and lower > 0 and ltp is not None and ltp <= lower)
    return "circuit_locked" if locked else None


def plan_marketable_order(side: str, symbol: str, qty: int,
                          quote: Optional[Dict[str, Any]], tick: float,
                          cfg, *, ltp_fallback: Optional[float] = None,
                          entry: bool = False,
                          now_ts: Optional[float] = None,
                          max_quote_age_sec: Optional[float] = None,
                          stale_policy: Optional[str] = None
                          ) -> Dict[str, Any]:
    """Plan a marketable order for ONE symbol from its live book.

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
        entry : ENTRY (guarantee-the-fill) mode. When True, a NORMAL book is a
                MARKET fallback (a genuine MARKET order that fills instantly at the
                touch for ANY gap and is margined on LTP — no limit-price margin
                inflation), and ONLY a stock LOCKED at its circuit is placed as a
                LIMIT queued exactly AT the circuit (the CEMPRO case). Default
                False = the EXIT path: a controlled marketable-LIMIT that crosses
                the touch by marketable_buffer_pct, capped at the circuit band.
                (2026-07-09 KALYANKJIL: a 0.3%-through-touch entry LIMIT sat BELOW
                a +2-3% opening gap and never filled → the day's best pick missed.)

    Returns dict {ok, order_type, price, reason, fallback_market}:
        ok=True            → {order_type:"LIMIT", price:<in-band tick-rounded>}
        fallback_market=True → {order_type:"MARKET", price:None}  (MARKET order)
    """
    s = (side or "").upper()
    if s not in ("BUY", "SELL"):
        # Unknown side → don't guess a direction; MARKET fallback (still places).
        return _market_fallback(f"{symbol}: invalid side {side!r} - market fallback")

    # ── QUOTE-FRESHNESS SLA (SPRINT CLUSTER 5 ITEM 3) ────────────────────────
    # Only enforced when the caller passes now_ts (existing callers/tests that
    # omit it are byte-for-byte unchanged). A quote older than the SLA is STALE:
    #   ENTRY  → NEVER price a LIMIT off a stale book. Per stale_policy either
    #            "skip" (don't enter) or "market" (DEFAULT — a MARKET fallback,
    #            flagged degraded_quote; still an order, but not priced off the
    #            stale book).
    #   EXIT   → NEVER skip; price/flatten as normal but carry degraded_quote so
    #            the caller can log the degraded fill.
    _max_age = (max_quote_age_sec if max_quote_age_sec is not None
                else getattr(cfg, "entry_quote_max_age_sec", None))
    _policy = (stale_policy if stale_policy is not None
               else getattr(cfg, "entry_stale_quote_policy", "market"))
    _qts = (quote or {}).get("ts")
    degraded_quote = False
    if (now_ts is not None and _qts is not None and _max_age is not None
            and (float(now_ts) - float(_qts)) > float(_max_age)):
        _age = float(now_ts) - float(_qts)
        if entry:
            if _policy == "skip":
                return {"ok": False, "fallback_market": False, "skip": True,
                        "degraded_quote": True, "order_type": None,
                        "price": None,
                        "reason": (f"{symbol}: ENTRY quote stale "
                                   f"({_age:.1f}s > {_max_age:.1f}s SLA) — "
                                   "SKIPPED (entry_stale_quote_policy=skip)")}
            return _market_fallback(
                f"{symbol}: ENTRY quote stale ({_age:.1f}s > {_max_age:.1f}s "
                "SLA) — MARKET fallback (not priced off the stale book)",
                degraded_quote=True)
        # EXIT: proceed (never block a needed exit) but flag the degradation.
        degraded_quote = True

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
            return _market_fallback(f"{symbol}: no usable BUY price - market fallback",
                                    degraded_quote=degraded_quote)
        if entry:
            # ENTRY (guarantee the fill). A NORMAL book — a live ask BELOW the
            # upper circuit — is a genuine MARKET order: it fills instantly at the
            # ask for ANY gap-up, and Kite margins a MARKET buy on LTP (so no
            # limit-price margin inflation that could trip an RMS margin reject).
            # ONLY a stock LOCKED at its upper circuit (no ask, or LTP at/above the
            # band) is placed as a LIMIT exactly AT the circuit — a valid QUEUED
            # order that fills when the lock breaks (the CEMPRO case).
            locked_up = (ask is None or ask <= 0) or (
                upper is not None and upper > 0 and ltp is not None and ltp >= upper)
            if locked_up and upper is not None and upper > 0:
                price = _floor_to_tick(float(upper), tick)
                if price <= 0:
                    price = round(tick, 2)
                # ITEM 4: this LIMIT is QUEUED at the circuit but does NOT sit
                # indefinitely — _reconcile_entry_fill DROPS it if it is unfilled
                # within the entry reconcile window (entry_circuit_locked_policy=
                # "drop"). Don't chase a locked stock.
                return {"ok": True, "fallback_market": False, "order_type": "LIMIT",
                        "price": price, "degraded_quote": False,
                        "reason": (f"BUY entry locked-up — LIMIT @ circuit {price} "
                                   "(dropped if unfilled within reconcile window)")}
            return _market_fallback(f"{symbol}: BUY entry — MARKET (fills at ask)",
                                    degraded_quote=degraded_quote)
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
            return _market_fallback(f"{symbol}: no usable SELL price - market fallback",
                                    degraded_quote=degraded_quote)
        if entry:
            # SHORT ENTRY (SELL to open): symmetric — a NORMAL book is a MARKET
            # order (fills at the bid on any gap-down, margined on LTP); ONLY a
            # stock LOCKED at its lower circuit is a LIMIT queued AT the circuit.
            locked_down = (bid is None or bid <= 0) or (
                lower is not None and lower > 0 and ltp is not None and ltp <= lower)
            if locked_down and lower is not None and lower > 0:
                price = _ceil_to_tick(float(lower), tick)
                if price <= 0:
                    price = round(tick, 2)
                # ITEM 4: queued at the circuit; DROPPED if unfilled within the
                # entry reconcile window (entry_circuit_locked_policy="drop").
                return {"ok": True, "fallback_market": False, "order_type": "LIMIT",
                        "price": price, "degraded_quote": False,
                        "reason": (f"SELL entry locked-down — LIMIT @ circuit "
                                   f"{price} (dropped if unfilled within "
                                   "reconcile window)")}
            return _market_fallback(f"{symbol}: SELL entry — MARKET (fills at bid)",
                                    degraded_quote=degraded_quote)
        raw = base * (1.0 - buffer_pct)
        if lower is not None and lower > 0:
            raw = max(raw, lower)          # FLOOR at the circuit (only cap)
        price = _ceil_to_tick(float(raw), tick)    # stay >= lower
        if price <= 0:
            price = round(tick, 2)

    return {"ok": True, "fallback_market": False, "order_type": "LIMIT",
            "price": price, "degraded_quote": degraded_quote,
            "reason": f"{s} marketable-limit @ {price}"}
