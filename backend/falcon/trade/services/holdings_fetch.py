"""Fetch held positions from Kite. 5-min in-memory cache.

Combines kite.holdings() (long-term + MTF carry-overs) with
kite.positions().net (intraday). Enriches entry dates from falcon_trade_orders.

F13: soft-fail on Kite API errors — returns empty dict so /preview can still run
with overlap detection disabled.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
from typing import Dict, Optional

from ...db import falcon_conn

log = logging.getLogger("kanida.falcon.trade.holdings")

_cache: Optional[Dict[str, dict]] = None
_fetched_at: Optional[datetime] = None
_last_error: Optional[str] = None
TTL = timedelta(minutes=5)


def _enrich_with_entry_dates(held: Dict[str, dict]) -> None:
    if not held:
        return
    syms = tuple(held.keys())
    placeholders = ",".join("?" * len(syms))
    today = datetime.now(IST).date()  # IST — not local server clock
    with falcon_conn() as con:
        rows = con.execute(f"""
            SELECT o.symbol, MIN(r.entry_date) AS entry_date
            FROM falcon_trade_orders o
            JOIN falcon_trade_runs r ON r.batch_id = o.batch_id
            WHERE o.role = 'ENTRY' AND o.status = 'PLACED' AND o.symbol IN ({placeholders})
            GROUP BY o.symbol
        """, syms).fetchall()
    for r in rows:
        h = held.get(r['symbol'])
        if not h or not r['entry_date']:
            continue
        h['entry_date'] = r['entry_date']
        try:
            ed = datetime.fromisoformat(r['entry_date']).date()
            h['days_held'] = (today - ed).days
        except Exception:
            h['days_held'] = None


def get_held_positions(kite, force_refresh: bool = False) -> Dict[str, dict]:
    """{symbol: {qty, avg_entry, current_price, product, entry_date, days_held}}."""
    global _cache, _fetched_at, _last_error
    now = datetime.now()
    if (not force_refresh and _cache is not None and _fetched_at
            and now - _fetched_at < TTL):
        return _cache

    held: Dict[str, dict] = {}
    try:
        # kite.holdings() returns BOTH CNC delivery (in top-level `quantity`) AND
        # MTF positions (in nested `mtf.quantity`). We handle both. Same symbol can
        # in rare cases have both — we combine with a weighted-average entry.
        for h in kite.holdings():
            sym = h.get("tradingsymbol")
            if not sym:
                continue
            cur_price = h.get("last_price") or 0
            cnc_qty = h.get("quantity", 0) or 0
            cnc_avg = h.get("average_price") or 0
            t1_qty = h.get("t1_quantity", 0) or 0   # bought today, in T+1 settlement
            mtf_obj = h.get("mtf") or {}
            mtf_qty = mtf_obj.get("quantity", 0) or 0
            mtf_avg = mtf_obj.get("average_price") or 0

            if cnc_qty <= 0 and mtf_qty <= 0:
                continue

            if cnc_qty > 0 and mtf_qty <= 0:
                qty, avg, product = cnc_qty, cnc_avg, "CNC"
            elif mtf_qty > 0 and cnc_qty <= 0:
                qty, avg, product = mtf_qty, mtf_avg, "MTF"
            else:
                # Rare: both CNC and MTF for the same symbol — combine
                qty = cnc_qty + mtf_qty
                avg = ((cnc_qty * cnc_avg) + (mtf_qty * mtf_avg)) / qty if qty else 0
                product = "MIXED"

            held[sym] = {
                "qty":           qty,
                "avg_entry":     avg,
                "current_price": cur_price,
                "product":       product,
                "entry_date":    None,
                "days_held":     None,
                # Hint for entry-date inference: if the entire current quantity
                # is in T+1 settlement, the position was opened today.
                "is_t1_only":    (t1_qty > 0 and t1_qty >= qty),
                "t1_quantity":   t1_qty,
            }

        positions = kite.positions() or {}
        for p in positions.get("net", []):
            qty = p.get("quantity", 0) or 0
            sym = p.get("tradingsymbol")
            if qty > 0 and sym and sym not in held:
                held[sym] = {
                    "qty":           qty,
                    "avg_entry":     p.get("average_price") or 0,
                    "current_price": p.get("last_price") or 0,
                    "product":       p.get("product", "MIS"),
                    "entry_date":    None,
                    "days_held":     None,
                }
    except Exception as e:
        # F13 soft-fail
        _last_error = str(e)
        log.warning("kite.holdings/positions failed: %s — returning empty", e)
        _cache = {}
        _fetched_at = now
        return _cache

    _enrich_with_entry_dates(held)
    _cache = held
    _fetched_at = now
    _last_error = None
    return _cache


def last_error() -> Optional[str]:
    return _last_error


def invalidate() -> None:
    global _cache, _fetched_at, _last_error
    _cache = None
    _fetched_at = None
    _last_error = None


def get_circuit_limits(kite, symbols: list) -> Dict[str, dict]:
    """Fetch lower_circuit_limit + upper_circuit_limit for symbols via kite.quote().
    Returns {symbol: {"lower": float, "upper": float, "ltp": float}}.
    Soft-fails to {} on error."""
    if not symbols:
        return {}
    out: Dict[str, dict] = {}
    try:
        keys = [f"NSE:{s}" for s in symbols]
        quotes = kite.quote(keys)
        for k, q in quotes.items():
            sym = k.replace("NSE:", "")
            out[sym] = {
                "lower": float(q.get("lower_circuit_limit") or 0),
                "upper": float(q.get("upper_circuit_limit") or 0),
                "ltp":   float(q.get("last_price") or 0),
            }
    except Exception as e:
        log.warning("kite.quote() for circuit limits failed: %s", e)
    return out
