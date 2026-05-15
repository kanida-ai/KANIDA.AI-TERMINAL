"""Per-share MTF margin calculator.

Asks Kite's order_margins API what margin Zerodha will lock for a given symbol
under MTF, sized for qty=1. Used by order_planner to compute correct qty when
the operator's PER_TRADE budget is interpreted as MARGIN (not cash):

    qty_mtf = floor(PER_TRADE / per_share_margin)

Strategy backtests assumed full MTF deployment. Without margin-based sizing
qty would be ~2-3x too low for typical MTF stocks (margin is 30-40% of notional),
causing live to under-perform backtest by the same factor.

Failure mode (skip-on-None): on any Kite API error we return None for that
symbol. The caller MUST fall back to cash sizing for that symbol with a visible
warning — never silently undersize. Identical principle as trail_data.

Caching: per (symbol, product) key with 24-hour TTL. Margins effectively
update overnight (Zerodha re-runs SPAN/VAR EOD); intraday changes are rare
and cosmetic. Cache invalidated on token refresh or process restart.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("kanida.falcon.trade.margin")
IST = timezone(timedelta(hours=5, minutes=30))
TTL = timedelta(hours=24)

# Cache: (symbol, product) -> (per_share_margin, fetched_at)
_cache: Dict[Tuple[str, str], Tuple[float, datetime]] = {}


def _build_order_payload(symbol: str, product: str) -> dict:
    """Order envelope for kite.order_margins() probe (qty=1, MARKET)."""
    return {
        "exchange":         "NSE",
        "tradingsymbol":    symbol,
        "transaction_type": "BUY",
        "variety":          "regular",
        "product":          product,
        "order_type":       "MARKET",
        "quantity":         1,
    }


def _extract_total(row: dict) -> Optional[float]:
    """Pull `total` margin from one Kite order_margins response row."""
    if not isinstance(row, dict):
        return None
    total = row.get("total")
    if total is None:
        # Older SDK shape: sum components
        components = ("span", "exposure", "var", "additional", "bo", "cash")
        try:
            total = sum(float(row.get(k) or 0) for k in components)
        except Exception:
            return None
    try:
        v = float(total)
        return v if v > 0 else None
    except Exception:
        return None


def fetch_margins_batch(kite, items: List[Tuple[str, str]],
                         force_refresh: bool = False) -> Dict[str, float]:
    """items = [(symbol, product), ...]. Returns {symbol: per_share_margin}.

    Symbols that error out are absent — caller falls back. ONE Kite API call
    per batch (not per symbol). Cache hits skip the API entirely. Mixed cached
    + fresh inputs are handled — we batch only the uncached ones, then merge.
    """
    if not items:
        return {}

    now = datetime.now(IST)
    out: Dict[str, float] = {}
    pending: List[Tuple[str, str]] = []

    # Cache check
    for sym, product in items:
        if not force_refresh:
            cached = _cache.get((sym, product))
            if cached and now - cached[1] < TTL:
                out[sym] = cached[0]
                continue
        pending.append((sym, product))

    if not pending:
        return out

    payload = [_build_order_payload(s, p) for s, p in pending]
    try:
        resp = kite.order_margins(payload)
    except Exception as e:
        log.warning("margin_calc: kite.order_margins() failed for batch of %d: %s",
                    len(pending), e)
        return out  # caller falls back to cash sizing for missing syms

    if not isinstance(resp, list):
        log.warning("margin_calc: unexpected order_margins shape: %s", type(resp).__name__)
        return out

    # Kite returns rows in the same order as the input. Match by index.
    for (sym, product), row in zip(pending, resp):
        margin = _extract_total(row)
        if margin is None or margin <= 0:
            log.warning("margin_calc: no margin in response for %s (product=%s): %s",
                        sym, product, row)
            continue
        _cache[(sym, product)] = (margin, now)
        out[sym] = margin

    return out


def get_per_share_margin(kite, symbol: str,
                          product: str = "MTF",
                          force_refresh: bool = False) -> Optional[float]:
    """Single-symbol convenience wrapper. Returns None on any error."""
    result = fetch_margins_batch(kite, [(symbol, product)], force_refresh=force_refresh)
    return result.get(symbol)


def invalidate(symbol: Optional[str] = None) -> None:
    """Clear cache. If symbol given, only that symbol's entries; else all.
    Called on token refresh."""
    global _cache
    if symbol is None:
        _cache = {}
        return
    _cache = {k: v for k, v in _cache.items() if k[0] != symbol}


def cache_size() -> int:
    return len(_cache)
