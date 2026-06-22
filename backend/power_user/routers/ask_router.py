"""Ask-Falcon read-only API — universe, EOD quotes, per-stock analysis.

  GET /api/power/universe/symbols          → full covered-symbol list (search dropdown)
  GET /api/power/quote?symbols=A,B,...      → last EOD close + prev close per symbol
  GET /api/power/ask/analyze-stock?symbol=  → per-stock analysis for any covered stock

ALL endpoints are read-only and PUBLIC (NOT auth-gated) — same policy as the
falcon_top20_router: the data is engine output, not a secret; the page that
calls them gates access at the UI layer. They use the same get_db dependency
the other power_user routers use, so they work in the same contexts.

Honesty rules (locked):
  - No company-name table exists → `name` == `symbol` (never invented).
  - /quote returns `last_close` (the most recent EOD close), NOT a live tick.
    Every quote carries `as_of` so the frontend labels it "last close (EOD)".
  - analyze-stock only states what the real feature/ohlc rows support; values
    that aren't derivable come back null with a clear flag.
  - Touches ONLY read tables. Never goes near the auto-trade execution path.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from .dependencies import get_db
from ..services.ask_analysis import build_analysis

log = logging.getLogger("kanida.power_user.ask_router")
router = APIRouter(prefix="/api/power", tags=["power_user_ask"])

# Bound the /quote batch so a single call can't fan out into 500 lookups.
MAX_QUOTE_SYMBOLS = 60

# ── In-process cache for the (heavy-ish, rarely-changing) universe list ─────
_UNIVERSE_CACHE: Dict[str, Any] = {"payload": None, "at": 0.0}
_UNIVERSE_TTL = 3600.0   # ~1 hour; rolls over naturally when a new EOD lands


# ──────────────────────────────────────────────────────────────────────────
# 1. Universe — distinct covered symbols (latest feature date) + sector
# ──────────────────────────────────────────────────────────────────────────

@router.get("/universe/symbols")
def universe_symbols(con: sqlite3.Connection = Depends(get_db)) -> Dict[str, Any]:
    """Full list of covered symbols for the Ask-Falcon search dropdown.

    Source: distinct symbols present in falcon_features on the LATEST feature
    date, left-joined to falcon_sectors for sector. No company-name table
    exists → name = symbol (NOT invented). Sorted A→Z. Cached ~1h.
    """
    now = time.time()
    if _UNIVERSE_CACHE["payload"] is not None and (now - _UNIVERSE_CACHE["at"]) < _UNIVERSE_TTL:
        return _UNIVERSE_CACHE["payload"]

    latest = con.execute("SELECT MAX(trade_date) FROM falcon_features").fetchone()
    latest_date = latest[0] if latest else None
    if not latest_date:
        payload = {"as_of": None, "count": 0, "symbols": []}
        _UNIVERSE_CACHE.update(payload=payload, at=now)
        return payload

    rows = con.execute(
        """
        SELECT f.symbol, s.sector
          FROM (SELECT DISTINCT symbol FROM falcon_features WHERE trade_date = ?) f
          LEFT JOIN falcon_sectors s ON s.symbol = f.symbol
         ORDER BY f.symbol
        """,
        (latest_date,),
    ).fetchall()

    symbols = [
        {"symbol": r[0], "name": r[0], "sector": (r[1] or None)}
        for r in rows
    ]
    payload = {"as_of": latest_date, "count": len(symbols), "symbols": symbols}
    _UNIVERSE_CACHE.update(payload=payload, at=now)
    return payload


# ──────────────────────────────────────────────────────────────────────────
# 2. Quote — last EOD close + prev close per symbol (NOT a live tick)
# ──────────────────────────────────────────────────────────────────────────

@router.get("/quote")
def quote(
    symbols: str = Query(..., description="Comma-separated symbols (max 60)"),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Dict[str, Any]]:
    """Last EOD close + previous close for each requested symbol.

    HONEST NAMING: `last_close` is the most recent end-of-day close (NOT a live
    LTP). `prev_close` is the bar before it. `as_of` is the last trade_date so
    the frontend can label it "last close (EOD)".

    One batched query over ohlc_daily for all symbols. Unknown symbols are
    simply absent from the result map (no fabricated rows). Capped at 60 to
    bound cost.
    """
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    # De-dupe while preserving order, then cap.
    seen: set = set()
    uniq: List[str] = []
    for s in syms:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    if not uniq:
        raise HTTPException(400, {"code": "NO_SYMBOLS",
                                   "message": "Provide at least one symbol."})
    if len(uniq) > MAX_QUOTE_SYMBOLS:
        raise HTTPException(400, {
            "code":    "TOO_MANY_SYMBOLS",
            "message": f"At most {MAX_QUOTE_SYMBOLS} symbols per call (got {len(uniq)}).",
        })

    placeholders = ",".join("?" * len(uniq))
    # Pull the last two bars per symbol via a window function — one round trip.
    rows = con.execute(
        f"""
        SELECT symbol, trade_date, close, rn FROM (
            SELECT symbol, trade_date, close,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
              FROM ohlc_daily
             WHERE symbol IN ({placeholders})
        ) WHERE rn <= 2
        ORDER BY symbol, rn
        """,
        tuple(uniq),
    ).fetchall()

    by_sym: Dict[str, Dict[int, Any]] = {}
    for sym, td, close, rn in rows:
        by_sym.setdefault(sym, {})[rn] = {"trade_date": td, "close": close}

    out: Dict[str, Dict[str, Any]] = {}
    for sym, bars in by_sym.items():
        last = bars.get(1)
        prev = bars.get(2)
        if not last:
            continue
        out[sym] = {
            "last_close": round(last["close"], 2) if last["close"] is not None else None,
            "prev_close": (round(prev["close"], 2)
                           if prev and prev["close"] is not None else None),
            "as_of":      last["trade_date"],
        }
    return out


# ──────────────────────────────────────────────────────────────────────────
# 3. analyze-stock — per-stock analysis for any covered stock
# ──────────────────────────────────────────────────────────────────────────

@router.get("/ask/analyze-stock")
def analyze_stock(
    symbol: str = Query(..., min_length=1, max_length=32),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Per-stock analysis built entirely from real data (see ask_analysis).

    404 NOT_COVERED if the symbol isn't in falcon_features (not engine-covered).
    """
    try:
        result = build_analysis(con, symbol)
    except Exception as e:                                  # noqa: BLE001
        log.exception("analyze-stock failed for %s: %s", symbol, e)
        raise HTTPException(500, {"code": "ANALYZE_FAILED", "message": str(e)[:200]})
    if result is None:
        raise HTTPException(404, {
            "code":    "NOT_COVERED",
            "message": f"{symbol.strip().upper()} is not covered by the Falcon engine.",
        })
    return result
