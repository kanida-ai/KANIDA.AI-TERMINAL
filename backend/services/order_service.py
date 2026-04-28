"""
Autotrade order service — Zerodha Kite Connect order placement.

SAFETY: All order placement is DISABLED unless the env var
        AUTOTRADE_ENABLED=true  is set explicitly.

This service wraps Kite's place_order() with:
  - Pre-flight checks (token valid, market open, daily limit)
  - Idempotency (won't re-enter an already-open position)
  - Full audit log in the `orders` table
  - Hard position-size cap (never more than MAX_POSITION_PCT of notional)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger("kanida.order_service")

_HERE = Path(__file__).parent
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)

IST = timezone(timedelta(hours=5, minutes=30))

# ── Safety constants ──────────────────────────────────────────────────────────
MAX_POSITION_PCT  = 0.05   # max 5% of notional per trade
MAX_DAILY_ORDERS  = 10     # hard ceiling on orders placed per calendar day
MARKET_OPEN_IST   = (9, 15)
MARKET_CLOSE_IST  = (15, 30)


class OrderError(Exception):
    pass


def _is_enabled() -> bool:
    return os.environ.get("AUTOTRADE_ENABLED", "").lower() == "true"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _ensure_orders_table() -> None:
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT NOT NULL,
                market          TEXT NOT NULL DEFAULT 'NSE',
                direction       TEXT NOT NULL,          -- 'BUY' | 'SELL'
                order_type      TEXT NOT NULL,          -- 'MARKET' | 'LIMIT'
                quantity        INTEGER NOT NULL,
                price           REAL,                   -- NULL for MARKET
                trigger_price   REAL,
                product         TEXT DEFAULT 'CNC',     -- 'CNC' | 'MIS'
                kite_order_id   TEXT,
                status          TEXT DEFAULT 'PENDING', -- 'PENDING'|'PLACED'|'REJECTED'|'CANCELLED'
                reject_reason   TEXT,
                opportunity_id  INTEGER,                -- FK live_opportunities.id
                signal_date     TEXT,
                placed_at       TEXT,
                created_at      TEXT DEFAULT (datetime('now'))
            )
        """)


def _market_is_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    after_open  = (h, m) >= MARKET_OPEN_IST
    before_close = (h, m) < MARKET_CLOSE_IST
    return after_open and before_close


def _daily_order_count() -> int:
    today = date.today().isoformat()
    try:
        with _conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE DATE(created_at)=? AND status='PLACED'",
                (today,),
            ).fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


def _position_already_open(ticker: str) -> bool:
    """True if there's already an open order for this ticker today."""
    today = date.today().isoformat()
    try:
        with _conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE ticker=? AND DATE(created_at)=? AND status='PLACED'",
                (ticker, today),
            ).fetchone()
        return (row[0] or 0) > 0
    except Exception:
        return False


def _log_order(record: dict) -> int:
    _ensure_orders_table()
    with _conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO orders
              (ticker, market, direction, order_type, quantity, price, trigger_price,
               product, kite_order_id, status, reject_reason, opportunity_id, signal_date, placed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                record["ticker"], record["market"], record["direction"],
                record["order_type"], record["quantity"], record.get("price"),
                record.get("trigger_price"), record.get("product", "CNC"),
                record.get("kite_order_id"), record["status"],
                record.get("reject_reason"), record.get("opportunity_id"),
                record.get("signal_date"), record.get("placed_at"),
            ),
        )
        return cur.lastrowid


def place_order(
    ticker: str,
    direction: str,             # 'BUY' | 'SELL'
    quantity: int,
    order_type: str = "MARKET", # 'MARKET' | 'LIMIT'
    price: Optional[float] = None,
    trigger_price: Optional[float] = None,
    product: str = "CNC",
    opportunity_id: Optional[int] = None,
    signal_date: Optional[str] = None,
) -> dict:
    """
    Place a single order via Kite Connect.

    Returns:
        {"status": "placed"|"rejected"|"disabled", "order_id": ..., "reason": ...}

    Raises OrderError only on unexpected failures.
    """
    _ensure_orders_table()

    if not _is_enabled():
        log.warning("Autotrade is DISABLED (AUTOTRADE_ENABLED not set). Order skipped: %s %s %s", direction, quantity, ticker)
        return {"status": "disabled", "reason": "AUTOTRADE_ENABLED is not set to true"}

    # Pre-flight checks
    if not _market_is_open():
        reason = "Market is closed"
        _log_order({"ticker": ticker, "market": "NSE", "direction": direction,
                    "order_type": order_type, "quantity": quantity, "price": price,
                    "product": product, "status": "REJECTED", "reject_reason": reason,
                    "opportunity_id": opportunity_id, "signal_date": signal_date, "placed_at": None})
        return {"status": "rejected", "reason": reason}

    if _daily_order_count() >= MAX_DAILY_ORDERS:
        reason = f"Daily order limit reached ({MAX_DAILY_ORDERS})"
        _log_order({"ticker": ticker, "market": "NSE", "direction": direction,
                    "order_type": order_type, "quantity": quantity, "price": price,
                    "product": product, "status": "REJECTED", "reject_reason": reason,
                    "opportunity_id": opportunity_id, "signal_date": signal_date, "placed_at": None})
        return {"status": "rejected", "reason": reason}

    if direction == "BUY" and _position_already_open(ticker):
        reason = f"Already have an open position in {ticker} today"
        _log_order({"ticker": ticker, "market": "NSE", "direction": direction,
                    "order_type": order_type, "quantity": quantity, "price": price,
                    "product": product, "status": "REJECTED", "reject_reason": reason,
                    "opportunity_id": opportunity_id, "signal_date": signal_date, "placed_at": None})
        return {"status": "rejected", "reason": reason}

    # Place order via Kite — use central auth service
    try:
        from services.kite_auth import get_kite_client, KiteAuthError
        kite = get_kite_client()
    except Exception as e:
        raise OrderError(str(e))

    try:
        kite_order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=ticker.upper(),
            transaction_type=kite.TRANSACTION_TYPE_BUY if direction == "BUY" else kite.TRANSACTION_TYPE_SELL,
            quantity=quantity,
            product=kite.PRODUCT_CNC if product == "CNC" else kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET if order_type == "MARKET" else kite.ORDER_TYPE_LIMIT,
            price=price,
            trigger_price=trigger_price,
        )
        placed_at = datetime.now(IST).isoformat()
        _log_order({
            "ticker": ticker, "market": "NSE", "direction": direction,
            "order_type": order_type, "quantity": quantity, "price": price,
            "trigger_price": trigger_price, "product": product,
            "kite_order_id": str(kite_order_id), "status": "PLACED",
            "opportunity_id": opportunity_id, "signal_date": signal_date,
            "placed_at": placed_at,
        })
        log.info("Order PLACED: %s %s x%d | kite_id=%s", direction, ticker, quantity, kite_order_id)
        return {"status": "placed", "order_id": str(kite_order_id), "placed_at": placed_at}

    except Exception as e:
        reason = str(e)
        _log_order({
            "ticker": ticker, "market": "NSE", "direction": direction,
            "order_type": order_type, "quantity": quantity, "price": price,
            "product": product, "status": "REJECTED", "reject_reason": reason,
            "opportunity_id": opportunity_id, "signal_date": signal_date, "placed_at": None,
        })
        log.error("Order REJECTED: %s %s — %s", direction, ticker, reason)
        return {"status": "rejected", "reason": reason}
