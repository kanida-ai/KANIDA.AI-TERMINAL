"""falcon_trade_runs + falcon_trade_orders CRUD + audit helpers.

Reads/writes the kanida_universe.db via falcon.db.falcon_conn.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from ...db import falcon_conn


def create_run(*, batch_id: str, signal_date: str, entry_date: str,
               selected_symbols: List[str], total_capital: int, per_trade: int,
               use_leverage: bool, hold_days: int, sl_pct: float,
               trail_trigger_pct: float, trail_lookback_days: int,
               started_at: str) -> None:
    with falcon_conn() as con:
        con.execute("""
            INSERT INTO falcon_trade_runs
              (batch_id, started_at, status, signal_date, entry_date, selected_symbols,
               total_capital, per_trade, use_leverage, hold_days, sl_pct,
               trail_trigger_pct, trail_lookback_days)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (batch_id, started_at, 'PLACING', signal_date, entry_date,
              json.dumps(selected_symbols), total_capital, per_trade,
              1 if use_leverage else 0, hold_days, sl_pct, trail_trigger_pct,
              trail_lookback_days))
        con.commit()


def insert_order(*, batch_id: str, symbol: str, side: str, role: str, qty: int,
                 idempotency_key: str, is_averaging: bool = False,
                 kite_order_id: Optional[str] = None, price: Optional[float] = None,
                 trigger_price: Optional[float] = None, product: str = 'MTF',
                 order_type: str = 'MARKET', status: str = 'PENDING',
                 error: Optional[str] = None) -> int:
    placed_at = datetime.now().isoformat() if status == 'PLACED' else None
    with falcon_conn() as con:
        cur = con.execute("""
            INSERT INTO falcon_trade_orders
              (batch_id, symbol, side, role, is_averaging, kite_order_id, qty, price,
               trigger_price, product, order_type, status, error, idempotency_key,
               placed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (batch_id, symbol, side, role, 1 if is_averaging else 0,
              kite_order_id, qty, price, trigger_price, product, order_type,
              status, error, idempotency_key, placed_at))
        con.commit()
        return cur.lastrowid


def update_order_status(order_id: int, status: str,
                        kite_order_id: Optional[str] = None,
                        error: Optional[str] = None) -> None:
    placed_at = datetime.now().isoformat() if status == 'PLACED' else None
    with falcon_conn() as con:
        con.execute("""
            UPDATE falcon_trade_orders
            SET status        = ?,
                kite_order_id = COALESCE(?, kite_order_id),
                error         = COALESCE(?, error),
                placed_at     = COALESCE(placed_at, ?)
            WHERE id = ?
        """, (status, kite_order_id, error, placed_at, order_id))
        con.commit()


def finalize_run(*, batch_id: str, status: str, n_attempted: int, n_filled: int,
                 n_failed: int, n_overlap: int, n_skip_action: int,
                 n_average_action: int, total_deployed: int, finished_at: str,
                 notes: Optional[str] = None) -> None:
    with falcon_conn() as con:
        con.execute("""
            UPDATE falcon_trade_runs
            SET finished_at = ?, status = ?, n_attempted = ?, n_filled = ?,
                n_failed = ?, n_overlap = ?, n_skip_action = ?,
                n_average_action = ?, total_deployed = ?,
                notes = COALESCE(?, notes)
            WHERE batch_id = ?
        """, (finished_at, status, n_attempted, n_filled, n_failed,
              n_overlap, n_skip_action, n_average_action,
              total_deployed, notes, batch_id))
        con.commit()


def get_run(batch_id: str) -> Optional[Dict[str, Any]]:
    with falcon_conn() as con:
        row = con.execute("SELECT * FROM falcon_trade_runs WHERE batch_id = ?",
                          (batch_id,)).fetchone()
    return dict(row) if row else None


def get_orders_for_batch(batch_id: str) -> List[Dict[str, Any]]:
    with falcon_conn() as con:
        rows = con.execute("""
            SELECT * FROM falcon_trade_orders
            WHERE batch_id = ?
            ORDER BY id ASC
        """, (batch_id,)).fetchall()
    return [dict(r) for r in rows]


def has_active_batch_for_today() -> bool:
    """S12: refuse new /place if a batch is already PLACING for today's entry_date."""
    today = date.today().isoformat()
    with falcon_conn() as con:
        row = con.execute("""
            SELECT 1 FROM falcon_trade_runs
            WHERE entry_date = ? AND status IN ('PENDING', 'PLACING')
            LIMIT 1
        """, (today,)).fetchone()
    return row is not None


def get_open_sl_orders() -> Dict[str, Dict[str, Any]]:
    """{symbol: {sl_price, kite_order_id, sl_type, batch_id, order_id, product}}.
    Only the most-recent open SL per symbol is returned."""
    with falcon_conn() as con:
        rows = con.execute("""
            SELECT id, symbol, trigger_price, kite_order_id, order_type, batch_id, product
            FROM falcon_trade_orders
            WHERE role = 'STOP' AND status IN ('PLACED', 'PENDING')
            ORDER BY id DESC
        """).fetchall()
    seen: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if r['symbol'] not in seen:
            seen[r['symbol']] = {
                'sl_price':      r['trigger_price'],
                'kite_order_id': r['kite_order_id'],
                'sl_type':       r['order_type'],
                'batch_id':      r['batch_id'],
                'order_id':      r['id'],
                'product':       r['product'],
            }
    return seen


def get_open_sl_order(symbol: str) -> Optional[Dict[str, Any]]:
    with falcon_conn() as con:
        row = con.execute("""
            SELECT id, symbol, trigger_price, kite_order_id, order_type, batch_id, product
            FROM falcon_trade_orders
            WHERE symbol = ? AND role = 'STOP' AND status IN ('PLACED', 'PENDING')
            ORDER BY id DESC
            LIMIT 1
        """, (symbol,)).fetchone()
    if not row:
        return None
    return {
        'id':            row['id'],
        'symbol':        row['symbol'],
        'sl_price':      row['trigger_price'],
        'kite_order_id': row['kite_order_id'],
        'sl_type':       row['order_type'],
        'batch_id':      row['batch_id'],
        'product':       row['product'],
    }


def get_entry_dates() -> Dict[str, str]:
    """{symbol: entry_date} — single source of truth.

    Priority (most accurate first):
      1. falcon_trade_orders ENTRY rows (Falcon-placed exact date)
      2. falcon_position_first_seen.first_seen_date (tradebook import or first-poll anchor)

    Frontend /positions uses this to populate the Entry date column."""
    out: Dict[str, str] = {}
    with falcon_conn() as con:
        # 1. Exact dates from Falcon-placed entries
        for r in con.execute("""
            SELECT o.symbol, MIN(r.entry_date) AS entry_date
            FROM falcon_trade_orders o
            JOIN falcon_trade_runs r ON r.batch_id = o.batch_id
            WHERE o.role = 'ENTRY' AND o.status = 'PLACED'
            GROUP BY o.symbol
        """).fetchall():
            if r['entry_date']:
                out[r['symbol']] = r['entry_date']
        # 2. Fallback from first-seen table (tradebook import / first-poll anchor)
        for r in con.execute("""
            SELECT symbol, first_seen_date FROM falcon_position_first_seen
        """).fetchall():
            if r['first_seen_date'] and r['symbol'] not in out:
                out[r['symbol']] = r['first_seen_date']
    return out
