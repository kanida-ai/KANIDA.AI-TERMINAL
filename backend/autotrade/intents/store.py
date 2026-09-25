"""Durable state for strategy intents, their legs and operator arms (OLTP DB via oltp_conn).

Portable DDL (TEXT keys, no AUTOINCREMENT) so it runs on SQLite today and Postgres when KANIDA_PG_ENABLED.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from oltp_db import oltp_conn

log = logging.getLogger("kanida.autotrade.intents")
IST = timezone(timedelta(hours=5, minutes=30))

DDL = [
    """CREATE TABLE IF NOT EXISTS autotrade_strategy_intents (
        id TEXT PRIMARY KEY, source TEXT NOT NULL, idempotency_key TEXT NOT NULL, payload_hash TEXT NOT NULL,
        user_id TEXT NOT NULL DEFAULT '', broker TEXT NOT NULL, broker_account_id TEXT NOT NULL DEFAULT '',
        mode TEXT NOT NULL, state TEXT NOT NULL, reason TEXT, max_loss REAL, net_premium REAL,
        reference TEXT, gates TEXT, arm_id TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_intent_key ON autotrade_strategy_intents(source, idempotency_key)",
    """CREATE TABLE IF NOT EXISTS autotrade_strategy_intent_legs (
        id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, seq INTEGER NOT NULL, tradingsymbol TEXT NOT NULL,
        underlying TEXT NOT NULL, expiry TEXT NOT NULL, strike REAL NOT NULL, option_type TEXT NOT NULL,
        side TEXT NOT NULL, quantity INTEGER NOT NULL, lot_size INTEGER NOT NULL, limit_price REAL NOT NULL,
        product TEXT NOT NULL, group_no INTEGER NOT NULL, client_order_id TEXT, broker_order_id TEXT,
        state TEXT NOT NULL, filled_qty INTEGER NOT NULL DEFAULT 0, avg_price REAL, error TEXT, updated_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS ix_strategy_intent_legs ON autotrade_strategy_intent_legs(intent_id, seq)",
    """CREATE TABLE IF NOT EXISTS autotrade_strategy_intent_arms (
        id TEXT PRIMARY KEY, user_id TEXT NOT NULL DEFAULT '', broker_account_id TEXT NOT NULL DEFAULT '',
        armed_by TEXT NOT NULL, armed_at TEXT NOT NULL, expires_at TEXT NOT NULL, max_baskets INTEGER NOT NULL,
        baskets_used INTEGER NOT NULL DEFAULT 0, max_loss_per_basket REAL NOT NULL, disarmed_at TEXT, note TEXT)""",
]

TERMINAL = {"completed", "dry_run_complete", "blocked", "failed", "cancelled", "attention_required"}
_schema_ready = False


def now_ist() -> datetime:
    """The autotrade clock (honours set_fake_now / FALCON_AUTOTRADE_FAKE_NOW), naive IST."""
    try:
        from ..session import now_ist as _n
        n = _n()
    except Exception:  # pragma: no cover - defensive
        n = datetime.now(IST)
    return n.replace(tzinfo=None) if n.tzinfo else n


def _iso(d: Optional[datetime] = None) -> str:
    return (d or now_ist()).strftime("%Y-%m-%dT%H:%M:%S")


def ensure_schema() -> None:
    global _schema_ready
    if _schema_ready:
        return
    with oltp_conn() as con:
        for stmt in DDL:
            con.execute(stmt)
        con.commit()
    _schema_ready = True


def _row(r) -> Dict[str, Any]:
    return dict(r) if r is not None else None


def find_by_key(source: str, key: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    with oltp_conn() as con:
        return _row(con.execute("SELECT * FROM autotrade_strategy_intents WHERE source=? AND idempotency_key=?",
                                (source, key)).fetchone())


def create(basket: Dict[str, Any], state: str, reason: Optional[str], gates: List[Dict[str, Any]],
           arm_id: Optional[str] = None) -> str:
    """Insert the basket and its legs in one transaction. Raises on a duplicate key (caller re-reads)."""
    ensure_schema()
    iid = uuid.uuid4().hex
    t = _iso()
    with oltp_conn() as con:
        con.execute(
            """INSERT INTO autotrade_strategy_intents (id, source, idempotency_key, payload_hash, user_id, broker,
               broker_account_id, mode, state, reason, max_loss, net_premium, reference, gates, arm_id,
               cancel_requested, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)""",
            (iid, basket["source"], basket["idempotency_key"], basket["payload_hash"], str(basket.get("user_id") or ""),
             basket["broker"], str(basket.get("broker_account_id") or ""), basket["mode"], state, reason,
             basket["max_loss"], basket["net_premium"], json.dumps(basket.get("reference") or {}),
             json.dumps(gates), arm_id, t, t))
        for i, l in enumerate(basket["legs"]):
            con.execute(
                """INSERT INTO autotrade_strategy_intent_legs (id, intent_id, seq, tradingsymbol, underlying, expiry,
                   strike, option_type, side, quantity, lot_size, limit_price, product, group_no, state, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (uuid.uuid4().hex, iid, i, l["tradingsymbol"], l["underlying"], l["expiry"], l["strike"],
                 l["option_type"], l["side"], l["quantity"], l["lot_size"], l["limit_price"], l["product"],
                 l["group"], "pending" if state not in TERMINAL else "not_sent", t))
        con.commit()
    return iid


def get(iid: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    with oltp_conn() as con:
        r = _row(con.execute("SELECT * FROM autotrade_strategy_intents WHERE id=?", (iid,)).fetchone())
        if not r:
            return None
        legs = [dict(x) for x in con.execute(
            "SELECT * FROM autotrade_strategy_intent_legs WHERE intent_id=? ORDER BY seq", (iid,)).fetchall()]
    r["reference"] = json.loads(r.get("reference") or "{}")
    r["gates"] = json.loads(r.get("gates") or "[]")
    r["cancel_requested"] = bool(r.get("cancel_requested"))
    for l in legs:
        l["group"] = l.pop("group_no")
    r["legs"] = legs
    return r


def list_for(source: Optional[str], user_id: Optional[str], limit: int = 50) -> List[Dict[str, Any]]:
    ensure_schema()
    q, p = "SELECT id FROM autotrade_strategy_intents WHERE 1=1", []
    if source:
        q += " AND source=?"
        p.append(source)
    if user_id is not None:
        q += " AND user_id=?"
        p.append(str(user_id))
    q += " ORDER BY created_at DESC LIMIT ?"
    p.append(int(limit))
    with oltp_conn() as con:
        ids = [r["id"] for r in con.execute(q, p).fetchall()]
    return [get(i) for i in ids]


def set_state(iid: str, state: str, reason: Optional[str] = None, *, only_from: Optional[set] = None) -> bool:
    """Guarded transition: with only_from, the update applies only if the current state is in it."""
    ensure_schema()
    q = "UPDATE autotrade_strategy_intents SET state=?, reason=COALESCE(?, reason), updated_at=? WHERE id=?"
    p: list = [state, reason, _iso(), iid]
    if only_from:
        q += f" AND state IN ({','.join('?' * len(only_from))})"
        p += sorted(only_from)
    with oltp_conn() as con:
        cur = con.execute(q, p)
        con.commit()
        return (cur.rowcount or 0) > 0


def set_gates(iid: str, gates: List[Dict[str, Any]]) -> None:
    with oltp_conn() as con:
        con.execute("UPDATE autotrade_strategy_intents SET gates=?, updated_at=? WHERE id=?",
                    (json.dumps(gates), _iso(), iid))
        con.commit()


def request_cancel(iid: str) -> None:
    with oltp_conn() as con:
        con.execute("UPDATE autotrade_strategy_intents SET cancel_requested=1, updated_at=? WHERE id=?", (_iso(), iid))
        con.commit()


def update_leg(leg_id: str, **fields: Any) -> None:
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields)
    with oltp_conn() as con:
        con.execute(f"UPDATE autotrade_strategy_intent_legs SET {cols}, updated_at=? WHERE id=?",
                    (*fields.values(), _iso(), leg_id))
        con.commit()


# ── operator arms ──────────────────────────────────────────────────────────────────────────────────────────────────

def arm(*, user_id: Optional[str], broker_account_id: Optional[str], armed_by: str, ttl_minutes: int,
        max_baskets: int, max_loss_per_basket: float, note: str = "") -> Dict[str, Any]:
    ensure_schema()
    aid = uuid.uuid4().hex
    n = now_ist()
    with oltp_conn() as con:
        # one active arm per user + account: arming again replaces the previous arm
        con.execute("""UPDATE autotrade_strategy_intent_arms SET disarmed_at=? WHERE user_id=? AND broker_account_id=?
                       AND disarmed_at IS NULL""", (_iso(n), str(user_id or ""), str(broker_account_id or "")))
        con.execute(
            """INSERT INTO autotrade_strategy_intent_arms (id, user_id, broker_account_id, armed_by, armed_at,
               expires_at, max_baskets, baskets_used, max_loss_per_basket, note) VALUES (?,?,?,?,?,?,?,0,?,?)""",
            (aid, str(user_id or ""), str(broker_account_id or ""), armed_by, _iso(n),
             _iso(n + timedelta(minutes=ttl_minutes)), int(max_baskets), float(max_loss_per_basket), note[:200]))
        con.commit()
    return active_arm(user_id, broker_account_id)


def disarm(user_id: Optional[str], broker_account_id: Optional[str]) -> int:
    ensure_schema()
    with oltp_conn() as con:
        cur = con.execute("""UPDATE autotrade_strategy_intent_arms SET disarmed_at=? WHERE user_id=? AND
                             broker_account_id=? AND disarmed_at IS NULL""",
                          (_iso(), str(user_id or ""), str(broker_account_id or "")))
        con.commit()
        return cur.rowcount or 0


def active_arm(user_id: Optional[str], broker_account_id: Optional[str]) -> Optional[Dict[str, Any]]:
    ensure_schema()
    with oltp_conn() as con:
        return _row(con.execute(
            """SELECT * FROM autotrade_strategy_intent_arms WHERE user_id=? AND broker_account_id=? AND
               disarmed_at IS NULL AND expires_at>? ORDER BY armed_at DESC LIMIT 1""",
            (str(user_id or ""), str(broker_account_id or ""), _iso())).fetchone())


def consume_arm(arm_id: str) -> bool:
    """Atomically take one basket from the arm's allowance; False when expired, disarmed or used up."""
    with oltp_conn() as con:
        cur = con.execute(
            """UPDATE autotrade_strategy_intent_arms SET baskets_used=baskets_used+1 WHERE id=? AND
               disarmed_at IS NULL AND expires_at>? AND baskets_used<max_baskets""", (arm_id, _iso()))
        con.commit()
        return (cur.rowcount or 0) > 0


def refund_arm(arm_id: str) -> None:
    with oltp_conn() as con:
        con.execute("""UPDATE autotrade_strategy_intent_arms SET baskets_used=baskets_used-1 WHERE id=? AND
                       baskets_used>0""", (arm_id,))
        con.commit()
