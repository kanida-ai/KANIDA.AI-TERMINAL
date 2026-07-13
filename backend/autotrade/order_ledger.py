"""Durable append-only ORDER-EVENT LEDGER + client_order_id / broker-tag helpers.

SPRINT CLUSTER 2 — the durable order ledger + idempotency backbone. This module
is the single home for:

  * client_order_id generation — a unique, durable ownership key minted BEFORE a
    broker submission ("FAL-<sess8>-<sym>-<epochms>-<attempt>").
  * compact_tag() — a <=20-char alphanumeric deterministic hash of a
    client_order_id, small enough for Kite's `tag` param (max 20 chars). The FULL
    client_order_id is stored in our DB (position row + ledger); the compact tag
    is what rides on the broker order and lets us recognise OUR OWN orders in the
    broker orderbook (tag-precise kill-cancel; future query-before-place).
  * append_event() — INSERT OR IGNORE into autotrade_order_events, the append-only
    lifecycle trail. Idempotent by a UNIQUE(broker_order_id, event_type) index: a
    replayed broker event (same broker order-id + type) is a no-op. Best-effort —
    a ledger write NEVER raises into the order path (it is observability +
    durability, never a gate that could block a real exit).
  * ledger_exit_evidence() — CAP 5 cross-day attribution: a persisted exit/close
    event for a position, consulted by the reconciler BEFORE the single-day
    orderbook so a carried position closed on a prior day is closed cleanly
    instead of looping UNATTRIBUTED_CLOSE.

DATA-ISOLATION: reads/writes ONLY autotrade_order_events (created by
db_migrations). Never touches falcon_position_state or any legacy table.

PAPER-SAFE: a paper (dry_run) order has NO real broker order-id/tag — the ledger
still records the synthetic client_order_id (broker_order_id NULL). Paper order
BEHAVIOUR is byte-for-byte unchanged; only rows are appended to the new table.
"""
from __future__ import annotations

import hashlib
import itertools
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.order_ledger")
IST = timezone(timedelta(hours=5, minutes=30))

# Event vocabulary (the lifecycle transitions we record).
EV_ORDER_CREATED = "ORDER_CREATED"      # durable intent, BEFORE broker submission
EV_ORDER_SUBMITTED = "ORDER_SUBMITTED"  # broker accepted, order-id assigned
EV_FILLED = "FILLED"                    # entry filled (full)
EV_PARTIAL = "PARTIAL"                  # entry partial fill
EV_REJECTED = "REJECTED"                # entry/exit rejected by broker
EV_EXIT_PLACED = "EXIT_PLACED"          # exit order placed (order-id known)
EV_EXIT_FILLED = "EXIT_FILLED"          # exit fill confirmed COMPLETE
EV_EXIT_PARTIAL = "EXIT_PARTIAL"        # exit partial fill
EV_EXIT_FAILED = "EXIT_FAILED"          # exit failed / rejected / cancelled
EV_POSITION_CLOSED = "POSITION_CLOSED"  # position row marked CLOSED
EV_RECONCILE_CLOSE = "RECONCILE_CLOSE"  # reconciler closed the position

# Events that constitute POSITIVE evidence a position's exit happened / was
# working — consulted by CAP 5 cross-day attribution.
_EXIT_EVENT_TYPES = (
    EV_EXIT_FILLED, EV_POSITION_CLOSED, EV_RECONCILE_CLOSE, EV_EXIT_PLACED,
)
# The strongest evidence (an actual close/fill), preferred over EXIT_PLACED.
_EXIT_CONFIRMED_TYPES = (EV_EXIT_FILLED, EV_POSITION_CLOSED, EV_RECONCILE_CLOSE)


def _now_iso() -> str:
    return datetime.now(IST).isoformat()


# Process-local monotone counter — guarantees uniqueness even for two placements
# minted within the same millisecond (the epoch-ms alone is not fine-grained
# enough on coarse OS clocks, e.g. Windows).
_SEQ = itertools.count(1)


def make_client_order_id(session_id: str, symbol: str, attempt: int = 0) -> str:
    """Mint a unique, durable client order-id for one placement attempt.

    Shape: FAL-<sess8>-<sym>-<epochms>-<attempt>-<nonce>. The epoch-ms + attempt +
    a process-local nonce (a monotone counter + a short uuid fragment) guarantee
    uniqueness even for two placements in the same millisecond; the session/symbol
    prefix make it human-readable and self-describing. This is the FULL key stored
    in our DB; the broker only ever sees compact_tag() of it (Kite's 20-char tag
    limit)."""
    sess8 = (str(session_id or "").replace("-", "")[:8]) or "00000000"
    sym = re.sub(r"[^A-Za-z0-9]", "", str(symbol or "")).upper()[:10] or "SYM"
    epochms = int(time.time() * 1000)
    nonce = f"{next(_SEQ)}{uuid.uuid4().hex[:4]}"
    return f"FAL-{sess8}-{sym}-{epochms}-{int(attempt)}-{nonce}"


def compact_tag(client_order_id: str) -> str:
    """A deterministic <=20-char ALPHANUMERIC tag for a client_order_id.

    Kite's `tag` param is capped at 20 characters. We hash the full
    client_order_id (sha1) and keep a compact hex prefix with a leading "AT"
    marker so OUR orders are recognisable in the broker orderbook. 2 + 16 = 18
    chars, all alphanumeric (hex is 0-9a-f). Deterministic → the SAME
    client_order_id always yields the SAME tag, so a persisted client_order_id can
    be matched back to a broker order by recomputing the tag."""
    h = hashlib.sha1(str(client_order_id or "").encode("utf-8")).hexdigest()
    return ("AT" + h)[:18]


def find_broker_order_by_tag(orders: Optional[List[Dict[str, Any]]],
                             tag: Optional[str],
                             closing_side: Optional[str] = None
                             ) -> Optional[Dict[str, Any]]:
    """CLUSTER 3 ITEM 3(b) — pure orderbook match by OUR compact tag.

    Scan a broker orderbook (list of raw order dicts) for an order whose `tag`
    equals `tag` (the compact_tag of a client_order_id we minted). Optionally
    require the CLOSING side (long exit → SELL, short cover → BUY) so a stray
    same-tag entry can never be mistaken for the exit. Prefers a COMPLETE order,
    else the most recent still-working order. Returns the matched order dict or
    None. NEVER a foreign order (a foreign order carries a foreign/absent tag).

    Query-before-place uses this to ADOPT an exit we already placed (surviving a
    retry / restart) instead of placing a duplicate."""
    if not orders or not tag:
        return None
    want_side = str(closing_side).upper() if closing_side else None
    matches: List[Dict[str, Any]] = []
    for o in orders:
        if not isinstance(o, dict):
            continue
        if str(o.get("tag") or "") != str(tag):
            continue
        if want_side:
            txn = str(o.get("transaction_type") or "").upper()
            if txn and txn != want_side:
                continue
        matches.append(o)
    if not matches:
        return None
    for o in matches:
        if str(o.get("status") or "").upper() == "COMPLETE":
            return o
    return matches[-1]


def match_recent_order(orders: Optional[List[Dict[str, Any]]], *,
                       tag: Optional[str], symbol: Optional[str],
                       txn: Optional[str], qty: Optional[int]
                       ) -> Optional[Dict[str, Any]]:
    """QUERY-BEFORE-RETRY matcher (2026-07-13 ZENSARTECH in-session double-fill).

    Scan a broker orderbook (raw order dicts) for OUR just-placed ENTRY order so a
    retry after a confirmation TIMEOUT does NOT place a SECOND identical order.
    Matches ALL of: our compact `tag` (the strongest key — the two dup ZENSARTECH
    orders shared the SAME Kite tag), `tradingsymbol`, `transaction_type`, and the
    ordered `quantity`. The tag alone is per-order-unique, the rest are defence in
    depth so a stray same-tag row can never be adopted for the wrong instrument.

    A REJECTED / CANCELLED order is NEVER matched: it reached the broker but left
    NO position, so returning it as "already placed" would create a phantom — and
    returning None for it (definitely no live/filled order) correctly lets the
    caller retry (a genuinely rejected order SHOULD be re-placed). Prefers a
    COMPLETE order, else the most recent still-working order. Returns the matched
    order dict (order_id + status) or None (definitively absent). NEVER a foreign
    order — a foreign order carries a foreign/absent tag."""
    if not orders or not tag:
        return None
    want_sym = str(symbol or "").upper()
    want_txn = str(txn or "").upper()
    matches: List[Dict[str, Any]] = []
    for o in orders:
        if not isinstance(o, dict):
            continue
        if str(o.get("tag") or "") != str(tag):
            continue
        if want_sym and str(o.get("tradingsymbol") or "").upper() != want_sym:
            continue
        if want_txn and str(o.get("transaction_type") or "").upper() != want_txn:
            continue
        if qty is not None:
            try:
                if int(o.get("quantity") or 0) != int(qty):
                    continue
            except (TypeError, ValueError):
                continue
        st = str(o.get("status") or "").upper()
        if st in ("REJECTED", "CANCELLED"):
            # Reached the broker but produced no position → not an adoptable
            # success. Excluding it → None → the caller safely re-places.
            continue
        matches.append(o)
    if not matches:
        return None
    for o in matches:
        if str(o.get("status") or "").upper() == "COMPLETE":
            return o
    return matches[-1]


def append_event(*, session_id: Optional[str], symbol: Optional[str],
                 event_type: str,
                 position_ref: Optional[str] = None,
                 product: Optional[str] = None,
                 broker_profile: Optional[str] = None,
                 broker_order_id: Optional[str] = None,
                 client_order_id: Optional[str] = None,
                 qty: Optional[int] = None,
                 price: Optional[float] = None,
                 source: Optional[str] = None,
                 detail: Optional[str] = None) -> bool:
    """Append ONE lifecycle event to autotrade_order_events (INSERT OR IGNORE).

    Idempotent via the UNIQUE(broker_order_id, event_type) index: a replayed
    broker event (same broker order-id + type) is silently ignored (one row).
    Rows with a NULL broker_order_id (paper orders / pre-submission intent) do
    NOT conflict (SQLite treats NULLs as distinct) — they are keyed by
    client_order_id for correlation, and are naturally single per transition.

    Returns True on a successful append/no-op, False on error. NEVER raises — a
    ledger write must never break the order path."""
    try:
        with falcon_conn() as con:
            con.execute(
                """INSERT OR IGNORE INTO autotrade_order_events
                   (ts, session_id, position_ref, symbol, product, broker_profile,
                    broker_order_id, client_order_id, event_type, qty, price,
                    source, detail)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (_now_iso(), session_id,
                 str(position_ref) if position_ref is not None else None,
                 symbol, product, broker_profile,
                 str(broker_order_id) if broker_order_id is not None else None,
                 client_order_id, event_type,
                 int(qty) if qty is not None else None,
                 float(price) if price is not None else None,
                 source, detail))
            con.commit()
        return True
    except Exception as e:  # pragma: no cover - defensive; ledger never blocks
        log.warning("order_ledger.append_event(%s %s/%s) failed: %s",
                    event_type, session_id, symbol, e)
        return False


def record_intent(*, session_id: str, symbol: str, client_order_id: str,
                  qty: Optional[int], side: str,
                  product: Optional[str] = None,
                  broker_profile: Optional[str] = None,
                  instrument_type: Optional[str] = None,
                  source: str = "entry") -> bool:
    """CAP 3 — write the durable ORDER_CREATED intent row BEFORE the broker call.

    Persists the intended qty/side + client_order_id so a crash BETWEEN broker
    accept and the position-row insert leaves a durable orphan the recovery /
    reconcile path can find (the intent carries the client_order_id whose
    compact_tag rides on the broker order)."""
    detail = f"side={side}"
    if instrument_type:
        detail += f" itype={instrument_type}"
    return append_event(
        session_id=session_id, symbol=symbol, event_type=EV_ORDER_CREATED,
        position_ref=f"{session_id}:{symbol}", product=product,
        broker_profile=broker_profile, broker_order_id=None,
        client_order_id=client_order_id, qty=qty, price=None,
        source=source, detail=detail)


def get_events(session_id: str, symbol: Optional[str] = None
               ) -> List[Dict[str, Any]]:
    """The ordered event trail for a session (optionally one symbol), oldest
    first. Used by tests + audit to reconstruct a position's lifecycle."""
    try:
        with falcon_conn() as con:
            if symbol is not None:
                rows = con.execute(
                    """SELECT * FROM autotrade_order_events
                       WHERE session_id=? AND symbol=? ORDER BY id ASC""",
                    (session_id, symbol)).fetchall()
            else:
                rows = con.execute(
                    """SELECT * FROM autotrade_order_events
                       WHERE session_id=? ORDER BY id ASC""",
                    (session_id,)).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:  # pragma: no cover - defensive
        log.warning("order_ledger.get_events(%s) failed: %s", session_id, e)
        return []


def find_intent_by_client_order_id(client_order_id: str
                                   ) -> Optional[Dict[str, Any]]:
    """Return the ORDER_CREATED intent row for a client_order_id, or None.

    CAP 3 recovery helper: after a crash, a durable intent whose broker order was
    accepted but never recorded as a position can be located here (then matched to
    the broker orderbook by recomputing compact_tag)."""
    try:
        with falcon_conn() as con:
            r = con.execute(
                """SELECT * FROM autotrade_order_events
                   WHERE client_order_id=? AND event_type=?
                   ORDER BY id ASC LIMIT 1""",
                (client_order_id, EV_ORDER_CREATED)).fetchone()
        return dict(r) if r else None
    except Exception as e:  # pragma: no cover - defensive
        log.warning("order_ledger.find_intent(%s) failed: %s",
                    client_order_id, e)
        return None


# Terminal event types for an ENTRY client_order_id — its lifecycle is RESOLVED
# (a position was filled/closed, or the order was rejected) → the orphan-adoption
# scan skips it (idempotency + don't re-page a resolved intent).
_ENTRY_TERMINAL_TYPES = (
    EV_FILLED, EV_REJECTED, EV_POSITION_CLOSED, EV_RECONCILE_CLOSE,
)


def entry_intents(session_id: str) -> List[Dict[str, Any]]:
    """SPRINT CLUSTER 9b ITEM 6 — the ENTRY order intents for a session, one record
    per client_order_id, merging its ORDER_CREATED (durable pre-submission intent)
    with its ORDER_SUBMITTED (broker accepted — carries the broker order-id + the
    register symbol/qty/price/product). Oldest first.

    Only source='entry' events are considered. Each record:
      {client_order_id, symbol, broker_profile, product, qty, price,
       broker_order_id (None until submitted), created (bool), submitted (bool)}

    The recovery path uses this to find a broker-accepted entry with NO position
    row (a crash between broker-accept and the position insert). Never raises."""
    try:
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT * FROM autotrade_order_events
                   WHERE session_id=? AND COALESCE(source,'')='entry'
                     AND event_type IN (?,?)
                   ORDER BY id ASC""",
                (session_id, EV_ORDER_CREATED, EV_ORDER_SUBMITTED)).fetchall()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("order_ledger.entry_intents(%s) failed: %s", session_id, e)
        return []
    by_coid: Dict[str, Dict[str, Any]] = {}
    for raw in rows:
        r = dict(raw)
        coid = r.get("client_order_id")
        if not coid:
            continue
        rec = by_coid.setdefault(coid, {
            "client_order_id": coid, "session_id": session_id,
            "symbol": r.get("symbol"), "broker_profile": r.get("broker_profile"),
            "product": r.get("product"), "qty": r.get("qty"),
            "price": r.get("price"), "broker_order_id": None,
            "created": False, "submitted": False,
        })
        if r.get("event_type") == EV_ORDER_CREATED:
            rec["created"] = True
            if rec["qty"] is None:
                rec["qty"] = r.get("qty")
        elif r.get("event_type") == EV_ORDER_SUBMITTED:
            rec["submitted"] = True
            # ORDER_SUBMITTED carries the REGISTER symbol (the FUT contract for a
            # short) + the confirmed qty/price/product + the broker order-id.
            rec["symbol"] = r.get("symbol") or rec["symbol"]
            rec["broker_order_id"] = r.get("broker_order_id") or rec["broker_order_id"]
            if r.get("qty") is not None:
                rec["qty"] = r.get("qty")
            if r.get("price") is not None:
                rec["price"] = r.get("price")
            if r.get("product"):
                rec["product"] = r.get("product")
    # Only records that carry an ORDER_CREATED intent are entry intents.
    return [rec for rec in by_coid.values() if rec.get("created")]


def entry_intent_resolved(session_id: str, client_order_id: str) -> bool:
    """True when this ENTRY client_order_id already has a TERMINAL event (FILLED /
    REJECTED / POSITION_CLOSED / RECONCILE_CLOSE) — its lifecycle is resolved, so
    the orphan-adoption scan skips it (idempotent; never re-pages/re-registers)."""
    try:
        with falcon_conn() as con:
            ph = ",".join("?" for _ in _ENTRY_TERMINAL_TYPES)
            r = con.execute(
                f"""SELECT 1 FROM autotrade_order_events
                    WHERE session_id=? AND client_order_id=?
                      AND event_type IN ({ph}) LIMIT 1""",
                (session_id, client_order_id, *_ENTRY_TERMINAL_TYPES)).fetchone()
        return bool(r)
    except Exception as e:  # pragma: no cover - defensive
        log.warning("order_ledger.entry_intent_resolved(%s) failed: %s",
                    client_order_id, e)
        return False


def ledger_exit_evidence(session_id: str, symbol: str,
                         broker_profile: Optional[str] = None,
                         exit_order_id: Optional[str] = None,
                         client_order_id: Optional[str] = None,
                         after_ts: Optional[str] = None
                         ) -> Optional[Dict[str, Any]]:
    """CAP 5 — the strongest persisted EXIT/close evidence for a position, or None.

    Consulted by the reconciler BEFORE the single-day orderbook so a position
    whose exit filled / closed on a PRIOR day (its order absent from today's
    orderbook) is still attributable. Prefers a confirmed exit/close event
    (EXIT_FILLED / POSITION_CLOSED / RECONCILE_CLOSE, carrying a fill price) over a
    mere EXIT_PLACED. Scoped to (session_id, symbol[, broker_profile]); optionally
    narrowed to a known exit_order_id / client_order_id. Never raises.

    Returns {event_type, broker_order_id, exit_price, ts, has_confirmed_close}
    or None when the ledger holds no exit evidence for this position.

    after_ts: when set, only events at/after this ISO timestamp are considered —
    pass the position's opened_at so a PRIOR lifecycle's close of the same
    (session, symbol) can never be mis-attributed to a later re-entry."""
    try:
        with falcon_conn() as con:
            placeholders = ",".join("?" for _ in _EXIT_EVENT_TYPES)
            params: List[Any] = [session_id, symbol]
            sql = (f"""SELECT * FROM autotrade_order_events
                       WHERE session_id=? AND symbol=?
                         AND event_type IN ({placeholders})""")
            params.extend(_EXIT_EVENT_TYPES)
            if after_ts is not None:
                sql += " AND ts>=?"
                params.append(after_ts)
            if broker_profile is not None:
                sql += " AND COALESCE(broker_profile,'')=COALESCE(?,'')"
                params.append(broker_profile)
            if exit_order_id is not None:
                sql += " AND COALESCE(broker_order_id,'')=COALESCE(?,'')"
                params.append(str(exit_order_id))
            if client_order_id is not None:
                sql += " AND COALESCE(client_order_id,'')=COALESCE(?,'')"
                params.append(client_order_id)
            sql += " ORDER BY id ASC"
            rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    except Exception as e:  # pragma: no cover - defensive
        log.warning("order_ledger.ledger_exit_evidence(%s/%s) failed: %s",
                    session_id, symbol, e)
        return None
    if not rows:
        return None
    # Prefer a CONFIRMED close/fill with a positive price; else any confirmed;
    # else fall back to the EXIT_PLACED intent (working-order evidence).
    confirmed = [r for r in rows
                 if str(r.get("event_type")) in _EXIT_CONFIRMED_TYPES]
    chosen = None
    for r in confirmed:
        px = r.get("price")
        if px is not None and float(px) > 0:
            chosen = r
            break
    if chosen is None and confirmed:
        chosen = confirmed[-1]
    if chosen is None:
        chosen = rows[-1]  # EXIT_PLACED only (order was working)
    return {
        "event_type": chosen.get("event_type"),
        "broker_order_id": chosen.get("broker_order_id"),
        "exit_price": chosen.get("price"),
        "ts": chosen.get("ts"),
        "has_confirmed_close": bool(confirmed),
    }
