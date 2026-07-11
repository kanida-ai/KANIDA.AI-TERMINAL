"""Money-losing-event → human-page wiring (SPRINT CLUSTER 6, ITEMS 2 + 3).

The reconciler + monitor already DETECT the real-money failure conditions; this
module ROUTES each through alerts.send_urgent (deduped) so a human is paged, and
adds the highest-severity institutional detector: a genuinely NAKED / unmanaged
real broker position.

Every page here is:
  * LIVE-ONLY — a paper/dry_run session never pages (byte-identical paper: no push,
    no alert row). The tick wiring passes is_live = not session.dry_run.
  * DEDUPED — send_urgent_deduped collapses a condition that is true every ~5s
    tick to ONE page per (kind, session, symbol) per window.
  * NON-MUTATING — nothing here closes/opens/mutates a position or places an order.

DATA-ISOLATION: reads autotrade_positions / autotrade_sessions + the broker book
ONLY. Never touches falcon_position_state.
"""
from __future__ import annotations

import logging
import os
import threading
import time as _time
from typing import Any, Dict, List, Optional

from .. import alerts

log = logging.getLogger("kanida.autotrade.alert_monitor")


# ── the money-losing DIVERGENCE kinds the reconciler emits (ITEM 2b) ──────────
_DIVERGENCE_ACTIONS = frozenset({
    "UNATTRIBUTED_CLOSE", "ORPHAN_AT_BROKER", "CORP_ACTION_SUSPECTED"})


def _is_live(session) -> bool:
    return not bool(getattr(session, "dry_run", True))


def _int_env(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default)).strip()))
    except (ValueError, AttributeError):
        return default


def _reconcile_stale_bound_sec() -> int:
    """Reconcile-age (s) beyond which a RUNNING live session in market hours pages.
    Reconcile runs ~5s; 120s ≈ 24 missed cycles = a real outage, not a blip."""
    return _int_env("FALCON_AUTOTRADE_RECONCILE_STALE_SEC", 120)


def _mark_stale_bound_ms() -> int:
    """Oldest-mark-age (ms) beyond which a live session pages (stalled ticker)."""
    return _int_env("FALCON_AUTOTRADE_MARK_STALE_MS", 60000)


# ── ITEM 2 — the five money-losing wires ──────────────────────────────────────
def page_exit_failed(session_id: str, exit_failed_positions: List[Dict[str, Any]],
                     killing_incomplete: bool, is_live: bool) -> List[int]:
    """(a) A live position with a FAILED exit (still held) / a KILLING_INCOMPLETE
    session. Page per stranded leg + once for the incomplete-kill state."""
    if not is_live:
        return []
    fired: List[int] = []
    for p in exit_failed_positions or []:
        sym = p.get("symbol")
        aid = alerts.send_urgent_deduped(
            kind="EXIT_FAILED", session_id=session_id, symbol=sym,
            detail=(f"EXIT_FAILED: live position {sym} ({session_id}) has a failed "
                    f"exit and is still held — manual exit required."))
        if aid is not None:
            fired.append(aid)
    if killing_incomplete:
        aid = alerts.send_urgent_deduped(
            kind="KILLING_INCOMPLETE", session_id=session_id, symbol=None,
            detail=(f"KILLING_INCOMPLETE: session {session_id} could not flatten "
                    f"every leg — stranded position(s) still held."))
        if aid is not None:
            fired.append(aid)
    return fired


def page_recon_divergences(session_id: str,
                           broker_reconciled: Optional[List[Dict[str, Any]]],
                           is_live: bool) -> List[int]:
    """(b) A NEW autotrade_recon_alerts divergence (UNATTRIBUTED_CLOSE /
    ORPHAN_AT_BROKER / CORP_ACTION_SUSPECTED) = a naked / unattributed position."""
    if not is_live:
        return []
    fired: List[int] = []
    for action in broker_reconciled or []:
        kind = str(action.get("action") or "")
        if kind not in _DIVERGENCE_ACTIONS:
            continue
        sym = action.get("symbol")
        aid = alerts.send_urgent_deduped(
            kind=kind, session_id=session_id, symbol=sym,
            detail=(f"{kind}: broker/DB divergence on {sym} "
                    f"(product={action.get('product')}) — {action}"))
        if aid is not None:
            fired.append(aid)
    return fired


def page_reconcile_stale(session_id: str, age_seconds: Optional[float],
                         is_running: bool, in_market_hours: bool,
                         is_live: bool,
                         bound_sec: Optional[int] = None) -> Optional[int]:
    """(c) Reconcile-staleness: the last HEALTHY broker reconcile is older than the
    bound for a RUNNING live session during market hours (broker book unreachable
    → we are trading blind on an unvalidated basket)."""
    if not is_live or not is_running or not in_market_hours:
        return None
    if age_seconds is None:
        return None
    bound = _reconcile_stale_bound_sec() if bound_sec is None else bound_sec
    if bound <= 0 or age_seconds <= bound:
        return None
    return alerts.send_urgent_deduped(
        kind="RECONCILE_STALE", session_id=session_id, symbol=None,
        detail=(f"RECONCILE_STALE: last healthy broker reconcile for {session_id} "
                f"was {age_seconds:.0f}s ago (>{bound}s) — trading on an "
                f"unvalidated basket."))


def page_mark_stale(session_id: str, oldest_mark_age_ms: Optional[int],
                    mark_stale_flag: bool, is_live: bool,
                    bound_ms: Optional[int] = None) -> Optional[int]:
    """(d) Mark-staleness: the C5 mark_stale_abstain flag fired, OR the oldest
    open-position mark is older than the bound (a stalled ticker)."""
    if not is_live:
        return None
    bound = _mark_stale_bound_ms() if bound_ms is None else bound_ms
    stale = bool(mark_stale_flag) or (
        oldest_mark_age_ms is not None and bound > 0
        and int(oldest_mark_age_ms) > bound)
    if not stale:
        return None
    return alerts.send_urgent_deduped(
        kind="MARK_STALE", session_id=session_id, symbol=None,
        detail=(f"MARK_STALE: session {session_id} marks are stale "
                f"(oldest_mark_age_ms={oldest_mark_age_ms}, "
                f"abstain_flag={bool(mark_stale_flag)}) — exit decisions on a "
                f"stale price are unsafe."))


def page_breaker(session_id: str, breaker_result: Optional[Dict[str, Any]],
                 is_live: bool) -> Optional[int]:
    """(e) The RMS portfolio daily-loss breaker (C4) fired → the user's whole book
    was flattened. Page it (deduped per session)."""
    if not is_live or not breaker_result:
        return None
    return alerts.send_urgent_deduped(
        kind="DAILY_LOSS_BREAKER", session_id=session_id, symbol=None,
        detail=(f"PORTFOLIO_DAILY_LOSS_BREAKER fired ({session_id}): "
                f"{breaker_result.get('reason') or breaker_result}"))


# ── ITEM 3 — NAKED-POSITION detector ──────────────────────────────────────────
def _owned_evidence(prof_scope: List[str]) -> Dict[str, Any]:
    """Gather POSITIVE ownership evidence from autotrade_positions for the given
    broker profiles (this account): OUR compact order tags, OUR recorded broker
    order-ids, and the (bare_symbol, product) pairs we hold a CLOSED row for.
    Best-effort — returns empty structures on any error."""
    from ..order_ledger import compact_tag
    from .position_reconciler import _bare_symbol, _kite_product
    tags: set = set()
    order_ids: set = set()
    closed_pairs: set = set()
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT symbol, product, status, client_order_id, entry_order_id, "
                "exit_order_id, broker_profile FROM autotrade_positions").fetchall()
        for r in rows:
            d = dict(r)
            if prof_scope is not None and str(d.get("broker_profile") or "") \
                    not in prof_scope:
                continue
            coid = d.get("client_order_id")
            if coid not in (None, ""):
                tags.add(compact_tag(str(coid)))
            for oid in (d.get("entry_order_id"), d.get("exit_order_id")):
                if oid not in (None, ""):
                    order_ids.add(str(oid))
            if str(d.get("status") or "").upper() == "CLOSED":
                closed_pairs.add(
                    (_bare_symbol(str(d.get("symbol") or "")),
                     _kite_product(d.get("product"))))
    except Exception as e:  # noqa: BLE001 — evidence gathering never crashes
        log.debug("naked: owned-evidence gather failed: %s", e)
    return {"tags": tags, "order_ids": order_ids, "closed_pairs": closed_pairs}


def _order_owned_for_symbol(orderbook: Optional[List[dict]], bare_sym: str,
                            evidence: Dict[str, Any]) -> bool:
    """True when the broker orderbook has an order for `bare_sym` carrying OUR tag
    or OUR recorded order-id — positive evidence WE placed the position."""
    from .position_reconciler import _bare_symbol
    for o in orderbook or []:
        if not isinstance(o, dict):
            continue
        if _bare_symbol(str(o.get("tradingsymbol") or "")) != bare_sym:
            continue
        tag = o.get("tag")
        oid = o.get("order_id")
        if tag is not None and str(tag) in evidence["tags"]:
            return True
        if oid is not None and str(oid) in evidence["order_ids"]:
            return True
    return False


# Per-session throttle so the tick path re-scans at most once per window (the
# reconciler already fetches every tick; the naked scan is the extra broker read).
_NAKED_THROTTLE_SEC = 60.0
_NAKED_LAST: Dict[str, float] = {}
_NAKED_LOCK = threading.Lock()


def maybe_detect_naked(session) -> List[Dict[str, Any]]:
    """Throttled wrapper for the tick path (≤ once per _NAKED_THROTTLE_SEC per
    session). Never raises."""
    sid = getattr(session, "session_id", None)
    if sid is None:
        return []
    with _NAKED_LOCK:
        last = _NAKED_LAST.get(sid, 0.0)
        if _time.monotonic() - last < _NAKED_THROTTLE_SEC:
            return []
        _NAKED_LAST[sid] = _time.monotonic()
    try:
        return detect_naked_positions(session)
    except Exception as e:  # noqa: BLE001 — never crash a tick
        log.warning("naked: detect raised for %s (ignored): %s", sid, e)
        return []


def detect_naked_positions(session) -> List[Dict[str, Any]]:
    """ITEM 3 — page URGENT on a genuinely NAKED / unmanaged real broker position.

    A broker net position (|qty|>0) for (symbol, product) that NO open autotrade
    session manages AND for which we hold POSITIVE ownership evidence WE placed it
    (an order in the broker book carrying OUR compact tag / recorded order-id, OR a
    CLOSED autotrade_positions row we own for that symbol+product). This is
    distinct from the per-symbol invariant: it is a REAL broker position that no
    live session is managing. Conservative — a broker position with NO ownership
    evidence is a manual/foreign holding → INVISIBLE (no page), exactly as the P7
    reconciler. Pages ONCE per (symbol, product). Mutates NOTHING, places NO order.
    LIVE only — paper / unreachable book → []."""
    if not _is_live(session):
        return []
    brokers = getattr(session, "brokers", None) or {}
    if not brokers:
        try:
            session._build_brokers()
            brokers = session.brokers or {}
        except Exception as e:  # noqa: BLE001
            log.debug("naked: _build_brokers failed for %s: %s",
                      session.session_id, e)
            return []

    from .position_reconciler import (_bare_symbol, _row_product, _num,
                                      _account_open_positions_for)
    prof_scope = list(brokers.keys()) or None
    evidence = _owned_evidence(prof_scope or [])
    _prod_cache: Dict[str, str] = {}
    naked: List[Dict[str, Any]] = []
    seen: set = set()

    for prof_id, broker in brokers.items():
        try:
            book = broker.get_positions_net()
        except Exception as e:  # noqa: BLE001
            log.debug("naked: get_positions_net raised %s/%s: %s",
                      session.session_id, prof_id, e)
            book = None
        if not isinstance(book, list):
            continue          # None/unreachable/paper → nothing to judge (fail-safe)
        try:
            orderbook = broker.get_orders()
        except Exception as e:  # noqa: BLE001
            log.debug("naked: get_orders raised %s/%s: %s",
                      session.session_id, prof_id, e)
            orderbook = None
        if not isinstance(orderbook, list):
            orderbook = []

        for row in book:
            if not isinstance(row, dict):
                continue
            qty = _num(row.get("quantity"))
            if qty is None or int(qty) == 0:
                continue                      # flat net row → not a broker position
            bare_sym = _bare_symbol(str(row.get("tradingsymbol") or ""))
            if not bare_sym:
                continue
            # Product: prefer the row's stated product, default CNC.
            from .position_reconciler import _kite_product
            product = _kite_product(_row_product(row) or "CNC")
            key = (bare_sym, product)
            if key in seen:
                continue

            # Is any OPEN autotrade session managing this (symbol, product)?
            account_open = _account_open_positions_for(
                bare_sym, product, prof_scope, _prod_cache)
            db_open = sum(int(p.get("qty") or 0) for p in account_open)
            if db_open > 0:
                continue                      # managed by a live session → not naked

            # Positive ownership evidence WE placed / owned this position?
            owned = (key in evidence["closed_pairs"]) or _order_owned_for_symbol(
                orderbook, bare_sym, evidence)
            if not owned:
                continue                      # manual / foreign holding → INVISIBLE

            seen.add(key)
            detail = (f"NAKED_POSITION: broker holds {int(qty)} {bare_sym} "
                      f"({product}) that NO live autotrade session is managing, "
                      f"but we have ownership evidence WE placed it — this is a "
                      f"real, unmanaged position (session {session.session_id}).")
            aid = alerts.send_urgent_deduped(
                kind="NAKED_POSITION", session_id=session.session_id,
                symbol=bare_sym, detail=detail)
            naked.append({"action": "NAKED_POSITION", "symbol": bare_sym,
                          "product": product, "broker_qty": int(qty),
                          "alert_id": aid})
    return naked
