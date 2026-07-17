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
import re
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .. import alerts

log = logging.getLogger("kanida.autotrade.alert_monitor")
IST = timezone(timedelta(hours=5, minutes=30))


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


def page_signal_stale(session_id: str, age_seconds: Optional[float],
                      in_market_hours: bool, is_live: bool,
                      bound_sec: Optional[int] = None) -> Optional[int]:
    """(f) TESLA SIGNAL staleness: the once-per-minute signal refresher has not
    successfully updated within the bound for a RUNNING live session during
    market hours (a stalled/failing recompute → we are ABSTAINING from new seat
    entries and trading only exits until it recovers). Deduped, live-only."""
    if not is_live or not in_market_hours:
        return None
    return alerts.send_urgent_deduped(
        kind="TESLA_SIGNAL_STALE", session_id=session_id, symbol=None,
        detail=(f"TESLA_SIGNAL_STALE: session {session_id} signal cache is stale "
                f"(age={age_seconds if age_seconds is None else f'{age_seconds:.0f}s'}, "
                f">{bound_sec}s) — abstaining from NEW seat entries; exits still run."))


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
    # P0 PAPER-LEAK FIX (2026-07-16): the book below is the LIVE broker's net book,
    # so "is any session managing this position?" must be answered from LIVE rows
    # only. A PAPER session's identically-keyed row (profile 'default', account
    # NULL, product 'MIS') would otherwise mark a genuinely NAKED live position
    # "managed" and SUPPRESS the alert. Same bug class as registry.sibling_open_qty.
    _sess_mode = str(getattr(session, "mode", "live") or "live").strip().lower()
    if _sess_mode not in ("live", "paper"):
        _sess_mode = "live"
    _mode_cache: Dict[str, Optional[str]] = {}
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
                bare_sym, product, prof_scope, _prod_cache,
                mode_scope=_sess_mode, _mode_cache=_mode_cache)
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


# ═════════════════════════════════════════════════════════════════════════════
# ALERT AUTO-TRIAGE (default-OFF behind FALCON_AUTOTRADE_AUTO_TRIAGE)
# ═════════════════════════════════════════════════════════════════════════════
# THE DESIGN LESSON, encoded (from triaging all 31 unacked alerts of 2026-07-16):
#
#   The "URGENT" alerts were NOISE and the QUIET ones were the real ₹8.3L event.
#   ECLERX CORP_ACTION_SUSPECTED was flagged URGENT but was pure coincidence
#   (1465/498 = 2.9417 vs a 3.0 tolerance of 0.06 → it matched a "1:3 reverse
#   split" by 0.0017). Meanwhile the QUIET UNATTRIBUTED_CLOSE deficits were the
#   paper-contaminates-live bug that left 706 real MAPMYINDIA shares unsold.
#
# THEREFORE: auto-ack keys ONLY on PROVEN-TRANSIENT EVIDENCE — never on an
# alert's KIND and never on its SEVERITY. Every rule below names the specific
# artefact (a registry certification, a later healthy reconcile timestamp, a
# ledger event) that PROVES the condition the alert reported is gone. No
# evidence → no ack, forever, no matter how noisy the alert is.
#
# THE THREE TIERS
#   Tier 1 — auto_resolve(): evidence-gated ack. Writes acknowledged=1 + an
#            ack_reason audit string + auto_resolved=1. NEVER deletes a row.
#   Tier 2 — suppression AT THE DETECTOR (position_reconciler.py) so a provably
#            transient divergence never becomes an alert at all. The real fix.
#   Tier 3 — ALWAYS page, NEVER auto-ack (_NEVER_AUTO_ACK below) + the
#            RECONCILED_FLAT-without-an-exit-order detector.
#
# HARD RULE: nothing here may EVER auto-ack a condition that could indicate an
# untracked / naked position. That is enforced structurally three times over:
# (1) only _AUTO_ACKABLE_KINDS are ever selected as candidates, (2) every
# candidate is re-checked against _NEVER_AUTO_ACK before the ack, and (3) a
# session with ANY unacked Tier-3 alert is quarantined — none of its alerts are
# acked while a real-money incident is open on it.
# NON-MUTATING: this layer NEVER closes/opens a position and NEVER places an
# order. It only flips ack flags and raises pages.

# Tier 3 — the kinds a machine must NEVER resolve. Each one means real money is
# (or may be) exposed: a stranded leg, an incomplete kill, an unmanaged broker
# position, a manual conflict, or a close we cannot tie to one of our orders.
_NEVER_AUTO_ACK = frozenset({
    "EXIT_FAILED", "KILLING_INCOMPLETE", "NAKED_POSITION", "MANUAL_CONFLICT",
    "UNATTRIBUTED_CLOSE", "ORPHAN_AT_BROKER", "DOUBLE_FILL",
    "RECONCILED_FLAT_NO_EXIT_ORDER", "DAILY_LOSS_BREAKER",
})

# Tier 1 — the ONLY kinds any rule may ever ack. An alert kind absent from this
# set can never be auto-acked, whatever evidence exists.
_AUTO_ACKABLE_KINDS = ("UNCERTIFIED_BROKER_BLOCKED", "RECONCILE_STALE")


def _triage_lookback_sec() -> int:
    """How far back the triage scans for candidate alerts (s). Default 7 days —
    the historical UNCERTIFIED_BROKER_BLOCKED backlog is exactly this shape."""
    return _int_env("FALCON_AUTOTRADE_TRIAGE_LOOKBACK_SEC", 604800)


def _triage_min_age_sec() -> int:
    """Minimum age (s) before an alert may be auto-acked. A condition that fired
    SECONDS ago has not proven itself transient yet — settle first, ack later."""
    return _int_env("FALCON_AUTOTRADE_TRIAGE_MIN_AGE_SEC", 300)


def _flat_detector_lookback_sec() -> int:
    """How far back the Tier-3 RECONCILED_FLAT detector scans closed rows (s).
    Default 24h — the detector is for REAL-TIME catching of today's closes."""
    return _int_env("FALCON_AUTOTRADE_TRIAGE_FLAT_LOOKBACK_SEC", 86400)


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    """Parse an ISO IST timestamp from a row; None when absent/unparseable.
    Naive timestamps are assumed IST (that is what every writer here emits)."""
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(str(ts))
    except (ValueError, TypeError):
        return None
    return d.replace(tzinfo=IST) if d.tzinfo is None else d


def _live_session_ids() -> set:
    """session_ids whose autotrade_sessions.mode is 'live'. A PAPER session is
    invisible to every triage rule: paper legitimately has no broker order-ids
    (synthetic fills), so a paper row would otherwise trip the Tier-3 detector on
    every single close. Never raises → empty set (= no pages) on error."""
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT session_id FROM autotrade_sessions "
                "WHERE LOWER(COALESCE(mode,''))='live'").fetchall()
        return {str(dict(r)["session_id"]) for r in rows}
    except Exception as e:  # noqa: BLE001
        log.debug("triage: live-session scan failed: %s", e)
        return set()


def _sessions_with_open_incident() -> set:
    """QUARANTINE (safety guard 3): session_ids carrying ANY unacked Tier-3
    alert. While a real-money incident is open on a session we do not touch ANY
    of its alerts — a "transient" RECONCILE_STALE next to a live EXIT_FAILED is
    context a human needs, not noise to sweep away. Fail-SAFE: on any error
    return a sentinel that quarantines nothing is WRONG, so we cannot fail open
    here — an error propagates to the caller which skips the ack entirely."""
    from falcon.db import falcon_conn  # noqa: WPS433
    kinds = tuple(sorted(_NEVER_AUTO_ACK))
    with falcon_conn() as con:
        rows = con.execute(
            f"""SELECT DISTINCT session_id FROM autotrade_alerts
                WHERE acknowledged=0 AND session_id IS NOT NULL
                  AND kind IN ({','.join('?' * len(kinds))})""",
            kinds).fetchall()
    return {str(dict(r)["session_id"]) for r in rows}


# ── Tier 1 rule (a): UNCERTIFIED_BROKER_BLOCKED superseded by certification ───
_BLOCKED_BROKER_RE = re.compile(
    r"UNCERTIFIED_BROKER_BLOCKED:\s*(\S+)\s+is not certified", re.IGNORECASE)


def _parse_blocked_broker(detail: Optional[str]) -> Optional[str]:
    """The broker name out of an UNCERTIFIED_BROKER_BLOCKED detail string, or
    None. BROKER-AGNOSTIC: the name is READ from the alert and fed straight to
    the registry — there is no broker literal anywhere in this module."""
    m = _BLOCKED_BROKER_RE.search(str(detail or ""))
    return m.group(1).strip() if m else None


def _session_live_order_count(session_id: str) -> int:
    """How many REAL broker orders a session has on the durable ledger — i.e.
    ledger events carrying a non-NULL broker_order_id (a paper order has none).
    Raises on a DB error so the caller fails CLOSED (no ack)."""
    from falcon.db import falcon_conn  # noqa: WPS433
    with falcon_conn() as con:
        r = con.execute(
            "SELECT COUNT(*) AS n FROM autotrade_order_events "
            "WHERE session_id=? AND broker_order_id IS NOT NULL "
            "AND broker_order_id<>''", (session_id,)).fetchone()
    return int(dict(r)["n"]) if r else 0


def _resolve_uncertified_broker_blocked(alert: Dict[str, Any]) -> Optional[str]:
    """EVIDENCE GATE — ack an UNCERTIFIED_BROKER_BLOCKED only when:

      (1) the broker named IN THE ALERT is_certified() == True RIGHT NOW — the
          exact condition the alert reported is provably gone (the operator
          certified the adapter since); AND
      (2) that session placed 0 LIVE orders — no ledger event with a real broker
          order-id. The block REFUSED the order, so a session that nonetheless
          has live orders is entangled with real execution → a human looks.

    Returns the audit reason on a pass, else None (no ack).

    ⚠️ ASSUMPTION / KNOWN LIMITATION (flagged): broker/base.py raises this alert
    with session_id=None (it pages from the adapter, which has no session
    handle), so for TODAY's alerts gate (2) is VACUOUS and gate (1) carries the
    full weight. That is acceptable ONLY because an UNCERTIFIED_BROKER_BLOCKED
    means the order was REFUSED — it can never leave a naked position (the
    opposite: nothing was placed). A blocked EXIT would surface separately and
    independently as EXIT_FAILED, which is Tier 3 and never acked. If base.py
    later threads a session_id through, gate (2) starts biting with no change
    here. broker/base.py is outside this task's surface, so it was NOT changed."""
    broker = _parse_blocked_broker(alert.get("detail"))
    if not broker:
        return None                     # can't name the broker → can't prove it
    try:
        from ..broker.registry import is_certified
        certified = bool(is_certified(broker))
    except Exception as e:  # noqa: BLE001 — fail CLOSED (no ack)
        log.debug("triage: is_certified(%s) raised: %s", broker, e)
        return None
    if not certified:
        return None                     # STILL uncertified → the alert is LIVE
    sid = alert.get("session_id")
    n_live = 0
    if sid:
        try:
            n_live = _session_live_order_count(str(sid))
        except Exception as e:  # noqa: BLE001 — fail CLOSED (no ack)
            log.debug("triage: live-order count failed for %s: %s", sid, e)
            return None
        if n_live != 0:
            return None                 # real orders exist → leave it to a human
    return (f"superseded by certification: broker '{broker}' is_certified=True "
            f"now (the blocking condition no longer holds) and the alert's "
            f"session placed {n_live} live orders"
            f"{'' if sid else ' (no session_id on this alert — adapter-level page)'}"
            f" — historical, auto-resolved")


# ── Tier 1 rule (b): RECONCILE_STALE self-healed ─────────────────────────────
def _exit_activity_between(session_id: str, start: datetime,
                           end: datetime) -> int:
    """Count EXIT-lifecycle ledger events for a session in [start, end].

    This is the "did an exit happen during the blind window?" evidence. If ANY
    exit was placed/filled/failed/closed while we were reconcile-blind, the stale
    alert is NOT benign — we may have exited against an unvalidated basket — so it
    must NOT be acked. Raises on a DB error so the caller fails CLOSED."""
    from falcon.db import falcon_conn  # noqa: WPS433
    from ..order_ledger import (EV_EXIT_PLACED, EV_EXIT_FILLED, EV_EXIT_PARTIAL,
                                EV_EXIT_FAILED, EV_POSITION_CLOSED,
                                EV_RECONCILE_CLOSE)
    kinds = (EV_EXIT_PLACED, EV_EXIT_FILLED, EV_EXIT_PARTIAL, EV_EXIT_FAILED,
             EV_POSITION_CLOSED, EV_RECONCILE_CLOSE)
    with falcon_conn() as con:
        r = con.execute(
            f"""SELECT COUNT(*) AS n FROM autotrade_order_events
                WHERE session_id=? AND ts>=? AND ts<=?
                  AND event_type IN ({','.join('?' * len(kinds))})""",
            (session_id, start.isoformat(), end.isoformat()) + kinds).fetchone()
    return int(dict(r)["n"]) if r else 0


def _resolve_reconcile_stale(alert: Dict[str, Any]) -> Optional[str]:
    """EVIDENCE GATE — ack a RECONCILE_STALE only when:

      (1) a LATER HEALTHY reconcile exists for that session: basket_gen's
          last-successful-reconcile timestamp is strictly AFTER the alert's ts.
          That is positive proof the broker book became reachable again — the
          blind window CLOSED. (No healthy reconcile recorded → no ack. This is
          also why a backend restart, which clears basket_gen's in-memory map,
          fails CLOSED: an unknown history is never treated as healed.)
      (2) NO exit occurred during the blind window [alert_ts − stale_bound,
          healed_ts]. If we exited real money while blind, the alert is a REAL
          finding about an unvalidated exit, not a blip → a human reads it.

    Returns the audit reason on a pass, else None."""
    sid = alert.get("session_id")
    if not sid:
        return None
    a_ts = _parse_iso(alert.get("ts"))
    if a_ts is None:
        return None
    try:
        from . import basket_gen as _bg
        healed_iso = _bg.last_successful_reconcile_ts(str(sid))
    except Exception as e:  # noqa: BLE001 — fail CLOSED
        log.debug("triage: reconcile-ts lookup failed for %s: %s", sid, e)
        return None
    healed = _parse_iso(healed_iso)
    if healed is None or healed <= a_ts:
        return None                     # no LATER healthy reconcile → still blind
    # The blind window: from one stale-bound BEFORE the page (when the book went
    # dark) through the healthy reconcile that closed it.
    win_start = a_ts - timedelta(seconds=_reconcile_stale_bound_sec())
    try:
        n_exits = _exit_activity_between(str(sid), win_start, healed)
    except Exception as e:  # noqa: BLE001 — fail CLOSED
        log.debug("triage: exit-activity scan failed for %s: %s", sid, e)
        return None
    if n_exits > 0:
        return None                     # we EXITED while blind → never auto-ack
    healed_after = (healed - a_ts).total_seconds()
    return (f"self-healed after {int(healed_after)}s: a healthy broker reconcile "
            f"landed at {healed.isoformat()} (after the page at {a_ts.isoformat()}) "
            f"and 0 exit events occurred during the blind window "
            f"[{win_start.isoformat()} .. {healed.isoformat()}] — transient")


_TIER1_RULES = {
    "UNCERTIFIED_BROKER_BLOCKED": _resolve_uncertified_broker_blocked,
    "RECONCILE_STALE": _resolve_reconcile_stale,
}


def auto_resolve(now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """TIER 1 — evidence-gated auto-resolution of PROVEN-TRANSIENT alerts.

    For every UNACKED alert of an _AUTO_ACKABLE_KINDS kind, run its rule. A rule
    returns an AUDIT REASON only when it can PROVE the reported condition is gone;
    None otherwise. On a reason we write acknowledged=1 + ack_reason +
    auto_resolved=1 — the row is NEVER deleted, so the alert and the exact
    evidence that resolved it stay auditable forever.

    Returns [{alert_id, kind, session_id, symbol, reason}] for each acked alert.
    Default-OFF; never raises."""
    if not alerts.auto_triage_enabled():
        return []
    now = now or datetime.now(IST)
    try:
        candidates = alerts.unacked_alerts(
            kinds=list(_AUTO_ACKABLE_KINDS),
            lookback_sec=_triage_lookback_sec(), now=now)
    except Exception as e:  # noqa: BLE001
        log.debug("triage: candidate scan failed: %s", e)
        return []
    if not candidates:
        return []
    # SAFETY GUARD 3 — quarantine every session with an OPEN Tier-3 incident.
    # Any error here means we cannot PROVE a session is incident-free → we ack
    # nothing this pass (fail closed).
    try:
        quarantined = _sessions_with_open_incident()
    except Exception as e:  # noqa: BLE001
        log.warning("triage: incident quarantine scan failed — acking nothing "
                    "this pass: %s", e)
        return []
    min_age = _triage_min_age_sec()
    resolved: List[Dict[str, Any]] = []
    for a in candidates:
        kind = str(a.get("kind") or "")
        # SAFETY GUARD 2 — re-check the never-ack list on the candidate itself.
        if kind in _NEVER_AUTO_ACK or kind not in _TIER1_RULES:
            continue
        sid = a.get("session_id")
        if sid and str(sid) in quarantined:
            log.info("triage: alert %s (%s) NOT acked — session %s has an open "
                     "Tier-3 incident", a.get("id"), kind, sid)
            continue
        a_ts = _parse_iso(a.get("ts"))
        if a_ts is None or (now - a_ts).total_seconds() < min_age:
            continue                    # too fresh to have proven itself transient
        try:
            reason = _TIER1_RULES[kind](a)
        except Exception as e:  # noqa: BLE001 — a rule error is a NO-ACK
            log.warning("triage: rule %s raised for alert %s (no ack): %s",
                        kind, a.get("id"), e)
            continue
        if not reason:
            continue
        aid = a.get("id")
        if aid is None:
            continue
        if alerts.acknowledge(int(aid), reason=f"AUTO_TRIAGE: {reason}",
                              auto_resolved=True):
            log.info("triage: AUTO-RESOLVED alert %s (%s, session=%s): %s",
                     aid, kind, sid, reason)
            resolved.append({"alert_id": int(aid), "kind": kind,
                             "session_id": sid, "symbol": a.get("symbol"),
                             "reason": reason})
    return resolved


# ── Tier 3 — the ₹8.3L detector ──────────────────────────────────────────────
def detect_reconciled_flat_without_exit_order(
        now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """TIER 3, HIGHEST VALUE — page on a *_RECONCILED_FLAT close that carries NO
    exit_order_id, on a LIVE session.

    THIS IS THE 2026-07-15 ₹8.3L MAPMYINDIA SHAPE, EXACTLY. A paper session's
    843 OPEN qty leaked into the LIVE broker-net computation, so our_held
    computed to 0 → "broker net qty is 0 … marking CLOSED, placing NO order" →
    position id 246 booked close_reason='STOP_RECONCILED_FLAT' with
    exit_order_id=NULL. 706 real shares (~₹8.33L notional) were never sold, and
    NOTHING paged, because the row looked cleanly CLOSED.

    THE INVARIANT: on a LIVE session, a position marked CLOSED with a
    *_RECONCILED_FLAT reason claims "the broker is already flat, so we placed no
    exit order". If that claim is WRONG, the shares are still held and nobody
    owns them — an untracked position, with NO order-id to prove otherwise. There
    is no evidence that can make this benign, so it ALWAYS pages and is in
    _NEVER_AUTO_ACK. A human must confirm the broker is genuinely flat.

    LIVE ONLY: a paper close has no broker order-id by construction, so paper
    would fire on every close — paper rows are excluded via session mode.
    Deduped per (session, symbol). Read-only: mutates NOTHING, places NO order."""
    if not alerts.auto_triage_enabled():
        return []
    now = now or datetime.now(IST)
    cutoff = (now - timedelta(seconds=_flat_detector_lookback_sec())).isoformat()
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            rows = [dict(r) for r in con.execute(
                """SELECT id, session_id, symbol, qty, avg_price, product,
                          close_reason, closed_at, exit_order_id
                   FROM autotrade_positions
                   WHERE status='CLOSED'
                     AND close_reason LIKE '%RECONCILED_FLAT%'
                     AND COALESCE(exit_order_id,'')=''
                     AND COALESCE(closed_at,'')>=?
                   ORDER BY id ASC""", (cutoff,)).fetchall()]
    except Exception as e:  # noqa: BLE001 — a detector must never crash a tick
        log.debug("triage: RECONCILED_FLAT scan failed: %s", e)
        return []
    if not rows:
        return []
    live = _live_session_ids()
    out: List[Dict[str, Any]] = []
    for r in rows:
        sid = str(r.get("session_id") or "")
        if sid not in live:
            continue                    # paper / unknown session → never pages
        sym = r.get("symbol")
        qty = int(r.get("qty") or 0)
        px = float(r.get("avg_price") or 0.0)
        notional = qty * px
        detail = (
            f"RECONCILED_FLAT_NO_EXIT_ORDER: LIVE position {sym} "
            f"(session {sid}, id={r.get('id')}) was booked CLOSED with "
            f"close_reason='{r.get('close_reason')}' and NO exit_order_id — we "
            f"placed NO exit order and have NO order-id proving the broker is "
            f"flat. If the broker still holds it, {qty} shares "
            f"(~Rs{notional:,.0f} notional) are UNSOLD and UNTRACKED. VERIFY THE "
            f"BROKER BOOK NOW. (This is the 2026-07-15 MAPMYINDIA ~Rs8.3L shape.)")
        aid = alerts.send_urgent_deduped(
            kind="RECONCILED_FLAT_NO_EXIT_ORDER", session_id=sid, symbol=sym,
            detail=detail)
        out.append({"action": "RECONCILED_FLAT_NO_EXIT_ORDER", "symbol": sym,
                    "session_id": sid, "position_id": r.get("id"), "qty": qty,
                    "notional": notional, "close_reason": r.get("close_reason"),
                    "alert_id": aid})
    return out


# Per-process throttle — auto_triage() is called from the ~5s tick path via
# alerts.maybe_escalate(); the scans are pure DB reads but need not run per tick.
_TRIAGE_THROTTLE_SEC = 30.0
_TRIAGE_LAST_MONO = 0.0
_TRIAGE_LOCK = threading.Lock()


def auto_triage(now: Optional[datetime] = None,
                force: bool = False) -> Dict[str, Any]:
    """The single throttled entry point for the whole triage layer, called from
    alerts.maybe_escalate() (the one already-wired per-tick alert hook) BEFORE
    the escalation scan, so a provably-transient alert is resolved rather than
    re-paged.

    Order matters: Tier 3 detection runs FIRST, so an incident it raises is
    already visible to Tier 1's quarantine guard in the SAME pass — a session
    that just got a Tier-3 page cannot have any alert acked out from under it.

    Default-OFF; throttled; never raises. `force=True` bypasses the throttle
    (tests + a deliberate operator-triggered pass)."""
    if not alerts.auto_triage_enabled():
        return {"enabled": False, "paged": [], "auto_resolved": []}
    global _TRIAGE_LAST_MONO
    if not force:
        with _TRIAGE_LOCK:
            if _time.monotonic() - _TRIAGE_LAST_MONO < _TRIAGE_THROTTLE_SEC:
                return {"enabled": True, "throttled": True, "paged": [],
                        "auto_resolved": []}
            _TRIAGE_LAST_MONO = _time.monotonic()
    paged: List[Dict[str, Any]] = []
    resolved: List[Dict[str, Any]] = []
    try:
        paged = detect_reconciled_flat_without_exit_order(now=now)
    except Exception as e:  # noqa: BLE001
        log.warning("triage: RECONCILED_FLAT detector raised (ignored): %s", e)
    try:
        resolved = auto_resolve(now=now)
    except Exception as e:  # noqa: BLE001
        log.warning("triage: auto_resolve raised (ignored): %s", e)
    return {"enabled": True, "paged": paged, "auto_resolved": resolved}
