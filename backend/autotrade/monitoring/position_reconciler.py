"""ORDER-ID-DRIVEN, INVARIANT-BASED broker→DB position reconciler (v2).

RECONCILIATION FRAMEWORK — Phase 2 (order-driven engine) + Phase 3 (invariant
checker + alerts, cross-check-only re-enable). This REPLACES the old
aggregate-based reconciler whose qty-correction from the ACCOUNT NET book
corrupted per-session quantities on 2026-07-07 (three same-pick sessions had
every qty overwritten to the account total). See docs/ops/RECONCILIATION_FRAMEWORK.md.

THE FOUR PRINCIPLES (the contract this enforces):
  1. Order-id is the atomic unit of truth. A position is reconciled through the
     broker orders (gtt_id / exit_order_id) that closed IT — never by writing a
     session's quantity from the account aggregate.
  2. Positive evidence only. A position is CLOSED only on a CONFIRMED order event
     (a triggered-GTT whose order filled COMPLETE, or an exit_order that filled
     COMPLETE). Never infer a close from net-0 / absence / aggregate.
  3. A continuous invariant. Each cycle, per (symbol, product):
        Σ open-position qty (ALL sessions on the account) == broker net + holdings.
     A violation drives order-id resolution or an ALERT — never a blind aggregate
     correction.
  4. Fail safe. Unreachable / delayed / empty broker → strict no-op. Ambiguity is
     flagged (ALERT), not guessed.

WHAT THIS NEVER DOES (the removed bug paths):
  * NEVER writes a session's qty from the account aggregate (no set_qty here).
  * NEVER closes on net-0 / day-flat WITHOUT a confirmed order-id owned by that
    position. A close we can't attribute to one of OUR orders → UNATTRIBUTED_CLOSE
    alert, position left OPEN.
  * NEVER adopts / mutates a broker orphan (broker holds more than we track) →
    ORPHAN_AT_BROKER alert.

DELIVERY CONTRACT (unchanged from v1):
  * LIVE only. Paper (dry_run) / None book (unreachable / expired) / empty book →
    return [] immediately, mutate NOTHING.
  * Places NO order, ever.
  * Fetches the broker net book + holdings in ONE call each, per broker profile.

DATA-ISOLATION: reads autotrade_positions / autotrade_sessions ONLY. Never touches
falcon_position_state. The account-wide invariant reads autotrade_positions
directly (all sessions on the account), scoped to (symbol, product).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.position_reconciler")
IST = timezone(timedelta(hours=5, minutes=30))


def _reconcile_disabled() -> bool:
    """Kill-switch for the broker→DB reconciler.

    v2 (this module) is ORDER-ID-DRIVEN and multi-session-safe: it NEVER writes a
    session's qty from the account aggregate, and only CLOSES on positive order-id
    evidence. It is therefore safe to default ON. FALCON_AUTOTRADE_BROKER_RECONCILE
    still gates it: "off"/"0"/"false"/"no" disables (returns []); anything else (or
    unset) enables. The operator sets config/.env back to "on" after review."""
    return os.environ.get(
        "FALCON_AUTOTRADE_BROKER_RECONCILE", "on").strip().lower() in (
        "off", "0", "false", "no")


# F&O instrument types (a FUT/OPT row lives on NFO, cash on NSE).
_FNO_TYPES = {"FUT", "OPT", "CE", "PE"}


def _bare_symbol(symbol: str) -> str:
    """Strip an exchange suffix ("INFY:BSE" → "INFY")."""
    if symbol and ":" in symbol:
        return symbol.split(":", 1)[0]
    return symbol


def _is_fno(instrument_type: Optional[str]) -> bool:
    return str(instrument_type or "EQ").upper() in _FNO_TYPES


def _num(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _kite_product(order_product: Optional[str]) -> str:
    """Normalise a session's order_product to the broker net-book product string.

    Kite books net positions under a `product` of CNC / MIS / NRML / MTF. Our
    session order_product uses the same tokens; EQ falls back to CNC (delivery).
    Uppercased + defaulted so a None / unknown value never mis-buckets the
    invariant."""
    p = str(order_product or "CNC").upper()
    if p == "EQ":
        return "CNC"
    if p in ("CNC", "MIS", "NRML", "MTF"):
        return p
    return "CNC"


def _row_product(broker_row: Dict[str, Any]) -> Optional[str]:
    """The Kite product token on a raw net row, uppercased, or None when absent.

    Zerodha net rows carry `product` ∈ {CNC, MIS, NRML, MTF}. Used to bucket the
    broker_held sum PER (symbol, product) so a same-symbol CNC leg and MIS leg on
    the SAME account are never conflated (the 2026-07-07 corruption vector)."""
    p = broker_row.get("product")
    return str(p).upper() if p not in (None, "") else None


def _row_matches_symbol_product(bare_sym: str, is_fno: bool, want_product: str,
                                broker_row: Dict[str, Any]) -> bool:
    """True if `broker_row` is the broker's view of our (symbol, product).

    Match rules (a SUPERSET of the v1 exchange rule + a NEW product rule):
      * tradingsymbol == bare_sym.
      * F&O leg → require exchange NFO (when stated); cash leg → reject a stray
        NFO contract row of the same base name.
      * product: when the broker row STATES a product it must equal want_product
        (so a CNC leg and an MIS leg of the same symbol don't cross-contaminate).
        A row with NO stated product is accepted (older/degraded books) — the
        (symbol, exchange) match still scopes it.
    """
    row_sym = str(broker_row.get("tradingsymbol") or "")
    if row_sym != bare_sym:
        return False
    rexch = str(broker_row.get("exchange") or "").upper()
    if is_fno:
        if rexch and rexch != "NFO":
            return False
    else:
        if rexch == "NFO":
            return False
    rprod = _row_product(broker_row)
    if rprod is not None and rprod != want_product:
        return False
    return True


def _session_product(session_id: str, _cache: Dict[str, str]) -> str:
    """The Kite product for a session (from its config_json.order_product), cached
    per reconcile cycle. Falls back to CNC when the session row / config is
    absent or unparseable (safe delivery bucket)."""
    if session_id in _cache:
        return _cache[session_id]
    prod = "CNC"
    try:
        with falcon_conn() as con:
            r = con.execute(
                "SELECT config_json FROM autotrade_sessions WHERE session_id=?",
                (session_id,)).fetchone()
        if r and r["config_json"]:
            cfg = json.loads(r["config_json"])
            prod = _kite_product(cfg.get("order_product"))
    except Exception as e:  # pragma: no cover - defensive
        log.debug("reconcile: could not resolve product for session %s: %s",
                  session_id, e)
    _cache[session_id] = prod
    return prod


def _account_open_positions_for(bare_sym: str, want_product: str,
                                prof_scope: Optional[List[str]],
                                _prod_cache: Dict[str, str]) -> List[Dict[str, Any]]:
    """ALL OPEN/EXIT_FAILED positions on the ACCOUNT for (bare_sym, want_product),
    across EVERY session (the invariant's left-hand side).

    Scoped to the same broker profile(s) as the reconciling session so a
    multi-account platform never sums positions from a DIFFERENT broker account
    into one account's invariant. Product is resolved per session from its
    config_json (cached). Returns the raw position dicts."""
    with falcon_conn() as con:
        rows = con.execute(
            """SELECT * FROM autotrade_positions
               WHERE status IN ('OPEN','EXIT_FAILED') AND qty > 0""",
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        if _bare_symbol(str(d.get("symbol") or "")) != bare_sym:
            continue
        if prof_scope is not None:
            if str(d.get("broker_profile") or "") not in prof_scope:
                continue
        if _session_product(str(d.get("session_id")), _prod_cache) != want_product:
            continue
        out.append(d)
    return out


def _confirmed_close(pos: Dict[str, Any], broker) -> Optional[Dict[str, Any]]:
    """POSITIVE order-id evidence that THIS position closed, or None.

    Checks, in a deterministic order, the position's OWN order-ids:
      1. gtt_id → broker.get_gtt_fill: a TRIGGERED GTT whose order filled COMPLETE
         → close at that fill. close_reason "GTT".
      2. exit_order_id → broker.get_order_status: COMPLETE → close at that fill.
         close_reason "RECONCILED_EXIT".
    Returns {"exit_price", "exit_order_id", "close_reason", "filled_qty"} on a
    CONFIRMED close, else None (leave OPEN). Never raises."""
    # 1. GTT fill (positive evidence via the fired order's status).
    gtt_id = pos.get("gtt_id")
    if gtt_id:
        try:
            fill = broker.get_gtt_fill(gtt_id)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("reconcile: get_gtt_fill raised for %s: %s", gtt_id, e)
            fill = None
        if fill and str(fill.get("status") or "").upper() == "COMPLETE" \
                and int(fill.get("filled_quantity") or 0) > 0:
            price = _num(fill.get("average_price"))
            return {
                "exit_price": price if (price and price > 0) else pos.get("ltp"),
                "exit_order_id": str(fill.get("order_id") or "") or None,
                "close_reason": "GTT",
                "filled_qty": int(fill.get("filled_quantity") or 0),
            }
    # 2. Our own exit order confirmed COMPLETE.
    exit_oid = pos.get("exit_order_id")
    if exit_oid:
        try:
            order = broker.get_order_status(exit_oid)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("reconcile: get_order_status raised for %s: %s", exit_oid, e)
            order = None
        if order and str(order.get("status") or "").upper() == "COMPLETE" \
                and int(order.get("filled_quantity") or 0) > 0:
            price = _num(order.get("average_price"))
            return {
                "exit_price": price if (price and price > 0) else pos.get("ltp"),
                "exit_order_id": str(exit_oid),
                "close_reason": "RECONCILED_EXIT",
                "filled_qty": int(order.get("filled_quantity") or 0),
            }
    return None


def _persist_alert(session_id: Optional[str], symbol: str, product: str,
                   kind: str, detail: str) -> None:
    """Persist a lightweight recon alert row (best-effort; never raises) + WARN.

    Alerts NEVER mutate positions — they exist so ops can SEE an unexplained
    broker/DB divergence. Table created by db_migrations (autotrade_recon_alerts).
    """
    log.warning("RECON ALERT [%s] %s/%s (session=%s): %s",
                kind, symbol, product, session_id, detail)
    try:
        now = datetime.now(IST).isoformat()
        with falcon_conn() as con:
            con.execute(
                """INSERT INTO autotrade_recon_alerts
                   (ts, session_id, symbol, product, kind, detail)
                   VALUES (?,?,?,?,?,?)""",
                (now, session_id, symbol, product, kind, detail))
            con.commit()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("reconcile: could not persist alert (%s %s/%s): %s",
                    kind, symbol, product, e)


def reconcile_broker_positions(session) -> List[Dict[str, Any]]:
    """Order-id-driven, invariant-based reconcile of THIS session's OPEN /
    EXIT_FAILED positions against the broker's live book — MULTI-SESSION-SAFE.

    Returns a list of action dicts:
      * CLOSED via a confirmed order-id: {action: CLOSED_RECONCILED, symbol,
        product, exit_price, exit_order_id, close_reason}
      * ALERTS (never mutate): {action: UNATTRIBUTED_CLOSE | ORPHAN_AT_BROKER,
        symbol, product, deficit|extra}
    In an integer invariant every mismatch is either a DEFICIT (broker < db →
    order-id resolution, else UNATTRIBUTED_CLOSE) or a SURPLUS (broker > db →
    ORPHAN_AT_BROKER). A qty that differs for a non-order reason (e.g. a corporate
    action) surfaces as one of these alerts (a surplus/deficit we can't attribute),
    never an auto-correction.
    Places NO order. LIVE only — paper / None book / empty book → [].
    """
    if _reconcile_disabled():
        return []
    if getattr(session, "dry_run", True):
        return []
    if not getattr(session, "brokers", None):
        try:
            session._build_brokers()
        except Exception as e:
            log.warning("reconcile: _build_brokers failed for %s: %s",
                        session.session_id, e)
            return []

    registry = session.registry
    monitor = session.monitor
    brokers: Dict[str, Any] = session.brokers or {}

    # 1. Fetch net book + holdings ONCE per profile. None ⇒ unreachable / expired
    #    (E5) ⇒ do NOTHING for that profile. get_positions_net() returning None IS
    #    the token-expired / unreachable signal (the broker adapter no-ops on a bad
    #    token / API error). We never mutate on None.
    books: Dict[str, Optional[List[dict]]] = {}
    holdings_book: Dict[str, Optional[List[dict]]] = {}
    for prof_id, broker in brokers.items():
        try:
            books[prof_id] = broker.get_positions_net()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("reconcile: get_positions_net raised for %s/%s: %s",
                        session.session_id, prof_id, e)
            books[prof_id] = None
        try:
            holdings_book[prof_id] = broker.get_holdings()
        except Exception as e:  # pragma: no cover - defensive
            log.debug("reconcile: get_holdings raised for %s/%s: %s",
                      session.session_id, prof_id, e)
            holdings_book[prof_id] = None

    if all(book is None for book in books.values()):
        log.debug("reconcile %s: all broker books None (unreachable/expired) — no-op",
                  session.session_id)
        return []

    # THIS session's OPEN / EXIT_FAILED positions (idempotent — terminal rows
    # untouched). We resolve closes only for THIS session's positions; the
    # account-wide sum is the invariant cross-check.
    my_positions = [p for p in registry.get_all_positions()
                    if str(p.get("status")) in ("OPEN", "EXIT_FAILED")
                    and int(p.get("qty") or 0) > 0]
    if not my_positions:
        return []

    prof_scope = list(brokers.keys()) or None
    _prod_cache: Dict[str, str] = {}
    actions: List[Dict[str, Any]] = []
    changed = False

    # Distinct (bare_symbol, product) groups THIS session has OPEN, in a
    # deterministic order.
    my_groups: List[Tuple[str, str]] = []
    seen = set()
    for p in my_positions:
        bare = _bare_symbol(str(p.get("symbol") or ""))
        prod = _session_product(str(p.get("session_id")), _prod_cache)
        key = (bare, prod)
        if key not in seen:
            seen.add(key)
            my_groups.append(key)
    my_groups.sort()

    for bare_sym, product in my_groups:
        is_fno = any(
            _is_fno(p.get("instrument_type")) for p in my_positions
            if _bare_symbol(str(p.get("symbol") or "")) == bare_sym)

        # ── broker_held for (symbol, product): Σ net qty over matching rows, plus
        #    (delivery CNC only) holdings when the net book shows net0/absent
        #    WITHOUT a sell. abs() so a short's negative net counts as held. ─────
        broker_net = 0
        matched_rows: List[dict] = []
        # Prefer THIS session's profile books; fall back to the single non-None
        # book for a single-broker session.
        candidate_books: List[Optional[List[dict]]] = []
        pos_profs = {str(p.get("broker_profile") or "") for p in my_positions
                     if _bare_symbol(str(p.get("symbol") or "")) == bare_sym}
        for pid in pos_profs:
            candidate_books.append(books.get(pid))
        non_none_books = [b for b in books.values() if b is not None]
        if all(b is None for b in candidate_books):
            if len(non_none_books) == 1:
                candidate_books = [non_none_books[0]]
            else:
                log.debug("reconcile %s/%s(%s): no broker book for profile — skip",
                          session.session_id, bare_sym, product)
                continue

        for book in candidate_books:
            if not isinstance(book, list):
                continue
            for r in book:
                if _row_matches_symbol_product(bare_sym, is_fno, product, r):
                    nq = _num(r.get("quantity"))
                    if nq is not None:
                        broker_net += int(nq)  # SIGNED day-net (a sell is negative)
                    matched_rows.append(r)

        # ── broker_held for (symbol, product) ─────────────────────────────────
        # CNC (delivery): the SIGNED day-net PLUS settled holdings (demat + t1).
        # The two are DISJOINT — today's CNC buys sit in net.quantity; an overnight
        # lot has already moved to holdings (verified LIVE 2026-07-06: AEGISLOG
        # net 57 today + t1 35 overnight = 92 held, with net.overnight_quantity 0).
        # A CNC SELL shows a NEGATIVE net that OFFSETS holdings, so a fully-exited
        # delivery nets to 0 — NOT abs() (ACUTAAS live: net -12 + holdings 0 → 0,
        # not a phantom 12). max(0, …) floors a fully-sold position at zero.
        # MIS / NRML / MTF: no holdings; |signed net| IS the exposure (a short MTF
        # nets negative — abs is the held size; live AARTIIND MTF net -630 → 630).
        held = 0
        if product == "CNC":
            for pid in (pos_profs or set(brokers.keys())):
                hl = holdings_book.get(pid)
                for h in (hl or []):
                    if _bare_symbol(str(h.get("tradingsymbol") or "")) == bare_sym:
                        held += int((_num(h.get("quantity")) or 0)
                                    + (_num(h.get("t1_quantity")) or 0))
            broker_held = max(0, broker_net + held)
        else:
            broker_held = abs(broker_net)

        # ── db_held_ALL: Σ OPEN qty across ALL sessions on the account for
        #    (symbol, product). The invariant's left side. ─────────────────────
        account_positions = _account_open_positions_for(
            bare_sym, product, prof_scope, _prod_cache)
        db_held_all = sum(int(p.get("qty") or 0) for p in account_positions)

        # ── INVARIANT ─────────────────────────────────────────────────────────
        if broker_held == db_held_all:
            # In sync. THE CORRUPTION FIX: with multiple sessions summing to the
            # broker aggregate, this branch fires and NOTHING is mutated.
            continue

        if broker_held < db_held_all:
            # We show MORE open than the broker holds → something CLOSED. Resolve
            # PER POSITION on POSITIVE order-id evidence only, in a deterministic
            # order, until the invariant is satisfied. Only THIS session's
            # positions are closeable here (we own their order-ids). A sibling
            # session's position is closed by ITS OWN reconcile pass.
            deficit = db_held_all - broker_held
            my_group_positions = sorted(
                (p for p in my_positions
                 if _bare_symbol(str(p.get("symbol") or "")) == bare_sym),
                key=lambda p: (int(p.get("id") or 0)))
            for pos in my_group_positions:
                if deficit <= 0:
                    break
                broker_for_pos = brokers.get(str(pos.get("broker_profile") or ""))
                if broker_for_pos is None and len(brokers) == 1:
                    broker_for_pos = next(iter(brokers.values()))
                if broker_for_pos is None:
                    continue
                ev = _confirmed_close(pos, broker_for_pos)
                if ev is None:
                    continue
                sym = pos.get("symbol")
                prof_id = pos.get("broker_profile")
                try:
                    registry.mark_closed(
                        sym, ev["close_reason"], exit_price=ev["exit_price"],
                        broker_profile=prof_id,
                        exit_order_id=ev["exit_order_id"])
                except Exception as e:  # pragma: no cover - defensive
                    log.warning("reconcile: mark_closed failed %s/%s: %s",
                                session.session_id, sym, e)
                    continue
                changed = True
                deficit -= int(pos.get("qty") or 0)
                log.warning(
                    "reconcile %s/%s: CLOSED via %s @ %s (order %s) — invariant "
                    "resolution", session.session_id, sym, ev["close_reason"],
                    ev["exit_price"], ev["exit_order_id"])
                actions.append({
                    "action": "CLOSED_RECONCILED", "symbol": sym,
                    "product": product, "exit_price": ev["exit_price"],
                    "exit_order_id": ev["exit_order_id"],
                    "close_reason": ev["close_reason"]})
            if deficit > 0:
                # A close we CANNOT attribute to any of our order-ids (e.g. an RMS
                # auto-square with no order we tracked). ALERT — never blind-close.
                detail = (f"broker_held={broker_held} < db_held_all={db_held_all}; "
                          f"unresolved deficit={deficit} after order-id evidence")
                _persist_alert(session.session_id, bare_sym, product,
                               "UNATTRIBUTED_CLOSE", detail)
                actions.append({
                    "action": "UNATTRIBUTED_CLOSE", "symbol": bare_sym,
                    "product": product, "deficit": int(deficit)})
            continue

        # broker_held > db_held_all → broker holds MORE than we track → orphan /
        # untracked position at the broker. NEVER adopt or mutate — ALERT.
        extra = broker_held - db_held_all
        detail = (f"broker_held={broker_held} > db_held_all={db_held_all}; "
                  f"extra={extra} untracked at broker")
        _persist_alert(session.session_id, bare_sym, product,
                       "ORPHAN_AT_BROKER", detail)
        actions.append({
            "action": "ORPHAN_AT_BROKER", "symbol": bare_sym,
            "product": product, "extra": int(extra)})

    # After any close, re-freeze the invested basis over the REMAINING OPEN
    # positions so the kill / trail denominator matches reality.
    if changed:
        try:
            monitor.refreeze_invested_basis()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("reconcile: refreeze_invested_basis failed for %s: %s",
                        session.session_id, e)

    return actions
