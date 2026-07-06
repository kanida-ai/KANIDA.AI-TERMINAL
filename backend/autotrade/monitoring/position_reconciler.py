"""AUTHORITATIVE broker→DB position reconciler.

THE GAP THIS CLOSES (real-money truth): the tick loop only reconciles positions
that HAVE a gtt_id (gtt_manager.reconcile_gtt_fills) and only checks the broker
net qty REACTIVELY inside the pre-exit guard. So a position closed AT THE BROKER
outside our exit path and without a GTT — a Zerodha RMS auto-square-off, a manual
broker exit, a missed GTT fill — stays stale (OPEN / EXIT_FAILED with phantom
P&L). This module makes the BROKER the single source of truth for position
existence / qty / exit-price, validated once per tick BEFORE mark-to-market.

DELIVERY CONTRACT:
  * LIVE only. Paper (dry_run) → returns [] immediately, mutates NOTHING.
  * Places NO order, ever. It only reconciles the DB to the broker's net book.
  * Fetches the whole net book in ONE broker call (broker.get_positions_net()).

SAFETY GUARANTEES (the whole point):
  * None book (broker unreachable / paper / API error) → RETURN [] immediately.
    An unreachable or delayed broker must NEVER mutate the DB. This is the most
    important guarantee — it makes a broker blip a no-op, not a mass-flatten.
  * EMPTY book ([]) while we hold OPEN positions → RETURN [] with a warning.
    An empty net list is almost always a transient API blip; a GENUINE close
    still surfaces a day-flat row (quantity 0, buy==sell). We do NOT mass-close.
  * ROW FOUND, net qty == 0 (bought AND sold, day-flat) → CLOSED_EXTERNAL at the
    REAL broker sell/buy average.
  * ROW FOUND, net qty > 0 but != DB qty → QTY_CORRECTED to the broker (broker
    is truth); optionally correct avg_price when it diverges > 0.5%.
  * ROW FOUND, net qty == DB qty → in sync, no action.
  * ROW NOT FOUND in a NON-EMPTY book → ABSENT_REVIEW (log + flag), do NOT close.
    A false close is worse than a flagged stale-open (the book could be partial).
  * Idempotent: only OPEN / EXIT_FAILED rows are examined; a terminal (CLOSED)
    row is never re-touched.
  * After any close / qty correction, the frozen invested_basis is
    RE-FROZEN (monitor.refreeze_invested_basis) over the remaining OPEN positions
    so the kill / trail denominator matches reality.

DATA-ISOLATION: reads/writes ONLY autotrade_positions / autotrade_sessions via
the session's registry / monitor. Never touches falcon_position_state.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger("kanida.autotrade.position_reconciler")

# F&O instrument types (match the same segment logic as
# zerodha.get_net_position_qty — a FUT/OPT row lives on NFO, cash on NSE).
_FNO_TYPES = {"FUT", "OPT", "CE", "PE"}
# avg_price divergence above which we treat the broker avg as truth and correct.
_AVG_CORRECT_THRESHOLD = 0.005  # 0.5%


def _bare_symbol(symbol: str) -> str:
    """Strip an exchange suffix ("INFY:BSE" → "INFY"). Mirrors
    zerodha._resolve_symbol's split so DB symbols with a suffix still match."""
    if symbol and ":" in symbol:
        return symbol.split(":", 1)[0]
    return symbol


def _is_fno(instrument_type: Optional[str]) -> bool:
    return str(instrument_type or "EQ").upper() in _FNO_TYPES


def _row_matches(position: Dict[str, Any], broker_row: Dict[str, Any]) -> bool:
    """True if `broker_row` (a raw broker net row) is the broker's view of our DB
    `position`. Match on tradingsymbol AND, for F&O, the NFO exchange (so a cash
    row can't shadow a same-name contract, and vice-versa) — the SAME rule as
    zerodha.get_net_position_qty."""
    want_sym = _bare_symbol(position.get("symbol") or "")
    row_sym = str(broker_row.get("tradingsymbol") or "")
    if row_sym != want_sym:
        return False
    itype = position.get("instrument_type")
    rexch = str(broker_row.get("exchange") or "").upper()
    if _is_fno(itype):
        # F&O leg: require the row to be on NFO (when the row states an exchange).
        if rexch and rexch != "NFO":
            return False
    else:
        # Cash leg: accept NSE / BSE / unstated; reject a stray NFO contract row
        # of the same base name.
        if rexch == "NFO":
            return False
    return True


def _num(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _real_exit_price(position: Dict[str, Any],
                     broker_row: Dict[str, Any]) -> tuple:
    """Compute the REAL exit price for a day-flat (net 0) position + whether it
    is approximate.

    A LONG that closed intraday SOLD to exit → the day SELL average is the exit
    price (sell_price, or sell_value/sell_quantity when present). A SHORT covered
    intraday BOUGHT to exit → the day BUY average. When the broker's exit-side avg
    is absent, fall back to the position's stored ltp and flag approx=True.

    Returns (exit_price_or_None, approx_bool)."""
    direction = str(position.get("direction") or "long").lower()
    if direction == "short":
        # covered by BUYING → buy average is the exit price
        avg = _num(broker_row.get("buy_price"))
        if avg is None:
            bv, bq = _num(broker_row.get("buy_value")), _num(broker_row.get("buy_quantity"))
            if bv is not None and bq:
                avg = bv / bq
    else:
        avg = _num(broker_row.get("sell_price"))
        if avg is None:
            sv, sq = _num(broker_row.get("sell_value")), _num(broker_row.get("sell_quantity"))
            if sv is not None and sq:
                avg = sv / sq
    if avg is not None and avg > 0:
        return avg, False
    # Fall back to the mark; flag approximate.
    return position.get("ltp"), True


def reconcile_broker_positions(session) -> List[Dict[str, Any]]:
    """Validate this session's OPEN / EXIT_FAILED DB positions against the broker's
    live net book and correct the DB to the broker (SINGLE SOURCE OF TRUTH).

    Returns a list of action dicts (see the decision table in the module docstring).
    Places NO order. LIVE only — paper returns [].
    """
    # Paper / dry-run → never reconcile (no real broker book; paper is unchanged).
    if getattr(session, "dry_run", True):
        return []
    if not getattr(session, "brokers", None):
        try:
            session._build_brokers()
        except Exception as e:  # never block on a build failure
            log.warning("reconcile: _build_brokers failed for %s: %s",
                        session.session_id, e)
            return []

    registry = session.registry
    monitor = session.monitor
    brokers: Dict[str, Any] = session.brokers or {}

    # 1. Fetch the net book ONCE. None ⇒ unreachable ⇒ DO NOTHING.
    #    We ask each distinct broker profile once and merge the books, keyed by
    #    profile, so a multi-broker session reconciles per profile. If ANY
    #    profile answers None we treat THAT profile's positions as unknown (skip
    #    them) — a mix of reachable + unreachable brokers still reconciles the
    #    reachable ones and never flattens on the unreachable one.
    books: Dict[str, Optional[List[dict]]] = {}
    for prof_id, broker in brokers.items():
        try:
            books[prof_id] = broker.get_positions_net()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("reconcile: get_positions_net raised for %s/%s: %s",
                        session.session_id, prof_id, e)
            books[prof_id] = None

    # If EVERY broker is unreachable (all None) → nothing to do at all.
    if all(book is None for book in books.values()):
        log.debug("reconcile %s: all broker books None (unreachable) — no-op",
                  session.session_id)
        return []

    # 2. Examine only OPEN / EXIT_FAILED rows (idempotent — never re-touch terminal).
    positions = [p for p in registry.get_all_positions()
                 if str(p.get("status")) in ("OPEN", "EXIT_FAILED")]
    if not positions:
        return []

    # DELIVERY (CNC) HOLDINGS: an equity CNC position settles OUT of
    # positions()['net'] into holdings on T+1 — the next day it shows net 0 (WITH
    # NO sell) or vanishes from the net book, even though it is STILL HELD. For a
    # delivery session we consult holdings so a real overnight hold is NEVER
    # mistaken for a close. symbol -> total held qty (delivered + t1 not-yet-
    # delivered). None/absent holdings → empty map (the exit-evidence guard below
    # is then what prevents a false close). Non-delivery sessions skip this.
    is_delivery = str(getattr(session.config, "order_product", "") or "").upper() == "CNC"
    holdings_qty: Dict[str, int] = {}
    if is_delivery:
        for prof_id, broker in brokers.items():
            try:
                hl = broker.get_holdings()
            except Exception as e:  # pragma: no cover - defensive
                log.debug("reconcile: get_holdings raised %s: %s", prof_id, e)
                hl = None
            for h in (hl or []):
                sym = _bare_symbol(str(h.get("tradingsymbol") or ""))
                if not sym:
                    continue
                q = int((_num(h.get("quantity")) or 0)
                        + (_num(h.get("t1_quantity")) or 0))
                holdings_qty[sym] = holdings_qty.get(sym, 0) + q

    actions: List[Dict[str, Any]] = []
    changed = False  # any close or qty-correction → re-freeze the basis after

    for pos in positions:
        prof_id = pos.get("broker_profile")
        symbol = pos.get("symbol")
        db_qty = int(pos.get("qty") or 0)
        is_short = str(pos.get("direction") or "long").lower() == "short"
        held = holdings_qty.get(_bare_symbol(symbol or ""), 0)

        book = books.get(prof_id)
        if book is None:
            # No book for this profile. Single-broker fallback: if exactly one book
            # is non-None use it; else skip (unknown for this profile).
            non_none = [b for b in books.values() if b is not None]
            if len(non_none) == 1:
                book = non_none[0]
            else:
                log.debug("reconcile %s/%s: no broker book for profile %s — skip",
                          session.session_id, symbol, prof_id)
                continue

        # EMPTY net book → do NOT close. A delivery position may be in holdings
        # (net book empty is normal on a later day); if holdings confirms it we
        # fall through to the held-check below, else it's a transient blip.
        if isinstance(book, list) and len(book) == 0 and held <= 0:
            log.warning(
                "reconcile %s/%s: broker net book EMPTY (holdings-held=0) — "
                "transient blip, NO change", session.session_id, symbol)
            continue

        match = next((r for r in book if _row_matches(pos, r)), None)

        # Exit evidence from the day book: a real round-trip (a long that SOLD, a
        # short that BOUGHT to cover). Absence of an exit trade means net-0 is a
        # DELIVERY/SETTLEMENT move, NOT a close.
        net_qty = None
        exit_evidence = False
        if match is not None:
            nq = _num(match.get("quantity"))
            net_qty = int(nq) if nq is not None else None
            sell_q = int(_num(match.get("sell_quantity")) or 0)
            buy_q = int(_num(match.get("buy_quantity")) or 0)
            exit_evidence = (buy_q > 0) if is_short else (sell_q > 0)

        # ── THE ONLY CLOSE PATH: net 0 AND an actual exit trade AND not sitting
        #    in holdings. A net-0 row with NO sell (a delivered/settled CNC) is
        #    NEVER closed here — that was the bug that would flatten overnight CNC.
        if net_qty == 0 and exit_evidence and held <= 0:
            exit_price, approx = _real_exit_price(pos, match)
            try:
                registry.mark_closed(symbol, "BROKER_RECONCILED_FLAT",
                                     exit_price=exit_price, broker_profile=prof_id)
            except Exception as e:  # pragma: no cover - defensive
                log.warning("reconcile: mark_closed failed %s/%s: %s",
                            session.session_id, symbol, e)
                continue
            changed = True
            log.warning(
                "reconcile %s/%s: broker exited (net 0, exit trade seen) — marked "
                "CLOSED @ %s%s", session.session_id, symbol, exit_price,
                " (approx=ltp)" if approx else "")
            act = {"symbol": symbol, "action": "CLOSED_EXTERNAL",
                   "exit_price": exit_price, "source": "BROKER_RECONCILED_FLAT"}
            if approx:
                act["exit_price_approx"] = True
            actions.append(act)
            continue

        # ── AUTHORITATIVE held qty = max(net book, holdings). A short's net is
        #    negative → abs(). Holdings carries a delivered CNC after T+1. ───────
        net_held = abs(net_qty) if (net_qty is not None and net_qty != 0) else 0
        broker_held = max(net_held, held)

        if broker_held > 0:
            # Genuinely held at the broker (net book and/or holdings). Correct the
            # DB qty to the broker when they diverge (broker = source of truth).
            if broker_held != db_qty:
                broker_avg = _num(match.get("average_price")) if match else None
                db_avg = _num(pos.get("avg_price"))
                new_avg = None
                if broker_avg and broker_avg > 0 and db_avg and db_avg > 0 \
                        and abs(broker_avg - db_avg) / db_avg > _AVG_CORRECT_THRESHOLD:
                    new_avg = broker_avg
                try:
                    registry.set_qty(symbol, broker_held, avg_price=new_avg,
                                     broker_profile=prof_id)
                except Exception as e:  # pragma: no cover - defensive
                    log.warning("reconcile: set_qty failed %s/%s: %s",
                                session.session_id, symbol, e)
                    continue
                changed = True
                log.warning("reconcile %s/%s: MISMATCH_QTY_CORRECTED %d → %d "
                            "(broker truth)%s", session.session_id, symbol, db_qty,
                            broker_held,
                            f" avg {db_avg}->{new_avg}" if new_avg is not None else "")
                act = {"symbol": symbol, "action": "QTY_CORRECTED",
                       "from": db_qty, "to": broker_held}
                if new_avg is not None:
                    act["avg_from"] = db_avg
                    act["avg_to"] = new_avg
                actions.append(act)
            # else: in sync → no action.
            continue

        # ── Not held anywhere AND no exit trade seen → AMBIGUOUS. Do NOT close.
        #    (net 0 with no sell and not in holdings, OR absent from a non-empty
        #    book.) A false close is worse than a flagged stale-open.
        log.warning(
            "reconcile %s/%s: not held at broker (net/holdings 0) and no exit "
            "trade seen — manual review (NOT closing)", session.session_id, symbol)
        actions.append({"symbol": symbol, "action": "ABSENT_REVIEW"})

    # 3. Re-freeze the invested basis over the REMAINING OPEN positions so the
    #    kill / trail denominator matches reality after any close / qty correction.
    if changed:
        try:
            monitor.refreeze_invested_basis()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("reconcile: refreeze_invested_basis failed for %s: %s",
                        session.session_id, e)

    return actions
