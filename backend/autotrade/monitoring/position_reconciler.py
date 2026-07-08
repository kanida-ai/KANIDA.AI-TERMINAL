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
  * NEVER adopts / mutates a broker orphan (broker holds more than we track).

PHASE 7 — ORDER-ID-SCOPED cross-check (a MANUAL trade is INVISIBLE):
  Zerodha's positions() net has NO order-ids, so a manual trade in a symbol our
  session also holds used to leak into the invariant (a SURPLUS → ORPHAN_AT_BROKER,
  a manual sell → UNATTRIBUTED_CLOSE). We reconcile by session_id + broker order-id,
  never by symbol/qty. The new invariant, per (symbol, product):
    * broker_held >= our_db_held → IN SYNC or SURPLUS. A surplus (broker holds MORE
      than we track) is a MANUAL trade / the trader's own holding / a different
      session — NOT ours. It is INVISIBLE: no ORPHAN_AT_BROKER, no action, no alert.
      (The one preserved exception: a CLEAN corp-action ratio, a split/bonus that
      multiplied OUR shares with no order → a NON-mutating CORP_ACTION_SUSPECTED.)
    * broker_held <  our_db_held → we appear SHORT of our OWN records. Resolve on
      OUR order-ids first (positive evidence); a shortfall REMAINING with no
      order-id evidence is the ONLY alerting case → UNATTRIBUTED_CLOSE (flag, NEVER
      close without evidence). This correctly stays silent for a manual sell of the
      trader's OTHER shares as long as the broker still covers our tracked qty.
  The IRREDUCIBLE shared-account case: a manual sell that dips the FUNGIBLE account
  BELOW our tracked qty is indistinguishable from our own position closing — we
  FLAG it (UNATTRIBUTED_CLOSE), never mutate. A flagged-but-open row beats a false
  close.

DELIVERY CONTRACT (unchanged from v1):
  * LIVE only. Paper (dry_run) / None book (unreachable / expired) / empty book →
    return [] immediately, mutate NOTHING.
  * Places NO order, ever.
  * Fetches the broker net book + holdings + orderbook in ONE call each, per broker
    profile (the orderbook STRENGTHENS attribution — a batched second source for
    OUR OWN recorded exit order-ids; a manual order-id is never consulted).

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


def _orderbook_exit_evidence(pos: Dict[str, Any],
                             order_map: Dict[str, Dict[str, Any]]
                             ) -> Optional[Dict[str, Any]]:
    """RECONCILIATION FRAMEWORK (Phase 7): POSITIVE order-id evidence that THIS
    position closed, resolved from a PRE-FETCHED day orderbook map (one
    get_orders() call), or None.

    STRICTLY OUR-ORDER-ID-SCOPED. Consults ONLY the position's OWN recorded
    exit_order_id. An order-id that is NOT one of our positions' order-ids is NEVER
    looked up here — a MANUAL order in an overlapping symbol can never be
    attributed as our close. This is a batched SECOND SOURCE alongside
    _confirmed_close's per-position get_order_status (kept EXACTLY as-is): if that
    transient per-position probe missed but the batched book shows OUR exit
    COMPLETE, we still attribute it — never a manual order.

    Requires the mapped order to be COMPLETE, filled_quantity > 0, and on the
    CLOSING side (long → SELL, short → BUY-to-cover) when the book states a side.
    Returns the same evidence shape as _confirmed_close, else None. Never raises."""
    exit_oid = pos.get("exit_order_id")
    if not exit_oid or not order_map:
        return None
    order = order_map.get(str(exit_oid))
    if not order:
        return None
    if str(order.get("status") or "").upper() != "COMPLETE":
        return None
    if int(_num(order.get("filled_quantity")) or 0) <= 0:
        return None
    # Closing side must match the position's direction so a mis-recorded id can't
    # mis-attribute. When the book states no side, accept (it IS our exit id).
    txn = str(order.get("transaction_type") or "").upper()
    if txn:
        want = ("BUY" if str(pos.get("direction") or "long").lower() == "short"
                else "SELL")
        if txn != want:
            return None
    price = _num(order.get("average_price"))
    return {
        "exit_price": price if (price and price > 0) else pos.get("ltp"),
        "exit_order_id": str(exit_oid),
        "close_reason": "RECONCILED_EXIT",
        "filled_qty": int(_num(order.get("filled_quantity")) or 0),
    }


# ── GUARD G3 (mode C3): corporate-action classifier ──────────────────────────
# A split / bonus changes the BROKER quantity with NO order — so the invariant
# sees a surplus (broker > db) or deficit (broker < db) it cannot attribute to
# any order-id. When the divergence is a CLEAN corporate-action ratio we raise a
# distinct CORP_ACTION_SUSPECTED alert carrying the detected ratio. On the DEFICIT
# side this REPLACES a generic UNATTRIBUTED_CLOSE; on the SURPLUS side (P7) a
# non-ratio surplus is INVISIBLE, so a clean ratio is the ONLY surplus that
# surfaces at all. We NEVER auto-mutate — this only RECLASSIFIES / gates the alert.
#
# Ratios: broker ≈ db × R for a split/bonus multiplier R. Common Indian equity
# corporate actions: 1:1 bonus (×2), 2:1 bonus (×3), 1:5 split from ₹10→₹2 face
# (×5), 3:2 bonus (×2.5), 1:10 split ₹10→₹1 (×10), and the fractional 1.5 that a
# 1:2 bonus (3-for-2) produces (×1.5). A reverse split shrinks qty (db × R with
# R<1) — the reciprocals are covered by classifying db/broker too.
_CORP_ACTION_RATIOS = (2.0, 3.0, 5.0, 1.5, 2.5, 10.0)
# Tolerance: broker qty is an INTEGER, so db×R may be off by rounding on odd lots.
# Accept a ratio within ±2% (a clean split/bonus lands on the integer exactly for
# a round lot; the tolerance only forgives fractional-share rounding).
_CORP_ACTION_TOL = 0.02


def _corp_action_ratio(broker_held: int, db_held: int) -> Optional[float]:
    """Return the CLEAN corporate-action multiplier R such that broker ≈ db × R
    (a split/bonus grew the broker qty) — or its reciprocal for a reverse split —
    else None. Conservative: only a ratio within tolerance of a known
    _CORP_ACTION_RATIOS entry qualifies; anything else stays None (generic alert).

    Both sides must be > 0 (a 0 on either side is a real close/orphan, not a
    ratio). Never raises."""
    try:
        b = int(broker_held)
        d = int(db_held)
    except (TypeError, ValueError):
        return None
    if b <= 0 or d <= 0 or b == d:
        return None
    # Growth (split/bonus): broker larger. Shrink (reverse split): broker smaller.
    hi, lo = (b, d) if b > d else (d, b)
    raw = hi / lo
    for R in _CORP_ACTION_RATIOS:
        if abs(raw - R) <= _CORP_ACTION_TOL * R:
            # Report the SIGNED multiplier vs db: >1 broker grew, <1 broker shrank.
            return round(R if b > d else (1.0 / R), 4)
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
      * ALERTS (never mutate): {action: UNATTRIBUTED_CLOSE | CORP_ACTION_SUSPECTED,
        symbol, product, deficit|ratio}
    PHASE 7 order-id-scoped invariant, per (symbol, product):
      * broker_held >= db → IN SYNC or a SURPLUS. A surplus is a MANUAL / other
        position (NOT ours) → INVISIBLE (no action, no alert), unless it is a
        CLEAN corp-action ratio → non-mutating CORP_ACTION_SUSPECTED.
      * broker_held <  db → a DEFICIT: order-id resolution (positive evidence,
        incl. the batched orderbook second source), else UNATTRIBUTED_CLOSE (a
        reverse-split-ratio deficit reclassifies to CORP_ACTION_SUSPECTED).
    NEVER an auto-correction from the aggregate.
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
    orderbooks: Dict[str, Optional[List[dict]]] = {}
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
        # PHASE 7: the day orderbook — a batched second source for OUR OWN recorded
        # exit order-ids (strengthens deficit attribution). None (unreachable /
        # paper) → the per-position get_order_status floor is used unchanged.
        try:
            orderbooks[prof_id] = broker.get_orders()
        except Exception as e:  # pragma: no cover - defensive
            log.debug("reconcile: get_orders raised for %s/%s: %s",
                      session.session_id, prof_id, e)
            orderbooks[prof_id] = None

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

    # PHASE 7: per-profile order_id → row maps from the day orderbook (built once).
    # ONLY consulted for OUR OWN recorded exit order-ids (never a manual order-id).
    order_maps: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for pid, ob in orderbooks.items():
        if isinstance(ob, list):
            m: Dict[str, Dict[str, Any]] = {}
            for o in ob:
                if isinstance(o, dict):
                    oid = o.get("order_id")
                    if oid not in (None, ""):
                        m[str(oid)] = o
            order_maps[pid] = m

    def _order_map_for(prof_id: Optional[str]) -> Dict[str, Dict[str, Any]]:
        """The orderbook map for a position's broker profile, or the single map for
        a single-broker session, else {} (no orderbook → per-position floor)."""
        m = order_maps.get(str(prof_id or ""))
        if m is not None:
            return m
        if len(order_maps) == 1:
            return next(iter(order_maps.values()))
        return {}

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
        # CNC (delivery): settled HOLDINGS (demat + t1) PLUS today's unsettled BUYS
        # only. Today's CNC buys sit in net.quantity (not yet in holdings) → ADD.
        # A CNC SELL is negative net, but the sold shares are ALREADY removed from
        # holdings (holdings reflects the post-sell balance) — so a negative net must
        # NOT be subtracted again or we DOUBLE-COUNT the sell. Hence `max(0, net)`,
        # not `net`. (Verified LIVE 2026-07-08 AEGISLOG: holdings t1 35 STILL held +
        # net -57 from OTHER sessions' ladder-exits selling their lots → true held is
        # 35, but the old `max(0, net+holdings)` gave max(0,-22)=0, a false
        # UNATTRIBUTED_CLOSE. And 2026-07-06 AEGISLOG: holdings 35 + net +57 buys =
        # 92 held → 35+max(0,57)=92 ✓. ACUTAAS fully sold: holdings 0 + net -12 →
        # 0+max(0,-12)=0 ✓.) A fully-sold delivery floors to 0; the DB position is
        # then closed by ITS OWN order-id evidence, not this aggregate.
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
            broker_held = held + max(0, broker_net)
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
                    # PHASE 7 strengthening: a batched second source for OUR OWN
                    # recorded exit_order_id (never a manual order-id). Catches an
                    # exit the per-position get_order_status transiently missed.
                    ev = _orderbook_exit_evidence(
                        pos, _order_map_for(pos.get("broker_profile")))
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
                # GUARD G3: if broker vs db is a CLEAN corp-action ratio (e.g. a
                # reverse split shrank the broker qty), RECLASSIFY as
                # CORP_ACTION_SUSPECTED with the ratio instead of the generic alert.
                ratio = _corp_action_ratio(broker_held, db_held_all)
                if ratio is not None:
                    detail = (f"broker_held={broker_held} vs db_held_all="
                              f"{db_held_all}; clean corp-action ratio={ratio} "
                              f"(no order evidence) — split/bonus SUSPECTED, NOT "
                              f"auto-mutated")
                    _persist_alert(session.session_id, bare_sym, product,
                                   "CORP_ACTION_SUSPECTED", detail)
                    actions.append({
                        "action": "CORP_ACTION_SUSPECTED", "symbol": bare_sym,
                        "product": product, "ratio": ratio,
                        "broker_held": int(broker_held),
                        "db_held": int(db_held_all)})
                else:
                    detail = (f"broker_held={broker_held} < db_held_all="
                              f"{db_held_all}; unresolved deficit={deficit} after "
                              f"order-id evidence")
                    _persist_alert(session.session_id, bare_sym, product,
                                   "UNATTRIBUTED_CLOSE", detail)
                    actions.append({
                        "action": "UNATTRIBUTED_CLOSE", "symbol": bare_sym,
                        "product": product, "deficit": int(deficit)})
            continue

        # broker_held > db_held_all → SURPLUS: the broker holds MORE than our
        # sessions track for (symbol, product). PHASE 7 CORE CHANGE — a surplus is
        # a MANUAL trade / the trader's own holding / a DIFFERENT session, NOT ours.
        # Because Zerodha's net book carries NO order-ids we CANNOT attribute the
        # extra to any of our orders, and it is not ours to reconcile → it is
        # INVISIBLE: NO ORPHAN_AT_BROKER, no action, no alert. (This is the literal
        # "reconcile by session_id + order_id, never by symbol/qty" model.)
        #
        # The ONE preserved exception is GUARD G3: a CLEAN corp-action ratio (a
        # split/bonus that multiplied OUR shares with no order — e.g. exactly ×2)
        # → a distinct, NON-mutating CORP_ACTION_SUSPECTED. This is conservative
        # (a bonus has no order-id, so it can never be CONFIRMED — we flag at most,
        # never mutate). The documented trade-off: a manual buy that lands on an
        # EXACT clean multiple (e.g. our 100 + a manual 100 = 200 = ×2) is the one
        # rare coincidence that still flags — acceptable, info-level, non-mutating.
        # An ARBITRARY surplus (our 100 + a manual 37 = 137, ratio 1.37) is fully
        # invisible.
        ratio = _corp_action_ratio(broker_held, db_held_all)
        if ratio is not None:
            detail = (f"broker_held={broker_held} vs db_held_all={db_held_all}; "
                      f"clean corp-action ratio={ratio} — split/bonus SUSPECTED, "
                      f"NOT auto-adopted (a manual clean-multiple buy would look "
                      f"identical; non-mutating)")
            _persist_alert(session.session_id, bare_sym, product,
                           "CORP_ACTION_SUSPECTED", detail)
            actions.append({
                "action": "CORP_ACTION_SUSPECTED", "symbol": bare_sym,
                "product": product, "ratio": ratio,
                "broker_held": int(broker_held), "db_held": int(db_held_all)})
        # else: INVISIBLE — a surplus we can't attribute to our order-ids is a
        # manual/other position, not ours. No alert, no action (the P7 change).

    # After any close, re-freeze the invested basis over the REMAINING OPEN
    # positions so the kill / trail denominator matches reality.
    if changed:
        try:
            monitor.refreeze_invested_basis()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("reconcile: refreeze_invested_basis failed for %s: %s",
                        session.session_id, e)

    return actions
