"""PositionRegistry — ISOLATED autotrade session position CRUD.

DATA-ISOLATION (CRITICAL FIX): all session positions live in the dedicated
`autotrade_positions` table, keyed by (session_id, symbol[, broker_profile]).
This registry NEVER reads, writes, upserts, or locks `falcon_position_state` —
that table is owned by the existing Falcon swing system and keyed by `symbol`,
so a paper session writing into it would overwrite a real held position and
plant a phantom row the live monitor would put a real SL on.

Every method here is scoped to self.session_id; a session can only ever see /
mutate its own rows.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn
from .pricing import resolve_ltp
from .. import order_ledger as _order_ledger

log = logging.getLogger("kanida.autotrade.registry")
IST = timezone(timedelta(hours=5, minutes=30))


# ── SESSION MODE resolution (P0 PAPER-LEAK FIX, 2026-07-16) ──────────────────
def session_mode(session_id: str) -> Optional[str]:
    """'live' | 'paper' for a session, or None when the session row is UNKNOWN.

    None (unknown) is meaningful: the mode-scoped sibling filters fall back to
    their pre-fix, un-scoped behaviour rather than guess — so a synthetic /
    orphan session_id (no autotrade_sessions row) behaves byte-for-byte as
    before. Never raises."""
    try:
        with falcon_conn() as con:
            r = con.execute(
                "SELECT mode FROM autotrade_sessions WHERE session_id=?",
                (session_id,)).fetchone()
    except Exception as e:  # pragma: no cover - defensive
        log.debug("session_mode(%s) failed: %s", session_id, e)
        return None
    if r is None:
        return None
    m = str(r["mode"] or "").strip().lower()
    return m if m in ("live", "paper") else None


def session_mode_cached(session_id: str, cache: Dict[str, Optional[str]]
                        ) -> Optional[str]:
    """session_mode() memoised in a caller-supplied dict (per reconcile cycle)."""
    if session_id not in cache:
        cache[session_id] = session_mode(session_id)
    return cache[session_id]


# ── SESSION-SCOPED pre-exit flat guard (C1) ──────────────────────────────────
# The broker's net book is ACCOUNT-wide: when two sessions hold the same symbol
# a plain `broker_net == 0` (or `broker_net < our_qty`) test cannot tell whose
# shares are gone, so session A could (a) wrongly treat session B's still-open
# shares as "already flat", or (b) place a market exit that SELLS INTO B's lot
# (an oversell). These helpers make the decision SESSION-SCOPED: they subtract
# the qty other sessions still hold from the broker net, leaving only the shares
# attributable to THIS session. Reads autotrade_positions ONLY (never
# falcon_position_state) — data-isolation preserved.
def sibling_open_qty(session_id: str, symbol: str,
                     instrument_type: Optional[str] = None,
                     *, broker_profile: Optional[str] = None,
                     broker_account_id: Optional[str] = None,
                     product: Optional[str] = None,
                     mode: Optional[str] = None) -> int:
    """Σ still-held qty for `symbol` across OTHER sessions ON THE SAME BROKER
    ACCOUNT **AND IN THE SAME MODE** (status OPEN or EXIT_FAILED — an EXIT_FAILED
    sibling still holds its shares at the broker). When instrument_type is given
    it must match (so a cash EQ lot and a FUT contract of the same base name never
    cross-count).

    🔴 P0 PAPER-LEAK FIX (2026-07-16) — MODE SCOPING. `broker_net` (in
    our_held_at_broker) is BROKER REALITY: it contains ONLY LIVE fills. A PAPER
    session places no order, so its rows are NOT in that net — yet its position
    rows carry IDENTICAL scope keys (profile 'default', broker_account_id NULL,
    product 'MIS'), so before this fix a paper sibling's qty was subtracted from a
    LIVE session's broker net. PROVEN 2026-07-15 (live b447b0d7f6dc, MAPMYINDIA):
    max(0, min(706, 2030 − (1324 live + 843 PAPER))) = 0 → the live session
    "reconciled flat" and NEVER SOLD 706 real shares (~₹8.33L). Without the paper
    term: 2030 − 1324 = 706 = correct. Siblings are now scoped to the CALLING
    session's mode (resolved from autotrade_sessions.mode): a live computation
    counts only live rows; a paper computation only paper rows. `mode` may be
    passed explicitly; when None it is resolved from `session_id`, and if THAT is
    unknown (synthetic/orphan id, no session row) NO mode filter is applied —
    byte-for-byte the pre-fix behaviour for those.

    CLUSTER 9 ITEM 3 (2026-07-11) — THE CORE MULTI-USER/MULTI-ACCOUNT ISOLATION FIX.
    `broker_net` (in our_held_at_broker) is a PER-ACCOUNT figure: it is the net qty
    of ONE broker client (selected by broker_profile, bound to ONE vaulted
    broker_account_id). So the siblings we subtract from it MUST be on the SAME
    account — a DIFFERENT account's same-symbol lot (another USER, or the same
    user's OTHER account) is NOT in this account's net and must NEVER be
    subtracted, else session A would misread its own still-held shares as flat.
    When broker_account_id / broker_profile / product are given, siblings are
    scoped to the SAME value via COALESCE('') matching (so NULL==NULL). All
    default None → no extra filter → byte-for-byte the old cross-account behaviour
    for the single-account world (where every row shares the same NULL account)."""
    clauses = ["p.symbol=?", "p.session_id<>?",
               "p.status IN ('OPEN','EXIT_FAILED')", "p.qty>0"]
    params: list = [symbol, session_id]
    if instrument_type is not None:
        clauses.append("COALESCE(p.instrument_type,'EQ')=COALESCE(?,'EQ')")
        params.append(instrument_type)
    if broker_account_id is not None:
        clauses.append("COALESCE(p.broker_account_id,'')=COALESCE(?,'')")
        params.append(broker_account_id)
    if broker_profile is not None:
        clauses.append("COALESCE(p.broker_profile,'')=COALESCE(?,'')")
        params.append(broker_profile)
    if product is not None:
        clauses.append("COALESCE(p.product,'')=COALESCE(?,'')")
        params.append(product)
    # P0 MODE SCOPE: only siblings in the SAME mode. A sibling whose session row is
    # missing has NO mode evidence → LOWER(COALESCE(s.mode,'')) is '' → it matches
    # neither 'live' nor 'paper' → it is NOT subtracted (an unattributable row is
    # not evidence that our shares are gone; the min(our_qty, ...) clamp still
    # bounds the exit size). Skipped entirely when the caller's mode is unknown.
    want_mode = mode if mode is not None else session_mode(session_id)
    if want_mode is not None:
        clauses.append("LOWER(COALESCE(s.mode,''))=?")
        params.append(str(want_mode).strip().lower())
    sql = ("SELECT COALESCE(SUM(p.qty),0) AS q FROM autotrade_positions p "
           "LEFT JOIN autotrade_sessions s ON s.session_id=p.session_id WHERE "
           + " AND ".join(clauses))
    with falcon_conn() as con:
        r = con.execute(sql, params).fetchone()
    return int(r["q"] or 0) if r else 0


def our_held_at_broker(session_id: str, symbol: str,
                       instrument_type: Optional[str],
                       our_qty: int, broker_net,
                       *, broker_profile: Optional[str] = None,
                       broker_account_id: Optional[str] = None,
                       product: Optional[str] = None,
                       mode: Optional[str] = None) -> Optional[int]:
    """The qty attributable to THIS session that is STILL held at the broker:

        clamp(|broker_net| - Σ(same-account other sessions' held qty), 0, our_qty)

    `broker_net` is the broker's PER-ACCOUNT net for the symbol (signed; abs() so a
    short's negative net counts as held). Returns None when broker_net is None
    (paper / unknown) → the caller proceeds with its normal exit (paper is
    byte-for-byte unchanged). The position is "already closed FOR THIS SESSION"
    (our shares gone) iff the return is 0 — NOT when the whole account is 0.
    When there are NO siblings this equals min(our_qty, |broker_net|), so a
    fully-flat broker (0) → 0 and a fully-held broker (>=our_qty) → our_qty,
    byte-identical to the old `broker_net == 0` decision for the single-session
    case.

    CLUSTER 9 ITEM 3: broker_profile / broker_account_id / product scope the
    sibling subtraction to the SAME broker account so a DIFFERENT account's
    same-symbol lot is never subtracted (never misread as flat).

    🔴 P0 PAPER-LEAK FIX (2026-07-16): the subtraction is ALSO mode-scoped —
    `broker_net` contains only LIVE fills, so a PAPER sibling must never reduce a
    LIVE session's held qty (it once zeroed a real 706-share MAPMYINDIA exit).
    See sibling_open_qty. `mode` is resolved from session_id when not passed."""
    if broker_net is None:
        return None
    try:
        net = abs(int(broker_net))
        oq = int(our_qty)
    except (TypeError, ValueError):
        return None
    other = sibling_open_qty(session_id, symbol, instrument_type,
                             broker_profile=broker_profile,
                             broker_account_id=broker_account_id,
                             product=product, mode=mode)
    return max(0, min(oq, net - other))


class PositionRegistry:
    def __init__(self, session_id: str, total_allocated_capital: float):
        self.session_id = session_id
        self.total_allocated_capital = total_allocated_capital

    # ── Register / partial-fill ───────────────────────────────────────────────
    def register(self, *, symbol: str, broker_profile: str, qty: int,
                 avg_price: float, product: str = "CNC",
                 instrument_type: str = "EQ", exchange: Optional[str] = None,
                 sl_level: Optional[float] = None,
                 target_price: Optional[float] = None,
                 entry_date: Optional[str] = None,
                 broker_account_id: Optional[str] = None,
                 direction: str = "long",
                 entry_order_id: Optional[str] = None,
                 client_order_id: Optional[str] = None,
                 _fill_event: Optional[str] = None) -> None:
        """Insert or update a session position in autotrade_positions.

        Keyed by (session_id, symbol, broker_profile). Seeds the mark (ltp) to
        avg_price so a pre-open gross_return ~= 0 rather than a bogus value.

        PHASE-2 MULTI-TENANT: broker_account_id (NULLABLE) records WHICH vaulted
        account this position was opened through, for per-account audit. NULL =
        the operator/global account (today's behaviour).

        FUTURES long/short: `direction` ('long' default | 'short') is persisted
        so the P&L sign + exit side invert ONLY for shorts. 'long' is byte-for-
        byte unchanged.

        RECONCILIATION FRAMEWORK (Phase 1): entry_order_id (NULLABLE) is the
        broker order-id of the ENTRY fill, persisted so a broker order can be
        attributed to THIS session/position by order-id (never the account
        aggregate). NULL = unknown (paper / pre-migration rows) — the reconcilers
        handle absent ids. On an UPDATE the id is only overwritten when a new,
        non-None value is supplied (COALESCE keeps the original otherwise).
        """
        now = datetime.now(IST).isoformat()
        direction = "short" if str(direction).lower() == "short" else "long"
        with falcon_conn() as con:
            existing = con.execute(
                """SELECT id FROM autotrade_positions
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                (self.session_id, symbol, broker_profile),
            ).fetchone()
            if existing:
                con.execute(
                    """UPDATE autotrade_positions
                       SET qty=?, avg_price=?, instrument_type=?, exchange=?,
                           direction=?,
                           product=COALESCE(?, product),
                           sl_level=COALESCE(?, sl_level),
                           target_price=COALESCE(?, target_price),
                           ltp=COALESCE(ltp, ?),
                           entry_order_id=COALESCE(?, entry_order_id),
                           client_order_id=COALESCE(?, client_order_id),
                           status='OPEN'
                       WHERE id=?""",
                    (qty, avg_price, instrument_type, exchange, direction,
                     product, sl_level, target_price, avg_price, entry_order_id,
                     client_order_id, existing[0]),
                )
                _row_id = existing[0]
            else:
                cur = con.execute(
                    """INSERT INTO autotrade_positions
                       (session_id, broker_profile, broker_account_id, symbol,
                        instrument_type, exchange, qty, avg_price, sl_level,
                        target_price, ltp, unrealised_pnl, status, exit_lock,
                        opened_at, direction, entry_order_id, client_order_id,
                        product)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'OPEN', 0, ?, ?, ?, ?, ?)""",
                    (self.session_id, broker_profile, broker_account_id, symbol,
                     instrument_type, exchange, qty, avg_price, sl_level,
                     target_price, avg_price, 0.0, now, direction,
                     entry_order_id, client_order_id, product),
                )
                _row_id = cur.lastrowid
            con.commit()
        # CAP 2 — the entry fill is now durably recorded; append the FILLED event
        # (mapping client_order_id → broker entry order-id). Best-effort; never
        # raises into the order path.
        _order_ledger.append_event(
            session_id=self.session_id, symbol=symbol,
            event_type=(_fill_event or _order_ledger.EV_FILLED),
            position_ref=str(_row_id),
            product=product, broker_profile=broker_profile,
            broker_order_id=entry_order_id, client_order_id=client_order_id,
            qty=qty, price=avg_price, source="entry")

    def register_add(self, *, symbol: str, broker_profile: str, add_qty: int,
                     add_price: float, product: str = "CNC",
                     instrument_type: str = "EQ", exchange: Optional[str] = None,
                     broker_account_id: Optional[str] = None,
                     direction: str = "long",
                     entry_order_id: Optional[str] = None,
                     client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """AVERAGE a second fill INTO an existing open position (blended cost).

        Used by the Falcon Intraday Magnifier's SECOND (09:16) leg: the first leg
        already registered qty1@avg1; this adds qty2@avg2 so the position becomes
        (qty1+qty2) @ (qty1*avg1 + qty2*avg2)/(qty1+qty2). Distinct from
        register() (which REPLACES qty/avg) — this ACCUMULATES. When no row exists
        yet (the first leg was skipped/failed) it falls back to a plain register()
        of the add leg. Scoped to (session_id, symbol, broker_profile).

        Returns {blended_qty, blended_avg} (the resulting row's qty + avg)."""
        now = datetime.now(IST).isoformat()
        direction = "short" if str(direction).lower() == "short" else "long"
        with falcon_conn() as con:
            row = con.execute(
                """SELECT id, qty, avg_price FROM autotrade_positions
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                (self.session_id, symbol, broker_profile),
            ).fetchone()
        if row is None:
            # No first leg → just register the add leg as the whole position.
            self.register(symbol=symbol, broker_profile=broker_profile,
                          qty=int(add_qty), avg_price=float(add_price),
                          product=product, instrument_type=instrument_type,
                          exchange=exchange, broker_account_id=broker_account_id,
                          direction=direction, entry_order_id=entry_order_id,
                          client_order_id=client_order_id)
            return {"blended_qty": int(add_qty), "blended_avg": float(add_price)}
        old_qty = int(row["qty"] or 0)
        old_avg = float(row["avg_price"] or 0.0)
        new_qty = old_qty + int(add_qty)
        if new_qty <= 0:
            return {"blended_qty": old_qty, "blended_avg": old_avg}
        new_avg = ((old_qty * old_avg) + (int(add_qty) * float(add_price))) / new_qty
        with falcon_conn() as con:
            con.execute(
                """UPDATE autotrade_positions
                   SET qty=?, avg_price=?, ltp=COALESCE(ltp, ?),
                       entry_order_id=COALESCE(?, entry_order_id),
                       client_order_id=COALESCE(?, client_order_id),
                       status='OPEN'
                   WHERE id=?""",
                (new_qty, new_avg, new_avg, entry_order_id, client_order_id,
                 row["id"]))
            con.commit()
        # Durable FILLED event for the second leg (best-effort).
        _order_ledger.append_event(
            session_id=self.session_id, symbol=symbol,
            event_type=_order_ledger.EV_FILLED, position_ref=str(row["id"]),
            product=product, broker_profile=broker_profile,
            broker_order_id=entry_order_id, client_order_id=client_order_id,
            qty=int(add_qty), price=float(add_price), source="entry")
        return {"blended_qty": new_qty, "blended_avg": new_avg}

    def register_partial(self, symbol: str, broker_profile: str,
                         filled_qty: int, avg_price: float,
                         product: str = "CNC",
                         instrument_type: str = "EQ",
                         exchange: Optional[str] = None,
                         broker_account_id: Optional[str] = None,
                         direction: str = "long",
                         entry_order_id: Optional[str] = None,
                         client_order_id: Optional[str] = None) -> None:
        """Partial fill: register the FILLED qty (spec parity check #3 — 80 not
        0/100). entry_order_id (RECONCILIATION Phase 1) is persisted the same as
        the normal path so a partial-fill leg is still attributable by order-id."""
        log.warning("Partial fill registered: %s filled=%d", symbol, filled_qty)
        self.register(symbol=symbol, broker_profile=broker_profile,
                      qty=filled_qty, avg_price=avg_price, product=product,
                      instrument_type=instrument_type, exchange=exchange,
                      broker_account_id=broker_account_id,
                      direction=direction,
                      entry_order_id=entry_order_id,
                      client_order_id=client_order_id,
                      _fill_event=_order_ledger.EV_PARTIAL)

    # ── Reads ─────────────────────────────────────────────────────────────────
    def get_open_positions(self) -> List[Dict[str, Any]]:
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT * FROM autotrade_positions
                   WHERE session_id=? AND status='OPEN' AND qty > 0""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_all_positions(self) -> List[Dict[str, Any]]:
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT * FROM autotrade_positions WHERE session_id=?",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def set_exit_client_order_id(self, symbol: str, exit_client_order_id: str,
                                 broker_profile: Optional[str] = None) -> None:
        """CLUSTER 3 ITEM 3(b) — persist the durable EXIT client_order_id for a
        position the moment its first exit is placed, so the broker tag
        (compact_tag) is STABLE across a retry / restart and query-before-place can
        recognise OUR own already-placed exit. Only sets it when currently NULL
        (COALESCE keeps the first-minted id stable across retries). Best-effort;
        never raises into the exit path. Scoped by (session, symbol[, profile])."""
        try:
            with falcon_conn() as con:
                if broker_profile is not None:
                    con.execute(
                        """UPDATE autotrade_positions
                           SET exit_client_order_id=
                               COALESCE(exit_client_order_id, ?)
                           WHERE session_id=? AND symbol=?
                             AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                        (exit_client_order_id, self.session_id, symbol,
                         broker_profile))
                else:
                    con.execute(
                        """UPDATE autotrade_positions
                           SET exit_client_order_id=
                               COALESCE(exit_client_order_id, ?)
                           WHERE session_id=? AND symbol=?""",
                        (exit_client_order_id, self.session_id, symbol))
                con.commit()
        except Exception as e:  # pragma: no cover - never block the exit path
            log.warning("registry.set_exit_client_order_id(%s/%s) failed: %s",
                        self.session_id, symbol, e)

    def get_exit_client_order_id(self, symbol: str,
                                 broker_profile: Optional[str] = None
                                 ) -> Optional[str]:
        """Read the persisted EXIT client_order_id for a position (or None)."""
        try:
            with falcon_conn() as con:
                if broker_profile is not None:
                    r = con.execute(
                        """SELECT exit_client_order_id FROM autotrade_positions
                           WHERE session_id=? AND symbol=?
                             AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                        (self.session_id, symbol, broker_profile)).fetchone()
                else:
                    r = con.execute(
                        """SELECT exit_client_order_id FROM autotrade_positions
                           WHERE session_id=? AND symbol=?""",
                        (self.session_id, symbol)).fetchone()
            return (r["exit_client_order_id"] if r else None)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("registry.get_exit_client_order_id(%s/%s) failed: %s",
                      self.session_id, symbol, e)
            return None

    def get_exit_failed_all(self) -> List[Dict[str, Any]]:
        """ALL EXIT_FAILED positions (qty>0) for this session, regardless of the
        exit_lock — for status() so a stranded, still-held leg is VISIBLE (C3).
        (monitor.get_exit_failed_positions returns only the retry-eligible
        exit_lock=0 subset; this is the full set for display.)"""
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT * FROM autotrade_positions
                   WHERE session_id=? AND status='EXIT_FAILED' AND qty > 0""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_ltp(self, symbol: str, ltp: Optional[float] = None,
                   broker: Any = None,
                   broker_profile: Optional[str] = None) -> None:
        """Mark a position to market and recompute uPnL.

        If `ltp` is None, resolve a SANE price (broker LTP → ohlc_daily latest
        close → existing avg_price) so we never persist a stale unrelated value.
        Scoped to (session_id, symbol[, broker_profile]).
        """
        with falcon_conn() as con:
            row = con.execute(
                """SELECT id, avg_price FROM autotrade_positions
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                (self.session_id, symbol, broker_profile),
            ).fetchone()
            if row is None:
                # If no broker_profile given, fall back to any row for the symbol.
                row = con.execute(
                    """SELECT id, avg_price FROM autotrade_positions
                       WHERE session_id=? AND symbol=?""",
                    (self.session_id, symbol),
                ).fetchone()
            if row is None:
                return
            if ltp is None:
                ltp = resolve_ltp(symbol, broker=broker,
                                  fallback_entry=row["avg_price"])
            if ltp is None:
                return
            # FUTURES long/short: sign-aware uPnL. For 'long' the CASE is +1 so
            # this is byte-identical to (ltp-avg)*qty; for 'short' it is
            # (avg-ltp)*qty (profit when price falls).
            # Lifecycle#8: stamp the mark time (as-of) so a stale mark is visible.
            con.execute(
                """UPDATE autotrade_positions
                   SET ltp=?, ltp_as_of=?,
                       unrealised_pnl=(CASE WHEN direction='short' THEN -1
                                            ELSE 1 END) * (? - avg_price) * qty
                   WHERE id=?""",
                (ltp, datetime.now(IST).isoformat(), ltp, row["id"]),
            )
            con.commit()

    def mark_closed(self, symbol: str, reason: str,
                    exit_price: Optional[float] = None,
                    broker_profile: Optional[str] = None,
                    exit_order_id: Optional[str] = None) -> None:
        """Mark a position CLOSED, recording the realised P&L at exit_price.

        RECONCILIATION FRAMEWORK (Phase 1): exit_order_id (NULLABLE) is the
        broker order-id of the EXIT fill (a market exit, or the GTT-fired order).
        Persisted so the closing order can be attributed to THIS position by
        order-id. Only overwritten when a non-None value is supplied (COALESCE
        keeps any earlier value otherwise). NULL = unknown (paper / legacy).

        ZERO-FILL FLOOR (2026-07-07, CEMPRO circuit incident): a CLOSED row must
        NEVER carry exit_price 0.0 — a 0 exit books a phantom ~-100% realised loss
        (avg×qty). `NULLIF(x, 0)` treats a 0/absent price as UNKNOWN so the
        effective exit price falls through supplied-price → ltp → avg_price. When
        it lands on avg_price the realised P&L is 0 (P&L-neutral: an honest
        "no known exit price", never a fabricated -100%). This is byte-for-byte
        identical whenever a positive price is supplied OR ltp>0 (the only real
        paper/live flows); it ONLY changes the pathological 0/absent case."""
        now = datetime.now(IST).isoformat()
        with falcon_conn() as con:
            # FUTURES long/short: sign-aware realised P&L. 'long' CASE = +1 →
            # byte-identical to (exit-avg)*qty; 'short' = (avg-exit)*qty.
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET status='CLOSED', close_reason=?, closed_at=?,
                           exit_price=COALESCE(NULLIF(?,0), NULLIF(ltp,0), avg_price),
                           exit_order_id=COALESCE(?, exit_order_id),
                           realised_pnl=(CASE WHEN direction='short' THEN -1
                                              ELSE 1 END)
                                        * (COALESCE(NULLIF(?,0), NULLIF(ltp,0), avg_price) - avg_price)*qty
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (reason, now, exit_price, exit_order_id, exit_price,
                     self.session_id, symbol, broker_profile),
                )
            else:
                con.execute(
                    """UPDATE autotrade_positions
                       SET status='CLOSED', close_reason=?, closed_at=?,
                           exit_price=COALESCE(NULLIF(?,0), NULLIF(ltp,0), avg_price),
                           exit_order_id=COALESCE(?, exit_order_id),
                           realised_pnl=(CASE WHEN direction='short' THEN -1
                                              ELSE 1 END)
                                        * (COALESCE(NULLIF(?,0), NULLIF(ltp,0), avg_price) - avg_price)*qty
                       WHERE session_id=? AND symbol=?""",
                    (reason, now, exit_price, exit_order_id, exit_price,
                     self.session_id, symbol),
                )
            con.commit()
        # CAP 2/5 — durable POSITION_CLOSED event (cross-day close evidence the
        # reconciler consults before the single-day orderbook). Best-effort.
        _order_ledger.append_event(
            session_id=self.session_id, symbol=symbol,
            event_type=_order_ledger.EV_POSITION_CLOSED,
            broker_profile=broker_profile, broker_order_id=exit_order_id,
            price=exit_price, source="close", detail=reason)

    def set_qty(self, symbol: str, qty: int,
                avg_price: Optional[float] = None,
                broker_profile: Optional[str] = None) -> None:
        """Correct a position's qty (and optionally avg_price) to the BROKER's
        truth — used by the authoritative position reconciler when the broker's
        net qty diverges from our DB (partial fill, missed reconcile). Recomputes
        unrealised_pnl from the persisted ltp so the panel stays consistent.

        Sign-aware uPnL (FUTURES long/short): long CASE = +1 (byte-identical to
        (ltp-avg)*qty), short = (avg-ltp)*qty. avg_price is updated only when a
        non-None value is passed (COALESCE keeps the existing avg otherwise).
        Scoped to (session_id, symbol[, broker_profile])."""
        with falcon_conn() as con:
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET qty=?,
                           avg_price=COALESCE(?, avg_price),
                           unrealised_pnl=(CASE WHEN direction='short' THEN -1
                                                ELSE 1 END)
                               * (COALESCE(ltp, avg_price)
                                  - COALESCE(?, avg_price)) * ?
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (qty, avg_price, avg_price, qty,
                     self.session_id, symbol, broker_profile),
                )
            else:
                con.execute(
                    """UPDATE autotrade_positions
                       SET qty=?,
                           avg_price=COALESCE(?, avg_price),
                           unrealised_pnl=(CASE WHEN direction='short' THEN -1
                                                ELSE 1 END)
                               * (COALESCE(ltp, avg_price)
                                  - COALESCE(?, avg_price)) * ?
                       WHERE session_id=? AND symbol=?""",
                    (qty, avg_price, avg_price, qty, self.session_id, symbol),
                )
            con.commit()

    # ── GTT-OCO backup (FEATURE 1) ────────────────────────────────────────────
    def set_gtt(self, symbol: str, gtt_id: Optional[str],
                gtt_stop: Optional[float] = None,
                gtt_target: Optional[float] = None,
                broker_profile: Optional[str] = None) -> None:
        """Store the per-position GTT-OCO id + levels on the position row.

        In paper mode gtt_id is None (no real GTT) but gtt_stop/gtt_target are
        still recorded so the UI shows the intended floor/ceiling. Also mirrors
        the levels into sl_level / target_price for the existing UI fields.
        Scoped to (session_id, symbol[, broker_profile])."""
        with falcon_conn() as con:
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET gtt_id=?, gtt_stop=?, gtt_target=?,
                           sl_level=COALESCE(?, sl_level),
                           target_price=COALESCE(?, target_price)
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (gtt_id, gtt_stop, gtt_target, gtt_stop, gtt_target,
                     self.session_id, symbol, broker_profile),
                )
            else:
                con.execute(
                    """UPDATE autotrade_positions
                       SET gtt_id=?, gtt_stop=?, gtt_target=?,
                           sl_level=COALESCE(?, sl_level),
                           target_price=COALESCE(?, target_price)
                       WHERE session_id=? AND symbol=?""",
                    (gtt_id, gtt_stop, gtt_target, gtt_stop, gtt_target,
                     self.session_id, symbol),
                )
            con.commit()

    def set_slm(self, symbol: str, slm_order_id: Optional[str],
                broker_profile: Optional[str] = None) -> None:
        """RMS CAP 3 — store (or CLEAR with None) the broker-side protective SL-M
        order-id on an MIS position row. Scoped to (session_id, symbol[, profile]).
        Best-effort — never raises into the entry / exit path."""
        try:
            with falcon_conn() as con:
                if broker_profile is not None:
                    con.execute(
                        """UPDATE autotrade_positions SET slm_order_id=?
                           WHERE session_id=? AND symbol=?
                             AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                        (slm_order_id, self.session_id, symbol, broker_profile))
                else:
                    con.execute(
                        "UPDATE autotrade_positions SET slm_order_id=? "
                        "WHERE session_id=? AND symbol=?",
                        (slm_order_id, self.session_id, symbol))
                con.commit()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("registry.set_slm(%s/%s) failed: %s",
                        self.session_id, symbol, e)

    def get_open_positions_missing_gtt(self) -> List[Dict[str, Any]]:
        """OPEN positions that have NO gtt_id yet — used to backfill the broker
        backup on session start and boot-resume (e.g. positions opened before
        this feature deployed)."""
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT * FROM autotrade_positions
                   WHERE session_id=? AND status='OPEN' AND qty > 0
                     AND (gtt_id IS NULL OR gtt_id='')""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_exit_failed(self, symbol: str, error: str,
                         broker_profile: Optional[str] = None,
                         exit_order_id: Optional[str] = None) -> None:
        """Mark a position EXIT_FAILED and RELEASE the exit gate for retry.

        RECONCILIATION FRAMEWORK (Phase 1): when the exit order WAS placed but
        then rejected/cancelled, exit_order_id records the order-id that failed so
        the failed broker order is still attributable to this position. Only
        overwritten when non-None (COALESCE keeps any earlier value).

        MULTI-PROFILE SCOPING (Fix 4, 2026-07-11): when broker_profile is given the
        UPDATE is scoped to (session_id, symbol, broker_profile) with the same
        COALESCE(broker_profile,'') pattern as mark_closed, so a session holding the
        SAME symbol on two broker profiles mutates ONLY the firing leg — never both.
        broker_profile None keeps the old symbol-wide behaviour (byte-identical for
        the single-profile case that has always been the norm)."""
        now = datetime.now(IST).isoformat()
        with falcon_conn() as con:
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET status='EXIT_FAILED', close_reason=?, closed_at=?,
                           exit_order_id=COALESCE(?, exit_order_id)
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (f"EXIT_FAILED: {error}", now, exit_order_id,
                     self.session_id, symbol, broker_profile),
                )
            else:
                con.execute(
                    """UPDATE autotrade_positions
                       SET status='EXIT_FAILED', close_reason=?, closed_at=?,
                           exit_order_id=COALESCE(?, exit_order_id)
                       WHERE session_id=? AND symbol=?""",
                    (f"EXIT_FAILED: {error}", now, exit_order_id,
                     self.session_id, symbol),
                )
            con.commit()
        # CAP 2 — durable EXIT_FAILED event for the lifecycle trail. Best-effort.
        _order_ledger.append_event(
            session_id=self.session_id, symbol=symbol,
            event_type=_order_ledger.EV_EXIT_FAILED,
            broker_profile=broker_profile, broker_order_id=exit_order_id,
            source="exit", detail=error)
        # Release the exit gate so a future retry can reclaim it.
        # Import here to avoid circular import at module level.
        # CLUSTER 9c FIX F1 (2026-07-11): the RELEASE must be scoped by
        # broker_profile, exactly like the CLAIM (C9-I1) and the EXIT_FAILED mutate
        # above. A symbol-wide release here would clear a SIBLING profile's exit lock
        # while THAT profile's exit is still IN FLIGHT — so p2 could be re-claimed /
        # re-fired mid-flight when only p1's exit failed. broker_profile=None keeps
        # the old symbol-wide release (byte-identical for the single-profile norm).
        from autotrade.exit_gate import release_exit_session
        release_exit_session(self.session_id, symbol, broker_profile=broker_profile)

    def update_partial_exit(self, symbol: str, filled_qty: int,
                            exit_price: Optional[float],
                            broker_profile: Optional[str] = None) -> None:
        """Record a partial fill on an exit — reduce qty and record partial P&L.

        The position remains OPEN (status unchanged) so the remaining qty is still
        tracked. The realised_pnl is updated to reflect the partial exit at
        exit_price (or current ltp when exit_price is None/0).
        Called by exit_poller.confirm_exit when Kite reports COMPLETE but filled_qty
        < expected qty.

        MULTI-PROFILE SCOPING (Fix 4, 2026-07-11): when broker_profile is given BOTH
        the read (avg/qty/direction) AND the write are scoped to
        (session_id, symbol, broker_profile) so a same-symbol-two-profile session
        reduces ONLY the firing leg. broker_profile None keeps the old symbol-wide
        behaviour (byte-identical for the single-profile norm)."""
        now = datetime.now(IST).isoformat()
        log.warning(
            "partial exit for %s/%s: filled=%d exit_price=%s",
            self.session_id, symbol, filled_qty, exit_price)
        with falcon_conn() as con:
            if broker_profile is not None:
                row = con.execute(
                    """SELECT avg_price, qty, direction FROM autotrade_positions
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (self.session_id, symbol, broker_profile),
                ).fetchone()
            else:
                row = con.execute(
                    """SELECT avg_price, qty, direction FROM autotrade_positions
                       WHERE session_id=? AND symbol=?""",
                    (self.session_id, symbol),
                ).fetchone()
            if row is None:
                log.warning("update_partial_exit: no row for %s/%s",
                            self.session_id, symbol)
                return
            avg_price = float(row["avg_price"] or 0)
            orig_qty = int(row["qty"] or 0)
            remaining_qty = max(0, orig_qty - filled_qty)
            # Compute realised P&L for the filled portion only. FUTURES
            # long/short: sign +1 for long (byte-identical), -1 for short.
            fill_price = float(exit_price) if exit_price else avg_price
            _sign = -1.0 if str(row["direction"]).lower() == "short" else 1.0
            partial_realised = _sign * (fill_price - avg_price) * filled_qty
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET qty=?,
                           realised_pnl=COALESCE(realised_pnl, 0.0) + ?,
                           exit_price=COALESCE(?, exit_price)
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (remaining_qty, partial_realised, exit_price,
                     self.session_id, symbol, broker_profile),
                )
            else:
                con.execute(
                    """UPDATE autotrade_positions
                       SET qty=?,
                           realised_pnl=COALESCE(realised_pnl, 0.0) + ?,
                           exit_price=COALESCE(?, exit_price)
                       WHERE session_id=? AND symbol=?""",
                    (remaining_qty, partial_realised, exit_price,
                     self.session_id, symbol),
                )
            con.commit()
        # CAP 2 — durable EXIT_PARTIAL event for the lifecycle trail. Best-effort.
        _order_ledger.append_event(
            session_id=self.session_id, symbol=symbol,
            event_type=_order_ledger.EV_EXIT_PARTIAL,
            broker_profile=broker_profile, qty=filled_qty, price=exit_price,
            source="exit")
