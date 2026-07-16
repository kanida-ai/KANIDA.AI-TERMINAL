"""Boot-time recovery for AutoTrade sessions.

PROBLEM (the bug this fixes): the per-session tick driver and entry scheduler are
in-memory daemon threads (see monitoring/tick_driver.py + monitoring/
entry_scheduler.py). They DIE on a backend restart, so after a restart:
  * RUNNING sessions stop refreshing ltp / gross_return (the "LTP not updating"
    symptom) and the kill switch no longer AUTO-fires, and
  * SCHEDULED sessions whose entry_time hasn't arrived yet silently never fire.

FIX: `resume_active_sessions()` scans autotrade_sessions on startup and re-arms
the in-memory threads from the persisted state:
  * status == 'RUNNING'   → re-arm tick_driver.start_for_session (LTP/gross_return
                            keep refreshing; kill switch live again).
  * status == 'SCHEDULED' and entry_time (today IST) still in the FUTURE
                            → re-arm entry_scheduler.start_for_session.
  * status == 'SCHEDULED' and entry_time already PASSED while we were down
                            → fire the entry leg NOW (so it doesn't silently
                            hang). Paper = no real orders.

SAFETY / SEMANTICS:
  * Additive + idempotent: start_for_session() on both threads is a no-op when a
    driver/scheduler is already armed, so calling resume twice is harmless.
  * DATA-ISOLATION is inherited from the existing paths — re-arming the tick
    driver and firing via session._fire_entries() write ONLY to
    autotrade_positions, NEVER falcon_position_state.
  * Paper sessions place NO real orders (brokers built dry_run=True). Live exits/
    entries remain gated by FALCON_AUTOTRADE_ENABLED + mode='live'.
  * Wrapped at the call site (main.py lifespan) in try/except so it can never
    block boot. Each per-session step is also individually guarded so one bad
    session can't abort recovery of the others.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from falcon.db import falcon_conn

from .monitoring import tick_driver
from .monitoring import entry_scheduler
from .monitoring import ws_driver
from .monitoring import square_off_scheduler

log = logging.getLogger("kanida.autotrade.recovery")

IST = timezone(timedelta(hours=5, minutes=30))


def _active_sessions() -> List[Dict[str, Any]]:
    """Sessions that need an in-memory thread re-armed after a restart:
      * RUNNING / SCHEDULED — the normal live states (tick / entry threads), and
      * KILLING / KILLING_INCOMPLETE (CLUSTER 3 ITEM 1) — a crash MID-FLATTEN
        leaves the kill half-done: some legs OPEN / EXIT_FAILED with NO driver, so
        the EXIT_FAILED retry sweep never runs and real exposure sits unmanaged.
        We resume them (reconcile → re-flatten OPEN legs → re-arm the tick driver
        so the sweep drives the rest to CLOSED)."""
    with falcon_conn() as con:
        rows = con.execute(
            """SELECT session_id, status FROM autotrade_sessions
               WHERE status IN ('RUNNING', 'SCHEDULED',
                                'KILLING', 'KILLING_INCOMPLETE')
               ORDER BY created_at ASC"""
        ).fetchall()
    return [dict(r) for r in rows]


def _backfill_live_gtts(session_id: str) -> int:
    """For a LIVE RUNNING session, place a GTT-OCO for any OPEN position MISSING
    a gtt_id (so positions opened before this feature deployed get the broker
    backup retroactively). Paper sessions are SKIPPED. Returns count placed.

    DATA-ISOLATION + best-effort: writes only autotrade_positions; never raises.
    """
    from .session import TradingSession

    sess = TradingSession.load(session_id)
    if sess is None:
        return 0
    if sess.mode != "live":
        return 0  # paper: skip (no real GTTs)
    if not sess.config.per_position_gtt_enabled:
        return 0
    try:
        sess._build_brokers()
        results = sess.gtt_manager.backfill_missing()
        placed = sum(1 for r in results if r.get("status") == "PLACED")
        if results:
            log.info("recovery: backfilled GTTs for %s — %d placed of %d missing",
                     session_id, placed, len(results))
        return placed
    except Exception as e:  # never block recovery on the backup floor
        log.warning("recovery: GTT backfill failed for %s: %s", session_id, e)
        return 0


def _position_exists_for_intent(session_id: str, symbol: str,
                                broker_profile: Optional[str],
                                client_order_id: Optional[str]) -> bool:
    """True when a position row ALREADY exists for this entry intent — either the
    exact client_order_id is on a row for (session, symbol, profile), or ANY row
    (OPEN or CLOSED) already tracks (session, symbol, profile). Idempotency guard so
    the orphan-adoption NEVER double-registers a leg the fire path already inserted.
    """
    try:
        with falcon_conn() as con:
            r = con.execute(
                """SELECT 1 FROM autotrade_positions
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')
                     AND (client_order_id=? OR status IN ('OPEN','EXIT_FAILED',
                          'CLOSED','PARTIAL'))
                   LIMIT 1""",
                (session_id, symbol, broker_profile, client_order_id)).fetchone()
        return bool(r)
    except Exception as e:  # pragma: no cover - defensive
        log.warning("recovery: position-exists probe failed for %s/%s: %s",
                    session_id, symbol, e)
        # Fail SAFE for double-registration: if we can't tell, assume it EXISTS
        # (skip adoption) — never risk a duplicate real position.
        return True


def _adopt_orphan_entry_intents(session_id: str) -> List[Dict[str, Any]]:
    """SPRINT CLUSTER 9b ITEM 6 — adopt a broker-FILLED entry that has a durable
    order intent but NO position row (a crash between broker-accept and the
    position insert leaves a real, UNMANAGED, stop-less position).

    For each ENTRY intent (order_ledger.entry_intents) of a RUNNING session with NO
    matching OPEN position row (scoped per session + broker_account_id/profile):
      * query the broker by OUR recorded order-id / compact client-tag,
      * FILLED at the broker  → REGISTER the position at the REAL fill qty/price
                                (adopt) so the monitor + GTT backup pick it up,
      * still OPEN/working     → page (await; the next resume re-checks),
      * ABSENT / rejected AFTER a recorded ORDER_SUBMITTED → page (nothing to
                                adopt; a broker-accepted order vanished).
    A pure ORDER_CREATED intent with NO ORDER_SUBMITTED and NO tag-matched broker
    order was never accepted (rejected at fire) → skipped SILENTLY (not an orphan).

    LIVE only (paper registers immediately; there is no orphan). Idempotent (skips a
    resolved intent + any already-registered leg). Best-effort — never raises."""
    from .session import TradingSession
    from . import order_ledger, alerts

    sess = TradingSession.load(session_id)
    if sess is None or sess.dry_run:
        return []
    try:
        intents = order_ledger.entry_intents(session_id)
    except Exception as e:  # pragma: no cover - defensive
        log.warning("recovery: entry_intents failed for %s: %s", session_id, e)
        return []
    if not intents:
        return []
    try:
        sess._build_brokers()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("recovery: build_brokers failed (adopt) for %s: %s",
                    session_id, e)
        return []
    actions: List[Dict[str, Any]] = []
    profiles = {p.profile_id: p for p in (sess.config.broker_profiles or [])}
    entry_side = "SELL" if getattr(sess.config, "direction", "long") == "short" \
        else "BUY"
    for it in intents:
        coid = it.get("client_order_id")
        symbol = it.get("symbol")
        prof_id = it.get("broker_profile")
        if not (coid and symbol):
            continue
        # Idempotency: a resolved intent or an already-registered leg is skipped.
        if order_ledger.entry_intent_resolved(session_id, coid):
            continue
        if _position_exists_for_intent(session_id, symbol, prof_id, coid):
            continue
        prof = profiles.get(prof_id)
        broker = sess.brokers.get(prof_id) if prof_id else None
        if broker is None:
            log.warning("recovery: adopt %s/%s — no broker for profile %r; skip",
                        session_id, symbol, prof_id)
            continue
        try:
            outcome = _resolve_and_adopt_one(sess, broker, prof, it,
                                             entry_side, order_ledger, alerts)
        except Exception as e:  # one intent must not abort the rest
            log.warning("recovery: adopt of %s/%s failed: %s",
                        session_id, symbol, e)
            outcome = "adopt_error"
        actions.append({"symbol": symbol, "client_order_id": coid,
                        "outcome": outcome})
    if actions:
        log.info("recovery: orphan-intent scan for %s — %s", session_id,
                 [(a["symbol"], a["outcome"]) for a in actions])
    return actions


def _resolve_and_adopt_one(sess, broker, prof, intent, entry_side,
                           order_ledger, alerts) -> str:
    """Resolve ONE orphan entry intent against the broker and adopt/await/page.
    Returns an outcome tag. Never raises (caller guards too)."""
    coid = intent["client_order_id"]
    symbol = intent["symbol"]
    prof_id = intent.get("broker_profile")
    broker_oid = intent.get("broker_order_id")
    tag = order_ledger.compact_tag(coid)

    # 1. Locate OUR order at the broker: by recorded order-id (submitted) first,
    #    else by our compact tag in the orderbook.
    order: Optional[Dict[str, Any]] = None
    if broker_oid:
        try:
            st = broker.get_order_status(str(broker_oid))
            if isinstance(st, dict) and st:
                order = dict(st)
                order.setdefault("order_id", str(broker_oid))
        except Exception as e:  # pragma: no cover - defensive
            log.warning("recovery: get_order_status(%s) failed: %s", broker_oid, e)
    if order is None:
        try:
            ob = broker.get_orders()
        except Exception:  # pragma: no cover - defensive
            ob = None
        matched = order_ledger.find_broker_order_by_tag(ob, tag,
                                                        closing_side=entry_side)
        if matched is not None:
            order = dict(matched)
            broker_oid = broker_oid or matched.get("order_id")

    # A pure ORDER_CREATED intent never accepted at the broker (no submitted id AND
    # no tag match) → the order was rejected/failed at fire; NOT an orphan. Skip.
    if order is None:
        if not intent.get("submitted"):
            return "skip_never_accepted"
        # Recorded as submitted but now ABSENT at the broker → genuine anomaly.
        _page_orphan(alerts, sess.session_id, symbol,
                     f"entry order {broker_oid} (client {coid}) was accepted but is "
                     f"ABSENT at the broker on recovery — nothing to adopt; verify "
                     f"the account manually.")
        order_ledger.append_event(
            session_id=sess.session_id, symbol=symbol,
            event_type=order_ledger.EV_REJECTED, position_ref=None,
            broker_order_id=broker_oid, client_order_id=coid,
            broker_profile=prof_id, source="recovery",
            detail="orphan entry intent unresolved — absent at broker on recovery")
        return "paged_absent"

    status = str(order.get("status") or "").upper()
    filled = int(order.get("filled_quantity") or order.get("filled_qty") or 0)
    avg_price = float(order.get("average_price") or order.get("avg_price") or 0.0)

    # 2. FILLED → adopt (register at the real fill).
    # CLUSTER 9c FIX F2 (2026-07-11): adopt ANY TERMINAL order that left a real
    # fill — not only COMPLETE. A partial-fill-then-CANCELLED / -REJECTED entry
    # bought real shares (filled>0 @ a real avg) but the order's terminal status is
    # CANCELLED/REJECTED; the old COMPLETE-only guard paged "nothing to adopt" and
    # left a REAL unmanaged position (no stop/GTT, invisible to the kill switch).
    # A still-WORKING order (OPEN/PENDING) with a partial fill is intentionally NOT
    # adopted here (it may fill more) — it falls through to the await path (§4).
    _TERMINAL = ("COMPLETE", "CANCELLED", "REJECTED")
    if status in _TERMINAL and filled > 0 and avg_price > 0:
        instrument_type = getattr(prof, "instrument_type", None) or \
            getattr(sess.config, "instrument_type", "EQ")
        product = intent.get("product") or getattr(prof, "order_product", None) \
            or sess.config.order_product
        exchange = "NFO" if str(instrument_type).upper() == "FUT" else "NSE"
        acct_id = (getattr(prof, "broker_account_id", None)
                   if prof is not None else None) or sess.broker_account_id
        direction = getattr(sess.config, "direction", "long")
        sess.registry.register(
            symbol=symbol, broker_profile=prof_id, qty=filled,
            avg_price=avg_price, product=product,
            instrument_type=instrument_type, exchange=exchange,
            broker_account_id=acct_id, direction=direction,
            entry_order_id=broker_oid, client_order_id=coid)
        log.critical("recovery: ADOPTED orphan entry %s/%s — %d @ %.2f (order %s, "
                     "status=%s) registered + now managed", sess.session_id, symbol,
                     filled, avg_price, broker_oid, status)
        _partial = "" if status == "COMPLETE" else \
            f" (PARTIAL fill on a {status} order)"
        _page_orphan(alerts, sess.session_id, symbol,
                     f"adopted an orphan FILLED entry ({filled} @ {avg_price:.2f})"
                     f"{_partial} that had no position row — now managed with a "
                     f"stop/GTT.", kind="ORPHAN_ENTRY_ADOPTED")
        return "adopted_filled"

    # 3. Rejected / cancelled with NO fill (created-never-accepted / zero-fill) →
    #    nothing to adopt; page. (A partial-fill terminal order was already adopted
    #    in §2 above.)
    if status in ("REJECTED", "CANCELLED"):
        _page_orphan(alerts, sess.session_id, symbol,
                     f"entry order {broker_oid} (client {coid}) is {status} at the "
                     f"broker — no position to adopt.")
        order_ledger.append_event(
            session_id=sess.session_id, symbol=symbol,
            event_type=order_ledger.EV_REJECTED, position_ref=None,
            broker_order_id=broker_oid, client_order_id=coid,
            broker_profile=prof_id, source="recovery",
            detail=f"orphan entry intent {status} at broker on recovery")
        return "paged_rejected"

    # 4. Still working (OPEN / PENDING / partial-not-complete) → await + page soft.
    _page_orphan(alerts, sess.session_id, symbol,
                 f"entry order {broker_oid} (client {coid}) is still WORKING "
                 f"(status={status or 'unknown'}, filled={filled}) on recovery — "
                 f"awaiting fill; will re-check on the next resume.",
                 kind="ORPHAN_ENTRY_WORKING")
    return "await_working"


def _page_orphan(alerts, session_id: str, symbol: str, detail: str,
                 kind: str = "ORPHAN_ENTRY_UNMANAGED") -> None:
    """Deduped urgent page for an orphan-entry event. Never raises."""
    try:
        alerts.send_urgent_deduped(kind=kind, session_id=session_id,
                                   symbol=symbol, detail=detail)
    except Exception as e:  # pragma: no cover - paging must never block recovery
        log.debug("recovery: orphan page failed for %s/%s: %s",
                  session_id, symbol, e)


def _reconcile_broker_positions(session_id: str) -> int:
    """AUTHORITATIVE broker→DB reconcile on resume: correct any position the
    broker closed (RMS auto-square / manual exit / missed GTT fill) OR whose qty
    diverged while we were down, immediately on restart — BEFORE the tick driver
    re-arms and marks to (stale) market.

    LIVE only (paper no-ops inside reconcile_broker_positions). Best-effort:
    builds the session + brokers, runs one net-book reconcile pass, never raises.
    Returns the number of actions taken (0 in paper / when the broker is
    unreachable — an unreachable book NEVER mutates the DB). DATA-ISOLATION: writes
    only autotrade_positions / autotrade_sessions.
    """
    from .session import TradingSession
    from .monitoring.position_reconciler import reconcile_broker_positions

    sess = TradingSession.load(session_id)
    if sess is None:
        return 0
    try:
        sess._build_brokers()
        actions = reconcile_broker_positions(sess)
        if actions:
            log.info("recovery: broker reconcile for %s — %d action(s): %s",
                     session_id, len(actions),
                     [a.get("action") for a in actions])
        return len(actions)
    except Exception as e:  # never block recovery on the reconcile pass
        log.warning("recovery: broker position reconcile failed for %s: %s",
                    session_id, e)
        return 0


def _rearm_square_off(session_id: str) -> None:
    """Re-arm the square-off scheduler for a resumed RUNNING session, using the
    SAME MIS-aware target selection as the fire path (`_arm_square_off`).

    BUG THIS FIXES (2026-07-06): this used only `square_off_time`, so on a restart
    a MIS intraday session was re-armed at 15:29 instead of its `mis_square_off_time`
    (15:12) defensive flatten. Zerodha's RMS then force-squared the position at
    ~15:20 and OUR 15:29 order was rejected ("Intraday orders (MIS) are allowed
    only till 3.25 PM") — leaving EXIT_FAILED rows with stale P&L. `_arm_square_off`
    arms at the EARLIER of square_off_time (intraday_basket) and mis_square_off_time
    (any MIS product), skips positional (square_off_enabled False) and kill-switch
    sessions, and no-ops when the time already passed (the in-tick square-off is
    the backstop). Delegating keeps ONE source of truth for the square-off target.

    The MULTI-SESSION MAX-HOLD CAP (max_hold_sessions>0) needs NO re-arm here: it
    recomputes the cap datetime from the PERSISTED started_at every tick, so once
    the tick driver is re-armed it fires on the Nth trading session across restarts.
    """
    from .session import TradingSession

    sess = TradingSession.load(session_id)
    if sess is None:
        return
    try:
        sess._arm_square_off()
    except Exception as e:  # never block recovery on the backup square-off
        log.warning("recovery: re-arm square-off failed for %s: %s", session_id, e)


def _resume_running(session_id: str) -> str:
    """Re-arm the tick + WS drivers for a RUNNING session, and backfill any
    missing per-position GTTs (live only). Returns an outcome tag."""
    # CLUSTER 9b ITEM 6: adopt any broker-FILLED entry that has a durable order
    # intent but NO position row (crash between broker-accept and the position
    # insert) BEFORE the GTT backfill — so an adopted position also gets its GTT
    # backup + reconcile pass this cycle. Best-effort, LIVE only, never blocks.
    try:
        _adopt_orphan_entry_intents(session_id)
    except Exception as e:  # pragma: no cover - never block recovery
        log.warning("recovery: orphan-intent adoption failed for %s: %s",
                    session_id, e)
    # FEATURE 1/3: retroactively place the broker GTT backup on live positions
    # that pre-date this feature, BEFORE re-arming the drivers.
    _backfill_live_gtts(session_id)
    # AUTHORITATIVE broker→DB reconcile: correct any position the broker closed /
    # resized while we were down BEFORE the tick driver marks a stale book to
    # market. Best-effort, LIVE only, never blocks recovery.
    try:
        _reconcile_broker_positions(session_id)
    except Exception as e:  # pragma: no cover - never block recovery
        log.warning("recovery: broker reconcile pass failed for %s: %s",
                    session_id, e)
    armed = tick_driver.start_for_session(session_id)
    # FEATURE 2: re-arm the sub-second WS-driven kill-switch path too.
    try:
        ws_driver.start_for_session(session_id)
    except Exception as e:  # pragma: no cover - never block recovery
        log.warning("recovery: ws driver start failed for %s: %s", session_id, e)
    # INTRADAY BASKET: re-arm the precise-time square-off scheduler so a resumed
    # session still flattens on the second at square_off_time. The trail state
    # (armed, peak) is already restored from the session row by load_trail_state,
    # so the trail continues mid-day. If square_off_time already passed while
    # down, the next tick squares the basket off (in-tick backstop). Best-effort.
    try:
        _rearm_square_off(session_id)
    except Exception as e:  # pragma: no cover - never block recovery
        log.warning("recovery: square-off re-arm failed for %s: %s",
                    session_id, e)
    # FALCON INTRADAY MAGNIFIER: a restart between the 09:15 and 09:16 legs would
    # otherwise leave the second leg unfired (its timer thread died) and the trail
    # dormant forever. Complete the split entry NOW (idempotent — a completed entry
    # is a no-op), so the remaining half fills, the basis freezes on the blended
    # cost, the SL-M/GTT backup is placed, and the trail arms. Best-effort.
    try:
        _complete_magnifier_if_pending(session_id)
    except Exception as e:  # pragma: no cover - never block recovery
        log.warning("recovery: magnifier completion failed for %s: %s",
                    session_id, e)
    # FALCON BTST OSCILLATOR: identical restart concern — a restart between the
    # 09:15 and 09:16 legs would leave the second leg unfired (its timer thread
    # died) and the basis unfrozen. Complete the split entry NOW (idempotent), so
    # the remaining half fills, the basis freezes on the blended cost, and the
    # per-position GTT backup is placed. Best-effort.
    try:
        _complete_btst_if_pending(session_id)
    except Exception as e:  # pragma: no cover - never block recovery
        log.warning("recovery: btst completion failed for %s: %s",
                    session_id, e)
    if armed:
        log.info("recovery: re-armed tick driver for RUNNING session %s", session_id)
        return "tick_rearmed"
    # Already running (or autostart disabled in tests) — idempotent no-op.
    log.info("recovery: tick driver already armed (or disabled) for %s", session_id)
    return "tick_already_armed"


def _complete_magnifier_if_pending(session_id: str) -> None:
    """If a resumed RUNNING session is a Falcon Intraday Magnifier whose split
    entry never completed (only the 09:15 leg is in), fire the second leg + arm
    the trail now. Idempotent (already-complete → no-op). LIVE places real orders;
    paper simulates. Never raises."""
    from .session import TradingSession

    sess = TradingSession.load(session_id)
    if sess is None:
        return
    if getattr(sess.config, "strategy", None) != "intraday_magnifier":
        return
    if sess._magnifier_entry_complete():
        return
    if not sess.brokers:
        sess._build_brokers()
    log.info("recovery: completing pending magnifier split entry for %s",
             session_id)
    asyncio.run(sess.complete_magnifier_entry())


def _complete_btst_if_pending(session_id: str) -> None:
    """If a resumed RUNNING session is a Falcon BTST Oscillator whose split entry
    never completed (only the 09:15 leg is in), fire the second leg + freeze the
    basis now. Idempotent (already-complete → no-op). LIVE places real orders;
    paper simulates. Never raises."""
    from .session import TradingSession

    sess = TradingSession.load(session_id)
    if sess is None:
        return
    if getattr(sess.config, "strategy", None) != "btst_oscillator":
        return
    if sess._magnifier_entry_complete():
        return
    if not sess.brokers:
        sess._build_brokers()
    log.info("recovery: completing pending btst split entry for %s", session_id)
    asyncio.run(sess.complete_btst_entry())


def _resume_killing(session_id: str, status: str) -> str:
    """CLUSTER 3 ITEM 1 — resume a session stranded in KILLING / KILLING_INCOMPLETE.

    A crash MID-FLATTEN (kill_switch.fire sets KILLING before it flattens, and
    KILLING_INCOMPLETE when a leg fails) leaves the session with NO driver: some
    legs OPEN (never got an exit attempt) and/or EXIT_FAILED (a placement failed).
    Nothing re-attempts them → real exposure sits unmanaged. This drives it toward
    CLOSED, safely:

      1. AUTHORITATIVE broker→DB reconcile FIRST — correct any leg the broker
         already closed (RMS square / manual / GTT fill) while we were down, so we
         never re-exit a position that is already flat.
      2. Re-flatten every STILL-OPEN leg via the per-leg exit path (the same
         _exit_single_position the sweep uses). The pre-exit net-probe guard +
         working-exit netting + the CLUSTER-3 query-before-place adoption prevent
         any double order (a leg whose exit was already placed pre-crash is
         ADOPTED, not re-placed). Each OPEN leg → CLOSED or EXIT_FAILED.
      3. Re-arm the tick driver so its EXIT_FAILED retry sweep re-attempts the
         stranded EXIT_FAILED legs and PROMOTES the session to CLOSED once every
         leg is confirmed flat (tick() already services KILLING_INCOMPLETE).

    Normalises a bare KILLING to KILLING_INCOMPLETE so the tick driver keeps
    servicing it (the driver loop runs for RUNNING + KILLING_INCOMPLETE). LIVE
    only for real orders; paper places none. Best-effort throughout — never raises.
    """
    from .session import TradingSession

    sess = TradingSession.load(session_id)
    if sess is None:
        log.warning("recovery: %s session %s not found — skipping", status,
                    session_id)
        return "killing_missing"
    try:
        sess._build_brokers()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("recovery: build_brokers failed for %s (%s): %s",
                    status, session_id, e)
    # 1. Authoritative reconcile FIRST (correct broker-closed legs).
    try:
        _reconcile_broker_positions(session_id)
    except Exception as e:  # pragma: no cover - never block recovery
        log.warning("recovery: kill-resume reconcile failed for %s: %s",
                    session_id, e)

    # A bare KILLING is a crash BEFORE the flatten finished; normalise it to the
    # NON-terminal KILLING_INCOMPLETE so the tick driver's servicing loop keeps
    # running it toward CLOSED (a truly all-flat session is promoted below).
    try:
        if sess._current_status() == "KILLING":
            sess._set_status("KILLING_INCOMPLETE")
    except Exception as e:  # pragma: no cover - defensive
        log.debug("recovery: could not normalise KILLING for %s: %s",
                  session_id, e)

    # 2. Re-flatten still-OPEN legs (the sweep only re-attempts EXIT_FAILED, so an
    #    OPEN leg left by the interrupted kill would otherwise sit forever).
    try:
        open_positions = sess.registry.get_open_positions()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("recovery: get_open_positions failed for %s: %s",
                    session_id, e)
        open_positions = []
    reflattened = 0
    if open_positions:
        from .session import _exit_single_position
        for pos in open_positions:
            try:
                asyncio.run(_exit_single_position(
                    session_id=session_id, position=pos,
                    reason="KILL_SWITCH", brokers=sess.brokers,
                    registry=sess.registry, gtt_manager=sess.gtt_manager,
                    kite_product=sess.config.order_product,
                    exec_cfg=sess.config))
                reflattened += 1
            except Exception as e:  # one bad leg must not abort the rest
                log.warning("recovery: kill-resume re-flatten failed %s/%s: %s",
                            session_id, pos.get("symbol"), e)

    # 3. Re-arm the tick driver so the EXIT_FAILED sweep drives the rest to CLOSED.
    armed = tick_driver.start_for_session(session_id)

    # If, after the reconcile + re-flatten, every leg is already flat, promote to
    # CLOSED now (the tick() promotion path would do this on its next iteration,
    # but doing it here means a session with no residual EXIT_FAILED closes cleanly
    # even if the background driver is disabled, e.g. in tests).
    try:
        if (sess._current_status() == "KILLING_INCOMPLETE"
                and not sess._has_unflat_positions()):
            from datetime import datetime as _dt
            sess._set_status("CLOSED", closed_at=_dt.now(IST).isoformat())
            log.critical("recovery: KILLING resolved on resume — all legs flat, "
                         "session CLOSED %s", session_id)
    except Exception as e:  # pragma: no cover - defensive
        log.debug("recovery: post-resume CLOSED promotion failed for %s: %s",
                  session_id, e)

    log.info("recovery: kill-resume %s — reflattened=%d tick_armed=%s",
             session_id, reflattened, armed)
    return "killing_resumed"


def _resume_scheduled(session_id: str) -> str:
    """Re-arm the entry scheduler for a SCHEDULED session, OR fire it now if its
    target already passed while the backend was down — BUT ONLY THROUGH THE
    TRADING-DAY / MARKET-OPEN GATE. A session that comes back up on a weekend /
    holiday / after-hours / with a missed window does NOT fire: _fire_entries()
    re-checks the gate and sets a terminal/deferred state (or carries per
    on_missed_window). Returns an outcome tag.
    """
    # Defer the heavy import so module import stays cheap and circular-safe.
    from .session import TradingSession, evaluate_fire_gate, now_ist

    sess = TradingSession.load(session_id)
    if sess is None:
        log.warning("recovery: SCHEDULED session %s not found — skipping", session_id)
        return "scheduled_missing"

    now = now_ist()
    try:
        target = sess.config.resolve_fire_datetime(now)
    except ValueError as e:
        # Unparseable entry_time → safe refusal (never fire blind). _fire_entries
        # re-evaluates the gate and sets the terminal state.
        log.warning("recovery: SCHEDULED session %s unparseable entry_time "
                    "(%s) — gating", session_id, e)
        asyncio.run(sess._fire_entries())
        return "scheduled_gated_unparseable"

    gate = evaluate_fire_gate(sess.config, now, fire_dt=target)
    if gate.allow:
        # Target is now-or-just-past AND market is open within grace → fire (the
        # gate is re-checked inside _fire_entries too — defence in depth).
        log.info("recovery: SCHEDULED session %s in fire window — firing now",
                 session_id)
        asyncio.run(sess._fire_entries())
        return "scheduled_fired_inwindow"

    if gate.status in ("SCHEDULED", "DEFERRED_MARKET_CLOSED"):
        # Future trading day (or before-the-bell) → re-arm for the resolved
        # target. Place NOTHING.
        armed = entry_scheduler.start_for_session(
            session_id, gate.fire_dt, now_fn=now_ist)
        seconds = int(max(0.0, (gate.fire_dt - now).total_seconds()))
        if armed:
            log.info("recovery: re-armed entry scheduler for %s — fires at %s "
                     "(in %ss)", session_id, gate.fire_dt.isoformat(), seconds)
            return "scheduled_rearmed"
        log.info("recovery: entry scheduler already armed for %s", session_id)
        return "scheduled_already_armed"

    # Missed window / non-trading-day → refuse (expire or carry per policy).
    # Run the same refusal path the start path uses (sets status / carries).
    sess._refuse_fire(gate, when="recovery")
    log.warning("recovery: SCHEDULED session %s NOT fired (%s): %s",
                session_id, gate.status, gate.reason)
    return "scheduled_gated_missed"


def resume_active_sessions() -> Dict[str, Any]:
    """Scan autotrade_sessions and re-arm in-memory threads lost to a restart.

    Idempotent + safe when there are no active sessions. Returns a summary dict
    (counts + per-session outcomes) and logs a one-line summary. Never raises:
    each per-session step is individually guarded so one bad row can't abort the
    rest of recovery.
    """
    summary: Dict[str, Any] = {
        "running": 0, "scheduled": 0, "killing": 0, "fired": 0, "rearmed": 0,
        "errors": 0, "sessions": [],
    }
    try:
        sessions = _active_sessions()
    except Exception as e:  # pragma: no cover - DB unavailable at boot
        log.exception("recovery: failed to query active sessions: %s", e)
        return summary

    if not sessions:
        log.info("recovery: no active AutoTrade sessions to resume.")
        return summary

    for s in sessions:
        sid = s["session_id"]
        status = s["status"]
        try:
            if status == "RUNNING":
                outcome = _resume_running(sid)
                summary["running"] += 1
                if outcome == "tick_rearmed":
                    summary["rearmed"] += 1
            elif status == "SCHEDULED":
                outcome = _resume_scheduled(sid)
                summary["scheduled"] += 1
                if "fired" in outcome:
                    summary["fired"] += 1
                elif "rearmed" in outcome:
                    summary["rearmed"] += 1
            elif status in ("KILLING", "KILLING_INCOMPLETE"):
                # CLUSTER 3 ITEM 1 — resume a stranded mid-flatten kill.
                outcome = _resume_killing(sid, status)
                summary["killing"] = summary.get("killing", 0) + 1
                if outcome == "killing_resumed":
                    summary["rearmed"] += 1
            else:  # pragma: no cover - filtered by the query above
                outcome = "skipped"
            summary["sessions"].append({"session_id": sid, "status": status,
                                        "outcome": outcome})
        except Exception as e:  # one bad session must not abort the rest
            log.exception("recovery: failed to resume session %s (%s): %s",
                          sid, status, e)
            summary["errors"] += 1
            summary["sessions"].append({"session_id": sid, "status": status,
                                        "outcome": "error", "error": str(e)})

    log.info("recovery: resumed AutoTrade sessions — running=%d scheduled=%d "
             "fired=%d rearmed=%d errors=%d",
             summary["running"], summary["scheduled"], summary["fired"],
             summary["rearmed"], summary["errors"])
    return summary
