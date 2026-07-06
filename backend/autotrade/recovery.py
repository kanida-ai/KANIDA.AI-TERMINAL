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
    """RUNNING + SCHEDULED sessions (the only states with live in-memory threads)."""
    with falcon_conn() as con:
        rows = con.execute(
            """SELECT session_id, status FROM autotrade_sessions
               WHERE status IN ('RUNNING', 'SCHEDULED')
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
    if armed:
        log.info("recovery: re-armed tick driver for RUNNING session %s", session_id)
        return "tick_rearmed"
    # Already running (or autostart disabled in tests) — idempotent no-op.
    log.info("recovery: tick driver already armed (or disabled) for %s", session_id)
    return "tick_already_armed"


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
        "running": 0, "scheduled": 0, "fired": 0, "rearmed": 0,
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
