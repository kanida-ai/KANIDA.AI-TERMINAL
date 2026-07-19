"""Unified alert dispatcher — a REAL notifier (SPRINT CLUSTER 6).

Was a log-only stub. Now every alert:
  (a) PERSISTS a durable record in `autotrade_alerts` (id, ts, severity, kind,
      session_id, symbol, detail, incident_id, acknowledged, escalated), and
  (b) DISPATCHES a push via the existing web_push Layer-2 path — BEST-EFFORT: a
      push-transport failure NEVER raises into the trading path (it is logged and
      the record is still persisted). The log line is always kept.

send_urgent() is what the kill switch / the money-losing wires use for
MANUAL-EXIT-REQUIRED + naked/divergence pages. send() is the informational path
(persists + logs; pushes only for critical/urgent severity).

DEDUP: send_urgent_deduped() collapses a repeating incident (per
(kind, session, symbol) within a window) to ONE page, so a condition that is true
every ~5s tick does not page every tick — mirrors the recon-alert per-day dedup.

ACK + ESCALATION: acknowledge(id) marks a record acked. maybe_escalate()
re-pushes any UNACKED urgent alert older than a configurable threshold and flags
it escalated (throttled so it is cheap to call every tick).

TRANSPORT (INJECTABLE): the real push is
`power_user.services.web_push.notify_playwright_broken` — the SAME Layer-2 path
`scripts/workflow_watchdog.py` already reuses. It is abstracted behind an
injectable transport so tests inject a fake and the real path stays a one-liner.
ASSUMED web_push API (verified against power_user/services/web_push.py):
  notify_playwright_broken(*, failure_class: str, hint: str) -> dict
      returns {"sent": int, "failed": int, ...}; when VAPID is unconfigured OR no
      subscriptions exist it returns {"sent": 0, ..., "skipped": True} WITHOUT
      touching the network — i.e. it no-ops gracefully (so an unconfigured test /
      paper backend never breaks the trading path).
  is_configured() -> bool

DATA-ISOLATION: writes ONLY autotrade_alerts. NEVER touches falcon_position_state
or the legacy path. Paper/dry_run: the trading path is unaffected — an alert row
is additive observability, and the push no-ops when web_push is unconfigured.
"""
from __future__ import annotations

import logging
import os
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("kanida.autotrade.alerts")
IST = timezone(timedelta(hours=5, minutes=30))


# ── Injectable push transport ─────────────────────────────────────────────────
# A transport is a callable dispatch(*, title, body, kind, severity) -> Any.
# Default = the real web_push path; tests inject a fake via set_transport().
Transport = Callable[..., Any]
_TRANSPORT: Optional[Transport] = None
_TLOCK = threading.Lock()


def set_transport(fn: Optional[Transport]) -> None:
    """Inject a push transport (tests) or clear it (None → real web_push)."""
    global _TRANSPORT
    with _TLOCK:
        _TRANSPORT = fn


def _default_transport(*, title: str, body: str, kind: str,
                       severity: str) -> Dict[str, Any]:
    """Real push via the existing web_push Layer-2 path (the one the workflow
    watchdog reuses). A one-liner around notify_playwright_broken — see the module
    docstring for the assumed API. Never raises (the caller wraps it, but this
    stays defensive)."""
    from power_user.services.web_push import notify_playwright_broken  # noqa: WPS433
    return notify_playwright_broken(
        failure_class=f"AUTOTRADE_{kind}",
        hint=f"{title} — {body}"[:240])


def _get_transport() -> Transport:
    with _TLOCK:
        return _TRANSPORT or _default_transport


# ── Config (env, all default-safe) ────────────────────────────────────────────
def _int_env(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default)).strip()))
    except (ValueError, AttributeError):
        return default


def _escalate_threshold_sec() -> int:
    """Age (s) after which an UNACKED urgent alert is escalated. 0 disables."""
    return _int_env("FALCON_AUTOTRADE_ALERT_ESCALATE_SEC", 300)


def _dedup_window_sec() -> int:
    """Window (s) within which the SAME incident pages at most once. 0 disables."""
    return _int_env("FALCON_AUTOTRADE_ALERT_DEDUP_SEC", 900)


# ── ALERT AUTO-TRIAGE master flag (DEFAULT OFF) ───────────────────────────────
def auto_triage_enabled() -> bool:
    """The ONE master gate for the whole ALERT AUTO-TRIAGE layer (Tier 1 evidence-
    gated auto-resolve, Tier 2 detector-level suppression, Tier 3 the
    RECONCILED_FLAT-without-exit-order detector).

    DEFAULT OFF. While this is off EVERY triage code path early-returns before it
    can read a row, write an ack, suppress a detector, or raise a page — i.e. the
    system is byte-for-byte identical to the pre-triage behaviour. The operator
    opts in with FALCON_AUTOTRADE_AUTO_TRIAGE=true.

    NOTE FOR THE OPERATOR: the Tier-3 RECONCILED_FLAT+exit_order_id-IS-NULL
    detector (the one that would have caught the 2026-07-15 ₹8.3L MAPMYINDIA
    event in real time) is behind this SAME flag, because flag-OFF must be
    byte-identical. Turning the flag on is what arms it."""
    return os.environ.get("FALCON_AUTOTRADE_AUTO_TRIAGE", "off").strip().lower() \
        in ("on", "1", "true", "yes")


def _now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


def _incident_id(kind: str, session_id: Optional[str],
                 symbol: Optional[str], extra: Optional[str] = None) -> str:
    """Stable dedup key for an incident: (kind, session, symbol[, extra]).

    `extra` (default None → byte-for-byte the old 3-part key) lets a caller add a
    stable discriminator so ONE logical suspicion pages once. It is used for
    CORP_ACTION_SUSPECTED to fold the detected split/bonus RATIO into the key, so a
    persistent 2× divergence collapses to a single incident instead of re-paging
    every reconcile cycle, while a genuinely NEW action at a DIFFERENT ratio is a
    distinct incident that still surfaces."""
    base = f"{kind}|{session_id or ''}|{symbol or ''}"
    return f"{base}|{extra}" if extra else base


# ── Persistence (best-effort; never raises into the trading path) ─────────────
def _persist(*, ts: str, severity: str, kind: str, session_id: Optional[str],
             symbol: Optional[str], detail: str, incident_id: Optional[str],
             pushed: bool, push_result: str) -> Optional[int]:
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            cur = con.execute(
                """INSERT INTO autotrade_alerts
                   (ts, incident_id, severity, kind, session_id, symbol, detail,
                    acknowledged, escalated, pushed, push_result)
                   VALUES (?,?,?,?,?,?,?,0,0,?,?)""",
                (ts, incident_id, severity, kind, session_id, symbol,
                 (detail or "")[:1000], 1 if pushed else 0,
                 (push_result or "")[:250]))
            con.commit()
            return cur.lastrowid
    except Exception as e:  # noqa: BLE001 — persist must never crash a caller
        log.warning("alerts: persist failed (kind=%s): %s", kind, e)
        return None


def _dispatch_push(*, title: str, body: str, kind: str,
                   severity: str) -> tuple[bool, str]:
    """Best-effort push. Returns (pushed, result_str). NEVER raises — a transport
    failure is logged; the alert record is still persisted by the caller."""
    try:
        res = _get_transport()(title=title, body=body, kind=kind,
                               severity=severity)
    except Exception as e:  # noqa: BLE001 — a push failure never breaks trading
        log.warning("alerts: push transport failed (kind=%s) — persisted only: %s",
                    kind, e)
        return False, f"transport_error: {type(e).__name__}"
    # A dict with sent>=1 is a real delivery; a skipped/unconfigured push (sent=0)
    # is a non-raising no-op (VAPID unset / no subscriptions) — still recorded.
    pushed = bool(
        (isinstance(res, dict) and int(res.get("sent") or 0) >= 1) or res is True)
    return pushed, (str(res)[:250] if res is not None else "")


# ── Public API ────────────────────────────────────────────────────────────────
def send(message: str, severity: str = "info", *, kind: str = "GENERIC",
         session_id: Optional[str] = None, symbol: Optional[str] = None,
         incident_id: Optional[str] = None,
         push: Optional[bool] = None) -> Optional[int]:
    """Log + persist an alert record, and (per severity/`push`) dispatch a push.

    Backward-compatible: send(msg) / send(msg, severity="warn") behave as before
    for the log, and now ALSO persist a record. A push is dispatched only when
    `push` is True, or (when push is None) severity is 'critical'/'urgent' — so
    the informational ladder alert stays non-paging. Returns the new alert id."""
    level = {"info": logging.INFO, "warn": logging.WARNING,
             "warning": logging.WARNING, "critical": logging.CRITICAL,
             "urgent": logging.CRITICAL}.get(severity, logging.INFO)
    log.log(level, "[ALERT:%s] %s", severity.upper(), message)
    do_push = push if push is not None else severity in ("critical", "urgent")
    pushed, result = (False, "")
    if do_push:
        pushed, result = _dispatch_push(
            title=f"AutoTrade {kind}", body=message, kind=kind, severity=severity)
    return _persist(ts=_now_ist_iso(), severity=severity, kind=kind,
                    session_id=session_id, symbol=symbol, detail=message,
                    incident_id=incident_id, pushed=pushed, push_result=result)


def send_urgent(message: str, *, kind: str = "MANUAL_EXIT_REQUIRED",
                session_id: Optional[str] = None, symbol: Optional[str] = None,
                incident_id: Optional[str] = None) -> Optional[int]:
    """URGENT alert — always dispatches a push AND persists an unacked record.
    Backward-compatible with the old log-only send_urgent(str) callers (kill
    switch MANUAL-EXIT-REQUIRED escalations)."""
    return send(message, severity="urgent", kind=kind, session_id=session_id,
                symbol=symbol, incident_id=incident_id, push=True)


def _recent_incident_exists(incident_id: str, window_sec: int,
                            now: Optional[datetime] = None) -> bool:
    """True when an alert with this incident_id was recorded within window_sec.
    Fail-OPEN (returns False) on any error — better a duplicate page than a
    missed one."""
    if not incident_id or window_sec <= 0:
        return False
    now = now or datetime.now(IST)
    cutoff = (now - timedelta(seconds=window_sec)).isoformat()
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            row = con.execute(
                "SELECT 1 FROM autotrade_alerts WHERE incident_id=? AND ts>=? "
                "LIMIT 1", (incident_id, cutoff)).fetchone()
            return bool(row)
    except Exception as e:  # noqa: BLE001
        log.debug("alerts: dedup check failed (%s): %s", incident_id, e)
        return False


def _unacked_incident_exists(incident_id: str) -> bool:
    """True when an UNACKED alert with this incident_id already exists (any age).

    Fail-CLOSED-to-page (returns False on error) — a DB glitch must never silence a
    genuine page. Used only by callers that pass suppress_if_unacked=True (currently
    the CORP_ACTION_SUSPECTED wire): while the operator has an open, un-acked
    suspicion for the SAME (symbol, ratio), re-detecting it every ~5s reconcile must
    not open a fresh page — it is the same, still-open incident. Acking it (the
    'clear') re-arms a future page."""
    if not incident_id:
        return False
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            row = con.execute(
                "SELECT 1 FROM autotrade_alerts WHERE incident_id=? "
                "AND acknowledged=0 LIMIT 1", (incident_id,)).fetchone()
            return bool(row)
    except Exception as e:  # noqa: BLE001
        log.debug("alerts: unacked-incident check failed (%s): %s", incident_id, e)
        return False


def send_urgent_deduped(*, kind: str, session_id: Optional[str],
                        symbol: Optional[str], detail: str,
                        window_sec: Optional[int] = None,
                        now: Optional[datetime] = None,
                        dedup_extra: Optional[str] = None,
                        suppress_if_unacked: bool = False) -> Optional[int]:
    """Page ONCE per (kind, session, symbol[, dedup_extra]) within the dedup window.
    Returns the new alert id, or None when suppressed as a duplicate (already paged
    in-window). This is the primitive every money-losing wire uses so one incident
    doesn't page on every tick.

    `dedup_extra` (default None → the classic 3-part key, unchanged) adds a stable
    discriminator to the incident key. `suppress_if_unacked` (default False →
    unchanged) additionally suppresses while an UN-ACKED alert with the same
    incident already exists — i.e. page once and stay quiet until the operator
    acknowledges (clears) it, then re-arm. Both default OFF, so every existing
    caller is byte-for-byte identical."""
    incident_id = _incident_id(kind, session_id, symbol, dedup_extra)
    win = _dedup_window_sec() if window_sec is None else window_sec
    if _recent_incident_exists(incident_id, win, now=now):
        log.debug("alerts: deduped %s (already paged in window)", incident_id)
        return None
    if suppress_if_unacked and _unacked_incident_exists(incident_id):
        log.debug("alerts: deduped %s (open unacked incident)", incident_id)
        return None
    return send_urgent(detail, kind=kind, session_id=session_id, symbol=symbol,
                       incident_id=incident_id)


def acknowledge(alert_id: int, reason: Optional[str] = None, *,
                auto_resolved: bool = False) -> bool:
    """Mark an alert acknowledged. Returns True when a row was updated.

    NEVER deletes — an acknowledged alert stays in autotrade_alerts forever; ack
    only flips a flag. Backward-compatible: acknowledge(id) with no reason behaves
    exactly as before (ack_reason stays NULL, auto_resolved stays 0) — the operator
    ack path is untouched.

    `reason` records the AUDIT REASON (mandatory in practice for the AUTO path:
    every auto-ack states the EVIDENCE that resolved it). `auto_resolved=True`
    marks the row as machine-acked so an operator can always tell an automated
    resolution from their own, and audit / reverse it.

    Tolerates a pre-migration table (no ack_reason/auto_resolved columns): falls
    back to the plain flag-only UPDATE so an ack can never fail on schema drift."""
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            try:
                cur = con.execute(
                    "UPDATE autotrade_alerts SET acknowledged=1, ack_reason=?, "
                    "auto_resolved=? WHERE id=?",
                    ((reason or "")[:1000] or None, 1 if auto_resolved else 0,
                     alert_id))
            except Exception:  # noqa: BLE001 — pre-migration schema fallback
                cur = con.execute(
                    "UPDATE autotrade_alerts SET acknowledged=1 WHERE id=?",
                    (alert_id,))
            con.commit()
            return cur.rowcount > 0
    except Exception as e:  # noqa: BLE001
        log.warning("alerts: ack failed for %s: %s", alert_id, e)
        return False


def unacked_alerts(kinds: Optional[List[str]] = None,
                   lookback_sec: Optional[int] = None,
                   now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Every UNACKED alert (optionally restricted to `kinds`, optionally only
    those raised within `lookback_sec`), oldest first.

    Read-only helper for the auto-triage layer. Never raises → [] on error."""
    sql = "SELECT * FROM autotrade_alerts WHERE acknowledged=0"
    params: List[Any] = []
    if kinds:
        sql += f" AND kind IN ({','.join('?' * len(kinds))})"
        params.extend(kinds)
    if lookback_sec is not None and lookback_sec > 0:
        cutoff = ((now or datetime.now(IST))
                  - timedelta(seconds=lookback_sec)).isoformat()
        sql += " AND ts>=?"
        params.append(cutoff)
    sql += " ORDER BY id ASC"
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            return [dict(r) for r in con.execute(sql, tuple(params)).fetchall()]
    except Exception as e:  # noqa: BLE001
        log.debug("alerts: unacked_alerts scan failed: %s", e)
        return []


def escalate_stale_alerts(threshold_sec: Optional[int] = None,
                          now: Optional[datetime] = None) -> List[int]:
    """Re-push every UNACKED, not-yet-escalated urgent alert older than the
    escalation threshold, flag it escalated, and return the escalated ids. The row
    is flagged escalated after the (best-effort) push attempt regardless of the
    push outcome, so it can never loop. Never raises."""
    thr = _escalate_threshold_sec() if threshold_sec is None else int(threshold_sec)
    if thr <= 0:
        return []
    now = now or datetime.now(IST)
    cutoff = (now - timedelta(seconds=thr)).isoformat()
    escalated: List[int] = []
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT id, kind, session_id, symbol, detail, ts "
                "FROM autotrade_alerts WHERE severity='urgent' "
                "AND acknowledged=0 AND escalated=0 AND ts<=?",
                (cutoff,)).fetchall()
            rows = [dict(r) for r in rows]
    except Exception as e:  # noqa: BLE001
        log.warning("alerts: escalation scan failed: %s", e)
        return []
    for r in rows:
        _dispatch_push(
            title=f"ESCALATION AutoTrade {r['kind']}",
            body=f"UNACKED >{thr}s: {r.get('detail') or ''}",
            kind=f"ESCALATED_{r['kind']}", severity="urgent")
        try:
            from falcon.db import falcon_conn  # noqa: WPS433
            with falcon_conn() as con:
                con.execute(
                    "UPDATE autotrade_alerts SET escalated=1 WHERE id=?",
                    (r["id"],))
                con.commit()
            escalated.append(r["id"])
        except Exception as e:  # noqa: BLE001
            log.warning("alerts: could not flag escalated for %s: %s", r["id"], e)
    if escalated:
        log.critical("alerts: ESCALATED %d unacked urgent alert(s): %s",
                     len(escalated), escalated)
    return escalated


# Process-local throttle so maybe_escalate() is cheap to call every tick.
_LAST_ESCALATE_MONO = 0.0
_ESCALATE_THROTTLE_SEC = 30.0
_ESC_LOCK = threading.Lock()


def maybe_escalate(threshold_sec: Optional[int] = None,
                   now: Optional[datetime] = None) -> List[int]:
    """Throttled wrapper around escalate_stale_alerts() for the tick path — at
    most once per _ESCALATE_THROTTLE_SEC. Never raises.

    ALERT AUTO-TRIAGE (default-OFF) hooks in HERE, immediately BEFORE the
    escalation scan, for two reasons:
      1. This is the one already-wired per-tick alert entry point (the tick calls
         alerts.maybe_escalate() every cycle), so the triage layer needs NO change
         to session.py.
      2. ORDERING IS THE POINT: an alert that auto-triage can PROVE transient
         (self-healed, superseded) must be resolved BEFORE the escalator re-pages
         it. Running triage first is what stops noise from escalating.
    Fully guarded + flag-gated: with FALCON_AUTOTRADE_AUTO_TRIAGE off this is a
    single env read and maybe_escalate() is byte-for-byte unchanged."""
    global _LAST_ESCALATE_MONO
    with _ESC_LOCK:
        if _time.monotonic() - _LAST_ESCALATE_MONO < _ESCALATE_THROTTLE_SEC:
            return []
        _LAST_ESCALATE_MONO = _time.monotonic()
    if auto_triage_enabled():
        try:
            # Lazy import: alert_monitor imports THIS module at module level, so a
            # top-level import here would be circular. Nothing is imported at all
            # while the flag is off.
            from .monitoring import alert_monitor as _am  # noqa: WPS433
            _am.auto_triage(now=now)
        except Exception as e:  # noqa: BLE001 — triage must never crash a tick
            log.warning("alerts: auto_triage raised (%s) — ignored", e)
    try:
        return escalate_stale_alerts(threshold_sec=threshold_sec, now=now)
    except Exception as e:  # noqa: BLE001 — never crash a tick
        log.warning("alerts: maybe_escalate raised (%s) — ignored", e)
        return []
