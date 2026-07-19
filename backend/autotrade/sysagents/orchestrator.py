"""PlatformHealthOrchestrator — collect → correlate → severity → page.

The one component that sits over the 9 monitors. Each run:
  1. GATE. If the layer is not active (master flag off OR kill-switch on) it
     returns a "disabled" view immediately — NO monitor is even instantiated, so
     it is structurally impossible to read a subsystem or raise a page while off.
  2. COLLECT. Runs the 8 primary monitors (each via safe_observe, so one failing
     monitor can never crash the run), then the Agent-Watcher over their signals.
  3. CORRELATE. llm.correlate() → the LLM (sanitized signals only) with a
     deterministic fallback, so the layer NEVER goes blind if the LLM is down.
  4. SEVERITY. overall_status = worst signal status; incident severity from the
     correlation.
  5. PAGE. If the incident severity pages (ALERT/CRITICAL) AND paging is enabled,
     raise ONE deduped web_push via the existing alerts.py plumbing — one incident,
     one page (not one per raw signal).
  6. PERSIST + LOG. A snapshot + incident row (history/audit) and a structured
     log line for every signal + every page. Never a secret.

Phase 1 = OBSERVE + CORRELATE + PAGE. NO remediation. This module places no
order, touches no position, and never writes falcon_position_state.
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional

from . import db, flags
from . import llm as _llm
from .monitors import build_primary_monitors, AgentWatcherMonitor
from .signals import HealthSignal, Status, PAGING_STATUSES, worse

log = logging.getLogger("kanida.sysagents.orchestrator")

_PAGE_KIND = "PLATFORM_HEALTH"


def _incident_signature(signals: List[HealthSignal]) -> str:
    """Stable id for an incident = hash of the sorted (subsystem,status) of the
    non-OK/non-NA signals. The SAME set of failing subsystems ⇒ the SAME id, so
    the page dedups across runs while the incident persists, and a NEW failure
    shape is a NEW incident."""
    key = sorted(
        f"{s.subsystem}:{s.status}" for s in signals
        if s.status in (Status.WARN, Status.ALERT, Status.CRITICAL, Status.UNKNOWN))
    raw = "|".join(key) or "all-ok"
    return "ph_" + hashlib.sha1(raw.encode()).hexdigest()[:16]


def _overall_status(signals: List[HealthSignal]) -> str:
    overall = Status.OK
    for s in signals:
        overall = worse(overall, s.status)
    # If nothing real fired, but some monitors are UNKNOWN, reflect UNKNOWN only
    # when NOTHING is OK (all unknown/na) — otherwise OK dominates unknown.
    return overall


def collect_signals() -> List[HealthSignal]:
    """Run the 8 primary monitors + the Agent-Watcher. Read-only. Never raises."""
    signals: List[HealthSignal] = []
    for mon in build_primary_monitors():
        signals.append(mon.safe_observe())
    watcher = AgentWatcherMonitor()
    signals.append(watcher.safe_observe(context={"signals": list(signals)}))
    return signals


def _disabled_view() -> Dict[str, Any]:
    return {
        "enabled": False,
        "status": "disabled",
        "reason": ("SYSAGENTS_KILL_SWITCH on" if flags.agents_killed()
                   else "SYSAGENTS_ENABLED off"),
        "signals": [],
        "incident": None,
    }


def run_once(*, page: bool = True, persist: bool = True,
             now=None) -> Dict[str, Any]:
    """One full observe→correlate→(page)→persist cycle. Returns the health view.

    `page=False` (the endpoint's default) collects + correlates + persists but
    raises NO page — a GET must never page. `page=True` (the run-loop) pages a
    correlated incident once, deduped. Never raises."""
    if not flags.layer_active():
        return _disabled_view()

    signals = collect_signals()
    overall = _overall_status(signals)
    incident = _llm.correlate(signals)
    signature = _incident_signature(signals)

    # Structured audit log of every signal (no secrets — summaries + status only).
    for s in signals:
        log.info("sysagents.signal subsystem=%s status=%s summary=%s",
                 s.subsystem, s.status, s.summary)

    sev = str(incident.get("severity") or Status.OK).upper()
    impacted = incident.get("impacted") or []
    paged = False
    alert_id: Optional[int] = None

    should_page = (page and sev in PAGING_STATUSES and flags.paging_enabled())
    if should_page:
        alert_id = _page_incident(signature, sev, incident, overall)
        paged = alert_id is not None
        log.warning("sysagents.PAGE incident=%s severity=%s root=%s paged=%s "
                    "alert_id=%s", signature, sev,
                    incident.get("root_cause"), paged, alert_id)

    if persist:
        try:
            db.persist_incident(
                incident_id=signature, severity=sev,
                root_cause=str(incident.get("root_cause") or ""),
                summary=str(incident.get("summary") or ""),
                impacted=[str(x) for x in impacted],
                source=str(incident.get("source") or "deterministic"),
                paged=paged, alert_id=alert_id,
                detail={"overall_status": overall})
            db.persist_snapshot(
                overall_status=overall,
                signals=[s.to_dict() for s in signals],
                incident_id=signature)
        except Exception as e:  # noqa: BLE001 — persistence never breaks a run
            log.warning("sysagents: persist failed: %s", e)

    return {
        "enabled": True,
        "status": overall,
        "overall_status": overall,
        "incident": {
            "incident_id": signature,
            "severity": sev,
            "root_cause": incident.get("root_cause"),
            "summary": incident.get("summary"),
            "impacted": impacted,
            "source": incident.get("source"),
            "paged": paged,
            "alert_id": alert_id,
        },
        "signals": [s.to_dict() for s in signals],
    }


def _page_incident(signature: str, severity: str, incident: Dict[str, Any],
                   overall: str) -> Optional[int]:
    """Raise ONE deduped page for a correlated incident via the existing alerts.py
    web_push plumbing. Deduped per (kind, incident-signature) so a persistent
    incident pages at most once per dedup window. Never raises → None on failure.

    Only the sanitized root-cause + summary reach the page (no secrets — the
    monitors never put a secret in a signal, and the LLM path is fed sanitized
    signals). The page is the SAME best-effort channel the money-path pages use;
    a transport failure is logged and the incident is still persisted."""
    try:
        from .. import alerts
        detail = (f"[{severity}] Platform health incident ({overall}). "
                  f"Root: {incident.get('root_cause')}. "
                  f"{incident.get('summary')}")[:900]
        return alerts.send_urgent_deduped(
            kind=_PAGE_KIND, session_id=None, symbol=signature, detail=detail)
    except Exception as e:  # noqa: BLE001
        log.warning("sysagents: page dispatch failed: %s", e)
        return None


def current_view(*, refresh: bool = False) -> Dict[str, Any]:
    """The health view for the read-only endpoint. Disabled → a disabled view.
    refresh=True → a fresh collect (NO page). Else the last persisted snapshot, or
    a fresh non-paging collect when there is no history yet. Never raises."""
    if not flags.layer_active():
        return _disabled_view()
    if refresh:
        return run_once(page=False, persist=True)
    try:
        snap = db.latest_snapshot()
    except Exception:  # noqa: BLE001
        snap = None
    if snap is None:
        return run_once(page=False, persist=True)
    inc = snap.get("incident") or {}
    return {
        "enabled": True,
        "status": snap.get("overall_status"),
        "overall_status": snap.get("overall_status"),
        "as_of": snap.get("ts"),
        "incident": {
            "incident_id": snap.get("incident_id"),
            "severity": inc.get("severity"),
            "root_cause": inc.get("root_cause"),
            "summary": inc.get("summary"),
            "impacted": inc.get("impacted"),
            "source": inc.get("source"),
            "paged": bool(inc.get("paged")),
            "alert_id": inc.get("alert_id"),
        } if inc else None,
        "signals": snap.get("signals") or [],
        "stale_view": True,
    }
