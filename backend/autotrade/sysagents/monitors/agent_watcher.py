"""Monitor 9 — Agent-Watcher: heartbeat/freshness of every OTHER monitor.

Unlike the other 8, this monitor observes the MONITORS, not a subsystem: it runs
LAST and receives the sibling signals via context["signals"]. It flags:
  * a monitor that FAILED this run (UNKNOWN with a "monitor error:" summary),
  * a monitor whose signal is STALE (observed_at far older than this run — a
    monitor that silently stopped producing),
  * a suspiciously EMPTY batch (fewer signals than the 8 expected → a collector
    bug),
  * contradiction heuristics (e.g. market-data OK while broker-health CRITICAL in
    hours is worth a human glance — surfaced as a note, not escalated).

It reads nothing but the in-memory signal list it is handed — no DB, no network.
"""
from __future__ import annotations

from ..base import MonitorAgent
from ..signals import HealthSignal, Status, worse
from ..util import age_seconds

_EXPECTED_PRIMARY = 8
_STALE_SIGNAL_SEC = 300.0


class AgentWatcherMonitor(MonitorAgent):
    subsystem = "agent-watcher"

    def observe(self, context=None) -> HealthSignal:
        signals = (context or {}).get("signals") or []
        metrics = {
            "n_monitors": len(signals),
            "n_expected": _EXPECTED_PRIMARY,
            "errored": [],
            "stale": [],
            "notes": [],
        }
        errored = []
        stale = []
        by_sub = {}
        for s in signals:
            d = s.to_dict() if hasattr(s, "to_dict") else dict(s)
            sub = d.get("subsystem")
            by_sub[sub] = d
            summ = str(d.get("summary") or "")
            if d.get("status") == Status.UNKNOWN and summ.startswith("monitor error:"):
                errored.append(sub)
            age = age_seconds(d.get("observed_at"))
            if age is not None and age > _STALE_SIGNAL_SEC:
                stale.append(sub)
        metrics["errored"] = errored
        metrics["stale"] = stale

        notes = []
        # Contradiction heuristic (surfaced, not escalated).
        bh = by_sub.get("broker-health", {}).get("status")
        md = by_sub.get("market-data", {}).get("status")
        if bh == Status.CRITICAL and md == Status.OK:
            notes.append("broker-health CRITICAL but market-data OK — "
                         "possible contradictory view; verify")
        metrics["notes"] = notes

        status = Status.OK
        reasons = []
        if len(signals) < _EXPECTED_PRIMARY:
            status = worse(status, Status.WARN)
            reasons.append(f"only {len(signals)}/{_EXPECTED_PRIMARY} monitors reported")
        if errored:
            status = worse(status, Status.WARN)
            reasons.append(f"errored: {', '.join(errored)}")
        if stale:
            status = worse(status, Status.WARN)
            reasons.append(f"stale: {', '.join(stale)}")

        summary = ("all monitors fresh + reporting" if status == Status.OK
                   else "; ".join(reasons))
        if notes:
            summary += " | " + "; ".join(notes)
        return self._signal(status, summary, metrics)
