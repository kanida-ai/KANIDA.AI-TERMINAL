"""MonitorAgent — the base every subsystem monitor extends.

CONTRACT (Phase 1):
  * observe(context) is READ-ONLY. It reads its subsystem's already-existing
    data source and returns exactly one HealthSignal. It NEVER writes, mutates,
    places an order, or touches falcon_position_state / any execution path.
  * safe_observe() wraps observe() so a monitor that raises can NEVER crash the
    orchestrator or a tick — a failure becomes an UNKNOWN signal with the error
    class in `summary` (never the raw exception text, which could leak a path or
    a value). The Agent-Watcher then flags the UNKNOWN.
  * Deterministic. No LLM, no network beyond the read-only source. Fast.

A monitor MUST NOT import or call anything that submits, cancels, or modifies an
order; the only sanctioned side effect anywhere in this package is a web_push
page raised by the orchestrator.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from .signals import HealthSignal, Status, now_ist_iso

log = logging.getLogger("kanida.sysagents.monitor")


class MonitorAgent:
    """Base class. Subclasses set `subsystem` and implement `observe`."""

    subsystem: str = "unknown"

    def observe(self, context: Optional[Dict[str, Any]] = None) -> HealthSignal:
        """Read the subsystem and return one HealthSignal. Override this.
        MUST be read-only. `context` carries orchestrator-supplied read-only data
        (e.g. the Agent-Watcher receives sibling signals) and is otherwise None."""
        raise NotImplementedError

    def _signal(self, status: str, summary: str,
                metrics: Optional[Dict[str, Any]] = None) -> HealthSignal:
        return HealthSignal(
            subsystem=self.subsystem, status=status, summary=summary,
            metrics=metrics or {}, observed_at=now_ist_iso(),
            monitor=type(self).__name__)

    def safe_observe(self,
                     context: Optional[Dict[str, Any]] = None) -> HealthSignal:
        """observe() wrapped so it can never raise into the caller. Any exception
        → an UNKNOWN signal tagged with the exception CLASS only (no message, to
        avoid leaking a path/value). Never raises."""
        try:
            sig = self.observe(context)
            if not isinstance(sig, HealthSignal):
                return self._signal(
                    Status.UNKNOWN,
                    f"{type(self).__name__} returned a non-signal")
            return sig
        except Exception as e:  # noqa: BLE001 — a monitor must never crash the run
            log.warning("sysagents: monitor %s raised %s (→ UNKNOWN)",
                        type(self).__name__, type(e).__name__)
            return self._signal(
                Status.UNKNOWN,
                f"monitor error: {type(e).__name__}")
