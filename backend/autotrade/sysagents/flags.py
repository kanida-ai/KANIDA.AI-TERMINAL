"""System-Engineering Agent Hierarchy — global gates (Phase 1).

THE MASTER FLAG + THE GLOBAL KILL-SWITCH. Everything in this package is
default-OFF and observation-only. Phase 1 takes ZERO corrective action: the
monitors READ their subsystems and emit HealthSignals; the orchestrator
correlates + PAGES. Nothing here ever touches a trade, an order, a position, the
kill-switch, falcon_position_state, or ANY execution/OMS/EMS path.

TWO INDEPENDENT GATES, both must be satisfied for the layer to do anything:
  1. agents_enabled()  — the MASTER flag (SYSAGENTS_ENABLED). Default OFF. While
     off, the orchestrator returns a "disabled" view WITHOUT collecting a single
     signal, the run-loop never starts, and the endpoint reports disabled. It is
     structurally impossible for the layer to read a subsystem or raise a page.
  2. agents_killed()   — the global KILL-SWITCH (SYSAGENTS_KILL_SWITCH). Default
     OFF (= not killed). When ON it hard-stops the layer even if the master flag
     is on: the run-loop exits and the orchestrator early-returns. This is the
     "stop the observers NOW" lever; because Phase 1 has no actions, it only ever
     stops observation + paging (it can never strand a position).

Neither gate, when tripped, can affect the trading system: the ONLY things this
layer does are (a) read-only DB/metric reads and (b) web_push pages. Turning it
off simply stops both.
"""
from __future__ import annotations

import os


def _truthy(name: str, default: str = "off") -> bool:
    return os.environ.get(name, default).strip().lower() in (
        "on", "1", "true", "yes")


def agents_enabled() -> bool:
    """MASTER flag for the whole System-Engineering Agent layer. DEFAULT OFF.

    Off ⇒ no signals collected, no correlation, no pages, run-loop inert,
    endpoint reports disabled. The operator opts in with SYSAGENTS_ENABLED=true.
    """
    return _truthy("SYSAGENTS_ENABLED", "off")


def agents_killed() -> bool:
    """GLOBAL kill-switch. DEFAULT OFF (= not killed). When ON the layer is hard
    stopped regardless of the master flag: the run-loop exits and the
    orchestrator early-returns. Phase 1 has no actions, so this only stops
    observation + paging — it can never affect a real position."""
    return _truthy("SYSAGENTS_KILL_SWITCH", "off")


def layer_active() -> bool:
    """True only when the layer may do work: master flag ON and NOT killed."""
    return agents_enabled() and not agents_killed()


def run_interval_sec() -> int:
    """Cadence (s) of the monitor run-loop. Default 60s. Bounded to a sane floor
    so a misconfig can never busy-spin the observers."""
    try:
        v = int(os.environ.get("SYSAGENTS_INTERVAL_SEC", "60").strip())
    except (ValueError, AttributeError, TypeError):
        v = 60
    return max(10, v)


def paging_enabled() -> bool:
    """Whether the orchestrator is allowed to PAGE (web_push) on an incident.
    DEFAULT ON *within* the layer — but the layer itself is default-OFF, so no
    page can fire unless the operator first enables the whole layer. This is a
    second, independent throttle so the operator can run the layer in
    OBSERVE-ONLY (no pages) mode while tuning thresholds. SYSAGENTS_PAGING."""
    return _truthy("SYSAGENTS_PAGING", "on")
