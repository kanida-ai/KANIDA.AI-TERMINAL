"""System-Engineering Agent Hierarchy — Phase 1 (platform HEALTH, OBSERVE only).

A "Platform Health Orchestrator" over 9 deterministic, read-only subsystem
monitors. Each monitor OBSERVES its subsystem and emits a structured HealthSignal;
the orchestrator correlates them (LLM with a deterministic fallback) into ONE
incident, classifies severity, and PAGES via the existing alerts.py plumbing.

PHASE 1 = OBSERVE + CORRELATE + PAGE. ZERO auto-remediation. No component here
touches a trade, order, position, the kill-switch, falcon_position_state, or ANY
execution/OMS/EMS path — observation is READ-ONLY. Everything is additive and
DEFAULT-OFF behind two gates (SYSAGENTS_ENABLED master flag + SYSAGENTS_KILL_SWITCH
global kill-switch); while off the layer is structurally inert.
"""
from __future__ import annotations

from .flags import agents_enabled, agents_killed, layer_active
from .signals import HealthSignal, Status
from .base import MonitorAgent

__all__ = [
    "agents_enabled", "agents_killed", "layer_active",
    "HealthSignal", "Status", "MonitorAgent",
]
