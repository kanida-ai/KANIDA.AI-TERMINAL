"""
Agent registry — the runtime discovers and serves agents from here.

Adding an agent = it self-registers on import (see agents/chart/agent.py), and gets listed
in load_builtin(). It then ships with the normal backend deploy (one image, no per-agent
service). Loading is lazy + guarded so one bad agent can never break the others or app boot.
"""
from __future__ import annotations
import importlib
import logging
from .base import BaseAgent

log = logging.getLogger("agents.registry")
_REGISTRY: dict = {}

# Built-in agents to import (each self-registers). Add new agents here, or auto-discover later.
_BUILTIN = ("agents.chart.agent",)


def register(agent: BaseAgent) -> BaseAgent:
    _REGISTRY[agent.manifest.agent_id] = agent
    return agent


def get(agent_id: str):
    return _REGISTRY.get(agent_id)


def all_agents() -> list:
    return list(_REGISTRY.values())


def load_builtin() -> None:
    for mod in _BUILTIN:
        try:
            importlib.import_module(mod)
        except Exception as e:  # noqa: BLE001 — one bad agent must never crash the rest
            log.warning("agent module %s not loaded (non-fatal): %s", mod, e)
