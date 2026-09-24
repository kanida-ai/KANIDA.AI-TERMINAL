"""
Options Agent · strategy registry.

Byte-for-byte the shape of backend/agents/chart/patterns/registry.py: strategies
self-register on import, loading is per-module guarded, and a broken strategy is skipped
with a warning rather than crashing the agent or app boot.
"""
from __future__ import annotations

import importlib
import logging

log = logging.getLogger("agents.options.strategies")

_REGISTRY: dict = {}

# Built-in strategies to import (each self-registers). Phase 1 is deliberately ONE strategy,
# mirroring the Chart Agent's one-pattern-at-a-time rule.
_BUILTIN = ("agents.options.strategies.iron_condor",)


def register(constructor):
    _REGISTRY[constructor.strategy_id] = constructor
    return constructor


def get(strategy_id: str):
    return _REGISTRY.get(strategy_id)


def all_strategies() -> list:
    return list(_REGISTRY.values())


def load_builtin() -> None:
    for mod in _BUILTIN:
        try:
            importlib.import_module(mod)
        except Exception as e:  # noqa: BLE001 — one bad strategy must never sink the rest
            log.warning("options strategy %s not loaded (non-fatal): %s", mod, e)
