"""
Chart Agent · pattern registry — mirrors backend/agents/registry.py.

Each pattern module self-registers on import. Loading is GUARDED: a broken pattern detector logs
a warning and is skipped — it can NEVER crash the Chart Agent, the other patterns, or app boot
(additive + guarded, per the platform rules).
"""
from __future__ import annotations
import importlib
import logging
from .base import PatternDetector

log = logging.getLogger("agents.chart.patterns")
_PATTERNS: dict = {}

# Pattern modules to load (each self-registers). Add new patterns here.
_BUILTIN = (
    "agents.chart.patterns.horizontal_trendline",
    "agents.chart.patterns.triangle",
    "agents.chart.patterns.channel",
)


def register(detector: PatternDetector) -> PatternDetector:
    _PATTERNS[detector.pattern_id] = detector
    return detector


def get(pattern_id: str):
    return _PATTERNS.get(pattern_id)


def all_patterns() -> list:
    return list(_PATTERNS.values())


def load_builtin() -> None:
    for mod in _BUILTIN:
        try:
            importlib.import_module(mod)
        except Exception as e:  # noqa: BLE001 — one bad detector must never break the rest
            log.warning("pattern module %s not loaded (non-fatal): %s", mod, e)
