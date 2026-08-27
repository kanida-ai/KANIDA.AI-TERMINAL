"""KANIDA Agent Platform (backend/agents) — runtime + registry for product agents.

Falcon = agent #0 (wraps falcon/); Chart = agent #1; Options / Events / … follow.
One shared lifecycle (Scan -> Decide -> Explain -> Track -> Learn); each agent brings only
its own playbook (detector, signature, evidence, decision). Auto-trade routes through
backend/autotrade/. See docs/AGENTS_PLATFORM.md.
"""
from .base import BaseAgent, Manifest, Intent  # noqa: F401
