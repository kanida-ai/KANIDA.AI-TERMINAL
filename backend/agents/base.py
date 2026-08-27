"""
KANIDA Agent Platform — the shared contract.

Every PRODUCT agent implements this one lifecycle; only its detector / signature / decision
(its *playbook*) differ. The runtime provides scheduling, market data, the point-in-time
evidence store, subscriptions, the storyline emitter, and auto-trade routing.

HARD BOUNDARY (see docs/AGENTS_PLATFORM.md):
  - Agents EMIT intents; they NEVER touch a broker, git, shell, or deploy.
  - Execution flows to backend/autotrade/ — paper-default, per-broker cert-gated, operator-armed.
  - Agents are strictly point-in-time: only data available at the decision bar.
  - Learning appends immutable occurrences; it NEVER edits rules live (governance does that).
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional, Literal

AgentClass = Literal["observe", "decision", "experience"]


@dataclass
class Manifest:
    agent_id: str
    name: str
    agent_class: AgentClass = "observe"
    universe: str = "nifty500"
    timeframe: str = "daily"                 # daily | 1h | 1m …
    schedule: str = "eod"                    # eod | intraday
    tools: list = field(default_factory=lambda: ["market_data", "evidence_store"])
    outputs: list = field(default_factory=lambda: ["observation", "evidence", "intent"])
    tracking: list = field(default_factory=lambda: [1, 3, 5, 10])   # horizons T+h
    learning: bool = True
    # Execution routing — NEVER a broker directly; always through autotrade/, paper by default.
    execution: dict = field(default_factory=lambda: {
        "route": "autotrade", "mode": "paper", "live": "requires_broker_cert+operator_arm"})
    permissions: dict = field(default_factory=lambda: {
        "trade_execution": "gated", "source_code": False})
    # Optional per-agent extension: agents that host multiple sub-detectors (e.g. the Chart
    # Agent's pattern library) advertise them here. Additive + default-empty so every existing
    # agent and app boot is unaffected.
    patterns: list = field(default_factory=list)
    version: str = "v1"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Intent:
    """What an agent emits. The runtime hands this to autotrade/ (paper-default, gated).
    An agent can never set mode='live' — going live is an operator-armed step downstream."""
    agent_id: str
    stock: str
    direction: Literal["long", "short"]
    signal_ts: str
    thesis: str                       # first-person, human-readable
    evidence_ref: Optional[str] = None
    mode: str = "paper"


class BaseAgent:
    """The contract. Subclasses implement scan()/decide(); the runtime drives the lifecycle:

        SCAN -> ANALYZE -> DECIDE -> EXPLAIN -> TRACK -> RESOLVE -> LEARN

    track/resolve/learn are provided by the shared evidence store, so most agents only
    implement scan() and decide(). Subclasses MUST honour point-in-time (data <= now)."""
    manifest: Manifest

    def __init__(self, manifest: Manifest):
        self.manifest = manifest

    def scan(self, ctx) -> list:
        """Detect candidate occurrences point-in-time. Return a list of occurrence dicts."""
        raise NotImplementedError

    def decide(self, occurrence: dict, ctx) -> dict:
        """Return {decision: 'TRADE'|'WATCH'|'NO_TRADE', reason, evidence, intent?}."""
        raise NotImplementedError

    def explain(self, decision: dict) -> str:
        return decision.get("reason", "")
