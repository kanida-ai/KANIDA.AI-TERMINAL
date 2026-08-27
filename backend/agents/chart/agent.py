"""
Chart Agent (#1) — Horizontal Trendline, daily.

SKELETON. This registers the manifest and the lifecycle contract so the platform can list and
serve it. The point-in-time detector, the as-of Historical Matcher, the strategy-replay ETV and
the decision gates are ported from R&D per the v3 playbook:
    Documents/Kanida_Falcon:  scripts/chart_agent.py · chart_agent_screener.py · chart_agent_replay.py
    docs/Chart_Agent_Playbook_v3.docx  (§5 detector, §7 matcher, §8 strategy-replay, §9 gates)

Until that logic is ported in, decide() returns an honest WATCH placeholder — it NEVER
fabricates evidence. Agents emit intents only; execution routes through backend/autotrade/.
"""
from __future__ import annotations
from ..base import BaseAgent, Manifest
from .. import registry

MANIFEST = Manifest(
    agent_id="chart-v1",
    name="Chart Agent",
    agent_class="observe",
    universe="nifty500",
    timeframe="daily",
    schedule="eod",
    tools=["market_data", "evidence_store", "historical_probability"],
    outputs=["observation", "direction", "probability", "evidence", "intent"],
    tracking=[1, 3, 5, 10],
)


class ChartAgent(BaseAgent):
    def scan(self, ctx):
        # TODO(port): point-in-time Horizontal-Trendline detector (R&D chart_agent.detect_*).
        return []

    def decide(self, occurrence, ctx):
        # TODO(port): as-of retrieval -> strategy-replay ETV -> gate stack (v3 §7–§9).
        return {
            "decision": "WATCH",
            "reason": "Chart Agent scaffolded; detector + strategy-replay evidence port pending (v3 SPEC).",
            "evidence": None,
        }


registry.register(ChartAgent(MANIFEST))
