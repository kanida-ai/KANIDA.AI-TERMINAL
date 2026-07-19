"""The 9 deterministic, read-only subsystem monitors (Phase 1 = OBSERVE only).

Each monitor OBSERVES its subsystem via an EXISTING data source and emits one
HealthSignal. None takes any action, touches an order/position/kill-switch, or
writes falcon_position_state. Where a subsystem's data does not exist in this
deployment (order-intent queue; cloud CloudWatch), the monitor returns NA/UNKNOWN
with a clear note — it never fabricates a metric.
"""
from __future__ import annotations

from typing import List

from ..base import MonitorAgent
from .platform_health import PlatformHealthMonitor
from .broker_health import BrokerHealthMonitor
from .execution_quality import ExecutionQualityMonitor
from .data_freshness import DataFreshnessMonitor
from .market_data import MarketDataMonitor
from .queue_latency import QueueLatencyMonitor
from .risk_rms import RiskRmsMonitor
from .trading_stats import TradingStatsMonitor
from .agent_watcher import AgentWatcherMonitor

__all__ = [
    "PlatformHealthMonitor", "BrokerHealthMonitor", "ExecutionQualityMonitor",
    "DataFreshnessMonitor", "MarketDataMonitor", "QueueLatencyMonitor",
    "RiskRmsMonitor", "TradingStatsMonitor", "AgentWatcherMonitor",
    "build_monitors", "AgentWatcherMonitor",
]

# The 8 primary monitors (Agent-Watcher runs LAST, over their signals).
_PRIMARY = [
    PlatformHealthMonitor, BrokerHealthMonitor, ExecutionQualityMonitor,
    DataFreshnessMonitor, MarketDataMonitor, QueueLatencyMonitor,
    RiskRmsMonitor, TradingStatsMonitor,
]


def build_primary_monitors() -> List[MonitorAgent]:
    """Fresh instances of the 8 primary monitors, in canonical order."""
    return [cls() for cls in _PRIMARY]
