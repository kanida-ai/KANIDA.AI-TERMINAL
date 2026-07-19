"""Monitor 6 — Queue-latency: order-intent queue depth / dequeue lag.

There is NO order-intent queue in the platform today: sessions place orders
in-process (asyncio.gather over broker adapters), not through a queue/broker/bus.
This is a CLOUD-FUTURE subsystem (a decoupled order-intent bus is on the Phase-2+
roadmap). Per the spec we do NOT fabricate a metric — this monitor returns NA with
a clear note until such a queue exists.

If a queue is introduced later, wire its depth/dequeue-lag reads HERE and switch
the status logic on real numbers.
"""
from __future__ import annotations

from ..base import MonitorAgent
from ..signals import HealthSignal, Status


class QueueLatencyMonitor(MonitorAgent):
    subsystem = "queue-latency"

    def observe(self, context=None) -> HealthSignal:
        return self._signal(
            Status.NA,
            "no order-intent queue in this deployment (in-process execution) — "
            "cloud-future subsystem; not applicable",
            {"queue_present": False, "queue_depth": None, "dequeue_lag_ms": None})
