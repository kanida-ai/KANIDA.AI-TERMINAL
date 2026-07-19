"""Monitor 1 — Platform-health: API p95/p99, task restarts, CPU/mem.

DATA SOURCE (local deploy today): best-effort process CPU/memory via `psutil`
IF it is installed. There is NO in-process API-latency histogram and NO task-
restart counter today, and CloudWatch is a CLOUD-future source — so p95/p99 and
restart metrics are reported UNKNOWN with a clear note (never fabricated). When
psutil is absent, CPU/mem are UNKNOWN too and the monitor says so.

READ-ONLY: reads only this process's own resource counters. Touches nothing.
"""
from __future__ import annotations

import os

from ..base import MonitorAgent
from ..signals import HealthSignal, Status, worse

# Thresholds (best-effort, env-tunable). Fractions of capacity.
_CPU_WARN = 85.0
_CPU_ALERT = 96.0
_MEM_WARN = 85.0
_MEM_ALERT = 95.0


class PlatformHealthMonitor(MonitorAgent):
    subsystem = "platform-health"

    def observe(self, context=None) -> HealthSignal:
        metrics = {
            "api_p95_ms": None,        # no in-process latency histogram today
            "api_p99_ms": None,
            "task_restarts": None,     # no restart counter today
            "cloudwatch": "NA",        # cloud-future source
            "pid": os.getpid(),
        }
        cpu = mem = None
        note = ""
        try:
            import psutil  # noqa: WPS433
            cpu = float(psutil.cpu_percent(interval=None))
            mem = float(psutil.virtual_memory().percent)
            metrics["cpu_percent"] = cpu
            metrics["mem_percent"] = mem
        except Exception:  # noqa: BLE001 — psutil optional / not installed
            metrics["cpu_percent"] = None
            metrics["mem_percent"] = None
            note = ("psutil unavailable — CPU/mem UNKNOWN; "
                    "p95/p99 & restarts have no local source (CloudWatch is cloud-future)")

        if cpu is None and mem is None:
            return self._signal(
                Status.UNKNOWN,
                note or "no local platform metrics available", metrics)

        status = Status.OK
        reasons = []
        if cpu is not None and cpu >= _CPU_ALERT:
            status = worse(status, Status.ALERT)
            reasons.append(f"CPU {cpu:.0f}%")
        elif cpu is not None and cpu >= _CPU_WARN:
            status = worse(status, Status.WARN)
            reasons.append(f"CPU {cpu:.0f}%")
        if mem is not None and mem >= _MEM_ALERT:
            status = worse(status, Status.ALERT)
            reasons.append(f"mem {mem:.0f}%")
        elif mem is not None and mem >= _MEM_WARN:
            status = worse(status, Status.WARN)
            reasons.append(f"mem {mem:.0f}%")
        summary = ("CPU/mem within bounds (p95/p99 & restarts: no local source)"
                   if status == Status.OK else
                   f"resource pressure: {', '.join(reasons)}")
        return self._signal(status, summary, metrics)
