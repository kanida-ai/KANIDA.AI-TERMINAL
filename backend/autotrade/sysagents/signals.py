"""HealthSignal — the ONE structured observation every monitor emits.

A monitor OBSERVES its subsystem (read-only) and returns exactly one
HealthSignal. The orchestrator correlates a batch of these into a single health
view. There is no action here — a HealthSignal is a fact, not an instruction.

STATUS ladder (worst-wins ordering, `rank`):
  OK       — subsystem healthy, within SLO.
  WARN     — degraded / approaching an SLO bound; informational, no page.
  ALERT    — an SLO is breached; pages (deduped).
  CRITICAL — a hard SLO breach / money-adjacent invariant; always pages.
  UNKNOWN  — the monitor could not determine status (error, no data). Non-ranking
             for "worst" but flagged by the Agent-Watcher. NEVER treated as OK.
  NA       — the subsystem does not exist in this deployment (e.g. the order-intent
             queue locally). Explicitly not-applicable; never a page, never OK.

SANITISATION: `metrics` must contain ONLY non-secret, non-PII values — the
orchestrator forwards a sanitized subset to the LLM. Monitors must never place a
token, credential, proxy URL, raw egress IP, or email into a HealthSignal. The
sanitizer in llm.py is a second line of defence, not a licence to be careless.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

IST = timezone(timedelta(hours=5, minutes=30))


class Status:
    OK = "OK"
    WARN = "WARN"
    ALERT = "ALERT"
    CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"
    NA = "NA"


# Worst-wins rank. UNKNOWN/NA are non-ranking (they do not escalate severity) but
# are surfaced separately. Higher = worse.
_RANK: Dict[str, int] = {
    Status.NA: -1,
    Status.UNKNOWN: 0,
    Status.OK: 1,
    Status.WARN: 2,
    Status.ALERT: 3,
    Status.CRITICAL: 4,
}

# Statuses that page (subject to the layer + paging gates).
PAGING_STATUSES = frozenset({Status.ALERT, Status.CRITICAL})


def rank(status: str) -> int:
    return _RANK.get(status, 0)


def worse(a: str, b: str) -> str:
    """Return the worse of two statuses under the paging ranking. UNKNOWN/NA lose
    to any real status but beat nothing except each other."""
    return a if rank(a) >= rank(b) else b


def now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


@dataclass
class HealthSignal:
    """One subsystem's health observation. Read-only fact; carries no action."""
    subsystem: str                       # e.g. "broker-health"
    status: str = Status.UNKNOWN         # Status.*
    summary: str = ""                    # one-line human-readable
    metrics: Dict[str, Any] = field(default_factory=dict)  # SANITIZED only
    observed_at: str = field(default_factory=now_ist_iso)  # ISO IST
    monitor: str = ""                    # the monitor class name that emitted it

    @property
    def paging(self) -> bool:
        return self.status in PAGING_STATUSES

    @property
    def rank(self) -> int:
        return rank(self.status)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "subsystem": self.subsystem,
            "status": self.status,
            "summary": self.summary,
            "metrics": dict(self.metrics),
            "observed_at": self.observed_at,
            "monitor": self.monitor,
        }
