"""
Chart Agent · Pattern library — the shared PatternDetector contract.

The Chart Agent hosts MANY patterns (horizontal trendline, triangle, channel, …). Each is an
independent detector that plugs into the same machine: the agent's scan() runs every registered
detector; decide() feeds each occurrence through the shared evidence + gate stack. Only the
detector + its signature are pattern-specific (v3 playbook §17).

STRICT POINT-IN-TIME LAW (v3 §0, §2, §3): a detector is handed OHLCV arrays and an ``as_of_idx``
and may read ONLY data at indices ``<= as_of_idx``. It must never index the future. A pivot at i
is only *confirmed* L bars later (§5.1); a breakout/retest stage is confirmed on a completed bar.
Entry is always the NEXT open (entry_idx = signal_idx + 1) — never a same-bar fill.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional
import numpy as np


@dataclass
class PatternOccurrence:
    """One point-in-time pattern occurrence — the unit of evidence (v3 §0).

    Recorded for TRADE, WATCH and NO-TRADE alike so the evidence base can't drift toward what the
    rulebook already liked (selection bias). ``signal_idx`` is the completed bar that confirms the
    stage; ``entry_idx = signal_idx + 1`` (next open, §3)."""
    pattern: str                       # detector id, e.g. "horizontal_trendline"
    stock: str
    stage: str                         # BREAKOUT | RETEST | APPROACHING | FAILED
    signal_idx: int                    # completed bar the stage confirms on (decision bar)
    entry_idx: int                     # signal_idx + 1 — enter at the next open
    level: Optional[float] = None      # the horizontal resistance R (geometry, flat patterns)
    geometry: dict = field(default_factory=dict)   # richer geometry for sloped patterns (triangle/channel)
    touches: list = field(default_factory=list)    # pivot indices defining the level/lines
    direction: str = "long"
    timeframe: str = "daily"
    signature: dict = field(default_factory=dict)  # hard keys + soft context (v3 §6)
    context: dict = field(default_factory=dict)    # regime / volume-state / distance-to-level …

    def to_dict(self) -> dict:
        d = asdict(self)
        # numpy scalars -> native for JSON/transport safety
        for k, v in list(d.items()):
            if isinstance(v, (np.floating, np.integer)):
                d[k] = v.item()
        return d


class PatternDetector:
    """Contract every chart pattern implements. Shared machine calls these; only these are
    pattern-specific (v3 §17). Detectors are STATLESS w.r.t. the future — point-in-time only."""

    pattern_id: str = "base"
    name: str = "Pattern"
    status: str = "spec"               # "built" | "spec" — honest advertisement of readiness

    def detect(self, df, as_of_idx: Optional[int] = None) -> list[PatternOccurrence]:
        """Return occurrences whose stage confirms at bar ``<= as_of_idx`` (None => full history,
        still point-in-time per-bar). MUST NOT read any bar index > as_of_idx."""
        raise NotImplementedError

    def manifest(self) -> dict:
        return {"pattern_id": self.pattern_id, "name": self.name, "status": self.status}
