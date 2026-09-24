"""
Options Agent · the strategy-constructor contract.

Mirrors backend/agents/chart/patterns/base.py so the shared runtime, the evidence machine
and the 3-column UI shape all carry over. Where the Chart Agent has a pattern DETECTOR that
finds geometry in bars, the Options Agent has a strategy CONSTRUCTOR that picks legs out of
a chain snapshot by governed rules.

POINT-IN-TIME CONTRACT (identical in spirit to the chart detectors):
  * a constructor reads ONLY the chain snapshot handed to it, which is already the snapshot
    for the decision date, and only underlying bars <= as_of_idx;
  * it never fetches, never looks at a later snapshot, and never resolves an expiry from
    today's live instrument master (that master lists surviving contracts only, which is
    both look-ahead and survivorship).
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict


@dataclass
class OptionsOccurrence:
    """One candidate structure, detected point-in-time. The options analogue of
    PatternOccurrence."""
    strategy: str                    # "iron_condor"
    underlying: str                  # "NIFTY"
    stage: str                       # CANDIDATE | ELIGIBLE | ILLIQUID | INCOMPLETE | STALE
    as_of_date: str                  # the decision date (the snapshot's date)
    expiry: str
    expiry_kind: str                 # "weekly" | "monthly" | "long_dated"
    dte: int
    spot: float
    forward: float                   # same-expiry FUT price from the SAME snapshot
    legs: list = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    greeks: dict = field(default_factory=dict)
    liquidity: dict = field(default_factory=dict)
    regime: dict = field(default_factory=dict)
    # Where this chain came from and what it is therefore MISSING -- basis (live vs backfill),
    # the survivorship boundary, capture_mode, forward_source, which expiries were quarantined,
    # and whether liquidity was measurable at all. Carried on every occurrence because a
    # consumer must never have to infer it, and load_chain's provenance used to be dropped on
    # the floor between the archive and the constructor.
    provenance: dict = field(default_factory=dict)
    direction: str = "neutral"
    timeframe: str = "daily"
    signal_idx: int = -1             # decision bar on the underlying frame
    entry_idx: int = -1              # signal_idx + 1 — entry = next open, unchanged law
    reasons: list = field(default_factory=list)   # why this stage, named
    signature: dict = field(default_factory=dict)
    context: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


class StrategyConstructor:
    """The contract. Subclasses implement construct(); the agent drives the lifecycle.

    `status` is an honesty field, exactly like the chart detectors': "built" means this
    constructor is real and its output may be decided on; "spec" means it is a skeleton and
    the agent must degrade to an honest WATCH rather than pretend it has an opinion."""
    strategy_id: str = "base"
    name: str = "Strategy"
    status: str = "spec"             # "built" | "spec"
    legs_count: int = 0
    defined_risk: bool = True
    underlyings: tuple = ("NIFTY",)
    expiries: tuple = ("weekly", "monthly")
    params_version: str = "v0"

    def construct(self, chain, underlying_df=None, as_of_idx=None, params=None) -> list:
        """Return a list of OptionsOccurrence built from ONE chain snapshot."""
        raise NotImplementedError

    def manifest(self) -> dict:
        return {"strategy_id": self.strategy_id, "name": self.name, "status": self.status,
                "legs_count": self.legs_count, "defined_risk": self.defined_risk,
                "underlyings": list(self.underlyings), "expiries": list(self.expiries),
                "params_version": self.params_version}
