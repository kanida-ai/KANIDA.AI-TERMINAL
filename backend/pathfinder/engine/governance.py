"""
Governance: the Constitution, the go/no-go gauntlet, and the L1–L4 hierarchy.

Three things live here and nowhere else:

1. **The Constitution is read, never written.** `load_constitution()` parses the YAML
   and there is deliberately no `save_constitution()`. L4 is a human editing a file.
2. **The gauntlet is a list of named gates, each with its bar and its measured
   value.** A gate returns a value even when it passes, because "it passed" is not
   evidence — the number is. Failing is a publishable outcome.
3. **A learning is a change-log entry**: what changed → why → evidence → previous
   version → new version → did it improve. `improved` may be `None`, and `None` is
   the correct answer far more often than people like.

**Where significance is enforced, and why it is not where you would first put it.**

An adversarial audit of this engine found that the original design killed an experiment
at *discovery* if its p-value did not clear `alpha / n_candidates`. With a correctly
specified null (see `replay.block_placebo` — these signals cluster on event days, so the
effective sample is events, not trades) nothing clears a bar that strict, and the loop
would never test anything out of sample. That is the wrong failure mode, and it comes
from putting a family-wise correction on the *screening* step.

So the gates are arranged the way the statistics actually work:

* **Discovery** is a SCREEN. It uses the naked bar, and it says so: passing it is not
  evidence of significance, and the family-wise bar is recorded next to the result as an
  ADVISORY gate so the inflation is visible rather than silently forgiven.
* **Validation** is a fresh test of ONE pre-specified hypothesis on data that had no say
  in choosing it. No multiple-testing correction is owed there. It kills on the economics
  — expectancy, the 2× slippage gate, edge over its own universe, sample size.
* **Significance gates PROMOTION, not survival.** An idea that is positive out of sample
  but not yet distinguishable from chance is *exactly* the thing a virtual book exists to
  accumulate evidence on. Killing it would be treating absence of evidence as evidence of
  absence; promoting it would be the opposite error. It is labelled and it trades virtual
  money, which is what §1's "labelled, not promoted" already said.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import NormalDist
from typing import Any, Optional, Sequence

import yaml

from .evidence import Metrics
from .hypothesis import HypothesisSpec


class ConstitutionError(RuntimeError):
    """The Constitution is missing, malformed, or was asked to change. All fatal."""


@dataclass(frozen=True)
class Constitution:
    version: str
    document: dict[str, Any]
    approved_by: str
    effective_from: str

    @property
    def is_signed(self) -> bool:
        """An unsigned draft governs the run but may not authorise a promotion."""
        return not self.approved_by.strip().upper().startswith("UNSIGNED")

    @property
    def approved_ranges(self) -> dict[str, dict[str, Any]]:
        return self.document["approved_parameter_ranges"]

    def get(self, path: str, default: Any = None) -> Any:
        node: Any = self.document
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node


def load_constitution(path: str | Path) -> Constitution:
    p = Path(path)
    if not p.exists():
        raise ConstitutionError(
            f"no Constitution at {p}. Pathfinder does not run ungoverned — "
            "L2 cannot be enforced without the approved ranges."
        )
    doc = yaml.safe_load(p.read_text(encoding="utf-8"))
    for required in ("version", "approved_by", "approved_parameter_ranges", "gauntlet", "learning"):
        if required not in doc:
            raise ConstitutionError(f"Constitution is missing `{required}`")
    if doc.get("authored_by") != "human":
        raise ConstitutionError("the Constitution must be human-authored (L4)")
    return Constitution(
        version=str(doc["version"]), document=doc,
        approved_by=str(doc["approved_by"]),
        effective_from=str(doc.get("effective_from", "")),
    )


# ── implementability ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Implementability:
    ok: bool
    reason: str


def check_implementable(spec: HypothesisSpec) -> Implementability:
    """
    Can a retail customer actually run this under the stated cost convention?

    The cost model is NSE **cash delivery**. In the Indian cash market a short
    position cannot be carried overnight — a multi-session short needs stock futures,
    which is a different universe (~200 names), different costs, lot sizes, and roll
    risk. Pathfinder models none of that, so a multi-session short is not a result we
    are entitled to publish as tradeable, however good it looks.

    This is a governance gate, not a statistical one. It exists because the best edge
    in the discovery scan was exactly this shape.
    """
    if spec.direction == "short" and spec.horizon_sessions > 1:
        return Implementability(False, (
            "multi-session short: not executable in the NSE cash segment under the "
            "stated delivery cost convention; would require stock futures, which "
            "Pathfinder does not model (different universe, costs, lot sizes, roll)"
        ))
    return Implementability(True, "executable under the stated cash-delivery convention")


# ── the gauntlet ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Gate:
    name: str
    passed: bool
    value: Optional[float]
    bar: Optional[float]
    statement: str
    #: An advisory gate is recorded and published but does not kill. It exists so a
    #: known weakness is visible in the record instead of being quietly forgiven.
    fatal: bool = True

    def __str__(self) -> str:
        mark = ("PASS" if self.passed else "FAIL") if self.fatal else \
               ("ok  " if self.passed else "NOTE")
        v = "—" if self.value is None else f"{self.value:.4g}"
        b = "—" if self.bar is None else f"{self.bar:.4g}"
        tag = "" if self.fatal else " [advisory]"
        return f"[{mark}]{tag} {self.name}: {v} vs bar {b} — {self.statement}"


@dataclass
class GateSet:
    gates: list[Gate] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Advisory gates are recorded, not enforced. Only a fatal failure kills."""
        return all(g.passed for g in self.gates if g.fatal)

    @property
    def failures(self) -> list[Gate]:
        return [g for g in self.gates if not g.passed and g.fatal]

    @property
    def advisories(self) -> list[Gate]:
        return [g for g in self.gates if not g.passed and not g.fatal]

    def add(self, name: str, passed: bool, value: Optional[float],
            bar: Optional[float], statement: str, fatal: bool = True) -> None:
        self.gates.append(Gate(name, passed, value, bar, statement, fatal))


def discovery_gauntlet(
    spec: HypothesisSpec, m: Metrics, c: Constitution, *, n_candidates: int,
    novel: bool, novelty_note: str,
) -> GateSet:
    """Steps 1–6 of docs/STRATEGY_METHODOLOGY.md §1, on the DISCOVERY window only."""
    g = GateSet()
    gt = c.document["gauntlet"]
    alpha = float(gt["max_placebo_p_value"])
    adjusted = alpha / max(1, n_candidates)

    g.add("discovery_expectancy_net", m.expectancy_pct_per_trade > float(gt["min_expectancy_pct_net"]),
          m.expectancy_pct_per_trade, float(gt["min_expectancy_pct_net"]),
          "expectancy per trade, net of the stated costs, must be positive")
    g.add("discovery_expectancy_2x_slippage",
          m.expectancy_2x_slippage_pct_per_trade > float(gt["min_expectancy_pct_net_at_2x_slippage"]),
          m.expectancy_2x_slippage_pct_per_trade, float(gt["min_expectancy_pct_net_at_2x_slippage"]),
          "an edge that only exists at the friendly slippage assumption is a cost artefact")
    g.add("discovery_edge_vs_baseline", m.edge_vs_baseline_pct > float(gt["min_edge_vs_baseline_pct"]),
          m.edge_vs_baseline_pct, float(gt["min_edge_vs_baseline_pct"]),
          "must beat the same universe, same window, same exits — not just the market")
    # SCREEN, not a significance test. The null is day-blocked (`placebo_kind`), because
    # these signals arrive in clusters on event days and an i.i.d. null understates its
    # own spread by about the square root of the average cluster size.
    g.add("discovery_placebo_screen", m.placebo_p_value <= alpha, m.placebo_p_value, alpha,
          f"day-blocked permutation p-value ({m.placebo_kind} null, {m.placebo_draws} draws); "
          "a screen, not a significance claim")
    # ADVISORY: the family-wise bar the best of N candidates would have to clear. It is
    # recorded so the selection inflation is visible; it gates PROMOTION, not survival.
    g.add("discovery_family_wise_significance", m.placebo_p_value <= adjusted,
          m.placebo_p_value, adjusted,
          f"the bar the best of {n_candidates} screened candidates must clear to be called "
          "significant; advisory here, required for promotion", fatal=False)
    impl = check_implementable(spec)
    g.add("implementable_under_cost_convention", impl.ok, None, None, impl.reason)
    g.add("novelty", novel, None, None, novelty_note)
    g.add("has_invalidation_not_a_target", bool(spec.invalidation_text), None, None,
          "the rulebook states what makes the idea wrong, not a price to hope for")
    return g


def validation_gauntlet(m: Metrics, c: Constitution) -> GateSet:
    """
    The out-of-sample hold. Nothing here was used to choose the rule, so no
    multiple-testing correction is owed — this is one pre-specified hypothesis meeting
    data for the first time.

    It kills on the economics. It does NOT kill on significance: an idea that is
    positive out of sample but not yet distinguishable from chance is what a virtual
    book is for. See `promotion_gate`.
    """
    g = GateSet()
    gt = c.document["gauntlet"]
    g.add("oos_expectancy_net", m.expectancy_pct_per_trade > 0.0, m.expectancy_pct_per_trade, 0.0,
          "the edge must still be there on data that had no say in choosing it")
    g.add("oos_expectancy_2x_slippage", m.expectancy_2x_slippage_pct_per_trade > 0.0,
          m.expectancy_2x_slippage_pct_per_trade, 0.0, "and it must survive twice the slippage")
    g.add("oos_edge_vs_baseline", m.edge_vs_baseline_pct > 0.0, m.edge_vs_baseline_pct, 0.0,
          "and it must still beat its own universe")
    g.add("oos_sample_size", m.n >= int(gt["min_n_for_promotion"]), float(m.n),
          float(gt["min_n_for_promotion"]), "a sample this small is labelled, not promoted")
    # ADVISORY. The number that should be believed: trades clustered on the signal date,
    # because a screen firing on three hundred names in one morning is one event.
    z = NormalDist().inv_cdf(1 - alpha_two_sided(c) / 2)
    g.add("oos_cluster_significance", m.cluster_t >= z, m.cluster_t, z,
          f"t on {m.signal_days} independent signal days, not on {m.n} trades; "
          "advisory here, required for promotion", fatal=False)
    g.add("oos_placebo", m.placebo_p_value <= alpha_two_sided(c), m.placebo_p_value,
          alpha_two_sided(c),
          f"day-blocked permutation p-value ({m.placebo_draws} draws); "
          "advisory here, required for promotion", fatal=False)
    return g


def alpha_two_sided(c: Constitution) -> float:
    return float(c.document["gauntlet"]["max_placebo_p_value"])


def book_gauntlet(m: Metrics, c: Constitution) -> GateSet:
    """
    The hardest gate, and the only one that is about a *book* rather than a rule.

    A rule's expectancy is measured over every firing. A book can only take the ones
    it has capital and slots for. If those differ, the book is the truth, because the
    book is the thing a person would have owned.
    """
    g = GateSet()
    g.add("book_expectancy_net", m.expectancy_pct_per_trade > 0.0, m.expectancy_pct_per_trade, 0.0,
          "the capacity-constrained virtual book, not the unconstrained rule")
    g.add("book_expectancy_2x_slippage", m.expectancy_2x_slippage_pct_per_trade > 0.0,
          m.expectancy_2x_slippage_pct_per_trade, 0.0, "the book must also survive twice the slippage")
    return g


def promotion_gauntlet(c: Constitution, *, discovery: GateSet, validation: GateSet) -> GateSet:
    """
    Everything survival did not require. This is where significance lives, and where a
    human signature lives, and nothing reaches a customer without both.
    """
    g = GateSet()
    signed = c.is_signed
    g.add("constitution_signed", signed, None, None,
          ("Constitution is signed" if signed else
           f"Constitution {c.version} is an UNSIGNED draft ({c.approved_by}); "
           "no experiment may be promoted until a human signs it"))
    for gate in list(discovery.gates) + list(validation.gates):
        if not gate.fatal:                       # the advisories become requirements here
            g.add(gate.name, gate.passed, gate.value, gate.bar, gate.statement)
    return g


def promotion_gate(c: Constitution) -> Gate:
    """The signature alone. Kept for callers that only need that one fact."""
    signed = c.is_signed
    return Gate(
        "constitution_signed", signed, None, None,
        ("Constitution is signed" if signed else
         f"Constitution {c.version} is an UNSIGNED draft ({c.approved_by}); "
         "no experiment may be promoted until a human signs it"),
    )


# ── the L1–L4 change-log ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class ChangeLogEntry:
    """What changed → why → evidence → previous version → new version → improved."""
    seq: int
    level: str                       # L1 | L2 | L3 | L4
    at: datetime
    what_changed: str
    why: str
    evidence_ids: tuple[str, ...]
    previous_version: Optional[str]
    new_version: str
    improved: Optional[bool]         # None = not yet enough forward evidence. Never guessed.
    decided_by: str                  # engine | llm | human
    approved_by: Optional[str] = None
    validation: Optional[str] = None
    constitution_version: str = ""

    def __post_init__(self) -> None:
        if self.level == "L4" and (self.decided_by != "human" or not self.approved_by):
            raise ConstitutionError("L4 changes are human-only and need a named approver")
        if self.level == "L3" and not self.validation:
            raise ConstitutionError(
                "L3 (a new strategy version) must state how it was backtested AND "
                "forward-validated before it may replace the incumbent"
            )
        if not self.evidence_ids:
            raise ConstitutionError("every change must cite at least one piece of evidence")
        if self.previous_version is not None and self.previous_version == self.new_version:
            raise ConstitutionError("a change must move the version")


def check_parameter_within_range(
    c: Constitution, trigger: str, name: str, value: float
) -> None:
    """L2. Raises rather than clamping — agreeing with a model by rounding is not agreement."""
    rng = c.approved_ranges.get(trigger, {}).get(name)
    if rng is None:
        raise ConstitutionError(f"{trigger}.{name} has no Constitution-approved range")
    lo, hi = float(rng["min"]), float(rng["max"])
    if not (lo <= value <= hi):
        raise ConstitutionError(
            f"L2 violation: {trigger}.{name}={value:g} outside approved range [{lo:g}, {hi:g}]"
        )
