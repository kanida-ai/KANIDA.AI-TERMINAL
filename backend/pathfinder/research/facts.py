"""
The only way a number is minted: `FactSet.add(...)` -> a `Fact` with full provenance.

A `Fact` (pathfinder.schemas) refuses a statistic without an `n`, derives its sample flag
from that `n`, and carries data source, period, `as_of`, cost convention, the deterministic
component that computed it, and the evidence LEVEL (same stock / peer group / sector /
whole market). LLM prose can only point at one of these by id.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Optional

import numpy as np

from ..schemas import DateRange, EvidenceLevel, Fact, Provenance, SampleFlag, Unit
from .config import ResearchConfig, now_ist

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slug(text: str) -> str:
    s = _SLUG_RE.sub("_", str(text).lower()).strip("_")
    return s or "x"


def _d(v: str | date) -> date:
    return v if isinstance(v, date) else date.fromisoformat(str(v)[:10])


class FactSet:
    """Facts for one finding. Ids are `fct_<finding-slug>_<name>`."""

    def __init__(self, *, finding_slug: str, cfg: ResearchConfig, as_of: str,
                 period_start: str, period_end: str, component: str,
                 computed_at: Optional[datetime] = None) -> None:
        self.prefix = f"fct_{slug(finding_slug)}"
        self.cfg = cfg
        self.as_of = _d(as_of)
        self.period = (_d(period_start), _d(period_end))
        self.component = cfg.component(component)
        self.computed_at = computed_at or now_ist()
        self.facts: list[Fact] = []
        self._ids: set[str] = set()

    def id(self, name: str) -> str:
        return f"{self.prefix}_{slug(name)}"

    def token(self, name: str) -> str:
        fid = self.id(name)
        if fid not in self._ids:
            raise KeyError(f"fact {fid} has not been minted")
        return "{{fact:" + fid + "}}"

    def add(self, name: str, label: str, value: Any, unit: Unit | str, *,
            n: Optional[int] = None, level: EvidenceLevel | str = EvidenceLevel.whole_market,
            note: Optional[str] = None, period: Optional[tuple[str, str]] = None,
            sample: str = "statistic", degenerate_ok: bool = False) -> Fact:
        """
        `sample` says what kind of number this is (S1 audit C8):
          * "statistic"   — computed over a sample; `n` is REQUIRED for pct/ratio/x units
          * "parameter"   — a threshold the engine was configured with; no n, flag not_applicable
          * "observation" — one reading (today's move, today's z-score); no n, flag not_applicable
        Before this existed the engine invented an n (n=1, n=20, n=60, n=cases) on parameters
        and single observations to satisfy the schema. That is fabrication and it is now rejected.
        """
        fid = self.id(name)
        if fid in self._ids:
            raise ValueError(f"duplicate fact {fid}")
        if sample not in ("statistic", "parameter", "observation"):
            raise ValueError(f"fact {fid}: unknown sample kind {sample!r}")
        if sample != "statistic" and n is not None:
            raise ValueError(f"fact {fid}: a {sample} has no sample; do not pass n")
        start, end = self.period if period is None else (_d(period[0]), _d(period[1]))
        if end > self.as_of:
            raise ValueError(f"fact {fid}: period ends after as_of — look-ahead")
        if isinstance(value, np.integer):
            value = int(value)
        elif isinstance(value, np.floating):
            value = float(value)
        if isinstance(value, float):
            if not np.isfinite(value):
                raise ValueError(f"fact {fid}: value is not finite")
            value = round(value, 4)
        if n is not None:
            n = int(n)
            # S1 second audit A6: "0.0% of 0 times" is not a statistic, it is a sentinel
            # rendered as a value. A statistic over zero cases cannot be minted; the caller
            # withholds the fact (or the card) instead.
            if n <= 0:
                raise ValueError(f"fact {fid}: a statistic over n={n} cases cannot be minted — withhold it")
        # Fail-loud guard (S1 audit C1): a share of exactly 0% or 100% over a hundred or
        # more cases is, in this engine, a computation bug (a mis-aligned frame, a
        # dtype accident), not a finding. The superseded smoke run published big_move=0.0%
        # over 13,119 cases; nothing caught it. Now something does.
        # It applies to SHARES (labels that begin with "share"), not to medians or means,
        # where an exact zero is an ordinary value. `degenerate_ok` is the documented opt-out
        # for a share that is structurally bounded (e.g. "still top-three" of three sectors).
        if sample == "statistic" and Unit(unit) == Unit.pct and n is not None and n >= 100 \
                and str(label).strip().lower().startswith("share") and not degenerate_ok \
                and isinstance(value, (int, float)) and float(value) in (0.0, 100.0):
            raise ValueError(
                f"fact {fid}: degenerate statistic {value}% over n={n} — refusing to mint; "
                "a share of exactly zero or a hundred over a hundred-plus cases is a bug until proven otherwise")
        flag_kw = {"sample_flag": SampleFlag.not_applicable} if sample != "statistic" else {}
        if sample != "statistic" and note is None:
            note = "a threshold parameter, not a statistic" if sample == "parameter" else "a single observation, not a statistic"
        f = Fact(
            id=fid, label=label, value=value, unit=Unit(unit), n=n, note=note, **flag_kw,
            provenance=Provenance(
                data_source=self.cfg.data_source,
                date_range=DateRange(start=start, end=end), as_of=self.as_of,
                cost_convention=self.cfg.cost_convention, computed_by=self.component,
                computed_at=self.computed_at, universe=self.cfg.universe_id,
                level=EvidenceLevel(level),
            ),
        )
        self.facts.append(f)
        self._ids.add(fid)
        return f

    def as_dicts(self) -> list[dict[str, Any]]:
        """The fact table handed to the gateway — the ONLY numbers a model may cite."""
        return [{"id": f.id, "label": f.label, "value": f.value, "unit": f.unit.value, "n": f.n,
                 "sample_flag": f.sample_flag.value}
                for f in self.facts]
