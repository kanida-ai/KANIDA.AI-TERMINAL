"""
Grading — Right / Wrong / Inconclusive, per finding type, FROZEN at publication.

Spec addendum 4 / principle 5: the horizon and the criteria are decided BEFORE a finding is
published and stored with it (`GradingRule`), never after the event. When the horizon
completes, `evaluate()` reads the frozen rule, measures the realised outcome on a frame
sealed at the grading date, and returns a verdict plus the realised numbers as facts.

The one convention shared by every kind: a realised outcome inside ±hurdle is
**Inconclusive** — the transaction-cost hurdle is the band inside which nobody can claim
to have been right. Entry/exit conventions are those of `data.py` (next open -> horizon
close), the same ones the base rates were computed on.

Every verdict is SYMMETRIC in the graded metric (S1 audit C2/C3): Right beyond the hurdle
one way, Wrong beyond it the other way, Inconclusive inside. No kind may add a second
condition that only makes "Right" easier — the old `theme_watch` rule ("Right if the
sector lagged OR dropped out of the top three") graded a bottom-four sector's rejection
Right 96% of the time regardless of outcome. Rank at the horizon is still minted as a
realised FACT; it is no longer a verdict input.

The rule text is versioned by the CONTENT HASH of this file (`GRADING_RULES_VERSION`,
S1 audit P2), and a rule's `spec` must be complete at build time — `evaluate()` never
reaches for the live config, so a config change after publication cannot change a grade.

S1 third audit:
  * N3 — the kind is chosen ON THE DECISION. A pair `watch` ("does not clear costs reliably;
    a watch, not a call") is graded `pair_watch`: Right if the spread trade lost more than
    twice the hurdle, Wrong if it paid more than twice the hurdle. It was graded
    `pair_convergence` — a claim the card never made — and a losing spread made a non-call
    "Wrong". Same device as the theme template's watch -> `theme_watch`.
  * N5 — when the horizon has completed and the outcome still cannot be measured (a
    synthetic open at entry, a corporate-action hole inside the window, more than a fifth of
    a sector's names unresolved) the finding is closed with a `void` grade that carries the
    reason. Void is not Right / Wrong / Inconclusive and is not counted; before this the
    finding stayed `pending` forever.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from ..schemas import EvidenceLevel, GradingKind, GradingRule, Verdict
from .config import GRADING_RULES_SEMVER, ResearchConfig
from .data import MarketData
from .facts import FactSet

_H = "the cost hurdle"


def _own_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


#: `grading_rules@<semver>+code.<hash of grading.py>` — a rule is attributable to the exact
#: evaluator text that will judge it.
GRADING_RULES_VERSION = f"grading_rules@{GRADING_RULES_SEMVER}+code.{_own_hash()}"

#: Every key the evaluator reads for a kind. `build_rule` refuses an incomplete spec, so a
#: frozen rule can never depend on a config value that was not frozen with it.
REQUIRED_SPEC: dict[GradingKind, tuple[str, ...]] = {
    GradingKind.directional_call: ("subject_kind", "direction"),
    GradingKind.no_trade_call: ("subject_kind",),
    GradingKind.theme_call: ("sector", "members", "window", "min_names"),
    GradingKind.theme_watch: ("sector", "members", "window", "min_names"),
    GradingKind.rotation_reject: ("sector", "members", "window", "min_names"),
    GradingKind.anomaly_move: ("symbol", "move_pct"),
    GradingKind.pair_convergence: ("a", "b", "direction"),
    GradingKind.pair_watch: ("a", "b", "direction"),
}


def build_rule(kind: GradingKind | str, *, horizon: int, hurdle_pct: float,
               spec: dict[str, Any], frozen_at: datetime, subject: str) -> GradingRule:
    """The frozen rule text + parameters for one finding type."""
    kind = GradingKind(kind)
    missing = [k for k in REQUIRED_SPEC[kind] if k not in spec]
    if missing:
        raise ValueError(f"grading rule {kind.value} for {subject}: spec is missing {missing} — "
                         "a rule must be complete when it is frozen")
    if spec.get("subject_kind") == "stock" and "symbol" not in spec:
        raise ValueError(f"grading rule {kind.value} for {subject}: a stock rule needs its symbol")
    if spec.get("subject_kind") == "market" and "nifty50_only" not in spec:
        raise ValueError(f"grading rule {kind.value} for {subject}: a market rule needs nifty50_only")
    h = f"{horizon} session{'s' if horizon != 1 else ''}"
    if kind == GradingKind.directional_call:
        direction = spec.get("direction", "long")
        metric = (f"{subject}: {direction} move from the next session's open to the close "
                  f"{h} later, in percent")
        right = f"Right if the move in the called direction exceeds {_H}"
        wrong = f"Wrong if the move against the called direction exceeds {_H}"
    elif kind == GradingKind.no_trade_call:
        metric = (f"{subject}: the move a long trade would have made from the next session's "
                  f"open to the close {h} later, in percent")
        right = f"Right if the avoided trade would have lost more than {_H}"
        wrong = f"Wrong if the avoided trade would have cleared {_H}"
    elif kind == GradingKind.theme_call:
        metric = (f"{subject}: equal-weight sector return minus equal-weight market return "
                  f"(sectors with at least the minimum number of names), next open to the close {h} later, in percent")
        right = f"Right if the sector beat the market by more than {_H}"
        wrong = f"Wrong if the sector lagged the market by more than {_H}"
    elif kind == GradingKind.theme_watch:
        metric = (f"{subject}: equal-weight sector return minus equal-weight market return "
                  f"(sectors with at least the minimum number of names), next open to the close {h} later, in percent; "
                  "the sector's relative-strength rank at the horizon is recorded as a fact, not judged")
        right = f"Right if it did not turn into a cycle: the sector lagged the market by more than {_H}"
        wrong = f"Wrong if it was a cycle after all: the sector beat the market by more than {_H}"
    elif kind == GradingKind.rotation_reject:
        metric = (f"{subject}: equal-weight sector return minus equal-weight market return "
                  f"(sectors with at least the minimum number of names), next open to the close {h} later, in percent; "
                  "the sector's relative-strength rank at the horizon is recorded as a fact, not judged")
        right = f"Right if the flip faded: the sector lagged the market by more than {_H}"
        wrong = f"Wrong if the flip was a rotation: the sector beat the market by more than {_H}"
    elif kind == GradingKind.anomaly_move:
        metric = (f"{subject}: absolute close-to-close move from the signal close to the close "
                  f"{h} later, in percent")
        right = "Right if the stock had moved at least the stated threshold, either way, by then"
        wrong = f"Wrong if the move fell short of the threshold by more than {_H}"
    elif kind == GradingKind.pair_convergence:
        metric = (f"{subject}: the spread trade's return (long the cheap leg, short the rich leg) "
                  f"from the next session's open to the close {h} later, in percent")
        right = f"Right if the spread converged by more than twice {_H} (two legs)"
        wrong = f"Wrong if the spread widened by more than twice {_H}"
    elif kind == GradingKind.pair_watch:
        metric = (f"{subject}: the spread trade's return (long the cheap leg, short the rich leg) "
                  f"from the next session's open to the close {h} later, in percent — the trade the card "
                  "declined to call")
        right = f"Right if it was not worth calling: the spread trade lost more than twice {_H} (two legs)"
        wrong = f"Wrong if it was a call after all: the spread trade paid more than twice {_H}"
    else:  # pragma: no cover
        raise ValueError(f"unknown grading kind {kind}")
    inconclusive = f"Inconclusive if the outcome finished inside {_H} either way"
    if kind == GradingKind.anomaly_move:
        inconclusive = f"Inconclusive if the move fell short of the threshold by no more than {_H}"
    if kind in (GradingKind.pair_convergence, GradingKind.pair_watch):
        inconclusive = f"Inconclusive if the spread trade finished inside twice {_H} either way"
    return GradingRule(
        kind=kind, horizon_sessions=horizon, hurdle_pct=hurdle_pct, metric=metric,
        right=right, wrong=wrong, inconclusive=inconclusive, frozen_at=frozen_at,
        rule_version=GRADING_RULES_VERSION,
        spec={k: v for k, v in spec.items()},
    )


@dataclass
class GradeResult:
    verdict: Verdict
    facts: FactSet
    due_session: str
    void_reason: Optional[str] = None      # set iff verdict == Verdict.void (N5)


class _Unmeasurable(Exception):
    """The horizon is complete but the outcome has a hole in it (N5). Carries the reason."""


def _band(x: float, hurdle: float) -> Verdict:
    if x > hurdle:
        return Verdict.right
    if x < -hurdle:
        return Verdict.wrong
    return Verdict.inconclusive


def _fwd(md: MarketData, symbol: str, d0: str, h: int) -> Optional[float]:
    """f{h} for (symbol, d0) inside the grading seal; None if not yet resolved."""
    col = f"f{h}"
    if col not in md.df:
        raise ValueError(f"horizon {h} is not a computed forward column")
    rows = md.df[(md.df["symbol"] == symbol) & (md.df["d"] == d0)]
    if rows.empty:
        return None
    v = rows.iloc[0][col]
    return None if pd.isna(v) else float(v)


def _market_fwd(md: MarketData, d0: str, h: int, *, nifty50_only: bool) -> Optional[tuple[float, int]]:
    rows = md.df[md.df["d"] == d0]
    if nifty50_only:
        rows = rows[rows["in_nifty50"] == 1]
    v = rows[f"f{h}"].dropna()
    if v.empty or len(v) < 0.8 * len(rows):
        return None
    return float(v.mean()), int(len(v))


def theme_universe(md: MarketData, d: str, min_names: int) -> pd.DataFrame:
    """
    The rows the theme templates call "the market": every name in a sector that has at least
    `min_names` names on session `d`. The base rates (library._sector_tables) and the grader
    use THIS definition, so what is graded is what was measured (S1 audit C6).
    """
    df = md.df.dropna(subset=["sector"])
    tdy = df[df["d"] == d]
    big = [s for s, n in tdy.groupby("sector")["symbol"].nunique().items() if n >= min_names]
    return df[df["sector"].isin(big)]


def _sector_rank_on(md: MarketData, d: str, window: int, min_names: int) -> pd.Series:
    """Relative-strength rank of every sector on session `d` (causal, as in the theme template)."""
    d2 = theme_universe(md, d, min_names)
    sret = d2.groupby(["d", "sector"])["ret"].mean().unstack()
    mkt = d2.groupby("d")["ret"].mean()
    rel = sret.rolling(window).sum().sub(mkt.rolling(window).sum(), axis=0)
    return rel.loc[d].rank(ascending=False)


def _spec_int(spec: dict[str, Any], key: str) -> int:
    if key not in spec:
        raise ValueError(f"frozen rule is missing {key!r}; the grader does not read live config")
    return int(spec[key])


def evaluate(rule: GradingRule, *, finding_slug: str, edition_date: str, md: MarketData,
             cfg: ResearchConfig, graded_at: Optional[datetime] = None) -> Optional[GradeResult]:
    """
    Apply a FROZEN rule on a frame sealed at the grading date. Returns None while the
    horizon is incomplete. Every realised number is minted as a fact. `cfg` is used for
    fact provenance (data source, cost convention) ONLY — never for a rule parameter.

    N5: once the horizon has completed inside the seal (`due <= md.as_of`) and the outcome
    is still a hole, the result is a `void` grade carrying the reason — never a silent None
    that leaves the finding pending forever.
    """
    h = int(rule.horizon_sessions)
    H = float(rule.hurdle_pct)
    spec = dict(rule.spec)
    if edition_date not in md.sessions:
        return None
    due = md.session_after(edition_date, h)
    if due is None:
        return None
    fs = FactSet(finding_slug=f"{finding_slug}_grade", cfg=cfg, as_of=md.as_of,
                 period_start=edition_date, period_end=due, component="grader",
                 computed_at=graded_at)
    try:
        verdict = _judge(rule, spec, h=h, H=H, edition_date=edition_date, due=due, md=md, fs=fs)
    except _Unmeasurable as e:
        if due > md.as_of:            # pragma: no cover — a due session inside the seal is <= as_of by construction
            return None
        reason = str(e)
        fs.add("void_reason", "why the outcome could not be measured once the horizon completed", reason, "text")
        fs.add("hurdle", "round-trip hurdle (costs plus slippage both ways) the verdict would have been judged against",
               H * 100, "bps")
        return GradeResult(verdict=Verdict.void, facts=fs, due_session=due, void_reason=reason)
    fs.add("hurdle", "round-trip hurdle (costs plus slippage both ways) the verdict was judged against", H * 100, "bps")
    return GradeResult(verdict=verdict, facts=fs, due_session=due)


def _judge(rule: GradingRule, spec: dict[str, Any], *, h: int, H: float, edition_date: str, due: str,
           md: MarketData, fs: FactSet) -> Verdict:
    """The per-kind evaluator. Raises `_Unmeasurable` when the outcome has a hole in it."""
    kind = rule.kind

    if kind in (GradingKind.directional_call, GradingKind.no_trade_call):
        if spec.get("subject_kind") == "market":
            got = _market_fwd(md, edition_date, h, nifty50_only=bool(spec["nifty50_only"]))
            if got is None:
                raise _Unmeasurable("fewer than four in five of the market's names have a resolved outcome over the window "
                                    "(data holes: synthetic opens, corporate-action days, glitch bars)")
            r, n = got
            fs.add("realized", "realised equal-weight market move, next open to horizon close",
                   r * 100, "pct", n=n, level=EvidenceLevel.whole_market)
        else:
            r = _fwd(md, str(spec["symbol"]), edition_date, h)
            if r is None:
                raise _Unmeasurable(f"{spec['symbol']}: no measurable outcome from the next open to the horizon close "
                                    "(a synthetic open at entry, or a corporate-action / glitch bar inside the window)")
            fs.add("realized", "realised move, next open to horizon close", r * 100, "pct",
                   sample="observation", level=EvidenceLevel.same_stock)
        x = r * 100
        if kind == GradingKind.directional_call and spec.get("direction") == "short":
            x = -x
        if kind == GradingKind.no_trade_call:
            x = -x           # "the avoided trade lost" is the Right direction
        verdict = _band(x, H)

    elif kind in (GradingKind.theme_call, GradingKind.theme_watch, GradingKind.rotation_reject):
        members = list(spec.get("members") or [])
        window, min_names = _spec_int(spec, "window"), _spec_int(spec, "min_names")
        uni = theme_universe(md, edition_date, min_names)
        rows = uni[uni["d"] == edition_date]
        sec = rows[rows["symbol"].isin(members)][f"f{h}"].dropna()
        mk = rows[f"f{h}"].dropna()
        if sec.empty or mk.empty or len(sec) < 0.8 * len(members):
            raise _Unmeasurable(f"{spec['sector']}: more than a fifth of the sector's names have no resolved outcome over "
                                "the window (data holes: synthetic opens, corporate-action days, glitch bars)")
        excess = (float(sec.mean()) - float(mk.mean())) * 100
        fs.add("sector_move", "realised equal-weight sector move, next open to horizon close",
               float(sec.mean()) * 100, "pct", n=int(len(sec)), level=EvidenceLevel.sector)
        fs.add("market_move", "realised equal-weight market move over the same window",
               float(mk.mean()) * 100, "pct", n=int(len(mk)), level=EvidenceLevel.whole_market)
        fs.add("excess", "sector minus market over the horizon", excess, "pct",
               n=int(len(sec)), level=EvidenceLevel.sector)
        if kind != GradingKind.theme_call:
            # Recorded, not judged (S1 audit C2): a bottom-four sector is top-three five
            # sessions later 4% of the time, so "dropped out of the top three" was a verdict
            # the subject satisfied by construction. Withheld, never zero, when the sector
            # cannot be ranked at the horizon (second audit A6: no zero sentinels).
            ranks = _sector_rank_on(md, due, window, min_names)
            rk = ranks.get(str(spec["sector"]))
            if rk is not None and not pd.isna(rk):
                fs.add("rank_at_horizon", "sector relative-strength rank at the horizon (recorded, not judged)",
                       int(rk), "count", level=EvidenceLevel.sector)
        if kind == GradingKind.theme_call:
            verdict = _band(excess, H)
        else:
            # The claim was "NOT a cycle / NOT a rotation": Right if the sector lagged by more
            # than the hurdle, Wrong if it beat by more than the hurdle. Symmetric (C3).
            verdict = _band(-excess, H)

    elif kind == GradingKind.anomaly_move:
        sym = str(spec["symbol"])
        # The RULE's horizon (second audit A8) — never a fixed week.
        col = f"r{h}cc"
        if col not in md.df:
            raise ValueError(f"horizon {h} is not a computed close-to-close window")
        rows = md.df[(md.df["symbol"] == sym) & (md.df["d"] == edition_date)]
        if rows.empty or pd.isna(rows.iloc[0][col]):
            raise _Unmeasurable(f"{sym}: no measurable close-to-close move across the window "
                                "(a corporate-action / glitch bar inside it)")
        mv = abs(float(rows.iloc[0][col])) * 100
        thr = float(spec["move_pct"])
        fs.add("week_move", f"absolute close-to-close move from the signal close to the close {h} sessions later",
               mv, "pct", sample="observation", level=EvidenceLevel.same_stock)
        if mv >= thr:
            verdict = Verdict.right
        elif mv >= thr - H:
            verdict = Verdict.inconclusive
        else:
            verdict = Verdict.wrong

    elif kind in (GradingKind.pair_convergence, GradingKind.pair_watch):
        a, b = str(spec["a"]), str(spec["b"])
        fa, fb = _fwd(md, a, edition_date, h), _fwd(md, b, edition_date, h)
        if fa is None or fb is None:
            raise _Unmeasurable(f"{a}/{b}: a leg has no measurable outcome from the next open to the horizon close "
                                "(a synthetic open at entry, or a corporate-action / glitch bar inside the window)")
        sign = -1.0 if spec.get("direction") == "short_a_long_b" else 1.0
        pnl = sign * (np.log1p(fa) - np.log1p(fb)) * 100
        fs.add("spread_trade", "spread trade return, long the cheap leg and short the rich leg",
               float(pnl), "pct", sample="observation", level=EvidenceLevel.same_stock)
        if kind == GradingKind.pair_convergence:
            verdict = _band(float(pnl), 2 * H)
        else:
            # N3: the card said "not a call". Right if the declined trade lost more than
            # costs, Wrong if it paid more than costs. Symmetric, like theme_watch.
            verdict = _band(-float(pnl), 2 * H)

    else:  # pragma: no cover
        raise ValueError(f"no evaluator for {kind}")
    return verdict
