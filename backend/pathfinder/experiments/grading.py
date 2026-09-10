"""
Grading an experiment period — Right / Wrong / Inconclusive / void, FROZEN before the period opens.

Spec principle 5 / addendum 4, applied to experiments: the metric, the horizon, the minimum
number of trades and the band are written into `ExperimentGradingRule` when the version is
created, stored with it, and read back by `judge()` when the period completes. `judge()` never
reads live config; a config change after the version opened cannot change a grade.

The claim a version makes is: "trades under this rule have a positive expectancy net of costs".
The graded metric is therefore the period's realised MEAN net P&L per closed trade, on the
same convention the expectation was computed on (next open -> horizon close, costs + slippage
both ways). The band inside which nobody may claim to have been right is ONE STANDARD ERROR
of that mean — the larger of the plain estimate and the cluster-robust one on signal days
(trades that fire together are one event; see `_band` for why the larger is taken) — and the
choice is recorded as a fact. The verdict is SYMMETRIC in the metric (S1 audit C2/C3):

    Right         mean > +band        the edge showed up
    Wrong         mean < -band        the edge failed
    Inconclusive  otherwise, or fewer closed trades than the frozen minimum
    void          no trade closed in the period (the rule did not fire, or no entry was
                  verifiable) — closed with the reason, never counted, never pending forever (N5)

`compare()` then states expected vs actual as a computed category — stronger / weaker /
failed / inconclusive / void — which the narrator may only repeat.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np

from ..research.facts import FactSet
from ..schemas import ComparisonCategory, EvidenceLevel, ExperimentGradingRule, Verdict
from .book import BookRun
from .hypotheses import Variant

GRADING_SEMVER = "1.0.0"


def _own_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


GRADING_RULES_VERSION = f"experiment_grading@{GRADING_SEMVER}+code.{_own_hash()}"


#: Audit finding 4 — frozen CUMULATIVE rules, so a losing version cannot hide behind a wide per-period
#: band forever and a rule that stops firing cannot be tracked forever. Both are written into the
#: version's rule and read back from it. FOUNDER INPUTS (stubs).
CUMULATIVE_MIN_TRADES = 10          # closed resolved forward trades before the cumulative t may bury
CUMULATIVE_MIN_SIGNAL_DAYS = 5      # independent signal days before the cluster t is believed
MAX_CONSECUTIVE_VOID = 4            # periods in a row with no closed trade -> the rule has stopped firing


def build_experiment_rule(variant: Variant, *, expected_net_pct: float, hurdle_pct: float, min_trades: int,
                          frozen_at: datetime, z: float = 1.959964) -> ExperimentGradingRule:
    h = variant.horizon
    return ExperimentGradingRule(
        kind="experiment_edge", horizon_sessions=h, hurdle_pct=hurdle_pct, min_trades=min_trades,
        metric=(f"mean net P&L per closed virtual trade over the period, in percent: "
                f"{'long' if variant.direction == 'long' else 'short'} from the next session's open to the close "
                f"{h} session{'s' if h != 1 else ''} later, net of {hurdle_pct:.2f}% costs and slippage both ways; "
                f"the band is one standard error of that mean — the larger of the plain estimate and the cluster-robust one on signal days"),
        right="Right if the mean net P&L per trade exceeds one standard error above zero — the edge showed up",
        wrong="Wrong if the mean net P&L per trade lies more than one standard error below zero — the edge failed",
        inconclusive=(f"Inconclusive if the mean lies inside one standard error of zero, or fewer than {min_trades} "
                      "trades closed in the period"),
        void="void if no trade closed in the period (the rule did not fire, or no entry was verifiable): not counted",
        frozen_at=frozen_at, rule_version=GRADING_RULES_VERSION,
        spec={"signature": variant.signature, "direction": variant.direction, "horizon": h,
              "min_trades": int(min_trades), "expected_net_pct": round(float(expected_net_pct), 4),
              "band": "max_of_plain_and_cluster_standard_error",
              # the cumulative rules (audit finding 4), frozen with the version
              "cumulative_bury_t": -float(z), "cumulative_min_trades": CUMULATIVE_MIN_TRADES,
              "cumulative_min_signal_days": CUMULATIVE_MIN_SIGNAL_DAYS, "max_consecutive_void": MAX_CONSECUTIVE_VOID,
              "cumulative": (f"buried if the version's whole forward record — at least {CUMULATIVE_MIN_TRADES} resolved "
                             f"trades on at least {CUMULATIVE_MIN_SIGNAL_DAYS} signal days — has a cluster-robust t below "
                             f"-{z:.2f}, or after {MAX_CONSECUTIVE_VOID} consecutive periods with no closed trade")},
    )


def cumulative_verdict(rule: ExperimentGradingRule, net: np.ndarray, days: np.ndarray, consecutive_void: int
                       ) -> Optional[str]:
    """
    The FROZEN cumulative rule on the version's whole forward record. Returns the burial cause or None.
    Read from `rule.spec` only.
    """
    spec = rule.spec
    if consecutive_void >= int(spec["max_consecutive_void"]):
        return "rule_stopped_firing"
    if net.size >= int(spec["cumulative_min_trades"]) and np.unique(days).size >= int(spec["cumulative_min_signal_days"]):
        mean = float(net.mean())
        resid = net - mean
        uniq = np.unique(days)
        cs = np.array([resid[days == d].sum() for d in uniq], dtype=float)
        se = float(np.sqrt((cs ** 2).sum())) / net.size
        if np.isfinite(se) and se > 0 and mean / se < float(spec["cumulative_bury_t"]):
            return "edge_did_not_persist_oos"
    return None


@dataclass
class PeriodGrade:
    verdict: Verdict
    facts: FactSet
    mean_net: Optional[float]
    band: Optional[float]
    n: int
    signal_days: int
    void_reason: Optional[str] = None


def _band(net: np.ndarray, days: np.ndarray) -> tuple[Optional[float], str]:
    """
    One standard error of the mean — the LARGER of the plain and the cluster-robust (CR0, on
    signal days) estimates. The cluster estimate is the one to believe when trades fire together
    on a few days; but with a handful of clusters it can collapse toward zero when a day's
    trades cancel each other, and a band that shrinks below the plain standard error would turn
    noise into a verdict (S2 test row 06 found exactly that). Taking the larger of the two can
    only make the period harder to call, never easier.
    """
    n = net.size
    if n < 2:
        return None, "undefined (one trade)"
    plain = float(net.std(ddof=1) / np.sqrt(n))
    uniq = np.unique(days)
    if uniq.size >= 2:
        resid = net - net.mean()
        cs = np.array([resid[days == d].sum() for d in uniq], dtype=float)
        cluster = float(np.sqrt((cs ** 2).sum())) / n
        if np.isfinite(cluster) and cluster > plain:
            se, kind = cluster, "cluster-robust standard error on signal days (larger than the plain one)"
        else:
            se, kind = plain, "plain standard error (the cluster-robust one on signal days was no larger)"
    else:
        se, kind = plain, "plain standard error (one signal day: the trades are not independent)"
    if not np.isfinite(se) or se <= 0:
        return None, kind
    return se, kind


def judge(rule: ExperimentGradingRule, run: BookRun, *, fs: FactSet) -> PeriodGrade:
    """
    Apply the FROZEN rule to a completed period's book. Every realised number becomes a fact.
    Only RESOLVED trades are judged (audit finding 7): a trade through a data hole is listed, not graded.
    """
    closed = run.graded
    n = len(closed)
    min_trades = int(rule.spec["min_trades"])
    expected = float(rule.spec["expected_net_pct"])
    fs.add("expected_net", "historical expectation frozen when the version opened: net P&L per trade",
           expected, "pct", sample="parameter")
    fs.add("signals_seen", "signals the rule fired in the period", run.signals_seen, "count")
    fs.add("signals_taken", "positions the book could take under its limits", run.signals_taken, "count")
    if run.unresolved:
        fs.add("unresolved_trades", "closed trades excluded from the verdict because a data hole sat inside their window",
               len(run.unresolved), "count")
    if n == 0:
        reason = ("no trade closed in the period: the rule did not fire, or no entry price was verifiable"
                  if run.signals_taken == 0 else
                  ("every closed trade ran through a data hole and could not be resolved" if run.unresolved
                   else "no position closed inside the seal"))
        fs.add("void_reason", "why the period could not be graded", reason, "text")
        return PeriodGrade(Verdict.void, fs, None, None, 0, 0, void_reason=reason)
    net = np.array([t.pnl_pct_net for t in closed], dtype=float)
    days = np.array([t.signal_date for t in closed])
    mean = float(net.mean())
    band, kind = _band(net, days)
    mdd, cdd = run.drawdowns()
    fs.add("closed_trades", "virtual trades closed in the period", n, "count")
    fs.add("signal_days", "distinct signal sessions those trades came from", int(np.unique(days).size), "count")
    fs.add("actual_net", "realised mean net P&L per closed trade over the period", mean, "pct", n=n,
           level=EvidenceLevel.whole_market)
    fs.add("hit_rate", "share of closed trades that finished positive net of costs", float((net > 0).mean() * 100), "pct", n=n)
    fs.add("worst_trade", "worst closed trade, net", float(net.min()), "pct", sample="observation")
    fs.add("best_trade", "best closed trade, net", float(net.max()), "pct", sample="observation")
    if run.total_return_pct is not None:
        fs.add("book_return", "virtual book return over the period, marked to close", run.total_return_pct, "pct",
               sample="observation")
    fs.add("max_drawdown", "worst peak-to-trough drawdown of the virtual book over the period", mdd, "pct",
           sample="observation")
    fs.add("band_kind", "how the band around zero was measured", kind, "text")
    if band is not None:
        fs.add("band", "one standard error of the realised mean (the band inside which the period is inconclusive)",
               band, "pct", sample="observation")
    fs.add("gap", "realised minus expected net P&L per trade", mean - expected, "pct", sample="observation")
    if n < min_trades or band is None:
        verdict = Verdict.inconclusive
    elif mean > band:
        verdict = Verdict.right
    elif mean < -band:
        verdict = Verdict.wrong
    else:
        verdict = Verdict.inconclusive
    return PeriodGrade(verdict, fs, mean, band, n, int(np.unique(days).size))


def compare(grade: PeriodGrade, expected_net_pct: float) -> ComparisonCategory:
    """Expected vs actual, as a computed category the narrator may only repeat."""
    if grade.verdict == Verdict.void:
        return ComparisonCategory.void
    if grade.verdict == Verdict.wrong:
        return ComparisonCategory.failed
    if grade.verdict == Verdict.inconclusive:
        return ComparisonCategory.inconclusive
    assert grade.mean_net is not None
    return ComparisonCategory.stronger if grade.mean_net >= expected_net_pct else ComparisonCategory.weaker
