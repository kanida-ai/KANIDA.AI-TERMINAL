"""
The two gates of the experiment loop, both built from P1's `engine/governance.py` GateSet and
the Constitution's gauntlet, both returning the measured value next to the bar (a gate that
only says "passed" is not evidence).

1. WORTH TESTING (spec: evidence strength + novelty + cost hurdle — "not just any finding").
   A variant of an S1 finding's family may receive virtual capital only if, on the sealed
   history, it clears the Constitution's DISCOVERY gauntlet — expectancy net of costs, the
   same at twice the slippage, an edge over the same window's whole pool, a day-blocked
   placebo — AND holds on the trailing validation window on its own (NDP: mine on the past,
   promote only what held after), AND is implementable, novel, and comes from a card whose
   evidence strength cleared the bar. The family-wise bar over the counted trials, the
   pre-discovery-end window alone at twice the slippage, and the cluster-robust t are
   ADVISORY here: recorded and published, required for graduation. Significance gates
   promotion, not survival (governance.py's argument, kept).

2. GRADUATION (spec addenda 5/9): champion/challenger only. The challenger's FORWARD virtual
   record (the only data that had no say in choosing it) must show a positive OOS edge on at
   least the Constitution's minimum n, at twice the slippage, beat the day-blocked placebo and
   the cluster-robust t, be rated Keep by the arena's constitutional score, and beat the
   incumbent on net return with no worse drawdown. The output is a PROPOSAL for a human; while
   the Constitution is unsigned the `constitution_signed` gate fails and nothing can promote.
   Never by narrative.
"""
from __future__ import annotations

from dataclasses import dataclass
from statistics import NormalDist
from typing import Optional

import numpy as np

from ..engine.governance import Constitution, GateSet
from .book import DRAWDOWN_CONVENTION, BookRun
from .config import ExperimentConfig
from .hypotheses import VariantEvidence, ReplayStats, t_critical

# ── the arena's constitutional score (Kanida_Falcon/arena/arena.py, verbatim constants) ──
MIN_CONF, CONF_FULL, SCORE_WIN, DRIFT_RECENT = 5, 40, 20, 8
TEST_WT, VAR_PEN, DRIFT_PEN = 0.25, 0.3, 0.5


def constitutional_score(closed_net: list[float]) -> tuple[float, float, str]:
    """ETV + confidence + downside-variance + recent-drift -> (weight, score, Keep/Watch/Test/Retire)."""
    n = len(closed_net)
    if n < MIN_CONF:
        return TEST_WT, 0.0, "Test"
    r = np.array(closed_net[-SCORE_WIN:], dtype=float)
    etv = float(r.mean())
    dn = r[r < 0]
    dsd = float(dn.std()) if len(dn) > 1 else (abs(float(dn.mean())) if len(dn) else 0.0)
    drift = float(r[-DRIFT_RECENT:].mean() - etv) if len(r) >= DRIFT_RECENT else 0.0
    sc = etv - VAR_PEN * dsd + DRIFT_PEN * drift
    if sc <= 0:
        return 0.0, sc, "Retire"
    conf = min(1.0, n / CONF_FULL)
    return TEST_WT + (1 - TEST_WT) * conf, sc, ("Keep" if conf >= 0.5 else "Watch")


def check_implementable(direction: str, horizon: int) -> tuple[bool, str]:
    """P1 governance.check_implementable, on the variant's shape."""
    if direction == "short" and horizon > 1:
        return False, ("multi-session short: not executable in the NSE cash segment under the stated delivery "
                       "cost convention; would need stock futures, which Pathfinder does not model")
    return True, "executable under the stated cash-delivery convention"


def _gt(c: Constitution, key: str) -> float:
    return float(c.document["gauntlet"][key])


def _z(c: Constitution) -> float:
    return NormalDist().inv_cdf(1 - _gt(c, "max_placebo_p_value") / 2)


def _t_bar(c: Constitution, signal_days: int) -> float:
    """
    Re-audit N5: the critical |t| for a CR3 cluster-robust t is Student's t with G−1 degrees of
    freedom, G the number of independent signal days — 2.78 on five days, 2.09 on twenty, 1.96
    only in the limit. Infinite (never passed) with fewer than two days.
    """
    return t_critical(_gt(c, "max_placebo_p_value"), max(0, int(signal_days) - 1))


def _v(x: Optional[float]) -> float:
    return float("nan") if x is None else float(x)


def _ok(x: Optional[float], bar: float, *, above: bool = True) -> bool:
    if x is None or not np.isfinite(x):
        return False
    return x > bar if above else x <= bar


def worth_testing_gates(ev: VariantEvidence, *, c: Constitution, xcfg: ExperimentConfig, n_trials: int,
                        evidence_strength: float, novel: bool, novelty_note: str) -> GateSet:
    """
    Every value below is measured on the population the BOOK would have taken (re-audit N1) when
    `ev` was measured with the book's limits — `ev.whole.selection` says which; the equal-weighted
    figure over every firing is on the record as context and gates nothing.
    """
    g = GateSet()
    w, d, t = ev.whole, ev.discovery, ev.trailing
    alpha = _gt(c, "max_placebo_p_value")
    pop = "the book-selected population" if ev.equal_weighted is not None else "every resolved signal"
    g.add("source_evidence_strength", evidence_strength >= xcfg.min_evidence_strength, evidence_strength,
          xcfg.min_evidence_strength, "the S1 card's evidence strength (its usefulness component) must clear the bar")
    g.add("history_sample", w.n >= xcfg.min_n_history, float(w.n), float(xcfg.min_n_history),
          f"cases on the whole sealed history ({pop}; {w.n_fired} fired, {w.n_skipped} the book could not take); "
          "below the bar the number is labelled, not tested")
    g.add("history_expectancy_net", _ok(w.expectancy_net, _gt(c, "min_expectancy_pct_net")), _v(w.expectancy_net),
          _gt(c, "min_expectancy_pct_net"),
          f"expectancy per trade net of costs and slippage on the whole sealed history — {pop}, the strategy the book trades")
    g.add("history_expectancy_2x_slippage", _ok(w.expectancy_2x_net, _gt(c, "min_expectancy_pct_net_at_2x_slippage")),
          _v(w.expectancy_2x_net), _gt(c, "min_expectancy_pct_net_at_2x_slippage"),
          "an edge that only exists at the friendly slippage assumption is a cost artefact")
    g.add("history_edge_vs_baseline", _ok(w.edge_pct, _gt(c, "min_edge_vs_baseline_pct")), _v(w.edge_pct),
          _gt(c, "min_edge_vs_baseline_pct"), "must beat the same universe over the same window under the same exits")
    g.add("history_placebo", _ok(w.placebo_p, alpha, above=False), _v(w.placebo_p), alpha,
          f"day-blocked permutation p-value ({w.placebo_draws} draws, {w.placebo_convention}"
          + (f", ±{2 * w.placebo_se:.3f} at two standard errors" if w.placebo_se is not None else "")
          + "): a screen, not a significance claim")
    g.add("trailing_sample", t.n >= xcfg.min_n_trailing, float(t.n), float(xcfg.min_n_trailing),
          f"cases on the trailing validation window (after {xcfg.discovery_end}; {pop})")
    g.add("trailing_expectancy_net", _ok(t.expectancy_net, 0.0), _v(t.expectancy_net), 0.0,
          "promote only what held: expectancy net of costs on the trailing window on its own")
    g.add("trailing_expectancy_2x_slippage", _ok(t.expectancy_2x_net, 0.0), _v(t.expectancy_2x_net), 0.0,
          "and it must survive twice the slippage there too")
    impl_ok, impl_note = check_implementable(ev.variant.direction, ev.variant.horizon)
    g.add("implementable_under_cost_convention", impl_ok, None, None, impl_note)
    g.add("novelty", novel, None, None, novelty_note)
    g.add("has_invalidation_not_a_target", True, None, None,
          "the version's frozen grading rule states what makes it wrong (a period more than one standard error "
          "below zero); there is no price to hope for")
    # advisory — recorded, published, required for graduation
    adjusted = alpha / max(1, n_trials)
    g.add("family_wise_significance", _ok(w.placebo_p, adjusted, above=False), _v(w.placebo_p), adjusted,
          f"the bar the best of {n_trials} counted trials — every trial this family ever had, across every retry "
          "(re-audit N4) — must clear to be called significant; advisory here", fatal=False)
    g.add("discovery_window_alone_2x_slippage", _ok(d.expectancy_2x_net, 0.0), _v(d.expectancy_2x_net), 0.0,
          f"expectancy at twice the slippage on the window ending {xcfg.discovery_end} alone; advisory here", fatal=False)
    tb = _t_bar(c, w.signal_days)
    g.add("history_cluster_significance", _ok(w.cluster_t, tb), _v(w.cluster_t), tb,
          f"CR3 cluster-robust t on {w.signal_days} independent signal days, not on {w.n} trades, against Student's t "
          f"with {max(0, w.signal_days - 1)} degrees of freedom; advisory here", fatal=False)
    # re-audit N3: the trailing window's persistence must not be one day's doing
    g.add("trailing_expectancy_without_best_day", _ok(t.expectancy_without_best_day_net, 0.0),
          _v(t.expectancy_without_best_day_net), 0.0,
          (f"expectancy net of costs on the trailing window with its single best signal day removed"
           + (f" (the three best days carry {t.top3_days_share_pct:.0f}% of the window's net P&L)"
              if t.top3_days_share_pct is not None else " (the window's total net P&L is not positive)")
           + "; advisory here"), fatal=False)
    return g


@dataclass(frozen=True)
class ForwardRecord:
    """The version's whole forward record across its periods — the OOS the graduation gate reads."""
    net_returns: tuple[float, ...]
    signal_dates: tuple[str, ...]
    total_return_pct: Optional[float]
    max_drawdown_pct: float
    placebo_p: Optional[float]
    placebo_draws: int
    cluster_t: Optional[float]
    #: re-audit N2: the forward null is the FIXED-DAY stock permutation; `placebo_kind` names it, and
    #: `min_signal_days` is the floor below which neither the null nor the cluster t is read.
    placebo_kind: str = "fixed-day stock permutation"
    min_signal_days: int = 0
    placebo_se: Optional[float] = None

    @property
    def n(self) -> int:
        return len(self.net_returns)

    @property
    def signal_days(self) -> int:
        return len(set(self.signal_dates))


def graduation_gates(fr: ForwardRecord, *, incumbent: Optional[BookRun], direction: str, horizon: int,
                     c: Constitution, slippage_pct: float) -> GateSet:
    g = GateSet()
    signed = c.is_signed
    g.add("constitution_signed", signed, None, None,
          (f"Constitution {c.version} is signed ({c.signature_status})" if signed else
           f"Constitution {c.version}: {c.signature_status}; nothing may be promoted until a human signs it "
           "(signed_by, signed_at and the sha256 of the document they signed)"))
    r = np.array(fr.net_returns, dtype=float)
    mean = float(r.mean()) if r.size else None
    g.add("oos_sample_size", fr.n >= int(_gt(c, "min_n_for_promotion")), float(fr.n), _gt(c, "min_n_for_promotion"),
          "closed forward virtual trades; a sample this small is labelled, not promoted")
    g.add("oos_expectancy_net", _ok(mean, 0.0), _v(mean), 0.0, "the forward edge, net, on data that had no say in choosing the rule")
    g.add("oos_expectancy_2x_slippage", _ok(None if mean is None else mean - 2 * slippage_pct, 0.0),
          _v(None if mean is None else mean - 2 * slippage_pct), 0.0, "and it must survive twice the slippage")
    alpha = _gt(c, "max_placebo_p_value")
    G = fr.signal_days
    few = G < fr.min_signal_days
    floor_note = (f" — INSUFFICIENT: {G} forward signal days, the floor is {fr.min_signal_days}; not passed, not failed"
                  if few else "")
    g.add("oos_placebo", _ok(fr.placebo_p, alpha, above=False), _v(fr.placebo_p), alpha,
          f"{fr.placebo_kind} p-value over the forward record: the version's own signal days held fixed, as many random "
          f"resolved names drawn from each day's pool as the book took that day ({fr.placebo_draws} draws, plain-mean "
          f"convention, the one the record is graded on){floor_note}", insufficient=few)
    tb = _t_bar(c, G)
    g.add("oos_cluster_significance", _ok(fr.cluster_t, tb), _v(fr.cluster_t), tb,
          f"CR3 cluster-robust t on {G} independent forward signal days against Student's t with "
          f"{max(0, G - 1)} degrees of freedom{floor_note}", insufficient=few)
    _, sc, roster = constitutional_score(list(fr.net_returns))
    g.add("arena_roster_keep", roster == "Keep", sc, 0.0,
          f"the arena's constitutional score (ETV, downside variance, drift, confidence) rates the record {roster}")
    if incumbent is None or incumbent.total_return_pct is None:
        g.add("beats_incumbent_net_return", False, _v(fr.total_return_pct), None, "no incumbent record over the forward window")
        g.add("no_worse_drawdown_than_incumbent", False, fr.max_drawdown_pct, None, "no incumbent record over the forward window")
    else:
        inc_ret = float(incumbent.total_return_pct)
        inc_dd = incumbent.drawdowns()[0]
        g.add("beats_incumbent_net_return", _ok(fr.total_return_pct, inc_ret), _v(fr.total_return_pct), inc_ret,
              f"the version's per-period virtual book returns compounded (each period a fresh book of the same capital; "
              f"a period's horizon tail overlaps the next period's first sessions) vs the incumbent "
              f"({incumbent.selection_rule}) over the same window")
        g.add("no_worse_drawdown_than_incumbent", fr.max_drawdown_pct <= inc_dd, fr.max_drawdown_pct, inc_dd,
              f"worst drawdown of that compounded curve vs the incumbent's, both as {DRAWDOWN_CONVENTION}")
    impl_ok, impl_note = check_implementable(direction, horizon)
    g.add("implementable_under_cost_convention", impl_ok, None, None, impl_note)
    return g


def proposal_status(g: GateSet) -> Optional[str]:
    """
    A proposal exists only when every gate but the signature passes. Its status names the
    human act it waits for. None = not proposable (the record says which gates failed).
    """
    others = [x for x in g.gates if x.name != "constitution_signed"]
    if not all(x.passed for x in others):
        return None
    signed = next(x for x in g.gates if x.name == "constitution_signed").passed
    return "proposed_awaiting_human" if signed else "blocked_unsigned_constitution"


def gate_rows(g: GateSet) -> list[dict]:
    return [{"name": x.name, "passed": bool(x.passed),
             "value": (None if x.value is None or not np.isfinite(x.value) else round(float(x.value), 6)),
             "bar": (None if x.bar is None or not np.isfinite(x.bar) else round(float(x.bar), 6)),
             "statement": x.statement, "fatal": bool(x.fatal), "insufficient": bool(x.insufficient)} for x in g.gates]


def summarize(stats: ReplayStats) -> dict:
    return {k: (None if v is None else (round(v, 6) if isinstance(v, float) else v)) for k, v in {
        "window_start": stats.window_start, "window_end": stats.window_end, "n": stats.n,
        "signal_days": stats.signal_days, "hit_pct": stats.hit_pct, "median_net": stats.median_net,
        "expectancy_net": stats.expectancy_net, "expectancy_2x_net": stats.expectancy_2x_net,
        "baseline_net": stats.baseline_net, "edge_pct": stats.edge_pct, "baseline_n": stats.baseline_n,
        "placebo_p": stats.placebo_p, "placebo_draws": stats.placebo_draws, "cluster_t": stats.cluster_t,
        # re-audit N1 / N3 / N8
        "selection": stats.selection, "n_fired": stats.n_fired, "n_skipped": stats.n_skipped,
        "top3_days_share_pct": stats.top3_days_share_pct, "best_day": stats.best_day,
        "expectancy_without_best_day_net": stats.expectancy_without_best_day_net,
        "placebo_se": stats.placebo_se, "placebo_convention": stats.placebo_convention,
    }.items()}


def summarize_evidence(ev: VariantEvidence) -> dict:
    """Every window of a variant's evidence, plus the equal-weighted context figure when the book selected."""
    out = {"whole": summarize(ev.whole), "discovery": summarize(ev.discovery), "trailing": summarize(ev.trailing)}
    if ev.equal_weighted is not None:
        out["equal_weighted"] = summarize(ev.equal_weighted)
    return out
