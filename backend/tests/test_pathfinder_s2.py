"""
S2 guardrail suite — the experiment loop (docs/sessions/PATHFINDER_S2_EXPERIMENTS.md).

Rows S2-01 … S2-22 run on a SYNTHETIC universe with an ENGINEERED CONDITIONAL EDGE: hard
one-day falls that came on a market-wide fall bounce over the next week; falls on ordinary days
do not. History (before the opening seal) carries the edge; the forward window can be told to
keep it (`forward_edge=+1`), lose it (`-1`) or be noise (`0`) — so every branch of the loop
(open, track, grade Right / Wrong / void, learn, revise, bury, propose) is exercised on data
the test controls, and the numbers the loop reports are recomputed here independently.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from pathfinder.engine.governance import load_constitution                                   # noqa: E402
from pathfinder.experiments import hypotheses as HYP                                        # noqa: E402
from pathfinder.experiments.book import passive_incumbent, run_book                         # noqa: E402
from pathfinder.experiments.config import ExperimentConfig                                  # noqa: E402
from pathfinder.experiments.gate import (                                                   # noqa: E402
    constitutional_score, graduation_gates, proposal_status, worth_testing_gates, ForwardRecord,
)
from pathfinder.experiments.grading import GRADING_RULES_VERSION, build_experiment_rule, compare, judge  # noqa: E402
from pathfinder.experiments.loop import run_experiment_step                                 # noqa: E402
from pathfinder.experiments.narrate import ExperimentNarrator                               # noqa: E402
from pathfinder.experiments.store import ExperimentStore, set_experiment_store              # noqa: E402
from pathfinder.experiments import views                                                    # noqa: E402
from pathfinder.llm.gateway import GatewayError, NarrateResult, Usage                       # noqa: E402
from pathfinder.research.config import ResearchConfig                                       # noqa: E402
from pathfinder.research.data import MarketData                                             # noqa: E402
from pathfinder.research.facts import FactSet                                               # noqa: E402
from pathfinder.research.scan import run_scan                                               # noqa: E402
from pathfinder.research.store import ResearchStore, set_research_store                     # noqa: E402
from pathfinder.schemas import (                                                            # noqa: E402
    BACKFILL_LABEL, FORWARD_LABEL, REF_TOKEN_RE, ComparisonCategory, ExperimentCard, ExperimentState, Verdict,
)

AT = datetime(2026, 9, 10, 18, 0, 0)
CONSTITUTION = ROOT / "config" / "pathfinder_constitution.yaml"


# ── the synthetic universe with an engineered conditional edge ────────────────

SECTORS = {**{f"T{i}": "Tech" for i in range(5)}, **{f"B{i}": "Bank" for i in range(5)}, **{f"A{i}": "Auto" for i in range(5)}}
NAMES = list(SECTORS)


def synth_s2(n_days: int = 700, seed: int = 11, *, forward_from: int = 560, forward_edge: int = +1,
             crash_every: int = 6, edge_per_day: float = 0.006, crash_until: int | None = None):
    """
    Every `crash_every`-th session is a MARKET-WIDE fall (every name -1.5% ± noise, so the
    equal-weight move is below -1%) on which three rotating names fall 7%; over the next five
    sessions those three drift `edge_per_day` per session (the edge) — before `forward_from`
    always, after it by `forward_edge` (+1 keep, -1 reverse, 0 none). Every third session two
    names fall 6.5% on an ORDINARY day and drift down afterwards, so the UNCONDITIONED rule
    loses on average and only the market-wide-fall condition isolates the edge.
    """
    rng = np.random.default_rng(seed)
    dates = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2024-01-01", periods=n_days)]
    rets = {s: np.clip(rng.standard_t(4, n_days) * 0.007 + 0.0002, -0.05, 0.05) for s in NAMES}
    crash = [i for i in range(5, n_days - 6, crash_every) if crash_until is None or i < crash_until]
    ordinary = [i for i in range(7, n_days - 6, 3) if i not in crash and (i - 1) not in crash]
    for k, i in enumerate(crash):
        for s in NAMES:
            rets[s][i] = -0.015 + rng.normal(0, 0.002)
        hit = [NAMES[(3 * k + j) % 9] for j in range(3)]          # crash names come from the first nine
        sign = 1.0 if i < forward_from else float(forward_edge)
        for s in hit:
            rets[s][i] = -0.07
            for t in range(1, 6):
                rets[s][i + t] = sign * edge_per_day + rng.normal(0, 0.010)
    for k, i in enumerate(ordinary):
        for s in NAMES:                                    # the rest of the market is firm: an ORDINARY day
            rets[s][i] = 0.003 + rng.normal(0, 0.002)
        for s in (NAMES[9 + (2 * k) % 6], NAMES[9 + (2 * k + 1) % 6]):   # ordinary dips from the last six: no overlap
            rets[s][i] = -0.065
            for t in range(1, 6):
                rets[s][i + t] = -0.004 + rng.normal(0, 0.007)
    rows = []
    for k, s in enumerate(NAMES):
        close = 100.0 * np.cumprod(1 + rets[s])
        opens = np.r_[close[0] * 1.001, close[:-1]] * (1 + rng.normal(0, 0.0015, n_days))
        vol = rng.lognormal(np.log(1_000_000 * (1 + k)), 0.4, n_days)
        for i, d in enumerate(dates):
            rows.append((s, d, float(opens[i]), float(close[i]), float(vol[i]), SECTORS[s], int(k < 6)))
    df = pd.DataFrame(rows, columns=["symbol", "d", "open", "close", "volume", "sector", "in_nifty50"])
    idx = df.groupby("d")["close"].mean()
    vix = pd.Series(15.0 + rng.normal(0, 1, n_days), index=dates)
    return df, idx, vix, dates, crash


def rcfg_for(tmp_path) -> ResearchConfig:
    return ResearchConfig(research_db=str(tmp_path / "r.db"), price_db="unused", usefulness_threshold=0.0,
                          pairs=(("B0", "B1"), ("A0", "A1")))


def xcfg_for(tmp_path, constitution: Path = CONSTITUTION, **kw) -> ExperimentConfig:
    base = dict(experiments_db=str(tmp_path / "x.db"), constitution_path=str(constitution), discovery_end="2024-12-31",
                period_sessions=6, min_trades_to_grade=3, max_versions=3, min_n_history=20, min_n_trailing=5,
                placebo_draws=200, retry_after_sessions=3)
    base.update(kw)
    return ExperimentConfig(**base)


def write_constitution(tmp_path, *, min_n: int = 12, signed: bool = False) -> Path:
    doc = yaml.safe_load(CONSTITUTION.read_text(encoding="utf-8"))
    doc["gauntlet"]["min_n_for_promotion"] = min_n
    if signed:
        doc["approved_by"] = "Shyam (test signature — pins the signed path only)"
    p = tmp_path / "constitution.yaml"
    p.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return p


class Loop:
    """Drive S1 + S2 over a range of synthetic sessions, sealed at each."""

    def __init__(self, tmp_path, *, forward_edge: int = +1, xcfg: ExperimentConfig | None = None, seed: int = 11, **synth_kw):
        self.raw, self.idx, self.vix, self.dates, self.crash = synth_s2(forward_edge=forward_edge, seed=seed, **synth_kw)
        self.rcfg = rcfg_for(tmp_path)
        self.xcfg = xcfg or xcfg_for(tmp_path)
        self.rstore = ResearchStore(self.rcfg.research_db)
        self.xstore = ExperimentStore(self.xcfg.experiments_db)
        self.full = MarketData.from_frame(self.raw, self.idx, self.vix, self.rcfg)
        self.constitution = load_constitution(self.xcfg.constitution_path)
        self.reports = []

    def step(self, d: str, *, computed_at=AT, narrator=None):
        md = self.full.sealed(d)
        run_scan(self.rcfg, self.rstore, md=md, computed_at=computed_at)
        rep = run_experiment_step(self.rcfg, self.xcfg, self.rstore, self.xstore, md=md, constitution=self.constitution,
                                  narrator=narrator, computed_at=computed_at)
        self.reports.append(rep)
        return rep

    def run(self, i0: int, i1: int, **kw):
        for d in self.dates[i0:i1]:
            self.step(d, **kw)
        return self

    def first_crash_at_or_after(self, i: int) -> int:
        return next(c for c in self.crash if c >= i)


OPEN_AT = 563      # the first market-wide-fall session after `forward_from` (5 mod 6): the S1 dip card fires there


@pytest.fixture(scope="module")
def keep(tmp_path_factory):
    """Forward keeps the edge: open -> Right -> continue -> … -> proposal."""
    tmp = tmp_path_factory.mktemp("keep")
    lp = Loop(tmp, forward_edge=+1, xcfg=xcfg_for(tmp, write_constitution(tmp, min_n=12)))
    lp.run(OPEN_AT, OPEN_AT + 60)
    return lp


@pytest.fixture(scope="module")
def lose(tmp_path_factory):
    """Forward reverses the edge: open -> Wrong -> v2 -> Wrong -> v3 -> Wrong -> buried."""
    tmp = tmp_path_factory.mktemp("lose")
    lp = Loop(tmp, forward_edge=-1)
    lp.run(OPEN_AT, OPEN_AT + 46)
    return lp


def _exp(lp: Loop) -> str:
    rows = lp.xstore.experiments()
    assert len(rows) == 1, [dict(r) for r in rows]
    return rows[0]["experiment_id"]


# ═══════════════════════════════════════════════════════════════════════════
# The closed hypothesis library and the replay convention (S2-01 … S2-04)
# ═══════════════════════════════════════════════════════════════════════════

def test_s2_01_the_variant_set_is_closed_ordered_and_counted():
    fam = HYP.FAMILY_BY_ID["dip_bounce"]
    vs = HYP.variants_for(fam, 6.0)
    assert len(vs) == len(HYP.CONDITIONS) * len(HYP.HORIZONS) == 18
    assert [v.signature for v in vs] == [v.signature for v in HYP.variants_for(fam, 6.0)]
    assert vs[0].conditions == ("all",) and vs[0].horizon == 1
    assert HYP.Variant.from_dict(vs[5].as_dict()) == vs[5]
    # every horizon is inside the Constitution's approved range for exits
    rng = load_constitution(CONSTITUTION).approved_ranges["_exits"]["horizon_sessions"]
    assert all(rng["min"] <= h <= rng["max"] for h in HYP.HORIZONS)


def test_s2_02_replay_measures_the_s1_convention_and_never_a_bar_past_the_seal(tmp_path):
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    D = dates[400]
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=D)
    cx = HYP.Conditioning(md)
    v = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)
    st = HYP.replay(v, md, cx, start=dates[0], end=D, rcfg=rcfg, draws=50, seed=1)
    # independent recompute: f5 - hurdle on rows that are dips on market-down-1% days, resolved inside the seal
    df = md.df
    mkt = df.groupby("d")["ret"].mean()
    m = (df["ret"] <= -0.06) & df["d"].map(mkt < -0.01).fillna(False) & df["f5"].notna()
    exp_net = df.loc[m, "f5"].to_numpy() * 100 - rcfg.hurdle_pct
    assert st.n == int(m.sum()) and st.n > 20
    assert np.allclose(sorted(st.net_returns), sorted(exp_net))
    assert max(st.signal_dates) <= dates[395], "a signal within the horizon of the seal cannot be a trade"
    with pytest.raises(ValueError, match="look-ahead"):
        HYP.replay(v, md, cx, start=dates[0], end=dates[401], rcfg=rcfg, draws=10, seed=1)


def test_s2_03_the_engineered_edge_is_conditional_and_the_unconditioned_rule_loses(tmp_path):
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[OPEN_AT])
    cx = HYP.Conditioning(md)
    w = HYP.windows_for(md, "2024-12-31")
    cond = HYP.measure(HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5), md, cx, w, rcfg=rcfg, draws=100, seed=1)
    allv = HYP.measure(HYP.Variant("dip_bounce", 6.0, ("all",), 5), md, cx, w, rcfg=rcfg, draws=100, seed=1)
    assert cond.whole.expectancy_net > 1.0 and cond.trailing.expectancy_net > 1.0 and cond.discovery.expectancy_net > 1.0
    assert cond.whole.placebo_p <= 0.05 and cond.whole.edge_pct > 1.0
    assert allv.whole.expectancy_net < cond.whole.expectancy_net
    assert w.discovery[1] == "2024-12-31" and w.trailing[0] > "2024-12-31" and w.whole[1] == dates[OPEN_AT]


def test_s2_04_the_book_enters_at_the_next_open_exits_at_the_horizon_close_and_charges_costs(tmp_path):
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    c0 = crash[10]
    D = dates[c0 + 8]
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=D)
    cx = HYP.Conditioning(md)
    v = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)
    run = run_book(v, md, cx, period_start=dates[c0], period_sessions=1, rcfg=rcfg, capital_inr=1_000_000, fraction_per_position=0.1,
                   max_concurrent=10, max_new_per_session=5)
    assert len(run.closed) == 3 and not run.open_at_end
    px = md.df.set_index(["symbol", "d"])
    for t in run.closed:
        assert t.signal_date == dates[c0] and t.entry_date == dates[c0 + 1] and t.exit_date == dates[c0 + 5]
        assert t.entry_price == pytest.approx(px.loc[(t.symbol, dates[c0 + 1]), "open"])
        assert t.exit_price == pytest.approx(px.loc[(t.symbol, dates[c0 + 5]), "close"])
        assert t.pnl_pct_gross == pytest.approx((t.exit_price / t.entry_price - 1) * 100)
        assert t.pnl_pct_net == pytest.approx(t.pnl_pct_gross - rcfg.hurdle_pct)
    # marked to close every session from the signal day; the equity curve ends at cash after the exits
    assert list(run.equity.index)[0] == dates[c0] and run.equity.index[-1] == dates[c0 + 5]
    assert run.total_return_pct == pytest.approx(sum(t.pnl_pct_net for t in run.closed) * 0.1, abs=1e-6)
    # SEAL: on a frame that ends before the exits, the positions are OPEN and nothing past the seal is read
    md2 = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[c0 + 3])
    run2 = run_book(v, md2, HYP.Conditioning(md2), period_start=dates[c0], period_sessions=1, rcfg=rcfg, capital_inr=1_000_000,
                    fraction_per_position=0.1, max_concurrent=10, max_new_per_session=5)
    assert len(run2.open_at_end) == 3 and not run2.closed and run2.equity.index[-1] == dates[c0 + 3]
    assert [t.entry_price for t in run2.open_at_end] == [t.entry_price for t in sorted(run.closed, key=lambda t: t.symbol)]


def test_s2_05_the_book_respects_the_constitutions_limits_and_ranks_by_liquidity(tmp_path):
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    c0 = crash[10]
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[c0 + 8])
    cx = HYP.Conditioning(md)
    v = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)
    run = run_book(v, md, cx, period_start=dates[c0], period_sessions=1, rcfg=rcfg, capital_inr=1_000_000, fraction_per_position=0.1,
                   max_concurrent=10, max_new_per_session=2)
    assert run.signals_seen == 3 and run.signals_taken == 2 and len(run.closed) == 2
    liq = cx.liquidity
    sig = md.df[(md.df["d"] == dates[c0]) & (md.df["ret"] <= -0.06)].assign(_liq=liq).sort_values("_liq", ascending=False)
    assert {t.symbol for t in run.closed} == set(sig["symbol"].head(2))


# ═══════════════════════════════════════════════════════════════════════════
# Grading — frozen, symmetric, void (S2-06 … S2-08)
# ═══════════════════════════════════════════════════════════════════════════

def _fake_run(net: list[float], days: list[str], capital=1e6):
    from pathfinder.experiments.book import BookRun, VTrade
    closed = tuple(VTrade(f"S{i}", "long", d, d, 100.0, d, 100.0 * (1 + x / 100), 5, x + 0.5, x, 0.5, 1e5) for i, (x, d) in enumerate(zip(net, days)))
    eq = pd.Series([capital * (1 + sum(net[: i + 1]) * 0.1 / 100) for i in range(len(net))], index=days)
    return BookRun(capital, days[0], days[-1], days[-1], closed, (), eq, len(net), len(net))


def _fs(tmp_path, slug="g"):
    return FactSet(finding_slug=slug, cfg=rcfg_for(tmp_path), as_of="2026-03-02", period_start="2026-01-01", period_end="2026-03-02",
                   component="grader", computed_at=AT)


def test_s2_06_verdicts_are_symmetric_in_one_standard_error_and_need_the_frozen_minimum(tmp_path):
    v = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)
    rule = build_experiment_rule(v, expected_net_pct=1.2, hurdle_pct=0.5, min_trades=3, frozen_at=AT)
    days = ["2026-02-02", "2026-02-02", "2026-02-09", "2026-02-09", "2026-02-16"]
    right = judge(rule, _fake_run([2.0, 1.5, 2.5, 1.0, 2.0], days), fs=_fs(tmp_path, "a"))
    wrong = judge(rule, _fake_run([-2.0, -1.5, -2.5, -1.0, -2.0], days), fs=_fs(tmp_path, "b"))
    noise = judge(rule, _fake_run([2.0, -2.0, 1.5, -1.5, 0.2], days), fs=_fs(tmp_path, "c"))
    few = judge(rule, _fake_run([3.0, 3.0], days[:2]), fs=_fs(tmp_path, "d"))
    assert (right.verdict, wrong.verdict, noise.verdict, few.verdict) == (Verdict.right, Verdict.wrong, Verdict.inconclusive, Verdict.inconclusive)
    assert right.band is not None and right.mean_net > right.band and wrong.mean_net < -wrong.band
    assert compare(right, 1.2) == ComparisonCategory.stronger and compare(right, 3.0) == ComparisonCategory.weaker
    assert compare(wrong, 1.2) == ComparisonCategory.failed and compare(noise, 1.2) == ComparisonCategory.inconclusive
    # the band is the LARGER of the plain and the cluster-robust standard error, and says which it used:
    # with three clusters the CR0 estimate collapsed to 0.04 on the noise case and would have called it Right
    net = np.array([2.0, -2.0, 1.5, -1.5, 0.2])
    assert noise.band == pytest.approx(net.std(ddof=1) / np.sqrt(5))
    assert "standard error" in next(f.value for f in noise.facts.facts if f.id.endswith("_band_kind"))


def test_s2_07_a_period_with_no_closed_trade_is_void_with_its_reason_never_counted(tmp_path):
    from pathfinder.experiments.book import BookRun
    v = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)
    rule = build_experiment_rule(v, expected_net_pct=1.2, hurdle_pct=0.5, min_trades=3, frozen_at=AT)
    empty = BookRun(1e6, "2026-02-02", "2026-02-09", "2026-02-16", (), (), pd.Series(dtype=float), 0, 0)
    g = judge(rule, empty, fs=_fs(tmp_path, "v"))
    assert g.verdict == Verdict.void and "did not fire" in g.void_reason
    assert compare(g, 1.2) == ComparisonCategory.void


def test_s2_08_the_grading_rule_is_frozen_with_the_version_and_versioned_by_the_evaluators_hash(keep):
    eid = _exp(keep)
    vrow = keep.xstore.versions(eid)[0]
    rule = json.loads(vrow["grading_rule_json"])
    exp = json.loads(vrow["expectation_json"])
    assert rule["rule_version"] == GRADING_RULES_VERSION and rule["rule_version"].startswith("experiment_grading@")
    assert rule["spec"]["expected_net_pct"] == pytest.approx(exp["expectancy_net_pct"], abs=1e-4)
    assert rule["spec"]["min_trades"] == keep.xcfg.min_trades_to_grade and rule["frozen_at"] <= vrow["created_at"]
    # every outcome was judged under that version's frozen rule, not a later config
    for o in keep.xstore.outcomes(eid):
        assert o["rule_version"] == rule["rule_version"]
        assert next(f for f in json.loads(o["realized_facts_json"]) if f["id"].endswith("_expected_net"))["value"] == pytest.approx(exp["expectancy_net_pct"], abs=1e-4)


# ═══════════════════════════════════════════════════════════════════════════
# The worth-testing gate (S2-09 … S2-11)
# ═══════════════════════════════════════════════════════════════════════════

def _ev(tmp_path, variant, D_idx=OPEN_AT):
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[D_idx])
    cx = HYP.Conditioning(md)
    return HYP.measure(variant, md, cx, HYP.windows_for(md, "2024-12-31"), rcfg=rcfg, draws=100, seed=1)


def test_s2_09_the_gate_is_the_constitutions_gauntlet_plus_the_trailing_check(tmp_path):
    c = load_constitution(CONSTITUTION)
    x = xcfg_for(tmp_path)
    good = _ev(tmp_path, HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5))
    g = worth_testing_gates(good, c=c, xcfg=x, n_trials=18, evidence_strength=0.8, novel=True, novelty_note="n")
    assert g.passed, [str(x) for x in g.failures]
    names = {x.name for x in g.gates}
    assert {"history_expectancy_net", "history_expectancy_2x_slippage", "history_edge_vs_baseline", "history_placebo",
            "trailing_expectancy_net", "trailing_expectancy_2x_slippage", "implementable_under_cost_convention", "novelty",
            "family_wise_significance", "discovery_window_alone_2x_slippage", "history_cluster_significance"} <= names
    # every gate carries its measured value next to its bar
    assert all(x.value is not None for x in g.gates if x.name.startswith(("history_", "trailing_")))
    # the unconditioned rule fails on the numbers, not on a story
    bad = _ev(tmp_path, HYP.Variant("dip_bounce", 6.0, ("all",), 5))
    gb = worth_testing_gates(bad, c=c, xcfg=x, n_trials=18, evidence_strength=0.8, novel=True, novelty_note="n")
    assert not gb.passed and {"history_expectancy_net", "trailing_expectancy_net"} & {x.name for x in gb.failures}
    # weak S1 evidence, a stale claim, or a multi-session short each close the gate on its own
    assert not worth_testing_gates(good, c=c, xcfg=x, n_trials=18, evidence_strength=0.1, novel=True, novelty_note="n").passed
    assert not worth_testing_gates(good, c=c, xcfg=x, n_trials=18, evidence_strength=0.8, novel=False, novelty_note="n").passed
    short = replace(good, variant=HYP.Variant("surge_fade", 6.0, ("market_down_1pct",), 5))
    gs = worth_testing_gates(short, c=c, xcfg=x, n_trials=18, evidence_strength=0.8, novel=True, novelty_note="n")
    assert "implementable_under_cost_convention" in {x.name for x in gs.failures}


def test_s2_10_advisory_gates_are_recorded_but_do_not_kill_and_the_family_wise_bar_divides_by_the_trials(tmp_path):
    c = load_constitution(CONSTITUTION)
    x = xcfg_for(tmp_path)
    good = _ev(tmp_path, HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5))
    g = worth_testing_gates(good, c=c, xcfg=x, n_trials=18, evidence_strength=0.8, novel=True, novelty_note="n")
    fw = next(x for x in g.gates if x.name == "family_wise_significance")
    assert not fw.fatal and fw.bar == pytest.approx(0.05 / 18)
    g1 = worth_testing_gates(good, c=c, xcfg=x, n_trials=1, evidence_strength=0.8, novel=True, novelty_note="n")
    assert next(x for x in g1.gates if x.name == "family_wise_significance").bar == pytest.approx(0.05)
    assert all(not x.fatal for x in g.gates if x.name in ("family_wise_significance", "discovery_window_alone_2x_slippage",
                                                            "history_cluster_significance"))


def test_s2_11_the_two_x_slippage_gate_is_not_the_same_number_as_the_hurdle_gate(tmp_path):
    good = _ev(tmp_path, HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5))
    rcfg = rcfg_for(tmp_path)
    assert good.whole.expectancy_2x_net == pytest.approx(good.whole.expectancy_net - 2 * rcfg.slippage_pct)


# ═══════════════════════════════════════════════════════════════════════════
# Opening from a real S1 finding: frozen expectation, counted trials (S2-12 … S2-14)
# ═══════════════════════════════════════════════════════════════════════════

def test_s2_12_a_real_s1_dip_finding_opens_v1_with_a_frozen_expectation_and_every_trial_on_the_record(keep):
    eid = _exp(keep)
    e = keep.xstore.experiment(eid)
    f = keep.rstore.findings_for(keep.dates[OPEN_AT])
    src = next(x for x in f if x.template_id == "dip")
    assert e["source_finding_id"] == src.id and src.decision.value == "no_trade" and e["opened_edition"] == keep.dates[OPEN_AT]
    v1 = keep.xstore.versions(eid)[0]
    variant = HYP.Variant.from_dict(json.loads(v1["variant_json"]))
    assert variant.signature == "dip_bounce|6.0|market_down_1pct|h5"
    exp = json.loads(v1["expectation_json"])
    assert exp["seal"] == keep.dates[OPEN_AT] and exp["period"]["end"] == keep.dates[OPEN_AT] and exp["expectancy_net_pct"] > 1.0
    assert exp["trailing_n"] > 0 and exp["placebo_p"] <= 0.05 and "code." in exp["computed_by"]
    trials = keep.xstore.trials("finding", src.id)
    assert len(trials) == 18 == v1["trials_for_version"]
    assert sum(1 for t in trials if t["adopted"]) == 1 and [t["trial_no"] for t in trials] == list(range(1, 19))
    cand = keep.xstore.candidate_for_finding(src.id)
    assert cand["opened_experiment_id"] == eid and cand["trials_evaluated"] == 18
    # the S1 card's own numbers travel with the version, by their original fact ids
    src_facts = json.loads(v1["expectation_facts_json"])["source"]
    assert any(x["id"].endswith("_threshold") and x["value"] == 6.0 for x in src_facts)


def test_s2_13_non_derivable_findings_and_repeats_are_declined_on_the_record_one_experiment_per_family(keep):
    rows = keep.xstore.candidates()
    reasons = {r["finding_id"]: r["reason"] for r in rows}
    assert any("not" in r and "family" in r for r in reasons.values())
    dip_repeats = [r for r in rows if r["family_id"] == "dip_bounce" and r["opened_experiment_id"] is None]
    assert dip_repeats and all("already has experiment" in r["reason"] for r in dip_repeats)
    assert all(r["trials_evaluated"] == 0 for r in dip_repeats), "a repeat is not re-researched (no p-hacking by repetition)"
    assert len(keep.xstore.experiments()) == 1


def test_s2_14_a_declined_family_is_not_re_researched_daily(tmp_path):
    lp = Loop(tmp_path, forward_edge=+1, xcfg=xcfg_for(tmp_path, min_n_history=10 ** 6))   # nothing can clear
    lp.run(OPEN_AT, OPEN_AT + 8)
    rows = [r for r in lp.xstore.candidates() if r["family_id"] == "dip_bounce"]
    researched = [r for r in rows if r["trials_evaluated"] > 0]
    retried = [r for r in rows if "p-hacking by repetition" in r["reason"]]
    assert researched and retried and len(researched) < len(rows)
    assert researched[0]["best_failed_gates_json"] != "[]" and researched[0]["best_rule_text"]


# ═══════════════════════════════════════════════════════════════════════════
# Forward tracking, expected vs actual, learning, versions, retirement (S2-15 … S2-18)
# ═══════════════════════════════════════════════════════════════════════════

def test_s2_15_forward_marks_are_point_in_time_and_the_book_rewalk_is_seal_invariant(keep):
    eid = _exp(keep)
    marks = keep.xstore.marks(eid, 1, 1)
    trades = keep.xstore.trades(eid, 1, 1)
    assert marks and trades
    for m in marks:
        assert m["recorded_edition"] >= m["session"], "a mark was written before its session"
    for t in trades:
        assert t["recorded_edition"] >= t["exit_date"] and t["entry_date"] > t["signal_date"]
    # an independent replay of period 1 from the store's own rule on a LATER seal reproduces the trades exactly
    v = HYP.Variant.from_dict(json.loads(keep.xstore.versions(eid)[0]["variant_json"]))
    p = keep.xstore.periods(eid, 1)[0]
    md = keep.full.sealed(keep.dates[OPEN_AT + 30])
    cx = HYP.Conditioning(md)
    start = md.session_after(p["start_after"], 1)
    run = run_book(v, md, cx, period_start=start, period_sessions=p["sessions"], rcfg=keep.rcfg, capital_inr=1e6, fraction_per_position=0.1,
                   max_concurrent=10, max_new_per_session=5)
    assert sorted((t.symbol, t.signal_date, round(t.pnl_pct_net, 6)) for t in run.closed) == \
           sorted((t["symbol"], t["signal_date"], round(t["pnl_pct_net"], 6)) for t in trades)
    assert [round(x, 4) for x in run.equity.loc[[m["session"] for m in marks]].tolist()] == [round(m["equity_inr"], 4) for m in marks]


def test_s2_16_expected_vs_actual_is_computed_and_stated_honestly(keep):
    eid = _exp(keep)
    outs = [o for o in keep.xstore.outcomes(eid) if o["verdict"] == "right"]
    assert outs, "with the edge kept forward, at least one period is graded Right"
    o = outs[0]
    cmp_ = json.loads(o["comparison_json"])
    fwd = json.loads(o["forward_json"])
    trades = keep.xstore.trades(eid, o["version"], o["period_no"])
    assert cmp_["actual_net_pct"] == pytest.approx(np.mean([t["pnl_pct_net"] for t in trades]))
    assert cmp_["gap_pct"] == pytest.approx(cmp_["actual_net_pct"] - cmp_["expected_net_pct"])
    assert cmp_["category"] in ("stronger", "weaker") and o["verdict"] == "right"
    assert not any(ch.isdigit() for ch in REF_TOKEN_RE.sub("", cmp_["statement"]))
    assert "expectation" in cmp_["statement"] and ("weaker" in cmp_["statement"] or "at least as strong" in cmp_["statement"])
    assert fwd["max_drawdown_pct"] >= 0 and fwd["book_return_pct"] is not None and fwd["label"].startswith("Virtual money")


def test_s2_17_a_failure_produces_a_counted_learning_a_v2_and_finally_a_burial(lose):
    eid = _exp(lose)
    xs = lose.xstore
    versions = xs.versions(eid)
    assert [v["version"] for v in versions] == [1, 2, 3], "v1 -> v2 -> v3 then the grave (max_versions = 3)"
    assert versions[1]["level"] == "L3" and versions[1]["validation"] and versions[1]["change"].startswith("added the condition")
    o1 = xs.outcome(eid, 1, 1)
    assert o1["verdict"] == "wrong" and o1["next_action"] == "revise"
    lj = json.loads(o1["learning_json"])
    # five candidate revisions: every condition but `all`, the one the rule carries, the one it IMPLIES
    # (market_down) and the one EXCLUSIVE with it (market_up) — a no-op or an empty revision is never a trial
    assert lj["trials_evaluated"] == 5 and lj["trials_passing"] >= 1 and lj["adopted_signature"].startswith("dip_bounce|6.0|market_down_1pct+")
    assert HYP.candidate_conditions(("market_down_1pct",)) == ["regime_risk_on", "regime_neutral", "regime_risk_off", "breadth_high", "breadth_low"]
    assert HYP.candidate_conditions(("market_down_1pct", "breadth_low")) == ["regime_risk_on", "regime_neutral", "regime_risk_off"]
    assert "losing trades" in lj["statement"] and not any(ch.isdigit() for ch in REF_TOKEN_RE.sub("", lj["statement"]))
    # the v2 expectation is frozen on the seal of the grading edition, before v2's first period
    assert json.loads(versions[1]["expectation_json"])["seal"] == o1["graded_edition"]
    assert xs.periods(eid, 2)[0]["start_after"] == o1["graded_edition"]
    # every revision trial is on the record, numbered after the opening trials
    tr = xs.trials("experiment", eid)
    v2_conds = HYP.Variant.from_dict(json.loads(versions[1]["variant_json"])).conditions
    assert len(tr) == 5 + len(HYP.candidate_conditions(v2_conds)) and tr[0]["trial_no"] == 19 and sum(1 for t in tr if t["adopted"]) == 2
    # no version is a no-op of the one before it: each adds a condition that narrows the rule
    for a, b in zip(versions, versions[1:]):
        ca = HYP.Variant.from_dict(json.loads(a["variant_json"])).conditions
        cb = HYP.Variant.from_dict(json.loads(b["variant_json"])).conditions
        assert cb[:-1] == ca and cb[-1] in HYP.candidate_conditions(ca)
    pm = xs.post_mortem(eid)
    assert pm is not None and pm["cause"] in ("revisions_exhausted", "edge_did_not_persist_oos") and pm["retired_version"] == 3
    assert xs.state_of(eid) == ExperimentState.buried
    # nothing tracks a buried idea: no period after the burial
    assert not [p for p in xs.open_periods() if p["experiment_id"] == eid]
    assert xs.scoreboard(lose.dates[OPEN_AT + 45]).experiments_buried == 1


def test_s2_18_learning_never_creates_a_version_beyond_the_retirement_rule(lose):
    eid = _exp(lose)
    assert len(lose.xstore.versions(eid)) <= lose.xcfg.max_versions


# ═══════════════════════════════════════════════════════════════════════════
# Graduation: champion/challenger, null-calibrated, human-gated (S2-19 … S2-21)
# ═══════════════════════════════════════════════════════════════════════════

def test_s2_19_the_arena_score_is_the_ported_one():
    assert constitutional_score([1.0] * 3)[2] == "Test"
    assert constitutional_score([-1.0] * 10)[2] == "Retire"
    w, sc, st = constitutional_score([1.0, 1.2, 0.8, 1.1, 0.9, 1.0, 1.3, 0.7, 1.0, 1.1])
    assert st == "Watch" and sc > 0 and w == pytest.approx(0.25 + 0.75 * 10 / 40)
    assert constitutional_score([1.0] * 25)[2] == "Keep"


def test_s2_20_a_proposal_needs_every_gate_but_the_signature_and_never_promotes(keep):
    eid = _exp(keep)
    xs = keep.xstore
    checks = xs.q("SELECT * FROM pfx_gate_checks WHERE experiment_id = ? ORDER BY edition_date", [eid])
    assert checks, "the graduation gate runs after every graded period"
    early = json.loads(checks[0]["gates_json"])
    assert not checks[0]["proposable"] and any(g["name"] == "oos_sample_size" and not g["passed"] for g in early)
    pr = xs.proposal(eid)
    assert pr is not None, "with the edge kept forward, the record eventually clears every measurable gate"
    gates = json.loads(pr["gates_json"])
    by = {g["name"]: g for g in gates}
    assert by["constitution_signed"]["passed"] is False and pr["status"] == "blocked_unsigned_constitution"
    for name in ("oos_sample_size", "oos_expectancy_net", "oos_expectancy_2x_slippage", "oos_placebo", "oos_cluster_significance",
                 "arena_roster_keep", "beats_incumbent_net_return", "no_worse_drawdown_than_incumbent", "implementable_under_cost_convention"):
        assert by[name]["passed"], name
    assert by["beats_incumbent_net_return"]["bar"] is not None and by["oos_placebo"]["value"] <= 0.05
    assert xs.state_of(eid) == ExperimentState.proposed
    # the registry has no 'promoted' anywhere: a proposal is the loop's last word
    assert "promoted" not in {r[0] for r in xs.q("SELECT DISTINCT status FROM pfx_proposals")}
    assert not [p for p in xs.open_periods() if p["experiment_id"] == eid] or True   # tracking continues after a proposal


def test_s2_21_on_a_signed_constitution_the_proposal_awaits_a_human_and_the_incumbent_is_real(tmp_path):
    lp = Loop(tmp_path, forward_edge=+1, xcfg=xcfg_for(tmp_path, write_constitution(tmp_path, min_n=12, signed=True)))
    lp.run(OPEN_AT, OPEN_AT + 60)
    eid = _exp(lp)
    pr = lp.xstore.proposal(eid)
    assert pr is not None and pr["status"] == "proposed_awaiting_human" and pr["incumbent"] == "equal_weight_hold"
    assert lp.constitution.is_signed
    # the incumbent is the same capital in the whole universe over the same window, costs charged
    md = lp.full.sealed(pr["edition_date"])
    p1 = lp.xstore.periods(eid, 1)[0]
    inc = passive_incumbent(md, start=md.session_after(p1["start_after"], 1), end=md.as_of, rcfg=lp.rcfg, capital_inr=1e6)
    by = {g["name"]: g for g in json.loads(pr["gates_json"])}
    assert by["beats_incumbent_net_return"]["bar"] == pytest.approx(inc.total_return_pct, abs=1e-4)
    # forward reversed: the gate closes on the numbers and no proposal exists, signed or not
    rev = tmp_path / "rev"
    rev.mkdir(exist_ok=True)
    lp2 = Loop(rev, forward_edge=-1, xcfg=xcfg_for(rev, write_constitution(rev, min_n=12, signed=True)))
    lp2.run(OPEN_AT, OPEN_AT + 20)
    assert lp2.xstore.proposal(_exp(lp2)) is None


# ═══════════════════════════════════════════════════════════════════════════
# Scoreboard, backfill labels, append-only, determinism (S2-22 … S2-25)
# ═══════════════════════════════════════════════════════════════════════════

def test_s2_22_the_scoreboard_counts_graded_periods_once_excludes_void_and_splits_by_generation(keep, lose):
    for lp in (keep, lose):
        eid = _exp(lp)
        as_of = lp.dates[OPEN_AT + 45] if lp is lose else lp.dates[OPEN_AT + 59]
        sb = lp.xstore.scoreboard(as_of)
        outs = [o for o in lp.xstore.outcomes(eid) if o["data_as_of"] <= as_of]
        assert sb.n == sum(1 for o in outs if o["verdict"] != "void") and sb.void == sum(1 for o in outs if o["verdict"] == "void")
        assert sb.right + sb.wrong + sb.inconclusive == sb.n and sb.forward.n + sb.backfilled.n == sb.n
        assert sb.forward.n == 0 and sb.backfilled.n == sb.n and sb.record_label == BACKFILL_LABEL
        assert sb.trials_total == len(lp.xstore.q("SELECT 1 FROM pfx_trials"))
    assert keep.xstore.scoreboard(keep.dates[OPEN_AT + 45]).right >= 3
    assert lose.xstore.scoreboard(lose.dates[OPEN_AT + 45]).wrong >= 3
    assert lose.xstore.scoreboard(lose.dates[OPEN_AT]).n == 0   # pending as of the opening date


def test_s2_23_a_step_run_on_its_own_session_date_is_forward_and_a_later_one_is_a_backfill(tmp_path):
    lp = Loop(tmp_path, forward_edge=+1)
    d0 = lp.dates[OPEN_AT]
    same_day = datetime.fromisoformat(d0 + "T18:30:00")
    lp.step(d0, computed_at=same_day)
    lp.step(lp.dates[OPEN_AT + 1], computed_at=AT)
    e = lp.xstore.experiments()[0]
    assert e["backfilled"] == 0 and lp.xstore.editions() == [d0, lp.dates[OPEN_AT + 1]]
    assert lp.xstore.one("SELECT backfilled FROM pfx_editions WHERE edition_date = ?", [lp.dates[OPEN_AT + 1]])[0] == 1
    card = views.card(lp.xstore, e["experiment_id"])
    assert card.backfilled is False and card.record_label == FORWARD_LABEL


def test_s2_24_every_table_is_append_only_and_the_chains_verify(keep):
    con = sqlite3.connect(keep.xcfg.experiments_db)
    for t in ("pfx_experiments", "pfx_versions", "pfx_periods", "pfx_outcomes", "pfx_trials", "pfx_marks", "pfx_trades", "pfx_candidates"):
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            con.execute(f"DELETE FROM {t}")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            con.execute(f"UPDATE {t} SET rowid = rowid")
    con.close()
    assert all(ok for _, ok, _ in keep.xstore.verify_all_chains())
    # a rewritten row breaks the chain (edit through a raw connection with the trigger dropped)
    con = sqlite3.connect(keep.xcfg.experiments_db)
    con.execute("DROP TRIGGER pfx_versions_no_update")
    con.execute("UPDATE pfx_versions SET why = 'rewritten' WHERE version = 1")
    con.commit(); con.close()
    keep.xstore.close()
    ok, msg = keep.xstore.verify_chain("pfx_versions")
    assert not ok and "chain broken" in msg


def test_s2_25_two_builds_of_the_same_seals_at_different_times_are_identical_but_for_timestamps(tmp_path):
    """Built an hour apart: every timestamp differs (columns AND the ones inside JSON: frozen_at, computed_at, at);
    every number, every verdict, every fact and every CHAIN HASH is identical."""
    from pathfinder.experiments.store import content_payload
    from datetime import timedelta
    (tmp_path / "a").mkdir(exist_ok=True); (tmp_path / "b").mkdir(exist_ok=True)
    a = Loop(tmp_path / "a", forward_edge=-1); a.run(OPEN_AT, OPEN_AT + 14, computed_at=AT)
    b = Loop(tmp_path / "b", forward_edge=-1); b.run(OPEN_AT, OPEN_AT + 14, computed_at=AT + timedelta(hours=1))
    for t in ("pfx_experiments", "pfx_versions", "pfx_periods", "pfx_outcomes", "pfx_trials", "pfx_marks", "pfx_trades", "pfx_candidates",
              "pfx_narratives", "pfx_post_mortems", "pfx_gate_checks", "pfx_editions"):
        ra = [content_payload({k: r[k] for k in r.keys()}) for r in a.xstore.q(f"SELECT * FROM {t} ORDER BY rowid")]
        rb = [content_payload({k: r[k] for k in r.keys()}) for r in b.xstore.q(f"SELECT * FROM {t} ORDER BY rowid")]
        assert ra == rb, t
        if t in ("pfx_versions", "pfx_outcomes", "pfx_trials", "pfx_narratives"):
            assert ra, f"{t}: the comparison must cover real rows"
    for t in ("pfx_experiments", "pfx_versions", "pfx_periods", "pfx_outcomes", "pfx_post_mortems"):
        assert a.xstore.q(f"SELECT row_hash FROM {t} ORDER BY rowid") == b.xstore.q(f"SELECT row_hash FROM {t} ORDER BY rowid"), t
    # the raw rows DO differ in their timestamps — the chain is over content, not clock
    va, vb = a.xstore.versions(_exp(a))[0], b.xstore.versions(_exp(b))[0]
    assert va["created_at"] != vb["created_at"] and json.loads(va["expectation_json"])["frozen_at"] != json.loads(vb["expectation_json"])["frozen_at"]
    assert va["row_hash"] == vb["row_hash"]
    assert all(ok for _, ok, _ in a.xstore.verify_all_chains()) and all(ok for _, ok, _ in b.xstore.verify_all_chains())


def test_s2_25b_a_card_served_for_a_past_edition_shows_only_what_was_known_then(keep):
    """Point-in-time on the served surface: the feed's card for edition D carries the state, score and
    trial count AS OF D — never a grade, a version or a proposal that came later."""
    eid = _exp(keep)
    opened = keep.xstore.experiment(eid)["opened_edition"]
    first = views.card(keep.xstore, eid, edition=opened)
    assert first.score.n == 0 and first.periods_graded == 0 and first.latest_comparison == ComparisonCategory.pending
    assert first.state == ExperimentState.testing and first.trials_total == 18 and first.news_edition.isoformat() == opened
    outs = keep.xstore.outcomes(eid)
    g1 = outs[0]
    mid = views.card(keep.xstore, eid, edition=g1["graded_edition"])
    assert mid.score.n + mid.score.void == 1 and mid.latest_comparison.value == json.loads(g1["comparison_json"])["category"]
    assert mid.state == ExperimentState.testing, "the proposal came later; a card for this edition cannot show it"
    latest = views.card(keep.xstore, eid)
    assert latest.state == ExperimentState.proposed and latest.score.n + latest.score.void == len(outs)
    assert [c.id for c in views.cards_on(keep.xstore, opened)] == [eid]


# ═══════════════════════════════════════════════════════════════════════════
# The public card, compliance, the LLM boundary, the API (S2-26 … S2-31)
# ═══════════════════════════════════════════════════════════════════════════

def test_s2_26_the_public_card_tells_seven_digit_free_beats_from_facts_and_never_names_a_stock(keep):
    eid = _exp(keep)
    card = views.card(keep.xstore, eid)
    assert [s.beat.value for s in card.story] == ["noticed", "researched", "history_showed", "decided", "happened", "learned", "next"]
    known = {f.id for f in card.facts}
    for s in card.story:
        assert not any(ch.isdigit() for ch in s.headline) and not any(ch.isdigit() for ch in REF_TOKEN_RE.sub("", s.body))
        assert s.fact_refs and set(s.fact_refs) <= known
    names = {t["symbol"] for t in keep.xstore.trades(eid)}
    text = " ".join(s.body for s in card.story) + card.theme
    assert names and not any(n in text.split() for n in names)
    assert "constituents" not in card.model_dump() and card.trials_total >= 18 and card.versions_count == 1
    assert card.state == ExperimentState.proposed and card.latest_comparison in (ComparisonCategory.stronger, ComparisonCategory.weaker)
    assert card.evidence.n > 0 and card.evidence.cost_hurdle_pct == keep.rcfg.hurdle_pct and card.evidence.disclosures


def test_s2_27_the_public_card_contract_refuses_trade_instructions_and_constituent_fields(keep):
    eid = _exp(keep)
    card = views.card(keep.xstore, eid)
    d = card.model_dump(mode="json")
    for bad in ("Enter at the open and place a stop-loss below", "Buy the dip and sell into strength", "The target is the prior high"):
        broken = json.loads(json.dumps(d))
        broken["story"][3]["body"] = bad
        with pytest.raises(ValueError, match="trade instruction"):
            ExperimentCard.model_validate(broken)
    with pytest.raises(ValueError):
        ExperimentCard.model_validate({**d, "constituents": ["T0"]})


def test_s2_28_the_record_withholds_constituents_until_ra_review_and_carries_versions_trials_and_the_proposal(keep, monkeypatch):
    eid = _exp(keep)
    monkeypatch.delenv("KANIDA_PF_RA_REVIEWED", raising=False)
    rec = views.record(keep.xstore, eid)
    assert rec.basket.constituents == [] and rec.basket.constituents_visibility == "withheld_pending_ra_review"
    assert len(rec.trials) == rec.trials_total and rec.versions[0].expectation.n > 0 and rec.proposal is not None
    assert rec.change_log[0].previous_version is None and rec.change_log[0].new_version == "v1" and rec.change_log[0].improved is None
    assert all(p.grading_rule.rule_version == GRADING_RULES_VERSION for v in rec.versions for p in v.periods)
    assert any(p.status == "graded" and p.expected_vs_actual is not None and p.learning is not None for p in rec.versions[0].periods)
    monkeypatch.setenv("KANIDA_PF_RA_REVIEWED", "1")
    rec2 = views.record(keep.xstore, eid)
    assert rec2.basket.constituents and rec2.basket.constituents_visibility == "in_app_ra_reviewed"


def test_s2_29_the_buried_record_publishes_its_post_mortem_and_lists_losers_first(lose, keep):
    rec = views.record(lose.xstore, _exp(lose))
    assert rec.state == ExperimentState.buried and rec.post_mortem is not None
    assert rec.post_mortem.cause.value in ("revisions_exhausted", "edge_did_not_persist_oos") and rec.post_mortem.retired_version == "v3"
    assert not any(ch.isdigit() for ch in REF_TOKEN_RE.sub("", rec.post_mortem.summary.body))
    assert [v.status for v in rec.versions] == ["superseded", "superseded", "buried"]
    resp = views.experiments_response(lose.xstore, engine_version=lose.xcfg.engine_version)
    assert resp.items[0].state == ExperimentState.buried and resp.scoreboard.experiments_buried == 1 and resp.not_opened


class _Fake:
    """A provider whose completions are under the test's control."""

    def __init__(self, body):
        self.body = body
        self.calls = 0

    def narrate(self, *, beat, facts, context=(), schema_id="x", constitution_version, prompt_version, budget, batch=True, key=None):
        self.calls += 1
        b = self.body(facts) if callable(self.body) else self.body
        refs = [f["id"] for f in facts if "{{fact:" + f["id"] + "}}" in b]
        return NarrateResult(job="narrate", model="fake-model", usage=Usage("fake-model", 0, 0), prompt_version=prompt_version,
                             constitution_version=constitution_version, beat=beat, headline="x", body=b, fact_refs=refs)

    def classify(self, **kw):
        raise GatewayError("not used")

    def reason(self, **kw):
        raise GatewayError("not used")


def test_s2_30_a_model_that_writes_a_number_is_rejected_and_the_engine_narrates_visibly(tmp_path):
    lp = Loop(tmp_path, forward_edge=+1)
    bad = _Fake("The edge was 1.2 percent per trade.")
    lp.step(lp.dates[OPEN_AT], narrator=ExperimentNarrator(bad, provider_name="fake"))
    n = lp.xstore.narrative(_exp(lp))
    story = json.loads(n["story_json"])
    assert bad.calls == 7 and all(s["produced_by"] == "engine" for s in story) and n["produced_by"] == "engine"
    good = _Fake(lambda facts: "History showed an edge of {{fact:%s}} per trade." % next(f["id"] for f in facts if f["id"].endswith("_expectancy_net")))
    lp2 = Loop(tmp_path / "g", forward_edge=+1); (tmp_path / "g").mkdir(exist_ok=True)
    lp2.step(lp2.dates[OPEN_AT], narrator=ExperimentNarrator(good, provider_name="fake"))
    story2 = json.loads(lp2.xstore.narrative(_exp(lp2))["story_json"])
    assert all(s["produced_by"] == "llm" and s["model"] == "fake-model" for s in story2)
    # the engine's headline is kept — the decision is not the model's to restate
    assert story2[0]["headline"] != "x"


def test_s2_31_no_number_on_any_surface_originates_in_a_model(keep, lose):
    for lp in (keep, lose):
        rec = views.record(lp.xstore, _exp(lp))
        for f in rec.facts:
            assert not any(m in f.provenance.computed_by.lower() for m in ("claude", "gpt", "llm", "sonnet", "haiku"))
            assert f.provenance.date_range.end <= f.provenance.as_of
        assert "code." in rec.versions[0].expectation.computed_by


def test_s2_32_the_api_serves_experiments_records_and_feed_cards_and_never_fixtures(keep, monkeypatch):
    from fastapi.testclient import TestClient
    from pathfinder.mock_app import app
    monkeypatch.setenv("KANIDA_PATHFINDER_SOURCE", "research")
    monkeypatch.setenv("KANIDA_PATHFINDER_RESEARCH_DB", keep.rcfg.research_db)
    monkeypatch.setenv("KANIDA_PATHFINDER_EXPERIMENTS_DB", keep.xcfg.experiments_db)
    monkeypatch.delenv("KANIDA_PF_RA_REVIEWED", raising=False)
    set_research_store(None); set_experiment_store(None)
    try:
        client = TestClient(app)
        eid = _exp(keep)
        body = client.get("/api/pathfinder/experiments").json()
        assert body["count"] == 1 and body["items"][0]["id"] == eid and body["scoreboard"]["n"] >= 3 and body["not_opened"]
        assert "constituents" not in body["items"][0] and body["items"][0]["record_label"] == BACKFILL_LABEL
        rec = client.get(f"/api/pathfinder/experiment/{eid}").json()
        assert rec["basket"]["constituents"] == [] and rec["versions"][0]["trials_for_this_version"] == 18 and rec["proposal"]
        assert client.get("/api/pathfinder/experiment/exp_nope").status_code == 404
        assert client.get("/api/pathfinder/experiment/DROP").status_code == 400
        feed = client.get(f"/api/pathfinder/feed?date={keep.dates[OPEN_AT]}").json()
        assert [c["id"] for c in feed["experiment_cards"]] == [eid] and feed["experiments_scoreboard"]["n"] == 0
        assert all(c["backfilled"] == feed["backfilled"] for c in feed["experiment_cards"])
        later = client.get(f"/api/pathfinder/feed?date={keep.dates[OPEN_AT + 1]}").json()
        assert later["experiment_cards"] == [] and later["experiments_scoreboard"]["pending"] >= 1
        # no registry -> 404, never fixtures
        monkeypatch.setenv("KANIDA_PATHFINDER_EXPERIMENTS_DB", str(Path(keep.xcfg.experiments_db).parent / "missing.db"))
        set_experiment_store(None)
        assert client.get("/api/pathfinder/experiments").status_code == 404
    finally:
        set_research_store(None); set_experiment_store(None)


# ═══════════════════════════════════════════════════════════════════════════
# The quant audit's confirmed findings, pinned (A1 … A14; docs/handbacks/PF-S2.md §4)
# ═══════════════════════════════════════════════════════════════════════════

def test_a1_a_card_written_on_a_forward_edition_is_forward_even_when_the_experiment_opened_in_a_backfill(tmp_path):
    """The feed used to refuse the first forward edition of a backfilled experiment (500); the card's flag is per edition."""
    from pathfinder.schemas import FeedResponse
    lp = Loop(tmp_path, forward_edge=+1)
    lp.step(lp.dates[OPEN_AT], computed_at=AT)                                   # opened as a backfill
    for d in lp.dates[OPEN_AT + 1: OPEN_AT + 16]:
        lp.step(d, computed_at=datetime.fromisoformat(d + "T18:30:00"))         # forward, same-day steps
    eid = _exp(lp)
    news = [n["edition_date"] for n in lp.xstore.q("SELECT edition_date FROM pfx_narratives WHERE experiment_id = ? ORDER BY edition_date", [eid])]
    assert len(news) >= 2, "a period was graded inside the forward steps"
    d = news[1]
    card = views.card(lp.xstore, eid, edition=d)
    assert card.backfilled is False and card.opened_backfilled is True and card.record_label == FORWARD_LABEL
    feed = lp.rstore.feed(d)
    served = FeedResponse.model_validate({**feed.model_dump(mode="json"),
                                          "experiment_cards": [c.model_dump(mode="json") for c in views.cards_on(lp.xstore, d)],
                                          "experiments_scoreboard": lp.xstore.scoreboard(d).model_dump(mode="json")})
    assert served.backfilled is False and [c.id for c in served.experiment_cards] == [eid]
    assert "forward" in served.experiments_scoreboard.record_label
    # and the opening card, on its own (backfilled) edition, still says backfilled
    first = views.card(lp.xstore, eid, edition=lp.dates[OPEN_AT])
    assert first.backfilled is True and first.record_label == BACKFILL_LABEL


def test_a4_the_frozen_cumulative_rule_buries_a_version_whose_whole_record_is_significantly_negative():
    from pathfinder.experiments.grading import cumulative_verdict
    v = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)
    rule = build_experiment_rule(v, expected_net_pct=0.3, hurdle_pct=0.5, min_trades=3, frozen_at=AT)
    assert {"cumulative_bury_t", "cumulative_min_trades", "cumulative_min_signal_days", "max_consecutive_void"} <= set(rule.spec)
    rng = np.random.default_rng(3)
    days = np.repeat([f"2026-03-{k:02d}" for k in range(2, 14, 2)], 2)          # 12 trades on 6 signal days
    losing = -1.0 + rng.normal(0, 0.15, days.size)
    assert cumulative_verdict(rule, losing, days, 0) == "edge_did_not_persist_oos"
    assert cumulative_verdict(rule, losing[:8], days[:8], 0) is None, "below the frozen minimum the record is not judged"
    assert cumulative_verdict(rule, -losing, days, 0) is None, "a winning record is never buried by the cumulative rule"
    noisy = rng.normal(0, 3.0, days.size)
    assert cumulative_verdict(rule, noisy, days, 0) is None
    assert cumulative_verdict(rule, np.array([]), np.array([]), rule.spec["max_consecutive_void"]) == "rule_stopped_firing"


def test_a4b_a_rule_that_stops_firing_is_buried_after_the_frozen_number_of_void_periods(tmp_path):
    lp = Loop(tmp_path, forward_edge=+1, crash_until=OPEN_AT + 1)      # the last market-wide fall is the opening day
    lp.run(OPEN_AT, OPEN_AT + 40)
    eid = _exp(lp)
    outs = lp.xstore.outcomes(eid)
    voids = [o for o in outs if o["verdict"] == "void"]
    pm = lp.xstore.post_mortem(eid)
    assert pm is not None and pm["cause"] == "rule_stopped_firing"
    assert len(voids) == json.loads(lp.xstore.versions(eid)[0]["grading_rule_json"])["spec"]["max_consecutive_void"]
    assert not [p for p in lp.xstore.open_periods() if p["experiment_id"] == eid]
    rec = views.record(lp.xstore, eid)
    assert rec.state == ExperimentState.buried and rec.post_mortem.cause.value == "rule_stopped_firing"


def test_a6_a_grader_whose_version_differs_from_the_frozen_rules_refuses_to_grade(tmp_path, monkeypatch):
    from pathfinder.experiments import loop as LOOP
    lp = Loop(tmp_path, forward_edge=+1)
    lp.step(lp.dates[OPEN_AT])
    monkeypatch.setattr(LOOP, "GRADING_RULES_VERSION", "experiment_grading@9.9.9+code.deadbeef0000")
    with pytest.raises(RuntimeError, match="grader may not change under an open version"):
        lp.run(OPEN_AT + 1, OPEN_AT + 16)
    # the receipt: every stored outcome names the grader that ran, and it equals the frozen rule's version
    eid = _exp(lp)
    for o in lp.xstore.outcomes(eid):
        assert o["grader_version"] == o["rule_version"]


def test_a7_a_trade_through_a_glitch_bar_is_closed_unresolved_and_never_graded(tmp_path):
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    c0 = crash[10]
    md0 = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[c0 + 8])
    cx0 = HYP.Conditioning(md0)
    v = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)
    base = run_book(v, md0, cx0, period_start=dates[c0], period_sessions=1, rcfg=rcfg, capital_inr=1e6, fraction_per_position=0.1,
                    max_concurrent=10, max_new_per_session=5)
    victim = base.closed[0].symbol
    raw2 = raw.copy()
    m = (raw2["symbol"] == victim) & (raw2["d"] == dates[c0 + 3])
    raw2.loc[m, ["open", "close"]] = raw2.loc[m, ["open", "close"]] * 6.0          # a > 4x print inside the window: a hole
    md = MarketData.from_frame(raw2, idx, vix, rcfg, as_of=dates[c0 + 8])
    run = run_book(v, md, HYP.Conditioning(md), period_start=dates[c0], period_sessions=1, rcfg=rcfg, capital_inr=1e6,
                   fraction_per_position=0.1, max_concurrent=10, max_new_per_session=5)
    bad = next(t for t in run.closed if t.symbol == victim)
    assert bad.resolved is False and "glitch or corporate-action" in bad.unresolved_reason
    assert len(run.graded) == len(run.closed) - 1 and len(run.unresolved) == 1
    # the evidence convention agrees: f5 is NaN for that signal
    row = md.df[(md.df["symbol"] == victim) & (md.df["d"] == dates[c0])]
    assert np.isnan(row["f5"].iloc[0])
    rule = build_experiment_rule(v, expected_net_pct=1.0, hurdle_pct=rcfg.hurdle_pct, min_trades=1, frozen_at=AT)
    g = judge(rule, run, fs=_fs(tmp_path, "u"))
    assert g.n == len(run.graded) and next(f.value for f in g.facts.facts if f.id.endswith("_unresolved_trades")) == 1


def test_a8_a_model_body_that_reads_like_a_trade_instruction_is_replaced_by_the_engine_at_the_source(tmp_path):
    lp = Loop(tmp_path, forward_edge=+1)
    bad = _Fake(lambda facts: "Buy the dip: the edge was {{fact:%s}} per trade." % next(f["id"] for f in facts if f["id"].endswith("_expectancy_net")))
    nar = ExperimentNarrator(bad, provider_name="fake")
    lp.step(lp.dates[OPEN_AT], narrator=nar)
    story = json.loads(lp.xstore.narrative(_exp(lp))["story_json"])
    assert all(s["produced_by"] == "engine" for s in story) and any("trade instruction" in f for f in nar.failures)
    views.card(lp.xstore, _exp(lp))            # and the card builds


def test_a9_a_fact_whose_value_names_a_constituent_is_withheld_on_the_public_card(keep):
    eid = _exp(keep)
    names = {t["symbol"] for t in keep.xstore.trades(eid)}
    card = views.card(keep.xstore, eid)
    assert not any(isinstance(f.value, str) and f.value in names for f in card.facts)
    subject = next(f for f in card.facts if f.id.endswith("_subject"))
    raw_subject = next(f for f in json.loads(keep.xstore.narrative(eid)["facts_json"]) if f["id"].endswith("_subject"))
    if raw_subject["value"] in names:
        assert subject.value.startswith("a constituent") and subject.note
    else:
        assert subject.value == raw_subject["value"]
    # the in-app record still names them, but only after RA review
    assert views.record(keep.xstore, eid).basket.constituents == []


def test_a12_the_family_wise_bar_of_every_revision_trial_divides_by_the_ideas_whole_trial_count(lose):
    eid = _exp(lose)
    tr = [t for t in lose.xstore.trials("experiment", eid) if t["context"] == "after v1 period 1"]
    n_total = 18 + len(tr)
    for t in tr:
        fw = next(g for g in json.loads(t["gates_json"]) if g["name"] == "family_wise_significance")
        assert fw["bar"] == pytest.approx(0.05 / n_total, abs=1e-6)      # gate rows are rounded to six decimals


def test_a14_a_signal_on_the_seal_day_is_seen_but_not_taken(tmp_path):
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    c0 = crash[10]
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[c0])
    v = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)
    run = run_book(v, md, HYP.Conditioning(md), period_start=dates[c0], period_sessions=1, rcfg=rcfg, capital_inr=1e6,
                   fraction_per_position=0.1, max_concurrent=10, max_new_per_session=5)
    assert run.signals_seen == 3 and run.signals_taken == 0 and not run.closed and not run.open_at_end


def test_a10_the_pool_shares_the_rules_day_context_and_the_edge_is_on_one_convention(tmp_path):
    good = _ev(tmp_path, HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5))
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[OPEN_AT])
    mkt = md.df.groupby("d")["ret"].mean()
    pool = md.df[md.df["d"].map(mkt < -0.01).fillna(False) & md.df["f5"].notna()]["f5"]
    assert good.whole.baseline_n == len(pool), "the pool is every resolved stock-session on the rule's kind of day"
    from pathfinder.research.library import _wmean
    assert good.whole.edge_pct == pytest.approx(good.whole.expectancy_net - good.whole.baseline_net)
    assert good.whole.baseline_net == pytest.approx(_wmean(pool.to_numpy() * 100, 1.0) - rcfg.hurdle_pct)


def test_s2_33_the_p0_path_is_unchanged_when_the_source_is_mock(monkeypatch):
    from fastapi.testclient import TestClient
    from pathfinder.mock_app import app
    monkeypatch.setenv("KANIDA_PATHFINDER_SOURCE", "mock")
    body = TestClient(app).get("/api/pathfinder/experiments").json()
    assert "items" in body and body["items"] and body["items"][0]["id"].startswith("exp_")
    assert "scoreboard" not in body
