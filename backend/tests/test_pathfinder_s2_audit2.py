"""
S2 independent re-audit — findings N1..N9 pinned (docs/handbacks/PF-S2.md §4.1).

Every row here FAILS on 187c488 (the pre-re-audit S2 commit) for its own reason: the names it
imports inside the test body did not exist, or the number it asserts was computed on the wrong
population / the wrong null / the wrong convention. Synthetic rows reuse the S2 suite's
engineered universe; the `real_*` rows read the price warehouse and the ARCHIVED pre-audit
registry (`var/pathfinder_experiments.db.archived-20260910T115630`) and are skipped where
either is absent — they are the demonstration numbers, labelled as such.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "backend" / "tests"))

from pathfinder.engine.governance import load_constitution                                   # noqa: E402
from pathfinder.experiments import hypotheses as HYP                                        # noqa: E402
from pathfinder.experiments import views                                                    # noqa: E402
from pathfinder.experiments.gate import ForwardRecord, graduation_gates, worth_testing_gates  # noqa: E402
from pathfinder.experiments.grading import build_experiment_rule, cumulative_verdict        # noqa: E402
from pathfinder.research.config import ResearchConfig, load_config                          # noqa: E402
from pathfinder.research.data import MarketData                                             # noqa: E402
from pathfinder.research.library import _wmean                                              # noqa: E402
from pathfinder.schemas import ExperimentCard, PUBLIC_CARD_BANNED_RE                        # noqa: E402

from test_pathfinder_s2 import AT, CONSTITUTION, OPEN_AT, Loop, _exp, rcfg_for, synth_s2, write_constitution, xcfg_for  # noqa: E402

PRICE_DB = os.environ.get("KANIDA_DB", r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
ARCHIVED = Path(os.environ.get("KANIDA_PFX_ARCHIVED_REGISTRY", str(ROOT / "var" / "pathfinder_experiments.db.archived-20260910T115630")))
real = pytest.mark.skipif(not Path(PRICE_DB).exists(), reason="price warehouse not present")
archived = pytest.mark.skipif(not (Path(PRICE_DB).exists() and ARCHIVED.exists()), reason="warehouse or archived registry not present")

DIP = HYP.Variant("dip_bounce", 6.0, ("market_down_1pct",), 5)


def _limits(n_new=5, n_conc=10):
    from pathfinder.experiments.hypotheses import BookLimits
    return BookLimits(max_new_per_session=n_new, max_concurrent=n_conc)


@pytest.fixture(scope="module")
def warehouse():
    """The real warehouse, loaded once, sealed per row (~10 s)."""
    if not Path(PRICE_DB).exists():
        pytest.skip("price warehouse not present")
    return MarketData.load(load_config(), as_of="2026-07-29")


@pytest.fixture(scope="module")
def opened_2(tmp_path_factory):
    """One opening step on the synthetic universe under a Constitution whose book takes at most TWO names per session,
    so the book-selected population differs from the equal-weighted one on every crash day (three names dip)."""
    tmp = tmp_path_factory.mktemp("open2")
    doc = yaml.safe_load(CONSTITUTION.read_text(encoding="utf-8"))
    doc["risk"]["max_new_positions_per_session"] = 2
    p = tmp / "c2.yaml"
    p.write_text(yaml.safe_dump(doc), encoding="utf-8")
    lp = Loop(tmp, forward_edge=+1, xcfg=xcfg_for(tmp, p))
    lp.step(lp.dates[OPEN_AT])
    return lp


# ═══════════════════════════════════════════════════════════════════════════
# N1 — the frozen expectation is the BOOK's own population
# ═══════════════════════════════════════════════════════════════════════════

def test_n1_book_select_walks_the_books_limits_liquidity_rank_caps_and_one_per_symbol():
    from pathfinder.experiments.hypotheses import BookLimits, book_select
    sessions = [f"d{i}" for i in range(10)]
    si = {d: i for i, d in enumerate(sessions)}
    rows = pd.DataFrame([
        ("A", "d1", 9.0), ("B", "d1", 8.0), ("C", "d1", 7.0), ("D", "d1", 6.0),       # four fire, cap 2 -> A, B
        ("A", "d2", 9.0), ("E", "d2", 5.0), ("F", "d2", 4.0),                         # A still held (exit d3) -> E, F
        ("G", "d3", 1.0), ("H", "d3", 2.0), ("I", "d3", 3.0),                         # A exits at d3 (closed first); cap 2 -> I, H
        ("J", "d4", 1.0),                                                             # concurrency: E,F (to d4? exit d4 closes) ...
    ], columns=["symbol", "d", "_liq"])
    take = book_select(rows, session_index=si, horizon=2, limits=BookLimits(max_new_per_session=2, max_concurrent=3))
    got = sorted(rows[take].apply(lambda r: (r["symbol"], r["d"]), axis=1).tolist())
    # d1: A,B (liquidity desc, cap 2). d2: A held -> skipped; E taken; concurrency 3 (A,B,E) -> F refused.
    # d3: A,B exit at d3 (closed before opening) -> open {E}; I,H taken (liq desc), G refused (cap 2).
    # d4: E exits; open {I,H} -> J taken.
    assert got == sorted([("A", "d1"), ("B", "d1"), ("E", "d2"), ("I", "d3"), ("H", "d3"), ("J", "d4")])


def test_n1_replay_with_the_books_limits_measures_the_selected_population_and_reports_the_skipped(tmp_path):
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[OPEN_AT])
    cx = HYP.Conditioning(md)
    ew = HYP.replay(DIP, md, cx, start=dates[0], end=dates[OPEN_AT], rcfg=rcfg, draws=50, seed=1)
    bk = HYP.replay(DIP, md, cx, start=dates[0], end=dates[OPEN_AT], rcfg=rcfg, draws=50, seed=1, limits=_limits(2, 10))
    assert ew.selection.startswith("equal_weighted") and bk.selection.startswith("book-selected")
    assert bk.n_fired == ew.n and bk.n + bk.n_skipped == bk.n_fired and bk.n_skipped > 0
    # exactly the two most liquid names of every crash day (three fire), never more than two per session
    per_day = pd.Series(bk.signal_dates).value_counts()
    assert per_day.max() <= 2
    liq = cx.liquidity
    df = md.df
    for d in per_day.index[:5]:
        fired = df[(df["d"] == d) & (df["ret"] <= -0.06)].assign(_liq=liq).sort_values("_liq", ascending=False)
        took = {s for s, dd in zip(bk.symbols, bk.signal_dates) if dd == d}
        assert took == set(fired["symbol"].head(2))
    assert bk.expectancy_net != pytest.approx(ew.expectancy_net, abs=1e-9)


def test_n1_the_loop_freezes_the_book_selected_expectation_and_mints_the_equal_weighted_one_as_context(opened_2):
    from pathfinder.experiments.hypotheses import book_select, BookLimits
    lp = opened_2
    eid = _exp(lp)
    v1 = lp.xstore.versions(eid)[0]
    exp = json.loads(v1["expectation_json"])
    assert exp["population"].startswith("book-selected") and "at most 2 new per signal session" in exp["population"]
    assert exp["signals_skipped"] > 0 and exp["signals_fired"] == exp["n"] + exp["signals_skipped"]
    assert exp["equal_weighted_n"] == exp["signals_fired"] and exp["equal_weighted_expectancy_net_pct"] != pytest.approx(exp["expectancy_net_pct"], abs=1e-9)
    # independent recompute: the book's own selection over the whole window, from the store's rule
    variant = HYP.Variant.from_dict(json.loads(v1["variant_json"]))
    md = lp.full.sealed(lp.dates[OPEN_AT])
    cx = HYP.Conditioning(md)
    df = md.df
    sig = variant.signals(md, cx) & df["f5"].notna()
    rows = df.loc[sig, ["symbol", "d", "f5"]].assign(_liq=cx.liquidity[sig])
    take = book_select(rows, session_index={d: i for i, d in enumerate(md.sessions)}, horizon=5,
                       limits=BookLimits(max_new_per_session=2, max_concurrent=10))
    net = rows.loc[take, "f5"].to_numpy() * 100 - lp.rcfg.hurdle_pct
    assert exp["expectancy_net_pct"] == pytest.approx(_wmean(net, 1.0), abs=1e-9) and exp["n"] == int(take.sum())
    assert json.loads(v1["grading_rule_json"])["spec"]["expected_net_pct"] == pytest.approx(exp["expectancy_net_pct"], abs=1e-4)
    # the facts say which is which, and every trial's stats carry both populations
    facts = {f["id"].rsplit("_", 1)[-1]: f for f in json.loads(v1["expectation_facts_json"])["expectation"]}
    assert facts["population"]["value"].startswith("book-selected")
    tr = lp.xstore.trials("finding", lp.xstore.experiment(eid)["source_finding_id"])
    st = json.loads(tr[0]["stats_json"])
    assert st["whole"]["selection"].startswith("book-selected") and st["equal_weighted"]["selection"].startswith("equal_weighted")
    card = views.card(lp.xstore, eid)
    assert any(f.id.endswith("_equal_weighted_expectancy") for f in card.facts)
    body = next(s.body for s in card.story if s.beat.value == "history_showed")
    assert "virtual book itself would have taken" in body and "equal-weighted" in body


@real
def test_n1_real_the_dip_rules_book_population_is_negative_where_the_equal_weighted_figure_was_positive(warehouse):
    """The demonstration number, recomputed on the raw warehouse at the archived seal (2026-05-04)."""
    md = warehouse.sealed("2026-05-04")
    rcfg = load_config()
    cx = HYP.Conditioning(md)
    w = HYP.windows_for(md, "2024-12-31")
    ev = HYP.measure(DIP, md, cx, w, rcfg=rcfg, draws=200, seed=1, limits=_limits(5, 10))
    assert ev.equal_weighted.n == 8337 and ev.equal_weighted.expectancy_net == pytest.approx(0.2456, abs=1e-3)
    assert ev.whole.expectancy_net < 0 and ev.whole.expectancy_net == pytest.approx(-0.285, abs=0.02)
    assert ev.whole.n == 1682 and ev.whole.hit_pct == pytest.approx(48.3, abs=0.2)
    assert ev.trailing.expectancy_net == pytest.approx(0.43, abs=0.05) and ev.equal_weighted.n > ev.whole.n
    g = worth_testing_gates(ev, c=load_constitution(CONSTITUTION), xcfg=xcfg_for(Path(os.environ.get("TEMP", ".")), min_n_history=100, min_n_trailing=30),
                            n_trials=18, evidence_strength=0.8, novel=True, novelty_note="n")
    assert not g.passed and "history_expectancy_net" in {x.name for x in g.failures}


# ═══════════════════════════════════════════════════════════════════════════
# N2 — the forward null is a fixed-day stock permutation, floored on signal days
# ═══════════════════════════════════════════════════════════════════════════

def test_n2_the_forward_null_holds_the_signal_days_fixed_and_draws_names_from_each_days_pool(tmp_path):
    from pathfinder.experiments.loop import fixed_day_permutation_means
    raw, idx, vix, dates, crash = synth_s2()
    rcfg = rcfg_for(tmp_path)
    md = MarketData.from_frame(raw, idx, vix, rcfg, as_of=dates[OPEN_AT + 30])
    d1, d2 = dates[crash[50]], dates[crash[51]]
    means = fixed_day_permutation_means(md, DIP, {d1: 2, d2: 3}, draws=300, seed=3)
    assert means.shape == (300,)
    df = md.df
    pools = {d: df.loc[(df["d"] == d) & df["f5"].notna(), "f5"].to_numpy() * 100 for d in (d1, d2)}
    # every draw is a mean of 2 names from d1's pool and 3 from d2's: bounded by those pools, and its
    # expectation is the pool-weighted mean — never a resample of OTHER days
    lo = (2 * pools[d1].min() + 3 * pools[d2].min()) / 5
    hi = (2 * pools[d1].max() + 3 * pools[d2].max()) / 5
    assert lo - 1e-9 <= means.min() and means.max() <= hi + 1e-9
    assert means.mean() == pytest.approx((2 * pools[d1].mean() + 3 * pools[d2].mean()) / 5, abs=0.15)
    assert np.array_equal(means, fixed_day_permutation_means(md, DIP, {d1: 2, d2: 3}, draws=300, seed=3)), "deterministic"


def test_n2_the_graduation_gate_is_insufficient_below_the_signal_day_floor_never_passed():
    c = load_constitution(CONSTITUTION)
    few = ForwardRecord(net_returns=(2.0,) * 8, signal_dates=("a", "a", "b", "b", "c", "c", "d", "d"), total_return_pct=3.0,
                        max_drawdown_pct=1.0, placebo_p=None, placebo_draws=0, cluster_t=None, min_signal_days=5)
    g = graduation_gates(few, incumbent=None, direction="long", horizon=5, c=c, slippage_pct=0.1)
    by = {x.name: x for x in g.gates}
    assert by["oos_placebo"].insufficient and not by["oos_placebo"].passed and "INSUFFICIENT" in by["oos_placebo"].statement
    assert by["oos_cluster_significance"].insufficient and not by["oos_cluster_significance"].passed
    enough = ForwardRecord(net_returns=(2.0,) * 10, signal_dates=tuple("aabbccddee"), total_return_pct=3.0, max_drawdown_pct=1.0,
                           placebo_p=0.01, placebo_draws=5000, cluster_t=9.0, min_signal_days=5)
    g2 = graduation_gates(enough, incumbent=None, direction="long", horizon=5, c=c, slippage_pct=0.1)
    by2 = {x.name: x for x in g2.gates}
    assert by2["oos_placebo"].passed and not by2["oos_placebo"].insufficient and "fixed-day stock permutation" in by2["oos_placebo"].statement
    assert by2["oos_cluster_significance"].passed


def test_n2_the_loop_records_the_floor_and_the_fixed_day_null_on_the_forward_gate_check(tmp_path):
    lp = Loop(tmp_path, forward_edge=+1, xcfg=xcfg_for(tmp_path, write_constitution(tmp_path, min_n=12), min_forward_signal_days=4))
    lp.run(OPEN_AT, OPEN_AT + 30)
    eid = _exp(lp)
    checks = lp.xstore.q("SELECT * FROM pfx_gate_checks WHERE experiment_id = ? ORDER BY edition_date", [eid])
    assert checks
    rows = [{g["name"]: g for g in json.loads(ch["gates_json"])} for ch in checks]
    for by in rows:
        assert "fixed-day stock permutation" in by["oos_placebo"]["statement"] and "insufficient" in by["oos_placebo"]
    days = [len({t["signal_date"] for t in lp.xstore.trades(eid, 1, resolved_only=True) if t["recorded_edition"] <= ch["edition_date"]})
            for ch in checks]
    assert any(d < 4 for d in days) and any(d >= 4 for d in days), "the run crosses the floor"
    for by, d in zip(rows, days):
        assert by["oos_placebo"]["insufficient"] == (d < 4) and (by["oos_placebo"]["value"] is None) == (d < 4)


@archived
def test_n2_real_the_archived_forward_record_scores_about_a_third_under_the_fixed_day_null(warehouse):
    """The demonstration: the 23 trades of 05-05 -> 06-23 over 5 signal days (p 0.162 under the old day-blocked null)."""
    from pathfinder.experiments.loop import fixed_day_permutation_means
    from pathfinder.experiments.hypotheses import placebo_p_value
    con = sqlite3.connect(str(ARCHIVED))
    con.row_factory = sqlite3.Row
    tr = [dict(r) for r in con.execute("SELECT symbol, signal_date, pnl_pct_net FROM pfx_trades WHERE exit_date <= '2026-06-23'")]
    con.close()
    assert len(tr) == 23 and len({t["signal_date"] for t in tr}) == 5
    md = warehouse.sealed("2026-06-23")
    rcfg = load_config()
    counts = {d: sum(1 for t in tr if t["signal_date"] == d) for d in {t["signal_date"] for t in tr}}
    stat = float(np.mean([t["pnl_pct_net"] for t in tr])) + rcfg.hurdle_pct
    p, used, se = placebo_p_value(lambda k, s: fixed_day_permutation_means(md, DIP, counts, draws=k, seed=s), stat,
                                  draws=5000, draws_near_bar=5000, bar=0.05, seed=20260910 + 7)
    assert used == 5000 and 0.25 <= p <= 0.45, (p, se)


# ═══════════════════════════════════════════════════════════════════════════
# N3 — concentration: how much of a window three days carry
# ═══════════════════════════════════════════════════════════════════════════

def test_n3_concentration_facts_and_the_advisory_without_best_day_gate(tmp_path):
    from pathfinder.experiments.hypotheses import concentration
    net = np.array([5.0, 4.0, -1.0, 0.5, -0.5, 0.2, 0.1, -0.3])
    days = np.array(["d1", "d1", "d2", "d3", "d3", "d4", "d5", "d6"])
    share, best, loo = concentration(net, days, 0.0)
    # day sums: d1 9.0, d2 -1.0, d3 0.0, d4 0.2, d5 0.1, d6 -0.3 -> the three best days carry 9.3 of a total of 8.0
    assert best == "d1" and share == pytest.approx((9.0 + 0.2 + 0.1) / net.sum() * 100) and loo == pytest.approx(net[2:].mean())
    assert concentration(-net, days, 0.0)[0] is None, "a share of a loss is not defined"
    good = HYP.measure(DIP, *_md_cx(tmp_path), rcfg=rcfg_for(tmp_path), draws=50, seed=1, limits=_limits())
    assert good.trailing.top3_days_share_pct is not None and good.trailing.expectancy_without_best_day_net is not None
    g = worth_testing_gates(good, c=load_constitution(CONSTITUTION), xcfg=xcfg_for(tmp_path), n_trials=18, evidence_strength=0.8,
                            novel=True, novelty_note="n")
    adv = next(x for x in g.gates if x.name == "trailing_expectancy_without_best_day")
    assert not adv.fatal and adv.value == pytest.approx(good.trailing.expectancy_without_best_day_net) and adv.bar == 0.0


def test_n3_the_card_and_the_expectation_carry_the_concentration_facts(opened_2):
    eid = _exp(opened_2)
    exp = json.loads(opened_2.xstore.versions(eid)[0]["expectation_json"])
    assert exp["trailing_expectancy_without_best_day_net_pct"] is not None and exp["top3_days_share_pct"] is not None
    card = views.card(opened_2.xstore, eid)
    ids = {f.id.split("_expectation_", 1)[-1] for f in card.facts if "_expectation_" in f.id}
    assert {"trailing_expectancy_without_best_day", "trailing_top3_days_share", "expectancy_without_best_day"} <= ids
    body = next(s.body for s in card.story if s.beat.value == "history_showed")
    assert "single best day removed" in body


@real
def test_n3_real_three_days_carry_the_trailing_window(warehouse):
    md = warehouse.sealed("2026-05-04")
    rcfg = load_config()
    cx = HYP.Conditioning(md)
    w = HYP.windows_for(md, "2024-12-31")
    t = HYP.replay(DIP, md, cx, start=w.trailing[0], end=w.trailing[1], rcfg=rcfg, draws=10, seed=1, with_placebo=False)
    assert t.n == 768 and t.expectancy_net == pytest.approx(1.927, abs=1e-2)
    assert t.best_day == "2025-04-04" and t.top3_days_share_pct == pytest.approx(105.7, abs=0.5)
    assert t.expectancy_without_best_day_net == pytest.approx(1.15, abs=0.01)


# ═══════════════════════════════════════════════════════════════════════════
# N4 — the family-wise trial count never restarts
# ═══════════════════════════════════════════════════════════════════════════

def test_n4_the_family_wise_bar_divides_by_the_familys_all_time_trials_across_retries(tmp_path):
    lp = Loop(tmp_path, forward_edge=+1, xcfg=xcfg_for(tmp_path, min_n_history=10 ** 6))   # nothing can clear; retry after 3 sessions
    lp.run(OPEN_AT, OPEN_AT + 8)
    rows = [r for r in lp.xstore.candidates() if r["family_id"] == "dip_bounce" and r["trials_evaluated"] > 0]
    assert len(rows) >= 2, "the family was researched, declined, and researched again after the retry window"
    assert [r["family_trials_all_time"] for r in rows[:2]] == [18, 36]
    assert lp.xstore.family_trials("dip_bounce") == 18 * len(rows)
    for r, n in zip(rows[:2], (18, 36)):
        for t in lp.xstore.trials("finding", r["finding_id"]):
            fw = next(g for g in json.loads(t["gates_json"]) if g["name"] == "family_wise_significance")
            assert fw["bar"] == pytest.approx(0.05 / n, abs=1e-6)
    resp = views.experiments_response(lp.xstore, engine_version=lp.xcfg.engine_version)
    assert sorted(x.family_trials_all_time for x in resp.not_opened if x.trials_evaluated)[:2] == [18, 36]


# ═══════════════════════════════════════════════════════════════════════════
# N5 — cluster statistics with a handful of clusters
# ═══════════════════════════════════════════════════════════════════════════

def test_n5_cr3_and_student_t_with_g_minus_1_govern_the_cumulative_kill_and_the_cluster_gate():
    from pathfinder.experiments.hypotheses import cluster_robust_se, t_critical
    assert t_critical(0.05, 4) == pytest.approx(2.776, abs=1e-3) and t_critical(0.05, 19) == pytest.approx(2.093, abs=1e-3)
    assert t_critical(0.05, 10 ** 6) == pytest.approx(1.960, abs=1e-3)
    rule = build_experiment_rule(DIP, expected_net_pct=0.3, hurdle_pct=0.5, min_trades=3, frozen_at=AT)
    assert rule.spec["cluster_t_kind"] == "CR3" and rule.spec["cumulative_bury_alpha"] == 0.05
    # five signal days, ten trades: a record whose CR0 t against z = 1.96 would bury, CR3 against t(4) = 2.78 does not
    days = np.repeat([f"2026-03-{k:02d}" for k in (2, 4, 6, 9, 11)], 2)
    net = np.array([0.1, 0.1, -1.1, -1.1, -0.1, -0.1, -0.9, -0.9, -0.5, -0.5])       # mean -0.5; CR0 t ~ -2.45
    mean, resid = net.mean(), net - net.mean()
    cr0 = np.sqrt(sum(resid[days == d].sum() ** 2 for d in np.unique(days))) / net.size
    assert mean / cr0 < -1.96, "CR0 against the normal bar would have buried this record"
    cr3 = cluster_robust_se(net, days)
    assert cr3 == pytest.approx(cr0 / (1 - 2 / 10)) and mean / cr3 > -t_critical(0.05, 4)
    assert cumulative_verdict(rule, net, days, 0) is None
    # a record that is negative by any standard (CR3 t about -35) still buries
    steady = np.array([-1.0, -0.8, -0.9, -1.1, -0.7, -1.3, -0.4, -1.6, -1.2, -0.6])
    assert steady.mean() / cluster_robust_se(steady, days) < -t_critical(0.05, 4)
    assert cumulative_verdict(rule, steady, days, 0) == "edge_did_not_persist_oos"
    c = load_constitution(CONSTITUTION)
    fr = ForwardRecord(net_returns=tuple(-net), signal_dates=tuple(days), total_return_pct=1.0, max_drawdown_pct=0.5,
                       placebo_p=0.01, placebo_draws=1000, cluster_t=2.2, min_signal_days=5)
    by = {x.name: x for x in graduation_gates(fr, incumbent=None, direction="long", horizon=5, c=c, slippage_pct=0.1).gates}
    assert by["oos_cluster_significance"].bar == pytest.approx(2.776, abs=1e-3) and not by["oos_cluster_significance"].passed


# ═══════════════════════════════════════════════════════════════════════════
# N6 — compliance lint: the five probes, and the engine's own beats
# ═══════════════════════════════════════════════════════════════════════════

PROBES = ("go long the dip", "a virtual long position from the next session's open",
          "hold for five sessions then exit at the close", "position size at most a tenth", "short it at the open")


def test_n6_the_public_card_rejects_every_probe_the_re_audit_slipped_past_the_lint(opened_2):
    for probe in PROBES:
        assert PUBLIC_CARD_BANNED_RE.search(probe), probe
    d = views.card(opened_2.xstore, _exp(opened_2)).model_dump(mode="json")
    for probe in PROBES:
        broken = json.loads(json.dumps(d))
        broken["story"][2]["body"] = f"History showed {probe}."
        with pytest.raises(ValueError, match="trade instruction"):
            ExperimentCard.model_validate(broken)


def test_n6_the_engines_public_beats_describe_a_study_not_a_position(opened_2):
    card = views.card(opened_2.xstore, _exp(opened_2))
    texts = [s.body for s in card.story] + [s.headline for s in card.story] + [card.theme]
    for text in texts:
        assert not PUBLIC_CARD_BANNED_RE.search(text), text
    hist = next(s.body for s in card.story if s.beat.value == "history_showed")
    decided = next(s.body for s in card.story if s.beat.value == "decided")
    assert "study window" in hist and "held for" not in hist
    assert "in any one position" not in decided and "fraction" not in decided
    rule = views.record(opened_2.xstore, _exp(opened_2)).current_rule_text
    assert "position" not in rule and "measured from the following session's open" in rule


# ═══════════════════════════════════════════════════════════════════════════
# N7 — a signature is three facts and a matching hash
# ═══════════════════════════════════════════════════════════════════════════

def test_n7_is_signed_needs_signed_by_signed_at_and_a_matching_document_hash(tmp_path):
    from pathfinder.engine.governance import Constitution, constitution_content_sha256
    doc = yaml.safe_load(CONSTITUTION.read_text(encoding="utf-8"))
    doc["approved_by"] = "Shyam"                       # the old string-prefix check would have called this signed
    assert not Constitution("v", doc, "Shyam", "").is_signed
    sha = constitution_content_sha256(doc)
    ok = Constitution("v", doc, "Shyam", "", signed_by="Shyam", signed_at="2026-09-10T18:00:00+05:30", document_sha256=sha)
    assert ok.is_signed and "signed by Shyam" in ok.signature_status
    assert not Constitution("v", doc, "Shyam", "", signed_by="Shyam", signed_at=None, document_sha256=sha).is_signed
    assert not Constitution("v", doc, "Shyam", "", signed_by=None, signed_at="t", document_sha256=sha).is_signed
    assert not Constitution("v", doc, "Shyam", "", signed_by="Shyam", signed_at="t", document_sha256="0" * 64).is_signed
    # a governed number changed AFTER signing: the hash no longer matches, the document is unsigned again
    doc2 = json.loads(json.dumps(doc))
    doc2["gauntlet"]["max_placebo_p_value"] = 0.10
    tampered = Constitution("v", doc2, "Shyam", "", signed_by="Shyam", signed_at="t", document_sha256=sha)
    assert not tampered.is_signed and "does not match" in tampered.signature_status
    # through the file: the signature block is read, and the shipped draft is unsigned
    p = write_constitution(tmp_path, signed=True)
    assert load_constitution(p).is_signed
    assert not load_constitution(CONSTITUTION).is_signed and "UNSIGNED" in load_constitution(CONSTITUTION).signature_status


# ═══════════════════════════════════════════════════════════════════════════
# N8 — placebo resolution and convention
# ═══════════════════════════════════════════════════════════════════════════

def test_n8_the_placebo_redraws_to_five_thousand_near_the_bar_and_compares_winsorised_means():
    from pathfinder.experiments.hypotheses import block_placebo_means, placebo_p_value
    rng0 = np.random.default_rng(0)
    null = rng0.normal(0, 1, 200_000)

    def means_fn(k, s):
        return np.random.default_rng(s).choice(null, size=k, replace=True)

    near = float(np.quantile(means_fn(1000, 1), 0.95))          # a statistic whose first-1000 p sits ON the bar
    p, used, se = placebo_p_value(means_fn, near, draws=1000, draws_near_bar=5000, bar=0.05, seed=1)
    assert used == 5000 and abs(p - 0.05) < 0.02 and se == pytest.approx(np.sqrt(p * (1 - p) / 5000), rel=1e-6)
    far = float(np.quantile(null, 0.50))
    p2, used2, _ = placebo_p_value(means_fn, far, draws=1000, draws_near_bar=5000, bar=0.05, seed=1)
    assert used2 == 1000 and abs(p2 - 0.5) < 0.05
    p3, used3, _ = placebo_p_value(means_fn, near, draws=1000, draws_near_bar=5000, bar=None, seed=1)
    assert used3 == 1000, "no bar, no redraw"
    # winsorised draw means: an outlier in the pool cannot fabricate a draw
    days = np.repeat(np.arange(20), 50)
    vals = np.zeros(1000); vals[7] = 1e6
    raw = block_placebo_means(days, vals, np.array([5, 5, 5, 5]), draws=400, seed=2)
    win = block_placebo_means(days, vals, np.array([5, 5, 5, 5]), draws=400, seed=2, winsor_pct=10.0)
    assert raw.max() > 1e4 and win.max() < 1.0


def test_n8_the_replay_records_its_convention_and_resolution(tmp_path):
    ev = HYP.measure(DIP, *_md_cx(tmp_path), rcfg=rcfg_for(tmp_path), draws=100, seed=1, limits=_limits(), bar=0.05, draws_near_bar=300)
    w = ev.whole
    assert w.placebo_convention.startswith("winsorised") and w.placebo_se is not None and w.placebo_draws in (100, 300)
    md, cx, _ = _md_cx(tmp_path)
    plain = HYP.replay(DIP, md, cx, start="2024-01-01", end=md.as_of, rcfg=rcfg_for(tmp_path), draws=100, seed=1)
    assert plain.placebo_draws == 100, "no bar given: no redraw"
    assert "winsorised" in next(x.statement for x in worth_testing_gates(
        ev, c=load_constitution(CONSTITUTION), xcfg=xcfg_for(tmp_path), n_trials=18, evidence_strength=0.8, novel=True,
        novelty_note="n").gates if x.name == "history_placebo")


# ═══════════════════════════════════════════════════════════════════════════
# N9 — one drawdown convention
# ═══════════════════════════════════════════════════════════════════════════

def test_n9_every_drawdown_is_relative_to_the_running_peak_on_both_sides_of_the_incumbent_gate():
    from pathfinder.experiments.book import BookRun, DRAWDOWN_CONVENTION, drawdowns_relative_to_peak
    from pathfinder.schemas import ForwardResult
    eq = pd.Series([1_000_000.0, 1_200_000.0, 900_000.0, 1_000_000.0], index=["a", "b", "c", "d"])
    run = BookRun(1e6, "a", "d", "d", (), (), eq, 0, 0)
    mdd, cdd = run.drawdowns()
    assert mdd == pytest.approx(25.0) and cdd == pytest.approx(100 * (1.2 - 1.0) / 1.2)      # NOT 30 / 20 points of capital
    assert drawdowns_relative_to_peak(eq.to_numpy() / 1e6) == (mdd, cdd)
    assert "running peak" in DRAWDOWN_CONVENTION and "running peak" in ForwardResult.model_fields["drawdown_convention"].default
    c = load_constitution(CONSTITUTION)
    inc = BookRun(1e6, "a", "d", "d", (), (), eq, 1, 1, "equal_weight_hold")
    fr = ForwardRecord(net_returns=(1.0,) * 10, signal_dates=tuple("aabbccddee"), total_return_pct=5.0, max_drawdown_pct=24.0,
                       placebo_p=0.01, placebo_draws=1000, cluster_t=3.0, min_signal_days=5)
    by = {x.name: x for x in graduation_gates(fr, incumbent=inc, direction="long", horizon=5, c=c, slippage_pct=0.1).gates}
    assert by["no_worse_drawdown_than_incumbent"].bar == pytest.approx(25.0) and by["no_worse_drawdown_than_incumbent"].passed
    assert "running peak" in by["no_worse_drawdown_than_incumbent"].statement


# ── helpers ───────────────────────────────────────────────────────────────────

def _md_cx(tmp_path):
    raw, idx, vix, dates, crash = synth_s2()
    md = MarketData.from_frame(raw, idx, vix, rcfg_for(tmp_path), as_of=dates[OPEN_AT])
    return md, HYP.Conditioning(md), HYP.windows_for(md, "2024-12-31")
