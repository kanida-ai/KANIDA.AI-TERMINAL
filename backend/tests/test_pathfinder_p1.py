"""
P1 guardrail suite — the engine.

P0's 37 rows tested the *contract* against fixtures. These test the *engine*: the
point-in-time seal, the marking conventions, the cost arithmetic, the governance
hierarchy, the gateway's output contract, and the laws the database enforces on its
own. Rows are listed in docs/TEST_PLAN.md as P1-01 … P1-46.

The last block runs against the REAL run database when one exists, so the honesty
rules are checked against engine output rather than against anything hand-written. It
skips (loudly) when the loop has not been run.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from pathfinder.engine import governance as GOV                       # noqa: E402
from pathfinder.engine import observe as OB                           # noqa: E402
from pathfinder.engine import replay as RP                            # noqa: E402
from pathfinder.engine.config import EngineConfig, load_config        # noqa: E402
from pathfinder.engine.costs import DEFAULT_COSTS                     # noqa: E402
from pathfinder.engine.evidence import Provenance, compute_metrics, drawdowns  # noqa: E402
from pathfinder.engine.hypothesis import HypothesisSpec, ParameterOutOfRange  # noqa: E402
from pathfinder.engine.market import (                                # noqa: E402
    LookAheadError, PriceFrames, Universe, expanding_percentile,
)
from pathfinder.engine.repository import Repository, row_hash         # noqa: E402
from pathfinder.llm import contracts as CT                            # noqa: E402
from pathfinder.llm.budget import BudgetLedger, cost_usd              # noqa: E402
from pathfinder.llm.gateway import (                                  # noqa: E402
    BudgetExceeded, GatewayError, JOB_MODEL_ROUTING, Job, OutputContractViolation,
)
from pathfinder.llm.schemas import SCHEMAS                            # noqa: E402

MIGRATION = ROOT / "migrations" / "0002_pathfinder_sqlite.sql"
CONSTITUTION = ROOT / "config" / "pathfinder_constitution.yaml"


# ── synthetic frames: total control over every bar ──────────────────────────

def make_frames(bars: dict[str, list[tuple[float, float, float, float, float]]],
                *, start: date = date(2024, 1, 1), as_of: date | None = None) -> PriceFrames:
    """`bars[symbol] = [(o, h, l, c, v), ...]`, one tuple per consecutive session."""
    n = len(next(iter(bars.values())))
    idx = pd.DatetimeIndex([pd.Timestamp(start) + timedelta(days=i) for i in range(n)])
    cols = list(bars)
    frame = {k: pd.DataFrame(index=idx, columns=cols, dtype=float) for k in "ohlcv"}
    for sym, rows in bars.items():
        for i, (o, h, l, c, v) in enumerate(rows):
            frame["o"].iloc[i, cols.index(sym)] = o
            frame["h"].iloc[i, cols.index(sym)] = h
            frame["l"].iloc[i, cols.index(sym)] = l
            frame["c"].iloc[i, cols.index(sym)] = c
            frame["v"].iloc[i, cols.index(sym)] = v
    cfg = EngineConfig()
    return PriceFrames(
        as_of=as_of or idx[-1].date(), opens=frame["o"], highs=frame["h"], lows=frame["l"],
        closes=frame["c"], volumes=frame["v"],
        index_close=frame["c"].mean(axis=1), universe=Universe("test", tuple(cols)),
        data_source="synthetic", cfg=cfg,
    )


@pytest.fixture(scope="module")
def constitution() -> GOV.Constitution:
    return GOV.load_constitution(CONSTITUTION)


@pytest.fixture()
def repo(tmp_path) -> Repository:
    r = Repository(str(tmp_path / "t.db"))
    r.migrate(MIGRATION)
    now = datetime(2026, 9, 8, 12, 0, 0)
    r.put_constitution(version="c@1", document={"x": 1}, approved_by="tester",
                       effective_from="2026-01-01", previous_version=None, created_at=now)
    r.put_hypothesis(hypothesis_id="hyp_1", question="q", statement="s", rationale="r",
                     proposed_by="engine", model=None, prompt_version=None, llm_call_id=None,
                     novelty_recall_ids=[], constitution_version="c@1",
                     created_at=now.isoformat())
    r.put_experiment(experiment_id="exp_0001", hypothesis_id="hyp_1",
                     opened_at=now.isoformat(), universe="u", direction="long",
                     horizon_sessions=5, cost_convention="costs@1", data_source="ds",
                     constitution_version="c@1", created_by="engine",
                     created_at=now.isoformat())
    r.commit()
    return r


# ═══════════════════════════════════════════════════════════════════════════
# Point-in-time (P1-01 … P1-05)
# ═══════════════════════════════════════════════════════════════════════════

def test_p1_01_frame_cannot_hold_a_bar_after_its_as_of():
    f = make_frames({"A": [(1, 1, 1, 1, 1)] * 5})
    with pytest.raises(LookAheadError):
        PriceFrames(as_of=f.dates[0].date(), opens=f.o, highs=f.h, lows=f.l, closes=f.c,
                    volumes=f.v, index_close=f.index_close, universe=f.universe,
                    data_source="x", cfg=f.cfg)


def test_p1_02_reseal_can_only_narrow():
    f = make_frames({"A": [(1, 1, 1, 1, 1)] * 5})
    assert f.sealed_at(f.dates[2].date()).dates[-1].date() == f.dates[2].date()
    with pytest.raises(LookAheadError):
        f.sealed_at(f.as_of + timedelta(days=10))


def test_p1_03_window_end_after_as_of_is_refused():
    f = make_frames({"A": [(1, 1, 1, 1, 1)] * 5})
    with pytest.raises(LookAheadError):
        f.session_slice(f.dates[0].date(), f.as_of + timedelta(days=1))


def test_p1_04_expanding_percentile_never_looks_forward():
    s = pd.Series([1.0, 2.0, 3.0, 100.0, 4.0])
    got = expanding_percentile(s, min_periods=1)
    # Nothing precedes the first reading, so there is no percentile to give.
    assert np.isnan(got.iloc[0])
    # The spike ranks top-of-history at the moment it happens...
    assert got.iloc[3] == 1.0
    # ...and only a LATER reading knows the spike happened: 4.0 sits above three of the
    # four values before it. A full-sample rank would have put 4.0 at 0.8 and — the real
    # tell — would have ranked the very first element at 0.2 using the future.
    assert got.iloc[4] == pytest.approx(0.75)
    assert s.rank(pct=True).iloc[0] == pytest.approx(0.2)


def test_p1_05_replay_window_after_the_seal_is_refused():
    f = make_frames({"A": [(1, 1, 1, 1, 1)] * 10})
    spec = HypothesisSpec("streak_up", {"sessions": 2}, "any", "long", 2, 5.0, None)
    with pytest.raises(LookAheadError):
        RP.replay(spec, f, window_start=f.dates[0].date(),
                  window_end=f.as_of + timedelta(days=1), costs=DEFAULT_COSTS)


# ═══════════════════════════════════════════════════════════════════════════
# Marking conventions (P1-06 … P1-12)
# ═══════════════════════════════════════════════════════════════════════════

def _rising(n: int = 40) -> list[tuple[float, float, float, float, float]]:
    """
    A steadily rising, deeply liquid name. It must be at least 40 bars: the
    point-in-time liquidity filter needs 20 sessions of history before ANY name is
    tradeable, so a short frame produces no signals at all — which is correct
    behaviour, and would otherwise make these tests pass vacuously.
    """
    return [(100 + i, 101 + i, 99 + i, 100.5 + i, 1e9) for i in range(n)]


def test_p1_06_entry_is_the_next_open_never_the_signal_bar():
    f = make_frames({"A": _rising()})
    spec = HypothesisSpec("streak_up", {"sessions": 2}, "any", "long", 3, 20.0, None)
    res = RP.replay(spec, f, window_start=f.dates[25].date(), window_end=f.dates[35].date(),
                    costs=DEFAULT_COSTS)
    assert res.n > 0
    for t in res.trades:
        assert t.entry_date > t.signal_date
        i = list(f.dates.date).index(t.signal_date)
        assert t.entry_price == pytest.approx(float(f.o.iloc[i + 1, 0]))


def test_p1_07_costs_are_charged_to_every_closed_trade_winners_included():
    f = make_frames({"A": _rising()})
    spec = HypothesisSpec("streak_up", {"sessions": 2}, "any", "long", 3, 20.0, None)
    res = RP.replay(spec, f, window_start=f.dates[25].date(), window_end=f.dates[35].date(),
                    costs=DEFAULT_COSTS)
    assert res.n > 0
    for t in res.trades:
        assert t.pnl_pct_net == pytest.approx(t.pnl_pct_gross - DEFAULT_COSTS.round_trip_pct)
        assert t.costs_pct > 0


def test_p1_08_same_session_stop_and_target_resolves_as_the_stop():
    # Session 3 spans both levels. A daily bar cannot order them; assuming the good
    # one is how a backtest lies, so the stop must win.
    bars = [(100, 101, 99, 100, 1e9), (100, 101, 99, 100, 1e9), (100, 101, 99, 100, 1e9),
            (100, 130, 70, 100, 1e9), (100, 101, 99, 100, 1e9), (100, 101, 99, 100, 1e9)]
    f = make_frames({"A": bars})
    spec = HypothesisSpec("inside_day", {"sessions": 1}, "any", "long", 3, 10.0, 10.0)
    g = RP.ExitGrid(spec, f, DEFAULT_COSTS)
    sig = np.zeros(g.entry.shape, bool)
    sig[2, 0] = True                              # entry at the open of session 3
    trades = g.trades_for(sig)
    assert trades and trades[0].exit_reason == "stop"


def test_p1_09_a_gap_through_the_stop_fills_at_the_open_not_at_the_level():
    bars = [(100, 101, 99, 100, 1e9), (100, 101, 99, 100, 1e9),
            (100, 101, 99, 100, 1e9), (80, 82, 78, 80, 1e9), (80, 81, 79, 80, 1e9)]
    f = make_frames({"A": bars})
    spec = HypothesisSpec("inside_day", {"sessions": 1}, "any", "long", 2, 10.0, None)
    g = RP.ExitGrid(spec, f, DEFAULT_COSTS)
    sig = np.zeros(g.entry.shape, bool)
    sig[1, 0] = True                              # entry at the open of session 3 (=100)
    t = g.trades_for(sig)[0]
    assert t.exit_reason == "stop"
    assert t.exit_price == pytest.approx(80.0)    # the open, worse than the 90 stop level
    assert t.exit_price < 90.0


def test_p1_10_a_signal_too_close_to_the_seal_is_not_evidence():
    """`entry_idx + horizon <= today_idx`, enforced mechanically by NaN, not by a check."""
    f = make_frames({"A": _rising()})
    spec = HypothesisSpec("streak_up", {"sessions": 2}, "any", "long", 4, 20.0, None)
    g = RP.ExitGrid(spec, f, DEFAULT_COSTS)
    # The last four rows cannot resolve a four-session horizon inside the frame...
    for k in range(1, 5):
        assert not g.resolved[-k].any(), f"row -{k} should not resolve"
    # ...and the row before them MUST, or the rule is over-conservative by one and
    # silently throwing away evidence. A one-sided check would not have caught that.
    assert g.resolved[-5].any(), "the last resolvable row must resolve: off-by-one"


def test_p1_11_two_times_slippage_is_exactly_two_extra_slippage_sides():
    f = make_frames({"A": _rising()})
    spec = HypothesisSpec("streak_up", {"sessions": 2}, "any", "long", 3, 20.0, None)
    res = RP.replay(spec, f, window_start=f.dates[25].date(), window_end=f.dates[35].date(),
                    costs=DEFAULT_COSTS)
    assert res.n > 0
    m = compute_metrics(res, costs_2x=DEFAULT_COSTS.at_slippage_multiple(2.0))
    delta = m.expectancy_pct_per_trade - m.expectancy_2x_slippage_pct_per_trade
    assert delta == pytest.approx(2 * DEFAULT_COSTS.slippage_bps_per_side / 100.0)
    assert m.expectancy_2x_slippage_pct_per_trade < m.expectancy_pct_per_trade


def test_p1_12_drawdown_is_reported_positive_and_from_the_running_peak():
    mdd, cdd = drawdowns(np.array([0.0, 5.0, 2.0, 8.0, 3.0]))
    assert mdd == pytest.approx(5.0)              # 8 -> 3
    assert cdd == pytest.approx(5.0)
    assert drawdowns(np.array([]))[0] == 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Selection discipline (P1-13 … P1-16)
# ═══════════════════════════════════════════════════════════════════════════

def test_p1_13_discovery_scan_refuses_a_frame_that_can_see_past_the_window():
    f = make_frames({"A": _rising(400)})  # noqa: PLR2004
    with pytest.raises(ValueError, match="could see the future"):
        OB.scan_discovery(f, EngineConfig(), DEFAULT_COSTS,
                          window_start=f.dates[0].date(),
                          window_end=f.dates[10].date())


def test_p1_14_out_of_range_parameters_are_rejected_never_clamped(constitution):
    payload = {"trigger": "oversold", "params": {"lookback": 3, "thr_pct": 99.0},
               "context": "any", "direction": "long", "horizon_sessions": 5, "stop_pct": 5.0}
    with pytest.raises(ParameterOutOfRange, match="outside the approved range"):
        HypothesisSpec.parse(payload, approved=constitution.approved_ranges)


def test_p1_15_unknown_primitives_and_contexts_are_rejected(constitution):
    for bad in ({"trigger": "moon_phase"}, {"context": "vibes"}, {"direction": "sideways"}):
        payload = {"trigger": "oversold", "params": {"lookback": 3, "thr_pct": 8.0},
                   "context": "any", "direction": "long", "horizon_sessions": 5,
                   "stop_pct": 5.0, **bad}
        with pytest.raises(ParameterOutOfRange):
            HypothesisSpec.parse(payload, approved=constitution.approved_ranges)


def test_p1_16_a_valid_proposal_parses_and_keeps_integer_parameters_integral(constitution):
    spec = HypothesisSpec.parse(
        {"trigger": "oversold", "params": {"lookback": 3, "thr_pct": 8.0}, "context": "any",
         "direction": "long", "horizon_sessions": 5, "stop_pct": 5.0, "target_pct": 8.0},
        approved=constitution.approved_ranges)
    assert isinstance(spec.params["lookback"], int)
    # The rulebook states what makes the idea WRONG. It never states a price to hope for.
    assert spec.invalidation_text
    assert "stop" in spec.invalidation_text.lower()
    for banned in ("price target", "target price", "expected return"):
        assert banned not in spec.invalidation_text.lower()
        assert banned not in spec.entry_text.lower()


# ═══════════════════════════════════════════════════════════════════════════
# Governance (P1-17 … P1-22)
# ═══════════════════════════════════════════════════════════════════════════

def test_p1_17_multi_session_shorts_are_not_implementable():
    short = HypothesisSpec("gap_down", {"thr_pct": 3.0}, "any", "short", 10, 8.0, None)
    assert not GOV.check_implementable(short).ok
    assert GOV.check_implementable(
        HypothesisSpec("gap_down", {"thr_pct": 3.0}, "any", "short", 1, 8.0, None)).ok
    assert GOV.check_implementable(
        HypothesisSpec("gap_up", {"thr_pct": 3.0}, "any", "long", 10, 8.0, None)).ok


def test_p1_18_promotion_is_blocked_while_the_constitution_is_unsigned(constitution):
    assert not constitution.is_signed
    assert not GOV.promotion_gate(constitution).passed


def test_p1_19_l4_is_human_only():
    with pytest.raises(GOV.ConstitutionError, match="human-only"):
        GOV.ChangeLogEntry(seq=1, level="L4", at=datetime.now(), what_changed="x", why="y",
                           evidence_ids=("evd_1",), previous_version=None, new_version="v2",
                           improved=None, decided_by="engine")


def test_p1_20_l3_must_state_its_validation():
    with pytest.raises(GOV.ConstitutionError, match="forward-validated"):
        GOV.ChangeLogEntry(seq=1, level="L3", at=datetime.now(), what_changed="x", why="y",
                           evidence_ids=("evd_1",), previous_version="v1", new_version="v2",
                           improved=None, decided_by="engine")


def test_p1_21_every_change_must_cite_evidence():
    with pytest.raises(GOV.ConstitutionError, match="evidence"):
        GOV.ChangeLogEntry(seq=1, level="L1", at=datetime.now(), what_changed="x", why="y",
                           evidence_ids=(), previous_version=None, new_version="v2",
                           improved=None, decided_by="engine")


def test_p1_22_l2_parameter_check_raises_rather_than_clamping(constitution):
    GOV.check_parameter_within_range(constitution, "oversold", "thr_pct", 8.0)
    with pytest.raises(GOV.ConstitutionError, match="L2 violation"):
        GOV.check_parameter_within_range(constitution, "oversold", "thr_pct", 99.0)


def test_p1_23_provenance_refuses_to_name_a_model_or_look_ahead():
    ok = dict(data_source="ds", range_start=date(2020, 1, 1), range_end=date(2021, 1, 1),
              as_of=date(2021, 1, 1), cost_convention="c", computed_by="replay@1.0.0",
              computed_at=datetime.now())
    Provenance(**ok)
    with pytest.raises(ValueError, match="names a model"):
        Provenance(**{**ok, "computed_by": "claude-sonnet-5"})
    with pytest.raises(ValueError, match="post-date"):
        Provenance(**{**ok, "range_end": date(2022, 1, 1)})


# ═══════════════════════════════════════════════════════════════════════════
# The gateway's output contract (P1-24 … P1-31)
# ═══════════════════════════════════════════════════════════════════════════

FACTS = [{"id": "fct_a", "label": "a", "value": 1.0, "unit": "pct", "n": 10}]


def test_p1_24_llm_prose_may_not_contain_a_bare_numeral():
    with pytest.raises(OutputContractViolation, match="literal numerals"):
        CT.assert_no_bare_numerals("expectancy fell to 0.7%", where="t")
    CT.assert_no_bare_numerals("expectancy fell to {{fact:fct_a}}", where="t")


def test_p1_25_a_narrate_headline_may_not_contain_a_digit_at_all():
    with pytest.raises(OutputContractViolation, match="headline"):
        CT.enforce_narrate({"beat": "outcome", "headline": "Down 7 percent",
                            "body": "x", "fact_refs": []}, FACTS)


def test_p1_26_a_model_may_not_reference_a_fact_it_was_not_given():
    with pytest.raises(OutputContractViolation, match="not supplied"):
        CT.enforce_narrate({"beat": "outcome", "headline": "Ok", "body": "x",
                            "fact_refs": ["fct_zzz"]}, FACTS)


def test_p1_27_fact_refs_used_in_prose_must_be_declared():
    with pytest.raises(OutputContractViolation, match="not declared"):
        CT.enforce_narrate({"beat": "outcome", "headline": "Ok",
                            "body": "it was {{fact:fct_a}}", "fact_refs": []}, FACTS)


def test_p1_28_classify_may_not_invent_a_category():
    with pytest.raises(OutputContractViolation, match="closed set"):
        CT.enforce_classify({"label": "maybe", "rationale": "x"}, ["yes", "no"])


def test_p1_29_reason_cannot_express_a_constitution_change():
    assert "L4" not in SCHEMAS["pathfinder.reason.v1"]["properties"]["learning_level"]["enum"]
    with pytest.raises(OutputContractViolation, match="L4 is human-only"):
        CT.enforce_reason({"claim": "c", "body": "b", "fact_refs": [], "abstained": False,
                           "learning_level": "L4"}, FACTS, job=Job.critique)


def test_p1_30_the_fact_table_handed_to_a_model_is_deterministically_ordered():
    a = CT.render_facts([{"id": "fct_b", "v": 2}, {"id": "fct_a", "v": 1}])
    b = CT.render_facts([{"id": "fct_a", "v": 1}, {"id": "fct_b", "v": 2}])
    assert a == b, "an unsorted fact table silently invalidates the prompt cache"


def test_p1_31_the_daily_budget_is_checked_before_the_call():
    led = BudgetLedger(daily_cost_usd=0.01)
    led.check(job="narrate")
    led.spent_usd = 0.02
    with pytest.raises(BudgetExceeded, match="budget"):
        led.check(job="narrate")


def test_p1_32_cost_comes_from_token_counts_at_the_published_rates():
    # 1M input + 1M output on Sonnet 5 = $2 + $10.
    assert cost_usd("claude-sonnet-5", input_tokens=1_000_000,
                    output_tokens=1_000_000) == pytest.approx(12.0)
    with pytest.raises(ValueError, match="no published rate"):
        cost_usd("gpt-9", input_tokens=1, output_tokens=1)


def test_p1_33_the_job_not_the_caller_picks_the_model():
    assert JOB_MODEL_ROUTING[Job.hypothesis] == "claude-sonnet-5"
    assert JOB_MODEL_ROUTING[Job.narrate] == "claude-haiku-4-5"
    assert set(JOB_MODEL_ROUTING.values()) <= {
        "claude-sonnet-5", "claude-haiku-4-5", "claude-opus-5"}


def test_p1_34_the_loop_refuses_to_wake_a_model_without_a_trigger(tmp_path, constitution):
    from pathfinder.engine.loop import PathfinderLoop
    from pathfinder.llm.providers.recorded import NullProvider
    r = Repository(str(tmp_path / "w.db"))
    r.migrate(MIGRATION)
    loop = PathfinderLoop(cfg=EngineConfig(), costs=DEFAULT_COSTS, repo=r,
                          llm=NullProvider(), constitution=constitution,
                          budget=BudgetLedger(daily_cost_usd=1.0).budget)
    with pytest.raises(GatewayError, match="never by a clock"):
        loop._wake(job=Job.hypothesis, trigger_id=None, question="q", facts=[],
                   prompt_version="p")


# ═══════════════════════════════════════════════════════════════════════════
# Laws the DATABASE enforces (P1-35 … P1-44)
# ═══════════════════════════════════════════════════════════════════════════

def _fact_row(**over):
    base = dict(fact_id="fct_x", experiment_id="exp_0001", label="l", value_num=1.0,
                value_text=None, unit="pct", n=10, data_source="ds",
                range_start="2020-01-01", range_end="2021-01-01", as_of="2021-01-01",
                cost_convention="c", computed_by="replay@1.0.0",
                computed_at="2026-09-08T00:00:00", universe="u", note=None)
    return {**base, **over}


def test_p1_35_history_tables_reject_update(repo):
    repo.put_fact(**_fact_row())
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        repo.con.execute("UPDATE facts SET label = 'edited' WHERE fact_id = 'fct_x'")


def test_p1_36_history_tables_reject_delete(repo):
    repo.put_fact(**_fact_row())
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        repo.con.execute("DELETE FROM facts WHERE fact_id = 'fct_x'")


def test_p1_37_a_fact_may_not_claim_a_model_computed_it(repo):
    with pytest.raises(sqlite3.IntegrityError, match="names a model"):
        repo.put_fact(**_fact_row(fact_id="fct_bad", computed_by="claude-sonnet-5"))


def test_p1_38_a_fact_may_not_be_computed_from_data_after_its_as_of(repo):
    with pytest.raises(sqlite3.IntegrityError):
        repo.put_fact(**_fact_row(fact_id="fct_ahead", range_end="2026-01-01",
                                  as_of="2021-01-01"))


def test_p1_39_llm_prose_with_a_numeral_is_rejected_by_the_database(repo):
    ok = dict(story_line_id="stl_1", experiment_id="exp_0001", cycle_id="cyc_1",
              beat="outcome", headline="Weaker out of sample",
              body="expectancy fell to {{fact:fct_x}}", produced_by="llm",
              model="claude-sonnet-5", prompt_version="p", llm_call_id=None,
              fact_ids=["fct_x"], at="2026-09-08T00:00:00")
    repo.put_story_line(**ok)
    with pytest.raises(sqlite3.IntegrityError, match="literal numeral"):
        repo.put_story_line(**{**ok, "story_line_id": "stl_2",
                               "body": "expectancy fell to 0.7 percent"})


def test_p1_40_a_trade_must_enter_after_its_signal_bar(repo):
    base = dict(trade_id="trd_1", experiment_id="exp_0001", strategy_version=None,
                symbol="A", direction="long", signal_date="2024-01-02",
                entry_date="2024-01-03", entry_price=100.0, exit_date="2024-01-08",
                exit_price=105.0, exit_reason="horizon", holding_sessions=5,
                pnl_pct_gross=5.0, pnl_pct_net=4.6, costs_pct=0.4, slippage_bps=10.0,
                mfe_pct=6.0, mae_pct=-1.0, as_of="2024-01-08",
                computed_by="virtual_book@1.0.0", created_at="2026-09-08T00:00:00")
    repo.put_trade(**base)
    with pytest.raises(sqlite3.IntegrityError, match="entry_is_after_the_signal_bar"):
        repo.put_trade(**{**base, "trade_id": "trd_2", "entry_date": "2024-01-02"})


def test_p1_41_a_closed_trade_without_costs_is_rejected(repo):
    with pytest.raises(sqlite3.IntegrityError, match="closed_trade_is_costed"):
        repo.put_trade(trade_id="trd_3", experiment_id="exp_0001", strategy_version=None,
                       symbol="A", direction="long", signal_date="2024-01-02",
                       entry_date="2024-01-03", entry_price=100.0, exit_date="2024-01-08",
                       exit_price=105.0, exit_reason="horizon", holding_sessions=5,
                       pnl_pct_gross=5.0, pnl_pct_net=None, costs_pct=None,
                       slippage_bps=10.0, mfe_pct=6.0, mae_pct=-1.0, as_of="2024-01-08",
                       computed_by="virtual_book@1.0.0", created_at="2026-09-08T00:00:00")


def test_p1_42_a_rulebook_may_not_contain_a_target_price(repo):
    base = dict(strategy_version="exp_0001:strategy@v1.0", experiment_id="exp_0001",
                version_label="v1.0", previous_version=None, created_by="engine",
                approved_by=None, validation=None, active_from=None, retired_at=None,
                created_at="2026-09-08T00:00:00")
    repo.put_strategy_version(**base, rulebook={"entry": "e", "exit": "x", "invalidation": "i"})
    with pytest.raises(sqlite3.IntegrityError, match="rulebook_has_no_target_price"):
        repo.put_strategy_version(
            **{**base, "strategy_version": "exp_0001:strategy@v9.9"},
            rulebook={"entry": "e", "exit": "x", "invalidation": "i", "target_price": 250})


def test_p1_43_l4_and_l3_are_enforced_by_the_database_too(repo):
    base = dict(experiment_id="exp_0001", at="2026-09-08T00:00:00", what_changed="w",
                why="y", evidence_ids=["evd_1"], previous_version=None, new_version="v2",
                outcome_before_id=None, outcome_after_id=None, improved=None,
                approved_by=None, validation=None, constitution_version="c@1",
                statement_story_line_id=None)
    with pytest.raises(sqlite3.IntegrityError, match="l4_is_human_only"):
        repo.put_learning_event(learning_event_id="lrn_1", seq=1, level="L4",
                                decided_by="engine", **base)
    with pytest.raises(sqlite3.IntegrityError, match="l3_requires_validation"):
        repo.put_learning_event(learning_event_id="lrn_2", seq=2, level="L3",
                                decided_by="engine", **base)


def test_p1_44_a_parameter_outside_its_approved_range_is_rejected(repo):
    repo.put_strategy_version(strategy_version="exp_0001:strategy@v2.0",
                              experiment_id="exp_0001", version_label="v2.0",
                              previous_version=None, created_by="engine", approved_by=None,
                              validation=None, active_from=None, retired_at=None,
                              created_at="2026-09-08T00:00:00",
                              rulebook={"entry": "e", "exit": "x", "invalidation": "i"})
    with pytest.raises(sqlite3.IntegrityError, match="parameter_within_approved_range"):
        repo.put_parameter(strategy_version="exp_0001:strategy@v2.0", name="oversold.thr_pct",
                           value_num=99.0, value_text=None, unit="pct", approved_min=3.0,
                           approved_max=25.0, set_by="engine", at="2026-09-08T00:00:00")


def test_p1_45_the_hash_chain_detects_a_rewritten_row(repo, tmp_path):
    repo.put_state(experiment_id="exp_0001", status="queued", at="2026-09-08T00:00:00",
                   decision_id=None, reason="opened")
    repo.put_state(experiment_id="exp_0001", status="testing", at="2026-09-08T00:01:00",
                   decision_id=None, reason="testing")
    repo.commit()
    ok, msg = repo.verify_chain("experiment_state")
    assert ok, msg
    # Bypass the append-only trigger the only way anyone could: another connection with
    # the trigger dropped. The chain must still notice.
    repo.close()
    raw = sqlite3.connect(str(tmp_path / "t.db"))
    raw.execute("DROP TRIGGER experiment_state_no_update")
    raw.execute("UPDATE experiment_state SET reason = 'tampered' WHERE status = 'queued'")
    raw.commit()
    raw.close()
    r2 = Repository(str(tmp_path / "t.db"))
    ok2, msg2 = r2.verify_chain("experiment_state")
    assert not ok2 and "mismatch" in msg2


# ═══════════════════════════════════════════════════════════════════════════
# The served payload, from the REAL run (P1-46 … P1-58)
# ═══════════════════════════════════════════════════════════════════════════

RUN_DB = ROOT / "var" / "pathfinder.db"
pytestmark_engine = pytest.mark.skipif(
    not RUN_DB.exists(),
    reason="no run database — `python scripts/run_pathfinder_loop.py --fresh` first",
)


@pytest.fixture(scope="module")
def served():
    if not RUN_DB.exists():
        pytest.skip("no run database")
    from pathfinder.store_engine import EngineStore
    s = EngineStore(str(RUN_DB))
    return {
        "loop": s.loop(),
        "list": s.experiments(),
        "learnings": s.learnings(),
        "details": [s.experiment(i.id) for i in s.experiments().items],
        "store": s,
    }


@pytestmark_engine
def test_p1_46_every_endpoint_serialises_under_the_p0_contract(served):
    for key in ("loop", "list", "learnings"):
        assert json.loads(served[key].model_dump_json())
    assert served["details"] and all(d is not None for d in served["details"])


@pytestmark_engine
def test_p1_47_no_llm_authored_line_in_the_real_run_contains_a_numeral(served):
    lines = list(served["loop"].story)
    for d in served["details"]:
        lines += list(d.story)
        if d.post_mortem:
            lines.append(d.post_mortem.summary)
    assert lines
    for ln in lines:
        if ln.produced_by.value == "llm":
            for text in (ln.headline, ln.body):
                assert not any(c.isdigit() for c in CT.strip_refs(text)), ln


@pytestmark_engine
def test_p1_48_no_number_in_the_real_run_claims_a_model_computed_it(served):
    provs = [f.provenance for f in served["loop"].facts]
    for d in served["details"]:
        provs += [f.provenance for f in d.facts]
        provs += [e.provenance for e in d.evidence]
        for blk in (d.historical_return, d.virtual_return):
            if blk:
                provs.append(blk.provenance)
    assert len(provs) > 20
    banned = ("claude", "gpt", "gemini", "sonnet", "haiku", "opus", "llm")
    for p in provs:
        assert not any(b in p.computed_by.lower() for b in banned), p.computed_by


@pytestmark_engine
def test_p1_49_no_number_in_the_real_run_was_computed_past_its_as_of(served):
    provs = [f.provenance for f in served["loop"].facts]
    for d in served["details"]:
        provs += [f.provenance for f in d.facts] + [e.provenance for e in d.evidence]
    for p in provs:
        assert p.date_range.end <= p.as_of, p


@pytestmark_engine
def test_p1_50_every_real_trade_enters_after_its_signal_and_is_costed(served):
    trades = [t for d in served["details"] if d.virtual_book
              for t in d.virtual_book.ledger_losers_first]
    if not trades:
        pytest.skip("no experiment reached the virtual book in this run")
    for t in trades:
        assert t.entry_date > t.signal_date
        assert t.costs_pct and t.costs_pct > 0
        assert t.slippage_bps is not None


@pytestmark_engine
def test_p1_51_the_ledger_is_losers_first(served):
    for d in served["details"]:
        if d.virtual_book:
            pnl = [t.pnl_pct_net for t in d.virtual_book.ledger_losers_first]
            assert pnl == sorted(pnl)


@pytestmark_engine
def test_p1_52_the_experiment_list_leads_with_the_graveyard(served):
    items = served["list"].items
    if any(i.status.value == "died" for i in items):
        assert items[0].status.value == "died"


@pytestmark_engine
def test_p1_53_every_fact_reference_in_the_real_run_resolves(served):
    known = {f.id for f in served["loop"].facts}
    for ln in served["loop"].story:
        assert set(ln.fact_refs) <= known, ln.beat
    for d in served["details"]:
        ids = {f.id for f in d.facts}
        for ln in d.story:
            assert set(ln.fact_refs) <= ids, (d.id, ln.beat)


@pytestmark_engine
def test_p1_54_a_dead_experiment_publishes_a_post_mortem_that_kept_something(served):
    dead = [d for d in served["details"] if d.status.value == "died"]
    assert dead, "a run where nothing died would be the suspicious one"
    for d in dead:
        assert d.post_mortem is not None
        assert d.post_mortem.what_we_kept.strip()
        assert d.post_mortem.evidence_refs


@pytestmark_engine
def test_p1_55_nothing_is_promoted_while_the_constitution_is_unsigned(served):
    from pathfinder.engine.governance import load_constitution
    if load_constitution(CONSTITUTION).is_signed:
        pytest.skip("the Constitution has been signed; this row no longer applies")
    assert not any(i.status.value == "promoted" for i in served["list"].items)


@pytestmark_engine
def test_p1_56_every_performance_block_leads_with_expectancy_and_carries_a_drawdown(served):
    blocks = []
    for d in served["details"]:
        blocks += [b for b in (d.historical_return, d.virtual_return) if b]
        blocks += [e.performance for e in d.evidence if e.performance]
    assert blocks
    for b in blocks:
        assert b.expectancy_pct_per_trade is not None
        assert b.max_drawdown_pct is not None and b.current_drawdown_pct is not None
        # 2x slippage is lower by construction; the row that earns its keep is the next
        # one: only a BOOK may claim a return, because only a book has capital.
        assert b.expectancy_2x_slippage_pct_per_trade < b.expectancy_pct_per_trade
        if b.basis == "historical_replay":
            assert b.total_return_pct is None, (
                "a replay is a per-trade study with overlapping signals; calling its "
                "cumulative unit-stake sum a `total_return_pct` is how a 3,866% "
                "'return' gets published"
            )
        else:
            assert b.total_return_pct is not None


@pytestmark_engine
def test_p1_57_a_live_llm_call_used_the_model_its_job_routes_to(served):
    rows = served["store"].repo.query("SELECT job, model, provider FROM llm_calls")
    for r in rows:
        if r["provider"] == "live:anthropic":
            assert r["model"] == JOB_MODEL_ROUTING[Job(r["job"])], dict(r)
        assert r["model"] in ("claude-sonnet-5", "claude-haiku-4-5", "claude-opus-5")


@pytestmark_engine
def test_p1_58_every_hash_chain_in_the_real_run_is_intact(served):
    for table, ok, msg in served["store"].repo.verify_all_chains():
        assert ok, msg


@pytest.fixture()
def engine_client(monkeypatch):
    """
    A TestClient wired to the ENGINE store, with the process left exactly as it was
    found. The first version of this leaked `KANIDA_PATHFINDER_SOURCE=engine` and the
    resolved singleton into every test that ran afterwards, which broke 31 P0 rows —
    a fixture that reaches into process state has to put it back.
    """
    from fastapi.testclient import TestClient
    from pathfinder import store as store_mod
    from pathfinder.mock_app import app
    monkeypatch.setenv("KANIDA_PATHFINDER_SOURCE", "engine")
    monkeypatch.setenv("KANIDA_PATHFINDER_DB", str(RUN_DB))
    store_mod.set_store(None)
    try:
        yield TestClient(app)
    finally:
        store_mod.set_store(None)


@pytestmark_engine
def test_p1_59_the_endpoints_serve_engine_data_over_http_from_any_thread(engine_client):
    """
    A regression row with a scar behind it: the store is a process-wide singleton and
    FastAPI runs sync endpoints in a THREADPOOL, so a thread-bound SQLite connection
    served the first request and killed every one after it. Hitting several endpoints
    in sequence is what catches that — a single call would have passed.
    """
    for url in ("/api/pathfinder/loop",
                "/api/pathfinder/experiments",
                "/api/pathfinder/experiments?status=died",
                "/api/pathfinder/learnings",
                "/api/pathfinder/loop"):
        r = engine_client.get(url)
        assert r.status_code == 200, (url, r.status_code, r.text[:200])
        assert r.json()


@pytestmark_engine
def test_p1_60_an_unknown_experiment_returns_a_guarded_error(engine_client):
    r = engine_client.get("/api/pathfinder/experiment/exp_9999")
    assert r.status_code == 404
    body = r.json()["error"]
    assert body["code"] and body["message"]
    # Never a stack trace, a SQL statement, or a filesystem path.
    assert not any(t in body["message"].lower()
                   for t in ("select ", "traceback", "sqlite", "\\", "/"))
