"""
S1 quant-audit regression suite — one row per CONFIRMED / mandatory-PLAUSIBLE finding.

Every row here would have FAILED on the S1 engine as handed over and encodes the auditor's
recomputed expectation (docs/handbacks/PF-S1.md §4). Rows marked `real` run on the price
warehouse (close 2026-07-29) and skip loudly when it is absent.

    C1  stale run / wrong published numbers    -> code hash on every edition, determinism, fail-loud share guard
    C2  rotation REJECT graded by a rule it satisfied mechanically -> rotation_reject on sector-minus-market only
    C3  theme_watch verdicts asymmetric         -> symmetric ±hurdle on the excess; rank is a fact, not a verdict
    C4  zero-lift anomaly published as evidence -> unconditional control minted, z on the lift, null result says so
    C5  pair evidence ≠ graded metric, n overstated -> next-open spread P&L on episodes is the primary fact
    C6  rotation base rate close-to-close       -> sector-minus-market f5 (next open -> horizon close)
    C7  theme persistence gate never binds      -> "leaders like this" on the card's own criteria, graded metric
    C8  fabricated n on parameters / observations -> sample_flag = not_applicable, no n
    P1  number words / quantifiers, headline replaced, zero refs -> strict feed contract, engine headline kept
    P2  rule version a constant, config fallback in the grader -> hash of grading.py, complete spec required
    P7  post-seal prices on a sealed frame      -> dropped in sealed()
    P4  scoreboard pending understated          -> pending as of X
    P5  decisions on median + hit rate only     -> expectancy (mean net of hurdle) minted and gating
    D1  (found while fixing) listing-day glitch bars counted as +6% surges -> holes
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from pathfinder.llm.contracts import OutputContractViolation, enforce_narrate                # noqa: E402
from pathfinder.llm.gateway import ClassifyResult, Job, NarrateResult, Usage                 # noqa: E402
from pathfinder.research import config as CFG                                               # noqa: E402
from pathfinder.research import data as DATA                                                # noqa: E402
from pathfinder.research import library as LIB                                              # noqa: E402
from pathfinder.research.config import ResearchConfig                                       # noqa: E402
from pathfinder.research.data import MarketData, POST_SEAL_COLUMNS                          # noqa: E402
from pathfinder.research.facts import FactSet                                               # noqa: E402
from pathfinder.research.grading import GRADING_RULES_VERSION, build_rule, evaluate         # noqa: E402
from pathfinder.research.narrate import Narrator                                            # noqa: E402
from pathfinder.research.ranking import score                                               # noqa: E402
from pathfinder.research.regime import build_regime, regime_on                              # noqa: E402
from pathfinder.research.scan import grade_due, run_scan                                    # noqa: E402
from pathfinder.research.store import ResearchStore                                         # noqa: E402
from pathfinder.schemas import Decision, Fact, GradingKind, GradingRule, SampleFlag, Verdict  # noqa: E402
from tests.test_pathfinder_s1 import AT, _tiny, load, scan_dates, synth                     # noqa: E402

H = 0.30


@pytest.fixture(scope="module")
def synthetic():
    return synth()


@pytest.fixture()
def cfg(tmp_path) -> ResearchConfig:
    return ResearchConfig(research_db=str(tmp_path / "r.db"), price_db="unused",
                          usefulness_threshold=0.0, pairs=(("B0", "B1"), ("A0", "A1")))


def _facts(draft) -> dict[str, Fact]:
    """fact id suffix (after the subject slug) -> Fact."""
    out = {}
    for f in draft.facts.facts:
        out[f.id[len(draft.facts.prefix) + 1:]] = f
    return out


# ═══════════════════════════════════════════════════════════════════════════
# C1 — a run is attributable to its code, deterministic, and cannot mint a 0%/100% share
# ═══════════════════════════════════════════════════════════════════════════

def test_c1_edition_carries_the_content_hash_of_the_research_code(synthetic, cfg):
    assert re.fullmatch(r"pathfinder_research@\d+\.\d+\.\d+\+code\.[0-9a-f]{12}", cfg.engine_version)
    assert cfg.code_hash == CFG.research_code_hash()
    store = ResearchStore(cfg.research_db)
    rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=scan_dates(synthetic)[320]), computed_at=AT)
    row = store.edition(rep.edition_date)
    assert row["engine_version"] == cfg.engine_version
    # every fact's computed_by is pinned to the same hash
    for f in store.findings_for(rep.edition_date):
        assert all(x.provenance.computed_by.endswith("+code." + cfg.code_hash) for x in f.facts)


def test_c1_the_code_hash_changes_when_a_research_file_changes(tmp_path):
    a = tmp_path / "a.py"
    b = tmp_path / "b.py"
    a.write_text("x = 1\n")
    b.write_text("y = 2\n")
    h1 = CFG.code_hash(a, b)
    b.write_text("y = 3\n")
    assert CFG.code_hash(a, b) != h1 and len(h1) == 12


def test_c1_scanning_the_same_seal_twice_gives_identical_cards(synthetic, tmp_path):
    """The smoke store and the handed-over store disagreed on an edition computed from the same bars."""
    cards = []
    for k in (1, 2):
        c = ResearchConfig(research_db=str(tmp_path / f"s{k}.db"), price_db="unused",
                           usefulness_threshold=0.0, pairs=(("B0", "B1"), ("A0", "A1")))
        st = ResearchStore(c.research_db)
        rep = run_scan(c, st, md=load(synthetic, c, as_of=scan_dates(synthetic)[320]), computed_at=AT)
        rows = st.con.execute("SELECT finding_id, card_json FROM pf_findings ORDER BY rank").fetchall()
        cards.append([(r[0], r[1]) for r in rows])
        assert rep.published
    assert cards[0] == cards[1]


def test_c1_a_share_of_exactly_zero_or_a_hundred_over_a_hundred_cases_is_refused():
    cfg = ResearchConfig(research_db="unused", price_db="unused")
    fs = FactSet(finding_slug="t", cfg=cfg, as_of="2026-07-29", period_start="2013-01-01",
                 period_end="2026-07-29", component="t", computed_at=AT)
    with pytest.raises(ValueError, match="degenerate"):
        fs.add("a", "share that had moved", 0.0, "pct", n=13119)       # the smoke run's big_move
    with pytest.raises(ValueError, match="degenerate"):
        fs.add("b", "share higher", 100.0, "pct", n=100)
    fs.add("c", "share higher", 0.0, "pct", n=50)                       # small n: labelled, allowed
    fs.add("d", "typical next-session move (median)", 0.0, "pct", n=12632)   # a median of zero is a value
    fs.add("e", "share still top-three", 100.0, "pct", n=301, degenerate_ok=True)  # structurally bounded


# ═══════════════════════════════════════════════════════════════════════════
# C2 / C3 — theme_watch and rotation_reject are symmetric on sector-minus-market
# ═══════════════════════════════════════════════════════════════════════════

def _five_sector_universe(cfg, subject_move: float):
    """
    Five sectors of two names. Over the five graded sessions the subject sector S0 moves
    `subject_move` from the next open; S1..S3 gain 3% late in the window so that S0 ranks
    FOURTH by relative strength at the horizon (window 3) even when it beat the market.
    """
    secs = {f"{s}{i}": s for s in ("S0", "S1", "S2", "S3", "S4") for i in (0, 1)}
    flat = [(100, 100)] * 20
    c0 = 100 * (1 + subject_move)
    paths = {}
    for sym, s in secs.items():
        if s == "S0":
            paths[sym] = flat + [(100, c0)] + [(c0, c0)] * 4
        elif s in ("S1", "S2", "S3"):
            paths[sym] = flat + [(100, 100)] * 3 + [(100, 103)] + [(103, 103)]
        else:
            paths[sym] = flat + [(100, 100)] * 5
    return _tiny(paths, cfg, sectors=secs), secs


def _sector_rule(kind, h=5):
    spec = {"sector": "S0", "members": ["S00", "S01"], "window": 3, "min_names": 2}
    return build_rule(kind, horizon=h, hurdle_pct=H, spec=spec, frozen_at=AT, subject="S0")


@pytest.mark.parametrize("kind", [GradingKind.theme_watch, GradingKind.rotation_reject])
def test_c2_c3_a_not_a_cycle_claim_is_wrong_when_the_sector_beat_the_market_even_if_it_ranks_fourth(cfg, kind):
    md, _ = _five_sector_universe(cfg, +0.06)        # S0 +6%, market +3.0% -> excess +3.0 > hurdle
    d0 = md.sessions[19]
    got = evaluate(_sector_rule(kind), finding_slug="t", edition_date=d0, md=md, cfg=cfg, graded_at=AT)
    facts = {f.id.rsplit("_", 1)[-1]: f for f in got.facts.facts}
    assert facts["excess"].value == pytest.approx(3.0, abs=0.05)
    assert facts["horizon"].value >= 4, "the fixture must rank the subject outside the top three"   # rank_at_horizon
    # Old rule: "Right if ... or dropped out of the top three" -> Right. New: the sector beat the market -> WRONG.
    assert got.verdict == Verdict.wrong


@pytest.mark.parametrize("kind", [GradingKind.theme_watch, GradingKind.rotation_reject])
def test_c3_the_not_a_cycle_verdicts_are_symmetric_around_the_hurdle(cfg, kind):
    lag, _ = _five_sector_universe(cfg, -0.06)       # S0 -6%, market -0.6% -> excess -5.4
    inside, _ = _five_sector_universe(cfg, +0.021)   # S0 +2.1%, market +2.22 -> excess -0.12: inside ±0.30
    d0 = lag.sessions[19]
    ev = lambda md: evaluate(_sector_rule(kind), finding_slug="t", edition_date=d0, md=md, cfg=cfg, graded_at=AT).verdict  # noqa: E731
    assert ev(lag) == Verdict.right and ev(inside) == Verdict.inconclusive


def test_c2_rotation_reject_rule_text_judges_only_the_excess_and_records_rank_as_a_fact():
    r = _sector_rule(GradingKind.rotation_reject)
    assert "top three" not in r.right.lower() and "top-three" not in r.right.lower()
    assert "recorded as a fact, not judged" in r.metric
    w = _sector_rule(GradingKind.theme_watch)
    assert "dropped out" not in w.right and "stayed" not in w.wrong


# ═══════════════════════════════════════════════════════════════════════════
# C4 — the anomaly is judged by its LIFT over a named control
# ═══════════════════════════════════════════════════════════════════════════

def test_c4_z_is_measured_on_the_lift_beyond_a_minimum_not_against_the_coin_flip():
    # the auditor's numbers: 47.35% on 13,154 vs 48.44% unconditional -> zero evidence
    assert LIB._z_lift(47.35 - 48.44, 48.44, 13154, 5.0) == 0.0
    assert LIB._z(47.35, 13154) > 6.0                       # what the old code scored it as
    assert LIB._z_lift(+15.0, 48.44, 13154, 5.0) > 3.0      # a real lift is evidence


def test_c4_the_anomaly_card_names_the_control_and_a_null_result_is_no_trade(synthetic, cfg):
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    card = LIB.compute_volume_anomaly(ctx, LIB.parameters_for(LIB.VOLUME_ANOMALY, cfg))[0]
    v = _facts(card)
    assert v["base_big_move"].n == int(md.df["r5cc"].notna().sum()) and "unconditional" in v["base_big_move"].label
    assert v["lift"].value == pytest.approx(v["big_move"].value - v["base_big_move"].value, abs=1e-3)
    assert "control" in card.comparison_group and "unconditional" in card.comparison_group
    assert card.evidence_z == pytest.approx(
        LIB._z_lift(v["lift"].value, v["base_big_move"].value, card.n, cfg.anomaly_min_lift_pp), abs=1e-2)
    if v["lift"].value < cfg.anomaly_min_lift_pp:
        assert card.decision == Decision.no_trade and card.grading_rule.kind == GradingKind.no_trade_call
    else:
        assert card.decision == Decision.new_experiment and card.grading_rule.kind == GradingKind.anomaly_move


# ═══════════════════════════════════════════════════════════════════════════
# C5 — the pair's primary fact is the traded metric, on episodes
# ═══════════════════════════════════════════════════════════════════════════

def test_c5_episodes_count_a_run_of_consecutive_extreme_sessions_once():
    assert list(LIB._episodes(np.array([3, 4, 5, 9, 10, 20]))) == [3, 9, 20]
    assert list(LIB._episodes(np.array([], dtype=int))) == []


def test_c5_the_pair_card_leads_with_the_spread_trade_and_n_is_episodes(synthetic, cfg):
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    card = LIB.compute_relationship(ctx, LIB.parameters_for(LIB.RELATIONSHIP, cfg))[0]
    v = _facts(card)
    assert card.n == v["episodes"].value <= v["event_days"].value
    assert v["spread_right"].n == v["episodes"].value and v["event_spread_right"].n == v["event_days"].value
    assert "overlapping" in v["event_days"].label and "not a trade result" in v["snapped_back"].label
    assert card.key_facts[1].endswith("_spread_right") and card.key_facts[2].endswith("_spread_typical")
    assert "next open" in v["spread_right"].label
    assert card.evidence_z == pytest.approx(LIB._z(v["spread_up"].value, card.n))


# ═══════════════════════════════════════════════════════════════════════════
# C6 / C7 — theme statistics on the graded metric (synthetic shape; real numbers below)
# ═══════════════════════════════════════════════════════════════════════════

def test_c7_the_strong_gate_uses_leaders_like_this_and_expectancy_not_any_leader_persistence(synthetic, cfg):
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    theme = LIB.compute_theme_cycle(ctx, LIB.parameters_for(LIB.THEME_CYCLE, cfg))[0]
    v = _facts(theme)
    assert "any leader" in v["persistence"].label and "context only" in v["persistence"].label
    assert theme.n == v["like_this_cases"].value and v["like_this_beat"].n == theme.n
    assert "next open" in v["like_this_beat"].label
    strong = (v["sector_return"].value > v["market_return"].value and v["days_out"].value >= 9
              and LIB.call_on_excess(n=theme.n, up_pct=v["like_this_up"].value, med=v["like_this_typical_excess"].value,
                                     etv=v["like_this_expectancy"].value, H=H, min_hit=58.0))
    assert (theme.decision == Decision.virtual_long) == strong
    assert any(k.endswith("_like_this_beat") for k in theme.key_facts)


# ═══════════════════════════════════════════════════════════════════════════
# C8 — no fabricated n
# ═══════════════════════════════════════════════════════════════════════════

def test_c8_a_statistic_without_n_is_refused_but_a_parameter_or_observation_is_flagged_not_applicable():
    cfg = ResearchConfig(research_db="unused", price_db="unused")
    fs = FactSet(finding_slug="t", cfg=cfg, as_of="2026-07-29", period_start="2013-01-01",
                 period_end="2026-07-29", component="t", computed_at=AT)
    with pytest.raises(ValueError, match="requires n"):
        fs.add("a", "share higher", 51.0, "pct")
    with pytest.raises(ValueError, match="do not pass n"):
        fs.add("b", "today's move", -12.7, "pct", n=1, sample="observation")
    o = fs.add("c", "today's move", -12.7, "pct", sample="observation")
    p = fs.add("d", "the threshold", 6.0, "pct", sample="parameter")
    assert o.n is None and o.sample_flag == SampleFlag.not_applicable and "observation" in o.note
    assert p.n is None and p.sample_flag == SampleFlag.not_applicable and "parameter" in p.note
    with pytest.raises(ValueError, match="cannot carry an n"):
        Fact(id="fct_x", label="x", value=1.0, unit="pct", n=5, sample_flag="not_applicable", provenance=o.provenance)


def test_c8_no_card_carries_an_invented_n_on_a_parameter_or_a_single_observation(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=scan_dates(synthetic)[320]), computed_at=AT)
    items = store.findings_for(rep.edition_date)
    single = ("_move_today", "_volume_x", "_sigma", "_threshold", "_move_threshold", "_band", "_min_lift",
              "_watch_1_move", "_watch_2_move", "_other_1_return", "_other_2_return", "_other_3_return", "_window")
    seen = 0
    for f in items:
        for x in f.facts:
            if x.sample_flag == SampleFlag.not_applicable:
                assert x.n is None
            if x.id.endswith(single) and f.template_id != "market_regime":   # the market's move is a mean over names
                seen += 1
                assert x.sample_flag == SampleFlag.not_applicable and x.n is None, x.id
            if x.unit.value in ("pct", "ratio", "x") and x.sample_flag != SampleFlag.not_applicable:
                assert x.n is not None and x.n >= 1
        for x in f.grading.realized_facts:
            assert x.n is None or x.n > 1 or x.sample_flag == SampleFlag.not_applicable
    assert seen >= 6


# ═══════════════════════════════════════════════════════════════════════════
# P1 — the strict feed contract
# ═══════════════════════════════════════════════════════════════════════════

FACTS = [{"id": "fct_a", "label": "share", "value": 90.0, "unit": "pct", "n": 100}]


def _nar(body, headline="Ok", refs=("fct_a",)):
    return {"beat": "noticed", "headline": headline, "body": body, "fact_refs": list(refs)}


@pytest.mark.parametrize("body", [
    "It resolved nine in ten times. Costs are {{fact:fct_a}}.",
    "Half of them moved. Costs are {{fact:fct_a}}.",
    "The edge doubled since then. Costs are {{fact:fct_a}}.",
    "It works most of the time. Costs are {{fact:fct_a}}.",
    "It usually carries. Costs are {{fact:fct_a}}.",
    "Roughly one in four failed; costs are {{fact:fct_a}}.",
])
def test_p1_a_quantity_written_in_words_needs_a_fact_ref_in_the_same_sentence(body):
    enforce_narrate(_nar(body), FACTS)                      # the old contract let it through
    with pytest.raises(OutputContractViolation, match="quantity in words"):
        enforce_narrate(_nar(body), FACTS, strict=True)


def test_p1_a_quantity_with_a_ref_in_the_same_sentence_is_fine_and_zero_refs_is_not():
    enforce_narrate(_nar("It resolved {{fact:fct_a}} of the time, nine in ten."), FACTS, strict=True)
    with pytest.raises(OutputContractViolation, match="at least one fact_ref"):
        enforce_narrate(_nar("Nothing carries.", refs=()), FACTS, strict=True)
    with pytest.raises(OutputContractViolation, match="number word"):
        enforce_narrate(_nar("Costs are {{fact:fct_a}}.", headline="Half the market moved"), FACTS, strict=True)


class _HeadlineLLM:
    provider_name = "fake"

    def classify(self, *, text, labels, facts=(), constitution_version, prompt_version, budget, batch=True, key=""):
        return ClassifyResult(job=Job.classify, model="claude-haiku-4-5", usage=Usage("claude-haiku-4-5", 1, 1),
                              prompt_version=prompt_version, constitution_version=constitution_version,
                              raw_json={"label": "publish", "rationale": "because"}, label="publish", rationale="because")

    def narrate(self, *, beat, facts, context=(), constitution_version, prompt_version, budget, batch=True, key=""):
        fid = facts[0]["id"]
        p = enforce_narrate({"beat": "noticed", "headline": "Buy everything now",
                             "body": "The move was {{fact:" + fid + "}}.", "fact_refs": [fid]}, facts)
        return NarrateResult(job=Job.narrate, model="claude-haiku-4-5", usage=Usage("claude-haiku-4-5", 1, 1),
                             prompt_version=prompt_version, constitution_version=constitution_version, raw_json=p,
                             beat=p["beat"], headline=p["headline"], body=p["body"], fact_refs=tuple(p["fact_refs"]))


def test_p1_the_model_cannot_replace_the_engines_headline(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    nar = Narrator(_HeadlineLLM(), provider_name="fake")
    rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=scan_dates(synthetic)[320]), narrator=nar, computed_at=AT)
    items = store.findings_for(rep.edition_date)
    assert items and all(f.narrative.produced_by.value == "llm" for f in items)
    assert not any("Buy everything" in f.narrative.headline for f in items)
    assert all(f.narrative.body.startswith("The move was") for f in items)


# ═══════════════════════════════════════════════════════════════════════════
# P2 — the rule version is the evaluator's hash; the spec must be complete
# ═══════════════════════════════════════════════════════════════════════════

def test_p2_rule_version_is_the_content_hash_of_grading_py_and_specs_must_be_complete():
    import hashlib
    src = (ROOT / "backend" / "pathfinder" / "research" / "grading.py").read_bytes()
    assert GRADING_RULES_VERSION.endswith("+code." + hashlib.sha256(src).hexdigest()[:12])
    with pytest.raises(ValueError, match="missing"):
        build_rule(GradingKind.theme_watch, horizon=5, hurdle_pct=H, frozen_at=AT, subject="X",
                   spec={"sector": "X", "members": ["A"]})                 # no window / min_names
    with pytest.raises(ValueError, match="missing"):
        build_rule(GradingKind.anomaly_move, horizon=5, hurdle_pct=H, frozen_at=AT, subject="X", spec={"symbol": "X"})
    with pytest.raises(ValueError, match="symbol"):
        build_rule(GradingKind.no_trade_call, horizon=1, hurdle_pct=H, frozen_at=AT, subject="X", spec={"subject_kind": "stock"})


def test_p2_the_grader_never_falls_back_to_live_config(cfg):
    md, _ = _five_sector_universe(cfg, +0.06)
    ok = _sector_rule(GradingKind.theme_watch)
    incomplete = GradingRule(**{**ok.model_dump(), "spec": {"sector": "S0", "members": ["S00", "S01"]}})
    with pytest.raises(ValueError, match="does not read live config"):
        evaluate(incomplete, finding_slug="t", edition_date=md.sessions[19], md=md, cfg=cfg, graded_at=AT)


# ═══════════════════════════════════════════════════════════════════════════
# P7 — no post-seal price on a sealed frame
# ═══════════════════════════════════════════════════════════════════════════

def test_p7_a_sealed_frame_holds_no_post_seal_columns(synthetic, cfg):
    md = load(synthetic, cfg)
    assert set(POST_SEAL_COLUMNS).isdisjoint(md.df.columns)
    resealed = md.sealed(scan_dates(synthetic)[300])
    assert set(POST_SEAL_COLUMNS).isdisjoint(resealed.df.columns)
    assert str(resealed.df["d"].max()) == scan_dates(synthetic)[300]
    # and the last five rows of every symbol cannot carry a week-later outcome
    last = resealed.df.sort_values(["symbol", "d"]).groupby("symbol").tail(5)
    assert last["f5"].isna().all() and last["r5cc"].isna().all()


# ═══════════════════════════════════════════════════════════════════════════
# P4 — pending as of a date
# ═══════════════════════════════════════════════════════════════════════════

def test_p4_pending_counts_findings_that_were_ungraded_as_of_that_date(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[320]), computed_at=AT)
    grade_due(store, load(synthetic, cfg, as_of=dates[326]), cfg, graded_at=AT)
    n_pub = store.con.execute("SELECT COUNT(*) FROM pf_findings").fetchone()[0]
    assert store.scoreboard(dates[326]).pending == 0
    assert store.scoreboard(dates[320]).pending == n_pub          # nothing was graded on the edition date
    assert store.scoreboard(dates[319]).pending == 0              # nothing was published yet


# ═══════════════════════════════════════════════════════════════════════════
# P5 — expectancy decides
# ═══════════════════════════════════════════════════════════════════════════

def test_p5_a_median_that_clears_costs_with_negative_expectancy_is_not_a_call():
    kw = dict(wr5=None, med5=None, etv5=None, H=H, min_hit=58.0)
    assert LIB.dip_decision(wr1=60.0, med1=0.5, etv1=-0.1, **kw) == (Decision.no_trade, 1)
    assert LIB.dip_decision(wr1=60.0, med1=0.5, etv1=+0.1, **kw) == (Decision.virtual_long, 1)
    assert LIB.dip_decision(wr1=40.0, med1=0.0, etv1=0.0, wr5=60.0, med5=1.0, etv5=-0.5, H=H, min_hit=58.0) == (Decision.no_trade, 1)
    assert LIB.surge_decision(wr1=40.0, med1=-0.5, etv1=+0.2, H=H, min_hit=58.0, fade_max_hit=45.0) == Decision.no_trade
    assert LIB.surge_decision(wr1=40.0, med1=-0.5, etv1=-0.2, H=H, min_hit=58.0, fade_max_hit=45.0) == Decision.reject
    assert LIB.carry_decision(med=0.4, etv=-0.05, H=H) == Decision.no_trade
    assert not LIB.call_on_excess(n=2655, up_pct=51.8, med=0.10, etv=-0.17, H=H, min_hit=58.0)
    assert LIB.call_on_excess(n=121, up_pct=62.0, med=0.71, etv=0.22, H=2 * H, min_hit=58.0)


def test_p5_every_group_card_mints_an_expectancy_fact(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=scan_dates(synthetic)[320]), computed_at=AT)
    for f in store.findings_for(rep.edition_date):
        if f.template_id == "volume_anomaly":
            continue
        assert any("expectancy" in x.id for x in f.facts), f.template_id


# ═══════════════════════════════════════════════════════════════════════════
# D1 — glitch bars are holes
# ═══════════════════════════════════════════════════════════════════════════

def test_d1_a_ten_x_bar_is_a_hole_and_no_forward_window_straddles_it(synthetic, cfg):
    raw, idx, vix = synthetic
    raw = raw.copy()
    dates = scan_dates(synthetic)
    sel = (raw["symbol"] == "A4") & (raw["d"] == dates[50])
    raw.loc[sel, ["open", "close"]] *= 10.0
    md = MarketData.from_frame(raw, idx, vix, cfg)
    s = md.symbol("A4").set_index("d")
    assert np.isnan(s.loc[dates[50], "ret"]) and np.isnan(s.loc[dates[51], "ret"])    # 10x then 0.1x
    assert s.loc[dates[45]:dates[50], "f5"].isna().all() and s.loc[dates[49]:dates[50], "f1"].isna().all()
    assert np.isfinite(s.loc[dates[44], "f5"]) and np.isfinite(s.loc[dates[51], "f5"])  # windows that do not straddle are intact
    assert np.isfinite(md.df["ret"].dropna()).all() and (md.df["ret"].dropna().abs() < 3).all()


# ═══════════════════════════════════════════════════════════════════════════
# The auditor's recomputed numbers on the REAL warehouse (close 2026-07-29)
# ═══════════════════════════════════════════════════════════════════════════

PRICE_DB = os.environ.get("KANIDA_DB", r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
real = pytest.mark.skipif(not Path(PRICE_DB).exists(), reason="price warehouse not present")


@pytest.fixture(scope="module")
def real_ctx():
    if not Path(PRICE_DB).exists():
        pytest.skip("price warehouse not present")
    cfg = ResearchConfig(research_db="unused", usefulness_threshold=0.0)
    md = MarketData.load(cfg, as_of="2026-07-29")
    if md.as_of != "2026-07-29":
        pytest.skip("warehouse does not contain 2026-07-29")
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    cards = {}
    for t in LIB.LIBRARY:
        for d in t.computation(ctx, LIB.parameters_for(t, cfg)):
            cards[(d.template_id, d.subject)] = (d, t)
    return cfg, md, ctx, cards


@real
def test_real_c4_the_dcmshriram_anomaly_has_zero_lift_and_no_evidence(real_ctx):
    cfg, md, ctx, cards = real_ctx
    d, t = cards[("volume_anomaly", "DCMSHRIRAM")]
    v = _facts(d)
    assert v["big_move"].value == pytest.approx(47.35, abs=0.05) and v["big_move"].n == 13154
    assert v["base_big_move"].value == pytest.approx(48.44, abs=0.05) and v["base_big_move"].n > 1_250_000
    assert v["lift"].value == pytest.approx(-1.09, abs=0.05)
    assert d.evidence_z == 0.0 and d.decision == Decision.no_trade
    s = score(d, t, novelty=1.0, cfg=cfg)
    assert s.evidence_strength == pytest.approx(0.5)            # was 1.0 with usefulness 0.805, ranked #1
    assert s.total < 0.70
    assert v["volume_x"].n is None and v["move_today"].n is None and v["move_threshold"].n is None


@real
def test_real_c5_the_pair_card_is_the_spread_trade_on_121_episodes(real_ctx):
    cfg, md, ctx, cards = real_ctx
    d, _ = cards[("relationship", "HDFCBANK / ICICIBANK")]
    v = _facts(d)
    assert v["event_days"].value == 412 and v["episodes"].value == 121 and d.n == 121
    # the auditor's recompute on the 412 event-days
    assert v["event_spread_right"].value == pytest.approx(51.0, abs=0.1)
    assert v["event_spread_wrong"].value == pytest.approx(31.3, abs=0.1)
    assert v["event_spread_typical"].value == pytest.approx(0.69, abs=0.01)
    # the primary (episode) numbers
    assert v["spread_right"].value == pytest.approx(52.1, abs=0.1) and v["spread_wrong"].value == pytest.approx(28.9, abs=0.1)
    assert v["spread_typical"].value == pytest.approx(0.71, abs=0.01) and v["spread_up"].value == pytest.approx(62.0, abs=0.1)
    assert v["snapped_back"].value == pytest.approx(89.1, abs=0.1) and v["snapped_back"].n == 412
    assert v["sigma"].n is None
    assert "spread trade" in d.headline


@real
def test_real_c6_the_rotation_base_rate_is_next_open_to_horizon_close(real_ctx):
    cfg, md, ctx, cards = real_ctx
    d, _ = cards[("theme_cycle", "Telecommunication")]
    v = _facts(d)
    # the auditor recomputed 770 / 41.8% / -0.42% on f5; 771 once the six listing-day glitch
    # bars are holes (test_real_d1 below pins the 770 with the guard off)
    assert v["flips"].value == 771 and v["beat_market"].value == pytest.approx(41.8, abs=0.1)
    assert v["typical_excess"].value == pytest.approx(-0.42, abs=0.01)
    assert v["beat_after_costs"].value == pytest.approx(36.9, abs=0.1)
    assert "next open" in v["beat_market"].label and "next open" in v["typical_excess"].label
    assert d.decision == Decision.reject and d.grading_rule.kind == GradingKind.rotation_reject
    assert "next open" in d.grading_rule.metric


@real
def test_real_c7_it_leader_is_a_watch_because_leaders_like_this_did_not_pay(real_ctx):
    cfg, md, ctx, cards = real_ctx
    d, _ = cards[("theme_cycle", "Information Technology")]
    v = _facts(d)
    assert v["days_out"].value == 8 and v["sector_return"].value > v["market_return"].value
    assert v["like_this_cases"].value == 2655 and d.n == 2655
    assert v["like_this_beat"].value == pytest.approx(45.95, abs=0.1) and v["like_this_beat"].value < 58
    assert v["like_this_expectancy"].value < 0
    assert v["persistence"].value == pytest.approx(70.1, abs=0.1) and v["persistence"].n == 3316
    assert d.decision == Decision.watch and d.grading_rule.kind == GradingKind.theme_watch


@real
def test_real_c8_p5_dip_and_surge_after_the_glitch_fix(real_ctx):
    cfg, md, ctx, cards = real_ctx
    dip, _ = cards[("dip", "J&KBANK")]
    v = _facts(dip)
    assert dip.n == 12632 and v["typical_1"].value == 0.0 and v["expectancy_1"].value == pytest.approx(0.0, abs=0.01)
    assert v["expectancy_5"].value == pytest.approx(0.08, abs=0.02)   # the raw mean was +14.3% off one 831x bar
    assert dip.decision == Decision.no_trade
    surge, _ = cards[("surge", "PCBL")]
    w = _facts(surge)
    assert surge.n == 24727 and w["expectancy_1"].value < 0 and surge.decision == Decision.reject


@real
def test_real_d1_the_auditors_770_reproduces_with_the_glitch_guard_off(monkeypatch):
    monkeypatch.setattr(DATA, "GLITCH_RATIO", (0.0, float("inf")))
    cfg = ResearchConfig(research_db="unused", usefulness_threshold=0.0)
    md = MarketData.load(cfg, as_of="2026-07-29")
    assert (md.df["ret"].dropna() > 4).sum() == 6, "the six listing-day glitch bars are back as 'returns'"
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    rot = [c for c in LIB.compute_theme_cycle(ctx, LIB.parameters_for(LIB.THEME_CYCLE, cfg)) if c.subject == "Telecommunication"][0]
    v = _facts(rot)
    assert v["flips"].value == 770 and v["beat_market"].value == pytest.approx(41.8, abs=0.1)
    assert v["typical_excess"].value == pytest.approx(-0.42, abs=0.01)
    surge = LIB.compute_surge(ctx, LIB.parameters_for(LIB.SURGE, cfg))[0]
    assert surge.n == 24733                                        # six of them were glitch bars


@real
def test_real_p7_a_resealed_real_frame_holds_no_post_seal_price(real_ctx):
    cfg, md, ctx, cards = real_ctx
    re_ = md.sealed("2026-07-22")
    assert set(POST_SEAL_COLUMNS).isdisjoint(re_.df.columns) and re_.as_of == "2026-07-22"
    assert re_.today_rows()["f1"].isna().all() and len(re_.today_rows()) > 490


@real
def test_real_c1_the_same_seal_scanned_twice_is_byte_identical(real_ctx, tmp_path):
    cfg, md, ctx, cards = real_ctx
    out = []
    for k in (1, 2):
        c = ResearchConfig(research_db=str(tmp_path / f"r{k}.db"))
        st = ResearchStore(c.research_db)
        rep = run_scan(c, st, md=md, computed_at=AT)
        assert not rep.skipped and rep.published
        out.append(st.con.execute("SELECT finding_id, card_json FROM pf_findings ORDER BY rank").fetchall())
        out[-1] = [(r[0], r[1]) for r in out[-1]]
        assert st.edition(rep.edition_date)["regime"].startswith("NEUTRAL (risk score 46/100, breadth>200DMA 56%)")
    assert out[0] == out[1]
