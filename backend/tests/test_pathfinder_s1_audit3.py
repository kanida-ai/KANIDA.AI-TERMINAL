"""
S1 THIRD quant-audit regression suite — one row per finding (docs/handbacks/PF-S1.md §4, "Third audit").

Every row here FAILS on the engine as committed after the second fix pass (f34ecea) and encodes
the third auditor's finding. Rows marked `real` run on the price warehouse and skip loudly when
it is absent.

    N1  continuations bypassed the usefulness threshold and were served (15 of 19 below 0.65)
        -> the threshold is applied FIRST; a sub-threshold repeat of an open claim is a candidate, never a card;
           the Finding and FeedResponse contracts refuse a served card below the threshold
    N2  rounded evidence signatures re-opened the same claim (ANURAS -1.20 -> TORNTPHARM -1.10 -> SOBHA -1.10):
        three overlapping grades of one null claim counted as independent, n_total == n
        -> the claim is (template, decision, comparison group) with the primary statistic inside a tolerance;
           the scoreboard counts one grade per claim per non-overlapping horizon from the grade rows themselves
    N3  the pair WATCH was graded by `pair_convergence` — a claim the card did not make (a losing spread = Wrong)
        -> the kind follows the decision: `pair_watch` (Right if the declined spread trade lost more than costs);
           the Finding contract refuses a watch frozen under a call kind
    N4  `like_this_*` statistics minted with n = the overlapping session count while the decision used the effective n
        -> minted on the effective n; the overlapping count stays a labelled count fact
    N5  a due finding whose outcome is a data hole stayed `pending` forever
        -> closed with a `void` grade carrying the reason; not Right / Wrong / Inconclusive; excluded from n
    N6  "a handful", "few", "many", "several", "a couple", "the bulk", "nearly every" passed the quantifier ban
        -> the list is extended (and "most", "some", "plenty", "a majority", "almost all", "hardly any")
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from pathfinder.llm.contracts import enforce_narrate                                        # noqa: E402
from pathfinder.llm.gateway import OutputContractViolation                                   # noqa: E402
from pathfinder.research import library as LIB                                              # noqa: E402
from pathfinder.research.config import ResearchConfig                                       # noqa: E402
from pathfinder.research.data import MarketData                                             # noqa: E402
from pathfinder.research.grading import build_rule, evaluate                                # noqa: E402
from pathfinder.research.regime import build_regime, regime_on                              # noqa: E402
from pathfinder.research.scan import grade_due, run_scan                                    # noqa: E402
from pathfinder.research.store import ResearchStore, set_research_store                     # noqa: E402
from pathfinder.schemas import (                                                            # noqa: E402
    Decision, FeedResponse, Finding, GradingKind, GradingStatus, Verdict,
)
from tests.test_pathfinder_s1 import AT, PASS1, _tiny, load, scan_dates, synth             # noqa: E402

PRICE_DB = os.environ.get("KANIDA_DB", r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
real = pytest.mark.skipif(not Path(PRICE_DB).exists(), reason="price warehouse not present")


@pytest.fixture(scope="module")
def synthetic():
    return synth()


@pytest.fixture()
def cfg(tmp_path) -> ResearchConfig:
    """The defaults (second-audit conventions), threshold 0 so every draft publishes."""
    return ResearchConfig(research_db=str(tmp_path / "r.db"), price_db="unused",
                          usefulness_threshold=0.0, pairs=(("B0", "B1"), ("A0", "A1")))


@pytest.fixture()
def cfg1(tmp_path) -> ResearchConfig:
    """Pass-1 conventions for the flat-bar grader fixtures (an open equal to its close is a bar there)."""
    return ResearchConfig(research_db=str(tmp_path / "r1.db"), price_db="unused",
                          usefulness_threshold=0.0, pairs=(("B0", "B1"), ("A0", "A1")), **PASS1)


def _facts(draft) -> dict:
    return {f.id[len(draft.facts.prefix) + 1:]: f for f in draft.facts.facts}


def _with_threshold(c: ResearchConfig, thr: float) -> ResearchConfig:
    return ResearchConfig(research_db=c.research_db, price_db="unused", usefulness_threshold=thr, pairs=c.pairs)


# ═══════════════════════════════════════════════════════════════════════════
# N1 — the threshold first: a sub-threshold continuation is never served
# ═══════════════════════════════════════════════════════════════════════════

def _continuation_scores(synthetic, cfg) -> tuple[str, str, float]:
    """Scan two sessions with no threshold; return (root date, next date, the highest continuation score)."""
    dates = scan_dates(synthetic)
    d0, d1 = dates[316], dates[317]
    store = ResearchStore(cfg.research_db)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=d0), computed_at=AT)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=d1), computed_at=AT)
    conts = store.con.execute(
        "SELECT usefulness FROM pf_findings WHERE edition_date = ? AND continues IS NOT NULL", [d1]).fetchall()
    store.close()
    assert conts, "the synthetic universe repeats at least one open claim on the next session"
    return d0, d1, max(r[0] for r in conts)


def test_n1_a_sub_threshold_repeat_of_an_open_claim_is_a_candidate_on_the_record_never_a_card(synthetic, cfg, tmp_path):
    d0, d1, top = _continuation_scores(synthetic, cfg)
    thr = round(top + 0.001, 4)
    c = ResearchConfig(research_db=str(tmp_path / "gated.db"), price_db="unused", usefulness_threshold=0.0, pairs=cfg.pairs)
    store = ResearchStore(c.research_db)
    run_scan(c, store, md=load(synthetic, c, as_of=d0), computed_at=AT)          # the roots publish
    rep = run_scan(_with_threshold(c, thr), store, md=load(synthetic, c, as_of=d1), computed_at=AT)
    served = store.con.execute("SELECT usefulness, continues FROM pf_findings WHERE edition_date = ?", [d1]).fetchall()
    assert all(r[0] >= thr for r in served), "nothing below the threshold is served — continuation or not"
    assert not any(r[1] for r in served) and rep.continued == []
    below = store.con.execute(
        "SELECT decision, reason, published, finding_id FROM pf_candidates WHERE edition_date = ? AND reason LIKE '%continues fnd_%'",
        [d1]).fetchall()
    assert below, "the sub-threshold repeat is on the record as a candidate"
    for dec, reason, published, fid in below:
        assert dec == "continue" and published == 0 and fid is None and reason.startswith("below usefulness threshold")
    assert any("continues fnd_" in line for line in rep.below_threshold)
    feed = store.feed(d1)
    assert feed.continued_count == 0 and all(f.usefulness.total >= feed.usefulness_threshold
                                             for f in feed.what_matters_now + feed.discoveries)


def test_n1_the_contracts_refuse_a_served_card_below_the_threshold_continuation_included(synthetic, cfg):
    dates = scan_dates(synthetic)
    store = ResearchStore(cfg.research_db)
    for i in (316, 317):
        run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[i]), computed_at=AT)
    feed = store.feed(dates[317])
    cont = [f for f in feed.what_matters_now + feed.discoveries if f.continues][0]
    assert cont.decision == Decision.continue_
    # the card contract: a continuation is no longer exempt from the threshold
    payload = cont.model_dump(mode="json")
    payload["usefulness"]["threshold"] = round(cont.usefulness.total + 0.01, 4)
    with pytest.raises(ValueError, match="below the usefulness threshold"):
        Finding.model_validate(payload)
    # the feed contract: the edition's threshold gates every served card
    body = feed.model_dump(mode="json")
    body["usefulness_threshold"] = round(min(f.usefulness.total for f in feed.what_matters_now + feed.discoveries) + 0.01, 4)
    with pytest.raises(ValueError, match="below the usefulness threshold"):
        FeedResponse.model_validate(body)


# ═══════════════════════════════════════════════════════════════════════════
# N2 — the claim within a tolerance; independent n from the grade rows
# ═══════════════════════════════════════════════════════════════════════════

def _chain_row(fid, ed, due, verdict, sig, tmpl="volume_anomaly", rank=1):
    return {"finding_id": fid, "template_id": tmpl, "edition_date": ed, "due_session": due, "verdict": verdict,
            "novelty_key": f"{tmpl}|{fid}|{sig}", "backfilled": 1, "continues": None, "rank": rank}


def test_n2_the_anuras_torntpharm_sobha_chain_is_one_independent_grade():
    # the auditor's chain, at the store's signature precision (lift first: the primary statistic)
    rows = [
        _chain_row("ANURAS", "2026-07-13", "2026-07-20", "wrong", "volume_anomaly|no_trade|-1.20|47.30|48.50"),
        _chain_row("TORNTPHARM", "2026-07-16", "2026-07-23", "wrong", "volume_anomaly|no_trade|-1.10|47.30|48.50"),
        _chain_row("SOBHA", "2026-07-21", "2026-07-28", "right", "volume_anomaly|no_trade|-1.10|47.30|48.40"),
    ]
    from pathfinder.research.store import independent_grades          # absent on f34ecea: the row fails
    same = lambda t, a, b: LIB.same_claim(t, a, b, 1.0)                                     # noqa: E731
    kept = independent_grades(rows, same)
    assert [r["finding_id"] for r in kept] == ["ANURAS"]                 # one null claim, one grade — the first
    # a repeat published once the previous horizon had completed is independent
    rows2 = rows[:1] + [_chain_row("LATER", "2026-07-20", "2026-07-27", "right", "volume_anomaly|no_trade|-1.10|47.30|48.50")]
    assert [r["finding_id"] for r in independent_grades(rows2, same)] == ["ANURAS", "LATER"]
    # a different claim (a real lift, a different decision) is never folded
    rows3 = rows[:1] + [_chain_row("REAL", "2026-07-16", "2026-07-23", "right", "volume_anomaly|new_experiment|6.40|54.80|48.50")]
    assert len(independent_grades(rows3, same)) == 2
    # beyond the tolerance on the primary statistic the claim is new
    rows4 = rows[:1] + [_chain_row("MOVED", "2026-07-16", "2026-07-23", "right", "volume_anomaly|no_trade|-2.40|46.10|48.50")]
    assert len(independent_grades(rows4, same)) == 2


def test_n2_claim_of_and_same_claim_read_the_templates_declaration():
    assert LIB.claim_of("volume_anomaly", "volume_anomaly|no_trade|-1.20|47.30|48.50") == ("volume_anomaly|no_trade", -1.2)
    assert LIB.claim_of("dip", "dip|no_trade|6.00|50.60|0.10|-0.20") == ("dip|no_trade|6.00", 50.6)
    assert LIB.claim_of("market_regime", "market_regime|no_trade|RISK_OFF|47.50|-0.03|-0.56") == ("market_regime|no_trade|RISK_OFF", 47.5)
    assert LIB.claim_of("theme_cycle", "theme_cycle|leader|Realty|watch") == ("theme_cycle|leader|Realty|watch", None)
    assert LIB.claim_of("relationship", "relationship|HDFCBANK|ICICIBANK|long_a_short_b|watch")[1] is None
    assert LIB.same_claim("dip", "dip|no_trade|6.00|50.60|0.10|-0.20", "dip|no_trade|6.00|51.50|0.30|-0.10", 1.0)
    assert not LIB.same_claim("dip", "dip|no_trade|6.00|50.60|0.10|-0.20", "dip|no_trade|6.00|51.70|0.30|-0.10", 1.0)
    assert not LIB.same_claim("dip", "dip|no_trade|6.00|50.60|0.10|-0.20", "dip|virtual_long|6.00|50.60|0.10|-0.20", 1.0)
    assert all(LIB.TEMPLATES[t].claim_parts > 0 for t in ("dip", "surge", "market_regime", "volume_anomaly", "theme_cycle", "relationship"))


def test_n2_the_same_statistic_within_tolerance_on_an_open_claim_is_a_continue(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[316]), computed_at=AT)
    key = store.con.execute("SELECT novelty_key FROM pf_findings WHERE template_id = 'dip'").fetchone()[0]
    tmpl, subj, sig = key.split("|", 2)
    parts = sig.split("|")
    hit = float(parts[3])
    near = "|".join(parts[:3] + [f"{hit + 0.9:.2f}"] + parts[4:])          # the same claim, re-read 0.9 pp away
    far = "|".join(parts[:3] + [f"{hit + 1.5:.2f}"] + parts[4:])           # beyond the tolerance: a new claim
    assert store.open_root(f"dip|T9|{near}", before=dates[317], lookback_editions=5, is_open=lambda e, h: True) is not None
    assert store.open_root(f"dip|T9|{far}", before=dates[317], lookback_editions=5, is_open=lambda e, h: True) is None
    assert store.novelty(f"dip|T9|{near}", before=dates[317], lookback_editions=5) == 0.25
    assert store.novelty(f"dip|T9|{far}", before=dates[317], lookback_editions=5) == 1.0


def test_n2_n_independent_is_computed_from_the_grade_rows_not_assumed_from_publication(synthetic, cfg, monkeypatch):
    # Disable publication-time continuation detection: every repeat becomes a ROOT and is graded —
    # the pre-fix store's failure mode. The scoreboard must still count one grade per claim per
    # non-overlapping horizon.
    monkeypatch.setattr(ResearchStore, "open_root", lambda self, *a, **k: None)
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    for i in (316, 317, 318):
        run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[i]), computed_at=AT)
    assert store.con.execute("SELECT COUNT(*) FROM pf_findings WHERE continues IS NOT NULL").fetchone()[0] == 0
    grade_due(store, load(synthetic, cfg, as_of=dates[329]), cfg, graded_at=AT)
    sb = store.scoreboard(dates[329])
    assert sb.n_total > sb.n == sb.n_independent and sb.regraded == sb.n_total - sb.n > 0
    assert sum(v.n for v in sb.by_template.values()) == sb.n and sb.forward.n + sb.backfilled.n == sb.n
    # the folded rows are the five-session claims repeated inside their own horizon
    rows = store.con.execute(
        "SELECT g.verdict, g.due_session, f.template_id, f.backfilled, f.continues, f.edition_date, f.rank, f.novelty_key "
        "FROM pf_grades g JOIN pf_findings f ON f.finding_id = g.finding_id ORDER BY f.edition_date, f.rank").fetchall()
    from pathfinder.research.store import independent_grades
    kept = independent_grades(rows, store._same_claim)
    assert len(kept) == sb.n and len(rows) == sb.n_total


# ═══════════════════════════════════════════════════════════════════════════
# N3 — the grading kind follows the decision: a pair watch is graded as a watch
# ═══════════════════════════════════════════════════════════════════════════

def test_n3_a_pair_watch_freezes_pair_watch_and_a_losing_spread_is_right(cfg1):
    flat = [(100, 100)] * 3
    d0 = _tiny({"A": flat + [(100, 100)] * 5, "B": flat + [(100, 100)] * 5}, cfg1).sessions[2]
    # A was rich (short A, long B). A rises -> the spread trade LOST; A falls -> it paid.
    lost = _tiny({"A": flat + [(100, 100)] * 4 + [(100, 104)], "B": flat + [(100, 100)] * 5}, cfg1)
    paid = _tiny({"A": flat + [(100, 100)] * 4 + [(100, 96)], "B": flat + [(100, 100)] * 5}, cfg1)
    flat5 = _tiny({"A": flat + [(100, 100)] * 4 + [(100, 100.3)], "B": flat + [(100, 100)] * 5}, cfg1)
    spec = {"a": "A", "b": "B", "direction": "short_a_long_b"}
    watch = build_rule(GradingKind.pair_watch, horizon=5, hurdle_pct=0.30, spec=spec, frozen_at=AT, subject="A/B")
    call = build_rule(GradingKind.pair_convergence, horizon=5, hurdle_pct=0.30, spec=spec, frozen_at=AT, subject="A/B")
    ev = lambda r, md: evaluate(r, finding_slug="t", edition_date=d0, md=md, cfg=cfg1, graded_at=AT).verdict  # noqa: E731
    assert ev(watch, lost) == Verdict.right and ev(watch, paid) == Verdict.wrong and ev(watch, flat5) == Verdict.inconclusive
    assert ev(call, lost) == Verdict.wrong and ev(call, paid) == Verdict.right          # the call reads the other way
    assert "not worth calling" in watch.right and "twice" in watch.wrong
    assert GradingKind.pair_watch in LIB.RELATIONSHIP.grading_kinds


def test_n3_the_relationship_template_picks_the_kind_on_the_decision_and_the_contract_refuses_a_mismatch(synthetic, cfg):
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    pair = LIB.compute_relationship(ctx, LIB.parameters_for(LIB.RELATIONSHIP, cfg))[0]
    expected = GradingKind.pair_watch if pair.decision == Decision.watch else GradingKind.pair_convergence
    assert pair.grading_rule.kind == expected
    # a published card cannot carry a watch under a call kind
    store = ResearchStore(cfg.research_db)
    rep = run_scan(cfg, store, md=md, computed_at=AT)
    card = [f for f in store.findings_for(rep.edition_date) if f.template_id == "relationship"][0]
    payload = card.model_dump(mode="json")
    payload["decision"] = "watch"
    payload["grading_rule"]["kind"] = "pair_convergence"
    with pytest.raises(ValueError, match="must judge the claim the card made"):
        Finding.model_validate(payload)
    payload["grading_rule"]["kind"] = "pair_watch"
    Finding.model_validate(payload)


@real
def test_real_n3_the_hdfcbank_icicibank_watch_of_july_twenty_is_graded_right_by_pair_watch():
    c = ResearchConfig(research_db="unused", usefulness_threshold=0.0)
    md = MarketData.load(c, as_of="2026-07-27")
    if md.as_of != "2026-07-27":
        pytest.skip("warehouse does not contain 2026-07-27")
    m20 = md.sealed("2026-07-20")
    ctx = LIB.ScanContext(md=m20, cfg=c, regime=regime_on(build_regime(m20), m20.as_of), computed_at=AT)
    pair = LIB.compute_relationship(ctx, LIB.parameters_for(LIB.RELATIONSHIP, c))[0]
    assert pair.subject == "HDFCBANK / ICICIBANK" and pair.decision == Decision.watch
    assert pair.grading_rule.kind == GradingKind.pair_watch                # was pair_convergence
    got = evaluate(pair.grading_rule, finding_slug=pair.slug, edition_date="2026-07-20", md=md, cfg=c, graded_at=AT)
    pnl = [f for f in got.facts.facts if f.id.endswith("_spread_trade")][0].value
    assert pnl == pytest.approx(-2.26, abs=0.05)                            # the spread trade lost
    assert got.verdict == Verdict.right                                    # a watch, not a call: Right — was WRONG


# ═══════════════════════════════════════════════════════════════════════════
# N4 — like_this statistics carry the effective n
# ═══════════════════════════════════════════════════════════════════════════

def test_n4_like_this_statistics_are_minted_on_the_effective_n_not_the_overlapping_count(synthetic, cfg):
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    theme = [c for c in LIB.compute_theme_cycle(ctx, LIB.parameters_for(LIB.THEME_CYCLE, cfg)) if c.decision != Decision.reject][0]
    v = _facts(theme)
    n_eff, n_over = v["like_this_independent"].value, v["like_this_cases"].value
    assert n_over > n_eff == theme.n
    for name in ("like_this_beat", "like_this_lagged", "like_this_up", "like_this_typical_excess", "like_this_expectancy"):
        assert v[name].n == n_eff, f"{name} must be weighed on the effective n"
        assert v[name].sample_flag == v["like_this_beat"].sample_flag
    assert "overlapping" in v["like_this_cases"].label and v["like_this_cases"].unit.value == "count"
    from pathfinder.schemas import sample_flag_for
    assert v["like_this_beat"].sample_flag == sample_flag_for(n_eff) != sample_flag_for(n_over) or n_eff >= 50


# ═══════════════════════════════════════════════════════════════════════════
# N5 — an unmeasurable outcome closes as void, never pending forever
# ═══════════════════════════════════════════════════════════════════════════

def test_n5_a_due_finding_whose_entry_is_a_synthetic_open_is_closed_void_with_the_reason(cfg):
    bars = [(100, 101), (101, 102), (102, 101)]
    md = _tiny({"X": bars + [(101, 101)] + [(101, 103), (103, 104)]}, cfg)      # entry bar: open == close
    d0 = md.sessions[2]
    rule = build_rule(GradingKind.directional_call, horizon=1, hurdle_pct=cfg.hurdle_pct,
                      spec={"subject_kind": "stock", "symbol": "X", "direction": "long"}, frozen_at=AT, subject="X")
    got = evaluate(rule, finding_slug="t", edition_date=d0, md=md, cfg=cfg, graded_at=AT)
    assert got is not None and got.verdict == Verdict.void and "synthetic open" in got.void_reason
    assert any(f.id.endswith("_void_reason") for f in got.facts.facts)
    # still inside the horizon: withheld, not void
    early = evaluate(rule, finding_slug="t", edition_date=d0, md=md.sealed(d0), cfg=cfg, graded_at=AT)
    assert early is None
    # the pair and theme kinds close the same way
    pr = build_rule(GradingKind.pair_watch, horizon=1, hurdle_pct=cfg.hurdle_pct,
                    spec={"a": "X", "b": "Y", "direction": "short_a_long_b"}, frozen_at=AT, subject="X/Y")
    md2 = _tiny({"X": bars + [(101, 101)] + [(101, 103), (103, 104)], "Y": bars + [(101, 102)] + [(102, 103), (103, 104)]}, cfg)
    assert evaluate(pr, finding_slug="t", edition_date=d0, md=md2, cfg=cfg, graded_at=AT).verdict == Verdict.void


def test_n5_the_store_closes_it_void_excludes_it_from_n_and_it_is_no_longer_pending(synthetic, cfg):
    raw, idx, vix = synthetic
    raw = raw.copy()
    dates = scan_dates(synthetic)
    D, nxt, later = dates[320], dates[321], dates[327]
    # T0 is the day's hardest fall on D; make its next bar (the ENTRY bar, whatever the horizon) a
    # synthetic open -> the outcome is a hole
    m = (raw.symbol == "T0") & (raw.d == nxt)
    raw.loc[m, "open"] = raw.loc[m, "close"].values
    md_all = MarketData.from_frame(raw, idx, vix, cfg, as_of=later)
    store = ResearchStore(cfg.research_db)
    rep = run_scan(cfg, store, md=md_all.sealed(D), computed_at=AT)
    dip = [f for f in store.findings_for(D) if f.template_id == "dip"][0]
    assert dip.subject == "T0" and dip.grading_rule.horizon_sessions <= 5
    graded = dict(grade_due(store, md_all, cfg, graded_at=AT))
    assert graded[dip.id] == "void"
    f = [x for x in store.findings_for(D) if x.id == dip.id][0]
    assert f.grading.status == GradingStatus.void and f.grading.verdict == Verdict.void
    assert "synthetic open" in f.grading.void_reason
    sb = store.scoreboard(later)
    assert sb.void == 1 and sb.n == sb.n_total == sum(1 for v in graded.values() if v != "void")
    pending_ids = {r["finding_id"] for r in store.pending()}
    assert dip.id not in pending_ids and sb.pending == 0
    feed = store.feed(D)
    assert any(x.grading.status == GradingStatus.void for x in feed.what_matters_now + feed.discoveries)
    assert rep.edition_date == D


# ═══════════════════════════════════════════════════════════════════════════
# D2 — found while rebuilding: pandas' numexpr backend intermittently zeroed a forward column
# ═══════════════════════════════════════════════════════════════════════════

def test_d2_the_research_engine_disables_the_numexpr_and_bottleneck_backends_for_reproducibility():
    """
    On this host numexpr 2.14.1 (12 threads) under pandas 2.3.3 intermittently returned an
    all-zero `f5` column for the 1.25M-row `_c5 / next_open - 1` arithmetic (1 of 4 passes;
    two of three full rebuilds refused by the C1 guard). A number that is sometimes zero is
    not a number this engine may mint: importing the research engine turns the backend off.
    """
    import pandas as pd
    import pathfinder.research.config  # noqa: F401  — the guard lives with the config every module imports
    assert pd.get_option("compute.use_numexpr") is False
    assert pd.get_option("compute.use_bottleneck") is False


# ═══════════════════════════════════════════════════════════════════════════
# N6 — vague quantifiers are quantities; without a fact they are invented
# ═══════════════════════════════════════════════════════════════════════════

_FACTS = [{"id": "fct_x_hit", "label": "share", "value": 51.2, "unit": "pct", "n": 100, "sample_flag": "ok"}]


def _narrate(body: str):
    return enforce_narrate({"beat": "noticed", "headline": "A quiet day", "body": body, "fact_refs": ["fct_x_hit"]},
                           _FACTS, strict=True)


def test_n6_the_probe_sentence_is_rejected():
    with pytest.raises(OutputContractViolation, match="quantity in words"):
        _narrate("The hit rate was {{fact:fct_x_hit}}. A handful of cases bounced, and many did not.")


@pytest.mark.parametrize("phrase", [
    "a handful of cases bounced", "few bounced", "many did not", "several bounced", "a couple bounced",
    "the bulk did not", "nearly every case bounced", "most bounced", "some bounced", "plenty bounced",
    "a majority bounced", "almost all bounced", "hardly any bounced", "the vast majority bounced",
    "a few bounced", "numerous cases bounced", "virtually every case bounced",
])
def test_n6_each_vague_quantifier_needs_a_fact_in_its_sentence(phrase):
    with pytest.raises(OutputContractViolation, match="quantity in words"):
        _narrate(f"The hit rate was {{{{fact:fct_x_hit}}}}. In history {phrase}.")
    _narrate(f"In history {phrase}, at {{{{fact:fct_x_hit}}}}.")          # cited in the same sentence: fine


def test_n6_plain_prose_without_a_quantity_still_passes():
    _narrate("The hit rate was {{fact:fct_x_hit}}. The sector led, and the evidence is thin.")


# ═══════════════════════════════════════════════════════════════════════════
# the feed over HTTP: nothing served below the threshold; the scoreboard shows the folds
# ═══════════════════════════════════════════════════════════════════════════

def test_the_feed_over_http_serves_nothing_below_the_threshold_and_reports_regrades_and_voids(synthetic, cfg, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from pathfinder.mock_app import app
    d0, d1, top = _continuation_scores(synthetic, cfg)
    thr = round(top + 0.001, 4)
    c = ResearchConfig(research_db=str(tmp_path / "http.db"), price_db="unused", usefulness_threshold=0.0, pairs=cfg.pairs)
    store = ResearchStore(c.research_db)
    run_scan(c, store, md=load(synthetic, c, as_of=d0), computed_at=AT)
    run_scan(_with_threshold(c, thr), store, md=load(synthetic, c, as_of=d1), computed_at=AT)
    grade_due(store, load(synthetic, c, as_of=scan_dates(synthetic)[329]), c, graded_at=AT)
    store.close()
    monkeypatch.setenv("KANIDA_PATHFINDER_RESEARCH_DB", c.research_db)
    set_research_store(None)
    try:
        client = TestClient(app)
        body = client.get(f"/api/pathfinder/feed?date={d1}").json()
        items = body["what_matters_now"] + body["discoveries"]
        assert items and all(i["usefulness"]["total"] >= body["usefulness_threshold"] for i in items)
        assert body["continued_count"] == 0 and not any(i["continues"] for i in items)
        sb = body["scoreboard"]
        assert {"regraded", "void", "n_independent", "n_total"} <= set(sb)
        assert sb["regraded"] == sb["n_total"] - sb["n"] and sb["n_independent"] == sb["n"]
    finally:
        set_research_store(None)
