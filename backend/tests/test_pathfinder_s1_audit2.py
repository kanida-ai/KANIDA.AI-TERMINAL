"""
S1 SECOND quant-audit regression suite — one row per finding (docs/handbacks/PF-S1.md §4, "Second audit").

Every row here FAILED on the engine as committed after the first fix pass (69c1f95) and encodes
the second auditor's recomputed expectation. Rows marked `real` run on the price warehouse
(close 2026-07-29) under the SECOND audit's conventions — hurdle = 0.30% costs + 0.10% slippage
each way = 0.50%, corporate-action days excluded, synthetic opens excluded — and skip loudly
when the warehouse is absent. (The pass-1 suites pin the pass-1 conventions explicitly.)

    A1  the whole scoreboard was a backfill presented as a track record
        -> `backfilled` on every edition / finding / grade; scoreboard split forward vs backfilled (forward = 0 today)
    A2  the usefulness threshold did not gate; the same claim republished daily and re-graded
        -> novelty keyed on the EVIDENCE SIGNATURE; a repeat of an open claim is a `continue`, graded once; independent n
    A3  the theme call's traded claim had no conditional base rate; persistence 70% was an overlap artefact
        -> graded-metric base rate on the card's own criteria gates the call; effective n; mechanical h=15 persistence
    A4  "corporate-action back-adjusted" overclaimed; demergers / split prints in the dip mask
        -> honest label; split/bonus/demerger/rights ex-dates and |move| > 30% excluded; disclosed on provenance
    A5  no slippage anywhere; the theme call had no hurdle
        -> hurdle = costs + 2 x slippage on every decision and every frozen rule; the strong test needs expectancy > 0
    A6  zero sentinels rendered as values ("0.0% of 0 times", rank 0)
        -> a statistic over n = 0 cannot be minted; facts / cards withheld instead
    A7  "typical next-session move 0.0%" sat on the pile of open == close placeholder bars
        -> synthetic opens: no outcome is measured from an entry at them (dip: hit 50.6%, median +0.10%)
    A8  survivorship undisclosed; --fresh deleted the append-only store; anomaly grader ignored the rule horizon;
        theme grader / template market definitions
        -> disclosure sentence on provenance; archive-not-delete with an explicit flag; r{h}cc per horizon; consistency pin
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scripts"))

from pathfinder.research import library as LIB                                              # noqa: E402
from pathfinder.research.config import IST, ResearchConfig                                  # noqa: E402
from pathfinder.research.data import MarketData                                             # noqa: E402
from pathfinder.research.facts import FactSet                                               # noqa: E402
from pathfinder.research.grading import build_rule, evaluate, theme_universe                # noqa: E402
from pathfinder.research.ranking import novelty_key                                         # noqa: E402
from pathfinder.research.regime import build_regime, regime_on                              # noqa: E402
from pathfinder.research.scan import grade_due, is_backfilled, run_scan                     # noqa: E402
from pathfinder.research.store import ResearchStore, StoreSchemaError, set_research_store   # noqa: E402
from pathfinder.schemas import (                                                            # noqa: E402
    BACKFILL_LABEL, FORWARD_LABEL, Decision, Fact, GradingKind, GradingStatus,
)
from tests.test_pathfinder_s1 import AT, PASS1, _tiny, load, scan_dates, synth             # noqa: E402

PRICE_DB = os.environ.get("KANIDA_DB", r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
real = pytest.mark.skipif(not Path(PRICE_DB).exists(), reason="price warehouse not present")


@pytest.fixture(scope="module")
def synthetic():
    return synth()


@pytest.fixture()
def cfg(tmp_path) -> ResearchConfig:
    """SECOND-audit conventions: slippage 0.10 each way, corp-action and synthetic-open exclusion ON."""
    return ResearchConfig(research_db=str(tmp_path / "r.db"), price_db="unused",
                          usefulness_threshold=0.0, pairs=(("B0", "B1"), ("A0", "A1")))


def _facts(draft) -> dict[str, Fact]:
    return {f.id[len(draft.facts.prefix) + 1:]: f for f in draft.facts.facts}


def _on_its_own_date(edition: str) -> datetime:
    return datetime.fromisoformat(edition + "T18:00:00").replace(tzinfo=IST)


# ═══════════════════════════════════════════════════════════════════════════
# A1 — backfill is labelled as backfill; the scoreboard splits forward / backfilled
# ═══════════════════════════════════════════════════════════════════════════

def test_a1_an_edition_generated_after_its_date_is_a_backfill_on_the_edition_every_card_and_the_scoreboard(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    for D in (dates[318], dates[320], dates[326]):          # AT is 2026-09-10: every one of these is after the fact
        rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=D), computed_at=AT)
        assert rep.backfilled and store.edition(D)["backfilled"] == 1
        assert all(r[0] == 1 for r in store.con.execute("SELECT backfilled FROM pf_findings WHERE edition_date = ?", [D]))
    feed = store.feed(dates[326])
    assert feed.backfilled is True and feed.record_label == BACKFILL_LABEL
    for f in feed.what_matters_now + feed.discoveries:
        assert f.backfilled and f.grading.backfilled and f.grading.record == BACKFILL_LABEL
    sb = feed.scoreboard
    assert sb.n > 0 and sb.forward.n == 0 and sb.backfilled.n == sb.n           # forward is ZERO, visibly
    assert sb.record_label == BACKFILL_LABEL and sb.n_independent == sb.n
    snap = store.con.execute("SELECT forward_json, backfilled_json FROM pf_scoreboard ORDER BY snapshot_id DESC").fetchone()
    assert json.loads(snap[0])["n"] == 0 and json.loads(snap[1])["n"] == sb.n


def test_a1_a_scan_run_on_the_sessions_own_date_is_a_forward_record(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    D, later = dates[318], dates[326]
    assert not is_backfilled(D, _on_its_own_date(D)) and is_backfilled(D, _on_its_own_date(later))
    rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=D), computed_at=_on_its_own_date(D))
    assert rep.backfilled is False and store.edition(D)["backfilled"] == 0
    feed = store.feed(D)
    assert feed.backfilled is False and feed.record_label == FORWARD_LABEL
    assert all(not f.backfilled and f.grading.record == FORWARD_LABEL for f in feed.what_matters_now + feed.discoveries)
    grade_due(store, load(synthetic, cfg, as_of=later), cfg, graded_at=_on_its_own_date(later))
    sb = store.scoreboard(later)
    assert sb.forward.n == sb.n > 0 and sb.backfilled.n == 0 and sb.record_label == FORWARD_LABEL
    # the day after its session, the same edition would have been a backfill — even by one day
    assert is_backfilled(D, _on_its_own_date(dates[319]))


def test_a1_a_store_written_under_the_superseded_schema_is_refused_not_relabelled(tmp_path):
    p = tmp_path / "old.db"
    con = sqlite3.connect(str(p))
    con.executescript(
        "CREATE TABLE pf_editions (edition_date TEXT PRIMARY KEY, data_as_of TEXT, generated_at TEXT, engine_version TEXT, "
        "llm_provider TEXT, regime TEXT, universe_scanned INTEGER, candidates INTEGER, threshold REAL, params_json TEXT);")
    con.commit()
    con.close()
    with pytest.raises(StoreSchemaError, match="superseded schema"):
        ResearchStore(str(p))


# ═══════════════════════════════════════════════════════════════════════════
# A2 — novelty on the evidence signature; a repeated open claim is a continue; independent n
# ═══════════════════════════════════════════════════════════════════════════

def test_a2_novelty_is_keyed_on_the_evidence_signature_not_the_days_subject(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[320]), computed_at=AT)
    row = store.con.execute("SELECT novelty_key FROM pf_findings WHERE template_id = 'dip'").fetchone()
    assert row is not None
    tmpl, subj, sig = row[0].split("|", 2)
    assert subj == "T0" and sig.startswith("dip|no_trade|") or sig.startswith("dip|virtual_long|")
    # tomorrow a DIFFERENT stock is the day's hardest fall, but the group statistic is the same claim
    assert store.novelty(f"dip|T4|{sig}", before=dates[321], lookback_editions=5) == 0.25
    # the same subject on different evidence is news-ish (0.6); a new claim is new (1.0)
    assert store.novelty("dip|T0|dip|virtual_long|6.00|99.00|1.00|1.00", before=dates[321], lookback_editions=5) == 0.6
    assert store.novelty("dip|T9|dip|virtual_long|6.00|99.00|1.00|1.00", before=dates[321], lookback_editions=5) == 1.0


def test_a2_the_signature_of_a_group_card_is_the_statistic_not_the_stock(synthetic, cfg):
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    dip = LIB.compute_dip(ctx, LIB.parameters_for(LIB.DIP, cfg))[0]
    v = _facts(dip)
    assert "T0" not in dip.evidence_signature
    assert f"{v['hit_1'].value:.1f}"[:4] in dip.evidence_signature and f"{v['typical_1'].value:.2f}" in dip.evidence_signature
    theme = [c for c in LIB.compute_theme_cycle(ctx, LIB.parameters_for(LIB.THEME_CYCLE, cfg)) if c.decision != Decision.reject][0]
    assert theme.evidence_signature == f"theme_cycle|leader|{theme.subject}|{theme.decision.value}"
    assert novelty_key(dip, "NEUTRAL").endswith(dip.evidence_signature)


def test_a2_a_repeat_of_an_open_claim_is_a_continue_not_a_publication_and_is_graded_once(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    reps = [run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[i]), computed_at=AT) for i in (316, 317, 318)]
    conts = store.con.execute("SELECT finding_id, continues, edition_date, decision FROM pf_findings WHERE continues IS NOT NULL").fetchall()
    assert conts, "the synthetic universe repeats at least one open claim on consecutive sessions"
    roots = {r[1] for r in conts}
    for fid, root, ed, dec in conts:
        assert dec == "continue"
        rr = store.con.execute("SELECT edition_date, horizon_sessions, continues FROM pf_findings WHERE finding_id = ?", [root]).fetchone()
        assert rr["continues"] is None and rr["edition_date"] < ed          # a chain always points at the ROOT
        assert dates.index(ed) < dates.index(rr["edition_date"]) + rr["horizon_sessions"]   # inside the open horizon
    # continuations are served, labelled, and NOT counted as publications
    for rep in reps[1:]:
        feed = store.feed(rep.edition_date)
        assert feed.continued_count == len(rep.continued)
        assert feed.published_count == len(rep.published)
        for f in feed.what_matters_now + feed.discoveries:
            if f.continues:
                assert f.decision == Decision.continue_ and f.grading.status == GradingStatus.continued
                assert f.grading.continues == f.continues and f.tier.value == "discovery"
                assert f.narrative.headline.startswith("Continuing")
    cand = store.con.execute("SELECT reason, published FROM pf_candidates WHERE decision = 'continue'").fetchall()
    assert cand and all("continuation" in r[0] and r[1] == 0 for r in cand)
    # grade everything: the roots are graded, the continuations never are, n is independent
    grade_due(store, load(synthetic, cfg, as_of=dates[329]), cfg, graded_at=AT)
    graded = {r[0] for r in store.con.execute("SELECT finding_id FROM pf_grades")}
    assert roots <= graded and not any(fid in graded for fid, *_ in conts)
    sb = store.scoreboard(dates[329])
    assert sb.n == sb.n_independent == sb.n_total == len(graded) and sb.continued == len(conts)
    assert sb.pending == 0


def test_a2_once_the_root_horizon_completes_the_same_claim_is_a_new_independent_publication(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[316]), computed_at=AT)
    key = store.con.execute("SELECT novelty_key FROM pf_findings WHERE horizon_sessions = 5 LIMIT 1").fetchone()[0]
    still_open = store.open_root(key, before=dates[318], lookback_editions=5, is_open=lambda ed, h: True)
    assert still_open is not None
    closed = store.open_root(key, before=dates[322], lookback_editions=5, is_open=lambda ed, h: False)
    assert closed is None                                    # -> a new publication, novelty-discounted, graded on its own


# ═══════════════════════════════════════════════════════════════════════════
# A3 — the theme call's conditional base rate on the graded metric, effective n, mechanical persistence
# ═══════════════════════════════════════════════════════════════════════════

def test_a3_effective_n_folds_the_same_sector_inside_one_horizon_window():
    assert LIB._effective_n(np.array([0, 1, 2, 3, 4, 5, 10]), np.array([0, 0, 0, 0, 0, 0, 0]), 5) == 3
    assert LIB._effective_n(np.array([0, 1, 2]), np.array([0, 1, 2]), 5) == 3          # different sectors: independent
    assert LIB._effective_n(np.array([], dtype=int), np.array([], dtype=int), 5) == 0


def test_a3_non_overlapping_persistence_is_a_stride_and_never_a_zero_sentinel():
    idx = [f"d{i}" for i in range(40)]
    rel = pd.DataFrame({"S0": np.r_[np.ones(20), np.zeros(20)], "S1": np.r_[np.zeros(20), np.ones(20)],
                        "S2": np.zeros(40) - 1, "S3": np.zeros(40) - 2}, index=idx)
    rank = rel.rank(axis=1, ascending=False)
    p_over, n_over = LIB._persistence(rel, rank, 15)
    p_mech, n_mech = LIB._persistence(rel, rank, 15, stride=15)
    assert n_mech < n_over and n_mech == 2
    empty = pd.DataFrame({"S0": [np.nan] * 5, "S1": [np.nan] * 5}, index=idx[:5])
    assert LIB._persistence(empty, empty.rank(axis=1), 3) == (None, 0)


def test_a3_a_leader_whose_conditional_base_rate_does_not_clear_the_hurdle_is_a_watch_not_a_call():
    # the graded metric's base rate: median +0.45, mean +0.45 -> at the pass-1 hurdle (0.30, no slippage) a call,
    # at the second-audit hurdle (0.30 + 2 x 0.10 = 0.50) expectancy is negative -> not a call
    assert LIB.call_on_excess(n=200, up_pct=60.0, med=0.45, etv=0.45 - 0.30, H=0.30, min_hit=58.0)
    assert not LIB.call_on_excess(n=200, up_pct=60.0, med=0.45, etv=0.45 - 0.50, H=0.50, min_hit=58.0)
    assert not LIB.call_on_excess(n=10, up_pct=70.0, med=1.0, etv=0.5, H=0.50, min_hit=58.0)      # effective n too small


@real
def test_real_a3_it_leader_is_judged_on_the_graded_metrics_conditional_base_rate_with_effective_n(real_cards):
    d, _ = real_cards[("theme_cycle", "Information Technology")]
    v = _facts(d)
    # the metric the card is GRADED on (sector minus market, next open -> t+5 close) on leaders meeting its criteria
    assert v["like_this_beat"].value == pytest.approx(42.3, abs=0.1) and v["like_this_lagged"].value == pytest.approx(38.3, abs=0.1)
    assert v["like_this_typical_excess"].value == pytest.approx(0.07, abs=0.01)
    assert v["like_this_expectancy"].value == pytest.approx(-0.37, abs=0.01) and v["like_this_expectancy"].value < 0
    assert v["like_this_cases"].value == 2649 and v["like_this_independent"].value == 879 == d.n
    assert v["like_this_beat"].n == 879 and "overlapping" in v["like_this_cases"].label     # third audit N4: effective n
    # persistence: the overlapping 70% is context and says so; the mechanical h = 15 number is ~25%
    assert v["persistence"].value == pytest.approx(70.0, abs=0.1) and "OVERLAPPING" in v["persistence"].label
    assert v["persistence_mechanical"].value == pytest.approx(25.5, abs=0.5) and v["persistence_mechanical"].n == 220
    assert "NON-overlapping" in v["persistence_mechanical"].label
    assert "persistence_mechanical" in d.body and "{{fact:" + v["persistence"].id + "}}" not in d.body
    assert d.decision == Decision.watch and d.grading_rule.hurdle_pct == pytest.approx(0.50)
    assert any(k.endswith("_like_this_expectancy") for k in d.key_facts)


# ═══════════════════════════════════════════════════════════════════════════
# A4 — honest data label; corporate-action days excluded and disclosed
# ═══════════════════════════════════════════════════════════════════════════

def test_a4_the_data_source_no_longer_claims_back_adjustment(cfg):
    assert "back-adjusted" not in cfg.data_source and "UNADJUSTED" in cfg.data_source
    assert any("demergers" in s and "not adjusted" in s for s in cfg.data_disclosures)
    assert any("Corporate-action days are excluded" in s for s in cfg.data_disclosures)


def test_a4_a_split_ex_date_and_a_forty_percent_print_are_excluded_from_the_dip_mask_and_from_subject_selection(synthetic, cfg):
    raw, idx, vix = synthetic
    raw = raw.copy()
    dates = scan_dates(synthetic)
    D = dates[320]
    # T3: a -40% print with no table entry (a demerger before the table starts) -> suspected corporate action
    prev = float(raw.loc[(raw.symbol == "T3") & (raw.d == dates[319]), "close"].iloc[0])
    raw.loc[(raw.symbol == "T3") & (raw.d == D), "close"] = prev * 0.60
    # T4: a split on the NSE table, printed unadjusted at -8%
    prev4 = float(raw.loc[(raw.symbol == "T4") & (raw.d == dates[319]), "close"].iloc[0])
    raw.loc[(raw.symbol == "T4") & (raw.d == D), "close"] = prev4 * 0.92
    ca = pd.DataFrame({"symbol": ["T4", "T1"], "ex_date": [D, dates[100]], "action_type": ["split", "dividend"]})
    md = MarketData.from_frame(raw, idx, vix, cfg, as_of=D, corp_actions=ca)
    t3 = md.df[(md.df.symbol == "T3") & (md.df.d == D)].iloc[0]
    t4 = md.df[(md.df.symbol == "T4") & (md.df.d == D)].iloc[0]
    t1 = md.df[(md.df.symbol == "T1") & (md.df.d == dates[100])].iloc[0]
    assert pd.isna(t3.ret) and t3._ca_suspect == 1 and pd.isna(t4.ret) and t4._ca == 1
    assert t1._ca == 0 and not pd.isna(t1.ret)                          # a dividend is not a price break
    # the +67% rebound the day after (on the unsealed history, past the seal) is flagged too
    assert md.exclusions.corp_action_bars == 1 and md.exclusions.suspected_corp_action_bars == 2
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    dip = LIB.compute_dip(ctx, LIB.parameters_for(LIB.DIP, cfg))[0]
    assert dip.subject == "T0", "the -40% corporate-action print must not be the day's hardest fall"
    # with the rule off, the bogus -40% IS the subject — the difference is the exclusion
    off = ResearchConfig(research_db="unused", price_db="unused", usefulness_threshold=0.0, exclude_corp_actions=False)
    md_off = MarketData.from_frame(raw, idx, vix, off, as_of=D, corp_actions=ca)
    ctx_off = LIB.ScanContext(md=md_off, cfg=off, regime=regime_on(build_regime(md_off), md_off.as_of), computed_at=AT)
    assert LIB.compute_dip(ctx_off, LIB.parameters_for(LIB.DIP, off))[0].subject == "T3"


def test_a4_a8_every_finding_carries_the_exclusion_and_survivorship_disclosures(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=scan_dates(synthetic)[320]), computed_at=AT)
    for f in store.findings_for(rep.edition_date):
        d = " ".join(f.provenance.disclosures)
        assert "Survivorship" in d and "delisted" in d and "Corporate-action days are excluded" in d and "synthetic open" in d
        assert "back-adjusted" not in f.provenance.data_source
    params = json.loads(store.edition(rep.edition_date)["params_json"])
    assert set(params["data_exclusions"]) >= {"corp_action_bars", "suspected_corp_action_bars", "synthetic_open_bars"}
    assert params["slippage_pct"] == 0.10 and params["hurdle_pct"] == pytest.approx(0.50)


@real
def test_real_a4_the_named_demergers_and_split_prints_are_holes_and_the_dip_mask_holds_no_thirty_percent_case(real_cards):
    _, md = real_cards["_ctx"]
    df = md.df
    for sym, d, how in [("CGPOWER", "2016-03-15", "_ca_suspect"), ("TATACHEM", "2020-03-04", "_ca"),
                        ("ABFRL", "2025-05-22", "_ca"), ("ADANIENT", "2015-06-03", "_ca_suspect"),
                        ("JBCHEPHARM", "2023-09-18", "_ca"), ("SPLPETRO", "2022-06-07", "_ca_suspect")]:
        r = df[(df.symbol == sym) & (df.d == d)].iloc[0]
        assert pd.isna(r.ret) and r[how] == 1 and r._bad == 1, f"{sym} {d} must be excluded ({how})"
    assert int((df.ret <= -0.30).sum()) == 0                          # the auditor's 23 cases are gone
    ex = md.exclusions.as_dict()
    assert ex["corp_action_bars"] == 179 and ex["suspected_corp_action_bars"] == 45 and ex["glitch_bars"] == 6
    dip, _ = real_cards[("dip", "J&KBANK")]
    assert dip.n == 12368                                             # 12,632 under the pass-1 rules


# ═══════════════════════════════════════════════════════════════════════════
# A5 — slippage in the hurdle; the theme call gated on expected excess above it
# ═══════════════════════════════════════════════════════════════════════════

def test_a5_the_hurdle_is_costs_plus_slippage_both_ways_on_every_decision_and_every_frozen_rule(synthetic, cfg):
    assert cfg.slippage_pct == 0.10 and cfg.hurdle_pct == pytest.approx(0.50)
    assert "slippage" in cfg.cost_convention and "0.10%" in cfg.cost_convention
    store = ResearchStore(cfg.research_db)
    rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=scan_dates(synthetic)[320]), computed_at=AT)
    for f in store.findings_for(rep.edition_date):
        assert f.grading_rule.hurdle_pct == pytest.approx(0.50) and f.provenance.cost_hurdle_pct == pytest.approx(0.50)
        hurdle = [x for x in f.facts if x.id.endswith("_hurdle")][0]
        assert hurdle.value == pytest.approx(50.0) and "slippage" in hurdle.label
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    assert ctx.hurdle == pytest.approx(0.50)
    n, wr, med, etv = LIB._rate(md.df, md.df["ret"] <= -0.06, 1, cfg)
    assert etv == pytest.approx(LIB._wmean(md.df.loc[(md.df["ret"] <= -0.06) & md.df["f1"].notna(), "f1"], 1.0) * 100 - 0.50)


def test_a5_a_founder_can_set_slippage_by_environment(monkeypatch):
    monkeypatch.setenv("KANIDA_PF_SLIPPAGE_PCT", "0.25")
    c = ResearchConfig(research_db="unused", price_db="unused")
    assert c.slippage_pct == 0.25 and c.hurdle_pct == pytest.approx(0.80)


@real
def test_real_a5_the_theme_and_rotation_decisions_are_judged_against_the_slippage_inclusive_hurdle(real_cards):
    it, _ = real_cards[("theme_cycle", "Information Technology")]
    rot, _ = real_cards[("theme_cycle", "Telecommunication")]
    v, w = _facts(it), _facts(rot)
    assert v["hurdle"].value == 50.0 and w["hurdle"].value == 50.0
    assert w["flips"].value == 776 and w["beat_after_costs"].value == pytest.approx(34.4, abs=0.1)
    assert w["expectancy"].value == pytest.approx(-0.72, abs=0.01) and rot.decision == Decision.reject
    assert it.decision == Decision.watch and v["like_this_expectancy"].value < 0


# ═══════════════════════════════════════════════════════════════════════════
# A6 — no zero sentinels
# ═══════════════════════════════════════════════════════════════════════════

def test_a6_a_statistic_over_zero_cases_cannot_be_minted():
    c = ResearchConfig(research_db="unused", price_db="unused")
    fs = FactSet(finding_slug="t", cfg=c, as_of="2026-07-29", period_start="2013-01-01", period_end="2026-07-29",
                 component="t", computed_at=AT)
    with pytest.raises(ValueError, match="n=0"):
        fs.add("a", "share of those where the spread narrowed", 0.0, "pct", n=0)     # "0.0% of 0 times"
    fs.add("b", "share higher", 50.0, "pct", n=1)


def test_a6_the_grader_withholds_the_rank_fact_when_the_sector_cannot_be_ranked(tmp_path):
    c = ResearchConfig(research_db="unused", price_db="unused", **PASS1)
    secs = {"A": "Tech", "B": "Tech", "C": "Bank", "D": "Bank"}
    flat = [(100, 100)] * 20
    md = _tiny({"A": flat + [(100, 105)] * 5, "B": flat + [(100, 105)] * 5,
                "C": flat + [(100, 100)] * 5, "D": flat + [(100, 100)] * 5}, c, sectors=secs)
    rule = build_rule(GradingKind.theme_watch, horizon=5, hurdle_pct=0.3, frozen_at=AT, subject="Tech",
                      spec={"sector": "Ghost", "members": ["A", "B"], "window": 3, "min_names": 2})
    got = evaluate(rule, finding_slug="t", edition_date=md.sessions[19], md=md, cfg=c, graded_at=AT)
    assert got is not None and not any(f.id.endswith("rank_at_horizon") for f in got.facts.facts)


def test_a6_the_theme_card_is_withheld_when_no_leader_like_this_has_resolved(synthetic, cfg, monkeypatch):
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    monkeypatch.setattr(LIB, "_leaders_like_this", lambda *a, **k: (np.array([]), 0))
    cards = LIB.compute_theme_cycle(ctx, LIB.parameters_for(LIB.THEME_CYCLE, cfg))
    assert not any(c.grading_rule.kind in (GradingKind.theme_call, GradingKind.theme_watch) for c in cards)
    monkeypatch.setattr(LIB, "_persistence", lambda *a, **k: (None, 0))
    monkeypatch.undo()
    monkeypatch.setattr(LIB, "_persistence", lambda *a, **k: (None, 0))
    cards = LIB.compute_theme_cycle(ctx, LIB.parameters_for(LIB.THEME_CYCLE, cfg))
    theme = [c for c in cards if c.grading_rule.kind == GradingKind.theme_watch or c.grading_rule.kind == GradingKind.theme_call][0]
    ids = {f.id for f in theme.facts.facts}
    assert not any(i.endswith("_persistence") or i.endswith("_persistence_mechanical") for i in ids)


def test_a6_the_pair_width_statistic_is_withheld_not_zero_when_nothing_resolved(synthetic, cfg, monkeypatch):
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    # every extreme session resolves inside the seal except when the horizon is pushed past the frame
    p = dict(LIB.parameters_for(LIB.RELATIONSHIP, cfg))
    pair = LIB.compute_relationship(ctx, p)[0]
    v = _facts(pair)
    assert "snapped_back" in v and v["snapped_back"].n > 0
    # a frame with exactly one extreme run (today's) has no resolved width -> the fact is withheld
    raw, idx, vix = synthetic
    md2 = load((raw[raw.d >= scan_dates(synthetic)[150]], idx, vix), cfg, as_of=scan_dates(synthetic)[320])
    monkeypatch.setattr(LIB, "_episodes", LIB._episodes)
    ctx2 = LIB.ScanContext(md=md2, cfg=cfg, regime=regime_on(build_regime(md2), md2.as_of), computed_at=AT)
    cards = LIB.compute_relationship(ctx2, p)
    for c in cards:
        vv = _facts(c)
        if "snapped_back" in vv:
            assert vv["snapped_back"].n > 0
        assert not any(f.n == 0 for f in c.facts.facts)


# ═══════════════════════════════════════════════════════════════════════════
# A7 — synthetic opens carry no outcome
# ═══════════════════════════════════════════════════════════════════════════

def test_a7_an_open_equal_to_its_close_is_a_synthetic_open_and_no_outcome_enters_at_it(synthetic, cfg):
    raw, idx, vix = synthetic
    raw = raw.copy()
    dates = scan_dates(synthetic)
    i = 200
    raw.loc[(raw.symbol == "A2") & (raw.d == dates[i + 1]), "open"] = \
        raw.loc[(raw.symbol == "A2") & (raw.d == dates[i + 1]), "close"].values
    md = MarketData.from_frame(raw, idx, vix, cfg)
    s = md.symbol("A2").set_index("d")
    assert s.loc[dates[i + 1], "_synth_open"] == 1 and pd.isna(s.loc[dates[i + 1], "oc"])
    assert all(pd.isna(s.loc[dates[i], f"f{h}"]) for h in (1, 3, 5)), "an entry at a synthetic open is not an outcome"
    assert not pd.isna(s.loc[dates[i], "r5cc"])                        # close-to-close statistics are untouched
    assert not pd.isna(s.loc[dates[i + 1], "f1"]) and md.exclusions.synthetic_open_bars == 1
    off = ResearchConfig(research_db="unused", price_db="unused", exclude_synthetic_opens=False)
    s_off = MarketData.from_frame(raw, idx, vix, off).symbol("A2").set_index("d")
    assert s_off.loc[dates[i], "f1"] == pytest.approx(0.0)              # the artefact: an exactly-zero outcome


@real
def test_real_a7_the_dip_cards_typical_move_is_no_longer_the_zero_pile(real_cards):
    _, md = real_cards["_ctx"]
    assert md.exclusions.synthetic_open_bars == 19083                  # 1.5% of the warehouse's bars
    dip, _ = real_cards[("dip", "J&KBANK")]
    v = _facts(dip)
    # the auditor's recompute excluding exact-zero outcomes: hit 50.6%, median +0.10%
    assert v["hit_1"].value == pytest.approx(50.6, abs=0.05) and v["typical_1"].value == pytest.approx(0.10, abs=0.005)
    assert v["expectancy_1"].value < 0 and dip.decision == Decision.no_trade
    assert any("synthetic open" in s for s in dip.facts.cfg.data_disclosures)


# ═══════════════════════════════════════════════════════════════════════════
# A8 — governance and consistency
# ═══════════════════════════════════════════════════════════════════════════

def test_a8_the_store_is_archived_never_deleted_and_only_with_the_explicit_flag(tmp_path):
    import run_pathfinder_scan as S
    p = tmp_path / "pathfinder_research.db"
    p.write_bytes(b"x")
    with pytest.raises(SystemExit, match="refusing to archive"):
        S.archive_store(str(p), acknowledged=False)
    assert p.exists()
    moved = S.archive_store(str(p), acknowledged=True, now=datetime(2026, 9, 10, 12, 0, 0))
    assert moved is not None and moved.exists() and moved.name == "pathfinder_research.db.archived-20260910T120000"
    assert not p.exists() and moved.read_bytes() == b"x"
    assert S.archive_store(str(p), acknowledged=True) is None          # nothing to archive is fine
    assert "--fresh" not in S.__doc__.split("--archive-store")[0] or "never deleted" in S.__doc__


def test_a8_the_anomaly_grader_reads_the_rules_own_horizon():
    c = ResearchConfig(research_db="unused", price_db="unused", **PASS1)
    flat = [(100, 100)] * 3
    # +4% by t+3, back to flat by t+5: a 3-session rule is RIGHT, a 5-session rule is WRONG
    md = _tiny({"X": flat + [(100, 100)] * 2 + [(100, 104)] + [(104, 100)] + [(100, 100)]}, c)
    d0 = md.sessions[2]
    r3 = build_rule(GradingKind.anomaly_move, horizon=3, hurdle_pct=0.3, spec={"symbol": "X", "move_pct": 3.0}, frozen_at=AT, subject="X")
    r5 = build_rule(GradingKind.anomaly_move, horizon=5, hurdle_pct=0.3, spec={"symbol": "X", "move_pct": 3.0}, frozen_at=AT, subject="X")
    assert evaluate(r3, finding_slug="t", edition_date=d0, md=md, cfg=c).verdict.value == "right"
    assert evaluate(r5, finding_slug="t", edition_date=d0, md=md, cfg=c).verdict.value == "wrong"
    assert "3 sessions later" in [f for f in evaluate(r3, finding_slug="t", edition_date=d0, md=md, cfg=c).facts.facts][0].label


def test_a8_the_theme_grader_and_the_template_measure_the_same_market(synthetic, cfg):
    dates = scan_dates(synthetic)
    md = load(synthetic, cfg, as_of=dates[326])
    D = dates[320]
    p = LIB.parameters_for(LIB.THEME_CYCLE, cfg)
    d2, sret, mkt, roll, rel, rank, exn = LIB._sector_tables(md, p["window"], p["min_names"], p["horizon"])
    template_mkt = float(d2[d2.d == D]["f5"].mean())
    members = sorted(d2[(d2.d == D) & (d2.sector == "Tech")].symbol.unique().tolist())
    rule = build_rule(GradingKind.theme_watch, horizon=5, hurdle_pct=cfg.hurdle_pct, frozen_at=AT, subject="Tech",
                      spec={"sector": "Tech", "members": members, "window": p["window"], "min_names": p["min_names"]})
    got = evaluate(rule, finding_slug="t", edition_date=D, md=md, cfg=cfg, graded_at=AT)
    mm = [f for f in got.facts.facts if f.id.endswith("_market_move")][0]
    assert mm.value == pytest.approx(template_mkt * 100, abs=1e-4)          # facts are minted at 4 dp
    assert theme_universe(md, D, p["min_names"]).equals(d2[d2.columns]) or set(theme_universe(md, D, p["min_names"]).sector) == set(d2.sector)
    # the template's market_return fact counts the names of that same market, not the whole universe
    ctx = LIB.ScanContext(md=md.sealed(D), cfg=cfg, regime=regime_on(build_regime(md.sealed(D)), D), computed_at=AT)
    theme = [c for c in LIB.compute_theme_cycle(ctx, p) if c.grading_rule.kind != GradingKind.rotation_reject][0]
    v = _facts(theme)
    assert v["market_return"].n == int(d2[d2.d == D].symbol.nunique()) and "at least the minimum number" in v["market_return"].label


# ═══════════════════════════════════════════════════════════════════════════
# The feed over HTTP carries every label
# ═══════════════════════════════════════════════════════════════════════════

def test_the_feed_over_http_shows_the_backfill_label_the_split_scoreboard_and_continuations(synthetic, cfg, monkeypatch):
    from fastapi.testclient import TestClient
    from pathfinder.mock_app import app
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    for i in (316, 317, 318, 326):
        run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[i]), computed_at=AT)
    store.close()
    monkeypatch.setenv("KANIDA_PATHFINDER_RESEARCH_DB", cfg.research_db)
    set_research_store(None)
    try:
        client = TestClient(app)
        body = client.get(f"/api/pathfinder/feed?date={dates[317]}").json()
        assert body["backfilled"] is True and body["record_label"] == BACKFILL_LABEL
        assert body["continued_count"] >= 1 and body["published_count"] + body["continued_count"] == \
            len(body["what_matters_now"] + body["discoveries"])
        for i in body["what_matters_now"] + body["discoveries"]:
            assert i["backfilled"] is True and i["grading"]["record"] == BACKFILL_LABEL
            assert i["grading"]["status"] in ("pending", "graded", "continued")
            if i["continues"]:
                assert i["decision"] == "continue" and i["grading"]["status"] == "continued"
            assert "Survivorship" in " ".join(i["provenance"]["disclosures"])
            assert "back-adjusted" not in i["provenance"]["data_source"]
            assert not any(f["n"] == 0 for f in i["facts"])
        sb = client.get("/api/pathfinder/feed").json()["scoreboard"]
        assert sb["forward"]["n"] == 0 and sb["backfilled"]["n"] == sb["n"] == sb["n_independent"] > 0
        assert sb["record_label"] == BACKFILL_LABEL and sb["continued"] >= 1
    finally:
        set_research_store(None)


# ═══════════════════════════════════════════════════════════════════════════
# real-warehouse fixture (second-audit conventions)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def real_cards():
    if not Path(PRICE_DB).exists():
        pytest.skip("price warehouse not present")
    c = ResearchConfig(research_db="unused", usefulness_threshold=0.0)
    md = MarketData.load(c, as_of="2026-07-29")
    if md.as_of != "2026-07-29":
        pytest.skip("warehouse does not contain 2026-07-29")
    ctx = LIB.ScanContext(md=md, cfg=c, regime=regime_on(build_regime(md), md.as_of), computed_at=AT)
    cards: dict = {"_ctx": (c, md)}
    for t in LIB.LIBRARY:
        for d in t.computation(ctx, LIB.parameters_for(t, c)):
            cards[(d.template_id, d.subject)] = (d, t)
    return cards
