"""
S1 guardrail suite — the research engine behind the clarity-first feed.

Rows are listed in docs/TEST_PLAN.md as S1-01 … S1-34. Most run on a SYNTHETIC universe
where every bar is under the test's control; the last block runs against the real price
warehouse (port fidelity vs the prototypes) and skips loudly when it is absent.
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

from pathfinder.llm.gateway import ClassifyResult, GatewayError, Job, NarrateResult, Usage  # noqa: E402
from pathfinder.llm.contracts import enforce_classify, enforce_narrate                       # noqa: E402
from pathfinder.research import library as LIB                                              # noqa: E402
from pathfinder.research.config import ResearchConfig                                       # noqa: E402
from pathfinder.research.data import LookAheadError, MarketData                             # noqa: E402
from pathfinder.research.grading import build_rule, evaluate                                # noqa: E402
from pathfinder.research.narrate import Narrator                                            # noqa: E402
from pathfinder.research.ranking import evidence_strength, novelty_key, score               # noqa: E402
from pathfinder.research.scan import grade_due, run_scan                                    # noqa: E402
from pathfinder.research.store import ResearchStore, set_research_store                     # noqa: E402
from pathfinder.schemas import (                                                            # noqa: E402
    REF_TOKEN_RE, Finding, GradingKind, Narrative, Verdict,
)

AT = datetime(2026, 9, 10, 18, 0, 0)


# ── a synthetic universe: fifteen names, three sectors, every bar controlled ────

SECTORS = {**{f"T{i}": "Tech" for i in range(5)}, **{f"B{i}": "Bank" for i in range(5)},
           **{f"A{i}": "Auto" for i in range(5)}}


def synth(n_days: int = 330, seed: int = 7, *, engineer_last: int = 320,
          calm_day: int = 250) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Fat-tailed random walks (Student-t, so 6%+ days and 3x-volume days exist in history)
    with engineered events on session `engineer_last` and a deliberately calm `calm_day`.
    """
    rng = np.random.default_rng(seed)
    dates = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2025-01-01", periods=n_days)]
    rows = []
    for k, (sym, sec) in enumerate(SECTORS.items()):
        rets = np.clip(rng.standard_t(3, n_days) * 0.012 + 0.0003, -0.25, 0.25)
        rets[calm_day] = rng.normal(0, 0.003)
        close = 100.0 * np.cumprod(1 + rets)
        opens = np.r_[close[0], close[:-1]] * (1 + rng.normal(0, 0.003, n_days))
        vol = rng.lognormal(np.log(1_000_000), 0.7, n_days)
        vol[calm_day] = 900_000.0
        e = engineer_last
        if sym == "T0":                       # the hard dip
            close[e] = close[e - 1] * 0.90
        if sym == "T1":                       # the big surge
            close[e] = close[e - 1] * 1.10
        if sym == "T2":                       # the volume anomaly, flat close
            vol[e] = vol[e - 21:e - 1].mean() * 6.0
            close[e] = close[e - 1] * 1.002
        if sym == "B0":                       # the pair break: B0 runs away from B1
            close[e - 8:e + 1] *= np.linspace(1.0, 1.25, 9)
        for i, d in enumerate(dates):
            rows.append((sym, d, float(opens[i]), float(close[i]), float(vol[i]), sec, int(k < 6)))
    df = pd.DataFrame(rows, columns=["symbol", "d", "open", "close", "volume", "sector", "in_nifty50"])
    idx = df.groupby("d")["close"].mean()
    vix = pd.Series(15.0 + rng.normal(0, 1, n_days), index=dates)
    return df, idx, vix


@pytest.fixture(scope="module")
def synthetic():
    return synth()


#: PASS-1 CONVENTIONS. This suite pins the S1 engine math as first audited: hurdle = 0.30%
#: costs with no slippage term, and the grader fixtures below build "flat" bars as
#: (open, close) = (100, 100). The second audit added a slippage term (A5) and treats an
#: open equal to its close as a synthetic open (A7); those conventions are pinned in
#: `test_pathfinder_s1_audit2.py`. Setting them explicitly here keeps these rows pinned to
#: the numbers they were written against, and says so.
PASS1 = dict(slippage_pct=0.0, exclude_synthetic_opens=False)
#: ...and before the corporate-action exclusion (A4): the real-warehouse pins below and in the
#: pass-1 audit suite reproduce the prototypes' and the first auditor's exact counts under it.
PASS1_DATA = dict(PASS1, exclude_corp_actions=False)


@pytest.fixture()
def cfg(tmp_path) -> ResearchConfig:
    return ResearchConfig(research_db=str(tmp_path / "r.db"), price_db="unused",
                          usefulness_threshold=0.0, pairs=(("B0", "B1"), ("A0", "A1")), **PASS1)


def scan_dates(synthetic):
    raw, _, _ = synthetic
    return sorted(raw["d"].unique())


def load(synthetic, cfg, as_of=None) -> MarketData:
    raw, idx, vix = synthetic
    return MarketData.from_frame(raw, idx, vix, cfg, as_of=as_of)


# ═══════════════════════════════════════════════════════════════════════════
# Point-in-time (S1-01 … S1-05)
# ═══════════════════════════════════════════════════════════════════════════

def test_s1_01_forward_outcomes_are_nan_past_the_seal(synthetic, cfg):
    md = load(synthetic, cfg)
    sym = md.symbol("T3").sort_values("d")
    for h in (1, 3, 5):
        assert sym[f"f{h}"].iloc[-h:].isna().all(), f"f{h} leaks past the seal"
        assert sym[f"f{h}"].iloc[-h - 1:-h].notna().all(), f"f{h}: off by one — last resolvable row is NaN"
    assert sym["r5cc"].iloc[-5:].isna().all()


def test_s1_02_entry_is_the_next_open_not_the_signal_close(synthetic, cfg):
    md = load(synthetic, cfg)
    s = md.symbol("T3").sort_values("d").reset_index(drop=True)
    i = 100
    assert s.loc[i, "f1"] == pytest.approx(s.loc[i + 1, "close"] / s.loc[i + 1, "open"] - 1)
    assert s.loc[i, "f5"] == pytest.approx(s.loc[i + 5, "close"] / s.loc[i + 1, "open"] - 1)
    # the close-to-close number the prototype used is NOT what f{h} carries
    assert s.loc[i, "f1"] != pytest.approx(s.loc[i + 1, "close"] / s.loc[i, "close"] - 1)


def test_s1_03_seal_invariance_truncated_history_equals_sealed_frame(synthetic, cfg):
    raw, idx, vix = synthetic
    D = scan_dates(synthetic)[300]
    full = MarketData.from_frame(raw, idx, vix, cfg).sealed(D)
    trunc = MarketData.from_frame(raw[raw["d"] <= D], idx[idx.index <= D], vix[vix.index <= D], cfg)
    assert full.as_of == trunc.as_of == D
    for col in ("f1", "f3", "f5", "r5cc", "ret", "vol20", "ma20"):
        a = full.df.set_index(["symbol", "d"])[col].sort_index()
        b = trunc.df.set_index(["symbol", "d"])[col].sort_index()
        pd.testing.assert_series_equal(a, b, check_names=False)


def test_s1_04_a_sealed_frame_cannot_be_widened(synthetic, cfg):
    D = scan_dates(synthetic)[300]
    md = load(synthetic, cfg, as_of=D)
    md._full = None
    with pytest.raises(LookAheadError):
        md.sealed(scan_dates(synthetic)[310])


def test_s1_05_zero_prices_are_holes_not_prices(synthetic, cfg):
    raw, idx, vix = synthetic
    raw = raw.copy()
    raw.loc[(raw["symbol"] == "A4") & (raw["d"] == scan_dates(synthetic)[50]), ["open", "close"]] = 0.0
    md = MarketData.from_frame(raw, idx, vix, cfg)
    s = md.symbol("A4").set_index("d")
    d50, d49 = scan_dates(synthetic)[50], scan_dates(synthetic)[49]
    assert np.isnan(s.loc[d50, "close"]) and np.isnan(s.loc[d50, "ret"]) and np.isnan(s.loc[d49, "f1"])
    assert np.isfinite(md.df["ret"].dropna()).all()


# ═══════════════════════════════════════════════════════════════════════════
# The question library is the only source of computations (S1-06 … S1-09)
# ═══════════════════════════════════════════════════════════════════════════

def test_s1_06_the_library_holds_the_six_seeded_templates():
    assert [t.id for t in LIB.LIBRARY] == [
        "market_regime", "theme_cycle", "dip", "surge", "volume_anomaly", "relationship"]
    for t in LIB.LIBRARY:
        assert callable(t.computation) and t.grading_kinds and t.question and t.source


def test_s1_07_the_scan_runs_only_library_templates(synthetic, cfg, monkeypatch):
    store = ResearchStore(cfg.research_db)
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    monkeypatch.setattr(LIB, "LIBRARY", (LIB.DIP,))
    rep = run_scan(cfg, store, md=md, computed_at=AT)
    items = store.findings_for(rep.edition_date)
    assert items and {f.template_id for f in items} == {"dip"}


def test_s1_08_every_seeded_template_fires_on_the_engineered_close(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    rep = run_scan(cfg, store, md=md, computed_at=AT)
    items = store.findings_for(rep.edition_date)
    assert {f.template_id for f in items} == {t.id for t in LIB.LIBRARY}
    subjects = {f.template_id: f.subject for f in items if f.template_id != "theme_cycle"}
    assert subjects["dip"] == "T0" and subjects["surge"] == "T1"
    assert subjects["volume_anomaly"] == "T2" and subjects["relationship"] == "B0 / B1"


def test_s1_09_a_close_with_nothing_unusual_publishes_fewer_cards(synthetic, cfg):
    """No dip, no surge, no anomaly, no pair break -> those templates return nothing. No padding."""
    store = ResearchStore(cfg.research_db)
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[250])
    rep = run_scan(cfg, store, md=md, computed_at=AT)
    ids = {f.template_id for f in store.findings_for(rep.edition_date)}
    assert "dip" not in ids and "surge" not in ids and "volume_anomaly" not in ids
    assert rep.candidates < len(LIB.LIBRARY) + 1


# ═══════════════════════════════════════════════════════════════════════════
# Provenance + honesty on every card (S1-10 … S1-15)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def edition(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    rep = run_scan(cfg, store, md=md, computed_at=AT)
    return store, rep, store.findings_for(rep.edition_date), md


def test_s1_10_every_card_carries_full_provenance(edition):
    _, _, items, md = edition
    assert items
    for f in items:
        p = f.provenance
        assert p.level and p.n >= 0 and p.period.end <= p.as_of and p.regime and p.comparison_group
        assert p.cost_hurdle_pct > 0 and "hurdle" in p.cost_convention and p.universe and p.data_source
        assert str(p.as_of) == md.as_of


def test_s1_11_every_fact_carries_provenance_with_a_level_and_no_look_ahead(edition):
    _, _, items, md = edition
    n = 0
    for f in items:
        for x in f.facts:
            n += 1
            assert x.provenance.level is not None
            assert x.provenance.date_range.end <= x.provenance.as_of
            assert str(x.provenance.as_of) <= md.as_of
            assert not any(m in x.provenance.computed_by.lower() for m in ("claude", "gpt", "llm", "sonnet"))
    assert n > 40


def test_s1_12_narratives_are_digit_free_and_every_ref_resolves(edition):
    _, _, items, _ = edition
    for f in items:
        assert not any(ch.isdigit() for ch in f.narrative.headline)
        assert not any(ch.isdigit() for ch in REF_TOKEN_RE.sub("", f.narrative.body))
        assert not any(ch.isdigit() for ch in f.decision_reason)
        known = {x.id for x in f.facts}
        assert set(f.narrative.fact_refs) <= known and set(f.key_fact_refs) <= known
        assert f.narrative.fact_refs, "a narrative that cites no fact is a story, not evidence"


def test_s1_13_small_samples_are_labelled_never_hidden(edition):
    _, _, items, _ = edition
    flags = {x.sample_flag.value for f in items for x in f.facts if x.n is not None}
    assert "greyed" in flags or "flagged" in flags
    for f in items:
        for x in f.facts:
            if x.n is not None and x.n < 20:
                assert x.sample_flag.value == "greyed"


def test_s1_14_the_cost_hurdle_is_on_every_card_and_decides(edition):
    _, _, items, _ = edition
    for f in items:
        assert f.grading_rule.hurdle_pct == pytest.approx(f.provenance.cost_hurdle_pct)
        hurdle_facts = [x for x in f.facts if x.id.endswith("_hurdle")]
        assert hurdle_facts and hurdle_facts[0].value == pytest.approx(f.provenance.cost_hurdle_pct * 100)
    # The decision must be the one the facts + hurdle imply — the prototype's rules plus the
    # expectancy gate the audit added (P5): median AND mean-net-of-hurdle must clear.
    H = items[0].provenance.cost_hurdle_pct
    for f in items:
        v = {x.id.rsplit("_", 1)[-1] if not x.id.endswith(("_1", "_5")) else x.id.split("_")[-2] + "_" + x.id.split("_")[-1]: x.value
             for x in f.facts}
        if f.template_id == "market_regime":
            assert (f.decision.value == "virtual_long") == (v["typical"] > H and v["expectancy"] > 0)
        elif f.template_id == "dip":
            week = "typical_5" in v and v["typical_5"] > 2 * H and v["hit_5"] >= 58 and v["expectancy_5"] > 0
            day = v["typical_1"] > H and v["hit_1"] >= 58 and v["expectancy_1"] > 0
            assert (f.decision.value == "virtual_long") == (week or day)
        elif f.template_id == "surge":
            fade = v["hit_1"] <= 45 and v["typical_1"] < -H and v["expectancy_1"] < 0
            go = v["hit_1"] >= 58 and v["typical_1"] > H and v["expectancy_1"] > 0
            assert (f.decision.value == "reject") == fade and (f.decision.value == "virtual_long") == go


def test_s1_15_a_narrative_with_a_bare_numeral_is_unrepresentable():
    with pytest.raises(ValueError, match="literal numeral"):
        Narrative(headline="x", body="it fell 7 percent", produced_by="engine", at=AT, fact_refs=[])
    with pytest.raises(ValueError, match="digit"):
        Narrative(headline="Down 7", body="x", produced_by="engine", at=AT, fact_refs=[])
    Narrative(headline="Down", body="it fell {{fact:fct_a}}", produced_by="engine", at=AT, fact_refs=["fct_a"])


# ═══════════════════════════════════════════════════════════════════════════
# Grading: frozen rules, verdicts, horizon (S1-16 … S1-24)
# ═══════════════════════════════════════════════════════════════════════════

def _tiny(paths: dict[str, list[tuple[float, float]]], cfg, sectors=None, n50=None) -> MarketData:
    """`paths[sym] = [(open, close), ...]` consecutive sessions."""
    n = len(next(iter(paths.values())))
    dates = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2026-01-01", periods=n)]
    rows = []
    for sym, bars in paths.items():
        for i, (o, c) in enumerate(bars):
            rows.append((sym, dates[i], o, c, 1e6, (sectors or {}).get(sym, "S"), int(sym in (n50 or paths))))
    raw = pd.DataFrame(rows, columns=["symbol", "d", "open", "close", "volume", "sector", "in_nifty50"])
    idx = raw.groupby("d")["close"].mean()
    return MarketData.from_frame(raw, idx, pd.Series(15.0, index=dates), cfg)


def _rule(kind, spec, h=1, hurdle=0.30):
    return build_rule(kind, horizon=h, hurdle_pct=hurdle, spec=spec, frozen_at=AT, subject="X")


def test_s1_16_directional_and_no_trade_verdicts_use_the_hurdle_band(cfg):
    flat = [(100, 100)] * 3
    up = _tiny({"X": flat + [(100, 102)]}, cfg)          # +2% from the next open
    inside = _tiny({"X": flat + [(100, 100.2)]}, cfg)    # +0.2%: inside the 0.30 hurdle
    down = _tiny({"X": flat + [(100, 97)]}, cfg)         # -3%
    d0 = up.sessions[2]
    long_ = _rule(GradingKind.directional_call, {"subject_kind": "stock", "symbol": "X", "direction": "long"})
    short = _rule(GradingKind.directional_call, {"subject_kind": "stock", "symbol": "X", "direction": "short"})
    no = _rule(GradingKind.no_trade_call, {"subject_kind": "stock", "symbol": "X"})
    ev = lambda r, md: evaluate(r, finding_slug="t", edition_date=d0, md=md, cfg=cfg, graded_at=AT).verdict  # noqa: E731
    assert ev(long_, up) == Verdict.right and ev(long_, down) == Verdict.wrong and ev(long_, inside) == Verdict.inconclusive
    assert ev(short, up) == Verdict.wrong and ev(short, down) == Verdict.right
    assert ev(no, down) == Verdict.right and ev(no, up) == Verdict.wrong and ev(no, inside) == Verdict.inconclusive


def test_s1_17_a_grade_is_withheld_until_the_horizon_completes(cfg):
    md = _tiny({"X": [(100, 100)] * 6}, cfg)
    d0 = md.sessions[2]
    r5 = _rule(GradingKind.directional_call, {"subject_kind": "stock", "symbol": "X", "direction": "long"}, h=5)
    assert evaluate(r5, finding_slug="t", edition_date=d0, md=md.sealed(md.sessions[5]), cfg=cfg) is None
    md7 = _tiny({"X": [(100, 100)] * 8}, cfg)
    assert evaluate(r5, finding_slug="t", edition_date=d0, md=md7, cfg=cfg) is not None


def test_s1_18_theme_call_and_theme_watch_verdicts(cfg):
    secs = {"A": "Tech", "B": "Tech", "C": "Bank", "D": "Bank", "E": "Auto", "F": "Auto"}
    flat = [(100, 100)] * 20
    win = _tiny({"A": flat + [(100, 105)] * 5, "B": flat + [(100, 105)] * 5,
                 "C": flat + [(100, 100)] * 5, "D": flat + [(100, 100)] * 5,
                 "E": flat + [(100, 100)] * 5, "F": flat + [(100, 100)] * 5}, cfg, sectors=secs)
    lose = _tiny({"A": flat + [(100, 95)] * 5, "B": flat + [(100, 95)] * 5,
                  "C": flat + [(100, 100)] * 5, "D": flat + [(100, 100)] * 5,
                  "E": flat + [(100, 100)] * 5, "F": flat + [(100, 100)] * 5}, cfg, sectors=secs)
    d0 = win.sessions[19]
    spec = {"sector": "Tech", "members": ["A", "B"], "window": 3, "min_names": 2}
    call = _rule(GradingKind.theme_call, spec, h=5)
    watch = _rule(GradingKind.theme_watch, spec, h=5)
    ev = lambda r, md: evaluate(r, finding_slug="t", edition_date=d0, md=md, cfg=cfg, graded_at=AT)  # noqa: E731
    assert ev(call, win).verdict == Verdict.right and ev(call, lose).verdict == Verdict.wrong
    # a "not yet a cycle" call is WRONG when it clearly was one, RIGHT when it was not
    assert ev(watch, win).verdict == Verdict.wrong and ev(watch, lose).verdict == Verdict.right
    got = ev(call, win)
    assert any(f.id.endswith("_excess") for f in got.facts.facts)


def test_s1_19_anomaly_and_pair_verdicts(cfg):
    flat = [(100, 100)] * 3
    moved = _tiny({"X": flat + [(100, 100)] * 4 + [(100, 104)]}, cfg)      # +4% by a week later
    still = _tiny({"X": flat + [(100, 100)] * 5}, cfg)
    d0 = moved.sessions[2]
    an = _rule(GradingKind.anomaly_move, {"symbol": "X", "move_pct": 3.0}, h=5)
    assert evaluate(an, finding_slug="t", edition_date=d0, md=moved, cfg=cfg).verdict == Verdict.right
    assert evaluate(an, finding_slug="t", edition_date=d0, md=still, cfg=cfg).verdict == Verdict.wrong
    # pair: A was rich (short A, long B); A falls, B flat -> converged
    conv = _tiny({"A": flat + [(100, 100)] * 4 + [(100, 96)], "B": flat + [(100, 100)] * 5}, cfg)
    wide = _tiny({"A": flat + [(100, 100)] * 4 + [(100, 104)], "B": flat + [(100, 100)] * 5}, cfg)
    pr = _rule(GradingKind.pair_convergence, {"a": "A", "b": "B", "direction": "short_a_long_b"}, h=5)
    assert evaluate(pr, finding_slug="t", edition_date=d0, md=conv, cfg=cfg).verdict == Verdict.right
    assert evaluate(pr, finding_slug="t", edition_date=d0, md=wide, cfg=cfg).verdict == Verdict.wrong


def test_s1_20_the_grading_rule_is_frozen_at_publication_not_read_from_config(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    D = scan_dates(synthetic)[320]
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=D), computed_at=AT)
    items = store.findings_for(D)
    assert all(f.grading_rule.frozen_at == AT and f.grading.status.value == "pending" for f in items)
    # The founder changes the hurdle AFTER publication. The grade must use the frozen one.
    later = ResearchConfig(research_db=cfg.research_db, price_db="unused", cost_hurdle_pct=5.0,
                           usefulness_threshold=0.0, pairs=cfg.pairs, **PASS1)
    md_later = load(synthetic, later, as_of=scan_dates(synthetic)[329])
    graded = grade_due(store, md_later, later, graded_at=AT)
    assert graded
    for f in store.findings_for(D):
        assert f.grading.status.value == "graded"
        hurdle = [x for x in f.grading.realized_facts if x.id.endswith("_hurdle")][0]
        assert hurdle.value == pytest.approx(0.30 * 100)          # bps of the FROZEN 0.30, not 5.0


def test_s1_21_findings_are_graded_only_when_their_horizon_completes(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    D = dates[320]
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=D), computed_at=AT)
    graded_2 = grade_due(store, load(synthetic, cfg, as_of=dates[322]), cfg, graded_at=AT)
    items = {f.id: f for f in store.findings_for(D)}
    for fid, _ in graded_2:
        assert items[fid].grading_rule.horizon_sessions <= 2
    assert any(f.grading.status.value == "pending" for f in items.values() if f.grading_rule.horizon_sessions == 5)
    grade_due(store, load(synthetic, cfg, as_of=dates[325]), cfg, graded_at=AT)
    assert all(f.grading.status.value == "graded" for f in store.findings_for(D))
    for f in store.findings_for(D):
        assert f.grading.due_session is not None and str(f.grading.due_session) == dates[320 + f.grading_rule.horizon_sessions]
        assert str(f.grading.data_as_of) >= str(f.grading.due_session)


def test_s1_22_the_scoreboard_counts_sum_to_n_and_snapshots_are_appended(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[320]), computed_at=AT)
    grade_due(store, load(synthetic, cfg, as_of=dates[326]), cfg, graded_at=AT)
    sb = store.scoreboard(dates[326])
    assert sb.n == sb.right + sb.wrong + sb.inconclusive > 0 and sb.pending == 0
    assert sum(v.n for v in sb.by_template.values()) == sb.n
    snaps = store.con.execute("SELECT COUNT(*) FROM pf_scoreboard").fetchone()[0]
    assert snaps >= 1


def test_s1_23_published_rows_and_grades_are_append_only(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[320]), computed_at=AT)
    grade_due(store, load(synthetic, cfg, as_of=dates[326]), cfg, graded_at=AT)
    for sql in ("UPDATE pf_findings SET decision = 'virtual_long'",
                "DELETE FROM pf_findings",
                "UPDATE pf_grades SET verdict = 'right'",
                "DELETE FROM pf_grades",
                "UPDATE pf_editions SET regime = 'x'",
                "DELETE FROM pf_scoreboard"):
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store.con.execute(sql)
    with pytest.raises(sqlite3.IntegrityError):                # one grade per finding, ever
        fid = store.con.execute("SELECT finding_id FROM pf_grades LIMIT 1").fetchone()[0]
        store.con.execute("INSERT INTO pf_grades VALUES (?, 'x', 'x', 'x', 'right', 'v', '[]')", [fid])


def test_s1_24_an_edition_is_never_recomputed(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    md = load(synthetic, cfg, as_of=scan_dates(synthetic)[320])
    a = run_scan(cfg, store, md=md, computed_at=AT)
    b = run_scan(cfg, store, md=md, computed_at=AT)
    assert not a.skipped and b.skipped
    assert store.con.execute("SELECT COUNT(*) FROM pf_editions").fetchone()[0] == 1


# ═══════════════════════════════════════════════════════════════════════════
# Ranking, threshold, novelty, the gateway (S1-25 … S1-30)
# ═══════════════════════════════════════════════════════════════════════════

def test_s1_25_the_threshold_gates_publication_and_zero_is_a_valid_edition(synthetic, tmp_path):
    strict = ResearchConfig(research_db=str(tmp_path / "s.db"), price_db="unused",
                            usefulness_threshold=0.999, pairs=(("B0", "B1"),))
    store = ResearchStore(strict.research_db)
    rep = run_scan(strict, store, md=load(synthetic, strict, as_of=scan_dates(synthetic)[320]), computed_at=AT)
    assert rep.candidates > 0 and rep.published == [] and rep.below_threshold
    feed = store.feed(rep.edition_date)
    assert feed is not None and feed.published_count == 0 and feed.candidates_considered == rep.candidates
    # the rejected candidates are on the record, with their scores
    assert store.con.execute("SELECT COUNT(*) FROM pf_candidates WHERE published = 0").fetchone()[0] == rep.candidates


def test_s1_26_clarity_first_ordering_and_tiers(edition):
    _, _, items, _ = edition
    ranks = [f.rank for f in items]
    assert ranks == list(range(1, len(items) + 1))
    totals = [f.usefulness.total for f in items]
    assert totals == sorted(totals, reverse=True)
    assert [f.tier.value for f in items[:3]] == ["what_matters_now"] * min(3, len(items))
    assert all(f.tier.value == "discovery" for f in items[3:])


def test_s1_27_novelty_decays_for_a_repeated_finding(synthetic, cfg):
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[318]), computed_at=AT)
    first = {f.template_id: f for f in store.findings_for(dates[318])}
    run_scan(cfg, store, md=load(synthetic, cfg, as_of=dates[319]), computed_at=AT)
    second = {f.template_id: f for f in store.findings_for(dates[319])}
    assert first["market_regime"].usefulness.novelty == 1.0
    key = f"market_regime|{second['market_regime'].subject}|{second['market_regime'].decision.value}"
    assert store.novelty(key + "|NEUTRAL", before=dates[319], lookback_editions=5) in (0.25, 0.6, 1.0)
    assert second["market_regime"].usefulness.novelty <= first["market_regime"].usefulness.novelty


def test_s1_28_evidence_strength_rewards_sample_and_clarity():
    assert evidence_strength(0, 5.0) == 0.0
    assert evidence_strength(400, 0.0) == pytest.approx(0.5)
    assert evidence_strength(400, 6.0) == pytest.approx(1.0)
    assert evidence_strength(25, 6.0) < evidence_strength(100, 6.0)


class _FakeLLM:
    """A provider that returns whatever the test says — through the SAME contracts."""
    provider_name = "fake"

    def __init__(self, narrate_payload=None, label="publish"):
        self._n = narrate_payload
        self._label = label
        self.asked: list[str] = []

    def classify(self, *, text, labels, facts=(), constitution_version, prompt_version, budget,
                 batch=True, key=""):
        self.asked.append(key)
        p = enforce_classify({"label": self._label, "rationale": "because"}, labels)
        return ClassifyResult(job=Job.classify, model="claude-haiku-4-5",
                              usage=Usage("claude-haiku-4-5", 1, 1), prompt_version=prompt_version,
                              constitution_version=constitution_version, raw_json=p,
                              label=p["label"], rationale=p["rationale"])

    def narrate(self, *, beat, facts, context=(), constitution_version, prompt_version, budget,
                batch=True, key=""):
        payload = dict(self._n)
        if "{REF}" in payload["body"]:                     # cite the card's first fact (the feed requires a ref)
            payload["body"] = payload["body"].replace("{REF}", facts[0]["id"])
            payload["fact_refs"] = [facts[0]["id"]]
        p = enforce_narrate(payload, facts)
        return NarrateResult(job=Job.narrate, model="claude-haiku-4-5",
                             usage=Usage("claude-haiku-4-5", 1, 1), prompt_version=prompt_version,
                             constitution_version=constitution_version, raw_json=p,
                             beat=p["beat"], headline=p["headline"], body=p["body"],
                             fact_refs=tuple(p["fact_refs"]))

    def reason(self, **kw):
        raise GatewayError("not used")


def test_s1_29_a_model_that_writes_a_number_is_rejected_and_the_engine_narrates(synthetic, cfg):
    bad = {"beat": "noticed", "headline": "Big move", "body": "It fell 12 percent.", "fact_refs": []}
    store = ResearchStore(cfg.research_db)
    nar = Narrator(_FakeLLM(narrate_payload=bad), provider_name="fake")
    rep = run_scan(cfg, store, md=load(synthetic, cfg, as_of=scan_dates(synthetic)[320]),
                   narrator=nar, computed_at=AT)
    items = store.findings_for(rep.edition_date)
    assert items and all(f.narrative.produced_by.value == "engine" for f in items)
    assert nar.failures and "literal numerals" in nar.failures[0]


def test_s1_30_the_model_may_only_veto_a_card_that_cleared_the_threshold(synthetic, tmp_path):
    # 0.70: since the second audit the theme card is weighed on its EFFECTIVE n (A3), which
    # caps its evidence strength below the old 1.0 — the veto semantics are what is tested.
    cfg2 = ResearchConfig(research_db=str(tmp_path / "v.db"), price_db="unused",
                          usefulness_threshold=0.70, pairs=(("B0", "B1"),), **PASS1)
    store = ResearchStore(cfg2.research_db)
    fake = _FakeLLM(label="hold")
    nar = Narrator(fake, provider_name="fake")
    rep = run_scan(cfg2, store, md=load(synthetic, cfg2, as_of=scan_dates(synthetic)[320]),
                   narrator=nar, computed_at=AT)
    assert rep.published == [] and rep.held
    # it was never even asked about the cards below the threshold
    assert len(fake.asked) == len(rep.held) and len(rep.below_threshold) + len(rep.held) == rep.candidates
    rows = store.con.execute("SELECT reason FROM pf_candidates WHERE published = 0").fetchall()
    assert any("held by llm" in r[0] for r in rows) and any("below usefulness" in r[0] for r in rows)
    # a good model narrates, and the line is stamped as its own
    good = {"beat": "noticed", "headline": "The market is quiet", "body": "Nothing carries beyond {{fact:{REF}}}.",
            "fact_refs": []}
    cfg3 = ResearchConfig(research_db=str(tmp_path / "g.db"), price_db="unused",
                          usefulness_threshold=0.0, pairs=(("B0", "B1"),))
    store3 = ResearchStore(cfg3.research_db)
    rep3 = run_scan(cfg3, store3, md=load(synthetic, cfg3, as_of=scan_dates(synthetic)[320]),
                    narrator=Narrator(_FakeLLM(narrate_payload=good), provider_name="fake"), computed_at=AT)
    assert all(f.narrative.produced_by.value == "llm" and f.narrative.model for f in store3.findings_for(rep3.edition_date))


# ═══════════════════════════════════════════════════════════════════════════
# The feed over HTTP (S1-31 … S1-33)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def feed_client(synthetic, cfg, monkeypatch):
    from fastapi.testclient import TestClient
    from pathfinder.mock_app import app
    store = ResearchStore(cfg.research_db)
    dates = scan_dates(synthetic)
    for d in (dates[318], dates[320], dates[326]):
        run_scan(cfg, store, md=load(synthetic, cfg, as_of=d), computed_at=AT)
    store.close()
    monkeypatch.setenv("KANIDA_PATHFINDER_RESEARCH_DB", cfg.research_db)
    set_research_store(None)
    try:
        yield TestClient(app), dates
    finally:
        set_research_store(None)


def test_s1_31_the_feed_serves_ranked_clarity_first_findings_with_grades(feed_client):
    client, dates = feed_client
    r = client.get("/api/pathfinder/feed")
    assert r.status_code == 200, r.text[:300]
    body = r.json()
    assert body["edition_date"] == dates[326] and body["published_count"] > 0
    items = body["what_matters_now"] + body["discoveries"]
    assert [i["rank"] for i in items] == list(range(1, len(items) + 1))
    assert items[0]["tier"] == "what_matters_now"
    for i in items:
        assert i["narrative"]["headline"] and not any(c.isdigit() for c in i["narrative"]["headline"])
        assert not any(c.isdigit() for c in REF_TOKEN_RE.sub("", i["narrative"]["body"]))
        assert i["provenance"]["level"] in ("same_stock", "peer_group", "sector", "whole_market")
        assert i["grading_rule"]["frozen_at"] and i["grading_rule"]["kind"]
        assert i["grading"]["status"] in ("pending", "graded")
        known = {f["id"] for f in i["facts"]}
        assert set(i["narrative"]["fact_refs"]) <= known
        assert "not a recommendation" in i["disclosure"].lower()
    sb = body["scoreboard"]
    assert sb["n"] == sb["right"] + sb["wrong"] + sb["inconclusive"] and sb["n"] > 0
    # the earlier edition's cards are graded by now, and the feed says so
    old = client.get(f"/api/pathfinder/feed?date={dates[318]}").json()
    assert all(i["grading"]["status"] == "graded" and i["grading"]["verdict"] for i in old["what_matters_now"] + old["discoveries"])
    assert Finding.model_validate(items[0])


def test_s1_32_feed_errors_are_guarded(feed_client):
    client, _ = feed_client
    assert client.get("/api/pathfinder/feed?date=DROP").status_code == 400
    r = client.get("/api/pathfinder/feed?date=1999-01-01")
    assert r.status_code == 404 and r.json()["error"]["code"] == "no_edition"
    text = r.text.lower()
    assert not any(t in text for t in ("traceback", "select ", "sqlite", "\\"))


def test_s1_33_no_research_store_means_no_edition_never_fixtures(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from pathfinder.mock_app import app
    monkeypatch.setenv("KANIDA_PATHFINDER_RESEARCH_DB", str(tmp_path / "missing.db"))
    set_research_store(None)
    try:
        r = TestClient(app).get("/api/pathfinder/feed")
        assert r.status_code == 404 and r.json()["error"]["code"] == "no_edition"
    finally:
        set_research_store(None)


# ═══════════════════════════════════════════════════════════════════════════
# Port fidelity on the REAL warehouse (S1-34) — skips when it is absent
# ═══════════════════════════════════════════════════════════════════════════

PRICE_DB = os.environ.get("KANIDA_DB", r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
real = pytest.mark.skipif(not Path(PRICE_DB).exists(), reason="price warehouse not present")


@real
def test_s1_34_theme_and_pair_math_reproduce_the_prototype_on_real_data(tmp_path):
    """
    pathfinder_theme.py / pathfinder_demo.py, run after close 2026-07-29, printed: IT beat the
    market on 8 of 15 sessions, +11.7% vs +2.6%, 93% breadth, persistence 70% of 3,296; the
    HDFCBANK/ICICIBANK pair snapped back 89% of 412 times. The port must reproduce those
    numbers exactly where the statistic is identical (these are close-to-close measurements
    that the next-open convention does not touch).
    """
    from datetime import datetime as _dt
    cfg = ResearchConfig(research_db=str(tmp_path / "r.db"), usefulness_threshold=0.0, **PASS1_DATA)
    md = MarketData.load(cfg, as_of="2026-07-29")
    if md.as_of != "2026-07-29":
        pytest.skip("warehouse does not contain 2026-07-29")
    from pathfinder.research.regime import build_regime, regime_on
    ctx = LIB.ScanContext(md=md, cfg=cfg, regime=regime_on(build_regime(md), md.as_of), computed_at=_dt.now())
    theme = LIB.compute_theme_cycle(ctx, LIB.parameters_for(LIB.THEME_CYCLE, cfg))[0]
    v = {f.id.rsplit("_", 1)[-1] if not f.id.endswith("_return") else f.id.split("technology_", 1)[1]: f
         for f in theme.facts.facts}
    assert theme.subject == "Information Technology"
    assert v["out"].value == 8 and v["window"].value == 15
    assert v["sector_return"].value == pytest.approx(11.7, abs=0.05)
    assert v["market_return"].value == pytest.approx(2.6, abs=0.05)
    # The prototype printed 3,296. It read MAZDOCK's eight 0.0-price bars as prices, which
    # turned every 15-session window touching them into NaN for ALL sectors and silently
    # dropped 20 leader-sessions. With the holes treated as holes the count is 3,316 —
    # verified by re-injecting the zeros, which reproduces 3,296 exactly.
    assert v["persistence"].n == 3316 and v["persistence"].value == pytest.approx(70, abs=0.5)
    pair = LIB.compute_relationship(ctx, LIB.parameters_for(LIB.RELATIONSHIP, cfg))[0]
    pv = {f.id.split("icicibank_", 1)[1]: f for f in pair.facts.facts}
    assert pair.subject == "HDFCBANK / ICICIBANK"
    assert pv["snapped_back"].n == 412 and pv["snapped_back"].value == pytest.approx(89, abs=0.5)
    dip = LIB.compute_dip(ctx, LIB.parameters_for(LIB.DIP, cfg))[0]
    # The prototype's 12,633 cases: one of them resolved across a listing-day glitch bar
    # (test_pathfinder_s1_audit D1), so its next-session outcome is a hole -> 12,632.
    assert dip.subject == "J&KBANK" and dip.n == 12632
    surge = LIB.compute_surge(ctx, LIB.parameters_for(LIB.SURGE, cfg))[0]
    # The prototype counted 24,734: one was MAZDOCK's +inf "surge" off a 0.0 bar, and six
    # more were listing-day glitch bars (DELHIVERY +9,200% ...) — 24,727 real jumps.
    assert surge.subject == "PCBL" and surge.n == 24727
