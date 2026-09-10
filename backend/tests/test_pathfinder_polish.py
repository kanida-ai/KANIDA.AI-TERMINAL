"""
Integration polish (after S1 / S2 / S3) — the seams the S3 hand-back flagged (§4), pinned.

  PL-01  /loop and /learnings under the research source: a guarded 404 that names the served
         paths — never a 500, never a stack trace. The mock source still serves them.
  PL-02  the mock app's / and /healthz never raise under the research source.
  PL-03  the exchange-calendar projection skips weekends and NSE closures.
  PL-04  the served feed stamps engine_version / schema_version and gives every pending card a
         due_session — the engine's own when stamped, else a LABELLED projection; a graded card
         is left exactly as the engine graded it.
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pathfinder.research.store import set_research_store            # noqa: E402
from pathfinder.experiments.store import set_experiment_store       # noqa: E402
from pathfinder.sessions import BASIS_PROJECTED, BASIS_SESSION_CALENDAR, project_session_after  # noqa: E402
from pathfinder.schemas import (                                    # noqa: E402
    BACKFILL_LABEL, Author, DateRange, Decision, EvidenceLevel, Fact, FeedResponse, Finding, FindingProvenance,
    GradingKind, GradingRule, GradingState, GradingStatus, Narrative, SampleFlag, ScoreCounts, Scoreboard,
    SubjectKind, Tier, Unit, UsefulnessScore, Verdict,
)


@pytest.fixture()
def research_client(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from pathfinder.mock_app import app
    from pathfinder.store import set_store
    monkeypatch.setenv("KANIDA_PATHFINDER_SOURCE", "research")
    monkeypatch.setenv("KANIDA_PATHFINDER_RESEARCH_DB", str(tmp_path / "missing_research.db"))
    monkeypatch.setenv("KANIDA_PATHFINDER_EXPERIMENTS_DB", str(tmp_path / "missing_experiments.db"))
    set_store(None); set_research_store(None); set_experiment_store(None)
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        set_store(None); set_research_store(None); set_experiment_store(None)


def test_pl01_loop_and_learnings_are_guarded_under_the_research_source_never_500(research_client):
    for path in ("/api/pathfinder/loop", "/api/pathfinder/learnings"):
        r = research_client.get(path)
        assert r.status_code == 404, (path, r.status_code, r.text[:200])
        body = r.json()
        assert body["error"]["code"] == "not_served_by_source"
        assert "/api/pathfinder/feed" in body["error"]["use"] and "/api/pathfinder/experiments" in body["error"]["use"]
        assert "/api/pathfinder/feed" in body["error"]["message"]
        low = r.text.lower()
        assert not any(t in low for t in ("traceback", "runtimeerror", "sqlite", "select ", "\\\\"))
    # the served paths still answer honestly with no store: 404 no_edition / no_experiments, not 500
    assert research_client.get("/api/pathfinder/feed").json()["error"]["code"] == "no_edition"
    assert research_client.get("/api/pathfinder/experiments").json()["error"]["code"] == "no_experiments"


def test_pl01b_the_mock_source_still_serves_loop_and_learnings(monkeypatch):
    from fastapi.testclient import TestClient
    from pathfinder.mock_app import app
    from pathfinder.store import set_store
    monkeypatch.setenv("KANIDA_PATHFINDER_SOURCE", "mock")
    set_store(None)
    try:
        c = TestClient(app)
        assert c.get("/api/pathfinder/loop").status_code == 200
        assert c.get("/api/pathfinder/learnings").status_code == 200
    finally:
        set_store(None)


def test_pl02_root_and_healthz_never_raise_under_the_research_source(research_client):
    r = research_client.get("/")
    assert r.status_code == 200 and r.json()["data_source"].startswith("research") and "mock" not in r.json()["data_source"]
    assert "/api/pathfinder/feed" in r.json()["endpoints"] and "/api/pathfinder/loop" not in r.json()["endpoints"]
    h = research_client.get("/healthz")
    assert h.status_code == 200 and h.json()["ok"] is True


def test_pl03_the_projection_skips_weekends_and_nse_closures():
    assert project_session_after(date(2026, 7, 29), 5) == date(2026, 8, 5)       # plain week, Wed -> Wed
    assert project_session_after(date(2026, 7, 31), 1) == date(2026, 8, 3)       # Fri -> Mon
    assert project_session_after(date(2026, 1, 23), 1) == date(2026, 1, 27)      # Republic Day Mon skipped
    assert project_session_after(date(2026, 10, 1), 1) == date(2026, 10, 5)      # Gandhi Jayanti Fri skipped
    assert project_session_after(date(2026, 10, 1), 10) == date(2026, 10, 16)
    with pytest.raises(ValueError):
        project_session_after(date(2026, 7, 29), 0)


# ── PL-04: a minimal, contract-valid edition built by hand (synthetic; not a result) ─────────

_AT = datetime(2026, 9, 10, 18, 0, 0)


def _fact(fid: str, value, unit: Unit, n=None) -> Fact:
    return Fact(id=fid, label=fid, value=value, unit=unit, n=n,
                sample_flag=(SampleFlag.not_applicable if n is None else SampleFlag.unknown),
                provenance={"data_source": "synthetic", "date_range": {"start": "2013-01-01", "end": "2026-07-29"},
                            "as_of": "2026-07-29", "cost_convention": "pf_cost_hurdle_v2",
                            "computed_by": "pathfinder_research@1.3.0+code.test", "computed_at": _AT.isoformat()})


def _finding(fid: str, rank: int, grading: GradingState, horizon: int = 5) -> Finding:
    return Finding(
        id=fid, edition_date=date(2026, 7, 29), rank=rank, tier=Tier.what_matters_now, template_id="dip",
        question="Does the dip bounce after costs?", subject=fid.upper(), subject_kind=SubjectKind.stock,
        decision=Decision.no_trade, decision_reason="History says the bounce does not clear costs.",
        narrative=Narrative(headline="A hard fall; history says it is usually noise",
                            body="It fell {{fact:fct_" + fid + "_move}} today.", produced_by=Author.engine, at=_AT,
                            fact_refs=["fct_" + fid + "_move"]),
        facts=[_fact("fct_" + fid + "_move", -8.2, Unit.pct), _fact("fct_" + fid + "_hit", 50.6, Unit.pct, n=12368)],
        key_fact_refs=["fct_" + fid + "_move"],
        provenance=FindingProvenance(level=EvidenceLevel.whole_market, n=12368, period=DateRange(start=date(2013, 1, 1), end=date(2026, 7, 29)),
                                     regime="NEUTRAL", comparison_group="every stock on sessions it fell at least 6%",
                                     cost_hurdle_pct=0.5, cost_convention="pf_cost_hurdle_v2", data_source="synthetic",
                                     universe="nifty500_today", as_of=date(2026, 7, 29), computed_by="pathfinder_research@1.3.0+code.test",
                                     computed_at=_AT, disclosures=["survivorship: today's members on history"]),
        grading_rule=GradingRule(kind=GradingKind.no_trade_call, horizon_sessions=horizon, hurdle_pct=0.5,
                                 metric="the move a long trade would have made", right="Right if …", wrong="Wrong if …",
                                 inconclusive="Inconclusive inside the hurdle", frozen_at=_AT, rule_version="grading_rules@1.3.0+code.test"),
        grading=grading, usefulness=UsefulnessScore(total=0.7, evidence_strength=0.6, novelty=0.8, trader_relevance=0.7,
                                                    magnitude=0.7, threshold=0.65, version="usefulness@1.0.0"),
        related_symbols=[fid.upper()], follow_up_questions=[], backfilled=True,
    )


def _feed(*findings: Finding) -> FeedResponse:
    zero = ScoreCounts(right=0, wrong=0, inconclusive=0, n=0)
    return FeedResponse(
        edition_date=date(2026, 7, 29), data_as_of=date(2026, 7, 29), generated_at=_AT, regime="NEUTRAL",
        universe_scanned=497, candidates_considered=6, published_count=len(findings), usefulness_threshold=0.65,
        what_matters_now=list(findings), discoveries=[],
        scoreboard=Scoreboard(right=1, wrong=0, inconclusive=0, n=1, pending=1, as_of=date(2026, 7, 29), forward=zero,
                              backfilled=ScoreCounts(right=1, wrong=0, inconclusive=0, n=1), n_total=1, n_independent=1,
                              record_label=BACKFILL_LABEL),
        llm_provider="none", backfilled=True, record_label=BACKFILL_LABEL,
    )


def test_pl04_the_served_feed_stamps_versions_and_a_labelled_due_session_on_pending_cards():
    from pathfinder.router import serve_feed, schema_version_string
    pending = _finding("fnd_a", 1, GradingState(status=GradingStatus.pending, backfilled=True), horizon=5)
    stamped = _finding("fnd_b", 2, GradingState(status=GradingStatus.pending, due_session=date(2026, 8, 5), backfilled=True))
    graded = _finding("fnd_c", 3, GradingState(status=GradingStatus.graded, verdict=Verdict.right, due_session=date(2026, 8, 5),
                                                graded_at=_AT, data_as_of=date(2026, 8, 5), backfilled=True,
                                                realized_facts=[_fact("fct_fnd_c_move", 1.2, Unit.pct)]))
    out = serve_feed(_feed(pending, stamped, graded), research_engine="pathfinder_research@1.3.0+code.abc123def456")
    assert out.engine_version == "pathfinder_research@1.3.0+code.abc123def456"
    assert out.schema_version == schema_version_string() and out.schema_version.startswith("pathfinder_feed@")
    a, b, c = out.what_matters_now
    # the engine left it null: projected over the exchange calendar, and SAID so
    assert a.grading.due_session == date(2026, 8, 5) and a.grading.due_session_basis == BASIS_PROJECTED
    assert a.grading.status == GradingStatus.pending and a.grading.verdict is None
    # the engine stamped it: kept verbatim, labelled as the data calendar's
    assert b.grading.due_session == date(2026, 8, 5) and b.grading.due_session_basis == BASIS_SESSION_CALENDAR
    # graded: untouched except the basis label; the verdict and realised facts are the engine's
    assert c.grading.verdict == Verdict.right and c.grading.due_session == date(2026, 8, 5)
    assert c.grading.due_session_basis == BASIS_SESSION_CALENDAR and c.grading.realized_facts[0].id == "fct_fnd_c_move"
    # the whole payload re-validated under the contract's own laws
    assert out.published_count == 3 and out.record_label == BACKFILL_LABEL
    # with S2 cards attached, both engine hashes ride on the feed
    out2 = serve_feed(_feed(pending), research_engine="pathfinder_research@1.3.0+code.aaa", experiment_cards=[],
                      experiments_scoreboard=None, experiments_engine="pathfinder_experiments@1.0.0+code.bbb")
    assert out2.engine_version == "pathfinder_research@1.3.0+code.aaa; pathfinder_experiments@1.0.0+code.bbb"


def test_pl04b_the_contract_carries_the_new_fields_as_optional_additive():
    fields = FeedResponse.model_fields
    assert fields["engine_version"].default is None and fields["schema_version"].default is None
    assert GradingState.model_fields["due_session_basis"].default is None
    assert os.environ.get("KANIDA_PATHFINDER_SOURCE", "mock") in ("mock", "research", "engine", "postgres")
