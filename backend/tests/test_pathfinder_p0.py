"""
Pathfinder P0 — the guardrail self-check.

This is the executable form of the non-negotiables in CLAUDE.md. It runs against
the SERVED payloads (through the router, not the fixture objects) so it tests the
contract as a client experiences it.

Every test here maps to a row in docs/TEST_PLAN.md § Pathfinder (P0).
"""
import json
import os
import re
import subprocess
import sys

import pytest
from fastapi.testclient import TestClient

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_BACKEND)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from pathfinder.mock_app import app  # noqa: E402
from pathfinder.schemas import (  # noqa: E402
    FACT_REF_RE,
    N_FLAG_BELOW,
    N_GREY_BELOW,
    REF_TOKEN_RE,
    SIMULATED_LABEL,
    VIRTUAL_LABEL,
)

client = TestClient(app)

ENDPOINTS = [
    "/api/pathfinder/loop",
    "/api/pathfinder/experiments",
    "/api/pathfinder/learnings",
    "/api/pathfinder/experiment/exp_0003",
    "/api/pathfinder/experiment/exp_0007",
    "/api/pathfinder/experiment/exp_0009",
    "/api/pathfinder/experiment/exp_0011",
    "/api/pathfinder/experiment/exp_0014",
    "/api/pathfinder/experiment/exp_0015",
]

#: Phrases that would turn research into a promise. Content lint (R-non-negotiable).
BANNED_PHRASES = [
    "guaranteed", "guarantee", "assured return", "sure shot", "sure-shot",
    "multibagger", "risk-free", "riskfree", "cannot lose", "can't lose",
    "double your", "target price", "price target", "will deliver", "will return",
    "no downside", "safe bet", "definitely will",
]

#: Field names that would smuggle a promise or a price target into the payload.
BANNED_FIELD_RE = re.compile(
    r"^(target|target_price|price_target|expected_return|projected_\w+|projection|"
    r"forecast|guaranteed_\w+|upside)$"
)


def _get(path: str) -> dict:
    r = client.get(path)
    assert r.status_code == 200, f"{path} -> {r.status_code}"
    return r.json()


@pytest.fixture(scope="module")
def payloads() -> dict[str, dict]:
    return {p: _get(p) for p in ENDPOINTS}


def _walk(node, path="$"):
    yield path, node
    if isinstance(node, dict):
        for k, v in node.items():
            yield from _walk(v, f"{path}.{k}")
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from _walk(v, f"{path}[{i}]")


# ── The endpoints exist and serve ───────────────────────────────────────────

def test_all_endpoints_serve(payloads):
    assert len(payloads) == len(ENDPOINTS)


def test_status_filter_narrows(payloads):
    died = _get("/api/pathfinder/experiments?status=died")
    assert died["count"] >= 1
    assert {i["status"] for i in died["items"]} == {"died"}
    assert died["count"] < payloads["/api/pathfinder/experiments"]["count"]


def test_every_lifecycle_status_has_a_fixture():
    for status in ("queued", "testing", "validating", "promising", "promoted", "died"):
        body = _get(f"/api/pathfinder/experiments?status={status}")
        assert body["count"] >= 1, f"no fixture for status {status}"


# ── Guarded errors: never leak internals ────────────────────────────────────

def test_unknown_experiment_is_a_clean_404():
    r = client.get("/api/pathfinder/experiment/exp_9999")
    assert r.status_code == 404
    body = r.json()
    assert body["error"]["code"] == "not_found"
    assert body["error"]["request_id"]
    assert set(body) == {"error"}


def test_malformed_experiment_id_is_a_clean_400():
    r = client.get("/api/pathfinder/experiment/DROP-TABLE")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_experiment_id"


def test_error_bodies_leak_nothing():
    for path in ("/api/pathfinder/experiment/exp_9999", "/api/pathfinder/experiment/bad!"):
        text = client.get(path).text.lower()
        for leak in ("traceback", "select ", "sqlite", "postgres", "c:\\", "/backend/",
                     "pathfinder.store", "site-packages"):
            assert leak not in text, f"{path} leaked {leak!r}"


# ── L-1 / L-2: expectancy is the hero, returns are paired with drawdown ─────

def _performance_blocks(payloads):
    for path, body in payloads.items():
        for where, node in _walk(body, path):
            if isinstance(node, dict) and "expectancy_pct_per_trade" in node:
                yield where, node


def test_every_performance_block_pairs_return_with_drawdown(payloads):
    seen = 0
    for where, pb in _performance_blocks(payloads):
        seen += 1
        assert pb.get("expectancy_pct_per_trade") is not None, where
        assert pb.get("expectancy_2x_slippage_pct_per_trade") is not None, where
        assert pb.get("max_drawdown_pct") is not None, where
        assert pb.get("current_drawdown_pct") is not None, where
        if pb.get("total_return_pct") is not None:
            assert pb["max_drawdown_pct"] is not None, f"{where}: return without drawdown"
    assert seen >= 8, "expected performance blocks across the fixtures"


def test_win_rate_is_never_the_only_metric(payloads):
    for where, pb in _performance_blocks(payloads):
        if pb.get("win_rate_pct") is not None:
            assert pb.get("expectancy_pct_per_trade") is not None, (
                f"{where}: win-rate shown without expectancy"
            )


# ── L-3: no promises, no target prices ──────────────────────────────────────

#: A lint that flagged "there is no price target" would be a bad lint. An explicit
#: negation of a banned phrase is exactly the copy we WANT, so it is allowed.
_NEGATIONS = ("no ", "not a ", "never a ", "never ", "without a ", "without ")


def _banned_hits(blob: str, phrase: str) -> list[int]:
    hits = []
    start = 0
    while (i := blob.find(phrase, start)) != -1:
        before = blob[max(0, i - 14):i]
        if not any(before.endswith(neg) for neg in _NEGATIONS):
            hits.append(i)
        start = i + len(phrase)
    return hits


def test_no_promise_phrases_anywhere(payloads):
    for path, body in payloads.items():
        blob = json.dumps(body, ensure_ascii=False).lower()
        for phrase in BANNED_PHRASES:
            hits = _banned_hits(blob, phrase)
            assert not hits, (
                f"{path} contains banned phrase {phrase!r} at {hits[0]}: "
                f"...{blob[max(0, hits[0] - 60):hits[0] + 60]}..."
            )


def test_no_target_or_projection_fields_anywhere(payloads):
    for path, body in payloads.items():
        for where, node in _walk(body, path):
            if isinstance(node, dict):
                for key in node:
                    assert not BANNED_FIELD_RE.match(key), f"{where}: banned field {key!r}"


def test_every_rulebook_has_an_invalidation_not_a_target(payloads):
    for path, body in payloads.items():
        for _, node in _walk(body, path):
            if isinstance(node, dict) and "invalidation" in node and "entry" in node:
                assert node["invalidation"].strip(), "invalidation must not be empty"
                assert "target" not in node["invalidation"].lower() or (
                    "no price target" in node["invalidation"].lower()
                )


# ── L-4 / L-5: provenance + point-in-time ───────────────────────────────────

def _provenances(payloads):
    for path, body in payloads.items():
        for where, node in _walk(body, path):
            if isinstance(node, dict) and "cost_convention" in node and "as_of" in node:
                yield where, node


def test_every_number_carries_source_range_asof_and_costs(payloads):
    seen = 0
    for where, prov in _provenances(payloads):
        seen += 1
        for field in ("data_source", "date_range", "as_of", "cost_convention",
                      "computed_by", "computed_at"):
            assert prov.get(field), f"{where}: provenance missing {field}"
    assert seen >= 20


def test_no_provenance_looks_ahead(payloads):
    """date_range.end never post-dates the as_of it was computed at."""
    for where, prov in _provenances(payloads):
        assert prov["date_range"]["end"] <= prov["as_of"], (
            f"{where}: window ends after as_of — look-ahead"
        )
        assert prov["date_range"]["start"] <= prov["date_range"]["end"], where


def test_nothing_is_computed_by_a_model(payloads):
    """computed_by must name a deterministic engine. The LLM never calculates."""
    for where, prov in _provenances(payloads):
        cb = prov["computed_by"].lower()
        for model_marker in ("claude", "gpt", "gemini", "sonnet", "haiku", "opus", "llm"):
            assert model_marker not in cb, f"{where}: computed_by names a model ({cb})"


def test_entries_are_the_next_session_after_the_signal(payloads):
    seen = 0
    for path, body in payloads.items():
        for where, node in _walk(body, path):
            if isinstance(node, dict) and "signal_date" in node and "entry_date" in node:
                seen += 1
                assert node["entry_date"] > node["signal_date"], (
                    f"{where}: entry is not after the signal bar"
                )
                assert node.get("slippage_bps") is not None, f"{where}: no slippage charged"
    assert seen >= 25


# ── L-6: n flags are derived, and small samples are labelled ────────────────

def test_sample_flags_match_the_rule(payloads):
    seen = 0
    for path, body in payloads.items():
        for where, node in _walk(body, path):
            if isinstance(node, dict) and "sample_flag" in node and "n" in node:
                seen += 1
                n, flag = node["n"], node["sample_flag"]
                if n is None:
                    expected = "unknown"
                elif n < N_GREY_BELOW:
                    expected = "greyed"
                elif n < N_FLAG_BELOW:
                    expected = "flagged"
                else:
                    expected = "ok"
                assert flag == expected, f"{where}: n={n} flagged {flag}, expected {expected}"
    assert seen >= 20


def test_fixtures_are_honest_about_small_samples(payloads):
    flags = set()
    for path, body in payloads.items():
        for _, node in _walk(body, path):
            if isinstance(node, dict) and "sample_flag" in node:
                flags.add(node["sample_flag"])
    assert "greyed" in flags, "no n<20 case — the fixtures are flattering reality"
    assert "flagged" in flags, "no 20<=n<50 case — the fixtures are flattering reality"


# ── L-7: losers first ───────────────────────────────────────────────────────

def test_every_ledger_is_losers_first(payloads):
    seen = 0
    for path, body in payloads.items():
        for where, node in _walk(body, path):
            if isinstance(node, dict) and "ledger_losers_first" in node:
                seen += 1
                pnl = [t["pnl_pct_net"] for t in node["ledger_losers_first"]]
                assert pnl == sorted(pnl), f"{where}: ledger is not losers-first"
                assert pnl and pnl[0] < 0, f"{where}: no loser shown first"
    assert seen >= 4


def test_experiment_list_leads_with_the_struggling_ones(payloads):
    items = payloads["/api/pathfinder/experiments"]["items"]
    statuses = [i["status"] for i in items]
    assert statuses[0] == "died", "the dead experiment must not be buried"
    assert statuses[-1] == "promoted", "the winner must not lead"


# ── L-8: THE LLM NEVER CALCULATES ───────────────────────────────────────────

def _story_lines(payloads):
    for path, body in payloads.items():
        for where, node in _walk(body, path):
            if isinstance(node, dict) and "produced_by" in node and "beat" in node:
                yield where, node


def test_llm_authored_prose_contains_no_numerals(payloads):
    seen = 0
    for where, line in _story_lines(payloads):
        if line["produced_by"] != "llm":
            continue
        seen += 1
        for field in ("headline", "body"):
            stripped = REF_TOKEN_RE.sub("", line[field])
            assert not any(ch.isdigit() for ch in stripped), (
                f"{where}.{field}: a number originated in the LLM"
            )
    assert seen >= 25, "expected plenty of LLM-authored story lines"


def test_every_fact_reference_resolves(payloads):
    for path, body in payloads.items():
        known = {f["id"] for f in body.get("facts", [])}
        if not known:
            continue
        for where, line in _walk(body, path):
            if not (isinstance(line, dict) and "produced_by" in line and "beat" in line):
                continue
            used = set()
            for field in ("headline", "body"):
                used |= {m.group("id") for m in FACT_REF_RE.finditer(line[field])}
            assert used <= known, f"{where}: dangling fact refs {sorted(used - known)}"
            assert used <= set(line["fact_refs"]), f"{where}: undeclared fact refs"


def test_llm_lines_name_their_model_and_engine_lines_do_not(payloads):
    for where, line in _story_lines(payloads):
        if line["produced_by"] == "llm":
            assert line.get("model"), f"{where}: llm line without a model id"
            assert line["model"] in {"claude-sonnet-5", "claude-haiku-4-5", "claude-opus-5"}, (
                f"{where}: model {line['model']!r} is not in the locked routing"
            )
        else:
            assert not line.get("model"), f"{where}: non-llm line names a model"


# ── Labels: backtests are always labelled ───────────────────────────────────

def test_every_performance_block_carries_its_honesty_label(payloads):
    for where, pb in _performance_blocks(payloads):
        expected = SIMULATED_LABEL if pb["basis"] == "historical_replay" else VIRTUAL_LABEL
        assert pb["label"] == expected, f"{where}: wrong or missing honesty label"


def test_every_response_carries_the_research_disclosure(payloads):
    for path, body in payloads.items():
        assert "disclosure" in body, f"{path}: no disclosure"
        assert "not advice" in body["disclosure"].lower()


# ── Governance: the change-log and the graveyard ────────────────────────────

def test_died_experiment_publishes_a_post_mortem(payloads):
    died = payloads["/api/pathfinder/experiment/exp_0007"]
    assert died["status"] == "died"
    pm = died["post_mortem"]
    assert pm and pm["cause"] and pm["what_we_kept"].strip()
    assert pm["evidence_refs"]


def test_only_died_experiments_carry_a_post_mortem(payloads):
    for path, body in payloads.items():
        if "/experiment/" not in path:
            continue
        if body["status"] != "died":
            assert body.get("post_mortem") is None, f"{path}: post-mortem without a death"


def test_change_log_records_what_why_evidence_and_versions(payloads):
    seen = 0
    for path, body in payloads.items():
        for entry in body.get("change_log", []) or []:
            seen += 1
            assert entry["what_changed"].strip()
            assert entry["why"].strip()
            assert entry["evidence_refs"]
            assert entry["new_version"]
            assert entry["level"] in {"L1", "L2", "L3", "L4"}
            assert "improved" in entry
            if entry["level"] == "L3":
                assert entry.get("validation"), "L3 must state its backtest + forward validation"
            if entry["level"] == "L4":
                assert entry["decided_by"] == "human", "the Constitution is human-only"
                assert entry.get("approved_by")
    assert seen >= 5


def test_change_log_is_append_only(payloads):
    for path, body in payloads.items():
        seqs = [e["seq"] for e in (body.get("change_log") or [])]
        assert seqs == sorted(seqs) == sorted(set(seqs)), f"{path}: change_log not append-only"


def test_queued_experiment_shows_no_results(payloads):
    queued = payloads["/api/pathfinder/experiment/exp_0015"]
    assert queued["status"] == "queued"
    assert queued["historical_return"] is None
    assert queued["virtual_return"] is None
    assert queued["n"] is None and queued["spark"] is None
    assert queued["virtual_book"] is None


def test_triggers_are_fired_by_the_engine_not_a_clock(payloads):
    seen = 0
    for path, body in payloads.items():
        for where, node in _walk(body, path):
            if isinstance(node, dict) and "fired_by" in node:
                seen += 1
                assert node["fired_by"] == "engine", f"{where}: a trigger not fired by the observer"
    assert seen >= 5


# ── The loop story ──────────────────────────────────────────────────────────

def test_loop_tells_the_six_beats_in_order(payloads):
    beats = [line["beat"] for line in payloads["/api/pathfinder/loop"]["story"]]
    assert beats == ["noticed", "hypothesis", "experiment", "outcome", "learning", "next"]


def test_loop_reports_real_token_usage(payloads):
    usage = payloads["/api/pathfinder/loop"]["llm_usage"]
    assert usage["by_model"] and usage["daily_budget_usd"] is not None
    for m in usage["by_model"]:
        assert m["input_tokens"] > 0 and m["cost_usd"] >= 0
        assert m["model"] in {"claude-sonnet-5", "claude-haiku-4-5", "claude-opus-5"}


def test_learnings_show_what_is_blocked_too(payloads):
    body = payloads["/api/pathfinder/learnings"]
    assert body["learned"] and body["testing_next"]
    assert any(n.get("blocked_by") for n in body["testing_next"]), (
        "an honest research agenda names what it cannot test yet"
    )


# ── Realism: the fixtures must not be flattering ────────────────────────────

def test_some_experiments_fail_the_2x_slippage_gate(payloads):
    failures = [
        where for where, pb in _performance_blocks(payloads)
        if pb["expectancy_2x_slippage_pct_per_trade"] <= 0
    ]
    assert failures, (
        "no fixture fails at 2x slippage — that is not what reality looks like"
    )


def test_forward_is_not_uniformly_better_than_history(payloads):
    worse = 0
    for path, body in payloads.items():
        if "/experiment/" not in path:
            continue
        h, v = body.get("historical_return"), body.get("virtual_return")
        if h and v and v["expectancy_pct_per_trade"] < h["expectancy_pct_per_trade"]:
            worse += 1
    assert worse >= 3, "forward results that all beat history would be a fabrication tell"


# ── The contract file itself ────────────────────────────────────────────────

def test_openapi_yaml_is_in_sync_with_the_models():
    r = subprocess.run(
        [sys.executable, os.path.join(_ROOT, "scripts", "gen_openapi.py"), "--check"],
        capture_output=True, text=True, cwd=_ROOT,
    )
    assert r.returncode == 0, r.stdout + r.stderr


def test_openapi_yaml_validates():
    r = subprocess.run(
        [sys.executable, os.path.join(_ROOT, "scripts", "validate_openapi.py")],
        capture_output=True, text=True, cwd=_ROOT,
    )
    assert r.returncode == 0, r.stdout + r.stderr
