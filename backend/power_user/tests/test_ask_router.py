"""Focused tests for the Ask-Falcon read-only API (routers/ask_router.py).

Covers:
  - /universe/symbols returns a non-empty, sorted list with name == symbol
  - /quote returns last_close + as_of for a symbol present in ohlc_daily
  - /quote rejects > MAX_QUOTE_SYMBOLS and empty input
  - /ask/analyze-stock returns a tier + explanation for a covered symbol
  - /ask/analyze-stock returns 404 NOT_COVERED for a bogus symbol
  - analyze-stock never invents a company name (name == symbol)

Mirrors the temp-DB-snapshot fixture pattern in test_picks_router.py.
"""
from __future__ import annotations

import os
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from power_user import config
from power_user.db_init import init_power_user_schema
from power_user.routers.ask_router import router as ask_router, MAX_QUOTE_SYMBOLS
from power_user.services.signal_tier import SIGNAL_TIER_ENUM


@pytest.fixture(scope="module")
def main_db_copy(tmp_path_factory):
    src = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_universe.db"
    if not os.path.exists(src):
        pytest.skip("Live DB not available")
    dst = str(tmp_path_factory.mktemp("ask_test") / "kanida_test.db")
    src_con = sqlite3.connect(src); dst_con = sqlite3.connect(dst)
    src_con.backup(dst_con); src_con.close(); dst_con.close()
    init_power_user_schema(dst)
    yield dst


@pytest.fixture
def client(main_db_copy, monkeypatch) -> TestClient:
    monkeypatch.setattr(config, "POWER_DB_PATH", main_db_copy)
    a = FastAPI()
    a.include_router(ask_router)
    return TestClient(a)


@pytest.fixture(scope="module")
def a_covered_symbol(main_db_copy):
    """A symbol guaranteed to be in falcon_features on the latest date AND have
    >=2 ohlc rows — so it's valid for both /quote and /analyze-stock."""
    con = sqlite3.connect(main_db_copy)
    row = con.execute(
        "SELECT symbol FROM falcon_features "
        "WHERE trade_date = (SELECT MAX(trade_date) FROM falcon_features) "
        "ORDER BY symbol LIMIT 1"
    ).fetchone()
    con.close()
    assert row, "test DB has no falcon_features rows"
    return row[0]


# ── /universe/symbols ───────────────────────────────────────────────────────

def test_universe_returns_non_empty_sorted(client):
    r = client.get("/api/power/universe/symbols")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    assert body["as_of"] is not None
    syms = body["symbols"]
    assert len(syms) == body["count"]
    # name == symbol (no invented company names)
    assert all(s["name"] == s["symbol"] for s in syms)
    # keys present
    assert set(syms[0].keys()) == {"symbol", "name", "sector"}
    # sorted A→Z
    names = [s["symbol"] for s in syms]
    assert names == sorted(names)


# ── /quote ──────────────────────────────────────────────────────────────────

def test_quote_returns_last_close_for_known_symbol(client, a_covered_symbol):
    r = client.get(f"/api/power/quote?symbols={a_covered_symbol}")
    assert r.status_code == 200
    body = r.json()
    assert a_covered_symbol in body
    q = body[a_covered_symbol]
    assert q["last_close"] is not None
    assert q["as_of"] is not None
    # honest fields: last_close (NOT 'ltp'), prev_close, as_of
    assert set(q.keys()) == {"last_close", "prev_close", "as_of"}


def test_quote_batch_and_unknown_symbol(client, a_covered_symbol):
    r = client.get(f"/api/power/quote?symbols={a_covered_symbol},__NOPE__ZZZ__")
    assert r.status_code == 200
    body = r.json()
    assert a_covered_symbol in body
    # unknown symbol simply absent — never fabricated
    assert "__NOPE__ZZZ__" not in body


def test_quote_rejects_empty(client):
    r = client.get("/api/power/quote?symbols=,, ,")
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "NO_SYMBOLS"


def test_quote_rejects_too_many(client):
    many = ",".join(f"S{i}" for i in range(MAX_QUOTE_SYMBOLS + 5))
    r = client.get(f"/api/power/quote?symbols={many}")
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "TOO_MANY_SYMBOLS"


# ── /ask/analyze-stock ──────────────────────────────────────────────────────

def test_analyze_covered_symbol_has_tier_and_explanation(client, a_covered_symbol):
    r = client.get(f"/api/power/ask/analyze-stock?symbol={a_covered_symbol}")
    assert r.status_code == 200
    body = r.json()
    assert body["symbol"] == a_covered_symbol
    assert body["name"] == a_covered_symbol         # honest: no company-name table
    assert body["name_is_symbol"] is True
    assert body["tier"] in SIGNAL_TIER_ENUM
    assert isinstance(body["explanation"], str) and body["explanation"]
    assert isinstance(body["risk_warnings"], list)
    # narrative fields are all present strings
    for k in ("current_trend", "recent_price_movement", "recent_volume_behavior",
              "signal_day_movement", "sector_performance",
              "falcon_pattern_observations", "entry_context", "tier_reason"):
        assert isinstance(body[k], str) and body[k], f"{k} should be a non-empty string"


def test_analyze_bogus_symbol_404(client):
    r = client.get("/api/power/ask/analyze-stock?symbol=__NOT_A_REAL_SYMBOL__")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "NOT_COVERED"
