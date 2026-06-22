"""Focused tests for the Co-Trading virtual-portfolio sim + router.

Covers (per the build brief):
  - simulate returns positions + equity + summary for a known past start_date
  - summary.return_pct sign sanity (finite number, equity ties to current_value)
  - NO look-ahead: every position's entry_date >= start_date
  - capital scaling: 2x capital -> ~2x per-trade allocation (qty doubles)
  - POST /cotrade/simulate happy path + 400 on bad input
  - GET /cotrade/portfolio returns the last simulated result

Uses the live engine DB (read-only). Skips cleanly if the DB isn't present
(CI / cloud deploy without Desktop access), mirroring test_ask_router.py.

NEVER touches the auto-trade execution path.
"""
from __future__ import annotations

import os
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from power_user import config
from power_user.services import cotrade_sim
from power_user.routers.cotrade_router import router as cotrade_router

# Single live DB that carries BOTH ohlc/features AND the pattern catalog.
_LIVE_DB = (
    r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
    r"\data\db\kanida_universe.db"
)
# A past window known to have signals (engine data starts 2021).
START = "2024-01-01"
END = "2024-03-31"


@pytest.fixture(scope="module")
def live_db():
    if not os.path.exists(_LIVE_DB):
        pytest.skip("Live engine DB not available")
    return _LIVE_DB


@pytest.fixture(scope="module")
def wired(live_db, monkeypatch_module=None):
    """Point PROD_DB + RND resolver at the live DB and clear the sim cache."""
    # Module-scoped monkeypatch via direct attribute set (restored at teardown).
    orig_prod = cotrade_sim.PROD_DB
    orig_rnd = cotrade_sim._resolve_rnd_db_path
    cotrade_sim.PROD_DB = live_db
    cotrade_sim._resolve_rnd_db_path = lambda: live_db
    cotrade_sim._CACHE.clear()
    cotrade_sim._LAST_RESULT.update(key=None, result=None, at=0.0)
    yield
    cotrade_sim.PROD_DB = orig_prod
    cotrade_sim._resolve_rnd_db_path = orig_rnd


@pytest.fixture(scope="module")
def result_5l(wired):
    return cotrade_sim.simulate_cotrade("falcon-top-10", 5_00_000.0, START, END)


@pytest.fixture(scope="module")
def result_10l(wired):
    return cotrade_sim.simulate_cotrade("falcon-top-10", 10_00_000.0, START, END)


# ── shape ────────────────────────────────────────────────────────────────

def test_payload_shape(result_5l):
    r = result_5l
    assert set(r["summary"]).issuperset({
        "starting_rs", "current_value_rs", "total_pnl_rs", "return_pct",
        "max_dd_pct", "n_open", "n_closed", "cash_rs", "win_rate_pct", "n_trades",
    })
    assert r["positions"], "expected at least one position"
    assert r["equity"], "expected an equity curve"
    assert r["actions"], "expected an actions feed"
    p = r["positions"][0]
    assert set(p).issuperset({
        "symbol", "sector", "tier", "entry_date", "entry_price", "qty",
        "capital_rs", "sl_level", "status", "pnl_rs", "pnl_pct",
        "hold_days", "last_close",
    })
    assert p["status"] in ("open", "closed")
    eq = r["equity"][0]
    assert set(eq) == {"date", "equity_rs"}
    act = r["actions"][0]
    assert act["type"] in ("entry", "exit", "trail", "skip")


def test_return_pct_sign_sanity(result_5l):
    s = result_5l["summary"]
    # finite + internally consistent: current_value = starting + total_pnl
    assert abs((s["starting_rs"] + s["total_pnl_rs"]) - s["current_value_rs"]) < 1.0
    # return_pct matches current/starting
    expected = (s["current_value_rs"] / s["starting_rs"] - 1) * 100
    assert abs(s["return_pct"] - round(expected, 2)) < 0.05
    assert s["max_dd_pct"] <= 0.0           # drawdown is non-positive


def test_starting_capital_honoured(result_5l):
    assert result_5l["summary"]["starting_rs"] == 5_00_000.0
    assert result_5l["equity"][0]["equity_rs"] == pytest.approx(5_00_000.0, abs=1.0)


# ── no look-ahead ──────────────────────────────────────────────────────────

def test_no_lookahead(result_5l):
    bad = [p for p in result_5l["positions"] if p["entry_date"] < START]
    assert bad == [], f"{len(bad)} positions entered before start_date"


# ── capital scaling ─────────────────────────────────────────────────────────

def test_capital_scaling_doubles_allocation(result_5l, result_10l):
    """2x capital -> ~2x per-trade allocation. Compare the qty of the SAME
    symbol on the SAME entry_date across the two runs (integer-share rounding
    keeps the ratio within a tight band)."""
    def by_key(r):
        return {(p["symbol"], p["entry_date"]): p for p in r["positions"]}
    a, b = by_key(result_5l), by_key(result_10l)
    common = set(a) & set(b)
    assert common, "expected overlapping positions across the two capital runs"
    ratios = [b[k]["qty"] / a[k]["qty"] for k in common if a[k]["qty"] > 0]
    avg = sum(ratios) / len(ratios)
    assert 1.9 <= avg <= 2.1, f"qty ratio {avg:.3f} not ~2x for 2x capital"


# ── router ──────────────────────────────────────────────────────────────────

@pytest.fixture
def client(wired) -> TestClient:
    app = FastAPI()
    app.include_router(cotrade_router)
    return TestClient(app)


def test_router_simulate_ok(client):
    resp = client.post("/api/power/cotrade/simulate", json={
        "style": "falcon-top-10", "capital": 5_00_000.0,
        "start_date": START, "end_date": END,
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["summary"]["starting_rs"] == 5_00_000.0
    assert body["positions"]


def test_router_simulate_bad_style(client):
    resp = client.post("/api/power/cotrade/simulate", json={
        "style": "nope", "capital": 5_00_000.0, "start_date": START,
    })
    assert resp.status_code == 400


def test_router_simulate_bad_capital(client):
    resp = client.post("/api/power/cotrade/simulate", json={
        "style": "falcon-top-10", "capital": -5, "start_date": START,
    })
    assert resp.status_code == 400


def test_router_simulate_bad_date(client):
    resp = client.post("/api/power/cotrade/simulate", json={
        "style": "falcon-top-10", "capital": 5_00_000.0, "start_date": "not-a-date",
    })
    assert resp.status_code == 400


def test_router_portfolio_returns_last(client):
    # simulate first, then GET should echo it back
    client.post("/api/power/cotrade/simulate", json={
        "style": "falcon-top-10", "capital": 5_00_000.0,
        "start_date": START, "end_date": END,
    })
    resp = client.get("/api/power/cotrade/portfolio")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("summary", {}).get("starting_rs") == 5_00_000.0
