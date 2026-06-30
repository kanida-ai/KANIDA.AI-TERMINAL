"""Tests for GET /autotrade/session/picks endpoint logic.

Calls the endpoint function directly (no HTTP stack needed) with the test DB
(seeded via conftest seed_signals / seed_universe helpers). The operator-token
dependency is tested via the router fixture test.
"""
import pytest

from autotrade.api.autotrade_routes import session_picks
from tests.autotrade.conftest import seed_signals, seed_universe


# ── helpers ───────────────────────────────────────────────────────────────────

def _seed_basic():
    """5 picks + universe_master rows (first two are nifty50)."""
    seed_signals([
        ("INFY",     1, 1842.5 * 14, 1820.5),  # score=25795, n_fires=14
        ("RELIANCE", 2, 1700.0 * 12, 2800.0),  # score=20400, n_fires=12
        ("TCS",      3, 1600.0 * 10, 3500.0),
        ("HDFC",     4, 1500.0 *  8, 1400.0),
        ("ZOMATO",   5, 1200.0 *  6,  180.0),
    ])
    seed_universe([
        ("INFY",     1, 1, 1),  # in nifty50, nifty100, nifty200
        ("RELIANCE", 1, 1, 1),  # in nifty50, nifty100, nifty200
        ("TCS",      0, 1, 1),  # nifty100 + nifty200, NOT nifty50
        ("HDFC",     0, 1, 1),  # nifty100 + nifty200, NOT nifty50
        ("ZOMATO",   0, 0, 1),  # only nifty200, NOT nifty50 or nifty100
    ])


# ── test_picks_endpoint_returns_ranked_list ───────────────────────────────────

def test_picks_endpoint_returns_ranked_list(clean_positions):
    """GET /session/picks returns a picks list with required fields."""
    _seed_basic()

    result = session_picks(universe="all500", top_n=5)

    assert "picks" in result
    assert "signal_date" in result
    assert result["universe_filter"] == "all500"
    assert result["top_n"] == 5

    picks = result["picks"]
    assert len(picks) == 5

    # Every pick must have the required response fields.
    required_keys = {"rank", "symbol", "sector", "score", "n_fires", "avg_lift",
                     "close_at_signal"}
    for p in picks:
        assert required_keys.issubset(set(p.keys())), (
            f"Missing keys in pick {p}: {required_keys - set(p.keys())}")

    # Ranked order: rank 1 first.
    assert picks[0]["symbol"] == "INFY"
    assert picks[0]["rank"] == 1


# ── test_picks_endpoint_universe_filter_applied ───────────────────────────────

def test_picks_endpoint_universe_filter_applied(clean_positions):
    """universe=nifty50 returns only symbols with in_nifty50=1."""
    _seed_basic()

    result_all = session_picks(universe="all500", top_n=10)
    result_n50 = session_picks(universe="nifty50",  top_n=10)

    all_syms = {p["symbol"] for p in result_all["picks"]}
    n50_syms = {p["symbol"] for p in result_n50["picks"]}

    # all500 returns all 5 seeded picks.
    assert all_syms == {"INFY", "RELIANCE", "TCS", "HDFC", "ZOMATO"}

    # nifty50 returns only the 2 symbols with in_nifty50=1.
    assert n50_syms == {"INFY", "RELIANCE"}
    assert len(result_n50["picks"]) == 2
    assert result_n50["universe_filter"] == "nifty50"

    # TCS / HDFC / ZOMATO (not in nifty50) must be absent.
    assert "TCS" not in n50_syms
    assert "HDFC" not in n50_syms
    assert "ZOMATO" not in n50_syms


# ── test_picks_endpoint_respects_top_n ───────────────────────────────────────

def test_picks_endpoint_respects_top_n(clean_positions):
    """top_n=3 returns at most 3 picks even when more are available."""
    _seed_basic()

    result = session_picks(universe="all500", top_n=3)

    assert len(result["picks"]) == 3
    assert result["top_n"] == 3

    # Must be the top-ranked 3 (rank 1, 2, 3).
    ranks = [p["rank"] for p in result["picks"]]
    assert ranks == [1, 2, 3]


# ── test_picks_endpoint_empty_when_no_signals ─────────────────────────────────

def test_picks_endpoint_empty_when_no_signals(clean_positions):
    """When falcon_signals_live is empty the endpoint returns an empty picks list."""
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute("DELETE FROM falcon_signals_live")
        con.commit()

    result = session_picks(universe="all500", top_n=10)

    assert result["picks"] == []


# ── test_picks_endpoint_invalid_universe_raises ───────────────────────────────

def test_picks_endpoint_invalid_universe_raises():
    """An unknown universe value raises HTTPException(400)."""
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        session_picks(universe="nifty1000", top_n=10)
    assert exc_info.value.status_code == 400
    assert "nifty1000" in str(exc_info.value.detail)


# ── test_picks_endpoint_all_universe_filters_accepted ────────────────────────

def test_picks_endpoint_all_universe_filters_accepted(clean_positions):
    """All 5 valid universe values are accepted without error."""
    _seed_basic()
    for univ in ("all500", "nifty50", "nifty100", "nifty200", "fno"):
        result = session_picks(universe=univ, top_n=10)
        assert "picks" in result, f"Missing 'picks' for universe={univ}"
