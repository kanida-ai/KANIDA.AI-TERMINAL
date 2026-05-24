"""Tests for portfolio_sizing — V3 audit-ready position sizing assistant
(post 2026-05-16 lock-down: fixed_per_trade_rs replaces per_trade_pct).

Strategy:
  - The math is pure (no DB, no network).
  - We test every key field the UI consumes:
    * per-trade size scales linearly with user capital
    * monthly P&L range derives from worst/best year operator-locked metrics
    * MIN/MAX clamps fire for absurd inputs
    * persona slugs + risk tiers come through as locked
"""
from __future__ import annotations

import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.normpath(os.path.join(_HERE, "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from power_user.services.portfolio_sizing import (    # noqa: E402
    MAX_CAPITAL_RS, MIN_CAPITAL_RS, compute_sizing,
)
from power_user.services.portfolio_defs import (      # noqa: E402
    ALL_PORTFOLIOS, VIRTUAL_CAPITAL_START,
)


def test_returns_all_five_v3_personas():
    r = compute_sizing(500_000)
    slugs = {p["slug"] for p in r["portfolios"]}
    assert slugs == {
        "daily-trader", "patient-trader", "weekly-trader",
        "monthly-trader", "btst-trader",
    }


def test_persona_names_locked():
    r = compute_sizing(500_000)
    by = {p["slug"]: p["name"] for p in r["portfolios"]}
    assert by["daily-trader"]   == "The Daily Trader"
    assert by["patient-trader"] == "The Patient Trader"
    assert by["weekly-trader"]  == "The Weekly Trader"
    assert by["monthly-trader"] == "The Monthly Trader"
    assert by["btst-trader"]    == "The BTST Trader"


def test_risk_tiers_locked():
    r = compute_sizing(500_000)
    by = {p["slug"]: p["risk_tier"] for p in r["portfolios"]}
    assert by["daily-trader"]   == "Medium"
    assert by["patient-trader"] == "Low-Medium"
    assert by["weekly-trader"]  == "Medium-High"
    assert by["monthly-trader"] == "Lowest"
    assert by["btst-trader"]    == "Highest"


def test_per_trade_size_at_default_capital_matches_locked():
    """At the ₹5 L audit base, per-trade size = the locked fixed value."""
    r = compute_sizing(VIRTUAL_CAPITAL_START)
    by = {p["slug"]: p["per_trade_size_rs"] for p in r["portfolios"]}
    assert by["daily-trader"]   == 35_000
    assert by["patient-trader"] == 35_000
    assert by["weekly-trader"]  == 50_000
    assert by["monthly-trader"] == 35_700      # ₹35,714 rounded to ₹100
    assert by["btst-trader"]    == 33_300      # ₹33,333 rounded to ₹100


def test_per_trade_size_scales_linearly():
    """₹10 L → 2× the ₹5 L per-trade size."""
    r = compute_sizing(1_000_000)
    daily = next(p for p in r["portfolios"] if p["slug"] == "daily-trader")
    assert daily["per_trade_size_rs"] == 70_000   # 35,000 × 2


def test_avg_yearly_return_rs_scales_with_capital():
    """₹1 L user → 1% of the locked ₹5 L scaling."""
    r = compute_sizing(100_000)
    daily = next(p for p in r["portfolios"] if p["slug"] == "daily-trader")
    # Daily Trader avg yearly = +207%. At ₹1 L, expected = ₹2,07,000
    assert daily["avg_yearly_return_pct"] == 207.0
    assert daily["avg_yearly_return_rs"]  == 207_000


def test_worst_year_negative_and_scaled():
    """Daily Trader's worst year is -23% (2021). Should be negative + rupee-scaled."""
    r = compute_sizing(1_000_000)
    daily = next(p for p in r["portfolios"] if p["slug"] == "daily-trader")
    assert daily["worst_year_pct"] == -23.0
    assert daily["worst_year_rs"]  == -230_000


def test_monthly_trader_never_lost_a_year():
    """Operator-locked: P4 is the only persona with 6/6 positive years."""
    r = compute_sizing(500_000)
    monthly = next(p for p in r["portfolios"] if p["slug"] == "monthly-trader")
    assert monthly["positive_years"]   == "6 of 6"
    assert monthly["worst_year_pct"]   >= 0           # never negative
    assert monthly["risk_tier"]        == "Lowest"


def test_btst_carries_disclosure_flag():
    """P5 BTST must carry has_disclosure=True so the UI shows the warning."""
    r = compute_sizing(500_000)
    btst = next(p for p in r["portfolios"] if p["slug"] == "btst-trader")
    assert btst["has_disclosure"]    is True
    assert btst["worst_year_pct"]    == -54.0
    assert btst["risk_tier"]         == "Highest"
    assert btst["avg_yearly_return_pct"] == 306.0


def test_capital_below_floor_is_clamped():
    r = compute_sizing(100)
    assert r["clamped"] is True
    assert r["user_capital_rs"] == MIN_CAPITAL_RS


def test_capital_above_ceiling_is_clamped():
    r = compute_sizing(10_000_000_000)
    assert r["clamped"] is True
    assert r["user_capital_rs"] == MAX_CAPITAL_RS


def test_capital_within_band_is_not_clamped():
    r = compute_sizing(2_500_000)
    assert r["clamped"] is False
    assert r["user_capital_rs"] == 2_500_000


def test_zero_capital_does_not_crash():
    r = compute_sizing(0)
    assert r["clamped"] is True
    assert r["user_capital_rs"] == MIN_CAPITAL_RS
