"""Position Sizing Assistant — V3-locked semantics (2026-05-16).

Given a user's capital, projects what each persona would look like for them.
V3 lock changed the capital model: every persona now sizes at FIXED rupees per
trade at the audited ₹5 L base. For a user with capital ≠ ₹5 L, we scale that
fixed size proportionally so the strategy keeps the same ~per-trade-allocation %.

Per-trade size at user capital U:
    per_trade_rs(U) = fixed_per_trade_rs × (U / VIRTUAL_CAPITAL_START)

That preserves both:
  1. The audited "no compounding within year" semantic (size doesn't drift
     with running equity)
  2. The user's intuition that doubling capital ≈ doubling per-trade size

All numbers projected here come from the operator-locked backtest_metrics in
portfolio_defs.py — never invented at request time.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List

from .portfolio_defs import (
    ALL_PORTFOLIOS,
    PortfolioDef,
    VIRTUAL_CAPITAL_START,
)


MIN_CAPITAL_RS = 10_000        # floor; below this most personas can't open a single position
MAX_CAPITAL_RS = 100_000_000   # ceiling; sanity guard on the public input


def compute_sizing(user_capital_rs: float) -> Dict[str, Any]:
    """Return sizing projections for ALL 5 personas at this capital."""
    cap = max(MIN_CAPITAL_RS, min(MAX_CAPITAL_RS, float(user_capital_rs or 0)))
    portfolios: List[Dict[str, Any]] = []
    for p in ALL_PORTFOLIOS:
        portfolios.append(_for_portfolio(p, cap))
    return {
        "user_capital_rs":      cap,
        "user_capital_rs_input": user_capital_rs,
        "clamped":              cap != float(user_capital_rs or 0),
        "min_capital_rs":       MIN_CAPITAL_RS,
        "max_capital_rs":       MAX_CAPITAL_RS,
        "portfolios":           portfolios,
    }


def _for_portfolio(p: PortfolioDef, capital: float) -> Dict[str, Any]:
    pars = p.params
    metrics = p.backtest_metrics

    # ── Fixed-₹ sizing, scaled linearly off the audited ₹5 L base
    scale         = capital / VIRTUAL_CAPITAL_START
    per_trade_rs  = pars.fixed_per_trade_rs * scale

    # Expected number of OPEN positions at any time (steady-state)
    if pars.entry_cadence == "daily":
        # Daily personas: top_n × hold_days fits the budget on average. Cap at
        # whatever per-trade × top_n × hold_days the user's capital supports.
        expected_n_open = min(pars.top_n * pars.hold_days_max,
                               int(capital // max(1.0, per_trade_rs)))
        # But never more than top_n × hold_days (the structural max)
        expected_n_open = min(expected_n_open, pars.top_n * pars.hold_days_max)
    elif pars.entry_cadence == "tuesday_only":
        expected_n_open = pars.top_n         # held one week each
    elif pars.entry_cadence == "first_of_month":
        expected_n_open = pars.top_n         # held ~22 days then cash
    else:
        expected_n_open = pars.top_n

    deployed_rs  = min(capital, expected_n_open * per_trade_rs)
    deployed_pct = (deployed_rs / capital) * 100.0 if capital > 0 else 0.0

    # ── Monthly P&L range — scale yearly figures down to monthly equivalent
    # (operator's locked monthly_pnl_range isn't in V3 metrics; derive from worst/best year)
    worst_year_pct = metrics.get("worst_year_pct", 0)
    best_year_pct  = metrics.get("best_year_pct",  0)
    # Roughly: monthly range ≈ yearly / 6 for ballpark display. We surface the
    # yearly figures directly because they're the audited number.
    avg_yearly_pct  = metrics.get("avg_yearly_return_pct", 0)
    avg_yearly_rs   = capital * (avg_yearly_pct / 100.0)

    # "Worst stretch in ₹" — operator's worst_year_pct × user capital
    worst_year_rs   = capital * (worst_year_pct / 100.0)
    best_year_rs    = capital * (best_year_pct  / 100.0)

    avg_trades_per_year = metrics.get("trades_per_year", 0)
    avg_trades_per_month = int(round(avg_trades_per_year / 12.0)) if avg_trades_per_year else 0

    return {
        "slug":                       p.slug,
        "name":                       p.name,
        "tagline":                    p.tagline,
        "persona_number":             p.persona_number,
        "risk_tier":                  p.risk_tier,
        "risk_tier_color":            p.risk_tier_color,
        "per_trade_size_rs":          _round_to(per_trade_rs, 100),
        "expected_n_positions":       expected_n_open,
        "invested_right_now_rs":      _round_to(deployed_rs, 100),
        "invested_right_now_pct":     round(deployed_pct, 1),
        "avg_yearly_return_pct":      avg_yearly_pct,
        "avg_yearly_return_rs":       _round_to(avg_yearly_rs, 100),
        "best_year_pct":              best_year_pct,
        "best_year_rs":               _round_to(best_year_rs, 100),
        "best_year_label":            metrics.get("best_year_label"),
        "worst_year_pct":             worst_year_pct,
        "worst_year_rs":              _round_to(worst_year_rs, 100),
        "worst_year_label":           metrics.get("worst_year_label"),
        "positive_years":             metrics.get("positive_years"),
        "win_rate_pct":               metrics.get("win_rate_pct"),
        "avg_max_drawdown_pct":       metrics.get("avg_max_drawdown_pct"),
        "avg_trades_per_year":        avg_trades_per_year,
        "avg_trades_per_month":       avg_trades_per_month,
        "backtest_window":            p.backtest_window,
        "has_disclosure":             p.disclosure is not None,
    }


def _round_to(value: float, step: int) -> int:
    if value == 0:
        return 0
    return int(math.copysign(round(abs(value) / step) * step, value))
