"""Tests for PortfolioMonitor — Fix 1: compute_gross_return_invested / compute_gross_return
now include realised P&L from CLOSED positions.

Covers:
  test_gross_return_includes_realised_invested
      — session with 1 open position (-100 uPnL) + 1 CLOSED position
        (-200 realised) → gross_return_invested = -300 / basis (NOT -100/basis)

  test_gross_return_with_no_closed_invested
      — open-only session: result unchanged from before the fix

  test_gross_return_realised_win_offsets_open_loss_invested
      — closed +300 realised + open -100 uPnL = net +200/basis

  test_gross_return_fund_includes_realised
      — same logic for compute_gross_return() (on-fund view)

  test_gross_return_fund_with_no_closed
      — on-fund, open-only: still works correctly

  test_total_realised_returns_zero_when_no_closed
      — _total_realised() returns 0.0 when no CLOSED rows

  test_total_realised_skips_null_realised_pnl
      — CLOSED rows with NULL realised_pnl are excluded from the sum

  test_invested_basis_frozen_does_not_shrink_on_close_regression
      — the basis denominator (frozen at entry) must not shrink when
        a position is closed (regression from the original design)
"""
from __future__ import annotations

import uuid

import pytest

from autotrade.monitoring.monitor import PortfolioMonitor
from autotrade.monitoring.registry import PositionRegistry


# ── helpers ───────────────────────────────────────────────────────────────────

def _sid():
    return uuid.uuid4().hex


def _make_session(sid, cap=500_000.0, invested_basis=None):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                invested_basis, config_json)
               VALUES (?,?,?,?,?,?,?)""",
            (sid, "2026-06-28T09:00:00", "RUNNING", "paper", cap,
             invested_basis, "{}"))
        con.commit()


def _register_open(sid, cap, symbol, qty, avg, ltp):
    reg = PositionRegistry(sid, cap)
    reg.register(symbol=symbol, broker_profile="zer", qty=qty, avg_price=avg)
    reg.update_ltp(symbol, ltp)
    return reg


def _force_close_with_realised(sid, symbol, realised_pnl, exit_price=None):
    """Directly set a position row to CLOSED with a known realised_pnl value."""
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """UPDATE autotrade_positions
               SET status='CLOSED', realised_pnl=?, exit_price=?, close_reason='GTT'
               WHERE session_id=? AND symbol=?""",
            (realised_pnl, exit_price, sid, symbol))
        con.commit()


# ── Fix 1 tests: compute_gross_return_invested ─────────────────────────────────

def test_gross_return_includes_realised_invested(clean_positions):
    """Open -100 uPnL + CLOSED -200 realised → invested return = -300/basis."""
    sid = _sid()
    cap = 100_000.0
    basis = 50_000.0  # frozen at entry
    _make_session(sid, cap, invested_basis=basis)

    # Open position: ltp below avg → -100 unrealised.
    reg = _register_open(sid, cap, "STOCK_A", qty=10, avg=110.0, ltp=100.0)
    # ltp=100, avg=110, qty=10 → uPnL = (100-110)*10 = -100

    # Closed position with -200 realised.
    reg.register(symbol="STOCK_B", broker_profile="zer", qty=20, avg_price=50.0)
    _force_close_with_realised(sid, "STOCK_B", realised_pnl=-200.0, exit_price=40.0)

    mon = PortfolioMonitor(sid, cap)
    gr = mon.compute_gross_return_invested()
    expected = (-100.0 + -200.0) / basis  # -300 / 50000 = -0.006
    assert abs(gr - expected) < 1e-9, f"Expected {expected:.6f}, got {gr:.6f}"


def test_gross_return_with_no_closed_invested(clean_positions):
    """Open-only session: compute_gross_return_invested unaffected by the fix."""
    sid = _sid()
    cap = 100_000.0
    basis = 50_000.0
    _make_session(sid, cap, invested_basis=basis)

    _register_open(sid, cap, "STOCK_A", qty=10, avg=100.0, ltp=110.0)
    # uPnL = (110-100)*10 = +100

    mon = PortfolioMonitor(sid, cap)
    gr = mon.compute_gross_return_invested()
    expected = 100.0 / basis  # 0.002
    assert abs(gr - expected) < 1e-9, f"Expected {expected:.6f}, got {gr:.6f}"


def test_gross_return_realised_win_offsets_open_loss_invested(clean_positions):
    """Closed +300 realised + open -100 uPnL → net +200 / basis."""
    sid = _sid()
    cap = 100_000.0
    basis = 40_000.0
    _make_session(sid, cap, invested_basis=basis)

    # Open: -100 uPnL.
    reg = _register_open(sid, cap, "LOSER", qty=10, avg=110.0, ltp=100.0)
    # Closed: +300 realised (winner exited by GTT).
    reg.register(symbol="WINNER", broker_profile="zer", qty=5, avg_price=100.0)
    _force_close_with_realised(sid, "WINNER", realised_pnl=300.0, exit_price=160.0)

    mon = PortfolioMonitor(sid, cap)
    gr = mon.compute_gross_return_invested()
    expected = (-100.0 + 300.0) / basis  # +200 / 40000 = +0.005
    assert abs(gr - expected) < 1e-9, f"Expected {expected:.6f}, got {gr:.6f}"


# ── Fix 1 tests: compute_gross_return (on-fund) ───────────────────────────────

def test_gross_return_fund_includes_realised(clean_positions):
    """compute_gross_return() (on-fund view) also includes realised P&L."""
    sid = _sid()
    cap = 100_000.0
    _make_session(sid, cap, invested_basis=50_000.0)

    reg = _register_open(sid, cap, "STOCK_A", qty=10, avg=110.0, ltp=100.0)  # -100 uPnL
    reg.register(symbol="STOCK_B", broker_profile="zer", qty=20, avg_price=50.0)
    _force_close_with_realised(sid, "STOCK_B", realised_pnl=-200.0)

    mon = PortfolioMonitor(sid, cap)
    gr_fund = mon.compute_gross_return()
    expected = (-100.0 + -200.0) / cap  # -300 / 100000 = -0.003
    assert abs(gr_fund - expected) < 1e-9, f"Expected {expected:.6f}, got {gr_fund:.6f}"


def test_gross_return_fund_with_no_closed(clean_positions):
    """On-fund view: open-only session still works correctly."""
    sid = _sid()
    cap = 200_000.0
    _make_session(sid, cap)

    _register_open(sid, cap, "ALPHA", qty=100, avg=50.0, ltp=55.0)
    # uPnL = (55-50)*100 = +500

    mon = PortfolioMonitor(sid, cap)
    gr_fund = mon.compute_gross_return()
    expected = 500.0 / cap  # 0.0025
    assert abs(gr_fund - expected) < 1e-9


# ── _total_realised unit tests ─────────────────────────────────────────────────

def test_total_realised_returns_zero_when_no_closed(clean_positions):
    """_total_realised() = 0.0 when no CLOSED rows exist."""
    sid = _sid()
    _make_session(sid, 100_000.0)
    _register_open(sid, 100_000.0, "ONLY_OPEN", qty=10, avg=100.0, ltp=100.0)

    mon = PortfolioMonitor(sid, 100_000.0)
    assert mon._total_realised() == 0.0


def test_total_realised_skips_null_realised_pnl(clean_positions):
    """CLOSED rows with NULL realised_pnl are excluded (COALESCE guard)."""
    sid = _sid()
    _make_session(sid, 100_000.0)
    reg = PositionRegistry(sid, 100_000.0)
    reg.register(symbol="NULLROW", broker_profile="zer", qty=5, avg_price=100.0)
    # Close it WITHOUT setting realised_pnl (left NULL).
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_positions SET status='CLOSED', realised_pnl=NULL "
            "WHERE session_id=? AND symbol=?", (sid, "NULLROW"))
        con.commit()

    mon = PortfolioMonitor(sid, 100_000.0)
    assert mon._total_realised() == 0.0


def test_total_realised_sums_multiple_closed_positions(clean_positions):
    """_total_realised() sums realised_pnl across multiple CLOSED rows."""
    sid = _sid()
    _make_session(sid, 100_000.0)
    reg = PositionRegistry(sid, 100_000.0)
    for sym, realised in [("P1", -100.0), ("P2", -200.0), ("P3", 50.0)]:
        reg.register(symbol=sym, broker_profile="zer", qty=10, avg_price=100.0)
        _force_close_with_realised(sid, sym, realised_pnl=realised)

    mon = PortfolioMonitor(sid, 100_000.0)
    assert abs(mon._total_realised() - (-100.0 + -200.0 + 50.0)) < 1e-9


# ── Regression: invested_basis denominator stays frozen after close ────────────

def test_invested_basis_frozen_does_not_shrink_on_close_regression(clean_positions):
    """The BASIS denominator (invested_basis) must NOT shrink when a position
    is closed — it is frozen at entry. Closing half the positions must NOT
    change the denominator even though it reduces the numerator."""
    sid = _sid()
    cap = 500_000.0
    basis = 20_000.0  # frozen
    _make_session(sid, cap, invested_basis=basis)

    reg = PositionRegistry(sid, cap)
    # Two positions, each contributing 10k to invested_basis.
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg.update_ltp("A", 100.0)  # flat, uPnL=0
    reg.register(symbol="B", broker_profile="zer", qty=50, avg_price=200.0)
    reg.update_ltp("B", 200.0)  # flat, uPnL=0

    mon = PortfolioMonitor(sid, cap)
    assert abs(mon.invested_basis() - basis) < 1e-6, "basis must be frozen at 20k"

    # Close B with a loss.
    _force_close_with_realised(sid, "B", realised_pnl=-500.0)

    # Basis must still be 20k (frozen).
    assert abs(mon.invested_basis() - basis) < 1e-6, (
        "invested_basis must not shrink after a position closes")

    # But gross_return_invested MUST now include the realised loss.
    gr = mon.compute_gross_return_invested()
    expected = (0.0 + -500.0) / basis  # open A flat + closed B -500
    assert abs(gr - expected) < 1e-9, (
        f"gross_return_invested should be {expected:.4f}, got {gr:.4f}")
