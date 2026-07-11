"""SPRINT CLUSTER 8 ITEM 5 — surface NET P&L (gross − estimated charges) in the
live panel / decision surface (status()).

The panel gross return + the trail/kill numerators are GROSS. status() now ALSO
carries a NET figure (gross − an estimated round-trip charge) so the user sees the
real number. The DECISION basis is unchanged (still gross invested-basis).

Each test PASSES with the fix and FAILS on the stated revert. Paper-safe.
"""
from autotrade.config import TradingSessionConfig
from autotrade.session import (TradingSession, estimate_session_charges_rs,
                               set_fake_now)
from datetime import datetime, timedelta, timezone
import pytest

IST = timezone(timedelta(hours=5, minutes=30))


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST))
    yield
    set_fake_now(None)


def _cfg(**kw):
    base = dict(total_allocated_capital=1_000_000.0, top_n_stocks=1,
                order_product="MIS", kill_switch_enabled=False)
    base.update(kw)
    return TradingSessionConfig(**base)


def test_status_surfaces_net_pnl(clean_positions):
    """status() carries gross_pnl, estimated_charges, net_pnl and net_return; net_pnl
    == gross_pnl − estimated_charges, and charges are non-zero (MIS) so net < gross.

    MUTATION REVERT: remove the `net_pnl` (and `estimated_charges`) keys from the
    status() out dict → the `'net_pnl' in out` / arithmetic asserts FAIL."""
    sess = TradingSession.create(_cfg(), mode="paper")
    prof = sess.config.broker_profiles[0].profile_id if sess.config.broker_profiles \
        else "zer"
    sess.registry.register(symbol="A", broker_profile=prof, qty=1000,
                           avg_price=100.0, product="MIS", instrument_type="EQ")
    sess.registry.update_ltp("A", 110.0, broker_profile=prof)   # +₹10,000 gross
    sess.monitor.freeze_invested_basis()

    out = sess.status()
    assert "gross_pnl" in out and "net_pnl" in out
    assert "estimated_charges" in out and "net_return" in out
    assert out["gross_pnl"] == pytest.approx(10_000.0, abs=1.0)
    assert out["estimated_charges"] > 0                    # MIS has real charges
    assert out["net_pnl"] == pytest.approx(
        round(out["gross_pnl"] - out["estimated_charges"], 2), abs=0.01)
    assert out["net_pnl"] < out["gross_pnl"]               # net is below gross


def test_estimate_charges_helper_floor_and_safe(clean_positions):
    """The charge estimator sums a positive round-trip charge for an open position
    and never raises for an empty session (returns 0.0)."""
    assert estimate_session_charges_rs("no-such-session", "CNC") == 0.0
    sess = TradingSession.create(_cfg(order_product="MIS"), mode="paper")
    prof = sess.config.broker_profiles[0].profile_id if sess.config.broker_profiles \
        else "zer"
    sess.registry.register(symbol="B", broker_profile=prof, qty=500, avg_price=200.0,
                           product="MIS", instrument_type="EQ")
    sess.registry.update_ltp("B", 205.0, broker_profile=prof)
    assert estimate_session_charges_rs(sess.session_id, "MIS") > 0
