"""RMS CAP 4 (partial) — surface the silent MIS/MTF margin fallback + validate
the new RMS config fields.

Only the margin-fallback SURFACING sub-item of CAP 4 is delivered here (the
concentration / fat-finger / max-notional clamp + the risk_basis label are
flagged staged-next). Proves a margin-sized (MTF) session that cash-falls-back
(margin API returns nothing) surfaces an amber warning in the start response
instead of silently shrinking the deployment.

MUTATION-VERIFIED (test_mtf_margin_fallback_surfaces_warning):
  Revert = delete the `if margin_fallback_warnings: _ok["margin_fallback_warnings"]
  = margin_fallback_warnings` line (and/or the detection block) in
  session._fire_entries. Then no warning is returned → this test's assert fails.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def test_mtf_margin_fallback_surfaces_warning(clean_positions, monkeypatch):
    ltps = {"A": 100.0, "B": 200.0}

    def fake_build_client(profile, dry_run=True):
        # MTF product but NO margins reported → sizing cash-falls-back.
        return MockBroker(profile=profile, dry_run=False, ltps=ltps,
                          margins={}, available_margin=None)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=400000.0, top_n_stocks=2,
                               sizing_mode="equal", order_product="MTF",
                               instrument_type="EQ", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    warns = res.get("margin_fallback_warnings")
    assert warns, "expected an amber margin-fallback warning"
    assert {w["symbol"] for w in warns} == {"A", "B"}
    assert all(w["product"] == "MTF" for w in warns)


def test_new_rms_config_validates():
    # max_daily_loss_pct must be a fraction in (0, 0.5].
    with pytest.raises(ValueError):
        TradingSessionConfig(total_allocated_capital=100000.0,
                             max_daily_loss_pct=1.0).validate()
    with pytest.raises(ValueError):
        TradingSessionConfig(total_allocated_capital=100000.0,
                             max_daily_loss_amount=-5.0).validate()
    # Valid values round-trip through JSON.
    cfg = TradingSessionConfig(total_allocated_capital=100000.0,
                               max_daily_loss_pct=0.05,
                               max_daily_loss_amount=25000.0,
                               mis_protective_slm_enabled=False)
    cfg.validate()
    cfg2 = TradingSessionConfig.from_json(cfg.to_json())
    assert cfg2.max_daily_loss_pct == pytest.approx(0.05)
    assert cfg2.max_daily_loss_amount == pytest.approx(25000.0)
    assert cfg2.mis_protective_slm_enabled is False
    # Defaults keep the breaker OFF + SL-M ON.
    d = TradingSessionConfig(total_allocated_capital=100000.0)
    assert d.max_daily_loss_pct is None
    assert d.max_daily_loss_amount is None
    assert d.mis_protective_slm_enabled is True
