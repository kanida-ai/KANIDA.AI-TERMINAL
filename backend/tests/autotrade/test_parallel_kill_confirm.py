"""R2 — kill-switch fill confirmations run CONCURRENTLY, not serially.

Each confirm_exit can block up to 60s; N legs used to confirm one-after-another
(up to N×60s). This asserts the confirms overlap in time (a per-leg delay does
not multiply by N) and that every leg still resolves.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)
_DELAY = 0.3


class _SlowConfirmBroker(MockBroker):
    def get_order_status(self, order_id):
        # A real broker's order poll takes network time; a serial confirm would
        # pay this once per leg. Concurrent confirms overlap it.
        time.sleep(_DELAY)
        return super().get_order_status(order_id)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mk(monkeypatch):
    def fake_build_client(profile, dry_run=True):
        return _SlowConfirmBroker(profile=profile, dry_run=False,
                                  ltps={"A": 99.0, "B": 99.0, "C": 99.0,
                                        "D": 99.0})

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=400000.0, top_n_stocks=4,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.02, order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _status(sess):
    return sess._current_status()


def test_confirms_run_concurrently(clean_positions, monkeypatch):
    sess = _mk(monkeypatch)
    prof = sess.config.broker_profiles[0].profile_id
    for sym in ("A", "B", "C", "D"):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=100,
                               avg_price=100.0, product="CNC",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.update_ltp(sym, 99.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()

    t0 = time.monotonic()
    summary = asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))
    elapsed = time.monotonic() - t0

    # All 4 legs closed OK.
    assert summary["n_exited_ok"] == 4
    assert summary["n_exit_failed"] == 0
    assert _status(sess) == "CLOSED"
    # 4 legs × 0.3s serial ≈ 1.2s; concurrent ≈ 0.3s. Anything under 2×_DELAY
    # proves the confirms overlapped (not N-serialized).
    assert elapsed < 2 * _DELAY, f"confirms look serialized: {elapsed:.2f}s"
