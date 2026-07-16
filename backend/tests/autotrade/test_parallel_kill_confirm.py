"""R2 — kill-switch fill confirmations run CONCURRENTLY, not serially.

Each confirm_exit can block up to 60s; N legs used to confirm one-after-another
(up to N×60s). This asserts the confirms overlap in time (a per-leg delay does
not multiply by N) and that every leg still resolves.
"""
import asyncio
import threading
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


class _Overlap:
    """Directly MEASURES how many confirms are in flight at the same instant.

    The property under test is 'the confirms OVERLAP (are not N-serialized)'.
    This records it as a FACT rather than inferring it from a wall-clock budget:
    max_inflight >= 2 is only reachable if two confirms were genuinely running
    concurrently, and (unlike elapsed time) it cannot be faked or broken by how
    busy the machine is.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.inflight = 0
        self.max_inflight = 0

    def enter(self):
        with self.lock:
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)

    def exit(self):
        with self.lock:
            self.inflight -= 1


class _SlowConfirmBroker(MockBroker):
    overlap: _Overlap = None    # set per-test by _mk

    def get_order_status(self, order_id):
        # A real broker's order poll takes network time; a serial confirm would
        # pay this once per leg. Concurrent confirms overlap it.
        if self.overlap is not None:
            self.overlap.enter()
        try:
            time.sleep(_DELAY)
            return super().get_order_status(order_id)
        finally:
            if self.overlap is not None:
                self.overlap.exit()


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mk(monkeypatch, overlap=None):
    def fake_build_client(profile, dry_run=True):
        b = _SlowConfirmBroker(profile=profile, dry_run=False,
                               ltps={"A": 99.0, "B": 99.0, "C": 99.0,
                                     "D": 99.0})
        b.overlap = overlap
        return b

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
    overlap = _Overlap()
    sess = _mk(monkeypatch, overlap=overlap)
    prof = sess.config.broker_profiles[0].profile_id
    for sym in ("A", "B", "C", "D"):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=100,
                               avg_price=100.0, product="CNC",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.update_ltp(sym, 99.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()

    summary = asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))

    # All 4 legs closed OK.
    assert summary["n_exited_ok"] == 4
    assert summary["n_exit_failed"] == 0
    assert _status(sess) == "CLOSED"
    # The confirms OVERLAPPED (were not N-serialized).
    #
    # This used to be inferred from a wall-clock budget
    # (`elapsed < 2*_DELAY` → 0.6s). That budget covered the WHOLE fire()
    # — order placement, DB writes, the order ledger, the snapshot — not just
    # the confirms, so on a busy machine it blew for reasons unrelated to
    # concurrency and failed the suite (reproduced here: 2/2 loaded full-suite
    # runs red, green in isolation). Serialization is now measured DIRECTLY:
    # max_inflight counts confirms actually running at the same instant, which
    # is exactly the property under test and is immune to machine load.
    # A serial implementation can never exceed 1.
    assert overlap.max_inflight >= 2, (
        "confirms look serialized: max concurrent confirms = "
        f"{overlap.max_inflight} (expected >= 2 of {len(summary['details'])} legs)")
