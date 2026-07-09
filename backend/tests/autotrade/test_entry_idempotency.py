"""C5 — entry firing is IDEMPOTENT.

Two concurrent start()/scheduled-fire invocations (or a double call) must place
orders exactly ONCE. A per-session in-memory claim closes the concurrent window;
a DB check (a session with OPEN positions already fired) is the durable guard.
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
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


@pytest.fixture
def patched(monkeypatch):
    created = {}
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        mb = created.get(profile.profile_id)
        if mb is None:
            mb = MockBroker(profile=profile, dry_run=False, ltps=ltps)
            created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _mk():
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    return TradingSession.create(cfg, mode="paper")


def test_concurrent_fire_places_once(clean_positions, patched):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = _mk()

    async def _race():
        return await asyncio.gather(sess._fire_entries(), sess._fire_entries())

    res = asyncio.run(_race())
    fired = [r for r in res if r.get("n_placed", 0) > 0]
    refused = [r for r in res if r.get("already_fired")]
    assert len(fired) == 1
    assert len(refused) == 1
    assert fired[0]["n_placed"] == 3

    # Every symbol placed exactly once across BOTH brokers.
    all_placed = [o.symbol for mb in patched.values() for o in mb.placed]
    assert sorted(all_placed) == ["A", "B", "C"]
    assert sess.status()["n_open_positions"] == 3


def test_sequential_re_fire_refused(clean_positions, patched):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = _mk()

    first = asyncio.run(sess._fire_entries())
    assert first["n_placed"] == 3
    # Second call: the session already has OPEN positions → refuse, place nothing.
    second = asyncio.run(sess._fire_entries())
    assert second.get("already_fired") is True
    assert second["n_placed"] == 0

    all_placed = [o.symbol for mb in patched.values() for o in mb.placed]
    assert sorted(all_placed) == ["A", "B", "C"]     # not doubled
    assert sess.status()["n_open_positions"] == 3
