"""End-to-end DRY-RUN session smoke + addendum full-day style sequence.

Patches broker.router.build_client to return MockBrokers so no real Kite is
touched, then drives create → start → tick → kill entirely in paper mode.
"""
import asyncio

import pytest

import time

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession
from autotrade import exit_gate
from autotrade.monitoring import tick_driver
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


def _falcon_rows_for_session(session_id):
    """Count any falcon_position_state rows referencing this autotrade session.
    Returns (total_rows_in_table, rows_linked_to_session)."""
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        total = con.execute(
            "SELECT COUNT(*) FROM falcon_position_state").fetchone()[0]
        cols = [r[1] for r in con.execute(
            "PRAGMA table_info(falcon_position_state)")]
        linked = 0
        if "session_id" in cols:
            linked = con.execute(
                "SELECT COUNT(*) FROM falcon_position_state WHERE session_id=?",
                (session_id,)).fetchone()[0]
    return total, linked


@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    # SHARED ltps dict so every broker built (incl. the background tick driver's
    # independently-loaded session) sees the same live prices. set_ltp on any
    # broker mutates this shared dict.
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0, "D": 150.0, "E": 300.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    # session.py imported build_client by name → patch there too.
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_full_paper_session(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0), ("D", 4, 6.0, 150.0),
                  ("E", 5, 5.0, 300.0)])
    cfg = TradingSessionConfig(total_allocated_capital=500000.0, top_n_stocks=5,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start())
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 5
    st = sess.status()
    assert st["n_open_positions"] == 5
    assert st["total_allocated_capital"] == 500000.0


def test_paper_kill_switch_fires_on_profit(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.005, kill_switch_direction="profit")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # Move LTPs up so gross return > 0.5%.
    for mb in patched_brokers.values():
        mb.set_ltp("A", 120.0)
        mb.set_ltp("B", 240.0)
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    # After kill, positions marked exited.
    assert sess.status()["n_open_positions"] == 0


def test_manual_kill(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    res = asyncio.run(sess.kill(reason="OPERATOR"))
    assert res["n_positions"] >= 1
    assert sess.status()["n_open_positions"] == 0


def test_trailing_stop_then_kill_denominator(clean_positions, patched_brokers):
    """Addendum: trailing stop on one stock, kill switch keeps monitoring the
    reduced portfolio; denominator stays total_allocated_capital."""
    seed_signals([("A", 1, 9, 100.0), ("B", 2, 8, 200.0), ("C", 3, 7, 50.0),
                  ("D", 4, 6, 150.0), ("E", 5, 5, 300.0)])
    cap = 500000.0
    cfg = TradingSessionConfig(total_allocated_capital=cap, top_n_stocks=5,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.5)  # high → won't auto-fire
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # Simulate a trailing-stop exit on C via the shared exit gate path.
    assert exit_gate.claim_exit_session(sess.session_id, "C", "TRAILING_STOP") is True
    sess.registry.mark_closed("C", "TRAILING_STOP")
    # Denominator unchanged.
    assert sess.monitor.total_allocated_capital == cap
    assert sess.status()["n_open_positions"] == 4


# ── REGRESSION TEST FOR THE BUG ──────────────────────────────────────────────
# A paper session must write ONLY to autotrade_positions and leave
# falcon_position_state COMPLETELY UNTOUCHED. This is the exact bug that
# overwrote real held positions in the prod smoke test.

def test_session_does_not_touch_falcon_position_state(clean_positions,
                                                      patched_brokers):
    from falcon.db import falcon_conn
    # Seed a REAL existing Falcon swing position for one of the same symbols.
    # If the bug regressed, the paper session would overwrite this row.
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO falcon_position_state
               (symbol, managed_by, product, qty, avg_entry, initial_sl_price,
                current_sl_price, target_price, high_water_price, entry_date)
               VALUES ('A','falcon','CNC', 999, 55.5, 50, 50, 70, 60, '2026-06-01')""")
        con.commit()

    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())

    # All session positions landed in autotrade_positions.
    with falcon_conn() as con:
        at_rows = con.execute(
            "SELECT symbol, qty, avg_price FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchall()
    assert len(at_rows) == 3
    at_map = {r["symbol"]: dict(r) for r in at_rows}
    assert set(at_map) == {"A", "B", "C"}

    # The pre-existing Falcon swing row is UNCHANGED — qty/avg_entry intact.
    with falcon_conn() as con:
        fp = con.execute(
            "SELECT symbol, qty, avg_entry, managed_by FROM falcon_position_state "
            "WHERE symbol='A'").fetchone()
    assert fp is not None
    assert fp["qty"] == 999            # NOT overwritten by the session's qty
    assert abs(fp["avg_entry"] - 55.5) < 1e-9
    assert fp["managed_by"] == "falcon"

    # ZERO falcon_position_state rows reference this session — full isolation.
    _, linked = _falcon_rows_for_session(sess.session_id)
    assert linked == 0

    # And the session never CREATED extra falcon_position_state rows.
    with falcon_conn() as con:
        total_fp = con.execute(
            "SELECT COUNT(*) FROM falcon_position_state").fetchone()[0]
    assert total_fp == 1               # only the one pre-existing real position


def test_gross_return_from_isolated_table_frozen_denominator(clean_positions,
                                                             patched_brokers):
    """gross_return is computed from autotrade_positions only, and the
    denominator stays the frozen total_allocated_capital."""
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cap = 100000.0
    cfg = TradingSessionConfig(total_allocated_capital=cap, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    for mb in patched_brokers.values():
        mb.set_ltp("A", 110.0)
        mb.set_ltp("B", 220.0)
    sess.monitor.refresh_ltps(sess.brokers)
    gr = sess.monitor.compute_gross_return()
    # qty A = 50000/100 = 500 → uPnL 5000; qty B = 50000/200 = 250 → uPnL 5000.
    assert abs(gr - (10000.0 / cap)) < 1e-9
    assert sess.monitor.total_allocated_capital == cap
    # Denominator frozen even after closing a position. Realised P&L stays in the
    # numerator (73619c4), so closing A at its LTP leaves the return unchanged.
    sess.registry.mark_closed("A", "TRAILING_STOP")
    assert sess.monitor.total_allocated_capital == cap
    assert abs(sess.monitor.compute_gross_return() - gr) < 1e-9


def test_kill_switch_autofires_via_tick_driver(clean_positions, patched_brokers):
    """The background tick driver refreshes ltp + gross_return and AUTO-fires the
    kill switch when the threshold is crossed (paper — no real orders)."""
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.005, kill_switch_direction="profit")
    sess = TradingSession.create(cfg, mode="paper")

    # Enable autostart + a fast interval for this test, then drive prices up so
    # the driver's own next tick crosses the threshold and fires.
    import os
    tick_driver.set_autostart(True)
    os.environ["FALCON_AUTOTRADE_TICK_INTERVAL"] = "0.2"
    try:
        asyncio.run(sess.start())
        assert tick_driver.is_running(sess.session_id)
        # The driver loads its OWN session/brokers; patch all brokers it created.
        for mb in patched_brokers.values():
            mb.set_ltp("A", 130.0)
            mb.set_ltp("B", 260.0)
        # Wait for the driver to tick + fire (interval default 5s; poll status).
        #
        # SYNCHRONISATION (do NOT assume write-ordering): kill_switch.fire()
        # writes status=CLOSED (kill_switch.py:658) and only THEN INSERTs the
        # audit row via _log_fire (kill_switch.py:670 → :751) in a SEPARATE
        # transaction, and only then returns so the driver thread can break +
        # deregister. So `status==CLOSED` is NOT evidence that the log row exists
        # nor that the driver has stopped — it is observed MID-sequence. Reading
        # either one immediately after seeing CLOSED is a race the driver thread
        # loses whenever it is descheduled in that window (proven by widening it:
        # CLOSED observed with kill_switch_log rows = 0). Under a full-suite run
        # the extra thread/GC contention makes that window hit occasionally —
        # which is a TEST race, not a kill-switch bug (the flatten itself had
        # already completed: n_exited_ok=2).
        #
        # Each condition below is therefore polled to its OWN deadline. The
        # assertions are UNCHANGED in strength: the kill MUST fire, the audit row
        # MUST be written, and the driver MUST self-stop — we just wait for each
        # rather than assuming one implies the others.
        from falcon.db import falcon_conn

        def _wait_for(predicate, timeout=12.0, interval=0.05):
            deadline = time.time() + timeout
            while time.time() < deadline:
                if predicate():
                    return True
                time.sleep(interval)
            return predicate()

        def _status():
            with falcon_conn() as con:
                row = con.execute(
                    "SELECT status FROM autotrade_sessions WHERE session_id=?",
                    (sess.session_id,)).fetchone()
            return row["status"] if row else None

        def _n_kill_log():
            with falcon_conn() as con:
                return con.execute(
                    "SELECT COUNT(*) FROM autotrade_kill_switch_log "
                    "WHERE session_id=?", (sess.session_id,)).fetchone()[0]

        assert _wait_for(lambda: _status() == "CLOSED"), (
            "kill switch did not auto-fire via the tick driver "
            "(status=%s)" % _status())
        # A kill-switch fire was logged for this session (written just AFTER the
        # CLOSED status → wait for it rather than reading it immediately).
        assert _wait_for(lambda: _n_kill_log() >= 1), (
            "kill switch fired but no autotrade_kill_switch_log row was written")
        # Driver self-stopped (session no longer RUNNING). It deregisters only
        # after fire() returns → poll rather than sleeping a fixed 0.5s.
        assert _wait_for(lambda: not tick_driver.is_running(sess.session_id)), (
            "tick driver did not self-stop after the kill switch fired")
    finally:
        tick_driver.set_autostart(False)
        tick_driver.stop_for_session(sess.session_id)
        os.environ.pop("FALCON_AUTOTRADE_TICK_INTERVAL", None)
