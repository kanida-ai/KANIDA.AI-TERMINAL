"""Two REAL-MONEY exit-path fixes surfaced in live trading 2026-07-10.

FIX A — per-stock capital-slice must NOT inflate as siblings exit. The per-stock
step-lock slice denominator was Σ(qty*avg) over CURRENTLY-OPEN positions, which
SHRINKS as names close, so survivors' slice_cap INFLATED and their
-per_stock_stop_pct stop fired at a progressively larger rupee loss (the last name
standing got a slice of the whole session capital). The fix freezes the ENTRY
basket notional once (entry_basket_notional) and uses it as a CONSTANT denominator.

FIX B — a broker connection error mid buy-to-cover + a re-entrant same-reason exit
claim from BOTH drivers caused a DOUBLE buy-to-cover → a NAKED long. Two parts:
  B1: when the pre-exit position probe RAISES, ABORT (no blind order), leg stays
      OPEN for retry — never place an exit without a successful position read.
  B2: single-flight ORDER PLACEMENT per (session, symbol) — a second concurrent
      claim (even same reason, even the other driver) is a NO-OP for placement.

All paper-safe (MockBroker, no real orders).
"""
import asyncio
import threading

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade import exit_gate
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, _exit_single_position
from falcon.db import falcon_conn
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


# ── shared broker factory with a MUTABLE ltp map + optional probe-raise ────────
def _install_brokers(monkeypatch, shared_ltps, *, raise_symbols=None,
                     exit_delay_sec=0.0):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps,
                        net_probe_raise_symbols=set(raise_symbols or ()),
                        exit_delay_sec=exit_delay_sec)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _cfg(cap, *, per_stock_stop_pct=0.03, stop_pct=0.10, top_n=3):
    # Basket catastrophic stop DELIBERATELY wide (0.10) so ONLY the per-stock
    # capital stop (per_stock_stop_pct) can act in these tests — the basket-
    # aggregate stop must stay silent while a single name is cut.
    return TradingSessionConfig(
        total_allocated_capital=cap, top_n_stocks=top_n, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, per_stock_stop_enabled=False,
        step_lock_scope="stock", per_stock_stop_pct=per_stock_stop_pct,
        trail_step_lock_enabled=True,
        arm_pct=0.05, floor_pct=0.01, trail_giveback_pct=0.0125,
        stop_pct=stop_pct, square_off_enabled=False,
        square_off_time="15:29:00", entry_time="09:15:00",
        mis_square_off_time="15:12:00")


def _normalise(sid, qty=100, avg=100.0):
    """Force every OPEN leg to qty/avg AND re-freeze the entry basket notional so
    it matches the normalised book (the real freeze happened at entry with whatever
    the allocator sized; these tests normalise afterwards)."""
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_positions SET qty=?, avg_price=? "
            "WHERE session_id=? AND status='OPEN'", (qty, avg, sid))
        total = con.execute(
            "SELECT COALESCE(SUM(qty*avg_price),0) t FROM autotrade_positions "
            "WHERE session_id=? AND status='OPEN'", (sid,)).fetchone()["t"]
        con.execute(
            "UPDATE autotrade_sessions SET entry_basket_notional=? "
            "WHERE session_id=?", (total, sid))
        con.commit()


def _open_count(sid):
    with falcon_conn() as con:
        r = con.execute("SELECT COUNT(*) c FROM autotrade_positions "
                        "WHERE session_id=? AND status='OPEN'", (sid,)).fetchone()
    return r["c"]


def _status(sid, symbol):
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?", (sid, symbol)).fetchone()
    return r["status"] if r else None


def _start(cfg, symbols):
    seed_signals([(s, i + 1, 9.0 - i, 100.0) for i, s in enumerate(symbols)])
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    sid = sess.session_id
    assert _open_count(sid) == len(symbols)
    _normalise(sid)
    return sid


def _tick(sid):
    return asyncio.run(TradingSession.load(sid).tick())


# ═══════════════════════════════════════════════════════════════════════════
# FIX A — the per-stock capital slice is CONSTANT as siblings close.
# ═══════════════════════════════════════════════════════════════════════════

def test_fixA_slice_constant_last_name_stops_at_same_threshold(clean_positions,
                                                               monkeypatch):
    """3 equal legs, slice = 10_000 each (cap 30k / 3). A name's -3% capital stop
    must fire at uPnL -300 (price 97) whether it is the FIRST or the LAST to go.

    Pre-fix, once A+B closed the survivor C's slice inflated to 30_000 (the whole
    capital), so C's g_stock at price 97 was only -0.01 → it would NOT stop. The
    frozen entry_basket_notional keeps C's slice at 10_000 → g_stock -0.03 → stop.
    The exit result's g_stock == -0.03 is the DIRECT proof the slice stayed 10_000.
    """
    ltps = {"A": 100.0, "B": 100.0, "C": 100.0}
    _install_brokers(monkeypatch, ltps)
    sid = _start(_cfg(30_000.0), ["A", "B", "C"])

    # Tick 1: A and B each down -3% of their OWN slice (price 97, uPnL -300) → both
    # STOP_STOCK and close. C flat. Basket G = -600/30000 = -0.02 > -0.10 → the
    # basket stop stays silent (only the per-name stops act).
    ltps["A"] = 97.0
    ltps["B"] = 97.0
    ltps["C"] = 100.0
    r1 = _tick(sid)
    stopped = {e["symbol"]: e["reason"] for e in r1["per_stock_exits"]}
    assert stopped == {"A": "STOP_STOCK", "B": "STOP_STOCK"}
    assert _open_count(sid) == 1                       # only C left

    # Tick 2: C — now the LAST name standing — falls to the SAME 97 (uPnL -300).
    # With the frozen denominator its slice is STILL 10_000 → g_stock -0.03 → STOP.
    ltps["C"] = 97.0
    r2 = _tick(sid)
    exits = {e["symbol"]: e for e in r2["per_stock_exits"]}
    assert set(exits) == {"C"}
    assert exits["C"]["reason"] == "STOP_STOCK"
    # DIRECT constant-slice proof: g_stock is on a 10_000 slice (=-0.03), NOT on the
    # inflated 30_000 whole-capital slice (which would give -0.01 and NO stop).
    assert exits["C"]["g_stock"] == pytest.approx(-0.03, abs=1e-6)
    assert _status(sid, "C") == "CLOSED"
    assert _open_count(sid) == 0


def test_fixA_last_name_does_not_over_fire_below_its_own_stop(clean_positions,
                                                              monkeypatch):
    """No-fire complement: the last survivor down only -2% of its (constant 10_000)
    slice must NOT stop. Pre-fix its slice inflated so g_stock was even smaller and
    it also wouldn't fire — this asserts the FIXED slice does not OVER-fire either."""
    ltps = {"A": 100.0, "B": 100.0, "C": 100.0}
    _install_brokers(monkeypatch, ltps)
    sid = _start(_cfg(30_000.0), ["A", "B", "C"])

    ltps["A"] = 97.0
    ltps["B"] = 97.0
    _tick(sid)
    assert _open_count(sid) == 1

    # C down only -2% of its 10_000 slice (price 98, uPnL -200) → g_stock -0.02 >
    # -0.03 → HOLD, still OPEN as the last name.
    ltps["C"] = 98.0
    r = _tick(sid)
    assert r["per_stock_exits"] == []
    assert _status(sid, "C") == "OPEN"
    assert _open_count(sid) == 1


def test_fixA_freeze_captures_entry_basket_notional(clean_positions, monkeypatch):
    """The freeze wiring: a fresh start() populates entry_basket_notional = Σ qty*avg
    across the filled legs (30_000 for 3×100×100)."""
    ltps = {"A": 100.0, "B": 100.0, "C": 100.0}
    _install_brokers(monkeypatch, ltps)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 100.0), ("C", 3, 7.0, 100.0)])
    sess = TradingSession.create(_cfg(30_000.0), mode="paper")
    asyncio.run(sess.start())
    sid = sess.session_id
    with falcon_conn() as con:
        ebn = con.execute("SELECT entry_basket_notional e FROM autotrade_sessions "
                          "WHERE session_id=?", (sid,)).fetchone()["e"]
    assert ebn == pytest.approx(30_000.0)
    assert sess.monitor.entry_basket_notional() == pytest.approx(30_000.0)


# ═══════════════════════════════════════════════════════════════════════════
# FIX B1 — a RAISING pre-exit probe must ABORT the exit (no blind order).
# ═══════════════════════════════════════════════════════════════════════════

def test_fixB1_probe_raise_places_no_order_leg_stays_open(clean_positions,
                                                          monkeypatch):
    """The broker's get_net_position_qty RAISES (connection reset) mid buy-to-cover.
    The exit must place NO order and leave the leg OPEN for a later retry — never a
    blind order that could double a short into a naked long."""
    ltps = {"A": 100.0, "B": 100.0, "C": 100.0}
    created = _install_brokers(monkeypatch, ltps, raise_symbols={"A"})
    sid = _start(_cfg(30_000.0), ["A", "B", "C"])

    # A falls to its per-stock stop (price 97) → the step-lock WANTS to exit A, but
    # the pre-exit probe for A raises → ABORT. B/C flat, untouched.
    ltps["A"] = 97.0
    r = _tick(sid)
    aborts = [e for e in r["per_stock_exits"]
              if e.get("status") == "EXIT_ABORTED_PROBE_FAILED"]
    assert len(aborts) == 1 and aborts[0]["symbol"] == "A"
    # NO broker exit order was placed for A across any broker.
    total_exits = sum(len(mb.exit_calls) for mb in created.values())
    assert total_exits == 0
    # A remains OPEN (available for retry once the broker is reachable).
    assert _status(sid, "A") == "OPEN"
    assert _open_count(sid) == 3

    # The exit gate was RELEASED so the next tick can retry: with the broker now
    # reachable, A exits cleanly (one order) — proves it wasn't left stuck.
    created_ok = _install_brokers(monkeypatch, ltps, raise_symbols=set())
    r2 = _tick(sid)
    stopped = {e["symbol"]: e.get("reason") for e in r2["per_stock_exits"]
               if e.get("status") == "EXITED"}
    assert stopped.get("A") == "STOP_STOCK"
    assert _status(sid, "A") == "CLOSED"
    assert sum(len(mb.exit_calls) for mb in created_ok.values()) == 1


# ═══════════════════════════════════════════════════════════════════════════
# FIX B2 — single-flight: two concurrent claims → EXACTLY ONE order.
# ═══════════════════════════════════════════════════════════════════════════

def _one_open_session(monkeypatch, ltps, *, exit_delay_sec=0.0):
    # A valid 3-leg intraday basket (top_n 3..10); we contend on leg "A" only.
    for s in ("B", "C"):
        ltps.setdefault(s, 100.0)
    created = _install_brokers(monkeypatch, ltps, exit_delay_sec=exit_delay_sec)
    sid = _start(_cfg(30_000.0, top_n=3), ["A", "B", "C"])
    sess = TradingSession.load(sid)
    sess._build_brokers()
    sess.monitor.refresh_ltps(sess.brokers)
    pos = next(p for p in sess.monitor._open_positions() if p["symbol"] == "A")
    return sess, sid, pos, created


def test_fixB2_two_concurrent_claims_place_exactly_one_order(clean_positions,
                                                             monkeypatch):
    """tick_driver + ws_driver both fire STOP_STOCK for the SAME (session, symbol)
    at the same instant (the BRIGADE double-cover). The single-flight mutex must let
    EXACTLY ONE placement through; the other is a NO-OP (BLOCKED_INFLIGHT)."""
    ltps = {"A": 100.0}
    # A real delay so the winner still holds the flight while the loser attempts.
    sess, sid, pos, created = _one_open_session(monkeypatch, ltps,
                                                exit_delay_sec=0.3)
    broker = next(iter(created.values()))

    results = {}
    barrier = threading.Barrier(2)

    def _fire(tag):
        barrier.wait()  # release both threads at the same instant
        results[tag] = asyncio.run(_exit_single_position(
            session_id=sid, position=dict(pos), reason="STOP_STOCK",
            brokers=sess.brokers, registry=sess.registry,
            gtt_manager=sess.gtt_manager, kite_product="CNC"))

    t1 = threading.Thread(target=_fire, args=("tick",))
    t2 = threading.Thread(target=_fire, args=("ws",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    # EXACTLY ONE broker order for A — never two (that was the naked-position bug).
    a_orders = [e for e in broker.exit_calls if e["symbol"] == "A"]
    assert len(a_orders) == 1, f"expected exactly 1 exit order, got {len(a_orders)}"
    statuses = sorted(r["status"] for r in results.values())
    assert "BLOCKED_INFLIGHT" in statuses          # the loser placed nothing
    assert any(s in ("EXITED", "COMPLETE", "DRY_RUN") for s in statuses)
    assert _status(sid, "A") == "CLOSED"


def test_fixB2_flight_released_allows_sequential_retry(clean_positions):
    """The mutex is single-flight, NOT a permanent lock: once a flight ENDS, a
    later (sequential) retry for the same key acquires it again. This preserves the
    legitimate EXIT_FAILED retry path."""
    assert exit_gate.begin_exit_flight("S1", "A") is True
    assert exit_gate.begin_exit_flight("S1", "A") is False     # concurrent → blocked
    assert exit_gate.is_exit_in_flight("S1", "A") is True
    exit_gate.end_exit_flight("S1", "A")
    assert exit_gate.is_exit_in_flight("S1", "A") is False
    assert exit_gate.begin_exit_flight("S1", "A") is True       # retry not blocked
    exit_gate.end_exit_flight("S1", "A")


def test_fixB2_regression_single_exit_places_one_and_closes(clean_positions,
                                                            monkeypatch):
    """Regression: a normal, uncontended single exit still places EXACTLY one order
    and closes cleanly."""
    ltps = {"A": 100.0}
    sess, sid, pos, created = _one_open_session(monkeypatch, ltps)
    broker = next(iter(created.values()))
    res = asyncio.run(_exit_single_position(
        session_id=sid, position=dict(pos), reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))
    assert res["status"] in ("EXITED", "COMPLETE", "DRY_RUN")
    assert len([e for e in broker.exit_calls if e["symbol"] == "A"]) == 1
    assert _status(sid, "A") == "CLOSED"


def test_fixB2_exit_failed_then_retry_succeeds(clean_positions, monkeypatch):
    """Regression: a genuine EXIT_FAILED (broker rejects the first attempt) followed
    by a sequential retry still works — the single-flight does NOT block the retry."""
    ltps = {"A": 100.0}
    sess, sid, pos, created = _one_open_session(monkeypatch, ltps)
    broker = next(iter(created.values()))
    # Attempt 1: broker FAILS the exit → EXIT_FAILED (gate + flight released).
    broker.fail_symbols = {"A"}
    r1 = asyncio.run(_exit_single_position(
        session_id=sid, position=dict(pos), reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))
    assert r1["status"] == "EXIT_FAILED"
    assert exit_gate.is_exit_in_flight(sid, "A") is False       # flight released
    # Attempt 2: broker now accepts → the retry places again and closes.
    broker.fail_symbols = set()
    r2 = asyncio.run(_exit_single_position(
        session_id=sid, position=dict(pos), reason="EXIT_RETRY",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))
    assert r2["status"] in ("EXITED", "COMPLETE", "DRY_RUN")
    assert _status(sid, "A") == "CLOSED"
    assert len([e for e in broker.exit_calls if e["symbol"] == "A"]) == 2
