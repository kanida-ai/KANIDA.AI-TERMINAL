"""PER-STOCK STEP-LOCKING (config.step_lock_scope == "stock").

A second scope for the intraday_basket trail: instead of running the step-lock
ladder + give-back on the WHOLE basket (scope=="basket", default, unchanged), the
SAME ladder + give-back run PER POSITION on each stock's capital-slice return

    g_stock = position_uPnL / capital_slice
    capital_slice = total_allocated_capital * (notional / Σ notional over OPEN)
    notional = qty * avg_price

so each stock's % is directly comparable to the basket %-of-capital (leverage
cancels in the proportion; equal-weight → total_capital / N). Each position
ratchets its OWN (pos_trail_armed, pos_trail_peak) and exits INDEPENDENTLY via
_exit_single_position; the sibling positions keep running.

The SAFETY NETS stay basket-level: the time SQUARE_OFF and the catastrophic basket
hard stop (basket G <= -stop_pct) still flatten ALL positions. Only the PROFIT
trail moves per-stock.

CHANGE 1 (give-back default 0.015 -> 0.0125) is also asserted here.

All paper-safe (MockBroker, no real orders).
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, LIVE_EDITABLE_SESSION_FIELDS
from falcon.db import falcon_conn
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))


# ── shared broker with a MUTABLE ltp map (A + B) ──────────────────────────────
@pytest.fixture
def ab_brokers(monkeypatch):
    shared_ltps = {"A": 100.0, "B": 100.0}
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return shared_ltps


def _signals_ab():
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 100.0)])


# total_cap == Σ notional (2 legs * 100 qty * 100 avg = 20_000) so the setup is
# 1x: each slice = 10_000 == its notional, and g_stock == the price return.
CAP = 20_000.0


def _cfg(*, scope="stock", square_off_enabled=True, stop_pct=0.03,
         square_off_time="15:29:00", entry_time="09:15:00",
         mis_square_off_time="15:12:00"):
    return TradingSessionConfig(
        total_allocated_capital=CAP, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, per_stock_stop_enabled=False,
        step_lock_scope=scope,
        # step-lock ON with the default ladder (arm at first rung 0.03); give-back
        # default is now 0.0125.
        trail_step_lock_enabled=True,
        arm_pct=0.05, floor_pct=0.01, trail_giveback_pct=0.0125,
        stop_pct=stop_pct, square_off_enabled=square_off_enabled,
        square_off_time=square_off_time, entry_time=entry_time,
        mis_square_off_time=mis_square_off_time)


def _normalise(sid, qty=100, avg=100.0):
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_positions SET qty=?, avg_price=? "
            "WHERE session_id=? AND status='OPEN'", (qty, avg, sid))
        con.commit()


def _open_count(sid):
    with falcon_conn() as con:
        r = con.execute("SELECT COUNT(*) c FROM autotrade_positions "
                        "WHERE session_id=? AND status='OPEN'", (sid,)).fetchone()
    return r["c"]


def _pos_state(sid, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT status, pos_trail_armed, pos_trail_peak "
            "FROM autotrade_positions WHERE session_id=? AND symbol=?",
            (sid, symbol)).fetchone()
    return dict(r) if r else None


def _start(cfg, shared_ltps):
    """Create + start a 2-leg (A, B) intraday CNC basket, normalise to qty100/avg100."""
    _signals_ab()
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    sid = sess.session_id
    assert _open_count(sid) == 2
    _normalise(sid)
    return sid


def _tick(sid):
    return asyncio.run(TradingSession.load(sid).tick())


# ── CHANGE 1: give-back default 0.0125 ────────────────────────────────────────

def test_giveback_default_is_0_0125():
    c = TradingSessionConfig(total_allocated_capital=500_000.0,
                             strategy="intraday_basket", top_n_stocks=5)
    assert c.trail_giveback_pct == pytest.approx(0.0125)
    c.validate()
    # §4 headline holds: peak 0.08 → give-back level 0.08 - 0.0125 = 0.0675, which
    # on ₹5L is ₹33,750.
    peak = 0.08
    exit_level = peak - c.trail_giveback_pct
    assert exit_level == pytest.approx(0.0675)
    assert exit_level * 500_000.0 == pytest.approx(33_750.0)


def test_giveback_default_roundtrips():
    # A dict WITHOUT the key loads with the NEW 0.0125 default.
    c2 = TradingSessionConfig.from_dict(
        {"total_allocated_capital": 500_000.0, "strategy": "intraday_basket",
         "top_n_stocks": 5})
    assert c2.trail_giveback_pct == pytest.approx(0.0125)


# ── CHANGE 2 config: step_lock_scope field ────────────────────────────────────

def test_step_lock_scope_default_basket_and_validates():
    c = TradingSessionConfig(total_allocated_capital=CAP,
                             strategy="intraday_basket", top_n_stocks=5)
    assert c.step_lock_scope == "basket"
    c.validate()
    # explicit "stock" validates + round-trips.
    c2 = TradingSessionConfig(total_allocated_capital=CAP,
                              strategy="intraday_basket", top_n_stocks=5,
                              step_lock_scope="stock")
    c2.validate()
    assert TradingSessionConfig.from_dict(c2.to_dict()).step_lock_scope == "stock"


def test_step_lock_scope_rejects_bad_value():
    c = TradingSessionConfig(total_allocated_capital=CAP,
                             strategy="intraday_basket", top_n_stocks=5,
                             step_lock_scope="portfolio")
    with pytest.raises(ValueError, match="step_lock_scope"):
        c.validate()


def test_step_lock_scope_is_live_editable():
    assert "step_lock_scope" in LIVE_EDITABLE_SESSION_FIELDS
    from autotrade.api.config_edit_routes import _coerce_and_validate
    from fastapi import HTTPException
    assert _coerce_and_validate("step_lock_scope", "stock") == "stock"
    assert _coerce_and_validate("step_lock_scope", "BASKET") == "basket"
    with pytest.raises(HTTPException):
        _coerce_and_validate("step_lock_scope", "whole")


# ── 1. STOCK: a single stock arms, ratchets, exits ALONE; sibling untouched ────

def test_single_stock_arms_ratchets_and_exits_alone(clean_positions, ab_brokers):
    sid = _start(_cfg(scope="stock", square_off_enabled=False), ab_brokers)

    # Tick 1: A up to +3% of its slice → arms at the first rung (0.03). B flat.
    ab_brokers["A"] = 103.0   # uPnL 300 / slice 10_000 = 0.03
    ab_brokers["B"] = 100.0
    r1 = _tick(sid)
    assert r1["step_lock_scope"] == "stock"
    a1 = _pos_state(sid, "A")
    b1 = _pos_state(sid, "B")
    assert a1["pos_trail_armed"] == 1
    assert a1["pos_trail_peak"] == pytest.approx(0.03)
    assert b1["pos_trail_armed"] == 0        # B never armed
    assert _open_count(sid) == 2             # nobody exited

    # Tick 2: A ratchets up to +8% (peak 0.08 → step floor 0.06, give-back 0.0675).
    ab_brokers["A"] = 108.0
    r2 = _tick(sid)
    assert r2["per_stock_exits"] == []       # 0.08 > trigger 0.0675 → HOLD
    a2 = _pos_state(sid, "A")
    assert a2["pos_trail_peak"] == pytest.approx(0.08)
    assert _open_count(sid) == 2

    # Tick 3: A pulls back below the trigger → A alone step-lock-exits (TRAIL_EXIT
    # binds since give-back 0.0675 > step floor 0.06). B is untouched.
    ab_brokers["A"] = 105.0   # g_stock 0.05 <= 0.0675
    r3 = _tick(sid)
    exits = r3["per_stock_exits"]
    assert len(exits) == 1
    assert exits[0]["symbol"] == "A"
    assert exits[0]["reason"] == "TRAIL_EXIT"
    assert _pos_state(sid, "A")["status"] == "CLOSED"
    assert _pos_state(sid, "B")["status"] == "OPEN"   # sibling still running
    assert _open_count(sid) == 1


# ── 2. DIVERGENCE: A runs +8% then pulls back → exits; B at -2% keeps running ──

def test_divergence_A_exits_B_keeps_running(clean_positions, ab_brokers):
    sid = _start(_cfg(scope="stock", square_off_enabled=False), ab_brokers)

    # Tick 1: A +8% (arms peak 0.08, no exit this tick); B -2% (never arms).
    ab_brokers["A"] = 108.0   # g_stock +0.08
    ab_brokers["B"] = 98.0    # g_stock -0.02
    _tick(sid)
    assert _pos_state(sid, "A")["pos_trail_armed"] == 1
    assert _pos_state(sid, "A")["pos_trail_peak"] == pytest.approx(0.08)
    assert _pos_state(sid, "B")["pos_trail_armed"] == 0
    assert _open_count(sid) == 2

    # Tick 2: A pulls back through its give-back → A EXITS ALONE. B still at -2%
    # is NOT exited by A's move (per-stock independence — the whole point).
    ab_brokers["A"] = 105.0   # g_stock 0.05 <= 0.0675 trigger
    ab_brokers["B"] = 98.0    # unchanged, -2%
    r = _tick(sid)
    exits = {e["symbol"] for e in r["per_stock_exits"]}
    assert exits == {"A"}
    assert _pos_state(sid, "A")["status"] == "CLOSED"
    # B untouched: still OPEN, still un-armed (a -2% stock does not step-lock).
    b = _pos_state(sid, "B")
    assert b["status"] == "OPEN"
    assert b["pos_trail_armed"] == 0
    assert _open_count(sid) == 1


# ── 3. per-position state persists + ratchets MONOTONICALLY ────────────────────

def test_per_position_state_persists_and_ratchets_monotonic(clean_positions,
                                                            ab_brokers):
    sid = _start(_cfg(scope="stock", square_off_enabled=False), ab_brokers)
    ab_brokers["B"] = 100.0   # B flat throughout

    # Climb A: 0.03 → 0.05 → 0.08, each persisted.
    for ltp, exp_peak in ((103.0, 0.03), (105.0, 0.05), (108.0, 0.08)):
        ab_brokers["A"] = ltp
        _tick(sid)
        st = _pos_state(sid, "A")
        assert st["pos_trail_armed"] == 1
        assert st["pos_trail_peak"] == pytest.approx(exp_peak)

    # DIP to 0.07 (> trigger 0.0675) → HOLD, and the peak STICKS at 0.08 (the
    # ratchet never steps down; state_changed False so nothing re-persisted).
    ab_brokers["A"] = 107.0   # g_stock 0.07
    r = _tick(sid)
    assert r["per_stock_exits"] == []
    st = _pos_state(sid, "A")
    assert st["pos_trail_peak"] == pytest.approx(0.08)   # monotonic-up, held
    assert st["status"] == "OPEN"


# ── 4a. SAFETY NET: basket -3% stop flattens ALL in stock mode ────────────────

def test_basket_stop_flattens_all_in_stock_mode(clean_positions, ab_brokers):
    # square-off OFF so ONLY the catastrophic basket stop can fire (clock-independent).
    sid = _start(_cfg(scope="stock", square_off_enabled=False, stop_pct=0.03),
                 ab_brokers)
    # Both legs down: uPnL = 2 * 100 * (97-100) = -600 → basket G = -600/20000 = -3%.
    ab_brokers["A"] = 97.0
    ab_brokers["B"] = 97.0
    r = _tick(sid)
    assert r["kill_reason"] == "STOP"
    assert r["kill_switch_fired"] is True
    assert r["gross_return"] == pytest.approx(-0.03)
    assert _open_count(sid) == 0             # WHOLE basket flattened


# ── 4b. SAFETY NET: time square-off flattens ALL in stock mode ────────────────

def test_square_off_flattens_all_in_stock_mode(clean_positions, ab_brokers):
    # Build square-off times in the PAST relative to real now so the basket
    # SQUARE_OFF branch fires deterministically (the pure engine uses real IST).
    now = datetime.now(IST)
    sq = (now - timedelta(seconds=60)).strftime("%H:%M:%S")
    entry = (now - timedelta(seconds=300)).strftime("%H:%M:%S")
    mis = (now - timedelta(seconds=120)).strftime("%H:%M:%S")
    sid = _start(_cfg(scope="stock", square_off_enabled=True, square_off_time=sq,
                      entry_time=entry, mis_square_off_time=mis), ab_brokers)
    # Positions roughly flat — square-off must flatten regardless of the trail.
    ab_brokers["A"] = 101.0
    ab_brokers["B"] = 100.0
    r = _tick(sid)
    assert r["kill_reason"] == "SQUARE_OFF"
    assert r["kill_switch_fired"] is True
    assert _open_count(sid) == 0             # WHOLE basket flattened


# ── 5. BASKET scope (default) still exits the WHOLE basket on a trail exit ─────

def test_basket_scope_exits_whole_basket(clean_positions, ab_brokers):
    """Contrast: in scope=="basket" a give-back exit flattens ALL positions (the
    unchanged single-decide flow), unlike per-stock independence."""
    sid = _start(_cfg(scope="basket", square_off_enabled=False), ab_brokers)
    # Arm the BASKET at +8% of capital: uPnL 2*100*(108-100)=1600 → G=0.08.
    ab_brokers["A"] = 108.0
    ab_brokers["B"] = 108.0
    r1 = _tick(sid)
    assert r1.get("step_lock_scope") is None      # basket path: no stock marker
    assert r1["trail_armed"] is True
    assert r1["trail_peak"] == pytest.approx(0.08)
    assert _open_count(sid) == 2
    # Basket gives back below the trigger → the WHOLE basket exits (both legs).
    ab_brokers["A"] = 105.0
    ab_brokers["B"] = 105.0
    r2 = _tick(sid)
    assert r2["trail_action"] == "EXIT"
    assert r2["kill_reason"] == "TRAIL_EXIT"
    assert _open_count(sid) == 0                    # ALL flattened, not one leg
