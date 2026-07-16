"""EQUITY MIS SHORT (intraday) — additive, PAPER-SAFE, direction-aware.

Extends the existing FUT-short machinery to EQUITY on MIS ONLY. Covers the two
HARD real-money constraints + every seam:

  HARD-1  short allowed ONLY for EQ+MIS (and FUT) — EQ+CNC and EQ+MTF REJECTED.
  HARD-2  a MIS short is MANDATORILY bought-to-cover intraday (before the broker
          ~15:20 auto-square) via the FEATURE-A MIS defensive square-off, and the
          cover order is a BUY.

Seams: SELL-to-open entry side, kill/monitor sign inversion (fires on RISE,
NO-fire on FALL), GTT-OCO level inversion (suppressed-but-recorded for MIS),
direction-aware intraday charge legs (STT on the SELL-open leg), reject/phantom
guard on a SELL-to-open.

Everything runs against MockBroker (no real Kite, no network, no real orders).
"""
import asyncio

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.execution.orders import build_order
from autotrade.monitoring import square_off_scheduler
from autotrade.monitoring.gtt_manager import GTTManager
from autotrade.monitoring.monitor import PortfolioMonitor
from autotrade.monitoring.registry import PositionRegistry
from autotrade.session import TradingSession
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


# ── HARD-1: config validation for ALL short combos ──────────────────────────────

def _cfg(**kw):
    base = dict(total_allocated_capital=100000.0, top_n_stocks=1,
                sizing_mode="equal", direction="short",
                per_position_gtt_enabled=False)
    base.update(kw)
    return TradingSessionConfig(**base)


def test_short_eq_mis_valid():
    _cfg(instrument_type="EQ", order_product="MIS").validate()  # allowed


def test_short_eq_cnc_rejected():
    # CNC = delivery; short-delivery is illegal on NSE cash → reject.
    with pytest.raises(ValueError, match="short is supported only for FUT"):
        _cfg(instrument_type="EQ", order_product="CNC").validate()


def test_short_eq_mtf_rejected():
    # MTF is a LONG-ONLY leverage (margin-funding) product → reject.
    with pytest.raises(ValueError, match="short is supported only for FUT"):
        _cfg(instrument_type="EQ", order_product="MTF").validate()


def test_short_fut_still_valid():
    # FUT short (any product) still allowed — unchanged.
    TradingSessionConfig(total_allocated_capital=1_000_000.0,
                         instrument_type="FUT", direction="short").validate()


def test_long_eq_mis_unaffected():
    # LONG on EQ+MIS is byte-for-byte unaffected (regression).
    _cfg(instrument_type="EQ", order_product="MIS", direction="long").validate()


def test_short_eq_mis_round_trips_json():
    cfg = _cfg(instrument_type="EQ", order_product="MIS")
    cfg2 = TradingSessionConfig.from_json(cfg.to_json())
    assert cfg2.direction == "short"
    cfg2.validate()


# ── Entry side: SELL-to-open (build_eq_order) ──────────────────────────────────

def test_short_eq_mis_entry_is_sell():
    cfg = _cfg(instrument_type="EQ", order_product="MIS")
    order = build_order("A", 10, cfg, MockBroker(profile=None, ltps={"A": 100.0}))
    assert order.transaction_type == "SELL"   # SELL to OPEN a short
    assert order.instrument_type == "EQ"
    assert order.exchange == "NSE"            # equity stays on NSE (not NFO)
    assert order.product == "MIS"


def test_long_eq_mis_entry_is_buy_regression():
    cfg = _cfg(instrument_type="EQ", order_product="MIS", direction="long")
    order = build_order("A", 10, cfg, MockBroker(profile=None, ltps={"A": 100.0}))
    assert order.transaction_type == "BUY"    # long unchanged


# ── Monitor P&L sign inversion (product-agnostic → holds for EQ) ────────────────

def _register_short(session_id, symbol="A", qty=100, avg=100.0):
    reg = PositionRegistry(session_id, 100000.0)
    reg.register(symbol=symbol, broker_profile="p", qty=qty, avg_price=avg,
                 product="MIS", instrument_type="EQ", direction="short")
    return reg


def test_short_eq_pnl_positive_when_price_falls(clean_positions):
    sid = "s_eqshort_down"
    reg = _register_short(sid)
    reg.update_ltp("A", ltp=90.0, broker_profile="p")
    mon = PortfolioMonitor(sid, 100000.0)
    # short: (avg-ltp)*qty = (100-90)*100 = +1000 (profit when price FALLS)
    assert abs(mon.total_unrealised() - 1000.0) < 1e-6


def test_short_eq_pnl_negative_when_price_rises(clean_positions):
    sid = "s_eqshort_up"
    reg = _register_short(sid)
    reg.update_ltp("A", ltp=110.0, broker_profile="p")
    mon = PortfolioMonitor(sid, 100000.0)
    assert abs(mon.total_unrealised() - (-1000.0)) < 1e-6


# ── GTT-OCO orientation for an EQ MIS short (SUPPRESSED but recorded inverted) ──

def test_short_eq_mis_gtt_suppressed_levels_inverted(clean_positions):
    sid = "s_eqshort_gtt"
    reg = _register_short(sid, avg=100.0)
    cfg = _cfg(instrument_type="EQ", order_product="MIS",
               per_position_gtt_enabled=True,
               per_position_stop_pct=0.08, per_position_target_pct=0.20)
    gm = GTTManager(sid, cfg, {"p": MockBroker(profile=None, ltps={"A": 100.0})},
                    reg)
    pos = PortfolioMonitor(sid, 100000.0)._open_positions()[0]
    out = gm.place_for_position(pos)
    # MIS legs never place a broker GTT (no orphan overnight); levels recorded.
    assert out["status"] == "SUPPRESSED_INTRADAY"
    assert out["gtt_id"] is None
    # SHORT inversion: stop ABOVE entry (buy-stop), target BELOW (buy-limit).
    # (leverage L clamps to 1 for a single-name EQ book here → price==capital pct.)
    assert out["stop"] > 100.0
    assert out["target"] < 100.0


# ── session fire/tick fixtures ─────────────────────────────────────────────────

@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created, shared_ltps


@pytest.fixture
def squareoff_autostart_on():
    square_off_scheduler.set_autostart(True)
    yield
    square_off_scheduler.set_autostart(False)


# ── HARD-2: MANDATORY intraday COVER (bought-to-cover with a BUY) ───────────────

def test_short_eq_mis_squares_off_via_buy_cover(clean_positions, patched_brokers):
    """The #1 correctness item: an EQ MIS short MUST be bought-to-cover intraday.
    Past its mis_square_off_time the deterministic in-tick MIS backstop flattens
    the short with a BUY (cover), tagged MIS_SQUARE_OFF, before the broker's
    ~15:20 auto-square. (portfolio_kill_switch strategy = deterministic tick
    backstop, independent of the wall clock.)"""
    created, _ = patched_brokers
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = _cfg(instrument_type="EQ", order_product="MIS",
               strategy="portfolio_kill_switch", top_n_stocks=1,
               kill_switch_enabled=False, mis_square_off_time="00:00:01")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    assert sess.status()["n_open_positions"] == 1
    # No wall-clock timer armed (autostart off in tests) → the tick backstop fires.
    assert square_off_scheduler.is_running(sess.session_id) is False
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert "MIS_SQUARE_OFF" in (out["kill_reason"] or "")
    assert sess.status()["n_open_positions"] == 0
    # The cover order was a BUY-to-cover (direction="short" recorded on the exit).
    exit_calls = [c for mb in created.values() for c in mb.exit_calls]
    assert exit_calls, "no exit was placed"
    assert all(c["direction"] == "short" for c in exit_calls)
    # close_reason tagged MIS_SQUARE_OFF on the flattened short.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT close_reason FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchall()
    assert all(r["close_reason"] == "MIS_SQUARE_OFF" for r in rows)


def test_short_intraday_basket_arms_cover_before_broker_square(
        clean_positions, patched_brokers, squareoff_autostart_on):
    """An EQ MIS SHORT intraday_basket session arms its defensive square-off at
    the MIS time (default 15:12), which is BEFORE the broker's ~15:20 compulsory
    auto-square — so the short is mandatorily bought-to-cover in time even if the
    15:29 basket square-off would otherwise ride past the broker window. Uses a
    LATE future MIS time so the daemon just sleeps (no fire mid-test)."""
    created, _ = patched_brokers
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0), ("C", 3, 7.0, 50.0)])
    cfg = _cfg(instrument_type="EQ", order_product="MIS",
               strategy="intraday_basket", top_n_stocks=3,
               square_off_time="23:59:00", mis_square_off_time="23:58:00",
               arm_pct=0.01, floor_pct=0.01)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    assert square_off_scheduler.is_running(sess.session_id) is True
    target = square_off_scheduler.target_for_session(sess.session_id)
    # The EARLIER MIS defensive time wins over the 23:59 basket square-off.
    assert (target.hour, target.minute) == (23, 58)


def test_mis_square_off_default_is_before_broker_autosquare():
    # 15:12 default MUST be strictly before the broker's ~15:20 compulsory square.
    cfg = _cfg(instrument_type="EQ", order_product="MIS")
    from autotrade.config import _parse_clock_to_seconds
    assert _parse_clock_to_seconds(cfg.mis_square_off_time) < 15 * 3600 + 20 * 60


# ── Kill-switch sign: FIRES on rise (loss), NO-FIRE on fall (profit) ────────────

def _short_kill_session(created_ltps, price):
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = _cfg(instrument_type="EQ", order_product="MIS",
               strategy="portfolio_kill_switch",
               kill_switch_enabled=True, kill_switch_pct=0.03,
               kill_switch_direction="loss")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    created_ltps["A"] = price   # move the shared mark after entry
    return sess


def test_short_eq_mis_kill_FIRES_on_rise(clean_positions, patched_brokers,
                                         market_hours_clock):
    created, shared = patched_brokers
    sess = _short_kill_session(shared, price=110.0)   # +10% up = short LOSS
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert "LOSS_LIMIT" in (out["kill_reason"] or "")
    # Flattened with a BUY-to-cover.
    exit_calls = [c for mb in created.values() for c in mb.exit_calls]
    assert exit_calls and all(c["direction"] == "short" for c in exit_calls)


def test_short_eq_mis_kill_NO_FIRE_on_fall(clean_positions, patched_brokers,
                                           market_hours_clock):
    created, shared = patched_brokers
    sess = _short_kill_session(shared, price=90.0)    # -10% down = short PROFIT
    out = asyncio.run(sess.tick())
    # A profitable short must NOT trip a LOSS kill (fail-safe no-fire case).
    assert out["kill_switch_fired"] is False
    assert sess.status()["n_open_positions"] == 1
    exit_calls = [c for mb in created.values() for c in mb.exit_calls]
    assert not exit_calls


# ── Charges: intraday-equity STT is charged on the SELL-OPEN leg for a short ────

def test_short_intraday_charges_stt_on_open_sell_leg(clean_positions):
    """For a short round-trip the SELL leg is the OPEN (avg_price), not the exit.
    STT (MIS sell-side 0.025%) must be computed on the open-sell value, not the
    buy-to-cover value — the direction-aware fix in journal_routes/pnl_summary."""
    cfg = _cfg(instrument_type="EQ", order_product="MIS",
               strategy="portfolio_kill_switch", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    reg = PositionRegistry(sess.session_id, 100000.0)
    reg.register(symbol="A", broker_profile="p", qty=100, avg_price=100.0,
                 product="MIS", instrument_type="EQ", direction="short")
    reg.update_ltp("A", ltp=90.0, broker_profile="p")
    reg.mark_closed("A", "TRAIL_EXIT", exit_price=90.0, broker_profile="p")
    from autotrade.api.journal_routes import build_journal
    j = build_journal(sess.session_id)
    row = next(p for p in j["positions"] if p["symbol"] == "A")
    ch = row["charges_estimate"]
    assert ch is not None
    # SELL-open leg value = 100*100 = 10,000 → STT = 0.00025 * 10000 = 2.5.
    # (A long-assumption would use the cover value 90*100=9,000 → 2.25 — WRONG.)
    assert abs(ch["stt"] - 2.5) < 1e-6
    # realised is the short GROSS profit: (100-90)*100 = +1000.
    assert abs(row["realised_pnl"] - 1000.0) < 1e-6


# ── Reject / phantom guard works for a SELL-to-open ────────────────────────────

def test_short_eq_mis_rejected_entry_registers_no_phantom(clean_positions,
                                                           monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0)])
    shared = {"A": 100.0}

    def fake_build_client(profile, dry_run=True):
        # dry_run False so the reject guard (which requires a broker_order_id in
        # live) engages; A is a reject symbol → PLACED-but-0-fill → dropped.
        return MockBroker(profile=profile, dry_run=False, ltps=shared,
                          reject_symbols={"A"})

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = _cfg(instrument_type="EQ", order_product="MIS",
               strategy="portfolio_kill_switch", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="live")   # exercise the live reject path
    asyncio.run(sess.start())
    # A SELL-to-open that never fills must NOT register a phantom short position.
    assert sess.status()["n_open_positions"] == 0
