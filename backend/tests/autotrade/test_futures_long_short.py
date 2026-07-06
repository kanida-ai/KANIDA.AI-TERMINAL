"""FUTURES long/short support — additive, PAPER-SAFE, direction-aware.

Covers every place `direction` / P&L sign / exit-side / GTT orientation is
threaded, PLUS a hard regression assert that a direction="long" (default /
equity) session is byte-for-byte unchanged.

Everything runs against MockBroker (no real Kite, no network, no real orders).
"""
import asyncio

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.capital import CapitalAllocator, InsufficientCapitalError
from autotrade.config import TradingSessionConfig
from autotrade.execution.orders import build_order
from autotrade.monitoring.gtt_manager import compute_levels
from autotrade.monitoring.monitor import PortfolioMonitor
from autotrade.monitoring.registry import PositionRegistry
from autotrade.session import TradingSession
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


# ── Config validation ─────────────────────────────────────────────────────────

def test_direction_defaults_long():
    cfg = TradingSessionConfig(total_allocated_capital=100000.0)
    assert cfg.direction == "long"
    cfg.validate()  # must not raise


def test_short_requires_fut():
    cfg = TradingSessionConfig(total_allocated_capital=100000.0,
                               instrument_type="EQ", direction="short")
    with pytest.raises(ValueError, match="short is currently supported only for FUT"):
        cfg.validate()


def test_short_fut_valid():
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0,
                               instrument_type="FUT", direction="short")
    cfg.validate()  # allowed


def test_invalid_direction_rejected():
    cfg = TradingSessionConfig(total_allocated_capital=100000.0,
                               direction="sideways")
    with pytest.raises(ValueError, match="invalid direction"):
        cfg.validate()


def test_direction_round_trips_through_json():
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0,
                               instrument_type="FUT", direction="short")
    cfg2 = TradingSessionConfig.from_json(cfg.to_json())
    assert cfg2.direction == "short"


def test_legacy_config_json_defaults_long():
    # A config_json written before this feature has no `direction` key.
    cfg = TradingSessionConfig.from_dict(
        {"total_allocated_capital": 100000.0, "instrument_type": "EQ"})
    assert cfg.direction == "long"


# ── Entry side (build_fut_order transaction_type) ──────────────────────────────

def test_fut_long_entry_is_buy():
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0,
                               instrument_type="FUT", direction="long")
    b = MockBroker(profile=None, ltps={"NIFTYW": 200.0}, lot_size=50)
    order = build_order("NIFTYW", 100, cfg, b)
    assert order.transaction_type == "BUY"
    assert order.instrument_type == "FUT"
    assert order.exchange == "NFO"
    assert order.product == "NRML"


def test_fut_short_entry_is_sell():
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0,
                               instrument_type="FUT", direction="short")
    b = MockBroker(profile=None, ltps={"NIFTYW": 200.0}, lot_size=50)
    order = build_order("NIFTYW", 100, cfg, b)
    assert order.transaction_type == "SELL"  # sell to OPEN a short


def test_equity_entry_always_buy():
    cfg = TradingSessionConfig(total_allocated_capital=100000.0,
                               instrument_type="EQ")
    b = MockBroker(profile=None, ltps={"A": 100.0})
    order = build_order("A", 10, cfg, b)
    assert order.transaction_type == "BUY"


# ── Futures MARGIN sizing (long AND short) ─────────────────────────────────────

def _fut_cfg(direction="long"):
    return TradingSessionConfig(total_allocated_capital=1_000_000.0,
                                top_n_stocks=5, sizing_mode="equal",
                                instrument_type="FUT", direction=direction)


def test_fut_margin_sizing_uses_margin_not_notional():
    cfg = _fut_cfg("long")
    alloc = CapitalAllocator(cfg)
    # notional per lot = ltp*lot = 200*50 = 10,000; margin per lot = 40,000.
    b = MockBroker(profile=None, ltps={"NIFTYW": 200.0}, lot_size=50,
                   fut_margin_per_lot=40_000.0)
    qty = alloc.calculate_quantity("NIFTYW", 200_000.0, b)
    # margin-sized: floor(200k / 40k) = 5 lots * 50 = 250. (notional would give
    # floor(200k/10k)=20 lots=1000 — proving we size on margin, not notional.)
    assert qty == 250


def test_fut_short_margin_sizing_same_as_long():
    long_alloc = CapitalAllocator(_fut_cfg("long"))
    short_alloc = CapitalAllocator(_fut_cfg("short"))
    b = MockBroker(profile=None, ltps={"NIFTYW": 200.0}, lot_size=50,
                   fut_margin_per_lot=40_000.0)
    q_long = long_alloc.calculate_quantity("NIFTYW", 200_000.0, b)
    q_short = short_alloc.calculate_quantity("NIFTYW", 200_000.0, b)
    assert q_long == q_short == 250  # direction does NOT change sizing


def test_fut_refuses_when_no_margin_api():
    cfg = _fut_cfg("long")
    alloc = CapitalAllocator(cfg)
    # No fut_margin_per_lot → base default None → must REFUSE (never notional).
    b = MockBroker(profile=None, ltps={"NIFTYW": 200.0}, lot_size=50)
    with pytest.raises(InsufficientCapitalError, match="margin unavailable"):
        alloc.calculate_quantity("NIFTYW", 1_000_000.0, b)


def test_fut_insufficient_for_one_lot_margin():
    cfg = _fut_cfg("long")
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps={"NIFTYW": 200.0}, lot_size=50,
                   fut_margin_per_lot=40_000.0)
    with pytest.raises(InsufficientCapitalError, match="1 FUT lot margin"):
        alloc.calculate_quantity("NIFTYW", 10_000.0, b)


# ── P&L sign (registry + monitor) ──────────────────────────────────────────────

def _register(session_id, symbol, qty, avg_price, direction):
    reg = PositionRegistry(session_id, 1_000_000.0)
    reg.register(symbol=symbol, broker_profile="p", qty=qty, avg_price=avg_price,
                 instrument_type="FUT", direction=direction)
    return reg


def test_long_pnl_positive_when_price_rises(clean_positions):
    sid = "s_long_up"
    reg = _register(sid, "XFUT", 50, 100.0, "long")
    reg.update_ltp("XFUT", ltp=110.0, broker_profile="p")
    mon = PortfolioMonitor(sid, 1_000_000.0)
    # (110-100)*50 = +500
    assert abs(mon.total_unrealised() - 500.0) < 1e-6


def test_short_pnl_positive_when_price_FALLS(clean_positions):
    sid = "s_short_down"
    reg = _register(sid, "XFUT", 50, 100.0, "short")
    reg.update_ltp("XFUT", ltp=90.0, broker_profile="p")
    mon = PortfolioMonitor(sid, 1_000_000.0)
    # short: (avg-ltp)*qty = (100-90)*50 = +500 (profit when price falls)
    assert abs(mon.total_unrealised() - 500.0) < 1e-6


def test_short_pnl_negative_when_price_rises(clean_positions):
    sid = "s_short_up"
    reg = _register(sid, "XFUT", 50, 100.0, "short")
    reg.update_ltp("XFUT", ltp=110.0, broker_profile="p")
    mon = PortfolioMonitor(sid, 1_000_000.0)
    # short: (100-110)*50 = -500 (loss when price rises)
    assert abs(mon.total_unrealised() - (-500.0)) < 1e-6


def test_short_realised_pnl_sign_on_close(clean_positions):
    sid = "s_short_close"
    reg = _register(sid, "XFUT", 50, 100.0, "short")
    reg.update_ltp("XFUT", ltp=90.0, broker_profile="p")
    reg.mark_closed("XFUT", "TRAIL_EXIT", exit_price=90.0, broker_profile="p")
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT realised_pnl FROM autotrade_positions WHERE session_id=? "
            "AND symbol='XFUT'", (sid,)).fetchone()
    # covered at 90 having shorted at 100 → +500 realised.
    assert abs(row["realised_pnl"] - 500.0) < 1e-6


def test_short_kill_basis_gross_return_positive_on_fall(clean_positions):
    sid = "s_short_gr"
    # A session row is required for invested_basis to persist/read.
    from falcon.db import falcon_conn
    from datetime import datetime, timezone, timedelta
    _ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
    # CAPITAL BASIS (commit b3fbb0c): for an F&O basket the kill/return basis is
    # the ALLOCATED CAPITAL (≈ margin at risk), NOT the notional Σ(qty*avg_price)
    # (~4-5x the margin). Use allocated=5000 here so the short-profit-on-fall sign
    # is checked cleanly. The POINT of this test is the SIGN (a short profits when
    # price falls), not the denominator choice — that is covered by the basis
    # test below.
    alloc = 5000.0
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json)
               VALUES (?,?,?,?,?,?)""",
            (sid, _ist, "RUNNING", "paper", alloc, "{}"))
        con.commit()
    reg = _register(sid, "XFUT", 50, 100.0, "short")
    mon = PortfolioMonitor(sid, alloc)
    # F&O basis = allocated capital (not notional).
    assert mon.freeze_invested_basis() == alloc
    assert mon.invested_basis() == alloc
    reg.update_ltp("XFUT", ltp=95.0, broker_profile="p")
    gr = mon.compute_gross_return_invested()
    # uPnL = (100-95)*50 = +250 (POSITIVE — short gains when price falls);
    # basis 5000 → +0.05.
    assert abs(gr - 0.05) < 1e-9


def test_fno_basis_is_allocated_not_notional(clean_positions):
    """Regression lock for commit b3fbb0c: an F&O basket freezes the invested
    basis to the ALLOCATED CAPITAL, never Σ(qty*avg_price) notional."""
    from falcon.db import falcon_conn
    from datetime import datetime, timezone, timedelta
    sid = "s_fno_basis"
    _ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
    alloc = 1_000_000.0
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json) VALUES (?,?,?,?,?,?)""",
            (sid, _ist, "RUNNING", "paper", alloc, "{}"))
        con.commit()
    _register(sid, "XFUT", 50, 100.0, "long")  # notional = 5000
    mon = PortfolioMonitor(sid, alloc)
    # NOT 5000 (notional) — the F&O basis is the allocated capital.
    assert mon.freeze_invested_basis() == alloc


# ── GTT-OCO direction (compute_levels) ─────────────────────────────────────────

def test_gtt_levels_long_stop_below_target_above():
    stop_t, stop_l, tgt_t, tgt_l = compute_levels(100.0, 0.03, 0.06,
                                                  direction="long")
    assert stop_t == 97.0        # below entry
    assert tgt_t == 106.0        # above entry
    assert stop_l < stop_t       # long stop limit below trigger


def test_gtt_levels_short_stop_above_target_below():
    stop_t, stop_l, tgt_t, tgt_l = compute_levels(100.0, 0.03, 0.06,
                                                  direction="short")
    assert stop_t == 103.0       # ABOVE entry (buy-stop to cover)
    assert tgt_t == 94.0         # BELOW entry (buy-limit to cover)
    assert stop_l > stop_t       # short stop limit ABOVE trigger (gap-up fill)


def test_gtt_levels_default_is_long():
    a = compute_levels(100.0, 0.03, 0.06)
    b = compute_levels(100.0, 0.03, 0.06, direction="long")
    assert a == b  # default byte-identical to long


# ── GTT-OCO leg construction (place_gtt_oco on ZerodhaBroker) ──────────────────

class _FakeKite:
    """Minimal Kite surface to capture place_gtt args (never hits network)."""
    PRODUCT_CNC = "CNC"; PRODUCT_MIS = "MIS"; PRODUCT_NRML = "NRML"
    ORDER_TYPE_LIMIT = "LIMIT"; ORDER_TYPE_MARKET = "MARKET"
    TRANSACTION_TYPE_BUY = "BUY"; TRANSACTION_TYPE_SELL = "SELL"
    GTT_TYPE_OCO = "two-leg"; EXCHANGE_NFO = "NFO"; EXCHANGE_NSE = "NSE"

    def __init__(self):
        self.gtt_args = None

    def place_gtt(self, **kw):
        self.gtt_args = kw
        return {"trigger_id": 111}


def _zerodha_with_fake_kite():
    from autotrade.broker.zerodha import ZerodhaBroker
    b = ZerodhaBroker(profile=None, dry_run=False)
    fk = _FakeKite()
    b._kite = fk
    # Force the live gate open WITHOUT touching env/order_executor.
    b._live_allowed = lambda: True
    return b, fk


def test_place_gtt_short_uses_buy_legs_and_sorted_triggers():
    b, fk = _zerodha_with_fake_kite()
    b.place_gtt_oco(symbol="XFUT", qty=50, stop_price=103.0, target_price=94.0,
                    last_price=100.0, product="NRML", exchange="NFO",
                    stop_limit_price=103.5, direction="short")
    assert fk.gtt_args is not None
    # trigger_values must be [lower, upper] = [target(94), stop(103)].
    assert fk.gtt_args["trigger_values"] == [94.0, 103.0]
    legs = fk.gtt_args["orders"]
    # both legs BUY-to-cover for a short.
    assert all(leg["transaction_type"] == "BUY" for leg in legs)


def test_place_gtt_long_unchanged_sell_legs():
    b, fk = _zerodha_with_fake_kite()
    b.place_gtt_oco(symbol="A", qty=10, stop_price=97.0, target_price=106.0,
                    last_price=100.0, product="CNC", exchange="NSE",
                    stop_limit_price=96.7, direction="long")
    assert fk.gtt_args["trigger_values"] == [97.0, 106.0]
    legs = fk.gtt_args["orders"]
    assert all(leg["transaction_type"] == "SELL" for leg in legs)


def test_place_gtt_rounds_all_prices_to_tick_size(monkeypatch):
    """Kite rejects GTT trigger/limit/target prices that aren't a multiple of the
    instrument tick ('…should be a multiple of tick size 0.10'). Off-grid levels
    (a tick=0.10 stock at 1277.18 / 1425.06) MUST be rounded before place_gtt,
    else EVERY per-position GTT-OCO silently fails (RECORDED_ONLY, no broker stop
    — the 2026-07-06 CNC overnight hole). Regression: prices land on the tick grid."""
    import falcon.trade.services.mtf_eligibility as mtf
    monkeypatch.setattr(mtf, "get_tick_size", lambda kite, symbol: 0.10)
    b, fk = _zerodha_with_fake_kite()
    b.place_gtt_oco(symbol="AEGISLOG", qty=35, stop_price=1277.18,
                    target_price=1425.06, last_price=1344.43, product="CNC",
                    exchange="NSE", stop_limit_price=1273.35, direction="long")
    # trigger_values rounded to 0.10 (long: [stop, target]).
    assert fk.gtt_args["trigger_values"] == [1277.20, 1425.10]
    # every leg price + last_price is an exact multiple of the tick.
    for leg in fk.gtt_args["orders"]:
        assert abs(round(leg["price"] / 0.10) * 0.10 - leg["price"]) < 1e-9
    lp = fk.gtt_args["last_price"]
    assert abs(round(lp / 0.10) * 0.10 - lp) < 1e-9


# ── Exit side (buy-to-cover) via kill switch ───────────────────────────────────

@pytest.fixture
def patched_fut_brokers(monkeypatch):
    created = {}
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps,
                        lot_size=50, fut_margin_per_lot=40_000.0)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_fut_short_session_exit_is_buy_to_cover(clean_positions,
                                                patched_fut_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0, top_n_stocks=2,
                               sizing_mode="equal", instrument_type="FUT",
                               direction="short", kill_switch_enabled=False,
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start())
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 2
    # positions registered under the FUT contract, direction short.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT symbol, direction FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchall()
    assert {r["symbol"] for r in rows} == {"AFUT", "BFUT"}
    assert all(r["direction"] == "short" for r in rows)
    # Manual kill → every exit is a BUY-to-cover.
    asyncio.run(sess.kill(reason="OPERATOR"))
    for mb in patched_fut_brokers.values():
        for call in mb.exit_calls:
            assert call["direction"] == "short"
    assert sess.status()["n_open_positions"] == 0


def test_fut_short_kill_fires_when_price_rises(clean_positions,
                                               patched_fut_brokers):
    # A short LOSES when price rises → loss-limit kill must fire on a rise.
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0, top_n_stocks=1,
                               sizing_mode="equal", instrument_type="FUT",
                               direction="short", kill_switch_enabled=True,
                               kill_switch_pct=0.01, kill_switch_direction="loss",
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # Price RISES → short loses.
    for mb in patched_fut_brokers.values():
        mb.set_ltp("AFUT", 110.0)
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert sess.status()["n_open_positions"] == 0


def test_fut_short_kill_NO_fire_when_price_falls(clean_positions,
                                                 patched_fut_brokers):
    # A short PROFITS when price falls → a loss-limit must NOT fire (no-fire case).
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0, top_n_stocks=1,
                               sizing_mode="equal", instrument_type="FUT",
                               direction="short", kill_switch_enabled=True,
                               kill_switch_pct=0.01, kill_switch_direction="loss",
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    for mb in patched_fut_brokers.values():
        mb.set_ltp("AFUT", 90.0)   # price falls → short is IN PROFIT
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is False    # loss limit must NOT fire
    assert out["gross_return"] > 0              # short is profitable
    assert sess.status()["n_open_positions"] == 1


# ── Symbol eligibility filter ──────────────────────────────────────────────────

@pytest.fixture
def patched_fut_brokers_missing_future(monkeypatch):
    created = {}
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps,
                        lot_size=50, fut_margin_per_lot=40_000.0,
                        no_future_symbols={"B"})   # B has no tradeable future
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_fut_session_skips_symbols_without_a_future(
        clean_positions, patched_fut_brokers_missing_future):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=1_500_000.0, top_n_stocks=3,
                               sizing_mode="equal", instrument_type="FUT",
                               direction="long", kill_switch_enabled=False,
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start())
    # B is F&O-ineligible → only A + C placed (never fabricated).
    assert res["status"] == "RUNNING"
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT symbol FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchall()
    syms = {r["symbol"] for r in rows}
    assert syms == {"AFUT", "CFUT"}
    assert "BFUT" not in syms


# ── REGRESSION: equity long path is byte-for-byte unchanged ────────────────────

@pytest.fixture
def patched_eq_brokers(monkeypatch):
    created = {}
    shared_ltps = {"A": 100.0, "B": 200.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_equity_long_session_order_pnl_exit_unchanged(clean_positions,
                                                      patched_eq_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               per_position_gtt_enabled=False)
    assert cfg.direction == "long"
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # positions registered under the BARE symbol (not a FUT contract), long.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT symbol, direction FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchall()
    assert {r["symbol"] for r in rows} == {"A", "B"}
    assert all(r["direction"] == "long" for r in rows)
    # P&L sign: price rises → long profits.
    for mb in patched_eq_brokers.values():
        mb.set_ltp("A", 110.0)
        mb.set_ltp("B", 210.0)
    out = asyncio.run(sess.tick())
    assert out["gross_return"] > 0          # long profits on a rise (unchanged)
    # Exit side: long → SELL (default).
    asyncio.run(sess.kill(reason="OPERATOR"))
    for mb in patched_eq_brokers.values():
        for call in mb.exit_calls:
            assert call["direction"] == "long"
    assert sess.status()["n_open_positions"] == 0


def test_equity_long_pnl_math_byte_identical(clean_positions):
    # Direct registry/monitor check: long uPnL == (ltp-avg)*qty exactly.
    sid = "s_eq_long"
    reg = PositionRegistry(sid, 100000.0)
    reg.register(symbol="A", broker_profile="p", qty=10, avg_price=100.0,
                 instrument_type="EQ")   # no direction arg → defaults long
    reg.update_ltp("A", ltp=105.0, broker_profile="p")
    mon = PortfolioMonitor(sid, 100000.0)
    assert abs(mon.total_unrealised() - 50.0) < 1e-9   # (105-100)*10


# ── EXCHANGE-CONSISTENCY (2026-07-02 F&O audit) ────────────────────────────────

def test_fut_position_row_persists_nfo_exchange(clean_positions,
                                                 patched_fut_brokers):
    """A FUT entry must persist exchange='NFO' on its position row so the GTT
    (and any exchange-keyed path) routes to the F&O segment, not NSE cash."""
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0, top_n_stocks=1,
                               sizing_mode="equal", instrument_type="FUT",
                               direction="long", kill_switch_enabled=False,
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT symbol, exchange, instrument_type FROM autotrade_positions "
            "WHERE session_id=?", (sess.session_id,)).fetchone()
    assert row["symbol"] == "AFUT"
    assert row["instrument_type"] == "FUT"
    assert row["exchange"] == "NFO"          # not NSE


def test_equity_position_row_exchange_is_nse(clean_positions, patched_eq_brokers):
    """Regression: equity entry still persists exchange='NSE' (unchanged)."""
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT exchange FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchone()
    assert row["exchange"] == "NSE"


def test_gtt_manager_routes_fut_to_nfo_even_when_row_exchange_missing():
    """GTTManager.place_for_position must never place a FUT OCO on NSE. Even when
    the stored exchange is NULL (legacy rows / backfill) it derives NFO from the
    instrument_type. Captures the exchange passed to broker.place_gtt_oco."""
    from autotrade.monitoring.gtt_manager import GTTManager

    captured = {}

    class _CaptureBroker:
        dry_run = False
        def get_ltp(self, s): return 100.0
        def place_gtt_oco(self, **kw):
            captured.update(kw)
            return "gid-1"

    class _Cfg:
        per_position_stop_pct = 0.03
        per_position_target_pct = 0.06
        order_product = "NRML"
        per_position_gtt_enabled = True

    class _Reg:
        def set_gtt(self, *a, **k): pass

    gm = GTTManager("s_gtt", _Cfg(), {"p": _CaptureBroker()}, _Reg())
    # exchange deliberately absent → must be derived as NFO from FUT.
    pos = {"symbol": "AFUT", "broker_profile": "p", "qty": 50,
           "avg_price": 100.0, "direction": "long", "instrument_type": "FUT",
           "exchange": None}
    gm.place_for_position(pos)
    assert captured.get("exchange") == "NFO"


def test_gtt_manager_equity_still_nse():
    from autotrade.monitoring.gtt_manager import GTTManager
    captured = {}

    class _CaptureBroker:
        dry_run = False
        def get_ltp(self, s): return 100.0
        def place_gtt_oco(self, **kw):
            captured.update(kw)
            return "gid-2"

    class _Cfg:
        per_position_stop_pct = 0.03
        per_position_target_pct = 0.06
        order_product = "CNC"
        per_position_gtt_enabled = True

    class _Reg:
        def set_gtt(self, *a, **k): pass

    gm = GTTManager("s_gtt2", _Cfg(), {"p": _CaptureBroker()}, _Reg())
    pos = {"symbol": "A", "broker_profile": "p", "qty": 10, "avg_price": 100.0,
           "direction": "long", "instrument_type": "EQ", "exchange": "NSE"}
    gm.place_for_position(pos)
    assert captured.get("exchange") == "NSE"


# ── GTT fill detection for a SHORT future (BUY-to-cover leg) ────────────────────

def test_gtt_execution_result_detects_short_buy_cover_fill():
    """A fired SHORT-future GTT closes via a BUY-to-cover leg. _gtt_execution_
    result must recognise a COMPLETE BUY as a confirmed fill — otherwise the
    position stays OPEN in our DB while flat at the broker and the monitor keeps
    retrying (naked-order loop)."""
    from autotrade.monitoring.gtt_manager import GTTManager
    state = {
        "status": "triggered",
        "orders": [
            {"transaction_type": "BUY", "status": "COMPLETE",
             "average_price": 103.0, "quantity": 50},
        ],
    }
    res = GTTManager._gtt_execution_result(state)
    assert res is not None
    assert res["status"] == "complete"
    assert res["exit_price"] == 103.0
    assert res["filled_qty"] == 50


def test_gtt_execution_result_long_sell_still_complete():
    """Regression: a long SELL-leg COMPLETE fill is still detected (unchanged)."""
    from autotrade.monitoring.gtt_manager import GTTManager
    state = {
        "status": "triggered",
        "orders": [
            {"transaction_type": "SELL", "status": "COMPLETE",
             "average_price": 97.0, "quantity": 10},
        ],
    }
    res = GTTManager._gtt_execution_result(state)
    assert res["status"] == "complete"
    assert res["exit_price"] == 97.0


def test_gtt_execution_result_triggered_but_pending_not_complete():
    """Regression: a triggered GTT whose closing leg is still OPEN stays pending
    (never closes prematurely) — for either side."""
    from autotrade.monitoring.gtt_manager import GTTManager
    state = {"status": "triggered",
             "orders": [{"transaction_type": "BUY", "status": "OPEN"}]}
    res = GTTManager._gtt_execution_result(state)
    assert res == {"status": "pending"}


# ── PRE-EXIT RECONCILIATION GUARD (2026-07-02 incident) ────────────────────────

def _patched_reconcile_brokers(monkeypatch, net_positions):
    created = {}
    shared_ltps = {"A": 100.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps,
                        lot_size=50, fut_margin_per_lot=40_000.0,
                        net_positions=net_positions)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_exit_single_reconciles_when_broker_already_flat(clean_positions,
                                                         monkeypatch):
    """If the broker reports the position is already flat (operator closed it),
    _exit_single_position must place NO order and mark the row CLOSED_RECONCILED,
    instead of firing a naked exit that keeps failing on retry."""
    from autotrade.session import _exit_single_position
    # Broker says AFUT is FLAT (0) — already closed externally.
    _patched_reconcile_brokers(monkeypatch, {"AFUT": 0})
    sid = "s_reconcile_flat"
    reg = PositionRegistry(sid, 1_000_000.0)
    reg.register(symbol="AFUT", broker_profile="p", qty=50, avg_price=100.0,
                 instrument_type="FUT", exchange="NFO", direction="long")
    # Build the (patched) mock broker for the unit exit path.
    from autotrade.config import BrokerProfile
    from autotrade.broker.router import build_client
    prof = BrokerProfile(profile_id="p", broker_name="zerodha",
                         allocated_capital=1_000_000.0, order_product="NRML",
                         instrument_type="FUT")
    broker = build_client(prof, dry_run=False)
    pos = {"symbol": "AFUT", "broker_profile": "p", "qty": 50,
           "avg_price": 100.0, "ltp": 100.0, "instrument_type": "FUT",
           "direction": "long", "gtt_id": None}
    res = asyncio.run(_exit_single_position(
        session_id=sid, position=pos, reason="STOP_STOCK",
        brokers={"p": broker}, registry=reg, gtt_manager=None,
        kite_product="NRML"))
    assert res["status"] == "RECONCILED_FLAT"
    # NO exit order was placed.
    assert broker.exits == []
    # Row is CLOSED (reconciled), not EXIT_FAILED.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status, close_reason FROM autotrade_positions "
            "WHERE session_id=? AND symbol='AFUT'", (sid,)).fetchone()
    assert row["status"] == "CLOSED"
    assert "RECONCILED_FLAT" in row["close_reason"]


def test_exit_single_proceeds_when_broker_still_holds(clean_positions,
                                                      monkeypatch):
    """When the broker still shows the position OPEN (net qty != 0), the exit
    proceeds normally (an order IS placed)."""
    from autotrade.session import _exit_single_position
    _patched_reconcile_brokers(monkeypatch, {"AFUT": 50})   # still 50 long
    sid = "s_reconcile_hold"
    reg = PositionRegistry(sid, 1_000_000.0)
    reg.register(symbol="AFUT", broker_profile="p", qty=50, avg_price=100.0,
                 instrument_type="FUT", exchange="NFO", direction="long")
    from autotrade.config import BrokerProfile
    from autotrade.broker.router import build_client
    prof = BrokerProfile(profile_id="p", broker_name="zerodha",
                         allocated_capital=1_000_000.0, order_product="NRML",
                         instrument_type="FUT")
    broker = build_client(prof, dry_run=False)
    pos = {"symbol": "AFUT", "broker_profile": "p", "qty": 50,
           "avg_price": 100.0, "ltp": 100.0, "instrument_type": "FUT",
           "direction": "long", "gtt_id": None}
    res = asyncio.run(_exit_single_position(
        session_id=sid, position=pos, reason="STOP_STOCK",
        brokers={"p": broker}, registry=reg, gtt_manager=None,
        kite_product="NRML"))
    assert res["status"] != "RECONCILED_FLAT"
    assert broker.exits and broker.exits[0][0] == "AFUT"


def test_exit_single_paper_unchanged_no_reconcile(clean_positions, monkeypatch):
    """Paper / broker that can't answer the net probe (returns None) → the exit
    path is byte-for-byte unchanged (an order is placed)."""
    from autotrade.session import _exit_single_position
    # net_positions=None → MockBroker.get_net_position_qty returns None.
    _patched_reconcile_brokers(monkeypatch, None)
    sid = "s_reconcile_none"
    reg = PositionRegistry(sid, 1_000_000.0)
    reg.register(symbol="AFUT", broker_profile="p", qty=50, avg_price=100.0,
                 instrument_type="FUT", exchange="NFO", direction="long")
    from autotrade.config import BrokerProfile
    from autotrade.broker.router import build_client
    prof = BrokerProfile(profile_id="p", broker_name="zerodha",
                         allocated_capital=1_000_000.0, order_product="NRML",
                         instrument_type="FUT")
    broker = build_client(prof, dry_run=False)
    pos = {"symbol": "AFUT", "broker_profile": "p", "qty": 50,
           "avg_price": 100.0, "ltp": 100.0, "instrument_type": "FUT",
           "direction": "long", "gtt_id": None}
    res = asyncio.run(_exit_single_position(
        session_id=sid, position=pos, reason="STOP_STOCK",
        brokers={"p": broker}, registry=reg, gtt_manager=None,
        kite_product="NRML"))
    assert res["status"] != "RECONCILED_FLAT"
    assert broker.exits and broker.exits[0][0] == "AFUT"
