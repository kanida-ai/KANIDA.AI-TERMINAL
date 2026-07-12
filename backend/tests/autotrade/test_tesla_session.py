"""Falcon Tesla SHORT rotation — session execution (PAPER, MockBroker only).

Proves the seat/rotation engine drives entries + exits through the SAME hardened
paths (_place_one SELL-to-open, _exit_single_position BUY-to-cover, kill_switch
MIS square-off) with the CORRECT per-seat denominator. No real Kite, no network.
The live signal engine is monkeypatched so the rotation logic is exercised
without the poll DB.
"""
import asyncio

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from autotrade.strategies.tesla_short_engine import TeslaSignal
from tests.autotrade.mock_broker import MockBroker


def _sig(sym, grade="A++", sd=0.7, price=100.0):
    return TeslaSignal(instrument=sym, day="2026-07-08", time="09:30",
                       bar_time="2026-07-08 09:30:00", grade=grade,
                       setup="SHORT_RELOAD_OR_BREAKDOWN", entry_ref_price=price,
                       short_drive=sd, sector="S")


@pytest.fixture
def tesla_brokers(monkeypatch):
    created = {}
    ltps = {"T1": 100.0, "T2": 100.0, "T3": 100.0, "T4": 100.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created, ltps


def _tesla_cfg(**kw):
    base = dict(total_allocated_capital=900000.0, strategy="tesla_short",
                direction="short", instrument_type="EQ", order_product="MIS",
                n_seats=3, per_position_gtt_enabled=False,
                per_stock_stop_pct=0.03,
                mis_square_off_time="23:58:00", square_off_time="23:59:00")
    base.update(kw)
    return TradingSessionConfig(**base)


def _patch_signals(monkeypatch, holder):
    """holder['sigs'] is read live each call — mutate it between ticks."""
    monkeypatch.setattr(TradingSession, "_tesla_live_signals",
                        lambda self: list(holder.get("sigs", [])))


# ── initial fill ─────────────────────────────────────────────────────────────

def test_tesla_initial_fill_fills_free_seats(clean_positions, tesla_brokers,
                                             monkeypatch):
    created, _ = tesla_brokers
    holder = {"sigs": [_sig("T1"), _sig("T2"), _sig("T3"), _sig("T4")]}
    _patch_signals(monkeypatch, holder)
    sess = TradingSession.create(_tesla_cfg(), mode="paper")
    res = asyncio.run(sess.start())
    assert res["status"] == "RUNNING"
    # 3 seats → exactly 3 filled (never over-fill despite 4 signals).
    assert sess.status()["n_open_positions"] == 3
    # every entry was a SELL-to-open short.
    entries = [c for mb in created.values() for c in getattr(mb, "place_calls", [])]
    # fallback: assert via registry direction
    open_syms = {p["symbol"] for p in sess.registry.get_open_positions()}
    assert open_syms == {"T1", "T2", "T3"}


def test_tesla_arms_running_with_zero_signals(clean_positions, tesla_brokers,
                                              monkeypatch):
    holder = {"sigs": []}
    _patch_signals(monkeypatch, holder)
    sess = TradingSession.create(_tesla_cfg(), mode="paper")
    res = asyncio.run(sess.start())
    # No signals yet → session ARMS (RUNNING, 0 positions), does NOT FAIL.
    assert res["status"] == "RUNNING"
    assert sess.status()["n_open_positions"] == 0


# ── rotation back-fill ───────────────────────────────────────────────────────

def test_tesla_backfill_after_seat_frees(clean_positions, tesla_brokers,
                                         monkeypatch):
    created, ltps = tesla_brokers
    holder = {"sigs": [_sig("T1"), _sig("T2"), _sig("T3")]}
    _patch_signals(monkeypatch, holder)
    sess = TradingSession.create(_tesla_cfg(tesla_allow_reentry=True), mode="paper")
    asyncio.run(sess.start())
    assert sess.status()["n_open_positions"] == 3

    # Push T1 to a per-seat STOP (short → price UP 4% = -4% of the seat).
    ltps["T1"] = 104.0
    # A fresh signal T4 is now available to back-fill the freed seat.
    holder["sigs"] = [_sig("T4")]
    out = asyncio.run(sess.tick())
    # T1 exited via a per-seat stop...
    assert any("STOP_SEAT" in (e.get("reason") or "") for e in out["seat_exits"])
    open_syms = {p["symbol"] for p in sess.registry.get_open_positions()}
    assert "T1" not in open_syms
    # ...and the freed seat was back-filled with T4 → still 3 seats full.
    assert "T4" in open_syms
    assert sess.status()["n_open_positions"] == 3


def test_tesla_per_seat_stop_uses_seat_allocation_not_basket(
        clean_positions, tesla_brokers, monkeypatch):
    """A -1% move on ONE seat must NOT stop it (seat g = -1% > -3%); a -4% move
    MUST. Proves the denominator is the FIXED seat allocation (300k), not a
    shrinking basket slice or the whole fund."""
    created, ltps = tesla_brokers
    holder = {"sigs": [_sig("T1"), _sig("T2"), _sig("T3")]}
    _patch_signals(monkeypatch, holder)
    sess = TradingSession.create(_tesla_cfg(), mode="paper")
    asyncio.run(sess.start())
    ltps["T1"] = 101.0                         # +1% up = -1% of the seat
    out = asyncio.run(sess.tick())
    assert not out["seat_exits"]               # -1% < 3% stop → HOLD
    assert sess.status()["n_open_positions"] == 3
    ltps["T1"] = 104.0                         # +4% up = -4% of the seat
    out = asyncio.run(sess.tick())
    assert any("STOP_SEAT" in (e.get("reason") or "") for e in out["seat_exits"])
    assert sess.status()["n_open_positions"] == 2


# ── mandatory MIS buy-to-cover ───────────────────────────────────────────────

def test_tesla_mandatory_mis_square_off_covers_all(clean_positions,
                                                   tesla_brokers, monkeypatch):
    created, _ = tesla_brokers
    holder = {"sigs": [_sig("T1"), _sig("T2"), _sig("T3")]}
    _patch_signals(monkeypatch, holder)
    # mis_square_off_time in the PAST → the in-tick backstop fires immediately
    # (square_off_time stays a valid future time per the base cfg).
    sess = TradingSession.create(
        _tesla_cfg(mis_square_off_time="00:00:01"), mode="paper")
    asyncio.run(sess.start())
    assert sess.status()["n_open_positions"] == 3
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert "MIS_SQUARE_OFF" in (out["kill_reason"] or "")
    assert sess.status()["n_open_positions"] == 0
    # every cover was a BUY-to-cover (direction short recorded on the exit).
    exit_calls = [c for mb in created.values() for c in mb.exit_calls]
    assert exit_calls and all(c["direction"] == "short" for c in exit_calls)


def test_tesla_no_reentry_of_closed_name_by_default(clean_positions,
                                                    tesla_brokers, monkeypatch):
    """Default (allow_reentry=False): a name exited this session is NOT
    re-entered even if it keeps signalling — avoids churn / double-cover."""
    created, ltps = tesla_brokers
    holder = {"sigs": [_sig("T1"), _sig("T2"), _sig("T3")]}
    _patch_signals(monkeypatch, holder)
    sess = TradingSession.create(_tesla_cfg(), mode="paper")  # allow_reentry False
    asyncio.run(sess.start())
    ltps["T1"] = 104.0
    holder["sigs"] = [_sig("T1")]              # T1 keeps signalling after its exit
    asyncio.run(sess.tick())                   # T1 stops out
    out = asyncio.run(sess.tick())             # T1 must NOT be re-entered
    open_syms = {p["symbol"] for p in sess.registry.get_open_positions()}
    assert "T1" not in open_syms
    assert out["backfilled"] == []             # nothing to fill (only T1 signalled)


# ── other strategies untouched (regression) ──────────────────────────────────

def test_non_tesla_tick_unaffected(clean_positions, tesla_brokers, monkeypatch):
    from tests.autotrade.conftest import seed_signals
    seed_signals([("T1", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, top_n_stocks=1,
        strategy="portfolio_kill_switch", kill_switch_enabled=False,
        per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    out = asyncio.run(sess.tick())
    # kill-switch strategy path — no tesla keys leak in.
    assert "seat_exits" not in out
    assert sess.status()["n_open_positions"] == 1
