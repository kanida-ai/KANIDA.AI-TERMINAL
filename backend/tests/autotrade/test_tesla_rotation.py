"""Falcon Tesla rotation — pure seat model + config gate.

Covers the seat allocation / per-seat denominator, the back-fill decision (free
seats, dedup, cooldown, min-grade, ranking, re-entry policy), and the config
validation that forces EQ+MIS+SHORT and the seat knobs. No I/O, no orders.
"""
from datetime import datetime, timedelta, timezone

import pytest

from autotrade.config import TradingSessionConfig
from autotrade.strategies import tesla_rotation as tr
from autotrade.strategies.tesla_short_engine import TeslaSignal

IST = timezone(timedelta(hours=5, minutes=30))
NOW = datetime(2026, 7, 8, 10, 0, tzinfo=IST)


def sig(sym, grade="A++", sd=0.7, price=100.0):
    return TeslaSignal(instrument=sym, day="2026-07-08", time="09:30",
                       bar_time="2026-07-08 09:30:00", grade=grade,
                       setup="SHORT_RELOAD_OR_BREAKDOWN", entry_ref_price=price,
                       short_drive=sd, sector="S")


# ── seat math (the correct rotation denominator) ─────────────────────────────

def test_seat_allocation_equal_split():
    assert tr.seat_allocation(900000, 3) == 300000.0


def test_seat_allocation_rejects_zero():
    with pytest.raises(ValueError):
        tr.seat_allocation(900000, 0)


def test_seat_g_uses_fixed_allocation_not_shrinking_basket():
    # A short seat: +₹6000 uPnL on a ₹300000 seat = +2% of the SEAT (leverage-
    # correct, independent of how many sibling seats are open).
    assert tr.seat_g(6000.0, 300000.0) == pytest.approx(0.02)
    assert tr.seat_g(-9000.0, 300000.0) == pytest.approx(-0.03)


# ── plan_backfill ────────────────────────────────────────────────────────────

def test_backfill_fills_free_seats_only():
    plan = tr.plan_backfill(
        signals=[sig("A"), sig("B"), sig("C"), sig("D")],
        open_symbols=["Z"], n_seats=3, total_capital=900000, now=NOW)
    assert len(plan) == 2                       # 3 seats - 1 open = 2 free
    assert all(e.allocation == 300000.0 for e in plan)


def test_backfill_none_when_full():
    plan = tr.plan_backfill(signals=[sig("A")], open_symbols=["X", "Y", "Z"],
                            n_seats=3, total_capital=900000, now=NOW)
    assert plan == []


def test_backfill_skips_already_open_symbol():
    plan = tr.plan_backfill(signals=[sig("OPEN"), sig("NEW")],
                            open_symbols=["OPEN"], n_seats=3,
                            total_capital=900000, now=NOW)
    assert [e.symbol for e in plan] == ["NEW"]


def test_backfill_skips_held_ever_by_default():
    plan = tr.plan_backfill(signals=[sig("DONE"), sig("FRESH")], open_symbols=[],
                            held_ever=["DONE"], n_seats=3, total_capital=900000,
                            now=NOW)
    assert [e.symbol for e in plan] == ["FRESH"]


def test_backfill_allows_reentry_when_enabled():
    plan = tr.plan_backfill(signals=[sig("DONE")], open_symbols=[],
                            held_ever=["DONE"], n_seats=3, total_capital=900000,
                            now=NOW, allow_reentry=True)
    assert [e.symbol for e in plan] == ["DONE"]


def test_backfill_respects_cooldown():
    last = {"C": NOW - timedelta(minutes=10)}    # entered 10 min ago
    plan = tr.plan_backfill(signals=[sig("C"), sig("D")], open_symbols=[],
                            n_seats=3, total_capital=900000, now=NOW,
                            last_entry_at=last, cooldown_minutes=30)
    assert [e.symbol for e in plan] == ["D"]     # C still cooling down


def test_backfill_cooldown_expired_allows():
    last = {"C": NOW - timedelta(minutes=40)}
    plan = tr.plan_backfill(signals=[sig("C")], open_symbols=[], n_seats=3,
                            total_capital=900000, now=NOW, last_entry_at=last,
                            cooldown_minutes=30)
    assert [e.symbol for e in plan] == ["C"]


def test_backfill_min_grade_a_plus_plus_plus_only():
    plan = tr.plan_backfill(signals=[sig("A", "A++"), sig("B", "A+++")],
                            open_symbols=[], n_seats=3, total_capital=900000,
                            now=NOW, min_grade="A+++")
    assert [e.symbol for e in plan] == ["B"]


def test_backfill_ranks_strongest_first():
    # one free seat → the A+++ (or highest short_drive) must win.
    plan = tr.plan_backfill(
        signals=[sig("WEAK", "A++", 0.62), sig("STRONG", "A+++", 0.80)],
        open_symbols=["a", "b"], n_seats=3, total_capital=900000, now=NOW)
    assert [e.symbol for e in plan] == ["STRONG"]


def test_backfill_ranks_by_short_drive_within_grade():
    plan = tr.plan_backfill(
        signals=[sig("LO", "A++", 0.61), sig("HI", "A++", 0.90)],
        open_symbols=["a", "b"], n_seats=3, total_capital=900000, now=NOW)
    assert [e.symbol for e in plan] == ["HI"]


def test_backfill_dedups_within_plan():
    plan = tr.plan_backfill(signals=[sig("A"), sig("A")], open_symbols=[],
                            n_seats=3, total_capital=900000, now=NOW)
    assert [e.symbol for e in plan] == ["A"]


def test_backfill_skips_bad_price():
    plan = tr.plan_backfill(signals=[sig("A", price=0.0), sig("B")],
                            open_symbols=[], n_seats=3, total_capital=900000,
                            now=NOW)
    assert [e.symbol for e in plan] == ["B"]


# ── config gate ──────────────────────────────────────────────────────────────

def _cfg(**over):
    base = dict(total_allocated_capital=900000, strategy="tesla_short",
                direction="short", instrument_type="EQ", order_product="MIS",
                n_seats=3)
    base.update(over)
    return TradingSessionConfig(**base)


def test_config_valid_tesla_short_roundtrips():
    c = _cfg()
    c.validate()
    back = TradingSessionConfig.from_json(c.to_json())
    assert back.strategy == "tesla_short" and back.n_seats == 3
    assert back.tesla_min_grade == "A++"
    assert back.tesla_personality_window_days == 5


@pytest.mark.parametrize("over", [
    dict(order_product="CNC"),
    dict(order_product="MTF"),
    dict(direction="long"),
    dict(instrument_type="FUT", order_product="NRML"),
    dict(n_seats=0),
    dict(tesla_min_grade="B"),
    dict(tesla_personality_window_days=0),
    dict(tesla_cooldown_minutes=-1),
    dict(square_off_enabled=False),
])
def test_config_rejects_bad_tesla(over):
    with pytest.raises(ValueError):
        _cfg(**over).validate()


def test_config_default_strategy_unaffected():
    c = TradingSessionConfig(total_allocated_capital=500000)
    c.validate()
    assert c.strategy == "portfolio_kill_switch"
    # tesla knobs carry inert defaults on a non-tesla session
    assert c.n_seats == 3 and c.tesla_signal_db_path is None
