"""Tests for the Daily Trade Journal endpoint (BUILD 2).

GET /autotrade/session/{session_id}/journal

Covers:
  test_journal_closed_session     — 3 positions (2 GTT-stop, 1 open); correct counts
  test_journal_running_session    — RUNNING session, mix open/closed → no crash
  test_journal_not_found          — 404 on unknown session_id
  test_review_flags               — CONTINUED_DECLINE_OPEN + WINNER_OPEN flags
  test_best_worst_trade           — identifies max/min pnl_rs among closed positions
  test_stop_vs_target_gtt         — heuristic distinguishes stop vs target GTT leg
  test_n_trail_exits              — TRAIL_EXIT / FLOOR_EXIT counted separately
  test_n_kill_switch              — KILL_SWITCH close_reason counted
  test_pnl_calculations           — invested_rs, pnl_rs, pnl_pct computed correctly
  test_avg_hold_minutes           — average hold time across closed positions
  test_leverage                   — leverage = invested_basis / fund
  test_deep_loss_flag             — pnl_pct < -4% → DEEP_LOSS flag
  test_large_win_flag             — pnl_pct > 3% → LARGE_WIN flag
  test_open_position_no_exit      — OPEN pos with no exit_price is safe
  test_journal_empty_positions    — session with zero positions returns safely
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from autotrade.api.journal_routes import build_journal
from fastapi import HTTPException

IST = timezone(timedelta(hours=5, minutes=30))


# ── DB helpers ────────────────────────────────────────────────────────────────

def _sid():
    return uuid.uuid4().hex


def _now_ist() -> str:
    return datetime.now(IST).isoformat()


def _insert_session(sid: str, *,
                    status: str = "CLOSED",
                    mode: str = "paper",
                    fund: float = 100_000.0,
                    invested_basis: Optional[float] = None,
                    config_json: str = '{"strategy": "portfolio_kill_switch"}',
                    started_at: Optional[str] = None,
                    entry_latency_ms: Optional[int] = None,
                    exit_latency_ms: Optional[int] = None):
    from falcon.db import falcon_conn
    inv = invested_basis if invested_basis is not None else fund
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, started_at, status, mode,
                total_allocated_capital, invested_basis, config_json,
                entry_latency_ms, exit_latency_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (sid, "2026-06-28T09:00:00+05:30",
             started_at or "2026-06-28T09:15:00+05:30",
             status, mode, fund, inv, config_json,
             entry_latency_ms, exit_latency_ms))
        con.commit()


def _insert_position(sid: str, symbol: str, *,
                     qty: int = 10,
                     avg_price: float = 1000.0,
                     status: str = "OPEN",
                     ltp: Optional[float] = None,
                     unrealised_pnl: Optional[float] = None,
                     exit_price: Optional[float] = None,
                     realised_pnl: Optional[float] = None,
                     close_reason: Optional[str] = None,
                     opened_at: str = "2026-06-28T09:15:00+05:30",
                     closed_at: Optional[str] = None,
                     sl_level: Optional[float] = None,
                     target_price: Optional[float] = None,
                     gtt_stop: Optional[float] = None,
                     gtt_target: Optional[float] = None):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_positions
               (session_id, symbol, instrument_type, exchange, qty, avg_price,
                sl_level, target_price, ltp, unrealised_pnl, status,
                exit_lock, opened_at, closed_at, exit_price, realised_pnl,
                close_reason, gtt_stop, gtt_target)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sid, symbol, "EQ", "NSE", qty, avg_price,
             sl_level, target_price, ltp, unrealised_pnl, status,
             0, opened_at, closed_at, exit_price, realised_pnl,
             close_reason, gtt_stop, gtt_target))
        con.commit()


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestJournalNotFound:
    def test_journal_not_found(self, clean_positions):
        with pytest.raises(HTTPException) as exc_info:
            build_journal("nonexistent-session-id")
        assert exc_info.value.status_code == 404


class TestJournalClosedSession:
    def test_journal_closed_session(self, clean_positions):
        """3 positions: 2 GTT stop hits + 1 open — verify summary counts."""
        sid = _sid()
        _insert_session(sid, status="CLOSED", fund=100_000.0, invested_basis=30_000.0)

        # pos1: GTT stop (exit_price < avg_price) — loss
        _insert_position(sid, "INFY", qty=10, avg_price=1500.0,
                         status="CLOSED", exit_price=1440.0,
                         realised_pnl=-600.0, close_reason="GTT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T11:00:00+05:30")
        # pos2: GTT target (exit_price > avg_price) — win
        _insert_position(sid, "TCS", qty=5, avg_price=4000.0,
                         status="CLOSED", exit_price=4200.0,
                         realised_pnl=1000.0, close_reason="GTT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T12:30:00+05:30")
        # pos3: still OPEN
        _insert_position(sid, "RELIANCE", qty=5, avg_price=2800.0,
                         status="OPEN", ltp=2850.0, unrealised_pnl=250.0)

        j = build_journal(sid)

        assert j["session_id"] == sid
        assert j["mode"] == "paper"
        assert j["strategy"] == "portfolio_kill_switch"

        s = j["session_summary"]
        assert s["n_positions"] == 3
        assert s["n_closed"] == 2
        assert s["n_open"] == 1
        assert s["n_winners"] == 1     # TCS realised_pnl > 0
        assert s["n_losers"] == 1      # INFY realised_pnl < 0
        assert s["n_stop_hits"] == 1   # INFY exit < avg → stop
        assert s["n_target_hits"] == 1 # TCS exit > avg → target
        assert s["n_trail_exits"] == 0
        assert s["n_square_off"] == 0
        assert s["n_kill_switch"] == 0

        # PnL: realised = -600 + 1000 = 400; unrealised = 250; total = 650
        assert abs(s["total_realised_pnl"] - 400.0) < 0.01
        assert abs(s["total_unrealised_pnl"] - 250.0) < 0.01
        assert abs(s["total_pnl"] - 650.0) < 0.01

        # Leverage: invested_basis / fund = 30000 / 100000 = 0.3
        assert abs(s["leverage"] - 0.3) < 1e-4

        # invested_basis expressed in pct
        assert s["total_pnl_pct_invested"] == round(650.0 / 30_000.0 * 100, 4)
        assert s["total_pnl_pct_fund"] == round(650.0 / 100_000.0 * 100, 4)

        assert s["session_status"] == "CLOSED"

        # Positions list
        syms = {p["symbol"] for p in j["positions"]}
        assert syms == {"INFY", "TCS", "RELIANCE"}

    def test_positions_include_invested_rs(self, clean_positions):
        """invested_rs = qty * avg_price per position."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        _insert_position(sid, "WIPRO", qty=20, avg_price=500.0,
                         status="CLOSED", exit_price=510.0,
                         realised_pnl=200.0, close_reason="SQUARE_OFF",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T14:00:00+05:30")
        j = build_journal(sid)
        pos = j["positions"][0]
        assert pos["invested_rs"] == 20 * 500.0
        assert pos["pnl_rs"] == 200.0
        assert abs(pos["pnl_pct"] - (200.0 / 10_000.0 * 100)) < 0.001


class TestJournalRunningSession:
    def test_journal_running_session(self, clean_positions):
        """Running session with a mix of open/closed — must not crash."""
        sid = _sid()
        _insert_session(sid, status="RUNNING", fund=200_000.0)
        _insert_position(sid, "HDFCBANK", qty=50, avg_price=1700.0,
                         status="OPEN", ltp=1720.0, unrealised_pnl=1000.0)
        _insert_position(sid, "KOTAKBANK", qty=20, avg_price=1800.0,
                         status="CLOSED", exit_price=1830.0,
                         realised_pnl=600.0, close_reason="TRAIL_EXIT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T11:30:00+05:30")

        j = build_journal(sid)
        s = j["session_summary"]
        assert s["session_status"] == "RUNNING"
        assert s["n_open"] == 1
        assert s["n_closed"] == 1
        assert s["n_trail_exits"] == 1
        # total_pnl = realised 600 + unrealised 1000
        assert abs(s["total_pnl"] - 1600.0) < 0.01

        # OPEN position has no exit_price
        hdfcbank_pos = next(p for p in j["positions"] if p["symbol"] == "HDFCBANK")
        assert hdfcbank_pos["exit_price"] is None
        assert hdfcbank_pos["unrealised_pnl"] == 1000.0


class TestJournalEmptyPositions:
    def test_journal_empty_positions(self, clean_positions):
        """Session with zero positions returns safely with zero counts."""
        sid = _sid()
        _insert_session(sid, status="CREATED")
        j = build_journal(sid)
        s = j["session_summary"]
        assert s["n_positions"] == 0
        assert s["n_closed"] == 0
        assert s["n_open"] == 0
        assert s["total_pnl"] == 0.0
        assert s["best_trade"] is None
        assert s["worst_trade"] is None
        assert s["avg_hold_minutes"] is None
        assert j["positions"] == []
        assert j["review_items"] == []


class TestReviewFlags:
    def test_continued_decline_open(self, clean_positions):
        """OPEN position with ltp < avg*0.985 → CONTINUED_DECLINE_OPEN."""
        sid = _sid()
        _insert_session(sid, status="RUNNING")
        avg = 1000.0
        ltp = avg * 0.98   # -2%, below -1.5% threshold
        _insert_position(sid, "ZOMATO", qty=100, avg_price=avg,
                         status="OPEN", ltp=ltp,
                         unrealised_pnl=(ltp - avg) * 100)

        j = build_journal(sid)
        pos = j["positions"][0]
        assert pos["review_flag"] == "CONTINUED_DECLINE_OPEN"
        assert "declining" in (pos["review_note"] or "").lower()

        # review_items should reflect it
        ri = j["review_items"]
        assert len(ri) == 1
        assert ri[0]["symbol"] == "ZOMATO"
        assert ri[0]["flag"] == "CONTINUED_DECLINE_OPEN"

    def test_winner_open(self, clean_positions):
        """OPEN position with ltp > avg*1.01 → WINNER_OPEN."""
        sid = _sid()
        _insert_session(sid, status="RUNNING")
        avg = 500.0
        ltp = avg * 1.02   # +2%
        _insert_position(sid, "BAJFINANCE", qty=10, avg_price=avg,
                         status="OPEN", ltp=ltp,
                         unrealised_pnl=(ltp - avg) * 10)

        j = build_journal(sid)
        pos = j["positions"][0]
        assert pos["review_flag"] == "WINNER_OPEN"

    def test_deep_loss_flag(self, clean_positions):
        """pnl_pct < -4% → DEEP_LOSS (highest priority)."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        # -5% loss: 10 * 1000 invested, -500 realised
        _insert_position(sid, "PAYTM", qty=10, avg_price=1000.0,
                         status="CLOSED", exit_price=950.0,
                         realised_pnl=-500.0, close_reason="GTT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T13:00:00+05:30")
        j = build_journal(sid)
        pos = j["positions"][0]
        assert pos["pnl_pct"] < -4.0
        assert pos["review_flag"] == "DEEP_LOSS"

    def test_large_win_flag(self, clean_positions):
        """pnl_pct > 3% → LARGE_WIN."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        # +5% win: 10 * 1000 invested, +500 realised
        _insert_position(sid, "TATAPOWER", qty=10, avg_price=1000.0,
                         status="CLOSED", exit_price=1050.0,
                         realised_pnl=500.0, close_reason="TRAIL_EXIT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T14:00:00+05:30")
        j = build_journal(sid)
        pos = j["positions"][0]
        assert pos["pnl_pct"] > 3.0
        assert pos["review_flag"] == "LARGE_WIN"

    def test_stop_recovered_flag(self, clean_positions):
        """GTT stop (exit < avg) → STOP_RECOVERED flag."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        _insert_position(sid, "PVR", qty=10, avg_price=1400.0,
                         status="CLOSED", exit_price=1358.0,   # 3% below avg
                         realised_pnl=-420.0, close_reason="GTT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T10:00:00+05:30")
        j = build_journal(sid)
        pos = j["positions"][0]
        # pnl_pct = -420 / 14000 * 100 = -3.0 — not deep enough for DEEP_LOSS (>-4%)
        assert pos["pnl_pct"] > -4.0
        assert pos["review_flag"] == "STOP_RECOVERED"
        assert "stopped" in (pos["review_note"] or "").lower()

    def test_no_flag_for_flat_position(self, clean_positions):
        """Position near entry price should have no flag."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        _insert_position(sid, "ITC", qty=50, avg_price=400.0,
                         status="CLOSED", exit_price=401.0,
                         realised_pnl=50.0, close_reason="SQUARE_OFF",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T15:00:00+05:30")
        j = build_journal(sid)
        pos = j["positions"][0]
        assert pos["review_flag"] is None
        assert pos["review_note"] is None
        assert j["review_items"] == []


class TestBestWorstTrade:
    def test_best_worst_trade(self, clean_positions):
        """best_trade = highest realised_pnl; worst_trade = lowest."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")

        # Three closed positions with distinct P&L.
        _insert_position(sid, "WINNER", qty=10, avg_price=1000.0,
                         status="CLOSED", exit_price=1100.0,
                         realised_pnl=1000.0, close_reason="TRAIL_EXIT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T12:00:00+05:30")
        _insert_position(sid, "MIDDLE", qty=10, avg_price=1000.0,
                         status="CLOSED", exit_price=1020.0,
                         realised_pnl=200.0, close_reason="SQUARE_OFF",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T14:00:00+05:30")
        _insert_position(sid, "LOSER", qty=10, avg_price=1000.0,
                         status="CLOSED", exit_price=960.0,
                         realised_pnl=-400.0, close_reason="GTT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T10:30:00+05:30")

        j = build_journal(sid)
        s = j["session_summary"]

        assert s["best_trade"]["symbol"] == "WINNER"
        assert s["best_trade"]["pnl_rs"] == 1000.0
        assert s["worst_trade"]["symbol"] == "LOSER"
        assert s["worst_trade"]["pnl_rs"] == -400.0

    def test_best_worst_none_when_no_closed(self, clean_positions):
        """No closed positions → best/worst are None."""
        sid = _sid()
        _insert_session(sid, status="RUNNING")
        _insert_position(sid, "LIVE", qty=5, avg_price=1000.0,
                         status="OPEN", ltp=1010.0, unrealised_pnl=50.0)
        j = build_journal(sid)
        assert j["session_summary"]["best_trade"] is None
        assert j["session_summary"]["worst_trade"] is None


class TestStopVsTargetGTT:
    def test_stop_vs_target_distinction(self, clean_positions):
        """GTT heuristic: exit < avg → stop_hit; exit > avg → target_hit."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")

        _insert_position(sid, "STOPSTOCK", qty=10, avg_price=1000.0,
                         status="CLOSED", exit_price=970.0,   # below avg → stop
                         realised_pnl=-300.0, close_reason="GTT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T10:00:00+05:30")
        _insert_position(sid, "TARGETSTOCK", qty=10, avg_price=1000.0,
                         status="CLOSED", exit_price=1060.0,  # above avg → target
                         realised_pnl=600.0, close_reason="GTT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T14:00:00+05:30")

        j = build_journal(sid)
        s = j["session_summary"]
        assert s["n_stop_hits"] == 1
        assert s["n_target_hits"] == 1


class TestCloseReasonCounts:
    def test_n_trail_exits(self, clean_positions):
        """TRAIL_EXIT + FLOOR_EXIT both count as n_trail_exits."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        _insert_position(sid, "A", qty=10, avg_price=100.0,
                         status="CLOSED", exit_price=110.0,
                         realised_pnl=100.0, close_reason="TRAIL_EXIT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T12:00:00+05:30")
        _insert_position(sid, "B", qty=10, avg_price=100.0,
                         status="CLOSED", exit_price=108.0,
                         realised_pnl=80.0, close_reason="FLOOR_EXIT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T13:00:00+05:30")
        _insert_position(sid, "C", qty=10, avg_price=100.0,
                         status="CLOSED", exit_price=95.0,
                         realised_pnl=-50.0, close_reason="KILL_SWITCH",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T15:00:00+05:30")
        j = build_journal(sid)
        s = j["session_summary"]
        assert s["n_trail_exits"] == 2
        assert s["n_kill_switch"] == 1

    def test_n_square_off(self, clean_positions):
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        for sym in ("X", "Y"):
            _insert_position(sid, sym, qty=5, avg_price=500.0,
                             status="CLOSED", exit_price=505.0,
                             realised_pnl=25.0, close_reason="SQUARE_OFF",
                             opened_at="2026-06-28T09:15:00+05:30",
                             closed_at="2026-06-28T15:25:00+05:30")
        j = build_journal(sid)
        assert j["session_summary"]["n_square_off"] == 2


class TestPnLCalculations:
    def test_pnl_rs_closed_uses_realised(self, clean_positions):
        """Closed position: pnl_rs = realised_pnl."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        _insert_position(sid, "TATA", qty=10, avg_price=1000.0,
                         status="CLOSED", exit_price=1050.0,
                         realised_pnl=500.0, close_reason="TRAIL_EXIT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T14:00:00+05:30")
        j = build_journal(sid)
        pos = j["positions"][0]
        assert pos["pnl_rs"] == 500.0
        assert abs(pos["pnl_pct"] - (500.0 / 10_000.0 * 100)) < 0.001

    def test_pnl_rs_open_uses_unrealised(self, clean_positions):
        """Open position: pnl_rs = unrealised_pnl."""
        sid = _sid()
        _insert_session(sid, status="RUNNING")
        _insert_position(sid, "HDFC", qty=10, avg_price=2000.0,
                         status="OPEN", ltp=2050.0, unrealised_pnl=500.0)
        j = build_journal(sid)
        pos = j["positions"][0]
        assert pos["pnl_rs"] == 500.0
        assert pos["realised_pnl"] is None


class TestAvgHoldMinutes:
    def test_avg_hold_minutes(self, clean_positions):
        """avg_hold_minutes is mean of (closed_at - opened_at) in minutes."""
        sid = _sid()
        _insert_session(sid, status="CLOSED")
        # 60 min hold
        _insert_position(sid, "ALPHA", qty=10, avg_price=100.0,
                         status="CLOSED", exit_price=105.0,
                         realised_pnl=50.0, close_reason="TRAIL_EXIT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T10:15:00+05:30")
        # 120 min hold
        _insert_position(sid, "BETA", qty=10, avg_price=100.0,
                         status="CLOSED", exit_price=110.0,
                         realised_pnl=100.0, close_reason="TRAIL_EXIT",
                         opened_at="2026-06-28T09:15:00+05:30",
                         closed_at="2026-06-28T11:15:00+05:30")
        j = build_journal(sid)
        # avg = (60 + 120) / 2 = 90.0
        assert j["session_summary"]["avg_hold_minutes"] == 90.0


class TestLeverage:
    def test_leverage_calculation(self, clean_positions):
        """leverage = invested_basis / total_allocated_capital."""
        sid = _sid()
        # MTF scenario: fund=100k, invested=260k → leverage ~2.6
        _insert_session(sid, fund=100_000.0, invested_basis=260_000.0)
        j = build_journal(sid)
        assert abs(j["session_summary"]["leverage"] - 2.6) < 0.001

    def test_leverage_defaults_to_one_when_no_invested(self, clean_positions):
        """When invested_basis is NULL / 0 falls back to fund → leverage=1."""
        sid = _sid()
        _insert_session(sid, fund=100_000.0, invested_basis=0.0)
        j = build_journal(sid)
        # fallback: inv = fund when inv_basis is 0, so leverage = 1.0
        assert j["session_summary"]["leverage"] == 1.0


class TestLatencyFields:
    def test_latency_fields_returned(self, clean_positions):
        """entry_latency_ms + exit_latency_ms surfaced in session_summary."""
        sid = _sid()
        _insert_session(sid, entry_latency_ms=312, exit_latency_ms=87)
        j = build_journal(sid)
        assert j["session_summary"]["entry_latency_ms"] == 312
        assert j["session_summary"]["exit_latency_ms"] == 87

    def test_latency_none_when_not_set(self, clean_positions):
        sid = _sid()
        _insert_session(sid)
        j = build_journal(sid)
        assert j["session_summary"]["entry_latency_ms"] is None
        assert j["session_summary"]["exit_latency_ms"] is None
