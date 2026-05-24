"""Lock semantics + scheduler tests.

Operator policy (locked 2026-05-14):
  9:30 ENTER/SKIP decisions are final.
  9:45 and 10:00 cycles only re-evaluate rows that were WAIT in earlier cycles.

These tests use the real run_cycle path with a STUBBED Kite client to drive
deterministic decisions. The lock logic is the only thing being tested — we
don't hit real broker.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from power_user.db_init import init_power_user_schema
from power_user.services.live_tier import (
    EARLIER_CYCLES,
    LOCKED_ACTIONS,
    _lookup_prior_decision,
    apply_tier_rule,
    get_decisions,
    tier_of,
)


@pytest.fixture
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_power_user_schema(path)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


def _insert_decision(con: sqlite3.Connection, *,
                      entry_date: str, cycle: str, rank: int,
                      action: str, decided_at: str = None,
                      tier: str = "ELITE", symbol: str = "X"):
    """Helper: drop a row into falcon_live_decisions."""
    con.execute("""
        INSERT INTO falcon_live_decisions
          (signal_date, entry_date, cycle, rank, symbol, tier, action,
           reason, decided_at_cycle, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("2026-05-13", entry_date, cycle, rank, symbol, tier, action,
          "test", decided_at or cycle, "2026-05-14T09:30:30+05:30"))
    con.commit()


# ──────────────────────────────────────────────────────────────────────────
# EARLIER_CYCLES mapping
# ──────────────────────────────────────────────────────────────────────────

class TestEarlierCyclesMapping:

    def test_0930_has_no_earlier(self):
        assert EARLIER_CYCLES["0930"] == []

    def test_0945_consults_0930(self):
        assert EARLIER_CYCLES["0945"] == ["0930"]

    def test_1000_consults_0945_then_0930(self):
        assert EARLIER_CYCLES["1000"] == ["0945", "0930"]

    def test_locked_actions_are_enter_and_skip(self):
        assert LOCKED_ACTIONS == frozenset({"ENTER", "SKIP"})
        assert "WAIT" not in LOCKED_ACTIONS


# ──────────────────────────────────────────────────────────────────────────
# _lookup_prior_decision — the heart of the lock rule
# ──────────────────────────────────────────────────────────────────────────

class TestLookupPriorDecision:

    def test_0930_never_looks_back(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            # Even if a row somehow existed for cycle 0930, the lookup should
            # not find one — there are no cycles before 0930.
            result = _lookup_prior_decision(con, "2026-05-15", 1, "0930")
        assert result is None

    def test_0945_finds_locked_enter_from_0930(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            _insert_decision(con, entry_date="2026-05-15", cycle="0930",
                             rank=1, action="ENTER")
            result = _lookup_prior_decision(con, "2026-05-15", 1, "0945")
        assert result is not None
        assert result["action"] == "ENTER"
        assert result["decided_at_cycle"] == "0930"

    def test_0945_finds_locked_skip_from_0930(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            _insert_decision(con, entry_date="2026-05-15", cycle="0930",
                             rank=5, action="SKIP")
            result = _lookup_prior_decision(con, "2026-05-15", 5, "0945")
        assert result is not None
        assert result["action"] == "SKIP"

    def test_0945_ignores_wait_from_0930(self, tmp_db):
        """WAIT is NOT locked — 0945 should re-evaluate."""
        with sqlite3.connect(tmp_db) as con:
            _insert_decision(con, entry_date="2026-05-15", cycle="0930",
                             rank=1, action="WAIT")
            result = _lookup_prior_decision(con, "2026-05-15", 1, "0945")
        assert result is None   # signals to re-evaluate

    def test_1000_prefers_0945_over_0930(self, tmp_db):
        """If 0945 already decided ENTER, 1000 uses that (most recent first)."""
        with sqlite3.connect(tmp_db) as con:
            # 0930 WAITed, then 0945 decided SKIP, now 1000 looks
            _insert_decision(con, entry_date="2026-05-15", cycle="0930",
                             rank=1, action="WAIT")
            _insert_decision(con, entry_date="2026-05-15", cycle="0945",
                             rank=1, action="SKIP", decided_at="0945")
            result = _lookup_prior_decision(con, "2026-05-15", 1, "1000")
        assert result["action"] == "SKIP"
        assert result["decided_at_cycle"] == "0945"

    def test_1000_falls_through_to_0930_when_0945_is_wait(self, tmp_db):
        """0930 ENTER + 0945 WAIT → 1000 still locks on 0930's ENTER."""
        with sqlite3.connect(tmp_db) as con:
            _insert_decision(con, entry_date="2026-05-15", cycle="0930",
                             rank=1, action="ENTER")
            _insert_decision(con, entry_date="2026-05-15", cycle="0945",
                             rank=1, action="WAIT")
            result = _lookup_prior_decision(con, "2026-05-15", 1, "1000")
        assert result is not None
        assert result["action"] == "ENTER"
        assert result["decided_at_cycle"] == "0930"

    def test_no_prior_rows_returns_none(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            result = _lookup_prior_decision(con, "2026-05-15", 1, "1000")
        assert result is None


# ──────────────────────────────────────────────────────────────────────────
# get_decisions exposes decided_at_cycle + locked count
# ──────────────────────────────────────────────────────────────────────────

class TestGetDecisionsLockMetadata:

    def test_decided_at_cycle_in_response(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            # Row for cycle 0945 that was actually decided at 0930
            _insert_decision(con, entry_date="2026-05-15", cycle="0945",
                             rank=1, action="ENTER", decided_at="0930")
            result = get_decisions(con, entry_date="2026-05-15", cycle="0945")
        assert len(result["decisions"]) == 1
        d = result["decisions"][0]
        assert d["decided_at_cycle"] == "0930"
        assert d["action"] == "ENTER"
        # Summary should report it as locked
        assert result["summary"]["locked"] == 1

    def test_fresh_decision_locked_count_zero(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            _insert_decision(con, entry_date="2026-05-15", cycle="0930",
                             rank=1, action="ENTER", decided_at="0930")
            result = get_decisions(con, entry_date="2026-05-15", cycle="0930")
        assert result["summary"]["locked"] == 0


# ──────────────────────────────────────────────────────────────────────────
# run_cycle integration — full lock flow with stubbed Kite
# ──────────────────────────────────────────────────────────────────────────

class FakeKite:
    """Minimal Kite stub for run_cycle tests. Returns the configured intraday
    data for every symbol so we can drive deterministic decisions.

    Reads the live symbol list from the test DB so any pick compute_top_n
    returns is resolvable to an instrument_token.

    Bars are generated to PRECISELY hit the configured ret_15 and vol_pct:
      - 15 today bars with linear open→close ramp; close = open * (1 + ret_15/100)
      - 100 yesterday bars each volume=1000 (yest_total = 100,000)
      - Today bar volume = vol_pct/100 * yest_total / 15 (so 15 today bars sum to vol_pct%)
    """

    def __init__(self, db_path: str, *,
                 ret_15: float = 1.0, vol_pct: float = 15.0):
        with sqlite3.connect(db_path) as con:
            rows = con.execute(
                "SELECT DISTINCT symbol FROM falcon_features ORDER BY symbol"
            ).fetchall()
        self._symbols = [r[0] for r in rows]
        self.ret_15  = ret_15
        self.vol_pct = vol_pct

    def instruments(self, exchange):
        return [{"tradingsymbol": s, "instrument_token": i + 1, "segment": "NSE"}
                for i, s in enumerate(self._symbols)]

    def historical_data(self, instrument_token, from_date, to_date, interval):
        # Yesterday: 100 bars vol=1000 → yest_total = 100_000
        yest_str = str(from_date)[:10]
        today_str = str(to_date)[:10]
        yest = [{"date": f"{yest_str} 09:{15 + (i // 60):02d}:{i % 60:02d}",
                 "open": 100.0, "high": 101.0, "low": 99.0,
                 "close": 100.0, "volume": 1000} for i in range(100)]
        # Today: 15 bars producing ret_15 exactly and vol_pct exactly
        yest_total_vol = 100 * 1000  # 100_000
        bar_vol = int(self.vol_pct / 100 * yest_total_vol / 15)
        open_p = 100.0
        close_p = open_p * (1 + self.ret_15 / 100.0)
        today = []
        for i in range(15):
            frac = (i + 1) / 15.0
            close_i = open_p + (close_p - open_p) * frac
            prev_close = open_p if i == 0 else today[i - 1]["close"]
            today.append({
                "date":   f"{today_str} 09:{15 + i:02d}:00",
                "open":   prev_close,
                "high":   max(prev_close, close_i) + 0.1,
                "low":    min(prev_close, close_i) - 0.1,
                "close":  close_i,
                "volume": bar_vol,
            })
        return yest + today


class TestRunCycleLockSemantics:
    """End-to-end: drive run_cycle for 0930 then 0945 and verify lock behaviour."""

    @pytest.fixture
    def main_db_copy(self, tmp_path_factory):
        """Copy of the real DB so we have falcon_features + falcon_pattern_*."""
        src = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_universe.db"
        if not os.path.exists(src):
            pytest.skip("Live DB not available")
        dst = str(tmp_path_factory.mktemp("lock_test") / "kanida_test.db")
        sc = sqlite3.connect(src); dc = sqlite3.connect(dst)
        sc.backup(dc); sc.close(); dc.close()
        init_power_user_schema(dst)
        return dst

    def test_0945_copies_enter_decisions_from_0930_unchanged(self, main_db_copy):
        """The core operator promise: trader sees ENTER at 9:30, 9:45 doesn't flip it."""
        from power_user.services.live_tier import run_cycle
        from power_user import config

        # Cycle 1: stub Kite returns strong intraday → ENTER decisions
        strong_kite = FakeKite(main_db_copy, ret_15=1.5, vol_pct=20.0)
        with patch.object(config, "POWER_DB_PATH", main_db_copy):
            summary_0930 = run_cycle("0930", kite=strong_kite,
                                       top_n=5, sleep_between_symbols=0)
        assert summary_0930.get("n_enter", 0) >= 1, \
            f"expected ENTERs from 0930, got summary={summary_0930}"

        # Cycle 2: stub Kite returns WEAK intraday — would normally produce
        # WAIT/SKIP for the same ranks. But lock semantics MUST copy the
        # 0930 ENTERs forward unchanged.
        weak_kite = FakeKite(main_db_copy, ret_15=-0.5, vol_pct=2.0)
        with patch.object(config, "POWER_DB_PATH", main_db_copy):
            summary_0945 = run_cycle("0945", kite=weak_kite,
                                       top_n=5, sleep_between_symbols=0)
        assert summary_0945["n_locked"] >= 1, \
            f"expected lock copies, got summary={summary_0945}"

        # Verify each 0930 ENTER row matches a 0945 ENTER row with decided_at_cycle='0930'
        with sqlite3.connect(main_db_copy) as con:
            con.row_factory = sqlite3.Row
            entered_0930 = con.execute(
                "SELECT rank, symbol FROM falcon_live_decisions "
                "WHERE cycle='0930' AND action='ENTER'"
            ).fetchall()
            for r in entered_0930:
                row_0945 = con.execute(
                    "SELECT action, decided_at_cycle FROM falcon_live_decisions "
                    "WHERE cycle='0945' AND rank=? AND symbol=?",
                    (r["rank"], r["symbol"])
                ).fetchone()
                assert row_0945 is not None, f"0945 missing rank={r['rank']}"
                assert row_0945["action"]           == "ENTER", "decision flipped"
                assert row_0945["decided_at_cycle"] == "0930", "wrong provenance"

    def test_0945_reevaluates_wait_rows(self, main_db_copy):
        """The OTHER half: WAIT rows from 0930 DO get re-evaluated at 0945."""
        from power_user.services.live_tier import run_cycle
        from power_user import config

        # 0930: weak Kite data → mostly WAIT decisions
        weak_kite = FakeKite(main_db_copy, ret_15=0.1, vol_pct=1.0)
        with patch.object(config, "POWER_DB_PATH", main_db_copy):
            run_cycle("0930", kite=weak_kite, top_n=5, sleep_between_symbols=0)

        # 0945: strong Kite data → those WAITs should flip to ENTER (re-evaluated!)
        strong_kite = FakeKite(main_db_copy, ret_15=1.5, vol_pct=20.0)
        with patch.object(config, "POWER_DB_PATH", main_db_copy):
            summary = run_cycle("0945", kite=strong_kite, top_n=5,
                                sleep_between_symbols=0)

        # At least some rows should be freshly-evaluated (NOT locked)
        n_fresh = summary["n_evaluated"] - summary["n_locked"]
        assert n_fresh >= 1, f"expected fresh re-evaluations, got summary={summary}"

    def test_idempotent_rerun_of_same_cycle(self, main_db_copy):
        """UPSERT on (entry_date, cycle, rank) — re-running 0930 twice doesn't double-up."""
        from power_user.services.live_tier import run_cycle
        from power_user import config

        kite = FakeKite(main_db_copy)
        with patch.object(config, "POWER_DB_PATH", main_db_copy):
            run_cycle("0930", kite=kite, top_n=5, sleep_between_symbols=0)
            run_cycle("0930", kite=kite, top_n=5, sleep_between_symbols=0)

        with sqlite3.connect(main_db_copy) as con:
            n = con.execute(
                "SELECT COUNT(*) FROM falcon_live_decisions WHERE cycle='0930'"
            ).fetchone()[0]
        assert n <= 5, f"UPSERT broken — expected at most 5 rows, got {n}"


# ──────────────────────────────────────────────────────────────────────────
# Scheduler — weekday gate, lock acquire, status
# ──────────────────────────────────────────────────────────────────────────

class TestScheduler:

    def test_is_market_weekday_true_for_monday(self):
        from power_user.services.scheduler import _is_market_weekday
        from datetime import datetime
        # Mon 2026-05-11 → weekday() = 0
        mon = datetime(2026, 5, 11, 10, 0)
        assert _is_market_weekday(mon)

    def test_is_market_weekday_false_for_saturday(self):
        from power_user.services.scheduler import _is_market_weekday
        from datetime import datetime
        # Sat 2026-05-16 → weekday() = 5
        sat = datetime(2026, 5, 16, 10, 0)
        assert not _is_market_weekday(sat)

    def test_scheduler_status_before_start(self):
        from power_user.services.scheduler import status
        s = status()
        assert "started" in s

    def test_seconds_until_next_target(self):
        from power_user.services.scheduler import _seconds_until_next_target
        from datetime import datetime, timezone, timedelta
        IST = timezone(timedelta(hours=5, minutes=30))
        # Now: Mon 08:00 IST, target 09:30:30 → 5430 sec
        now = datetime(2026, 5, 11, 8, 0, 0, tzinfo=IST)
        sec = _seconds_until_next_target(now, (9, 30, 30))
        assert 5400 <= sec <= 5500   # approx 1h30m

    def test_seconds_until_next_target_skips_weekend(self):
        from power_user.services.scheduler import _seconds_until_next_target
        from datetime import datetime, timezone, timedelta
        IST = timezone(timedelta(hours=5, minutes=30))
        # Friday 11:00 IST, target 09:30:30 → Monday morning
        fri = datetime(2026, 5, 15, 11, 0, 0, tzinfo=IST)
        sec = _seconds_until_next_target(fri, (9, 30, 30))
        # Should be ~2.9 days (Fri 11:00 → Mon 09:30:30) ≈ 250230s
        assert 240000 < sec < 260000, f"got {sec}s, expected ~3 days"


# ──────────────────────────────────────────────────────────────────────────
# Decided-at-cycle round-trips through API surface
# ──────────────────────────────────────────────────────────────────────────

class TestDecidedAtCycleVisibility:

    def test_picks_live_returns_decided_at_cycle(self, tmp_db):
        """The /picks/live endpoint must surface decided_at_cycle so the UI
        can render 'decided at 9:30 IST' badges."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from power_user.routers.picks_router import router as picks_router
        from power_user.services import auth as auth_svc
        from power_user import config

        with sqlite3.connect(tmp_db) as con:
            # Seed: row at cycle 0945 but decided at 0930
            _insert_decision(con, entry_date="2026-05-15", cycle="0945",
                             rank=1, action="ENTER", decided_at="0930",
                             symbol="HFCL")
            con.execute("""
                INSERT INTO power_user_users
                  (email, google_sub, role, is_active, created_at)
                VALUES ('t@x', 'g_t', 'user', 1, 'now')
            """)
            con.commit()
            uid = con.execute("SELECT id FROM power_user_users WHERE email='t@x'").fetchone()[0]

        token = auth_svc.issue_jwt(user_id=uid, email="t@x", google_sub="g_t")

        with patch.object(config, "POWER_DB_PATH", tmp_db), \
             patch.object(config, "GOOGLE_CLIENT_ID", "test"):
            app = FastAPI(); app.include_router(picks_router)
            client = TestClient(app)
            r = client.get("/api/power/picks/live?cycle=0945&entry_date=2026-05-15",
                           headers={"Authorization": f"Bearer {token}"})

        assert r.status_code == 200
        body = r.json()
        assert body["summary"]["locked"] == 1
        d = body["decisions"][0]
        assert d["decided_at_cycle"] == "0930"
        # decided_at exposes the earlier provenance; the row's stored cycle is 0945
        # but get_decisions doesn't echo it in the decision dict (only at top level)
