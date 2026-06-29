"""Tests for GTTManager compute_levels slippage buffer (BUILD 1).

Covers:
  test_stop_limit_below_trigger      — default 0.3% buffer → stop_limit < trigger
  test_target_limit_above_or_equal   — target leg limit == trigger (no buffer)
  test_buffer_env_override           — FALCON_GTT_STOP_BUFFER=0.005 → 0.5% applied
  test_buffer_default_exact          — default 0.003 applied precisely
  test_stop_limit_persisted_in_gtt   — place_for_position records stop_limit in MockBroker
  test_paper_still_records_trigger   — paper mode records trigger prices, not limit
"""
from __future__ import annotations

import os
import uuid

import pytest

from autotrade.monitoring.gtt_manager import GTTManager, compute_levels, _gtt_stop_buffer
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from tests.autotrade.mock_broker import MockBroker


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sid():
    return uuid.uuid4().hex


def _make_session(sid, cap=500_000.0, mode="live"):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_sessions "
            "(session_id, created_at, status, mode, total_allocated_capital, config_json) "
            "VALUES (?,?,?,?,?,?)",
            (sid, "2026-06-28T09:00:00", "RUNNING", mode, cap, "{}"))
        con.commit()


# ── BUILD 1 TESTS ─────────────────────────────────────────────────────────────

class TestComputeLevelsBuffer:
    """Unit tests for compute_levels() 4-tuple return and slippage buffer."""

    def test_stop_limit_below_trigger(self):
        """Default buffer (0.003) → stop_limit is ~0.3% below stop_trigger."""
        # Ensure env override is clear so default is used.
        os.environ.pop("FALCON_GTT_STOP_BUFFER", None)
        stop_trig, stop_lim, tgt_trig, tgt_lim = compute_levels(100.0, 0.03, 0.06)
        assert stop_trig == 97.0, "stop trigger must be entry*(1-0.03)"
        assert stop_lim < stop_trig, "stop_limit must be BELOW stop_trigger"
        # Default buffer = 0.3%: 97.0 * (1 - 0.003) = 96.709
        expected_lim = round(97.0 * (1.0 - 0.003), 2)
        assert stop_lim == expected_lim, (
            f"Expected stop_lim={expected_lim}, got {stop_lim}")

    def test_target_limit_above_or_equal_trigger(self):
        """Target leg limit should equal (or exceed) the trigger — no buffer."""
        os.environ.pop("FALCON_GTT_STOP_BUFFER", None)
        stop_trig, stop_lim, tgt_trig, tgt_lim = compute_levels(100.0, 0.03, 0.06)
        assert tgt_trig == 106.0
        assert tgt_lim >= tgt_trig, (
            "target limit must be >= target trigger (no slippage buffer on target leg)")
        assert tgt_lim == tgt_trig, "target limit should equal trigger by default"

    def test_buffer_env_override(self):
        """FALCON_GTT_STOP_BUFFER=0.005 → 0.5% buffer applied to stop_limit."""
        os.environ["FALCON_GTT_STOP_BUFFER"] = "0.005"
        try:
            stop_trig, stop_lim, tgt_trig, tgt_lim = compute_levels(100.0, 0.03, 0.06)
            assert stop_trig == 97.0
            expected_lim = round(97.0 * (1.0 - 0.005), 2)
            assert stop_lim == expected_lim, (
                f"Expected 0.5% buffer → stop_lim={expected_lim}, got {stop_lim}")
            # Must be strictly below trigger.
            assert stop_lim < stop_trig
        finally:
            os.environ.pop("FALCON_GTT_STOP_BUFFER", None)

    def test_buffer_default_exact(self):
        """_gtt_stop_buffer() returns 0.003 when env is unset."""
        os.environ.pop("FALCON_GTT_STOP_BUFFER", None)
        assert _gtt_stop_buffer() == 0.003

    def test_buffer_env_override_reader(self):
        """_gtt_stop_buffer() reads FALCON_GTT_STOP_BUFFER from env."""
        os.environ["FALCON_GTT_STOP_BUFFER"] = "0.005"
        try:
            assert _gtt_stop_buffer() == 0.005
        finally:
            os.environ.pop("FALCON_GTT_STOP_BUFFER", None)

    def test_invalid_env_falls_back_to_default(self):
        """Invalid / out-of-range FALCON_GTT_STOP_BUFFER falls back to 0.003."""
        for bad in ("abc", "", "0.0", "0.2", "-0.1"):
            os.environ["FALCON_GTT_STOP_BUFFER"] = bad
            assert _gtt_stop_buffer() == 0.003, f"expected default for bad value {bad!r}"
        os.environ.pop("FALCON_GTT_STOP_BUFFER", None)

    def test_four_values_returned(self):
        """compute_levels must return exactly 4 values."""
        result = compute_levels(500.0, 0.05, 0.10)
        assert len(result) == 4

    def test_levels_at_different_prices(self):
        """Buffer scales with the stop trigger, not entry, across different prices."""
        os.environ.pop("FALCON_GTT_STOP_BUFFER", None)
        for entry, stop_pct, tgt_pct in [(50.0, 0.02, 0.04), (1500.0, 0.03, 0.06)]:
            st, sl, tt, tl = compute_levels(entry, stop_pct, tgt_pct)
            assert sl < st, f"stop_limit must be below stop_trigger for entry={entry}"
            assert tl == tt, f"target limit must equal trigger for entry={entry}"


class TestGTTManagerStopLimit:
    """Integration tests: GTTManager places GTT with correct stop_limit_price."""

    def test_stop_limit_persisted_in_gtt(self, clean_positions):
        """Live GTT placement: MockBroker.gtts records stop_limit < stop trigger."""
        os.environ.pop("FALCON_GTT_STOP_BUFFER", None)
        sid = _sid()
        cap = 500_000.0
        _make_session(sid, cap, mode="live")
        reg = PositionRegistry(sid, cap)
        reg.register(symbol="INFY", broker_profile="zer", qty=100, avg_price=1500.0)
        reg.update_ltp("INFY", 1500.0)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"INFY": 1500.0})
        cfg = TradingSessionConfig(total_allocated_capital=cap,
                                   per_position_stop_pct=0.03,
                                   per_position_target_pct=0.06)
        mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
        res = mgr.backfill_missing()
        assert len(res) == 1 and res[0]["status"] == "PLACED"

        g = broker.gtts[0]
        # Trigger price stored under "stop".
        expected_trig = round(1500.0 * (1.0 - 0.03), 2)          # 1455.0
        expected_lim  = round(expected_trig * (1.0 - 0.003), 2)   # 1450.64
        assert g["stop"] == expected_trig, "trigger price must be entry*(1-stop_pct)"
        assert g["stop_limit"] < g["stop"], "stop_limit must be strictly below trigger"
        assert g["stop_limit"] == expected_lim, (
            f"Expected stop_limit={expected_lim}, got {g['stop_limit']}")
        # Target leg: no change.
        assert g["target"] == round(1500.0 * (1.0 + 0.06), 2)

    def test_return_dict_includes_stop_limit(self, clean_positions):
        """place_for_position return dict carries 'stop_limit' key."""
        os.environ.pop("FALCON_GTT_STOP_BUFFER", None)
        sid = _sid()
        cap = 100_000.0
        _make_session(sid, cap, mode="live")
        reg = PositionRegistry(sid, cap)
        reg.register(symbol="TCS", broker_profile="zer", qty=10, avg_price=4000.0)
        reg.update_ltp("TCS", 4000.0)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"TCS": 4000.0})
        cfg = TradingSessionConfig(total_allocated_capital=cap,
                                   per_position_stop_pct=0.03,
                                   per_position_target_pct=0.06)
        mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
        pos = reg.get_open_positions()[0]
        result = mgr.place_for_position(pos)
        assert "stop_limit" in result, "place_for_position must return stop_limit key"
        assert result["stop_limit"] < result["stop"], "stop_limit must be below trigger"

    def test_paper_records_trigger_not_limit(self, clean_positions):
        """Paper mode: gtt_stop column stores the trigger (not stop_limit) as the
        displayed level.  No real GTT is placed so stop_limit is a broker detail
        that only matters in live mode."""
        os.environ.pop("FALCON_GTT_STOP_BUFFER", None)
        sid = _sid()
        cap = 100_000.0
        _make_session(sid, cap, mode="paper")
        reg = PositionRegistry(sid, cap)
        reg.register(symbol="RELIANCE", broker_profile="zer", qty=5, avg_price=2800.0)
        reg.update_ltp("RELIANCE", 2800.0)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=True,
                            ltps={"RELIANCE": 2800.0})
        cfg = TradingSessionConfig(total_allocated_capital=cap,
                                   per_position_stop_pct=0.03,
                                   per_position_target_pct=0.06)
        mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
        res = mgr.backfill_missing()
        assert res[0]["status"] == "RECORDED_ONLY"
        row = reg.get_open_positions()[0]
        expected_trig = round(2800.0 * (1.0 - 0.03), 2)
        assert row["gtt_stop"] == expected_trig, (
            "gtt_stop in DB must be the trigger price (the displayed level)")
        assert row["gtt_id"] is None  # no real GTT in paper
