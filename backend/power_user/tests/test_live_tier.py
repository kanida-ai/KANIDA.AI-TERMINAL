"""Tests for live tier decision rules + cycle runner.

Pure unit tests for the rule logic (no Kite calls). The end-to-end run_cycle
path with mocked Kite client is tested separately in test_live_tier_cycle.py
(adding later — depends on a Kite mock fixture).
"""
from __future__ import annotations

import pytest

from power_user.services.live_tier import (
    CYCLES,
    TOP_N,
    _compute_15min_metrics,
    apply_tier_rule,
    tier_of,
)


class TestTierOf:
    """6-tier internal bucketing — drives the decision rule per rank."""

    def test_elite_bucket(self):
        for r in (1, 2, 3):
            assert tier_of(r) == "ELITE"

    def test_high_bucket(self):
        for r in (4, 5, 6, 7):
            assert tier_of(r) == "HIGH"

    def test_mid_bucket(self):
        for r in (8, 10, 14):
            assert tier_of(r) == "MID"

    def test_lower_bucket(self):
        for r in (15, 20, 25):
            assert tier_of(r) == "LOWER"

    def test_tail_bucket(self):
        for r in (26, 40, 50):
            assert tier_of(r) == "TAIL"

    def test_deep_tail_bucket(self):
        """Rank 51-100: distinct from TAIL — stricter rule."""
        for r in (51, 75, 100):
            assert tier_of(r) == "DEEP-TAIL"


class TestApplyTierRule:
    """Rule outcome per tier × intraday-data combination."""

    # ── ELITE (1-3) ───────────────────────────────────────────────

    def test_elite_enter_on_positive_ret(self):
        action, tier, reason = apply_tier_rule(1, ret_15=0.5, vol_pct=8, close_loc=0.7)
        assert (action, tier) == ("ENTER", "ELITE")

    def test_elite_wait_on_borderline_ret(self):
        action, _, _ = apply_tier_rule(2, ret_15=0.1, vol_pct=20, close_loc=0.9)
        assert action == "WAIT"   # need > 0.2%, not just >= 0

    def test_elite_wait_on_negative_ret(self):
        action, _, _ = apply_tier_rule(3, ret_15=-0.3, vol_pct=15, close_loc=0.8)
        assert action == "WAIT"

    # ── HIGH (4-7) ────────────────────────────────────────────────

    def test_high_needs_both_ret_and_vol(self):
        # Strong ret, low vol → wait
        action, _, _ = apply_tier_rule(5, ret_15=1.0, vol_pct=5, close_loc=0.8)
        assert action == "WAIT"
        # Adequate vol but weak ret → wait
        action, _, _ = apply_tier_rule(5, ret_15=0.3, vol_pct=15, close_loc=0.8)
        assert action == "WAIT"
        # Both → enter
        action, _, _ = apply_tier_rule(5, ret_15=0.6, vol_pct=12, close_loc=0.8)
        assert action == "ENTER"

    # ── MID (8-14) — different rule (close_loc + vol, NOT ret) ──

    def test_mid_uses_close_loc_not_ret(self):
        # Ret > 0.5 alone is NOT enough for MID
        action, _, _ = apply_tier_rule(10, ret_15=1.5, vol_pct=20, close_loc=0.5)
        assert action == "WAIT", "MID requires close_loc > 0.66"
        # close_loc + vol satisfied → enter
        action, _, _ = apply_tier_rule(10, ret_15=-0.1, vol_pct=10, close_loc=0.75)
        assert action == "ENTER"

    # ── LOWER (15-25) — SKIPs instead of WAIT on miss ────────────

    def test_lower_skips_when_rule_fails(self):
        action, _, _ = apply_tier_rule(20, ret_15=0.3, vol_pct=5, close_loc=0.8)
        assert action == "SKIP"

    def test_lower_enters_on_strict_match(self):
        action, _, _ = apply_tier_rule(20, ret_15=0.7, vol_pct=15, close_loc=0.5)
        assert action == "ENTER"

    # ── TAIL (26-50) ───────────────────────────────────────────────

    def test_tail_enter_simple(self):
        action, _, _ = apply_tier_rule(30, ret_15=0.6, vol_pct=2, close_loc=0.4)
        assert action == "ENTER"

    def test_tail_skip_weak_ret(self):
        action, _, _ = apply_tier_rule(30, ret_15=0.4, vol_pct=2, close_loc=0.4)
        assert action == "SKIP"

    # ── DEEP-TAIL (51-100) — strictest ─────────────────────────────

    def test_deep_tail_skip_when_only_ret(self):
        action, _, _ = apply_tier_rule(75, ret_15=0.6, vol_pct=8, close_loc=0.5)
        assert action == "SKIP"

    def test_deep_tail_enter_only_when_both_very_strong(self):
        action, _, _ = apply_tier_rule(75, ret_15=1.0, vol_pct=20, close_loc=0.5)
        assert action == "ENTER"

    # ── Missing data → WAIT, never crash ────────────────────────────

    def test_none_ret_returns_wait(self):
        action, _, reason = apply_tier_rule(1, ret_15=None, vol_pct=10, close_loc=0.7)
        assert action == "WAIT"
        assert "market data" in reason.lower()

    def test_all_none_returns_wait(self):
        action, _, _ = apply_tier_rule(50, ret_15=None, vol_pct=None, close_loc=None)
        assert action == "WAIT"


class TestCompute15MinMetrics:
    """The math that turns 15 1-min bars into (ret_15, vol_pct, close_loc)."""

    def _bars(self, opens, highs, lows, closes, vols):
        return [
            {"open": o, "high": h, "low": l, "close": c, "volume": v}
            for o, h, l, c, v in zip(opens, highs, lows, closes, vols)
        ]

    def test_too_few_bars_returns_none(self):
        bars = self._bars([100] * 10, [101] * 10, [99] * 10, [100] * 10, [1000] * 10)
        ret, vol, cl = _compute_15min_metrics(bars, yest_total_vol=100000)
        assert ret is None and vol is None and cl is None

    def test_zero_yest_vol_returns_none(self):
        bars = self._bars([100] * 15, [101] * 15, [99] * 15, [100] * 15, [1000] * 15)
        ret, vol, cl = _compute_15min_metrics(bars, yest_total_vol=0)
        assert ret is None

    def test_basic_positive_close(self):
        # Open at 100, all bars rise to close at 102 → +2% ret_15
        opens  = [100.0] * 15
        highs  = list(range(101, 116))    # 101..115
        lows   = [99.0] * 15
        closes = list(range(101, 116))    # closes match highs
        vols   = [1000] * 15
        ret, vol, cl = _compute_15min_metrics(
            self._bars(opens, highs, lows, closes, vols),
            yest_total_vol=100000,
        )
        # last close = 115; first open = 100 → 15% ret
        assert ret == pytest.approx(15.0, abs=0.01)
        assert vol == pytest.approx(15.0, abs=0.01)    # 15000 / 100000 = 15%
        # close_loc: c=115, range_high=115, range_low=99 → (115-99)/(115-99) = 1.0
        assert cl == pytest.approx(1.0, abs=0.01)

    def test_negative_close(self):
        opens  = [100.0] * 15
        highs  = [100.0] * 15
        lows   = [95.0] * 15
        closes = [97.0] * 15
        vols   = [500] * 15
        ret, vol, cl = _compute_15min_metrics(
            self._bars(opens, highs, lows, closes, vols),
            yest_total_vol=50000,
        )
        # ret = (97/100 - 1) * 100 = -3%
        assert ret == pytest.approx(-3.0, abs=0.01)
        # close_loc = (97-95)/(100-95) = 0.4
        assert cl == pytest.approx(0.4, abs=0.01)


class TestCyclesConstant:
    """Cycle metadata must match the documented schedule."""

    def test_three_cycles(self):
        assert len(CYCLES) == 3
        names = [c[0] for c in CYCLES]
        assert names == ["0930", "0945", "1000"]

    def test_cycle_times_ist(self):
        # (hour, minute, second) tuples
        assert CYCLES[0][1] == (9, 30, 30)
        assert CYCLES[1][1] == (9, 45,  0)
        assert CYCLES[2][1] == (10, 0,  0)

    def test_top_n_constant(self):
        assert TOP_N == 100


class TestRunCycleValidation:
    """Edge cases the cycle runner must handle without crashing."""

    def test_unknown_cycle_name_raises(self):
        from power_user.services.live_tier import run_cycle
        with pytest.raises(ValueError, match="unknown cycle_name"):
            run_cycle("9999", kite=object(), con=None)
