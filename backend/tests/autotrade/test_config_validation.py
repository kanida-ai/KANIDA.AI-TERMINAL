"""Defensive unit-scaling validation on TradingSessionConfig.validate().

Percentages are FRACTIONS (0.01 = 1%). The UI has historically sent 1.0
intending "100%", which would make the kill switch effectively never fire — a
silent no-fire. validate() must reject obviously-mis-scaled fractions.
"""
import pytest

from autotrade.config import TradingSessionConfig


def _cfg(**kw) -> TradingSessionConfig:
    base = dict(total_allocated_capital=500000.0)
    base.update(kw)
    return TradingSessionConfig(**base)


# ── kill_switch_pct ──────────────────────────────────────────────────────────

def test_kill_switch_fraction_1_0_raises():
    """The exact UI bug: 1.0 (=100%) when enabled must be rejected."""
    cfg = _cfg(kill_switch_enabled=True, kill_switch_pct=1.0)
    with pytest.raises(ValueError, match="kill_switch_pct must be a fraction"):
        cfg.validate()


def test_kill_switch_pct_zero_raises_when_enabled():
    cfg = _cfg(kill_switch_enabled=True, kill_switch_pct=0.0)
    with pytest.raises(ValueError, match="kill_switch_pct must be a fraction"):
        cfg.validate()


def test_kill_switch_pct_above_half_raises_when_enabled():
    cfg = _cfg(kill_switch_enabled=True, kill_switch_pct=0.51)
    with pytest.raises(ValueError, match="kill_switch_pct must be a fraction"):
        cfg.validate()


def test_kill_switch_valid_fractions_pass():
    # default 0.012, a small fraction, and the boundary 0.5 are all valid.
    for v in (0.012, 0.005, 0.5):
        _cfg(kill_switch_enabled=True, kill_switch_pct=v).validate()


def test_kill_switch_not_validated_when_disabled():
    # When disabled the value is inert — even a mis-scaled 1.0 must not raise.
    _cfg(kill_switch_enabled=False, kill_switch_pct=1.0).validate()


# ── per_position_stop / target pct ───────────────────────────────────────────

def test_per_position_stop_1_0_raises():
    cfg = _cfg(per_position_gtt_enabled=True, per_position_stop_pct=1.0)
    with pytest.raises(ValueError, match="per_position_stop_pct must be a fraction"):
        cfg.validate()


def test_per_position_target_1_0_raises():
    cfg = _cfg(per_position_gtt_enabled=True, per_position_target_pct=1.0)
    with pytest.raises(ValueError, match="per_position_target_pct must be a fraction"):
        cfg.validate()


def test_per_position_valid_fractions_pass():
    _cfg(per_position_gtt_enabled=True,
         per_position_stop_pct=0.03, per_position_target_pct=0.06).validate()


def test_per_position_not_validated_when_gtt_disabled():
    _cfg(per_position_gtt_enabled=False,
         per_position_stop_pct=1.0, per_position_target_pct=1.0).validate()


def test_default_config_passes():
    """The shipped defaults (kill switch off, gtt on @ 0.03/0.06) must validate."""
    _cfg().validate()
