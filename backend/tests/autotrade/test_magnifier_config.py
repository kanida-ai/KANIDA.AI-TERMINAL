"""Config + validation lock for the Falcon Intraday Magnifier strategy.

Pins that the magnifier is a DISTINCT strategy (not a variant of intraday_basket),
locked to EQ + MIS (5×) + long, with the split-entry knobs + high-tier vocabulary
validated at the door, and that it round-trips through from_dict.
"""
import pytest

from autotrade.config import TradingSessionConfig


def _cfg(**kw):
    base = dict(total_allocated_capital=100000.0, strategy="intraday_magnifier",
                order_product="MIS", instrument_type="EQ", direction="long",
                top_n_stocks=15, arm_pct=0.06, floor_pct=0.02,
                trail_giveback_pct=0.05, stop_pct=0.03)
    base.update(kw)
    return TradingSessionConfig(**base)


def test_magnifier_is_a_valid_distinct_strategy():
    c = _cfg()
    c.validate()
    assert c.strategy == "intraday_magnifier"     # NOT intraday_basket
    assert c.magnifier_split_fraction == 0.5
    assert c.magnifier_second_leg_offset_sec == 60
    # the validated magnifier trail preset (capital basis)
    assert (c.arm_pct, c.floor_pct, c.trail_giveback_pct, c.stop_pct) == \
        (0.06, 0.02, 0.05, 0.03)


def test_default_high_tier_is_the_real_vocabulary():
    c = _cfg()
    assert set(c.magnifier_high_tier) == {
        "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline",
        "PREMIUM-Compression", "PREMIUM-Pullback"}


def test_rejects_non_mis_product():
    for prod in ("CNC", "MTF", "NRML"):
        with pytest.raises(ValueError):
            _cfg(order_product=prod).validate()


def test_rejects_short_and_non_equity():
    with pytest.raises(ValueError):
        _cfg(direction="short").validate()
    with pytest.raises(ValueError):
        _cfg(instrument_type="FUT").validate()


def test_rejects_positional_no_square_off():
    with pytest.raises(ValueError):
        _cfg(square_off_enabled=False).validate()


def test_rejects_bad_split_fraction_and_offset():
    for f in (0.0, 1.0, 1.5, -0.1):
        with pytest.raises(ValueError):
            _cfg(magnifier_split_fraction=f).validate()
    with pytest.raises(ValueError):
        _cfg(magnifier_second_leg_offset_sec=0).validate()


def test_rejects_invented_tier():
    with pytest.raises(ValueError):
        _cfg(magnifier_high_tier=["MEGA-TIER"]).validate()
    with pytest.raises(ValueError):
        _cfg(magnifier_high_tier=[]).validate()


def test_from_dict_round_trip():
    d = {"strategy": "intraday_magnifier", "total_allocated_capital": 100000.0,
         "order_product": "MIS", "instrument_type": "EQ", "direction": "long",
         "top_n_stocks": 15, "arm_pct": 0.06, "floor_pct": 0.02,
         "trail_giveback_pct": 0.05, "stop_pct": 0.03,
         "magnifier_split_fraction": 0.5, "magnifier_second_leg_offset_sec": 60}
    c = TradingSessionConfig.from_dict(d)
    c.validate()
    assert c.strategy == "intraday_magnifier"
    assert c.magnifier_split_fraction == 0.5
    # a config with no magnifier keys still gets the default high-tier set
    c2 = TradingSessionConfig.from_dict(
        {k: v for k, v in d.items() if not k.startswith("magnifier_")})
    assert "GOLD" in c2.magnifier_high_tier


def test_intraday_basket_and_kill_switch_unchanged():
    # Additive: the existing strategies still validate exactly as before.
    TradingSessionConfig(total_allocated_capital=500000.0,
                         strategy="intraday_basket", order_product="MIS",
                         top_n_stocks=5).validate()
    TradingSessionConfig(total_allocated_capital=500000.0,
                         strategy="portfolio_kill_switch",
                         kill_switch_enabled=True, kill_switch_pct=0.01).validate()
