"""Regression lock for the VALIDATED basket-only intraday config (2026-07-04).

Pins the exit parameters the strategy doc validated over 530 days (arm 5% of
CAPITAL / floor 1% / giveback 1.5% / stop 3%; arm raised from 2.5% when the trail
moved to the allocated-capital basis 2026-07-07), the widened -5% GTT backstop,
and — critically — that Layer A (the per-stock software stop) is OFF by default
so live exits are BASKET-ONLY, matching the backtest. A silent drift of any of
these would make live diverge from the documented, validated behaviour.
"""
from autotrade.config import TradingSessionConfig


def _cfg(**kw):
    base = dict(total_allocated_capital=500000.0, strategy="intraday_basket",
                order_product="MIS", top_n_stocks=5)
    base.update(kw)
    return TradingSessionConfig(**base)


def test_validated_basket_only_defaults():
    c = _cfg()
    c.validate()
    assert c.arm_pct == 0.05             # profit-lock arms at +5% OF CAPITAL
    assert c.floor_pct == 0.01           # locked floor +1%
    assert c.trail_giveback_pct == 0.0125 # 1.25% give-back (2026-07-08 default)
    assert c.stop_pct == 0.03            # -3% basket hard stop
    assert c.per_position_stop_pct == 0.08   # GTT = 8% OF CAPITAL (→ price via leverage)
    assert c.per_stock_stop_enabled is False # Layer A OFF → basket-only


def test_from_dict_preserves_basket_only_and_parses_flag():
    # Portal path: a config dict with no per_stock flag must default to basket-only.
    d = _cfg().from_dict({"strategy": "intraday_basket",
                          "total_allocated_capital": 500000.0,
                          "order_product": "MIS", "top_n_stocks": 5})
    assert d.per_stock_stop_enabled is False
    assert d.arm_pct == 0.05 and d.stop_pct == 0.03 and d.per_position_stop_pct == 0.08
    # Explicit opt-in to the (worse-returning) two-layer variant is honoured.
    two = _cfg().from_dict({"strategy": "intraday_basket",
                            "total_allocated_capital": 500000.0,
                            "order_product": "MIS", "top_n_stocks": 5,
                            "per_stock_stop_enabled": True})
    assert two.per_stock_stop_enabled is True


def test_new_params_pass_validation_and_floor_le_arm():
    # floor (1%) <= arm (5%) and all four in (0, 0.5] — no ValueError.
    _cfg().validate()


def test_options_ce_pe_hard_blocked_by_default(monkeypatch):
    # Options (CE/PE) are uncertified — validate() must reject them unless the
    # explicit env flag is set. Equity + futures are unaffected.
    monkeypatch.delenv("FALCON_AUTOTRADE_OPTIONS_ENABLED", raising=False)
    for it in ("CE", "PE"):
        try:
            TradingSessionConfig(total_allocated_capital=500000.0, strategy="intraday_basket",
                                 instrument_type=it, order_product="NRML", direction="long",
                                 top_n_stocks=5).validate()
            assert False, f"{it} should be blocked by default"
        except ValueError as e:
            assert "options" in str(e).lower()
    # The env flag unlocks it (for the certification phase).
    monkeypatch.setenv("FALCON_AUTOTRADE_OPTIONS_ENABLED", "true")
    TradingSessionConfig(total_allocated_capital=500000.0, strategy="intraday_basket",
                         instrument_type="CE", order_product="NRML", direction="long",
                         top_n_stocks=5).validate()
