"""Unit tests for volatility_gate."""
from falcon.trade.services.volatility_gate import sl_order_type, sl_limit_price


def test_low_vix_no_gap_returns_sl_l():
    assert sl_order_type(vix=14.2) == "SL-L"
    assert sl_order_type(vix=14.2, gap_pct=0.5) == "SL-L"
    assert sl_order_type(vix=14.2, gap_pct=-1.5) == "SL-L"


def test_high_vix_returns_sl_m():
    assert sl_order_type(vix=18.1) == "SL-M"
    assert sl_order_type(vix=25.0, gap_pct=0.0) == "SL-M"


def test_large_gap_returns_sl_m():
    assert sl_order_type(vix=14.0, gap_pct=2.5) == "SL-M"
    assert sl_order_type(vix=14.0, gap_pct=-3.0) == "SL-M"


def test_large_intraday_range_returns_sl_m():
    assert sl_order_type(vix=14.0, intraday_range_pct=5.0) == "SL-M"


def test_intraday_range_below_threshold_keeps_sl_l():
    assert sl_order_type(vix=14.0, intraday_range_pct=3.5) == "SL-L"


def test_sl_limit_price_is_two_tenths_below_trigger():
    # 100.00 → 99.80
    assert sl_limit_price(100.0) == 99.80
    # 87.45 → 87.45 * 0.998 = 87.27510 → 87.28 (rounded)
    assert sl_limit_price(87.45) == 87.28
    # 1845.50 → 1841.81
    assert sl_limit_price(1845.50) == 1841.81
