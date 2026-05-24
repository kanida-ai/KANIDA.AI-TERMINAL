"""Unit tests for order_planner."""
from falcon.trade.services.order_planner import plan_orders


def _signals(*sym_close):
    """Helper: build a signals list from (symbol, close) tuples."""
    return [
        {"symbol": s, "sector": "Test", "close_at_signal": c, "rank": i + 1}
        for i, (s, c) in enumerate(sym_close)
    ]


def _all_mtf(_):
    return True


def _slt(_):
    return "SL-L"


def test_basic_selection_no_overlap():
    sigs = _signals(("HFCL", 87.45), ("CHENNPETRO", 542.30), ("CEMPRO", 1845.50))
    orders, skipped, ac = plan_orders(
        signals=sigs, selected_symbols=["HFCL", "CHENNPETRO"],
        per_trade=300_000, sl_pct=-7.0, trail_trigger_pct=10.0,
        held_by_symbol={}, hold_actions={},
        mtf_eligible_fn=_all_mtf, sl_order_type_fn=_slt,
    )
    assert len(orders) == 2
    assert orders[0].symbol == "HFCL"
    assert orders[0].qty == int(300_000 // 87.45)
    assert orders[0].notional == orders[0].qty * 87.45
    assert orders[0].sl_price == round(87.45 * 0.93, 2)
    assert orders[0].target_price == round(87.45 * 1.10, 2)
    assert ac == {"n_skip_action": 0, "n_average_action": 0}
    assert skipped == []


def test_non_mtf_skipped():
    sigs = _signals(("HFCL", 87.45), ("PAYTM", 845.20))
    orders, skipped, _ = plan_orders(
        signals=sigs, selected_symbols=["HFCL", "PAYTM"],
        per_trade=300_000, sl_pct=-7, trail_trigger_pct=10,
        held_by_symbol={}, hold_actions={},
        mtf_eligible_fn=lambda s: s != "PAYTM", sl_order_type_fn=_slt,
    )
    assert len(orders) == 1
    assert orders[0].symbol == "HFCL"
    assert {"symbol": "PAYTM", "reason": "MTF_INELIGIBLE"} in skipped


def test_overlap_default_skip():
    sigs = _signals(("HFCL", 87.45), ("CEMPRO", 1845.50))
    held = {"HFCL": {"qty": 100, "avg_entry": 85.0}}
    orders, _, ac = plan_orders(
        signals=sigs, selected_symbols=["HFCL", "CEMPRO"],
        per_trade=300_000, sl_pct=-7, trail_trigger_pct=10,
        held_by_symbol=held, hold_actions={},  # default → skip
        mtf_eligible_fn=_all_mtf, sl_order_type_fn=_slt,
    )
    assert len(orders) == 1
    assert orders[0].symbol == "CEMPRO"
    assert ac["n_skip_action"] == 1
    assert ac["n_average_action"] == 0


def test_overlap_average_action_includes_with_flag():
    sigs = _signals(("HFCL", 87.45), ("CEMPRO", 1845.50))
    held = {"HFCL": {"qty": 100, "avg_entry": 85.0}}
    orders, _, ac = plan_orders(
        signals=sigs, selected_symbols=["HFCL", "CEMPRO"],
        per_trade=300_000, sl_pct=-7, trail_trigger_pct=10,
        held_by_symbol=held, hold_actions={"HFCL": "average"},
        mtf_eligible_fn=_all_mtf, sl_order_type_fn=_slt,
    )
    assert len(orders) == 2
    hfcl = next(o for o in orders if o.symbol == "HFCL")
    assert hfcl.is_averaging is True
    assert hfcl.existing_qty == 100
    cempro = next(o for o in orders if o.symbol == "CEMPRO")
    assert cempro.is_averaging is False
    assert ac == {"n_skip_action": 0, "n_average_action": 1}


def test_position_too_small_skipped():
    sigs = _signals(("MCX", 6543.40))
    orders, skipped, _ = plan_orders(
        signals=sigs, selected_symbols=["MCX"],
        per_trade=5000, sl_pct=-7, trail_trigger_pct=10,  # too small for 6543 stock
        held_by_symbol={}, hold_actions={},
        mtf_eligible_fn=_all_mtf, sl_order_type_fn=_slt,
    )
    assert orders == []
    assert {"symbol": "MCX", "reason": "POSITION_TOO_SMALL"} in skipped


def test_unknown_symbol_skipped():
    sigs = _signals(("HFCL", 87.45))
    orders, skipped, _ = plan_orders(
        signals=sigs, selected_symbols=["GHOST"],
        per_trade=300_000, sl_pct=-7, trail_trigger_pct=10,
        held_by_symbol={}, hold_actions={},
        mtf_eligible_fn=_all_mtf, sl_order_type_fn=_slt,
    )
    assert orders == []
    assert {"symbol": "GHOST", "reason": "NOT_IN_SIGNALS"} in skipped


def test_no_leverage_math_in_planner():
    """Confirms qty = floor(per_trade / close), no leverage multiplier."""
    sigs = _signals(("HFCL", 100.0))
    orders, _, _ = plan_orders(
        signals=sigs, selected_symbols=["HFCL"],
        per_trade=300_000, sl_pct=-7, trail_trigger_pct=10,
        held_by_symbol={}, hold_actions={},
        mtf_eligible_fn=_all_mtf, sl_order_type_fn=_slt,
    )
    # 300000 / 100 = 3000 — NOT 9000 (which would be 3x)
    assert orders[0].qty == 3000
    assert orders[0].notional == 300000.0


def test_sl_limit_price_only_for_sl_l():
    sigs = _signals(("HFCL", 100.0), ("CEMPRO", 1000.0))
    orders, _, _ = plan_orders(
        signals=sigs, selected_symbols=["HFCL", "CEMPRO"],
        per_trade=300_000, sl_pct=-7, trail_trigger_pct=10,
        held_by_symbol={}, hold_actions={},
        mtf_eligible_fn=_all_mtf,
        sl_order_type_fn=lambda s: "SL-M" if s == "HFCL" else "SL-L",
    )
    hfcl   = next(o for o in orders if o.symbol == "HFCL")
    cempro = next(o for o in orders if o.symbol == "CEMPRO")
    assert hfcl.sl_order_type == "SL-M"
    assert hfcl.sl_limit_price is None      # SL-M doesn't use limit price
    assert cempro.sl_order_type == "SL-L"
    assert cempro.sl_limit_price is not None
    assert cempro.sl_limit_price < cempro.sl_price
