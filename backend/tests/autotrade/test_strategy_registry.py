"""Strategy registry + admin-controlled visibility (Tasks 1 & 2) + the BTST
display rename (Task 1)."""
from autotrade import strategy_registry as sr


def _reset_visibility():
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute("DELETE FROM strategy_visibility")
        con.commit()


def test_btst_display_name_is_falcon_btst_oscillator():
    d = sr.get_descriptor("falcon_btst_oscillator")
    assert d is not None
    assert d.display_name == "Falcon BTST Oscillator"   # Task 1 rename
    # It builds the SAME 2-session BTST ladder (internal mechanism unchanged).
    assert d.kind == "campaign" and d.ladder_preset == "btst"


def test_magnifier_registered_and_hidden_by_default():
    _reset_visibility()
    d = sr.get_descriptor("intraday_magnifier")
    assert d is not None
    assert d.display_name == "Falcon Intraday Magnifier"
    assert d.default_visible is False          # ships HIDDEN
    assert sr.is_visible("intraday_magnifier") is False


def test_experimental_strategies_hidden_shipped_visible():
    _reset_visibility()
    # New/experimental ship hidden.
    assert sr.is_visible("intraday_magnifier") is False
    assert sr.is_visible("falcon_btst_oscillator") is False
    # Long-shipped strategies stay visible.
    assert sr.is_visible("portfolio_kill_switch") is True
    assert sr.is_visible("intraday_basket") is True
    # Unknown id → hidden (Task-2 default-hidden contract).
    assert sr.is_visible("some_unknown_strategy") is False


def test_non_admin_list_excludes_hidden_admin_sees_all():
    _reset_visibility()
    non_admin = {s["strategy_id"] for s in sr.list_for_caller(is_admin=False)}
    assert "intraday_magnifier" not in non_admin
    assert "falcon_btst_oscillator" not in non_admin
    assert "portfolio_kill_switch" in non_admin

    admin = sr.list_for_caller(is_admin=True)
    admin_ids = {s["strategy_id"] for s in admin}
    assert "intraday_magnifier" in admin_ids and "falcon_btst_oscillator" in admin_ids
    # admin rows carry the visibility annotation
    mag = next(s for s in admin if s["strategy_id"] == "intraday_magnifier")
    assert mag["visible_to_power_users"] is False
    assert mag["default_visible"] is False


def test_admin_can_flip_visibility_and_power_user_then_sees_it():
    _reset_visibility()
    assert sr.is_visible("intraday_magnifier") is False
    sr.set_visibility("intraday_magnifier", True, updated_by="operator")
    assert sr.is_visible("intraday_magnifier") is True
    non_admin = {s["strategy_id"] for s in sr.list_for_caller(is_admin=False)}
    assert "intraday_magnifier" in non_admin
    # and back off
    sr.set_visibility("intraday_magnifier", False, updated_by="operator")
    assert sr.is_visible("intraday_magnifier") is False
    _reset_visibility()


def test_set_visibility_rejects_unknown_strategy():
    import pytest
    with pytest.raises(ValueError):
        sr.set_visibility("not_a_real_strategy", True)
