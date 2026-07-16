"""API surface for the strategy list + admin visibility endpoints (Task 2)."""
import pytest

from autotrade.api.autotrade_routes import (
    Caller, list_strategies, admin_list_strategies,
    admin_set_strategy_visibility, StrategyVisibilityRequest)
from fastapi import HTTPException


ADMIN = Caller(user_id=None, is_admin=True, authenticated=False)
POWER = Caller(user_id="u1", is_admin=False, authenticated=True)


def _reset():
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute("DELETE FROM strategy_visibility")
        con.commit()


def test_non_admin_strategy_list_excludes_hidden():
    _reset()
    ids = {s["strategy_id"] for s in list_strategies(caller=POWER)["strategies"]}
    assert "intraday_magnifier" not in ids
    assert "falcon_btst_oscillator" not in ids
    assert "portfolio_kill_switch" in ids


def test_admin_strategy_list_includes_hidden():
    _reset()
    ids = {s["strategy_id"] for s in list_strategies(caller=ADMIN)["strategies"]}
    assert {"intraday_magnifier", "falcon_btst_oscillator"} <= ids


def test_admin_endpoints_require_admin():
    with pytest.raises(HTTPException) as e:
        admin_list_strategies(caller=POWER)
    assert e.value.status_code == 403
    with pytest.raises(HTTPException) as e2:
        admin_set_strategy_visibility(
            StrategyVisibilityRequest(strategy_id="intraday_magnifier",
                                      visible=True), caller=POWER)
    assert e2.value.status_code == 403


def test_admin_flip_then_power_user_sees_it():
    _reset()
    admin_set_strategy_visibility(
        StrategyVisibilityRequest(strategy_id="intraday_magnifier", visible=True),
        caller=ADMIN)
    ids = {s["strategy_id"] for s in list_strategies(caller=POWER)["strategies"]}
    assert "intraday_magnifier" in ids
    _reset()


def test_admin_set_unknown_strategy_400():
    with pytest.raises(HTTPException) as e:
        admin_set_strategy_visibility(
            StrategyVisibilityRequest(strategy_id="nope", visible=True),
            caller=ADMIN)
    assert e.value.status_code == 400
