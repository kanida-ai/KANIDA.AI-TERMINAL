"""Auth-scoping tests for the P&L dashboard ROUTER (power_jwt gate).

Calls require_power_caller + the endpoint functions directly (no HTTP server),
minting real JWTs via power_user.services.auth.issue_jwt, on the isolated temp DB.
Proves: no token → 401; user JWT → own scope; admin JWT → all + ?user_id filter.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from falcon.db import falcon_conn
from power_user.services.auth import issue_jwt

from autotrade.api import pnl_routes as pr
from autotrade.api.pnl_routes import require_power_caller, pnl_summary

IST = timezone(timedelta(hours=5, minutes=30))
USER_A, USER_B = "101", "202"
WIN_FROM, WIN_TO = "2026-06-20", "2026-06-30"
STARTED = "2026-06-24T09:15:00+05:30"
IN_WINDOW = "2026-06-24T15:29:00+05:30"


def _make_request(headers=None, cookies=None) -> Request:
    raw = []
    for k, v in (headers or {}).items():
        raw.append((k.lower().encode(), v.encode()))
    if cookies:
        raw.append((b"cookie", "; ".join(f"{k}={v}" for k, v in cookies.items()).encode()))
    return Request({"type": "http", "method": "GET", "path": "/",
                    "headers": raw, "query_string": b""})


def _bearer(user_id, role="user"):
    tok = issue_jwt(user_id=int(user_id), email=f"u{user_id}@x.com",
                    google_sub=f"s{user_id}", role=role)
    return _make_request(headers={"Authorization": f"Bearer {tok}"})


def _cookie(user_id, role="user"):
    tok = issue_jwt(user_id=int(user_id), email=f"u{user_id}@x.com",
                    google_sub=f"s{user_id}", role=role)
    return _make_request(cookies={"power_jwt": tok})


def _seed(session_id, user_id, symbol):
    cfg = {"strategy": "intraday_basket", "square_off_enabled": True,
           "order_product": "CNC", "total_allocated_capital": 100000.0}
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_sessions (session_id, created_at, started_at, "
            "status, mode, total_allocated_capital, invested_basis, config_json, "
            "ladder_id, user_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (session_id, STARTED, STARTED, "CLOSED", "live", 100000.0, 100000.0,
             json.dumps(cfg), None, user_id))
        con.execute(
            "INSERT INTO autotrade_positions (session_id, symbol, instrument_type, "
            "qty, avg_price, exit_price, realised_pnl, status, opened_at, closed_at, "
            "close_reason, direction, entry_order_id, exit_order_id) "
            "VALUES (?,?,?,?,?,?,?,'CLOSED',?,?,?,?,?,?)",
            (session_id, symbol, "EQ", 10, 100.0, 110.0, 100.0, STARTED,
             IN_WINDOW, "SQUARE_OFF", "long", "E1", "X1"))
        con.commit()


def test_no_token_401():
    with pytest.raises(HTTPException) as ei:
        require_power_caller(_make_request())
    assert ei.value.status_code == 401


def test_bad_token_401():
    req = _make_request(headers={"Authorization": "Bearer not-a-jwt"})
    with pytest.raises(HTTPException) as ei:
        require_power_caller(req)
    assert ei.value.status_code == 401


def test_bearer_resolves_user():
    c = require_power_caller(_bearer(USER_A))
    assert c.user_id == USER_A and c.is_admin is False and c.authenticated


def test_cookie_resolves_user():
    c = require_power_caller(_cookie(USER_A))
    assert c.user_id == USER_A and c.authenticated


def test_admin_role():
    c = require_power_caller(_bearer("1", role="admin"))
    assert c.is_admin is True


def test_endpoint_user_scoped(clean_positions):
    _seed("s_a", USER_A, "AAA")
    _seed("s_b", USER_B, "BBB")
    caller = require_power_caller(_bearer(USER_A))
    out = pnl_summary(request=_bearer(USER_A), period="custom", from_=WIN_FROM,
                      to=WIN_TO, mode="live", user_id=None, caller=caller)
    syms = {d["id"] for s in out["strategies"] for d in s["sessions"]}
    assert "s_a" in syms and "s_b" not in syms


def test_endpoint_admin_all(clean_positions):
    _seed("s_a", USER_A, "AAA")
    _seed("s_b", USER_B, "BBB")
    caller = require_power_caller(_bearer("1", role="admin"))
    out = pnl_summary(request=_bearer("1", role="admin"), period="custom",
                      from_=WIN_FROM, to=WIN_TO, mode="live", user_id=None,
                      caller=caller)
    session_ids = {d["id"] for s in out["strategies"] for d in s["sessions"]}
    assert {"s_a", "s_b"} <= session_ids


def test_endpoint_non_admin_ignores_user_id_param(clean_positions):
    _seed("s_a", USER_A, "AAA")
    _seed("s_b", USER_B, "BBB")
    # User A tries to probe user B via ?user_id=B → must be ignored (own scope).
    caller = require_power_caller(_bearer(USER_A))
    out = pnl_summary(request=_bearer(USER_A), period="custom", from_=WIN_FROM,
                      to=WIN_TO, mode="live", user_id=USER_B, caller=caller)
    session_ids = {d["id"] for s in out["strategies"] for d in s["sessions"]}
    assert session_ids == {"s_a"}
