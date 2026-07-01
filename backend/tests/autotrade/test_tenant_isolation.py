"""PER-USER TENANT ISOLATION tests for the AutoTrade Sessions API.

SECURITY: a logged-in Power User must NOT see/read/kill/delete the operator's or
another user's AutoTrade sessions. These tests exercise the enforcement wired
into backend/autotrade/api/autotrade_routes.py:
  * resolve_caller()        — JWT (Bearer / power_jwt cookie) → Caller identity
  * _assert_session_access()— per-session ownership gate (404 on non-owner)
  * TradingSession.list_sessions(owner_user_id=...) — strict WHERE s.user_id = ?

They call the endpoint functions directly (no HTTP server) with the isolated
temp DB from conftest.py, minting real JWTs via power_user.services.auth.issue_jwt
(the dev-fallback POWER_JWT_SECRET is process-stable, so issue_jwt/verify_jwt
agree within the test process).

No real broker, no real Kite, no real orders.
"""
import uuid

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from falcon.db import falcon_conn
from power_user.services.auth import issue_jwt

from autotrade.api import autotrade_routes as ar
from autotrade.api.autotrade_routes import (
    Caller,
    resolve_caller,
    _assert_session_access,
    session_list,
    session_status,
    session_positions,
    session_delete,
    session_create,
    CreateSessionRequest,
)
from autotrade.session import TradingSession
from autotrade.config import TradingSessionConfig


# ── request builders ─────────────────────────────────────────────────────────

def _make_request(headers=None, cookies=None) -> Request:
    """Build a minimal ASGI Request carrying optional headers + a Cookie header."""
    raw_headers = []
    for k, v in (headers or {}).items():
        raw_headers.append((k.lower().encode(), v.encode()))
    if cookies:
        cookie_val = "; ".join(f"{k}={v}" for k, v in cookies.items())
        raw_headers.append((b"cookie", cookie_val.encode()))
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": raw_headers,
        "query_string": b"",
    }
    return Request(scope)


def _bearer_request(user_id: int, role: str = "user") -> Request:
    tok = issue_jwt(user_id=user_id, email=f"u{user_id}@x.com",
                    google_sub=f"sub{user_id}", role=role)
    return _make_request(headers={"Authorization": f"Bearer {tok}"})


def _cookie_request(user_id: int, role: str = "user") -> Request:
    tok = issue_jwt(user_id=user_id, email=f"u{user_id}@x.com",
                    google_sub=f"sub{user_id}", role=role)
    return _make_request(cookies={"power_jwt": tok})


# ── session-row seeding (direct DB, no orders) ───────────────────────────────

def _make_session(user_id):
    """Create a real autotrade_sessions row owned by user_id (or None) and return
    its session_id."""
    cfg = TradingSessionConfig(total_allocated_capital=100000.0)
    sess = TradingSession.create(cfg, mode="paper", user_id=user_id)
    return sess.session_id


# ── resolve_caller ───────────────────────────────────────────────────────────

def test_resolve_caller_bearer_jwt_user():
    c = resolve_caller(_bearer_request(2, role="user"))
    assert c.user_id == "2"
    assert c.is_admin is False
    assert c.authenticated is True


def test_resolve_caller_cookie_jwt_user():
    c = resolve_caller(_cookie_request(7, role="user"))
    assert c.user_id == "7"
    assert c.is_admin is False
    assert c.authenticated is True


def test_resolve_caller_admin_jwt():
    c = resolve_caller(_bearer_request(1, role="admin"))
    assert c.user_id == "1"
    assert c.is_admin is True
    assert c.authenticated is True


def test_resolve_caller_no_jwt_nonstrict_is_admin_operator(monkeypatch):
    """(e) no-JWT operator path unchanged when strict off → admin, unauthenticated."""
    monkeypatch.delenv("AUTOTRADE_PORTAL_STRICT", raising=False)
    c = resolve_caller(_make_request())
    assert c.user_id is None
    assert c.is_admin is True
    assert c.authenticated is False


def test_resolve_caller_no_jwt_strict_rejects(monkeypatch):
    """(f) strict-on rejects no-JWT with 401."""
    monkeypatch.setenv("AUTOTRADE_PORTAL_STRICT", "true")
    with pytest.raises(HTTPException) as ei:
        resolve_caller(_make_request())
    assert ei.value.status_code == 401


def test_resolve_caller_invalid_jwt_falls_to_operator(monkeypatch):
    """A garbage token is NOT an authenticated user; non-strict → operator path."""
    monkeypatch.delenv("AUTOTRADE_PORTAL_STRICT", raising=False)
    req = _make_request(headers={"Authorization": "Bearer not.a.jwt"})
    c = resolve_caller(req)
    assert c.authenticated is False and c.is_admin is True


def test_resolve_caller_import_error_does_not_500(monkeypatch):
    """Defensive: an auth-stack import/verify failure must NOT 500 the router —
    it degrades to the unauthenticated operator path (non-strict)."""
    monkeypatch.delenv("AUTOTRADE_PORTAL_STRICT", raising=False)

    def _boom(_tok):
        raise RuntimeError("auth stack exploded")

    import power_user.services.auth as pa
    monkeypatch.setattr(pa, "verify_jwt", _boom)
    req = _make_request(headers={"Authorization": "Bearer anything"})
    c = resolve_caller(req)  # must not raise
    assert c.authenticated is False and c.is_admin is True


# ── _assert_session_access ───────────────────────────────────────────────────

def test_assert_access_owner_ok(clean_positions):
    sid = _make_session("2")
    _assert_session_access(sid, Caller(user_id="2", is_admin=False,
                                       authenticated=True))  # no raise


def test_assert_access_nonowner_404(clean_positions):
    sid = _make_session("2")
    with pytest.raises(HTTPException) as ei:
        _assert_session_access(sid, Caller(user_id="3", is_admin=False,
                                           authenticated=True))
    assert ei.value.status_code == 404


def test_assert_access_null_owned_invisible_to_user(clean_positions):
    """NULL-owned (operator/legacy) session is invisible to a non-admin user."""
    sid = _make_session(None)
    with pytest.raises(HTTPException) as ei:
        _assert_session_access(sid, Caller(user_id="2", is_admin=False,
                                           authenticated=True))
    assert ei.value.status_code == 404


def test_assert_access_admin_sees_any(clean_positions):
    sid_user = _make_session("2")
    sid_null = _make_session(None)
    admin = Caller(user_id=None, is_admin=True, authenticated=False)
    _assert_session_access(sid_user, admin)  # no raise
    _assert_session_access(sid_null, admin)  # no raise


def test_assert_access_missing_session_404(clean_positions):
    with pytest.raises(HTTPException) as ei:
        _assert_session_access("does-not-exist", Caller(
            user_id="2", is_admin=False, authenticated=True))
    assert ei.value.status_code == 404


# ── (a) non-admin sees ONLY their own, not NULL-owned or others' ─────────────

def test_session_list_nonadmin_only_own(clean_positions):
    mine = _make_session("2")
    _make_session("3")     # other user
    _make_session(None)    # operator/legacy

    caller = Caller(user_id="2", is_admin=False, authenticated=True)
    out = session_list(user_id=None, caller=caller)
    ids = {s["session_id"] for s in out["sessions"]}
    assert ids == {mine}


def test_session_list_nonadmin_ignores_supplied_user_id(clean_positions):
    """A non-admin cannot widen scope by passing someone else's ?user_id."""
    mine = _make_session("2")
    other = _make_session("3")
    caller = Caller(user_id="2", is_admin=False, authenticated=True)
    out = session_list(user_id="3", caller=caller)  # try to peek at user 3
    ids = {s["session_id"] for s in out["sessions"]}
    assert ids == {mine}
    assert other not in ids


# ── (c) admin sees all ───────────────────────────────────────────────────────

def test_session_list_admin_sees_all(clean_positions):
    a = _make_session("2")
    b = _make_session("3")
    c = _make_session(None)
    admin = Caller(user_id=None, is_admin=True, authenticated=False)
    out = session_list(user_id=None, caller=admin)
    ids = {s["session_id"] for s in out["sessions"]}
    assert {a, b, c}.issubset(ids)


def test_session_list_admin_can_scope_to_user(clean_positions):
    """Operator console's existing ?user_id scope still works for admin."""
    a = _make_session("2")
    _make_session("3")
    admin = Caller(user_id=None, is_admin=True, authenticated=False)
    out = session_list(user_id="2", caller=admin)
    ids = {s["session_id"] for s in out["sessions"]}
    assert ids == {a}


# ── (b) non-admin status/positions/delete on a session they don't own → 404 ──

def test_status_nonowner_404(clean_positions):
    sid = _make_session("2")
    caller = Caller(user_id="3", is_admin=False, authenticated=True)
    with pytest.raises(HTTPException) as ei:
        session_status(sid, caller=caller)
    assert ei.value.status_code == 404


def test_positions_nonowner_404(clean_positions):
    sid = _make_session("2")
    caller = Caller(user_id="3", is_admin=False, authenticated=True)
    with pytest.raises(HTTPException) as ei:
        session_positions(sid, caller=caller)
    assert ei.value.status_code == 404


def test_delete_nonowner_404_and_row_survives(clean_positions):
    sid = _make_session("2")
    caller = Caller(user_id="3", is_admin=False, authenticated=True)
    with pytest.raises(HTTPException) as ei:
        session_delete(sid, caller=caller)
    assert ei.value.status_code == 404
    # The victim row must still exist (delete was blocked before touching rows).
    with falcon_conn() as con:
        row = con.execute(
            "SELECT 1 FROM autotrade_sessions WHERE session_id=?", (sid,)
        ).fetchone()
    assert row is not None


def test_delete_owner_ok(clean_positions):
    sid = _make_session("2")
    caller = Caller(user_id="2", is_admin=False, authenticated=True)
    out = session_delete(sid, caller=caller)
    assert out["deleted"] == 1
    with falcon_conn() as con:
        row = con.execute(
            "SELECT 1 FROM autotrade_sessions WHERE session_id=?", (sid,)
        ).fetchone()
    assert row is None


def test_admin_can_act_on_any(clean_positions):
    """(c) admin can status + delete a user-owned session."""
    sid = _make_session("2")
    admin = Caller(user_id=None, is_admin=True, authenticated=False)
    st = session_status(sid, caller=admin)
    assert st is not None
    out = session_delete(sid, caller=admin)
    assert out["deleted"] == 1


# ── (d) create as non-admin forces owner = caller ────────────────────────────

def test_create_nonadmin_forces_owner(clean_positions):
    caller = Caller(user_id="2", is_admin=False, authenticated=True)
    # Client tries to plant the session under user 9 — must be ignored.
    req = CreateSessionRequest(config={"total_allocated_capital": 100000.0},
                               mode="paper", user_id="9")
    out = session_create(req, caller=caller)
    assert out["user_id"] == "2"
    with falcon_conn() as con:
        row = con.execute(
            "SELECT user_id FROM autotrade_sessions WHERE session_id=?",
            (out["session_id"],)).fetchone()
    assert str(row["user_id"]) == "2"


def test_create_admin_may_set_user_id(clean_positions):
    admin = Caller(user_id=None, is_admin=True, authenticated=False)
    req = CreateSessionRequest(config={"total_allocated_capital": 100000.0},
                               mode="paper", user_id="42")
    out = session_create(req, caller=admin)
    assert out["user_id"] == "42"


def test_create_operator_no_user_id_unchanged(clean_positions):
    """(e) operator (no JWT → admin/unauth) creating with no user_id → NULL-owned,
    exactly as today."""
    admin = Caller(user_id=None, is_admin=True, authenticated=False)
    req = CreateSessionRequest(config={"total_allocated_capital": 100000.0},
                               mode="paper", user_id=None)
    out = session_create(req, caller=admin)
    assert out["user_id"] is None
    with falcon_conn() as con:
        row = con.execute(
            "SELECT user_id FROM autotrade_sessions WHERE session_id=?",
            (out["session_id"],)).fetchone()
    assert row["user_id"] is None


# ── list_sessions strict param directly ──────────────────────────────────────

def test_list_sessions_owner_param_excludes_null_and_others(clean_positions):
    mine = _make_session("2")
    _make_session("3")
    _make_session(None)
    rows = TradingSession.list_sessions(owner_user_id="2")
    assert {r["session_id"] for r in rows} == {mine}


def test_list_sessions_no_scope_is_full_view(clean_positions):
    a = _make_session("2")
    b = _make_session(None)
    rows = TradingSession.list_sessions()
    ids = {r["session_id"] for r in rows}
    assert {a, b}.issubset(ids)
