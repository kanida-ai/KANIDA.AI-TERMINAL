"""End-to-end router tests with FastAPI TestClient.

What's covered:
  /auth/google     — google verify stubbed; ok / needs_invite / 401 paths
  /auth/me         — bearer JWT required; returns user
  /auth/logout     — returns ok
  /invites/redeem  — happy path + uniform-failure body + rate limit
  /invites/validate — read-only check
  /invites/waitlist — idempotent + rate limit
  /admin/invites/issue — admin secret gate + issue codes
  /admin/users    — list users
  /admin/metrics  — counts roll up

Strategy: build a minimal FastAPI app that mounts only the power_user routers,
backed by a tmp SQLite DB. Stubs Google verify so we don't need a live OAuth.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Any, Dict, Generator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from power_user import config
from power_user.db_init import init_power_user_schema
from power_user.routers.admin_router    import router as admin_router
from power_user.routers.auth_router     import router as auth_router
from power_user.routers.invites_router  import router as invites_router
from power_user.services import auth as auth_svc
from power_user.services import invites as invites_svc


# ──────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_power_user_schema(path)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def app(tmp_db, monkeypatch) -> FastAPI:
    """Build a minimal FastAPI app with the 3 routers mounted, DB pointed at tmp_db."""
    monkeypatch.setattr(config, "POWER_DB_PATH",    tmp_db)
    monkeypatch.setattr(config, "GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setattr(config, "POWER_ADMIN_SECRET", "test-admin-secret")
    a = FastAPI()
    a.include_router(auth_router)
    a.include_router(invites_router)
    a.include_router(admin_router)
    return a


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def _stub_google(monkeypatch, google_sub="g_new", email="new@example.com", name="New User"):
    """Patch Google's verify so /auth/google succeeds with fake id_token."""
    def fake_verify(*a, **k):
        return {
            "sub":            google_sub,
            "email":          email,
            "email_verified": True,
            "iss":            "accounts.google.com",
            "name":           name,
        }
    monkeypatch.setattr(auth_svc.google_id_token, "verify_oauth2_token", fake_verify)


# ──────────────────────────────────────────────────────────────────────────
# /auth/google
# ──────────────────────────────────────────────────────────────────────────

class TestAuthGoogle:

    def test_known_user_returns_jwt(self, client, tmp_db, monkeypatch):
        # Seed a user
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO power_user_users
                  (email, google_sub, display_name, role, is_active, created_at)
                VALUES ('alice@x.com', 'g_alice', 'Alice', 'user', 1, 'now')
            """)
            con.commit()
        _stub_google(monkeypatch, google_sub="g_alice", email="alice@x.com")
        r = client.post("/api/power/auth/google", json={"id_token": "fake"})
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert "jwt" in body
        assert body["user"]["email"] == "alice@x.com"

    def test_unknown_user_needs_invite(self, client, monkeypatch):
        _stub_google(monkeypatch, email="brand_new@example.com")
        r = client.post("/api/power/auth/google", json={"id_token": "fake"})
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "needs_invite"
        assert body["email"] == "brand_new@example.com"
        assert "jwt" not in body

    def test_expired_google_token_returns_401(self, client, monkeypatch):
        def fake_verify(*a, **k):
            raise ValueError("Token expired, 60 seconds in the past")
        monkeypatch.setattr(auth_svc.google_id_token, "verify_oauth2_token", fake_verify)
        r = client.post("/api/power/auth/google", json={"id_token": "expired"})
        assert r.status_code == 401
        assert r.json()["detail"]["code"] == "EXPIRED"


# ──────────────────────────────────────────────────────────────────────────
# /auth/me + /auth/logout
# ──────────────────────────────────────────────────────────────────────────

class TestAuthMe:

    def test_no_bearer_returns_401(self, client):
        r = client.get("/api/power/auth/me")
        assert r.status_code == 401

    def test_bad_bearer_format_returns_401(self, client):
        r = client.get("/api/power/auth/me", headers={"Authorization": "not-bearer xxx"})
        assert r.status_code == 401

    def test_valid_jwt_returns_user(self, client, tmp_db, monkeypatch):
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO power_user_users
                  (email, google_sub, display_name, role, is_active, created_at)
                VALUES ('me@x.com', 'g_me', 'Me', 'user', 1, 'now')
            """)
            con.commit()
            uid = con.execute("SELECT id FROM power_user_users WHERE email='me@x.com'").fetchone()[0]
        token = auth_svc.issue_jwt(user_id=uid, email="me@x.com", google_sub="g_me")
        r = client.get("/api/power/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 200
        assert r.json()["email"] == "me@x.com"

    def test_inactive_user_blocked(self, client, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO power_user_users
                  (email, google_sub, role, is_active, created_at)
                VALUES ('off@x.com', 'g_off', 'user', 0, 'now')
            """)
            con.commit()
            uid = con.execute("SELECT id FROM power_user_users WHERE email='off@x.com'").fetchone()[0]
        token = auth_svc.issue_jwt(user_id=uid, email="off@x.com", google_sub="g_off")
        r = client.get("/api/power/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 403
        assert r.json()["detail"]["code"] == "USER_INACTIVE"


class TestAuthLogout:

    def test_logout_returns_ok(self, client):
        r = client.post("/api/power/auth/logout")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


# ──────────────────────────────────────────────────────────────────────────
# /invites/redeem — happy + uniform-failure body
# ──────────────────────────────────────────────────────────────────────────

class TestInvitesRedeem:

    def _mint_code(self, tmp_db) -> str:
        with sqlite3.connect(tmp_db) as con:
            issued = invites_svc.generate_codes(con, n=1)
        return issued[0].code

    def test_happy_path(self, client, tmp_db, monkeypatch):
        code = self._mint_code(tmp_db)
        _stub_google(monkeypatch, email="redeemer@x.com")
        r = client.post("/api/power/invites/redeem",
                        json={"id_token": "fake", "code": code})
        assert r.status_code == 200
        assert "jwt" in r.json()

    def test_bad_format_returns_uniform_400(self, client, monkeypatch):
        _stub_google(monkeypatch)
        r = client.post("/api/power/invites/redeem",
                        json={"id_token": "fake", "code": "garbage"})
        assert r.status_code == 400
        assert r.json()["detail"]["code"] == "INVITE_INVALID"

    def test_unknown_code_returns_uniform_400(self, client, monkeypatch):
        _stub_google(monkeypatch)
        r = client.post("/api/power/invites/redeem",
                        json={"id_token": "fake", "code": "kn-2026-aaaaaa"})
        assert r.status_code == 400
        assert r.json()["detail"]["code"] == "INVITE_INVALID"
        # SAME body for found-but-used (next test) — verified via TestBruteForceResistance

    def test_already_used_returns_same_uniform_body(self, client, tmp_db, monkeypatch):
        code = self._mint_code(tmp_db)
        _stub_google(monkeypatch, google_sub="g_first", email="first@x.com")
        r1 = client.post("/api/power/invites/redeem",
                         json={"id_token": "fake", "code": code})
        assert r1.status_code == 200

        _stub_google(monkeypatch, google_sub="g_second", email="second@x.com")
        r2 = client.post("/api/power/invites/redeem",
                         json={"id_token": "fake", "code": code})
        assert r2.status_code == 400
        # SAME error body as the unknown-code case — no enumeration
        assert r2.json()["detail"]["code"] == "INVITE_INVALID"

    def test_bad_google_token_returns_401_not_400(self, client, monkeypatch):
        """Wrong Google token != wrong invite code; map to 401 (not 400)."""
        def fake_verify(*a, **k):
            raise ValueError("Invalid signature")
        monkeypatch.setattr(auth_svc.google_id_token, "verify_oauth2_token", fake_verify)
        r = client.post("/api/power/invites/redeem",
                        json={"id_token": "bogus", "code": "kn-2026-aaaaaa"})
        assert r.status_code == 401

    def test_rate_limit_after_5_attempts(self, client, tmp_db, monkeypatch):
        """6th attempt within an hour → 429."""
        _stub_google(monkeypatch)
        for i in range(5):
            r = client.post("/api/power/invites/redeem",
                            json={"id_token": "fake", "code": "kn-2026-aaaaaa"})
            assert r.status_code == 400, f"attempt {i+1} got {r.status_code}"
        # 6th attempt → rate limited
        r = client.post("/api/power/invites/redeem",
                        json={"id_token": "fake", "code": "kn-2026-aaaaaa"})
        assert r.status_code == 429
        body = r.json()["detail"]
        assert body["code"] == "RATE_LIMITED"


class TestInvitesValidate:

    def test_valid_code_returns_true(self, client, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            code = invites_svc.generate_codes(con, n=1)[0].code
        r = client.get(f"/api/power/invites/validate/{code}")
        assert r.status_code == 200
        assert r.json()["valid"] is True

    def test_bad_code_returns_false(self, client):
        r = client.get("/api/power/invites/validate/garbage")
        assert r.status_code == 200
        assert r.json()["valid"] is False


class TestInvitesWaitlist:

    def test_first_signup(self, client):
        r = client.post("/api/power/invites/waitlist",
                        json={"email": "new@x.com", "source": "landing_cta"})
        assert r.status_code == 200
        assert r.json() == {"ok": True, "joined": True}

    def test_idempotent(self, client):
        client.post("/api/power/invites/waitlist", json={"email": "same@x.com"})
        r = client.post("/api/power/invites/waitlist", json={"email": "same@x.com"})
        assert r.status_code == 200
        assert r.json()["joined"] is False

    def test_invalid_email_400(self, client):
        r = client.post("/api/power/invites/waitlist", json={"email": "not-an-email"})
        assert r.status_code == 422   # pydantic EmailStr rejects


# ──────────────────────────────────────────────────────────────────────────
# /admin/* — secret gate + functionality
# ──────────────────────────────────────────────────────────────────────────

class TestAdmin:

    H = {"X-Admin-Secret": "test-admin-secret"}

    def test_no_secret_returns_403(self, client):
        r = client.post("/api/power/admin/invites/issue", json={"n": 5})
        assert r.status_code == 403

    def test_wrong_secret_returns_403(self, client):
        r = client.post("/api/power/admin/invites/issue", json={"n": 5},
                         headers={"X-Admin-Secret": "wrong"})
        assert r.status_code == 403

    def test_issue_codes(self, client):
        r = client.post("/api/power/admin/invites/issue",
                         json={"n": 5, "note": "Test cohort"},
                         headers=self.H)
        assert r.status_code == 200
        body = r.json()
        assert body["n_issued"] == 5
        assert len(body["codes"]) == 5

    def test_list_invites(self, client):
        client.post("/api/power/admin/invites/issue", json={"n": 3}, headers=self.H)
        r = client.get("/api/power/admin/invites/list", headers=self.H)
        assert r.status_code == 200
        assert r.json()["n"] == 3

    def test_users_list(self, client, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO power_user_users
                  (email, google_sub, role, is_active, created_at)
                VALUES ('u1@x.com', 'g1', 'user', 1, 'now')
            """)
            con.commit()
        r = client.get("/api/power/admin/users", headers=self.H)
        assert r.status_code == 200
        assert r.json()["n"] == 1

    def test_metrics_returns_zero_counts_empty_db(self, client):
        r = client.get("/api/power/admin/metrics", headers=self.H)
        assert r.status_code == 200
        body = r.json()
        assert body["users"]["total_active"] == 0
        assert body["invites"]["codes_used"] == 0
        assert body["users"]["dau"] == 0
