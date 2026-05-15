"""Tests for auth.py — Google ID-token verify path + internal JWT cycle.

Strategy:
  * JWT issue/verify is pure crypto with our own secret — tested directly
  * Google ID-token verify path is stubbed via monkeypatch (real verification
    requires a live Google-issued token; covered by manual E2E test, not unit)
  * DB lookup functions tested against tmp SQLite with the real schema
  * Full sign_in_with_google flow tested with stubbed Google verify
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone

import pytest

from power_user.services import auth
from power_user.services.auth import (
    AuthError,
    GoogleUser,
    JWTPayload,
    find_user_by_email,
    find_user_by_google_sub,
    issue_jwt,
    sign_in_with_google,
    touch_last_seen,
    verify_jwt,
)
from power_user.db_init import init_power_user_schema

IST = timezone(timedelta(hours=5, minutes=30))


@pytest.fixture
def tmp_db():
    """Fresh SQLite file with the power_user schema applied."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_power_user_schema(path)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def seeded_user(tmp_db):
    """Insert a known user; return (db_path, user_dict)."""
    with sqlite3.connect(tmp_db) as con:
        con.execute("""
            INSERT INTO power_user_users
              (email, google_sub, display_name, role, is_active, created_at)
            VALUES (?, ?, ?, 'user', 1, ?)
        """, ("alice@example.com", "google_sub_alice", "Alice", datetime.now(IST).isoformat()))
        con.commit()
        row = con.execute(
            "SELECT * FROM power_user_users WHERE email = 'alice@example.com'"
        ).fetchone()
    return tmp_db, dict(zip(
        ("id","email","google_sub","display_name","picture_url","invite_code",
         "role","is_active","created_at","last_seen_at"),
        row
    ))


# ───────────────────────────────────────────────────────────────────
# JWT issue + verify cycle (pure crypto, no DB)
# ───────────────────────────────────────────────────────────────────

class TestJWTCycle:

    def test_issue_then_verify_roundtrip(self):
        token = issue_jwt(user_id=42, email="x@y.com", google_sub="sub42")
        payload = verify_jwt(token)
        assert payload.user_id == 42
        assert payload.email == "x@y.com"
        assert payload.google_sub == "sub42"
        assert payload.role == "user"
        assert payload.issued_at <= payload.expires_at
        assert payload.expires_at - payload.issued_at == 24 * 3600   # default 24h

    def test_custom_role_carries_through(self):
        token = issue_jwt(user_id=1, email="e", google_sub="s", role="partner")
        assert verify_jwt(token).role == "partner"

    def test_expired_token_rejected(self, monkeypatch):
        """Issue a token with iat far in the past so exp has expired."""
        # Make time.time() return a value 25 hours ago
        long_ago = int(time.time()) - 25 * 3600
        monkeypatch.setattr(auth.time, "time", lambda: long_ago)
        token = issue_jwt(user_id=1, email="e", google_sub="s")
        monkeypatch.undo()
        with pytest.raises(AuthError) as ei:
            verify_jwt(token)
        assert ei.value.code == "EXPIRED"

    def test_tampered_token_rejected(self):
        token = issue_jwt(user_id=1, email="e", google_sub="s")
        # Flip a byte in the signature segment
        head, payload, sig = token.split(".")
        bad = ".".join([head, payload, sig[:-2] + "AA"])
        with pytest.raises(AuthError) as ei:
            verify_jwt(bad)
        assert ei.value.code == "JWT_DECODE_FAILED"

    def test_empty_token_rejected(self):
        with pytest.raises(AuthError) as ei:
            verify_jwt("")
        assert ei.value.code == "JWT_DECODE_FAILED"

    def test_token_with_wrong_secret_rejected(self, monkeypatch):
        token = issue_jwt(user_id=1, email="e", google_sub="s")
        monkeypatch.setattr(auth.config, "POWER_JWT_SECRET", "different_secret_xxxx")
        with pytest.raises(AuthError) as ei:
            verify_jwt(token)
        assert ei.value.code == "JWT_DECODE_FAILED"


# ───────────────────────────────────────────────────────────────────
# DB lookup functions
# ───────────────────────────────────────────────────────────────────

class TestUserLookup:

    def test_find_by_google_sub_hit(self, seeded_user):
        db, user = seeded_user
        with sqlite3.connect(db) as con:
            found = find_user_by_google_sub(con, "google_sub_alice")
        assert found is not None
        assert found["email"] == "alice@example.com"

    def test_find_by_google_sub_miss(self, seeded_user):
        db, _ = seeded_user
        with sqlite3.connect(db) as con:
            found = find_user_by_google_sub(con, "nonexistent_sub")
        assert found is None

    def test_find_by_email_case_insensitive(self, seeded_user):
        db, _ = seeded_user
        with sqlite3.connect(db) as con:
            found = find_user_by_email(con, "ALICE@example.com")
        assert found is not None
        assert found["google_sub"] == "google_sub_alice"

    def test_find_by_email_whitespace_trimmed(self, seeded_user):
        db, _ = seeded_user
        with sqlite3.connect(db) as con:
            found = find_user_by_email(con, "  alice@example.com  ")
        assert found is not None

    def test_touch_last_seen_updates(self, seeded_user):
        db, user = seeded_user
        with sqlite3.connect(db) as con:
            touch_last_seen(con, user["id"])
            row = con.execute(
                "SELECT last_seen_at FROM power_user_users WHERE id = ?",
                (user["id"],),
            ).fetchone()
        assert row[0] is not None
        # Should be a recent IST timestamp
        assert "+05:30" in row[0]


# ───────────────────────────────────────────────────────────────────
# Google ID-token verify — error paths (real verification needs a
# live id_token, so we stub the lib at the import-site for unit tests)
# ───────────────────────────────────────────────────────────────────

class TestGoogleVerify:

    def test_missing_client_id_raises_config_missing(self, monkeypatch):
        monkeypatch.setattr(auth.config, "GOOGLE_CLIENT_ID", "")
        with pytest.raises(AuthError) as ei:
            auth.verify_google_id_token("anything")
        assert ei.value.code == "CONFIG_MISSING"

    def test_expired_google_token_mapped(self, monkeypatch):
        monkeypatch.setattr(auth.config, "GOOGLE_CLIENT_ID", "test-client-id")

        def fake_verify(*a, **k):
            raise ValueError("Token expired, 60 seconds in the past")

        monkeypatch.setattr(auth.google_id_token, "verify_oauth2_token", fake_verify)
        with pytest.raises(AuthError) as ei:
            auth.verify_google_id_token("bad")
        assert ei.value.code == "EXPIRED"

    def test_wrong_audience_mapped(self, monkeypatch):
        monkeypatch.setattr(auth.config, "GOOGLE_CLIENT_ID", "test-client-id")

        def fake_verify(*a, **k):
            raise ValueError("Wrong audience: got xyz, expected test-client-id")

        monkeypatch.setattr(auth.google_id_token, "verify_oauth2_token", fake_verify)
        with pytest.raises(AuthError) as ei:
            auth.verify_google_id_token("bad")
        assert ei.value.code == "WRONG_AUD"

    def test_unverified_email_rejected(self, monkeypatch):
        monkeypatch.setattr(auth.config, "GOOGLE_CLIENT_ID", "test-client-id")
        monkeypatch.setattr(auth.google_id_token, "verify_oauth2_token",
                            lambda *a, **k: {
                                "sub": "s1", "email": "a@b.com",
                                "email_verified": False,
                                "iss": "accounts.google.com",
                            })
        with pytest.raises(AuthError) as ei:
            auth.verify_google_id_token("token")
        assert ei.value.code == "EMAIL_UNVERIFIED"

    def test_wrong_issuer_rejected(self, monkeypatch):
        monkeypatch.setattr(auth.config, "GOOGLE_CLIENT_ID", "test-client-id")
        monkeypatch.setattr(auth.google_id_token, "verify_oauth2_token",
                            lambda *a, **k: {
                                "sub": "s1", "email": "a@b.com",
                                "email_verified": True,
                                "iss": "evil.example.com",
                            })
        with pytest.raises(AuthError) as ei:
            auth.verify_google_id_token("token")
        assert ei.value.code == "INVALID_GOOGLE_TOKEN"

    def test_happy_path_returns_google_user(self, monkeypatch):
        monkeypatch.setattr(auth.config, "GOOGLE_CLIENT_ID", "test-client-id")
        monkeypatch.setattr(auth.google_id_token, "verify_oauth2_token",
                            lambda *a, **k: {
                                "sub":            "s1",
                                "email":          "Bob@Example.COM",
                                "email_verified": True,
                                "iss":            "https://accounts.google.com",
                                "name":           "Bob",
                                "picture":        "http://...",
                            })
        g = auth.verify_google_id_token("good")
        assert isinstance(g, GoogleUser)
        assert g.google_sub == "s1"
        assert g.email == "bob@example.com"   # lowercased + trimmed
        assert g.display_name == "Bob"


# ───────────────────────────────────────────────────────────────────
# Full sign_in_with_google flow
# ───────────────────────────────────────────────────────────────────

class TestSignInFlow:

    def _stub_google(self, monkeypatch, google_sub="g_new", email="new@example.com"):
        monkeypatch.setattr(auth.config, "GOOGLE_CLIENT_ID", "test-client-id")
        monkeypatch.setattr(auth.google_id_token, "verify_oauth2_token",
                            lambda *a, **k: {
                                "sub":            google_sub,
                                "email":          email,
                                "email_verified": True,
                                "iss":            "accounts.google.com",
                                "name":           "Test User",
                            })

    def test_unknown_user_returns_needs_invite(self, tmp_db, monkeypatch):
        self._stub_google(monkeypatch)
        with sqlite3.connect(tmp_db) as con:
            result = sign_in_with_google(con, "id_token_str")
        assert result["status"] == "needs_invite"
        assert result["email"] == "new@example.com"
        assert result["google_sub"] == "g_new"

    def test_known_user_returns_jwt(self, seeded_user, monkeypatch):
        db, user = seeded_user
        self._stub_google(monkeypatch, google_sub="google_sub_alice", email="alice@example.com")
        with sqlite3.connect(db) as con:
            result = sign_in_with_google(con, "id_token_str")
        assert result["status"] == "ok"
        assert "jwt" in result
        # JWT must round-trip
        payload = verify_jwt(result["jwt"])
        assert payload.user_id == user["id"]
        assert payload.email == "alice@example.com"

    def test_inactive_user_rejected(self, seeded_user, monkeypatch):
        db, user = seeded_user
        with sqlite3.connect(db) as con:
            con.execute("UPDATE power_user_users SET is_active=0 WHERE id=?", (user["id"],))
            con.commit()
        self._stub_google(monkeypatch, google_sub="google_sub_alice", email="alice@example.com")
        with pytest.raises(AuthError) as ei, sqlite3.connect(db) as con:
            sign_in_with_google(con, "id_token_str")
        assert ei.value.code == "USER_INACTIVE"

    def test_email_match_updates_google_sub(self, seeded_user, monkeypatch):
        """If user's email matches but google_sub differs (Google account rotated),
        we should update google_sub instead of treating as new user."""
        db, user = seeded_user
        self._stub_google(monkeypatch, google_sub="ROTATED_SUB", email="alice@example.com")
        with sqlite3.connect(db) as con:
            result = sign_in_with_google(con, "id_token_str")
        assert result["status"] == "ok"
        # google_sub should now reflect the new value in DB
        with sqlite3.connect(db) as con:
            row = con.execute(
                "SELECT google_sub FROM power_user_users WHERE email='alice@example.com'"
            ).fetchone()
        assert row[0] == "ROTATED_SUB"

    def test_last_seen_updated_on_sign_in(self, seeded_user, monkeypatch):
        db, user = seeded_user
        self._stub_google(monkeypatch, google_sub="google_sub_alice", email="alice@example.com")
        with sqlite3.connect(db) as con:
            sign_in_with_google(con, "id_token_str")
            row = con.execute(
                "SELECT last_seen_at FROM power_user_users WHERE id=?", (user["id"],)
            ).fetchone()
        assert row[0] is not None
