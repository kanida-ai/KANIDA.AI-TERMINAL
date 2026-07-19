"""Tests for the laptop → cloud Kite access-token SYNC ingest endpoint.

    POST /api/falcon/publish/kite-token   (falcon/routers/publish_router.py)

Cases:
  - auth: secret unset          → 503 PUBLISH_NOT_CONFIGURED (fail closed)
  - auth: missing/wrong header  → 403 FORBIDDEN
  - empty / whitespace token    → 400 EMPTY_TOKEN
  - happy path                  → 200 {ok, stored:true}; the token is then
                                  returned by kite_auth get_access_token() /
                                  _load_token_from_db()
  - idempotent re-post          → today's row is REPLACED (one row), latest wins

Strategy: mount only publish_router; point kite_auth.DB_PATH at a tmp SQLite DB
so we never touch the real kite_tokens store. The endpoint persists through
kite_auth._save_token_to_db (lazy-imported), which reads DB_PATH via _conn(), so
monkeypatching the module global is sufficient.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from falcon.routers import publish_router
from services import kite_auth

SECRET = "test-publish-secret"
URL = "/api/falcon/publish/kite-token"
TOKEN = "aBcDeFgH12345_live_token_value"


@pytest.fixture
def tmp_kite_db(monkeypatch):
    """Redirect kite_auth's DB_PATH at a throwaway SQLite file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    monkeypatch.setattr(kite_auth, "DB_PATH", path)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def app(tmp_kite_db, monkeypatch):
    monkeypatch.setenv("FALCON_PUBLISH_SECRET", SECRET)
    # Keep get_access_token off the env fallback so the DB path is what we test.
    monkeypatch.delenv("KITE_ACCESS_TOKEN", raising=False)
    a = FastAPI()
    a.include_router(publish_router.router, prefix="/api")
    return a


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=False)


# ── Auth ────────────────────────────────────────────────────────────────────
def test_secret_unset_returns_503(app, monkeypatch):
    monkeypatch.delenv("FALCON_PUBLISH_SECRET", raising=False)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post(URL, json={"access_token": TOKEN}, headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 503
    assert r.json()["detail"]["code"] == "PUBLISH_NOT_CONFIGURED"


def test_missing_header_returns_403(client):
    r = client.post(URL, json={"access_token": TOKEN})
    assert r.status_code == 403
    assert r.json()["detail"]["code"] == "FORBIDDEN"


def test_wrong_secret_returns_403(client):
    r = client.post(URL, json={"access_token": TOKEN},
                    headers={"X-Publish-Secret": "nope"})
    assert r.status_code == 403


# ── Validation ──────────────────────────────────────────────────────────────
def test_empty_token_returns_400(client):
    r = client.post(URL, json={"access_token": ""},
                    headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "EMPTY_TOKEN"


def test_whitespace_token_returns_400(client):
    r = client.post(URL, json={"access_token": "   "},
                    headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "EMPTY_TOKEN"


# ── Happy path: stored + then served by kite_auth ───────────────────────────
def test_happy_path_stores_and_is_served(client, tmp_kite_db):
    r = client.post(URL, json={"access_token": TOKEN},
                    headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["stored"] is True
    assert body["token_date"]  # non-empty stored date

    # The stored token must now flow through kite_auth's OWN read paths.
    assert kite_auth._load_token_from_db() == TOKEN
    assert kite_auth.get_access_token() == TOKEN

    # And exactly one row exists for today (the row get_access_token keys on).
    con = sqlite3.connect(tmp_kite_db)
    n = con.execute("SELECT COUNT(*) FROM kite_tokens").fetchone()[0]
    set_by = con.execute(
        "SELECT set_by FROM kite_tokens ORDER BY id DESC LIMIT 1").fetchone()[0]
    con.close()
    assert n == 1
    assert set_by == "cloud-sync"


def test_token_is_stripped(client, tmp_kite_db):
    r = client.post(URL, json={"access_token": f"  {TOKEN}  "},
                    headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 200
    assert kite_auth._load_token_from_db() == TOKEN


def test_custom_set_by_honored(client, tmp_kite_db):
    r = client.post(URL, json={"access_token": TOKEN, "set_by": "laptop-auth-worker"},
                    headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 200
    con = sqlite3.connect(tmp_kite_db)
    set_by = con.execute(
        "SELECT set_by FROM kite_tokens ORDER BY id DESC LIMIT 1").fetchone()[0]
    con.close()
    assert set_by == "laptop-auth-worker"


# ── Idempotency: re-post replaces today's row ───────────────────────────────
def test_idempotent_repost_replaces_row(client, tmp_kite_db):
    client.post(URL, json={"access_token": "first_token_value"},
                headers={"X-Publish-Secret": SECRET})
    r2 = client.post(URL, json={"access_token": "second_token_value"},
                     headers={"X-Publish-Secret": SECRET})
    assert r2.status_code == 200
    con = sqlite3.connect(tmp_kite_db)
    n = con.execute("SELECT COUNT(*) FROM kite_tokens").fetchone()[0]
    con.close()
    assert n == 1                                        # replaced, not appended
    assert kite_auth.get_access_token() == "second_token_value"   # latest wins
