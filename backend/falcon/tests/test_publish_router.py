"""Tests for the laptop → cloud publish-intelligence ingest endpoint.

NOTE: written but NOT executed in this environment (no Python here). Cases:
  - auth: secret unset  → 503 PUBLISH_NOT_CONFIGURED (fail closed)
  - auth: missing/wrong header → 403 FORBIDDEN
  - empty promoted → 400 REFUSING_EMPTY_PUBLISH (never wipe PROD)
  - unknown table → 400 TABLE_NOT_ALLOWED
  - unknown column → 400 UNKNOWN_COLUMN
  - happy path → 200, full-replace semantics
  - idempotent → re-POST yields identical state
  - atomicity → a failing INSERT rolls back; cloud DB unchanged

Strategy: build a minimal FastAPI app mounting only publish_router, backed by a
tmp SQLite DB seeded with the three allowlisted tables. The router uses
`falcon_conn()` (a contextmanager) from backend/falcon/db.py — we monkeypatch it
to open the tmp DB so we never touch the real PROD volume.
"""
from __future__ import annotations

import contextlib
import os
import sqlite3
import tempfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from falcon.routers import publish_router

SECRET = "test-publish-secret"
URL = "/api/falcon/publish/intelligence"


# ── Fixtures ────────────────────────────────────────────────────────────────
@pytest.fixture
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE falcon_pattern_candidates (
            pattern_id TEXT PRIMARY KEY,
            rule_json  TEXT,
            mined_year INTEGER
        );
        CREATE TABLE falcon_promoted_patterns (
            pattern_id TEXT PRIMARY KEY,
            hit_rate   REAL,
            n_fires    INTEGER
        );
        CREATE TABLE falcon_pattern_taxonomy (
            pattern_id   TEXT PRIMARY KEY,
            plain_english TEXT,
            regime       TEXT
        );
        CREATE TABLE falcon_signal_runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name      TEXT NOT NULL,
            started_at    TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at   TEXT,
            status        TEXT NOT NULL DEFAULT 'running',
            rows_affected INTEGER,
            notes         TEXT,
            error         TEXT
        );
        -- pre-seed PROD with stale rows so we can prove full-replace.
        INSERT INTO falcon_promoted_patterns VALUES ('OLD_P', 0.5, 3);
        INSERT INTO falcon_pattern_candidates VALUES ('OLD_P', '{}', 2020);
        """
    )
    con.commit()
    con.close()
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def app(tmp_db, monkeypatch):
    """Mount publish_router; redirect falcon_conn() at the tmp DB; set secret."""
    monkeypatch.setenv("FALCON_PUBLISH_SECRET", SECRET)

    @contextlib.contextmanager
    def _fake_conn():
        con = sqlite3.connect(tmp_db)
        con.row_factory = sqlite3.Row
        try:
            yield con
        finally:
            con.close()

    monkeypatch.setattr(publish_router, "falcon_conn", _fake_conn)

    a = FastAPI()
    a.include_router(publish_router.router, prefix="/api")
    return a


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=False)


def _bundle(**overrides):
    b = {
        "bundle_version": 1,
        "generated_at": "2026-06-14 19:30:00",
        "source": "research-laptop",
        "cutoff_year": 2022,
        "tables": {
            "falcon_pattern_candidates": [
                {"pattern_id": "NEW_P", "rule_json": "{}", "mined_year": 2025},
            ],
            "falcon_promoted_patterns": [
                {"pattern_id": "NEW_P", "hit_rate": 0.69, "n_fires": 11},
            ],
            "falcon_pattern_taxonomy": [
                {"pattern_id": "NEW_P", "plain_english": "Compression breakout",
                 "regime": "TRENDING"},
            ],
        },
    }
    b.update(overrides)
    return b


# ── Auth ────────────────────────────────────────────────────────────────────
def test_secret_unset_returns_503(app, monkeypatch):
    monkeypatch.delenv("FALCON_PUBLISH_SECRET", raising=False)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post(URL, json=_bundle(), headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 503
    assert r.json()["detail"]["code"] == "PUBLISH_NOT_CONFIGURED"


def test_missing_header_returns_403(client):
    r = client.post(URL, json=_bundle())
    assert r.status_code == 403
    assert r.json()["detail"]["code"] == "FORBIDDEN"


def test_wrong_secret_returns_403(client):
    r = client.post(URL, json=_bundle(), headers={"X-Publish-Secret": "nope"})
    assert r.status_code == 403


# ── Validation guards ───────────────────────────────────────────────────────
def test_empty_promoted_returns_400(client):
    b = _bundle()
    b["tables"]["falcon_promoted_patterns"] = []
    r = client.post(URL, json=b, headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "REFUSING_EMPTY_PUBLISH"


def test_unknown_table_returns_400(client):
    b = _bundle()
    b["tables"]["falcon_features"] = [{"x": 1}]
    r = client.post(URL, json=b, headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "TABLE_NOT_ALLOWED"


def test_unknown_column_returns_400(client):
    b = _bundle()
    b["tables"]["falcon_promoted_patterns"][0]["bogus_col"] = 1
    r = client.post(URL, json=b, headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "UNKNOWN_COLUMN"


def test_unsupported_version_returns_400(client):
    r = client.post(URL, json=_bundle(bundle_version=2),
                    headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "UNSUPPORTED_BUNDLE_VERSION"


# ── Happy path + idempotency ────────────────────────────────────────────────
def test_happy_path_full_replace(client, tmp_db):
    r = client.post(URL, json=_bundle(), headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "published"
    assert body["source"] == "research-laptop"
    assert body["tables"]["falcon_promoted_patterns"]["after"] == 1
    # stale OLD_P must be gone (full-replace), only NEW_P remains.
    con = sqlite3.connect(tmp_db)
    ids = [row[0] for row in con.execute(
        "SELECT pattern_id FROM falcon_promoted_patterns").fetchall()]
    con.close()
    assert ids == ["NEW_P"]


def test_audit_row_written(client, tmp_db):
    client.post(URL, json=_bundle(), headers={"X-Publish-Secret": SECRET})
    con = sqlite3.connect(tmp_db)
    row = con.execute(
        "SELECT job_name, status, notes FROM falcon_signal_runs "
        "WHERE job_name='publish_intelligence' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    con.close()
    assert row is not None
    assert row[1] == "success"
    assert "source=research-laptop" in row[2]


def test_idempotent_repost(client, tmp_db):
    r1 = client.post(URL, json=_bundle(), headers={"X-Publish-Secret": SECRET})
    r2 = client.post(URL, json=_bundle(), headers={"X-Publish-Secret": SECRET})
    assert r1.status_code == 200 and r2.status_code == 200
    con = sqlite3.connect(tmp_db)
    n = con.execute("SELECT COUNT(*) FROM falcon_promoted_patterns").fetchone()[0]
    con.close()
    assert n == 1  # re-POST → identical state (full-replace)


# ── Atomicity ───────────────────────────────────────────────────────────────
def test_atomic_rollback_on_bad_row(client, tmp_db):
    """A duplicate PK in the SAME bundle violates the PK constraint mid-INSERT.
    The whole transaction must roll back → cloud DB unchanged (still OLD_P)."""
    b = _bundle()
    b["tables"]["falcon_promoted_patterns"] = [
        {"pattern_id": "DUP", "hit_rate": 0.6, "n_fires": 5},
        {"pattern_id": "DUP", "hit_rate": 0.7, "n_fires": 6},  # PK collision
    ]
    r = client.post(URL, json=b, headers={"X-Publish-Secret": SECRET})
    assert r.status_code == 500
    assert r.json()["detail"]["code"] == "PUBLISH_FAILED"
    con = sqlite3.connect(tmp_db)
    ids = [row[0] for row in con.execute(
        "SELECT pattern_id FROM falcon_promoted_patterns").fetchall()]
    # original stale row survived → nothing committed.
    assert ids == ["OLD_P"]
    con.close()
