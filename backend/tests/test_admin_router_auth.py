"""AUTH regression — /api/falcon/admin/* + /falcon/preflight/{summary,smoke}.

2026-07-16 security fix. Before this, EVERY endpoint on falcon_admin_router was
reachable with NO credential at all, including:
  * POST /falcon/admin/rerun/{job_name} → unauthenticated background-job
    execution against a ~35 GB DB (trivial resource exhaustion), and
  * POST /falcon/preflight/smoke        → places + cancels REAL Kite orders.

These tests assert the gate FIRES (no/!bad token → refused) and does NOT
over-fire (valid token → passes auth; `GET /falcon/preflight` stays public
because PreflightBanner.tsx fetches it browser-direct without a token).

SAFETY: no test here may actually run a job or place an order. `rerun` is
exercised with an INVALID job name (400 = auth passed, nothing started) plus a
stubbed threading.Thread; `smoke` stubs `run_smoke`. A test that triggers real
work would defeat the purpose of the fix.
"""
import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from falcon.routers.admin_router import router

TOKEN = "test-operator-token-abc123"

# (method, path) for every endpoint that MUST require the operator token.
PROTECTED = [
    ("GET",  "/api/falcon/preflight/summary"),
    ("POST", "/api/falcon/preflight/smoke"),
    ("GET",  "/api/falcon/admin/status"),
    ("GET",  "/api/falcon/admin/runs"),
    ("GET",  "/api/falcon/admin/inbox"),
    ("POST", "/api/falcon/admin/inbox/mark-read"),
    ("POST", "/api/falcon/admin/rerun/daily_signals"),
]


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("FALCON_OPERATOR_TOKEN", TOKEN)
    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


def _call(client, method, path, **kw):
    return client.request(method, path, **kw)


# ── The gate fires ───────────────────────────────────────────────────────

@pytest.mark.parametrize("method,path", PROTECTED)
def test_no_token_is_refused(client, method, path):
    """No X-Operator-Token → 403, and the handler never runs."""
    r = _call(client, method, path, json=[] if method == "POST" else None)
    assert r.status_code == 403, f"{method} {path} returned {r.status_code}"


@pytest.mark.parametrize("method,path", PROTECTED)
def test_wrong_token_is_refused(client, method, path):
    r = _call(client, method, path, headers={"X-Operator-Token": "wrong"},
              json=[] if method == "POST" else None)
    assert r.status_code == 403, f"{method} {path} returned {r.status_code}"


@pytest.mark.parametrize("method,path", PROTECTED)
def test_unconfigured_server_secret_fails_closed(monkeypatch, method, path):
    """Env secret missing → 503 refuse-all, never open-by-default."""
    monkeypatch.delenv("FALCON_OPERATOR_TOKEN", raising=False)
    app = FastAPI()
    app.include_router(router, prefix="/api")
    c = TestClient(app, raise_server_exceptions=False)
    r = c.request(method, path, headers={"X-Operator-Token": TOKEN},
                  json=[] if method == "POST" else None)
    assert r.status_code == 503, f"{method} {path} returned {r.status_code}"


# ── The gate does not over-fire (valid token passes auth) ────────────────

@pytest.mark.parametrize("method,path", PROTECTED)
def test_valid_token_passes_auth(client, monkeypatch, method, path):
    """With the token, the request must get PAST auth.

    We assert only that it is not 401/403/503 — the handler may still 400/500
    (stubbed job name, DB shape), which is fine: it proves auth let it through.
    """
    # HARD SAFETY: this test must never do real work. Stub the two endpoints
    # with side effects — `smoke` places real Kite orders, and `rerun` spawns a
    # real job thread against the ~35 GB DB (it did, until this stub was added).
    import importlib
    import threading as _t

    import falcon.integration_smoke as smoke
    monkeypatch.setattr(smoke, "run_smoke", lambda: {"ok": True, "stubbed": True},
                        raising=False)
    monkeypatch.setattr(importlib, "import_module",
                        lambda name, *a, **k: _StubJob())
    monkeypatch.setattr(_t, "Thread", lambda *a, **k: _NoopThread())

    r = _call(client, method, path, headers={"X-Operator-Token": TOKEN},
              json=[] if method == "POST" else None)
    assert r.status_code not in (401, 403, 503), \
        f"{method} {path} blocked a valid token: {r.status_code} {r.text[:200]}"


def test_rerun_with_token_rejects_unknown_job_without_starting_anything(client, monkeypatch):
    """Auth passes → handler validates the job name → 400. Nothing started."""
    started = []
    import threading as _t
    monkeypatch.setattr(_t, "Thread",
                        lambda *a, **k: started.append(k) or _Dummy(), raising=False)
    r = client.post("/api/falcon/admin/rerun/not_a_real_job",
                    headers={"X-Operator-Token": TOKEN})
    assert r.status_code == 400
    assert started == [], "an unknown job must never spawn a thread"


def test_rerun_without_token_never_spawns_a_job(monkeypatch, client):
    """The core exploit: unauthenticated job execution. Must not reach importlib."""
    import importlib
    calls = []
    real_import = importlib.import_module
    monkeypatch.setattr(importlib, "import_module",
                        lambda name, *a, **k: calls.append(name) or real_import(name, *a, **k))
    r = client.post("/api/falcon/admin/rerun/daily_signals")
    assert r.status_code == 403
    assert not any(c.startswith("falcon.jobs.") for c in calls), \
        f"unauthenticated call imported a job module: {calls}"


class _Dummy:
    def start(self):
        raise AssertionError("no job thread may start in tests")


class _StubJob:
    """Stands in for a falcon.jobs.* module. Its run() must never be invoked."""
    @staticmethod
    def run():                          # pragma: no cover
        raise AssertionError("a real job must never run in tests")


class _NoopThread:
    """Swallows .start() so an authed rerun test spawns nothing."""
    def start(self):
        pass


# ── Deliberately public: PreflightBanner fetches this browser-direct ─────

def test_preflight_get_stays_public(client, monkeypatch):
    """GET /falcon/preflight must remain unauthenticated.

    components/PreflightBanner.tsx:44 fetches NEXT_PUBLIC_API_URL directly (NOT
    via /api/falcon-proxy), so it sends no token. Gating this would 403 the
    health banner on /falcon/admin, /falcon/premarket and /falcon/trade.
    If this test ever fails, move PreflightBanner onto the proxy FIRST.
    """
    import falcon.preflight as pf

    class _R:
        def to_dict(self):
            return {"ok": True}

    monkeypatch.setattr(pf, "run", lambda force=False: _R())
    r = client.get("/api/falcon/preflight")
    assert r.status_code == 200
