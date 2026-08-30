"""
Agent Platform · ADMIN current-day refresh endpoint tests (Kite/EOD fully MOCKED — no prod write).

Covers the auth gate (503 unset / 401 wrong / 202 ok), single-flight (409), and the status route
(job record / 404 unknown). run_eod is monkeypatched so no Kite fetch or prod-data write happens.

Runnable two ways:
    pytest backend/agents/chart/tests/test_admin_refresh.py
    python  backend/agents/chart/tests/test_admin_refresh.py
"""
from __future__ import annotations
import os
import sys
import threading
import time

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from fastapi import FastAPI                                    # noqa: E402
from fastapi.testclient import TestClient                     # noqa: E402

from agents import router as R                                # noqa: E402
import agents.chart.eod as eodmod                             # noqa: E402

HDR = {"X-Agent-Admin-Token": "s3cr3t"}


def _client():
    app = FastAPI()
    app.include_router(R.router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


def _set_token(val):
    """Set/unset AGENT_ADMIN_TOKEN; return the previous value for restore."""
    prev = os.environ.get("AGENT_ADMIN_TOKEN")
    if val is None:
        os.environ.pop("AGENT_ADMIN_TOKEN", None)
    else:
        os.environ["AGENT_ADMIN_TOKEN"] = val
    return prev


def _restore_token(prev):
    if prev is None:
        os.environ.pop("AGENT_ADMIN_TOKEN", None)
    else:
        os.environ["AGENT_ADMIN_TOKEN"] = prev


def _fast_run_eod(date=None, fetch=True):
    return {"count": 3, "scanned": 10, "by_pattern": {"horizontal_trendline": 3},
            "stored": "mem://screen", "fetched": False}


# --------------------------------------------------------------------------- auth gate
def test_503_when_token_unset():
    prev = _set_token(None)
    R._REFRESH_JOBS.clear()
    try:
        r = _client().post("/api/agents/chart/refresh")
        assert r.status_code == 503, r.status_code
        return "unset AGENT_ADMIN_TOKEN -> 503 (fail-closed)"
    finally:
        _restore_token(prev)


def test_401_missing_or_wrong_header():
    prev = _set_token("s3cr3t")
    R._REFRESH_JOBS.clear()
    try:
        c = _client()
        assert c.post("/api/agents/chart/refresh").status_code == 401, "missing header"
        assert c.post("/api/agents/chart/refresh",
                      headers={"X-Agent-Admin-Token": "nope"}).status_code == 401, "wrong header"
        return "missing/wrong header -> 401"
    finally:
        _restore_token(prev)


def test_202_starts_job_and_status_reports_it():
    prev = _set_token("s3cr3t")
    R._REFRESH_JOBS.clear()
    saved = eodmod.run_eod
    eodmod.run_eod = _fast_run_eod
    try:
        c = _client()
        r = c.post("/api/agents/chart/refresh?date=2026-07-31", headers=HDR)
        assert r.status_code == 202, r.status_code
        body = r.json()
        assert body["ok"] and body["status"] == "running" and body["job_id"], body
        jid = body["job_id"]
        # daemon thread finishes quickly (mocked run_eod); poll status route
        deadline = time.time() + 3
        st = None
        while time.time() < deadline:
            st = c.get(f"/api/agents/chart/refresh/status?job_id={jid}", headers=HDR)
            assert st.status_code == 200, st.status_code
            if st.json().get("status") == "done":
                break
            time.sleep(0.05)
        sj = st.json()
        assert sj["status"] == "done", sj
        assert sj["result"]["count"] == 3 and sj["result"]["scanned"] == 10, sj
        return f"202 started job {jid[:8]}; status route -> done with result"
    finally:
        eodmod.run_eod = saved
        _restore_token(prev)


def test_single_flight_returns_409():
    prev = _set_token("s3cr3t")
    R._REFRESH_JOBS.clear()
    saved = eodmod.run_eod
    gate = threading.Event()

    def _blocking(date=None, fetch=True):
        gate.wait(5)
        return _fast_run_eod(date, fetch)

    eodmod.run_eod = _blocking
    try:
        c = _client()
        r1 = c.post("/api/agents/chart/refresh", headers=HDR)
        assert r1.status_code == 202, r1.status_code
        jid = r1.json()["job_id"]
        # job is still running (blocked on gate) -> second call must be refused
        r2 = c.post("/api/agents/chart/refresh", headers=HDR)
        assert r2.status_code == 409, r2.status_code
        assert r2.json()["status"] == "busy" and r2.json()["job_id"] == jid, r2.json()
        return "single-flight: second concurrent refresh -> 409 busy"
    finally:
        gate.set()
        eodmod.run_eod = saved
        time.sleep(0.1)
        _restore_token(prev)


def test_status_404_unknown_job():
    prev = _set_token("s3cr3t")
    R._REFRESH_JOBS.clear()
    try:
        c = _client()
        st = c.get("/api/agents/chart/refresh/status?job_id=deadbeef", headers=HDR)
        assert st.status_code == 404, st.status_code
        return "unknown job_id -> 404"
    finally:
        _restore_token(prev)


if __name__ == "__main__":
    tests = [test_503_when_token_unset,
             test_401_missing_or_wrong_header,
             test_202_starts_job_and_status_reports_it,
             test_single_flight_returns_409,
             test_status_404_unknown_job]
    results = []
    for fn in tests:
        try:
            results.append((fn.__name__, "PASS", fn()))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e)))
    print("\n=== admin refresh endpoint tests ===")
    for name, status, detail in results:
        print(f"  [{status}] {name}" + (f"  -> {detail}" if detail else ""))
    if any(s == "FAIL" for _, s, _ in results):
        sys.exit(1)
