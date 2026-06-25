"""GET /api/falcon/egress-ip — self-service egress IP for the Kite allowlist.

Tests the detection helper (echo mocked, never a real network call) + the
operator-token gate. No real broker / Kite / orders.
"""
import contextlib
import io

import pytest

from falcon.trade.routers import trade_router as tr


@pytest.fixture(autouse=True)
def _reset_egress_cache():
    """Clear the in-process cache before each test so results don't bleed."""
    tr._EGRESS_CACHE.update({"ip": None, "as_of": None, "error": None, "_ts": None})
    yield
    tr._EGRESS_CACHE.update({"ip": None, "as_of": None, "error": None, "_ts": None})


def _fake_urlopen(body: str):
    @contextlib.contextmanager
    def _cm(url, timeout=None):  # noqa: ARG001
        yield io.BytesIO(body.encode())
    return _cm


# ── detection helper ─────────────────────────────────────────────────────────

def test_detect_returns_string_ip_on_success(monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen("203.0.113.7\n"))
    out = tr._detect_egress_ip()
    assert out["ip"] == "203.0.113.7"          # trimmed string
    assert out["error"] is None
    assert isinstance(out["as_of"], str) and out["as_of"]


def test_detect_caches_within_ttl(monkeypatch):
    calls = {"n": 0}

    @contextlib.contextmanager
    def _counting(url, timeout=None):  # noqa: ARG001
        calls["n"] += 1
        yield io.BytesIO(b"198.51.100.2")

    monkeypatch.setattr("urllib.request.urlopen", _counting)
    first = tr._detect_egress_ip()
    second = tr._detect_egress_ip()
    assert first["ip"] == second["ip"] == "198.51.100.2"
    assert calls["n"] == 1                       # 2nd call served from cache


def test_detect_never_crashes_when_all_echoes_fail(monkeypatch):
    @contextlib.contextmanager
    def _boom(url, timeout=None):  # noqa: ARG001
        raise OSError("network down")
        yield  # pragma: no cover

    monkeypatch.setattr("urllib.request.urlopen", _boom)
    out = tr._detect_egress_ip()
    assert out["ip"] is None
    assert out["error"]                           # non-empty error message
    assert isinstance(out["as_of"], str)


def test_detect_falls_through_to_second_echo(monkeypatch):
    seq = iter([OSError("first echo down"), "192.0.2.55"])

    @contextlib.contextmanager
    def _seq(url, timeout=None):  # noqa: ARG001
        item = next(seq)
        if isinstance(item, Exception):
            raise item
        yield io.BytesIO(item.encode())

    monkeypatch.setattr("urllib.request.urlopen", _seq)
    out = tr._detect_egress_ip()
    assert out["ip"] == "192.0.2.55"
    assert out["error"] is None


# ── operator-token gate ──────────────────────────────────────────────────────

def test_gate_rejects_missing_token():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as ei:
        tr._require_operator_token(x_operator_token=None)
    assert ei.value.status_code == 403


def test_gate_accepts_correct_token():
    # conftest sets FALCON_OPERATOR_TOKEN=test-operator-token
    tr._require_operator_token(x_operator_token="test-operator-token")  # no raise
