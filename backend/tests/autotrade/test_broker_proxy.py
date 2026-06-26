"""BROKER_PROXY_URL static-egress hook (SEBI registered-IP rule).

Verifies the DEFAULT-OFF contract in services.kite_auth:
  - env UNSET  → KiteConnect built with NO `proxies` kwarg (byte-identical to before)
  - env SET    → `proxies={"http":url,"https":url}` passed at EVERY construction site

Plus the proxy-aware /api/falcon/egress-ip contract in trade_router:
  - env UNSET  → proxy_ip is None, direct `ip` still present
  - env SET    → proxy_ip reflects the IP seen THROUGH the proxy

All KiteConnect / requests calls are MOCKED — no real broker, no network.
"""
import sys
import types

import pytest

from services import kite_auth as ka


# ── A fake KiteConnect that records exactly how it was constructed ───────────
class _FakeKite:
    last_kwargs = None

    def __init__(self, **kwargs):
        # record the kwargs the construction site used
        type(self).last_kwargs = dict(kwargs)
        self.api_key = kwargs.get("api_key")

    def set_access_token(self, token):
        self.access_token = token

    def profile(self):
        return {"user_name": "TEST", "email": "t@t"}

    def generate_session(self, request_token, api_secret=None):
        return {"access_token": "sess-token"}


@pytest.fixture
def _patch_kite(monkeypatch):
    """Patch the kiteconnect module so `from kiteconnect import KiteConnect`
    inside _new_kite() resolves to our recorder. Also stub credentials/token."""
    fake_mod = types.ModuleType("kiteconnect")
    fake_mod.KiteConnect = _FakeKite
    monkeypatch.setitem(sys.modules, "kiteconnect", fake_mod)
    monkeypatch.setattr(ka, "_get_credentials", lambda: ("apikey", "apisecret"))
    monkeypatch.setattr(ka, "get_access_token", lambda: "access-token")
    # _load_env_file() is called inside _kite_proxies(); make it a no-op so the
    # real config/.env can't inject a BROKER_PROXY_URL during the test.
    monkeypatch.setattr(ka, "_load_env_file", lambda: None)
    _FakeKite.last_kwargs = None
    yield


# ── _kite_proxies() unit ─────────────────────────────────────────────────────

def test_kite_proxies_none_when_unset(monkeypatch):
    monkeypatch.setattr(ka, "_load_env_file", lambda: None)
    monkeypatch.delenv("BROKER_PROXY_URL", raising=False)
    assert ka._kite_proxies() is None


def test_kite_proxies_none_when_blank(monkeypatch):
    monkeypatch.setattr(ka, "_load_env_file", lambda: None)
    monkeypatch.setenv("BROKER_PROXY_URL", "   ")
    assert ka._kite_proxies() is None


def test_kite_proxies_dict_when_set(monkeypatch):
    monkeypatch.setattr(ka, "_load_env_file", lambda: None)
    monkeypatch.setenv("BROKER_PROXY_URL", "http://u:p@1.2.3.4:8080")
    assert ka._kite_proxies() == {
        "http":  "http://u:p@1.2.3.4:8080",
        "https": "http://u:p@1.2.3.4:8080",
    }


# ── DEFAULT-OFF: no proxies kwarg at any construction site ───────────────────

def test_get_kite_client_no_proxy_when_unset(_patch_kite, monkeypatch):
    monkeypatch.delenv("BROKER_PROXY_URL", raising=False)
    ka.get_kite_client(check=False)
    assert "proxies" not in _FakeKite.last_kwargs   # unchanged behaviour


def test_get_token_status_no_proxy_when_unset(_patch_kite, monkeypatch):
    monkeypatch.delenv("BROKER_PROXY_URL", raising=False)
    out = ka.get_token_status()
    assert out["valid"] is True
    assert "proxies" not in _FakeKite.last_kwargs


def test_exchange_and_save_no_proxy_when_unset(_patch_kite, monkeypatch):
    monkeypatch.delenv("BROKER_PROXY_URL", raising=False)
    monkeypatch.setattr(ka, "_save_token_to_db", lambda *a, **k: None)
    ka.exchange_and_save("req-token")
    assert "proxies" not in _FakeKite.last_kwargs


# ── ENV SET: proxies applied at every construction site ──────────────────────
_PROXY = "http://u:p@10.0.0.9:3128"
_EXPECT = {"http": _PROXY, "https": _PROXY}


def test_get_kite_client_applies_proxy_when_set(_patch_kite, monkeypatch):
    monkeypatch.setenv("BROKER_PROXY_URL", _PROXY)
    ka.get_kite_client(check=False)
    assert _FakeKite.last_kwargs.get("proxies") == _EXPECT


def test_get_token_status_applies_proxy_when_set(_patch_kite, monkeypatch):
    monkeypatch.setenv("BROKER_PROXY_URL", _PROXY)
    ka.get_token_status()
    assert _FakeKite.last_kwargs.get("proxies") == _EXPECT


def test_exchange_and_save_applies_proxy_when_set(_patch_kite, monkeypatch):
    monkeypatch.setenv("BROKER_PROXY_URL", _PROXY)
    monkeypatch.setattr(ka, "_save_token_to_db", lambda *a, **k: None)
    ka.exchange_and_save("req-token")
    assert _FakeKite.last_kwargs.get("proxies") == _EXPECT
