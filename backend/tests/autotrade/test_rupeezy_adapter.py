"""Rupeezy (Vortex API) adapter tests — Stage 2.

ALL HTTP is mocked (monkeypatch on `requests`). NO real network, NO real
credentials, NO real orders. Covers:

  AUTH (RupeezyAuth):
    * capabilities matrix
    * checksum = SHA-256(application_id + auth + x-api-key)
    * login_url shape (applicationId + state)
    * exchange parses data.access_token
    * validate maps 200→ACTIVE, 401→EXPIRED, 403→REVOKED, 5xx→ERROR
    * refresh raises RefreshNotSupported; expiry is None

  EXECUTION (RupeezyBroker):
    * registry / router wiring
    * dry-run makes ZERO HTTP calls (place / exit / cancel / gtt)
    * order mapping: MARKET→RL-MKT (exact body), LIMIT→RL+price, MTF→MTF product
    * place_market_exit → SELL RL-MKT
    * instrument master resolves token; absent master → clear error → FAILED result
    * get_order_status normalises Vortex status → exit_poller vocabulary
"""
import asyncio
import hashlib
import json
from types import SimpleNamespace

import pytest

from autotrade.broker import registry, router
from autotrade.broker.auth_base import RefreshNotSupported
from autotrade.broker.rupeezy import RupeezyBroker, _normalise_status
from autotrade.broker.rupeezy_auth_provider import RupeezyAuth


# ── fake requests plumbing ────────────────────────────────────────────────────

class _FakeResp:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _RequestsRecorder:
    """Stands in for the `requests` module. Records every call and returns a
    canned response (default 200 with an empty {})."""
    def __init__(self):
        self.calls = []
        self._next = None
        self._map = {}  # (method, path-substr) -> _FakeResp

    def set_next(self, resp):
        self._next = resp

    def on(self, substr, resp):
        self._map[substr] = resp

    def _resolve(self, method, url):
        for substr, resp in self._map.items():
            if substr in url:
                return resp
        if self._next is not None:
            return self._next
        return _FakeResp(200, {"data": {}})

    def request(self, method, url, headers=None, json=None, params=None,
                timeout=None, proxies=None):
        self.calls.append({"method": method, "url": url, "headers": headers,
                           "json": json, "params": params})
        return self._resolve(method, url)

    def post(self, url, headers=None, json=None, timeout=None, proxies=None):
        self.calls.append({"method": "POST", "url": url, "headers": headers,
                           "json": json})
        return self._resolve("POST", url)

    def get(self, url, headers=None, timeout=None, proxies=None, params=None):
        self.calls.append({"method": "GET", "url": url, "headers": headers,
                           "params": params})
        return self._resolve("GET", url)


@pytest.fixture
def fake_requests(monkeypatch):
    rec = _RequestsRecorder()
    import sys
    monkeypatch.setitem(sys.modules, "requests", rec)
    return rec


def _run(coro):
    return asyncio.run(coro)


def _creds(**kw):
    base = dict(api_key="app-123", api_secret="xkey-secret",
                access_token="tok-abc")
    base.update(kw)
    return SimpleNamespace(**base)


def _live_profile(**kw):
    """A profile that would be live (access_token + api_secret present)."""
    base = dict(api_key="app-123", api_secret="xkey-secret",
                access_token="tok-abc", broker_account_id="acc-1",
                broker_name="rupeezy")
    base.update(kw)
    return SimpleNamespace(**base)


@pytest.fixture
def master_file(tmp_path, monkeypatch):
    """A local instrument-master JSON so token resolution succeeds in tests."""
    p = tmp_path / "rupeezy_instruments.json"
    p.write_text(json.dumps({"NSE_EQ": {"INFY": 408065, "TCS": 2953217}}),
                 encoding="utf-8")
    monkeypatch.setenv("RUPEEZY_INSTRUMENT_MASTER", str(p))
    return p


# ── AUTH ──────────────────────────────────────────────────────────────────────

def test_capabilities():
    c = RupeezyAuth.capabilities
    assert c.auth_kind == "oauth2_flow"
    assert c.has_refresh_token is False
    assert c.token_lifetime == "session"
    assert c.supports_gtt and c.supports_mtf and c.fno


def test_checksum_matches_sha256_spec():
    application_id, auth, x_api_key = "app-123", "req-tok-xyz", "xkey-secret"
    expected = hashlib.sha256(
        f"{application_id}{auth}{x_api_key}".encode("utf-8")).hexdigest()
    got = RupeezyAuth.compute_checksum(application_id, auth, x_api_key)
    assert got == expected


def test_login_url_shape():
    url = RupeezyAuth().login_url(_creds(), redirect_uri="https://cb", state="s99")
    assert url.startswith("https://flow.rupeezy.in?applicationId=app-123")
    assert "state=s99" in url


def test_login_url_requires_application_id():
    with pytest.raises(ValueError):
        RupeezyAuth().login_url(_creds(api_key=""), redirect_uri="", state="")


def test_exchange_parses_access_token(fake_requests):
    fake_requests.set_next(_FakeResp(200, {"data": {"access_token": "JWT-999"}}))
    ts = RupeezyAuth().exchange(_creds(), request_token="req-tok-xyz")
    assert ts.access_token == "JWT-999"
    assert ts.refresh_token is None
    assert ts.expires_at is None
    # The POST body carried the spec-correct checksum + fields.
    call = [c for c in fake_requests.calls if c["method"] == "POST"][-1]
    assert call["url"].endswith("/user/session")
    body = call["json"]
    assert body["applicationId"] == "app-123"
    assert body["token"] == "req-tok-xyz"
    assert body["checksum"] == RupeezyAuth.compute_checksum(
        "app-123", "req-tok-xyz", "xkey-secret")


def test_exchange_missing_token_raises(fake_requests):
    fake_requests.set_next(_FakeResp(200, {"data": {}}))
    with pytest.raises(ValueError):
        RupeezyAuth().exchange(_creds(), request_token="rt")


def test_validate_200_active(fake_requests):
    fake_requests.set_next(_FakeResp(200, {"data": {}}))
    h = RupeezyAuth().validate(_creds())
    assert h.ok is True and h.status == "ACTIVE"


def test_validate_401_expired(fake_requests):
    fake_requests.set_next(_FakeResp(401, {}, text="token expired"))
    h = RupeezyAuth().validate(_creds())
    assert h.ok is False and h.status == "EXPIRED"


def test_validate_403_revoked(fake_requests):
    fake_requests.set_next(_FakeResp(403, {}, text="forbidden"))
    h = RupeezyAuth().validate(_creds())
    assert h.ok is False and h.status == "REVOKED"


def test_validate_5xx_error(fake_requests):
    fake_requests.set_next(_FakeResp(503, {}, text="bad gateway"))
    h = RupeezyAuth().validate(_creds())
    assert h.ok is False and h.status == "ERROR"


def test_validate_no_token_expired():
    h = RupeezyAuth().validate(_creds(access_token=""))
    assert h.ok is False and h.status == "EXPIRED"


def test_refresh_not_supported():
    with pytest.raises(RefreshNotSupported):
        RupeezyAuth().refresh(_creds())


def test_expiry_none():
    from autotrade.broker.auth_base import TokenSet
    assert RupeezyAuth().expiry(TokenSet(access_token="a")) is None


# ── registry / router wiring ──────────────────────────────────────────────────

def test_registry_rupeezy_live():
    assert registry.is_live("rupeezy") is True
    assert registry.get_client_cls("rupeezy") is RupeezyBroker
    assert isinstance(registry.get_auth("rupeezy"), RupeezyAuth)


def test_router_builds_rupeezy():
    client = router.build_client(_live_profile(), dry_run=True)
    assert isinstance(client, RupeezyBroker)


def test_vault_valid_brokers_includes_rupeezy():
    from autotrade import vault
    assert "rupeezy" in vault.VALID_BROKERS


def test_list_supported_includes_rupeezy():
    rows = {r["broker"]: r for r in registry.list_supported()}
    assert "rupeezy" in rows
    assert rows["rupeezy"]["live"] is True
    assert rows["rupeezy"]["capabilities"]["auth_kind"] == "oauth2_flow"


# ── EXECUTION: dry-run makes ZERO HTTP ────────────────────────────────────────

def test_dry_run_place_order_no_http(fake_requests, master_file):
    b = RupeezyBroker(_live_profile(), dry_run=True)
    order = SimpleNamespace(symbol="INFY", qty=10, order_type="MARKET",
                            product="CNC", exchange="NSE", price=None)
    res = _run(b.place_order(order))
    assert res.status == "DRY_RUN"
    assert fake_requests.calls == []


def test_dry_run_exit_and_cancel_no_http(fake_requests, master_file):
    b = RupeezyBroker(_live_profile(), dry_run=True)
    res = _run(b.place_market_exit("INFY", 5, "MTF"))
    assert res.status == "DRY_RUN"
    c = _run(b.cancel_order("OID-1"))
    assert c["status"] == "DRY_RUN"
    assert b.cancel_order_sync("OID-1") is True
    gid = b.place_gtt_oco("INFY", 5, 90.0, 110.0, 100.0, product="CNC")
    assert gid is None
    assert fake_requests.calls == []


# ── EXECUTION: order mapping (live path) ──────────────────────────────────────

def _enable_live(monkeypatch):
    monkeypatch.setenv("FALCON_AUTOTRADE_ENABLED", "true")


def test_place_market_order_maps_to_rl_mkt(fake_requests, master_file, monkeypatch):
    _enable_live(monkeypatch)
    fake_requests.on("/trading/orders/regular",
                     _FakeResp(200, {"data": {"order_id": "OID-777"}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    order = SimpleNamespace(symbol="INFY", qty=10, order_type="MARKET",
                            product="CNC", exchange="NSE", price=None)
    res = _run(b.place_order(order))
    assert res.status == "PLACED" and res.broker_order_id == "OID-777"
    body = [c for c in fake_requests.calls
            if "/trading/orders/regular" in c["url"]][-1]["json"]
    assert body["variety"] == "RL-MKT"
    assert body["transaction_type"] == "BUY"
    assert body["product"] == "DELIVERY"     # CNC → DELIVERY
    assert body["exchange"] == "NSE_EQ"
    assert body["token"] == 408065           # from master
    assert body["quantity"] == 10
    assert body["validity"] == "DAY"
    assert "price" not in body               # MARKET carries no price


def test_place_limit_order_maps_to_rl_with_price(fake_requests, master_file, monkeypatch):
    _enable_live(monkeypatch)
    fake_requests.on("/trading/orders/regular",
                     _FakeResp(200, {"data": {"order_id": "OID-1"}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    order = SimpleNamespace(symbol="INFY", qty=3, order_type="LIMIT",
                            product="MTF", exchange="NSE", price=1234.5)
    res = _run(b.place_order(order))
    assert res.status == "PLACED"
    body = [c for c in fake_requests.calls
            if "/trading/orders/regular" in c["url"]][-1]["json"]
    assert body["variety"] == "RL"
    assert body["product"] == "MTF"          # MTF → MTF
    assert body["price"] == 1234.5


def test_market_exit_sell_rl_mkt(fake_requests, master_file, monkeypatch):
    _enable_live(monkeypatch)
    fake_requests.on("/trading/orders/regular",
                     _FakeResp(200, {"data": {"order_id": "EX-9"}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    res = _run(b.place_market_exit("INFY", 7, "EQ", kite_product="MTF"))
    assert res.status == "PLACED" and res.broker_order_id == "EX-9"
    body = [c for c in fake_requests.calls
            if "/trading/orders/regular" in c["url"]][-1]["json"]
    assert body["variety"] == "RL-MKT"
    assert body["transaction_type"] == "SELL"
    assert body["product"] == "MTF"          # kite_product override wins
    assert body["quantity"] == 7


def test_order_fails_without_master(fake_requests, monkeypatch):
    """No instrument master configured → clear failure, never a fabricated token,
    and NO order HTTP call is made."""
    _enable_live(monkeypatch)
    monkeypatch.delenv("RUPEEZY_INSTRUMENT_MASTER", raising=False)
    b = RupeezyBroker(_live_profile(), dry_run=False)
    # Force an empty master (avoid picking up any bundled cache file).
    b._master = {}
    order = SimpleNamespace(symbol="NOSUCH", qty=1, order_type="MARKET",
                            product="CNC", exchange="NSE", price=None)
    res = _run(b.place_order(order))
    assert res.status == "FAILED"
    assert "instrument master not configured" in (res.error or "")
    assert not any("/trading/orders/regular" in c["url"]
                   for c in fake_requests.calls)


# ── EXECUTION: order-status normalisation ─────────────────────────────────────

def test_get_order_status_normalises(fake_requests, monkeypatch):
    _enable_live(monkeypatch)
    fake_requests.on("/trading/orders/OID-1", _FakeResp(200, {"data": {
        "order_status": "EXECUTED", "filled_quantity": 10,
        "average_price": 101.25}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    st = b.get_order_status("OID-1")
    assert st["status"] == "COMPLETE"        # EXECUTED → COMPLETE
    assert st["filled_quantity"] == 10
    assert st["average_price"] == 101.25


def test_status_map():
    assert _normalise_status("EXECUTED") == "COMPLETE"
    assert _normalise_status("FILLED") == "COMPLETE"
    assert _normalise_status("REJECTED") == "REJECTED"
    assert _normalise_status("CANCELED") == "CANCELLED"
    assert _normalise_status("OPEN") == "OPEN"      # unknown passes through


def test_dry_run_order_status_synthetic():
    b = RupeezyBroker(_live_profile(), dry_run=True)
    st = b.get_order_status("anything")
    assert st["status"] == "COMPLETE"
