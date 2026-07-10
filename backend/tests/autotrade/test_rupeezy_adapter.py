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
from autotrade.broker import rupeezy as rupeezy_mod
from autotrade.broker.rupeezy import (
    RupeezyBroker, _normalise_status, _extract_margin)
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


# ── JWT exp decoding (real, short-lived Vortex token) ─────────────────────────

def _make_jwt(exp_ts):
    """A minimal (unsigned) JWT carrying an `exp` claim — enough for the
    read-only exp decode (no signature verification)."""
    import base64
    import json as _json

    def b64(o):
        return base64.urlsafe_b64encode(
            _json.dumps(o).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'RS512', 'typ': 'JWT'})}.{b64({'exp': int(exp_ts)})}.sig"


def _epoch(dt):
    return int(dt.timestamp())


def test_exchange_captures_jwt_exp(fake_requests):
    """A real Vortex JWT carries a short `exp` — exchange must surface it as
    TokenSet.expires_at (not None) so the card shows a true expiry."""
    from datetime import datetime, timedelta
    from autotrade.broker.auth_base import IST
    exp_dt = datetime.now(IST) + timedelta(hours=2)
    jwt = _make_jwt(_epoch(exp_dt))
    fake_requests.set_next(_FakeResp(200, {"data": {"access_token": jwt}}))
    ts = RupeezyAuth().exchange(_creds(), request_token="rt")
    assert ts.expires_at is not None
    assert _epoch(ts.expires_at) == _epoch(exp_dt)


def test_expiry_decodes_jwt():
    from datetime import datetime, timedelta
    from autotrade.broker.auth_base import IST, TokenSet
    exp_dt = datetime.now(IST) + timedelta(hours=1)
    ts = TokenSet(access_token=_make_jwt(_epoch(exp_dt)))
    got = RupeezyAuth().expiry(ts)
    assert got is not None and _epoch(got) == _epoch(exp_dt)


def test_validate_expired_jwt_short_circuits_no_http(fake_requests):
    """An already-expired JWT is EXPIRED locally — NO funds round-trip."""
    from datetime import datetime, timedelta
    from autotrade.broker.auth_base import IST
    past = _make_jwt(_epoch(datetime.now(IST) - timedelta(minutes=5)))
    h = RupeezyAuth().validate(_creds(access_token=past))
    assert h.ok is False and h.status == "EXPIRED"
    assert fake_requests.calls == []                 # no network call


def test_validate_live_jwt_hits_network(fake_requests):
    """A still-valid JWT falls through to the live funds ping (200 → ACTIVE)."""
    from datetime import datetime, timedelta
    from autotrade.broker.auth_base import IST
    future = _make_jwt(_epoch(datetime.now(IST) + timedelta(hours=2)))
    fake_requests.set_next(_FakeResp(200, {"data": {}}))
    h = RupeezyAuth().validate(_creds(access_token=future))
    assert h.ok is True and h.status == "ACTIVE"
    assert any("/user/funds" in c["url"] for c in fake_requests.calls)


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


# ── MIS/MTF margin-per-share (broker leverage sizing) ─────────────────────────

@pytest.fixture(autouse=True)
def _clear_margin_cache():
    """The per-share margin cache is module-level — clear it between tests so a
    cached value from one test never leaks into another."""
    rupeezy_mod._margin_cache.clear()
    yield
    rupeezy_mod._margin_cache.clear()


def _quote(ltp):
    return _FakeResp(200, {"data": {"ltp": ltp}})


def test_margin_per_share_parses_required_margin(fake_requests, master_file,
                                                 monkeypatch):
    """MIS margin probe: qty=1 order-margin → per-share margin, and the probe body
    carries INTRADAY product + quantity=1 at the live LTP."""
    fake_requests.on("/data/quote", _quote(1000.0))
    fake_requests.on("/trading/margins",
                     _FakeResp(200, {"data": {"required_margin": 200.0}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    mps = b.get_margin_per_share("INFY", "MIS")
    assert mps == 200.0                      # 5x intraday leverage on ₹1000
    body = [c for c in fake_requests.calls
            if "/trading/margins" in c["url"]][-1]["json"]
    assert body["product"] == "INTRADAY"     # MIS → INTRADAY
    assert body["quantity"] == 1
    assert body["price"] == 1000.0
    assert body["token"] == 408065           # from master


def test_margin_mtf_product_mapped(fake_requests, master_file, monkeypatch):
    fake_requests.on("/data/quote", _quote(500.0))
    fake_requests.on("/trading/margins",
                     _FakeResp(200, {"data": {"total_margin": 175.0}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_margin_per_share("INFY", "MTF") == 175.0
    body = [c for c in fake_requests.calls
            if "/trading/margins" in c["url"]][-1]["json"]
    assert body["product"] == "MTF"


def test_margin_cnc_returns_none_no_probe(fake_requests, master_file):
    """Cash product → None (1x, size on LTP) and NO margin HTTP call."""
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_margin_per_share("INFY", "CNC") is None
    assert not any("/trading/margins" in c["url"] for c in fake_requests.calls)


def test_margin_rejects_value_above_ltp(fake_requests, master_file):
    """A margin > full cash price implies leverage < 1x → mis-parse; refuse it
    (cash fallback) rather than mis-size."""
    fake_requests.on("/data/quote", _quote(1000.0))
    fake_requests.on("/trading/margins",
                     _FakeResp(200, {"data": {"margin": 5000.0}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_margin_per_share("INFY", "MIS") is None


def test_margin_endpoint_error_falls_back_none(fake_requests, master_file):
    fake_requests.on("/data/quote", _quote(1000.0))
    fake_requests.on("/trading/margins", _FakeResp(500, {}, text="boom"))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_margin_per_share("INFY", "MIS") is None


def test_margin_no_ltp_returns_none(fake_requests, master_file):
    """No LTP (quote fails) → None without ever probing margin."""
    fake_requests.on("/data/quote", _FakeResp(500, {}, text="no quote"))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_margin_per_share("INFY", "MIS") is None
    assert not any("/trading/margins" in c["url"] for c in fake_requests.calls)


def test_margins_batch_aggregates(fake_requests, master_file):
    fake_requests.on("/data/quote", _quote(1000.0))     # both symbols priced same
    fake_requests.on("/trading/margins",
                     _FakeResp(200, {"data": {"required_margin": 250.0}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    out = b.get_margins_batch(["INFY", "TCS"], "MIS")
    assert out == {"INFY": 250.0, "TCS": 250.0}


def test_margins_batch_cash_product_empty(fake_requests, master_file):
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_margins_batch(["INFY"], "CNC") == {}


def test_margin_cache_hit_skips_second_probe(fake_requests, master_file):
    fake_requests.on("/data/quote", _quote(1000.0))
    fake_requests.on("/trading/margins",
                     _FakeResp(200, {"data": {"required_margin": 200.0}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_margin_per_share("INFY", "MIS") == 200.0
    n_probe = sum(1 for c in fake_requests.calls if "/trading/margins" in c["url"])
    assert b.get_margin_per_share("INFY", "MIS") == 200.0   # served from cache
    n_probe2 = sum(1 for c in fake_requests.calls if "/trading/margins" in c["url"])
    assert n_probe2 == n_probe                              # no extra probe


def test_margin_path_env_override(fake_requests, master_file, monkeypatch):
    monkeypatch.setenv("RUPEEZY_MARGIN_PATH", "/v2/margins/order")
    fake_requests.on("/data/quote", _quote(1000.0))
    fake_requests.on("/v2/margins/order",
                     _FakeResp(200, {"data": {"required_margin": 300.0}}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_margin_per_share("INFY", "MIS") == 300.0
    assert any("/v2/margins/order" in c["url"] for c in fake_requests.calls)


def test_extract_margin_field_variants():
    assert _extract_margin({"data": {"required_margin": 10.0}}) == 10.0
    assert _extract_margin({"total_margin": 12.5}) == 12.5
    assert _extract_margin([{"margin": 7.0}]) == 7.0
    assert _extract_margin({"data": {"margin": {"total": 9.0}}}) == 9.0
    assert _extract_margin({"data": {}}) is None
    assert _extract_margin({"data": {"required_margin": 0}}) is None
    assert _extract_margin("nonsense") is None


# ── PRE-EXIT GUARD: get_net_position_qty + B1 re-raise fail-safe (2026-07-10) ──

def test_get_net_position_qty_paper_returns_none(fake_requests, master_file):
    """Paper / not-live → None WITHOUT any HTTP call (byte-for-byte unchanged)."""
    b = RupeezyBroker(_live_profile(), dry_run=True)
    assert b.get_net_position_qty("INFY") is None
    assert fake_requests.calls == []


def test_get_net_position_qty_live_returns_signed(fake_requests, master_file,
                                                  monkeypatch):
    """A live book with our symbol → its SIGNED net qty (short = negative)."""
    _enable_live(monkeypatch)
    fake_requests.on("/trading/portfolio/positions",
                     _FakeResp(200, {"data": [
                         {"tradingsymbol": "INFY", "quantity": -50},
                         {"tradingsymbol": "TCS", "quantity": 10}]}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_net_position_qty("INFY") == -50


def test_get_net_position_qty_live_flat_returns_zero(fake_requests, master_file,
                                                     monkeypatch):
    """Book retrieved OK but our symbol absent → 0 (genuinely flat)."""
    _enable_live(monkeypatch)
    fake_requests.on("/trading/portfolio/positions",
                     _FakeResp(200, {"data": [{"tradingsymbol": "TCS",
                                               "quantity": 10}]}))
    b = RupeezyBroker(_live_profile(), dry_run=False)
    assert b.get_net_position_qty("INFY") == 0


def test_get_net_position_qty_reraises_on_transport_error(fake_requests,
                                                          master_file, monkeypatch):
    """B1 FAIL-SAFE: a genuine transport error must RE-RAISE, never be swallowed
    to None — None is the paper sentinel and would make the caller place a BLIND
    exit (the BRIGADE double-cover naked-position bug). Mutation check: if the
    method swallowed the error to None this asserts-raises test would fail."""
    _enable_live(monkeypatch)
    monkeypatch.setattr(rupeezy_mod.time, "sleep", lambda *a, **k: None)

    def _boom(*a, **k):
        raise ConnectionResetError("An existing connection was forcibly closed (10054)")
    monkeypatch.setattr(fake_requests, "request", _boom)
    b = RupeezyBroker(_live_profile(), dry_run=False)
    with pytest.raises(ConnectionResetError):
        b.get_net_position_qty("INFY")
