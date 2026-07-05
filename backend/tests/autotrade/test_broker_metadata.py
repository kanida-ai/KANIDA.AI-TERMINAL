"""BROKER ONBOARDING METADATA tests.

Verifies `registry.list_supported()` (served by GET /autotrade/brokers/supported)
returns the full BrokerMeta contract the guided "add a broker" UI is built
against: brand chip + display_name + exchanges + ordered credential fields +
setup steps, on top of the existing broker/live/capabilities.

Contract (authoritative):
  BrokerMeta = {broker, display_name, brand:{color,initial}, live, capabilities,
                exchanges:[...], fields:[FieldDef...], setup:{docs_url?,
                callback_url?, steps:[...], token_note?}}
  FieldDef = {name, label, type:"text"|"password", secret, required,
              placeholder?, maps_to:"api_key"|"api_secret"|None}

No broker / Kite / network. Pure registry+metadata merge.
"""
import pytest

from autotrade.broker import registry
from autotrade.broker import broker_metadata


@pytest.fixture
def supported():
    return {d["broker"]: d for d in registry.list_supported()}


# ── contract shape (every broker) ─────────────────────────────────────────────

def test_every_entry_matches_contract(supported):
    for broker, d in supported.items():
        assert d["broker"] == broker
        assert isinstance(d["live"], bool)
        assert isinstance(d["capabilities"], dict)
        # brand chip — CSS only, no external image URL
        assert set(d["brand"]) == {"color", "initial"}
        assert d["brand"]["color"].startswith("#")
        assert len(d["brand"]["initial"]) >= 1
        assert "http" not in d["brand"]["initial"]
        assert isinstance(d["display_name"], str) and d["display_name"]
        assert isinstance(d["exchanges"], list) and d["exchanges"]
        assert isinstance(d["fields"], list) and d["fields"]
        setup = d["setup"]
        assert isinstance(setup.get("steps"), list) and setup["steps"]
        # field shape
        for f in d["fields"]:
            assert set(["name", "label", "type", "secret", "required",
                        "maps_to"]).issubset(f)
            assert f["type"] in ("text", "password")
            assert isinstance(f["secret"], bool)
            assert isinstance(f["required"], bool)
            assert f["maps_to"] in ("api_key", "api_secret", None)


def test_brand_has_no_external_image_urls(supported):
    for d in supported.values():
        blob = repr(d["brand"])
        assert "http://" not in blob and "https://" not in blob


# ── live flags ────────────────────────────────────────────────────────────────

def test_live_flags_correct(supported):
    assert supported["zerodha"]["live"] is True
    assert supported["rupeezy"]["live"] is True
    assert supported["fivepaisa"]["live"] is False
    for coming_soon in ("upstox", "dhan", "angel", "fyers"):
        assert supported[coming_soon]["live"] is False


def test_fivepaisa_present_as_coming_soon(supported):
    assert "fivepaisa" in supported
    assert supported["fivepaisa"]["live"] is False
    assert supported["fivepaisa"]["display_name"] == "5Paisa"


# ── zerodha metadata ──────────────────────────────────────────────────────────

def test_zerodha_metadata(supported):
    z = supported["zerodha"]
    assert z["display_name"] == "Zerodha (Kite Connect)"
    assert "NSE" in z["exchanges"] and "NFO" in z["exchanges"]
    assert z["setup"]["docs_url"] == "https://developers.kite.trade"
    assert "06:00" in z["setup"]["token_note"]
    names = [f["name"] for f in z["fields"]]
    assert names == ["api_key", "api_secret"]
    api_key, api_secret = z["fields"]
    assert api_key["type"] == "text" and api_key["secret"] is False
    assert api_key["maps_to"] == "api_key"
    assert api_secret["type"] == "password" and api_secret["secret"] is True
    assert api_secret["maps_to"] == "api_secret"
    # callback_url present + substituted into the steps (no raw placeholder left)
    cb = z["setup"]["callback_url"]
    assert cb.startswith("http")
    assert any(cb in s for s in z["setup"]["steps"])
    assert all("{callback_url}" not in s for s in z["setup"]["steps"])


# ── rupeezy metadata ──────────────────────────────────────────────────────────

def test_rupeezy_metadata(supported):
    r = supported["rupeezy"]
    assert r["display_name"] == "Rupeezy (Vortex API)"
    assert "NSE" in r["exchanges"]
    assert r["setup"]["docs_url"].startswith("http")
    labels = [f["label"] for f in r["fields"]]
    assert labels[0] == "Application ID"
    assert "x-api-key" in labels[1].lower()
    # Application ID → api_key ; x-api-key → api_secret
    assert r["fields"][0]["maps_to"] == "api_key"
    assert r["fields"][1]["maps_to"] == "api_secret"
    assert r["fields"][1]["secret"] is True
    # steps reference the real Rupeezy hosts
    steps_blob = " ".join(r["setup"]["steps"]).lower()
    assert "rupeezy.in" in steps_blob


# ── live brokers map cleanly to the 2-field vault ─────────────────────────────

def test_live_broker_fields_map_to_vault(supported):
    for broker in ("zerodha", "rupeezy"):
        d = supported[broker]
        maps = sorted(f["maps_to"] for f in d["fields"])
        assert maps == ["api_key", "api_secret"], broker
        for f in d["fields"]:
            assert f["maps_to"] in ("api_key", "api_secret")
            assert f["required"] is True


# ── coming-soon multi-field brokers are display-only (no vault mapping) ────────

def test_fivepaisa_is_display_only(supported):
    d = supported["fivepaisa"]
    names = [f["name"] for f in d["fields"]]
    assert names == ["app_name", "user_id", "api_key", "encryption_key"]
    # 4-field schema does NOT map to the 2-field vault yet
    assert all(f["maps_to"] is None for f in d["fields"])
    # coming-soon copy present
    assert any("coming soon" in s.lower() for s in d["setup"]["steps"])


def test_dhan_client_id_maps_secret_present(supported):
    d = supported["dhan"]
    names = [f["name"] for f in d["fields"]]
    assert names == ["client_id", "api_key", "api_secret"]
    # Dhan (coming-soon): client_id → api_key, api_secret → api_secret
    by = {f["name"]: f for f in d["fields"]}
    assert by["client_id"]["maps_to"] == "api_key"
    assert by["api_secret"]["maps_to"] == "api_secret"
    assert by["api_secret"]["secret"] is True


def test_upstox_two_fields(supported):
    d = supported["upstox"]
    names = [f["name"] for f in d["fields"]]
    assert names == ["api_key", "api_secret"]


# ── callback_url env override + fallback meta ─────────────────────────────────

def test_callback_url_env_override(monkeypatch):
    monkeypatch.setenv("AUTOTRADE_OAUTH_REDIRECT", "https://example.test/cb/")
    d = {x["broker"]: x for x in registry.list_supported()}["zerodha"]
    assert d["setup"]["callback_url"] == "https://example.test/cb"
    assert any("https://example.test/cb" in s for s in d["setup"]["steps"])


def test_fallback_meta_for_unknown_broker():
    # A registered broker with no rich entry still yields a valid card.
    d = broker_metadata.meta_dict("somenewbroker", live=False,
                                  capabilities={})
    assert d["display_name"] == "Somenewbroker"
    assert [f["maps_to"] for f in d["fields"]] == ["api_key", "api_secret"]
    assert d["brand"]["initial"] == "S"
