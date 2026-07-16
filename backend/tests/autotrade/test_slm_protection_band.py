"""BUG 1 (2026-07-15) — every protective SL-M was REJECTED (Kite error 16448
"Difference between limit price and trigger price is beyond permissible range").

Root cause: ZerodhaBroker.place_protective_slm sent market_protection=5.0. Kite
converts the SL-M into an SL-LIMIT with limit = trigger ± 5%, which exceeds the
permissible SL execution band → the exchange REJECTED every protective stop
(placed at entry) so NO broker-side stop existed on any live leg that day.

Fix:
  * market_protection is now a conservative IN-BAND % (default 1.0, HARD-CLAMPED
    to (0, 3.0]) — the crash BACKSTOP behind the software stop, so acceptance
    beats fill-headroom. The 5% mistake can never be reintroduced via env.
  * place_order succeeds (returns an id) even when RMS then REJECTS the order
    asynchronously (how 2026-07-15 failed silently). A post-placement status
    probe now detects REJECTED/CANCELLED → PAGES + returns None (not "protected").

MUTATION-VERIFIED (test_slm_uses_in_band_protection):
  Revert market_protection back to 5.0 in place_protective_slm → the assert
  captured["market_protection"] == 1.0 fails.
MUTATION-VERIFIED (test_rejected_slm_pages_and_returns_none):
  Delete the `_alert_if_slm_rejected` return-None branch (return str(oid)
  unconditionally) → the assert oid is None + the alert-row assert both fail.
"""
import pytest

from autotrade.broker.zerodha import (
    ZerodhaBroker, _slm_market_protection_pct,
    _SLM_PROTECTION_DEFAULT_PCT, _SLM_PROTECTION_MAX_PCT,
)
from autotrade.config import BrokerProfile


# ── The load-bearing value: in-band, clamped, safe-defaulting ────────────────

def test_default_protection_is_in_band(monkeypatch):
    monkeypatch.delenv("FALCON_AUTOTRADE_SLM_PROTECTION_PCT", raising=False)
    assert _slm_market_protection_pct() == _SLM_PROTECTION_DEFAULT_PCT == 1.0
    assert _SLM_PROTECTION_DEFAULT_PCT < 5.0          # NOT the rejected 5%
    assert _SLM_PROTECTION_DEFAULT_PCT <= _SLM_PROTECTION_MAX_PCT


def test_env_override_in_band(monkeypatch):
    monkeypatch.setenv("FALCON_AUTOTRADE_SLM_PROTECTION_PCT", "2.0")
    assert _slm_market_protection_pct() == 2.0


def test_env_clamped_to_max(monkeypatch):
    # The 5% that caused 16448 can NEVER be reintroduced via config.
    monkeypatch.setenv("FALCON_AUTOTRADE_SLM_PROTECTION_PCT", "5.0")
    assert _slm_market_protection_pct() == _SLM_PROTECTION_MAX_PCT == 3.0


def test_env_garbage_and_nonpositive_fall_back(monkeypatch):
    for bad in ("abc", "", "0", "-1", "-0.5"):
        monkeypatch.setenv("FALCON_AUTOTRADE_SLM_PROTECTION_PCT", bad)
        assert _slm_market_protection_pct() == _SLM_PROTECTION_DEFAULT_PCT


# ── The real adapter builds an IN-BAND SL-M and self-detects a rejection ─────

class _FakeKite:
    VARIETY_REGULAR = "regular"
    EXCHANGE_NSE = "NSE"
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"
    ORDER_TYPE_SLM = "SL-M"
    VALIDITY_DAY = "DAY"
    PRODUCT_CNC = "CNC"
    PRODUCT_MIS = "MIS"
    PRODUCT_NRML = "NRML"

    def __init__(self, order_rows=None, oid="SLM-1"):
        self.last_params = None
        self._oid = oid
        self._order_rows = order_rows or []

    def place_order(self, **params):
        self.last_params = params
        return self._oid

    def orders(self):
        return self._order_rows


def _live_zerodha(fake, monkeypatch):
    # get_tick_size hits the live kite — stub it to a plain 0.05 tick.
    monkeypatch.setattr(
        "falcon.trade.services.mtf_eligibility.get_tick_size",
        lambda kite, sym: 0.05)
    b = ZerodhaBroker(BrokerProfile("z1", "zerodha"), dry_run=False)
    b._kite = fake
    b._live_allowed = lambda: True
    return b


def test_slm_uses_in_band_protection(monkeypatch, clean_positions):
    # An accepted (TRIGGER PENDING) SL-M: correct order_type + IN-BAND protection.
    fake = _FakeKite(order_rows=[{"order_id": "SLM-1", "status": "TRIGGER PENDING"}])
    b = _live_zerodha(fake, monkeypatch)
    oid = b.place_protective_slm("SHORTY", 100, 108.0, direction="short",
                                 product="MIS")
    assert oid == "SLM-1"
    p = fake.last_params
    assert p["order_type"] == "SL-M"
    assert p["market_protection"] == 1.0          # in-band, NOT 5.0
    assert p["trigger_price"] == pytest.approx(108.0)
    assert p["transaction_type"] == "BUY"         # short → BUY-to-cover
    assert p["validity"] == "DAY"


def test_rejected_slm_pages_and_returns_none(monkeypatch, clean_positions):
    # place_order returns an id, but RMS then REJECTS it (the 16448 case).
    fake = _FakeKite(order_rows=[{
        "order_id": "SLM-1", "status": "REJECTED",
        "status_message": ("16448 : Difference between limit price and trigger "
                           "price is beyond permissible range")}])
    b = _live_zerodha(fake, monkeypatch)
    oid = b.place_protective_slm("RRKABEL", 50, 950.0, direction="long",
                                 product="MIS")
    # A rejected stop is NOT protection → caller must not persist the dead id.
    assert oid is None
    # ...and it must PAGE (a naked position with no working broker stop).
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT kind, symbol FROM autotrade_alerts "
            "WHERE kind='PROTECTIVE_STOP_REJECTED' AND symbol='RRKABEL'"
        ).fetchone()
    assert row is not None


def test_accepted_slm_does_not_page(monkeypatch, clean_positions):
    fake = _FakeKite(order_rows=[{"order_id": "SLM-1", "status": "TRIGGER PENDING"}])
    b = _live_zerodha(fake, monkeypatch)
    oid = b.place_protective_slm("CHALET", 30, 700.0, direction="long",
                                 product="MIS")
    assert oid == "SLM-1"
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        n = con.execute(
            "SELECT COUNT(*) AS n FROM autotrade_alerts "
            "WHERE kind='PROTECTIVE_STOP_REJECTED'").fetchone()["n"]
    assert n == 0
