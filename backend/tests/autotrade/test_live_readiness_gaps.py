"""LIVE-readiness gap regression tests (two power-user-live audit findings).

FIX A — a USER-OWNED live session must NEVER fall back to the operator's global
        Kite client. If it can't resolve an ACTIVE, OWNED broker account with
        valid creds, the live client build must REFUSE (never global fallback).
        OPERATOR / global sessions (owner_user_id is None) keep today's global
        fallback. Paper (dry_run) is unaffected.

FIX B — Sessions LIVE orders must pass the falcon.preflight 13-check gate before
        touching Kite, mirroring the legacy /place path. RED → refuse (no kite
        call). Paper (dry_run) bypasses preflight entirely.

These are the SAFETY invariants: they must fail-closed. All broker construction
is monkeypatched — NO real Kite is ever contacted.
"""
from __future__ import annotations

import asyncio

import pytest

from autotrade.broker.zerodha import ZerodhaBroker
from autotrade.config import BrokerProfile


# ── shared helpers ───────────────────────────────────────────────────────────

class _SpyKite:
    """A stand-in KiteConnect that records that it was built/used."""
    def __init__(self, tag: str):
        self.tag = tag
        self.access_token = None
        self.placed = []
        # kite product / txn constants used by Order.to_kite_params
        self.PRODUCT_CNC = "CNC"; self.PRODUCT_MIS = "MIS"
        self.PRODUCT_NRML = "NRML"
        self.VARIETY_REGULAR = "regular"
        self.TRANSACTION_TYPE_BUY = "BUY"; self.TRANSACTION_TYPE_SELL = "SELL"
        self.ORDER_TYPE_LIMIT = "LIMIT"; self.ORDER_TYPE_MARKET = "MARKET"
        self.EXCHANGE_NSE = "NSE"

    def set_access_token(self, tok):
        self.access_token = tok

    def place_order(self, **kw):
        self.placed.append(kw)
        return "OID-123"


def _profile(broker_account_id=None, owner_user_id=None, api_key="", access_token=""):
    p = BrokerProfile(profile_id="p1", broker_name="zerodha",
                      allocated_capital=100000.0, order_product="CNC",
                      instrument_type="EQ", broker_account_id=broker_account_id)
    p.api_key = api_key
    p.access_token = access_token
    # owner_user_id is the additive field FIX A threads in from the session.
    p.owner_user_id = owner_user_id
    return p


def _live_env(monkeypatch):
    """Turn the master live switch ON so _live_allowed() is True."""
    monkeypatch.setenv("FALCON_AUTOTRADE_ENABLED", "true")


# ══ FIX A ════════════════════════════════════════════════════════════════════

def test_A1_user_owned_live_unbound_refuses_no_global(monkeypatch):
    """(1) user-owned LIVE session, NO bound account → refuse; global client
    NEVER built."""
    global_built = {"n": 0}

    def _boom_global(check=False):
        global_built["n"] += 1
        return _SpyKite("GLOBAL")

    monkeypatch.setattr("services.kite_auth.get_kite_client", _boom_global,
                        raising=False)

    prof = _profile(broker_account_id=None, owner_user_id="user-42")
    broker = ZerodhaBroker(prof, dry_run=False)  # live
    with pytest.raises(ValueError) as ei:
        broker._build_kite()
    assert "NO_OWNED_BROKER_ACCOUNT" in str(ei.value)
    assert global_built["n"] == 0, "must NOT touch the operator global client"


def test_A2_user_owned_live_bound_no_token_refuses_no_global(monkeypatch):
    """(2) user-owned LIVE session whose bound account has NO/expired token →
    refuse, NOT global fallback."""
    global_built = {"n": 0}
    monkeypatch.setattr("services.kite_auth.get_kite_client",
                        lambda check=False: global_built.__setitem__("n", global_built["n"] + 1),
                        raising=False)

    prof = _profile(broker_account_id="acct-9", owner_user_id="user-42",
                    api_key="k", access_token="")  # token missing
    broker = ZerodhaBroker(prof, dry_run=False)
    with pytest.raises(ValueError) as ei:
        broker._build_kite()
    # the bound-but-no-token branch already raises; assert it never went global
    assert global_built["n"] == 0


def test_A3_operator_global_live_still_builds(monkeypatch):
    """(3) OPERATOR / global (owner_user_id None) LIVE session → still builds the
    global client, unchanged."""
    spy = _SpyKite("GLOBAL")
    built = {"n": 0}

    def _global(check=False):
        built["n"] += 1
        return spy

    monkeypatch.setattr("services.kite_auth.get_kite_client", _global,
                        raising=False)

    prof = _profile(broker_account_id=None, owner_user_id=None)  # operator
    broker = ZerodhaBroker(prof, dry_run=False)
    kite = broker._build_kite()
    assert kite is spy
    assert built["n"] == 1


def test_A4_user_owned_paper_unaffected(monkeypatch):
    """(4) paper (dry_run) user-owned session → NOT refused. Paper never builds a
    live client on the order path; even if _build_kite is called for market data
    it must not raise the NO_OWNED_BROKER_ACCOUNT refusal (that gate is live-only)."""
    spy = _SpyKite("GLOBAL")
    monkeypatch.setattr("services.kite_auth.get_kite_client",
                        lambda check=False: spy, raising=False)
    prof = _profile(broker_account_id=None, owner_user_id="user-42")
    broker = ZerodhaBroker(prof, dry_run=True)  # paper
    # Must NOT raise the live refusal in paper mode.
    kite = broker._build_kite()
    assert kite is spy


def test_A5_user_owned_live_place_order_fails_leg_not_global(monkeypatch):
    """End-to-end: place_order on a user-owned live unbound session returns a
    FAILED leg (NO_OWNED_BROKER_ACCOUNT), never places at the global client."""
    _live_env(monkeypatch)
    # Preflight GREEN so the leg reaches _build_kite — the NO_OWNED refusal (Fix A)
    # must fire on its own even when every reliability check passes.
    _patch_preflight(monkeypatch, ok=True)
    global_built = {"n": 0}
    monkeypatch.setattr("services.kite_auth.get_kite_client",
                        lambda check=False: global_built.__setitem__("n", global_built["n"] + 1),
                        raising=False)
    from autotrade.execution.orders import Order
    prof = _profile(broker_account_id=None, owner_user_id="user-42")
    broker = ZerodhaBroker(prof, dry_run=False)
    order = Order(symbol="INFY", qty=1, exchange="NSE", transaction_type="BUY",
                  product="CNC", order_type="MARKET")
    res = asyncio.run(broker.place_order(order))
    assert res.status == "FAILED"
    assert "NO_OWNED_BROKER_ACCOUNT" in (res.error or "")
    assert global_built["n"] == 0


# ══ FIX B ════════════════════════════════════════════════════════════════════

def _patch_preflight(monkeypatch, ok: bool, red_names=None):
    """Force falcon.preflight to a GREEN or RED cached result."""
    import falcon.preflight as pf

    class _FakeCheck:
        def __init__(self, name, status):
            self.name = name; self.status = status
            self.detail = "d"; self.remediation = "r"

    reds = red_names or (["kite_ip_allowed"] if not ok else [])
    checks = [_FakeCheck(n, pf.RED) for n in reds]
    if ok:
        checks = [_FakeCheck("kite_token_valid", pf.GREEN)]

    class _FakeResult:
        def __init__(self):
            self.ok = ok
            self.checks = checks

    monkeypatch.setattr(pf, "get_cached", lambda: _FakeResult())
    # run() should not be needed (cache hit) but patch it defensively.
    monkeypatch.setattr(pf, "run", lambda force=False, include_kite=True: _FakeResult())


def test_B1_live_order_refused_on_red_preflight(monkeypatch):
    """RED preflight → live order refused with the named reason, NO kite call."""
    _live_env(monkeypatch)
    _patch_preflight(monkeypatch, ok=False, red_names=["kite_ip_allowed"])
    spy = _SpyKite("GLOBAL")
    monkeypatch.setattr("services.kite_auth.get_kite_client",
                        lambda check=False: spy, raising=False)
    from autotrade.execution.orders import Order
    prof = _profile(broker_account_id=None, owner_user_id=None)  # operator/global
    broker = ZerodhaBroker(prof, dry_run=False)
    order = Order(symbol="INFY", qty=1, exchange="NSE", transaction_type="BUY",
                  product="CNC", order_type="MARKET")
    res = asyncio.run(broker.place_order(order))
    assert res.status == "FAILED"
    assert "PREFLIGHT" in (res.error or "").upper()
    assert "kite_ip_allowed" in (res.error or "")
    assert spy.placed == [], "no kite.place_order call on RED preflight"


def test_B2_live_order_proceeds_on_green_preflight(monkeypatch):
    """GREEN preflight → order proceeds to kite."""
    _live_env(monkeypatch)
    _patch_preflight(monkeypatch, ok=True)
    spy = _SpyKite("GLOBAL")
    monkeypatch.setattr("services.kite_auth.get_kite_client",
                        lambda check=False: spy, raising=False)
    # legacy retry wrapper just runs the lambda
    monkeypatch.setattr(
        "falcon.trade.services.order_executor._retry_kite_call",
        lambda fn, *a, **k: fn(), raising=False)
    from autotrade.execution.orders import Order
    prof = _profile(broker_account_id=None, owner_user_id=None)
    broker = ZerodhaBroker(prof, dry_run=False)
    order = Order(symbol="INFY", qty=1, exchange="NSE", transaction_type="BUY",
                  product="CNC", order_type="MARKET")
    res = asyncio.run(broker.place_order(order))
    assert res.status == "PLACED"
    assert len(spy.placed) == 1


def test_B3_paper_bypasses_preflight(monkeypatch):
    """Paper (dry_run) → preflight NEVER consulted, synthetic DRY_RUN result."""
    called = {"pf": 0}
    import falcon.preflight as pf
    monkeypatch.setattr(pf, "get_cached",
                        lambda: called.__setitem__("pf", called["pf"] + 1))
    monkeypatch.setattr(pf, "run",
                        lambda *a, **k: called.__setitem__("pf", called["pf"] + 1))
    from autotrade.execution.orders import Order
    prof = _profile(broker_account_id=None, owner_user_id="user-42")
    broker = ZerodhaBroker(prof, dry_run=True)  # paper
    order = Order(symbol="INFY", qty=1, exchange="NSE", transaction_type="BUY",
                  product="CNC", order_type="MARKET")
    res = asyncio.run(broker.place_order(order))
    assert res.status == "DRY_RUN"
    assert called["pf"] == 0, "paper must not consult preflight"


def test_B4_market_exit_not_blocked_by_preflight(monkeypatch):
    """An EXIT must ALWAYS attempt. A RED preflight (stale signals/data — the
    engine's daily readiness, NOT whether a broker exit can execute) must NEVER
    block a capital-protecting exit, or the kill switch / stop-loss could not
    flatten a live position. Only ENTRIES gate on preflight."""
    _live_env(monkeypatch)
    _patch_preflight(monkeypatch, ok=False, red_names=["signals_fresh"])
    spy = _SpyKite("GLOBAL")
    monkeypatch.setattr("services.kite_auth.get_kite_client",
                        lambda check=False: spy, raising=False)
    prof = _profile(broker_account_id=None, owner_user_id=None)
    broker = ZerodhaBroker(prof, dry_run=False)
    res = asyncio.run(broker.place_market_exit("INFY", 1, "EQ"))
    assert res.status == "PLACED"                       # exit proceeded despite RED
    assert "PREFLIGHT" not in (res.error or "").upper()
    assert len(spy.placed) == 1                         # order reached the broker


def test_A6_admin_owner_live_may_use_global(monkeypatch):
    """An ADMIN-owned live session with NO bound account MAY use the operator's
    global account (the admin IS the operator). Non-admin owners are still
    refused — see test_A1 / A2 / A5."""
    _live_env(monkeypatch)
    spy = _SpyKite("GLOBAL")
    monkeypatch.setattr("services.kite_auth.get_kite_client",
                        lambda check=False: spy, raising=False)
    prof = _profile(broker_account_id=None, owner_user_id="admin-1")
    prof.owner_is_admin = True
    broker = ZerodhaBroker(prof, dry_run=False)
    kite = broker._build_kite()
    assert kite is spy   # built the global client, no NO_OWNED_BROKER_ACCOUNT
