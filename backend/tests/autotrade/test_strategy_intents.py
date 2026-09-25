"""Strategy intents: policy (defined risk, hedge first), idempotent intake, the live gate checklist, operator arms,
and group-by-group dispatch. No real broker: live dispatch runs against FakeBroker; dry runs use the real Zerodha
adapter in dry_run mode, whose place_order returns DRY_RUN before any network call."""
import asyncio
from datetime import date

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from autotrade.broker.base import OrderResult
from autotrade.intents import dispatcher as D
from autotrade.intents import gates as G
from autotrade.intents import policy as P
from autotrade.intents import store
from autotrade.order_ledger import get_events

EXP = "2026-07-02"
TODAY = date(2026, 6, 25)
LOT = 75


def leg(strike, ot, side, group, px=10.0, qty=LOT):
    return {"tradingsymbol": f"NIFTY26702{int(strike)}{ot}", "exchange": "NFO", "underlying": "NIFTY", "expiry": EXP,
            "strike": strike, "option_type": ot, "side": side, "quantity": qty, "lot_size": LOT,
            "limit_price": px, "product": "NRML", "group": group}


def condor():
    return [leg(24400, "CE", "BUY", 1, 10.0), leg(23600, "PE", "BUY", 1, 12.0),
            leg(24200, "CE", "SELL", 2, 30.0), leg(23800, "PE", "SELL", 2, 35.0)]


_n = [0]


def body(legs=None, mode="dry_run", **kw):
    _n[0] += 1
    return {"source": "kanida-app", "idempotency_key": f"test-key-{_n[0]:06d}-{mode}", "mode": mode,
            "broker": "zerodha", "user_id": "u1", "broker_account_id": "acct1", "legs": legs or condor(), **kw}


class FakeKiteClient:
    """Just enough of KiteConnect for Order.to_kite_params + one place_order call. Records every call."""
    PRODUCT_CNC, PRODUCT_MIS, PRODUCT_NRML = "CNC", "MIS", "NRML"
    VARIETY_REGULAR, ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET = "regular", "LIMIT", "MARKET"
    TRANSACTION_TYPE_BUY, TRANSACTION_TYPE_SELL, EXCHANGE_NFO = "BUY", "SELL", "NFO"

    def __init__(self, broker):
        self.b = broker

    def place_order(self, **params):
        self.b.calls.append(params)
        oid = f"oid{len(self.b.placed) + 1}"
        if params["tradingsymbol"] in self.b.timeout_reaches:          # the order reached Kite, the reply did not
            self.b.placed.append((params["tradingsymbol"], params["transaction_type"], params["price"],
                                  params["product"], params["exchange"], oid, params["tag"]))
            raise TimeoutError("read timeout")
        if params["tradingsymbol"] in self.b.timeout_lost:
            raise TimeoutError("read timeout")
        self.b.placed.append((params["tradingsymbol"], params["transaction_type"], params["price"], params["product"],
                              params["exchange"], oid, params["tag"]))
        return oid


class FakeBroker:
    """A LIVE-behaving broker double driven through the dispatcher's real single-shot path: orders rest until
    `fill` says they fill. No retry exists anywhere in this path - every place_order call is recorded."""
    dry_run = False

    def __init__(self, *, fill=None, reject=(), margin=(50_000.0, 1_000_000.0), on_status=None,
                 timeout_reaches=(), timeout_lost=(), lookup_fails=False):
        self.placed, self.cancelled, self.calls, self.status_calls = [], [], [], 0
        self.fill = fill if fill is not None else (lambda o: True)
        self.reject, self.margin, self.on_status = set(reject), margin, on_status
        self.timeout_reaches, self.timeout_lost, self.lookup_fails = set(timeout_reaches), set(timeout_lost), lookup_fails
        self.kite = FakeKiteClient(self)

    def _live_allowed(self):
        return True

    def _token_abort_reason(self):
        return None

    def _preflight_block_reason(self):
        return None

    def find_recent_order(self, order):
        if self.lookup_fails:
            raise ConnectionError("orderbook unavailable")
        hit = [p for p in self.placed if p[6] == order.tag]
        return {"order_id": hit[-1][5], "status": "OPEN"} if hit else None

    def get_order_status(self, oid):
        self.status_calls += 1
        if self.on_status:
            self.on_status(oid)
        sym, side, *_ = next(p for p in self.placed if p[5] == oid)
        if sym in self.reject:
            return {"status": "REJECTED", "filled_quantity": 0, "status_message": "RMS"}
        if self.fill(sym):
            return {"status": "COMPLETE", "filled_quantity": LOT, "average_price": 10.0}
        return {"status": "OPEN", "filled_quantity": 0}

    def cancel_order_sync(self, oid):
        self.cancelled.append(oid)
        return True

    def basket_margin(self, orders):
        return self.margin[0]

    def available_margin(self):
        return self.margin[1]


@pytest.fixture(autouse=True)
def _hermetic_calendar(monkeypatch):
    """The real market-hours gate reads the legacy quant DB's OHLC coverage; keep these tests off the repo DB.
    Market closed by default; live_on opens it."""
    monkeypatch.setattr(G, "_market_open", lambda: False)


@pytest.fixture
def live_on(monkeypatch):
    for k in ("FALCON_AUTOTRADE_ENABLED", "FALCON_AUTOTRADE_OPTIONS_ENABLED", "AUTOTRADE_STRATEGY_INTENTS_LIVE"):
        monkeypatch.setenv(k, "true")
    monkeypatch.setattr(G, "_master_enabled", lambda: True)
    monkeypatch.setattr(G, "_broker_certified", lambda b: True)
    monkeypatch.setattr(G, "_market_open", lambda: True)
    store.disarm("u1", "acct1")
    return store.arm(user_id="u1", broker_account_id="acct1", armed_by="Shyam", ttl_minutes=30, max_baskets=5,
                     max_loss_per_basket=100_000)


def run(iid, broker, **kw):
    return asyncio.run(D.run(iid, broker_factory=lambda rec, live: broker, poll_s=0.0, group_timeout_s=0.3, **kw))


# ── policy ─────────────────────────────────────────────────────────────────────────────────────────────────────────

def test_condor_is_accepted_with_exact_max_loss():
    b = P.validate(body(), TODAY)
    # credit = (30+35-10-12)=43 per unit; width 200 -> max loss (200-43)*75
    assert b["net_premium"] == 43 * LOT and b["max_loss"] == (200 - 43) * LOT and b["groups"] == [1, 2]


@pytest.mark.parametrize("legs,code", [
    ([leg(23800, "PE", "SELL", 1)], "UNDEFINED_RISK"),                                   # naked short put
    ([leg(24200, "CE", "SELL", 1), leg(23800, "PE", "SELL", 1), leg(24400, "CE", "BUY", 1)], "UNDEFINED_RISK"),
    ([leg(24400, "CE", "BUY", 2), leg(24200, "CE", "SELL", 1)], "HEDGE_NOT_FIRST"),
    ([leg(24400, "CE", "BUY", 1), leg(24200, "CE", "SELL", 1)], "HEDGE_NOT_FIRST"),       # same group
    ([leg(24400, "CE", "BUY", 1, px=10.03)], "BAD_PRICE"),
    ([leg(24400, "CE", "BUY", 1, qty=100)], "BAD_QUANTITY"),
    ([{**leg(24400, "CE", "BUY", 1), "tradingsymbol": "NIFTY2670224500CE"}], "SYMBOL_MISMATCH"),
    ([{**leg(24400, "CE", "BUY", 1), "underlying": "BANKNIFTY", "tradingsymbol": "BANKNIFTY26702CE"}], "UNDERLYING_NOT_ALLOWED"),
    ([leg(24400, "CE", "BUY", 1), {**leg(24200, "CE", "SELL", 2), "expiry": "2026-07-09", "tradingsymbol": "NIFTY2670924200CE"}], "MULTI_EXPIRY"),
    ([{**leg(24400, "CE", "BUY", 1), "expiry": "2026-06-18"}], "EXPIRED"),
    ([{**leg(24400, "CE", "BUY", 1), "limit_price": 0}], "BAD_PRICE"),
    ([leg(24400, "CE", "BUY", 1, qty=LOT * 25)], "ABOVE_FREEZE"),
])
def test_policy_refusals(legs, code):
    with pytest.raises(P.PolicyError) as e:
        P.validate(body(legs), TODAY)
    assert e.value.code == code


def test_debit_spread_max_loss_is_the_debit():
    b = P.validate(body([leg(24000, "CE", "BUY", 1, 120.0), leg(24200, "CE", "SELL", 2, 50.0)]), TODAY)
    assert b["max_loss"] == 70 * LOT


# ── intake, idempotency, gates ─────────────────────────────────────────────────────────────────────────────────────

def test_dry_run_goes_group_by_group_through_the_real_adapter_without_orders():
    rec, replayed = D.submit(body())
    assert not replayed and rec["state"] == "accepted" and rec["mode"] == "dry_run"
    out = asyncio.run(D.run(rec["id"]))                         # default factory: ZerodhaBroker(dry_run=True)
    assert out["state"] == "dry_run_complete" and "no broker order" in out["reason"]
    assert [l["state"] for l in out["legs"]] == ["dry_run"] * 4 and all(l["broker_order_id"] is None for l in out["legs"])
    ev = get_events(f"intent:{rec['id']}")
    assert len(ev) == 4 and {e["event_type"] for e in ev} == {"ORDER_CREATED"}


def test_idempotent_replay_and_conflict():
    b = body()
    first, r1 = D.submit(b)
    again, r2 = D.submit(dict(b))
    assert not r1 and r2 and again["id"] == first["id"]
    with pytest.raises(D.IntakeError) as e:
        D.submit({**b, "legs": [leg(24400, "CE", "BUY", 1)]})
    assert e.value.status == 409 and e.value.code == "IDEMPOTENCY_CONFLICT"


def test_live_request_with_gates_off_is_blocked_never_downgraded(monkeypatch):
    monkeypatch.delenv("FALCON_AUTOTRADE_ENABLED", raising=False)
    rec, _ = D.submit(body(mode="live"))
    assert rec["state"] == "blocked" and rec["mode"] == "live"
    for g in ("master", "options", "intents_live", "broker_certified", "armed"):
        assert g in rec["reason"]
    assert all(l["state"] == "not_sent" for l in rec["legs"])
    called = []
    asyncio.run(D.run(rec["id"], broker_factory=lambda *a: called.append(1)))
    assert not called                                           # a blocked basket never reaches a broker


def test_arm_loss_cap_and_basket_allowance(live_on):
    store.arm(user_id="u1", broker_account_id="acct1", armed_by="Shyam", ttl_minutes=30, max_baskets=1,
              max_loss_per_basket=1_000)
    rec, _ = D.submit(body(mode="live"))
    assert rec["state"] == "blocked" and "arm_loss_cap" in rec["reason"]
    store.arm(user_id="u1", broker_account_id="acct1", armed_by="Shyam", ttl_minutes=30, max_baskets=1,
              max_loss_per_basket=100_000)
    ok, _ = D.submit(body(mode="live"))
    assert ok["state"] == "accepted"
    nope, _ = D.submit(body(mode="live"))
    assert nope["state"] == "blocked" and "armed" in nope["reason"]


# ── live dispatch against the fake broker ──────────────────────────────────────────────────────────────────────────

def test_live_hedges_fill_before_any_sell_is_sent(live_on):
    rec, _ = D.submit(body(mode="live"))
    br = FakeBroker()
    out = run(rec["id"], br)
    assert out["state"] == "completed", out["reason"]
    sides = [p[1] for p in br.placed]
    assert sides == ["BUY", "BUY", "SELL", "SELL"]
    assert all(p[3] == "NRML" and p[4] == "NFO" for p in br.placed)
    assert all(l["state"] == "filled" and l["filled_qty"] == LOT for l in out["legs"])


def test_unfilled_hedge_stops_the_basket_and_no_sell_is_sent(live_on):
    rec, _ = D.submit(body(mode="live"))
    br = FakeBroker(fill=lambda s: s.endswith("CE"))            # the put hedge never fills
    out = run(rec["id"], br)
    assert [p[1] for p in br.placed] == ["BUY", "BUY"]
    assert br.cancelled == ["oid2"]
    assert out["state"] == "attention_required" and "later groups were not sent" in out["reason"]
    assert [l["state"] for l in out["legs"]][2:] == ["not_sent", "not_sent"]


def test_rejected_hedge_stops_the_basket(live_on):
    rec, _ = D.submit(body(mode="live"))
    br = FakeBroker(reject={"NIFTY2670223600PE"})
    out = run(rec["id"], br)
    assert "SELL" not in [p[1] for p in br.placed]
    assert out["state"] == "attention_required"                 # the call hedge filled and stays open


def test_insufficient_or_unknown_margin_sends_nothing(live_on):
    rec, _ = D.submit(body(mode="live"))
    br = FakeBroker(margin=(200_000.0, 50_000.0))
    out = run(rec["id"], br)
    assert not br.placed and out["state"] == "failed" and "INSUFFICIENT_MARGIN" in out["reason"]
    rec2, _ = D.submit(body(mode="live"))
    br2 = FakeBroker(margin=(None, 50_000.0))
    out2 = run(rec2["id"], br2)
    assert not br2.placed and "MARGIN_UNKNOWN" in out2["reason"]


def test_disarm_between_groups_stops_the_sells(live_on):
    rec, _ = D.submit(body(mode="live"))
    br = FakeBroker(on_status=lambda oid: store.disarm("u1", "acct1"))
    out = run(rec["id"], br)
    assert [p[1] for p in br.placed] == ["BUY", "BUY"]
    assert out["state"] == "attention_required" and "armed" in out["reason"]


def test_run_is_single_shot_and_cancel_before_dispatch(live_on):
    rec, _ = D.submit(body(mode="live"))
    c = D.cancel(rec["id"])
    assert c["state"] == "cancelled" and all(l["state"] == "not_sent" for l in c["legs"])
    br = FakeBroker()
    assert run(rec["id"], br)["state"] == "cancelled" and not br.placed


# ── routes ─────────────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def client(monkeypatch):
    from autotrade.api.intents_routes import router
    monkeypatch.setenv("FALCON_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.delenv("AUTOTRADE_PORTAL_STRICT", raising=False)
    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app)


H = {"X-Operator-Token": "test-operator-token"}


def test_routes_gate_submit_and_arming(client, monkeypatch):
    assert client.get("/api/autotrade/intents/capability").status_code == 403
    cap = client.get("/api/autotrade/intents/capability?user_id=u9&broker_account_id=a9", headers=H).json()
    assert cap["live_allowed"] is False and cap["default_mode"] == "dry_run" and cap["arm"] is None
    assert {g["gate"] for g in cap["gates"]} >= {"master", "options", "intents_live", "broker_certified", "armed", "market_open"}
    r = client.post("/api/autotrade/intents", headers=H, json=body())
    assert r.status_code == 201 and r.json()["intent"]["mode"] == "dry_run"
    iid = r.json()["intent"]["id"]
    assert client.get(f"/api/autotrade/intents/{iid}", headers=H).json()["intent"]["id"] == iid
    bad = client.post("/api/autotrade/intents", headers=H, json=body([leg(23800, "PE", "SELL", 1)]))
    assert bad.status_code == 400 and bad.json()["code"] == "UNDEFINED_RISK"
    arm = {"user_id": "u9", "broker_account_id": "a9", "armed_by": "Shyam", "ttl_minutes": 15, "max_baskets": 1,
           "max_loss_per_basket": 20_000}
    monkeypatch.delenv("FALCON_OPERATOR_ARM_TOKEN", raising=False)
    assert client.post("/api/autotrade/intents/arm", headers=H, json=arm).status_code == 503
    monkeypatch.setenv("FALCON_OPERATOR_ARM_TOKEN", "arm-secret")
    assert client.post("/api/autotrade/intents/arm", headers=H, json=arm).status_code == 403   # operator token alone
    ok = client.post("/api/autotrade/intents/arm", headers={**H, "X-Operator-Arm-Token": "arm-secret"}, json=arm)
    assert ok.status_code == 200 and ok.json()["arm"]["armed_by"] == "Shyam"
    cap = client.get("/api/autotrade/intents/capability?user_id=u9&broker_account_id=a9", headers=H).json()
    assert cap["arm"]["max_baskets"] == 1
    assert client.post("/api/autotrade/intents/disarm", headers=H, json={"user_id": "u9", "broker_account_id": "a9"}).json()["disarmed"] == 1


# ── reviewer findings (2026-09-25) ─────────────────────────────────────────────────────────────────────────────────

def test_a_timed_out_order_that_reached_the_broker_is_adopted_never_resent(live_on):
    rec, _ = D.submit(body(mode="live"))
    br = FakeBroker(timeout_reaches={"NIFTY2670223800PE"})            # a SELL leg: the dangerous one to double
    out = run(rec["id"], br)
    assert len([c for c in br.calls if c["tradingsymbol"] == "NIFTY2670223800PE"]) == 1
    assert out["state"] == "completed", out["reason"]


def test_a_timeout_with_unknown_outcome_stops_and_flags(live_on):
    rec, _ = D.submit(body(mode="live"))
    br = FakeBroker(timeout_lost={"NIFTY2670223600PE"}, lookup_fails=True)   # a hedge; outcome cannot be known
    out = run(rec["id"], br)
    leg_states = {l["tradingsymbol"]: l["state"] for l in out["legs"]}
    assert leg_states["NIFTY2670223600PE"] == "unknown" and "SELL" not in [c["transaction_type"] for c in br.calls]
    assert out["state"] == "attention_required" and len(br.calls) == 2


def test_a_timeout_confirmed_absent_is_a_clean_failure(live_on):
    rec, _ = D.submit(body([leg(24000, "CE", "BUY", 1, 120.0)], mode="live"))
    br = FakeBroker(timeout_lost={"NIFTY2670224000CE"})
    out = run(rec["id"], br)
    assert out["state"] == "failed" and out["legs"][0]["state"] == "failed" and "confirmed absent" in out["legs"][0]["error"]


def test_a_crash_after_minting_the_order_id_is_attention_required(live_on, monkeypatch):
    rec, _ = D.submit(body(mode="live"))
    br = FakeBroker()
    real = D._submit_live
    async def boom(broker, order):
        if order.symbol == "NIFTY2670223600PE":
            raise RuntimeError("process died")
        return await real(broker, order)
    monkeypatch.setattr(D, "_submit_live", boom)
    out = run(rec["id"], br)
    assert out["state"] == "attention_required"


def test_live_needs_a_named_user_and_account(live_on):
    rec, _ = D.submit(body(mode="live", broker_account_id=None))
    assert rec["state"] == "blocked" and "LIVE_NEEDS_ACCOUNT" in rec["reason"]


def test_broker_mismatch_is_refused_before_any_order(live_on, monkeypatch):
    from autotrade import vault
    class Creds:
        broker, api_key, api_secret, access_token = "rupeezy", "k", "s", "t"
    monkeypatch.setattr(vault, "vault_enabled", lambda: True)
    monkeypatch.setattr(vault, "get_decrypted_creds", lambda acct, user_id=None: Creds())
    rec, _ = D.submit(body(mode="live"))
    out = asyncio.run(D.run(rec["id"], poll_s=0.0, group_timeout_s=0.3))       # the real factory
    assert out["state"] == "failed" and "BROKER_MISMATCH" in out["reason"]
    assert all(l["state"] == "not_sent" for l in out["legs"])


def test_symbol_must_encode_this_expiry_and_underlying():
    for bad in ("NIFTYNXT5026702" + "24400CE", "NIFTY2670924400CE", "NIFTY26AUG24400CE"):
        with pytest.raises(P.PolicyError) as e:
            P.validate(body([{**leg(24400, "CE", "BUY", 1), "tradingsymbol": bad}]), TODAY)
        assert e.value.code == "SYMBOL_MISMATCH"
    P.validate(body([{**leg(24400, "CE", "BUY", 1), "tradingsymbol": "NIFTY26JUL24400CE"}]), TODAY)   # monthly form


def test_sweep_moves_an_interrupted_basket_to_attention(live_on):
    from autotrade.session import set_fake_now
    from datetime import datetime, timedelta
    rec, _ = D.submit(body(mode="live"))
    store.set_state(rec["id"], "dispatching", only_from={"accepted"})
    store.update_leg(rec["legs"][0]["id"], client_order_id="FAL-x")
    try:
        set_fake_now(datetime(2026, 6, 25, 10, 30))
        assert D.sweep_stale() >= 1
    finally:
        set_fake_now(None)
    assert store.get(rec["id"])["state"] == "attention_required"


def test_arm_token_equal_to_operator_token_is_refused(client, monkeypatch):
    monkeypatch.setenv("FALCON_OPERATOR_ARM_TOKEN", "test-operator-token")
    arm = {"user_id": "u9", "broker_account_id": "a9", "armed_by": "Shyam", "max_loss_per_basket": 1000}
    r = client.post("/api/autotrade/intents/arm", headers={**H, "X-Operator-Arm-Token": "test-operator-token"}, json=arm)
    assert r.status_code == 503
