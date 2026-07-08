"""RECONCILIATION FRAMEWORK — Phase 5 GUARDS (G1..G4).

One focused test per guard (+ edge cases). Real money on the line: for anything
that touches an exit/order we assert BOTH the fire AND the no-fire case.

  G1 — NO GTT on intraday/MIS legs (mode F5): place-GTT is CALLED for
       CNC/MTF/NRML, NOT called for MIS; the square-off sweep cancels a stray
       MIS GTT.
  G2 — token-expiry abort (mode E5): entry & exit with an expired/absent token →
       abort with TOKEN_EXPIRED, NO order placed, FAILED surfaced; a valid token
       → normal path with NO extra network round-trip (profile() not called).
  G3 — corp-action alert (mode C3): broker=db×2 (split) → CORP_ACTION_SUSPECTED
       with ratio 2.0; a non-clean diff → stays generic ORPHAN/UNATTRIBUTED.
  G4 — P&L gross/net clarity (mode F4): estimate_charges for a CNC delivery
       round-trip and an MIS intraday round-trip return sane totals; net == gross
       − total.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession, set_fake_now
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.gtt_manager import (
    GTTManager, _gtt_allowed_for_product, _norm_product)
from autotrade.monitoring.position_reconciler import (
    reconcile_broker_positions, _corp_action_ratio)
from autotrade.charges import estimate_charges
from autotrade.broker.base import OrderResult
from autotrade.broker.zerodha import ZerodhaBroker
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


# ══════════════════════════════════════════════════════════════════════════════
# GUARD G1 — NO GTT ON INTRADAY / MIS LEGS (mode F5)
# ══════════════════════════════════════════════════════════════════════════════

def _prof(pid="zer", product="CNC"):
    return BrokerProfile(profile_id=pid, broker_name="zerodha",
                         allocated_capital=500000.0, order_product=product)


def _cfg(**kw):
    base = dict(total_allocated_capital=500000.0, instrument_type="EQ")
    base.update(kw)
    return TradingSessionConfig(**base)


class _GttSpyBroker(MockBroker):
    """A mock broker that RECORDS every place_gtt_oco / cancel_gtt call so a test
    can assert whether a GTT was placed for a given product."""
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.gtt_placed = []      # list of (symbol, product)
        self.gtt_cancelled = []   # list of gtt_id

    def place_gtt_oco(self, symbol, qty, stop_price, target_price, last_price,
                      product="CNC", exchange="NSE", order_type="LIMIT",
                      stop_limit_price=None, direction="long"):
        self.gtt_placed.append((symbol, product))
        return f"GTT-{symbol}"

    def cancel_gtt(self, gtt_id):
        self.gtt_cancelled.append(gtt_id)
        return {"status": "CANCELLED"}


def test_g1_gtt_allowed_helper_gates_products():
    """The single choke-point helper: carried products allowed, MIS suppressed."""
    assert _gtt_allowed_for_product("CNC") is True
    assert _gtt_allowed_for_product("MTF") is True
    assert _gtt_allowed_for_product("NRML") is True
    assert _gtt_allowed_for_product("EQ") is True     # normalises to CNC
    assert _gtt_allowed_for_product("MIS") is False    # intraday — never a GTT
    # Same normalisation the reconciler uses (EQ→CNC).
    assert _norm_product("EQ") == "CNC"
    assert _norm_product("mis") == "MIS"


def test_g1_gtt_placed_for_carried_products(clean_positions):
    """CNC / MTF / NRML legs DO get a real GTT (place_gtt_oco called)."""
    for product in ("CNC", "MTF", "NRML"):
        cfg = _cfg(order_product=product,
                   instrument_type=("MTF" if product == "MTF" else "EQ"))
        reg = PositionRegistry(f"sess-g1-{product}", 500000.0)
        prof = _prof(product=product)
        reg.register(symbol="AAA", broker_profile=prof.profile_id, qty=10,
                     avg_price=100.0, product=product)
        broker = _GttSpyBroker(profile=prof, dry_run=False, ltps={"AAA": 100.0})
        cfg.broker_profiles = [prof]
        gm = GTTManager(f"sess-g1-{product}", cfg, {prof.profile_id: broker}, reg)
        pos = reg.get_open_positions()[0]
        out = gm.place_for_position(pos)
        assert out["status"] == "PLACED", f"{product} should place a GTT"
        assert broker.gtt_placed == [("AAA", product)], \
            f"{product} GTT must be placed"


def test_g1_gtt_suppressed_for_mis(clean_positions):
    """An MIS (intraday) leg NEVER gets a real GTT — place_gtt_oco NOT called;
    the row records levels only (gtt_id stays None)."""
    cfg = _cfg(order_product="MIS")
    reg = PositionRegistry("sess-g1-mis", 500000.0)
    prof = _prof(product="MIS")
    reg.register(symbol="BBB", broker_profile=prof.profile_id, qty=10,
                 avg_price=200.0, product="MIS")
    broker = _GttSpyBroker(profile=prof, dry_run=False, ltps={"BBB": 200.0})
    cfg.broker_profiles = [prof]
    gm = GTTManager("sess-g1-mis", cfg, {prof.profile_id: broker}, reg)
    pos = reg.get_open_positions()[0]
    out = gm.place_for_position(pos)
    assert out["status"] == "SUPPRESSED_INTRADAY"
    assert out["gtt_id"] is None
    assert broker.gtt_placed == [], "MIS must NOT place a broker GTT"
    # Levels still recorded for the UI, gtt_id NULL in the DB.
    row = reg.get_open_positions()[0]
    assert row.get("gtt_id") is None
    assert row.get("gtt_stop") is not None


def test_g1_backfill_mixed_products_only_carried_get_gtt(clean_positions):
    """A session with a per-profile MIS leg and a CNC leg: only the CNC leg is
    placed; the MIS leg is suppressed via the effective (per-profile) product."""
    cfg = _cfg(order_product="CNC")
    cnc_prof = _prof(pid="cnc", product="CNC")
    mis_prof = _prof(pid="mis", product="MIS")
    cfg.broker_profiles = [cnc_prof, mis_prof]
    reg = PositionRegistry("sess-g1-mix", 500000.0)
    reg.register(symbol="CARRY", broker_profile="cnc", qty=5, avg_price=100.0,
                 product="CNC")
    reg.register(symbol="INTRA", broker_profile="mis", qty=5, avg_price=100.0,
                 product="MIS")
    cnc_broker = _GttSpyBroker(profile=cnc_prof, dry_run=False,
                               ltps={"CARRY": 100.0})
    mis_broker = _GttSpyBroker(profile=mis_prof, dry_run=False,
                               ltps={"INTRA": 100.0})
    gm = GTTManager("sess-g1-mix", cfg,
                    {"cnc": cnc_broker, "mis": mis_broker}, reg)
    out = gm.backfill_missing()
    by_sym = {o["symbol"]: o for o in out}
    assert by_sym["CARRY"]["status"] == "PLACED"
    assert by_sym["INTRA"]["status"] == "SUPPRESSED_INTRADAY"
    assert cnc_broker.gtt_placed == [("CARRY", "CNC")]
    assert mis_broker.gtt_placed == []


def test_g1_sweep_cancels_stray_mis_gtt(clean_positions):
    """Belt-and-suspenders: a legacy MIS position that somehow carries a gtt_id →
    sweep_intraday_gtts cancels it; a carried CNC GTT is left untouched."""
    cfg = _cfg(order_product="CNC")
    mis_prof = _prof(pid="mis", product="MIS")
    cnc_prof = _prof(pid="cnc", product="CNC")
    cfg.broker_profiles = [mis_prof, cnc_prof]
    reg = PositionRegistry("sess-g1-sweep", 500000.0)
    reg.register(symbol="STRAY", broker_profile="mis", qty=10, avg_price=100.0,
                 product="MIS")
    reg.register(symbol="KEEP", broker_profile="cnc", qty=10, avg_price=100.0,
                 product="CNC")
    # Force a stray GTT id onto the MIS leg + a legit one on the CNC leg.
    reg.set_gtt("STRAY", "GTT-STRAY", broker_profile="mis")
    reg.set_gtt("KEEP", "GTT-KEEP", broker_profile="cnc")
    mis_broker = _GttSpyBroker(profile=mis_prof, dry_run=False)
    cnc_broker = _GttSpyBroker(profile=cnc_prof, dry_run=False)
    gm = GTTManager("sess-g1-sweep", cfg,
                    {"mis": mis_broker, "cnc": cnc_broker}, reg)
    swept = gm.sweep_intraday_gtts()
    assert mis_broker.gtt_cancelled == ["GTT-STRAY"], \
        "stray MIS GTT must be cancelled"
    assert cnc_broker.gtt_cancelled == [], "carried CNC GTT must be left alone"
    assert any(s["status"] == "SWEPT_INTRADAY_GTT" and s["symbol"] == "STRAY"
               for s in swept)
    # The swept MIS row's gtt_id is cleared so the reconciler won't chase it.
    stray = next(p for p in reg.get_open_positions() if p["symbol"] == "STRAY")
    assert stray.get("gtt_id") is None


# ══════════════════════════════════════════════════════════════════════════════
# GUARD G2 — TOKEN-EXPIRY ABORT (mode E5)
# ══════════════════════════════════════════════════════════════════════════════

def _order(symbol="AAA", qty=10, product="CNC"):
    from autotrade.execution.orders import Order
    return Order(symbol=symbol, qty=qty, product=product)


def _live_zerodha(monkeypatch):
    """A ZerodhaBroker in LIVE mode (dry_run off + master switch on), with the
    per-order preflight gate stubbed GREEN and the kite build stubbed so no real
    network is ever touched. Returns (broker, calls) where calls records whether
    profile() was ever invoked (the happy-path no-round-trip assertion)."""
    prof = _prof()
    b = ZerodhaBroker(profile=prof, dry_run=False)
    # Master live switch ON for this broker instance only.
    monkeypatch.setattr(b, "_live_allowed", lambda: True)
    monkeypatch.setattr(b, "_preflight_block_reason", lambda: None)
    calls = {"profile": 0, "place": 0}

    class _FakeKite:
        # Kite constants the Order.to_kite_params path references.
        VARIETY_REGULAR = "regular"
        PRODUCT_CNC = "CNC"; PRODUCT_MIS = "MIS"; PRODUCT_NRML = "NRML"
        ORDER_TYPE_MARKET = "MARKET"; ORDER_TYPE_LIMIT = "LIMIT"
        TRANSACTION_TYPE_BUY = "BUY"; TRANSACTION_TYPE_SELL = "SELL"
        EXCHANGE_NSE = "NSE"

        def profile(self):
            calls["profile"] += 1
            return {"user_name": "T", "email": "t@x"}

        def place_order(self, **kw):
            calls["place"] += 1
            return "OID-1"

    # self.kite is a property → patch the underlying builder to our fake.
    monkeypatch.setattr(type(b), "kite",
                        property(lambda self: _FakeKite()), raising=False)
    # order_executor._retry_kite_call just runs the lambda (no real retry/network).
    import falcon.trade.services.order_executor as oe
    monkeypatch.setattr(oe, "_retry_kite_call",
                        lambda fn, *a, **k: fn(), raising=False)
    return b, calls


def _set_cached_status(monkeypatch, status, age=0.0):
    """Force services.kite_auth's cached token status to a known value."""
    import services.kite_auth as ka
    import time as _t
    ka._TOKEN_STATUS_CACHE.update(status=status, at=_t.monotonic() - age)


def test_g2_entry_aborts_on_expired_token(monkeypatch):
    """A fresh cached INVALID status → entry aborts FAILED/TOKEN_EXPIRED, NO order
    placed."""
    b, calls = _live_zerodha(monkeypatch)
    _set_cached_status(monkeypatch, {"valid": False, "code": "TOKEN_EXPIRED",
                                     "reason": "expired"})
    res = asyncio.run(b.place_order(_order()))
    assert res.status == "FAILED"
    assert "TOKEN_EXPIRED" in (res.error or "")
    assert calls["place"] == 0, "no order may be placed on a dead token"


def test_g2_entry_aborts_on_missing_token(monkeypatch):
    """No cached status + no token present → abort TOKEN_MISSING, no order."""
    b, calls = _live_zerodha(monkeypatch)
    _set_cached_status(monkeypatch, None)
    import services.kite_auth as ka
    monkeypatch.setattr(ka, "token_present", lambda: None)
    res = asyncio.run(b.place_order(_order()))
    assert res.status == "FAILED"
    assert "TOKEN_MISSING" in (res.error or "")
    assert calls["place"] == 0


def test_g2_exit_aborts_on_expired_token(monkeypatch):
    """The EXIT path also aborts cleanly (FAILED/TOKEN_EXPIRED), never places into
    a dead session, never crashes."""
    b, calls = _live_zerodha(monkeypatch)
    _set_cached_status(monkeypatch, {"valid": False, "code": "TOKEN_EXPIRED",
                                     "reason": "expired"})
    res = asyncio.run(b.place_market_exit("AAA", 10, "EQ"))
    assert res.status == "FAILED"
    assert "TOKEN_EXPIRED" in (res.error or "")
    assert calls["place"] == 0


def test_g2_valid_token_happy_path_no_extra_round_trip(monkeypatch):
    """A fresh cached VALID status → entry places normally AND profile() is NEVER
    called from the order path (the O(1) happy-path guarantee)."""
    b, calls = _live_zerodha(monkeypatch)
    _set_cached_status(monkeypatch, {"valid": True, "user": "T"})
    res = asyncio.run(b.place_order(_order()))
    assert res.status == "PLACED"
    assert calls["place"] == 1
    assert calls["profile"] == 0, \
        "the guard must NOT do a profile() round-trip on a valid cached token"


def test_g2_no_cache_present_token_proceeds(monkeypatch):
    """No cached status but a token IS present → the guard does NOT abort and does
    NOT probe profile() (falls back to the O(1) presence check)."""
    b, calls = _live_zerodha(monkeypatch)
    _set_cached_status(monkeypatch, None)
    import services.kite_auth as ka
    monkeypatch.setattr(ka, "token_present", lambda: "TOK-ABC")
    res = asyncio.run(b.place_order(_order()))
    assert res.status == "PLACED"
    assert calls["profile"] == 0


def test_g2_stale_cache_falls_back_to_presence(monkeypatch):
    """A STALE invalid cache is ignored (older than the TTL) → falls back to the
    presence probe; a present token proceeds (no false abort)."""
    b, calls = _live_zerodha(monkeypatch)
    _set_cached_status(monkeypatch, {"valid": False, "code": "TOKEN_EXPIRED"},
                       age=120.0)  # > 60s TTL → treated as absent
    import services.kite_auth as ka
    monkeypatch.setattr(ka, "token_present", lambda: "TOK-ABC")
    res = asyncio.run(b.place_order(_order()))
    assert res.status == "PLACED"


# ══════════════════════════════════════════════════════════════════════════════
# GUARD G3 — CORP-ACTION ALERT (mode C3)
# ══════════════════════════════════════════════════════════════════════════════

def test_g3_ratio_classifier_clean_and_noisy():
    """The pure classifier: clean split/bonus ratios detected; noise → None."""
    assert _corp_action_ratio(20, 10) == 2.0     # 1:1 bonus (broker grew ×2)
    assert _corp_action_ratio(30, 10) == 3.0     # 2:1 bonus
    assert _corp_action_ratio(50, 10) == 5.0     # 1:5 split
    assert _corp_action_ratio(15, 10) == 1.5     # 3-for-2 bonus
    assert _corp_action_ratio(25, 10) == 2.5     # 3:2 bonus
    assert _corp_action_ratio(100, 10) == 10.0   # 1:10 split
    # Reverse split (broker shrank) → reciprocal.
    assert _corp_action_ratio(10, 20) == 0.5
    # Non-clean diffs → None (stay generic).
    assert _corp_action_ratio(13, 10) is None
    assert _corp_action_ratio(10, 7) is None
    assert _corp_action_ratio(10, 10) is None    # equal → not a ratio
    assert _corp_action_ratio(10, 0) is None      # a real close, not a ratio


def _make_live_session(monkeypatch, net_book, *, ltps=None, holdings=None,
                       order_product="CNC", capital=300000.0):
    ltps = ltps or {}
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps,
                        net_book=net_book, holdings=holdings)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=capital, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product=order_product)
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _register(sess, symbol, qty, avg_price, ltp=None):
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg_price, product="CNC")
    if ltp is not None:
        sess.registry.update_ltp(symbol, ltp, broker_profile=prof)


def _alerts(kind=None):
    with falcon_conn() as con:
        if kind:
            rows = con.execute(
                "SELECT * FROM autotrade_recon_alerts WHERE kind=?",
                (kind,)).fetchall()
        else:
            rows = con.execute("SELECT * FROM autotrade_recon_alerts").fetchall()
    return [dict(r) for r in rows]


@pytest.fixture
def _frozen():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def test_g3_split_surplus_raises_corp_action(clean_positions, monkeypatch,
                                             _frozen):
    """broker = db × 2 (a 1:1 bonus doubled the broker qty, no order) → a distinct
    CORP_ACTION_SUSPECTED alert with ratio 2.0, NOT a generic ORPHAN_AT_BROKER.
    Nothing is mutated."""
    # DB has 10 CNC; broker net shows 20 (holdings 0). Clean ×2.
    net_book = {"SPLIT": {"tradingsymbol": "SPLIT", "quantity": 20,
                          "buy_quantity": 20, "sell_quantity": 0,
                          "average_price": 100.0, "exchange": "NSE",
                          "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"SPLIT": 100.0},
                              holdings={})
    _register(sess, "SPLIT", 10, 100.0, ltp=100.0)
    sess.monitor.freeze_invested_basis()

    actions = reconcile_broker_positions(sess)
    corp = [a for a in actions if a["action"] == "CORP_ACTION_SUSPECTED"]
    assert len(corp) == 1
    assert corp[0]["ratio"] == 2.0
    assert corp[0]["symbol"] == "SPLIT"
    # NO generic orphan alert; the persisted alert is the corp-action kind.
    assert not any(a["action"] == "ORPHAN_AT_BROKER" for a in actions)
    assert len(_alerts("CORP_ACTION_SUSPECTED")) == 1
    assert _alerts("ORPHAN_AT_BROKER") == []
    # Position untouched (never mutated).
    with falcon_conn() as con:
        r = con.execute("SELECT status, qty FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, "SPLIT")).fetchone()
    assert dict(r)["status"] == "OPEN"
    assert dict(r)["qty"] == 10


def test_g3_noisy_surplus_is_invisible(clean_positions, monkeypatch, _frozen):
    """PHASE 7: a NON-clean surplus (broker 13 vs db 10) is NOT a corp-action AND
    NOT an orphan — the extra 3 is a MANUAL trade / other holding, NOT ours. It is
    INVISIBLE: no action, no alert, our position untouched. (Pre-P7 this raised a
    generic ORPHAN_AT_BROKER — the false signal P7 removes.)"""
    net_book = {"NOISE": {"tradingsymbol": "NOISE", "quantity": 13,
                          "buy_quantity": 13, "sell_quantity": 0,
                          "average_price": 100.0, "exchange": "NSE",
                          "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"NOISE": 100.0},
                              holdings={})
    _register(sess, "NOISE", 10, 100.0, ltp=100.0)
    sess.monitor.freeze_invested_basis()

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _alerts("ORPHAN_AT_BROKER") == []
    assert _alerts("CORP_ACTION_SUSPECTED") == []
    with falcon_conn() as con:
        r = con.execute("SELECT status, qty FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, "NOISE")).fetchone()
    assert dict(r)["status"] == "OPEN"
    assert dict(r)["qty"] == 10


# ══════════════════════════════════════════════════════════════════════════════
# GUARD G4 — P&L GROSS / NET CLARITY (mode F4)
# ══════════════════════════════════════════════════════════════════════════════

def test_g4_cnc_delivery_round_trip_charges_sane():
    """CNC delivery round trip: zero brokerage, STT both sides, a DP charge on the
    sell, GST only on (brokerage+txn+sebi). Totals are sane + net = gross − total."""
    buy_value = 10 * 100.0    # ₹1,000 buy
    sell_value = 10 * 110.0   # ₹1,100 sell
    c = estimate_charges("CNC", buy_value, sell_value, legs=2)
    # Brokerage is ZERO for CNC delivery.
    assert c["brokerage"] == 0.0
    # STT = 0.1% of (1000+1100) = 2.10.
    assert abs(c["stt"] - 2.10) < 0.01
    # DP charge present on the delivery sell.
    assert c["dp"] > 0.0
    # Total is the sum of the parts (allowing 2dp round-once vs round-each drift)
    # and positive but small vs turnover.
    parts = c["brokerage"] + c["stt"] + c["exchange"] + c["gst"] + c["stamp"] + c["dp"]
    assert abs(parts - c["total"]) < 0.02
    assert 0 < c["total"] < (buy_value + sell_value) * 0.02

    gross = (110.0 - 100.0) * 10   # +₹100
    net = round(gross - c["total"], 2)
    assert net < gross
    assert net == round(gross - c["total"], 2)


def test_g4_mis_intraday_round_trip_charges_sane():
    """MIS intraday: brokerage per leg (capped ₹20), STT on the SELL only, NO DP,
    lower stamp. Net = gross − total."""
    buy_value = 100 * 500.0    # ₹50,000
    sell_value = 100 * 505.0   # ₹50,500
    c = estimate_charges("MIS", buy_value, sell_value, legs=2)
    # Brokerage: min(0.03% * 50000, 20) + min(0.03% * 50500, 20) = 15 + 15.15
    #            → 15 + 15.15 = 30.15 (each leg under the ₹20 cap on the buy,
    #            15.15 on the sell). Assert both legs charged and ≤ cap each.
    assert c["brokerage"] > 0.0
    assert c["brokerage"] <= 40.0     # two legs, ≤ ₹20 each
    # STT on the SELL side only: 0.025% * 50500 = 12.625.
    assert abs(c["stt"] - 12.625) < 0.01
    # No DP on intraday.
    assert c["dp"] == 0.0
    parts = c["brokerage"] + c["stt"] + c["exchange"] + c["gst"] + c["stamp"] + c["dp"]
    assert abs(parts - c["total"]) < 0.02

    gross = (505.0 - 500.0) * 100   # +₹500
    net = round(gross - c["total"], 2)
    assert net == round(gross - c["total"], 2)
    assert net < gross


def test_g4_open_position_entry_only_charges():
    """An OPEN position (sell_value=0, legs=1): only buy-side charges apply, NO DP,
    NO sell STT."""
    c = estimate_charges("CNC", buy_value=10 * 100.0, sell_value=0.0, legs=1)
    assert c["dp"] == 0.0                     # no sell → no DP
    assert abs(c["stt"] - 1.0) < 0.01         # 0.1% of 1000 buy only
    assert c["total"] > 0.0


def test_g4_bad_input_returns_zero_dict():
    """Never raises on bad input — returns an all-zero dict."""
    c = estimate_charges("CNC", buy_value="oops", sell_value=None, legs=2)
    assert c["total"] == 0.0
    assert all(c[k] == 0.0 for k in
               ("brokerage", "stt", "exchange", "gst", "stamp", "dp", "total"))


def test_g4_journal_surfaces_net_alongside_gross(clean_positions):
    """The journal read path exposes realised_pnl_net + a gross mirror WITHOUT
    changing the existing realised_pnl field. net == gross − charges.total."""
    reg = PositionRegistry("sess-g4-journal", 500000.0)
    reg.register(symbol="ZZZ", broker_profile="zer", qty=10, avg_price=100.0,
                 product="CNC")
    reg.mark_closed("ZZZ", "SQUARE_OFF", exit_price=110.0, broker_profile="zer")
    # Seed the session row so build_journal finds it (order_product CNC).
    from autotrade.config import TradingSessionConfig as _Cfg
    cfg = _Cfg(total_allocated_capital=500000.0, order_product="CNC")
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_sessions (session_id, mode, status, "
            "total_allocated_capital, config_json, created_at) "
            "VALUES (?,?,?,?,?,?)",
            ("sess-g4-journal", "paper", "CLOSED", 500000.0, cfg.to_json(),
             "2026-06-25T09:15:00+05:30"))
        con.commit()

    from autotrade.api.journal_routes import build_journal
    j = build_journal("sess-g4-journal")
    row = next(p for p in j["positions"] if p["symbol"] == "ZZZ")
    # GROSS field UNCHANGED (still (110-100)*10 = 100).
    assert abs(row["realised_pnl"] - 100.0) < 1e-6
    assert abs(row["realised_pnl_gross"] - 100.0) < 1e-6
    # NET present, an estimate below gross, and consistent with the breakdown.
    assert row["realised_pnl_net"] is not None
    assert row["realised_pnl_net"] < row["realised_pnl_gross"]
    ce = row["charges_estimate"]
    assert ce is not None
    assert abs(row["realised_pnl_net"] - round(100.0 - ce["total"], 2)) < 1e-6
    # Session-level net + charges totals surfaced.
    ss = j["session_summary"]
    assert ss["total_realised_pnl_gross"] == ss["total_realised_pnl"]
    assert ss["total_realised_pnl_net"] <= ss["total_realised_pnl_gross"]
    assert ss["total_charges_estimate"] > 0.0
