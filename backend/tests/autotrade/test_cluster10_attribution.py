"""SPRINT CLUSTER 10 — ORDER-ID / CLIENT-TAG attribution everywhere.

The operator's chosen path: make the shared / multi-account / fungible book safe by
ATTRIBUTION (identity), NOT by mandating a dedicated account. Each fix below ships a
MUTATION-VERIFIED test (passes WITH the fix, FAILS on the stated revert).

  ITEM 1 — TAG-COVERAGE: EVERY order-placing path mints + persists + transmits a
           Falcon client_order_id / compact tag (8 paths, parametrized).
  ITEM 2 — TAG-PRIMARY reconciliation: an exit we placed (our tag rode on it) whose
           broker order-id we never recorded is STILL attributed to us BY TAG; a
           foreign same-symbol fill stays invisible.
  ITEM 3 — MANUAL-CONFLICT by attribution: OUR qty is defined by OUR fills; a manual
           same-symbol buy can never inflate our tracked position or our exit.
  ITEM 4 — PER-BROKER tag transmission: an adapter that supports a tag field (Upstox)
           transmits it; OUR-side coid persistence works regardless of the broker.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade import order_ledger
from autotrade.order_ledger import compact_tag
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.execution.orders import Order
from autotrade.monitoring.registry import PositionRegistry, our_held_at_broker
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade.monitoring.gtt_manager import GTTManager
from autotrade.monitoring.exit_poller import (cancel_and_retry_exit,
                                              slice_and_confirm_exit)
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from autotrade.monitoring.alert_monitor import detect_naked_positions
from autotrade.session import TradingSession, _exit_single_position, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


@pytest.fixture(autouse=True)
def _clean_ledger():
    """The shared conftest.clean_positions does NOT wipe autotrade_order_events, and
    the MockBroker reuses "exit-<SYM>" as the broker order-id — so without this the
    UNIQUE(broker_order_id,event_type,profile) dedup would carry a PRIOR test's coid
    across tests (a test artifact; real broker order-ids are unique). Wipe the ledger
    per test. Isolation-guarded: only the temp test DB."""
    with falcon_conn() as con:
        _dbs = con.execute("PRAGMA database_list").fetchall()
        _main = next((d[2] for d in _dbs if d[1] == "main"), "")
        if "kanida_autotrade_test_" not in str(_main):
            raise RuntimeError("refusing to wipe a non-test DB: %s" % _main)
        con.execute("DELETE FROM autotrade_order_events")
        con.commit()
    yield


def _prof(pid="zer", product="CNC", instrument_type="EQ"):
    return BrokerProfile(profile_id=pid, broker_name="zerodha",
                         allocated_capital=300000.0, order_product=product,
                         instrument_type=instrument_type)


def _patch_brokers(monkeypatch, factory):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = created.get(profile.profile_id) or factory(profile)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _events(session_id, symbol=None, source=None):
    evs = order_ledger.get_events(session_id, symbol)
    if source is not None:
        evs = [e for e in evs if e.get("source") == source]
    return evs


def _ledger_coid(session_id, symbol, source):
    """A non-null client_order_id persisted in the ledger for this path's source."""
    for e in _events(session_id, symbol, source):
        if e.get("client_order_id"):
            return e["client_order_id"]
    return None


def _pos(session_id, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT * FROM autotrade_positions WHERE session_id=? AND symbol=?",
            (session_id, symbol)).fetchone()
    return dict(r) if r else None


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 1 — TAG-COVERAGE across ALL 8 order-placing paths (parametrized).
#
# Each runner drives ONE real code path and returns a result dict:
#   ledger_coid       — the client_order_id PERSISTED IN THE LEDGER for this path
#                       (the coid whose compact_tag is our ownership key).
#   identifier_reached — proof the identifier reached the broker payload:
#                        entry → Order.tag; exit → the threaded client_order_id;
#                        slm → the client_tag; gtt → the ledger row's broker_order_id
#                        equals the gtt_id (Kite drops leg tags — attributed by id).
#   broker_tag / expected_tag — the compact tag on the broker payload where the API
#                        SUPPORTS a tag (entry legs + SL-M); None for exit (mock does
#                        not build kite params) and GTT (Kite strips the leg tag).
# ══════════════════════════════════════════════════════════════════════════════

def _run_entry_place_one(monkeypatch):
    ltps = {"AAA": 100.0}
    created = _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=False, ltps=ltps))
    seed_signals([("AAA", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    asyncio.run(sess._fire_entries())
    broker = next(iter(created.values()))
    order = broker.placed[0]
    return {
        "ledger_coid": _ledger_coid(sess.session_id, "AAA", "entry"),
        "identifier_reached": order.tag == compact_tag(order.client_order_id),
        "broker_tag": order.tag,
        "expected_tag": compact_tag(order.client_order_id),
    }


def _run_entry_iceberg_child(monkeypatch):
    ltps = {"AAA": 100.0}
    created = _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=False, ltps=ltps))
    seed_signals([("AAA", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC", iceberg_enabled=True,
                               iceberg_slice_qty=300)
    sess = TradingSession.create(cfg, mode="live")
    asyncio.run(sess._fire_entries())
    broker = next(iter(created.values()))
    assert len(broker.placed) >= 2, "iceberg did not slice into children"
    order = broker.placed[0]
    return {
        "ledger_coid": _ledger_coid(sess.session_id, "AAA", "entry_iceberg"),
        "identifier_reached": order.tag == compact_tag(order.client_order_id),
        "broker_tag": order.tag,
        "expected_tag": compact_tag(order.client_order_id),
    }


def _live_exit_session(monkeypatch, symbol="AAA", qty=100, *, net=None, cfg=None):
    ltps = {symbol: 100.0}
    created = _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=False, ltps=ltps, net_positions=net))
    sess = TradingSession.create(
        cfg or TradingSessionConfig(total_allocated_capital=300000.0,
                                    order_product="CNC"),
        mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=100.0, product="CNC", instrument_type="EQ",
                           exchange="NSE", client_order_id="FAL-entry-" + symbol)
    sess.registry.update_ltp(symbol, 100.0, broker_profile=prof)
    return sess, next(iter(created.values())), prof


def _run_exit_single(monkeypatch):
    sess, broker, prof = _live_exit_session(monkeypatch)
    pos = sess.registry.get_open_positions()[0]
    asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=None, kite_product="CNC", exec_cfg=sess.config))
    threaded = broker.exit_calls[-1]["client_order_id"]
    row = _pos(sess.session_id, "AAA")
    return {
        "ledger_coid": _ledger_coid(sess.session_id, "AAA", "exit"),
        "identifier_reached": bool(threaded) and threaded == row["exit_client_order_id"],
        "broker_tag": None, "expected_tag": None,
    }


def _run_exit_iceberg_child(monkeypatch):
    sess, broker, prof = _live_exit_session(monkeypatch, qty=1000)
    parent = order_ledger.make_client_order_id(sess.session_id, "AAA", attempt=1)
    asyncio.run(slice_and_confirm_exit(
        session_id=sess.session_id, symbol="AAA", total_qty=1000,
        legs=[300, 300, 300, 100], broker=broker, registry=sess.registry,
        close_reason="KILL_SWITCH", broker_profile=prof, direction="long",
        instrument_type="EQ", kite_product="CNC", parent_exit_coid=parent))
    threaded = broker.exit_calls[0]["client_order_id"]
    return {
        "ledger_coid": _ledger_coid(sess.session_id, "AAA", "exit"),
        "identifier_reached": bool(threaded) and threaded.startswith(parent),
        "broker_tag": None, "expected_tag": None,
    }


def _run_kill_first_exit(monkeypatch):
    cfg = TradingSessionConfig(total_allocated_capital=300000.0,
                               order_product="CNC", kill_switch_enabled=True)
    sess, broker, prof = _live_exit_session(monkeypatch, cfg=cfg)
    ks = KillSwitchExecutor(sess.session_id, sess.config, sess.brokers,
                            sess.registry, sess.gtt_manager)
    asyncio.run(ks.fire("cluster10-kill"))
    threaded = broker.exit_calls[-1]["client_order_id"]
    row = _pos(sess.session_id, "AAA")
    return {
        "ledger_coid": _ledger_coid(sess.session_id, "AAA", "exit"),
        "identifier_reached": bool(threaded) and threaded == row["exit_client_order_id"],
        "broker_tag": None, "expected_tag": None,
    }


def _run_kill_retry(monkeypatch):
    sess, broker, prof = _live_exit_session(monkeypatch)
    asyncio.run(cancel_and_retry_exit(
        session_id=sess.session_id, symbol="AAA", order_id="stale-1", qty=100,
        broker=broker, registry=sess.registry, close_reason="EXIT_RETRY",
        max_retries=2, kite_product="CNC", direction="long",
        instrument_type="EQ", broker_profile=prof))
    threaded = broker.exit_calls[-1]["client_order_id"]
    row = _pos(sess.session_id, "AAA")
    return {
        "ledger_coid": _ledger_coid(sess.session_id, "AAA", "exit"),
        "identifier_reached": bool(threaded) and threaded == row["exit_client_order_id"],
        "broker_tag": None, "expected_tag": None,
    }


def _run_gtt_place(monkeypatch):
    coid = "FAL-entry-GGG"
    prof = _prof(product="CNC")
    broker = MockBroker(profile=prof, dry_run=False, ltps={"GGG": 100.0})
    broker.place_gtt_oco = lambda **kw: "GTT-123"      # live GTT id
    reg = PositionRegistry("sess-c10-gtt", 100000.0)
    reg.register(symbol="GGG", broker_profile="zer", qty=100, avg_price=100.0,
                 product="CNC", instrument_type="EQ", exchange="NSE",
                 client_order_id=coid)
    cfg = TradingSessionConfig(total_allocated_capital=100000.0,
                               order_product="CNC")
    gm = GTTManager("sess-c10-gtt", cfg, {"zer": broker}, reg)
    pos = reg.get_open_positions()[0]
    out = gm.place_for_position(pos)
    assert out["gtt_id"] == "GTT-123"
    ev = [e for e in _events("sess-c10-gtt", "GGG", "gtt")]
    ledger_coid = ev[0]["client_order_id"] if ev else None
    return {
        "ledger_coid": ledger_coid,
        # Kite strips the GTT leg tag → attribution is by the recorded gtt_id, which
        # the ledger event maps to OUR client_order_id.
        "identifier_reached": bool(ev) and ev[0]["broker_order_id"] == "GTT-123",
        "broker_tag": None, "expected_tag": None,
    }


def _run_slm_place(monkeypatch):
    coid = "FAL-entry-MMM"
    prof = _prof(product="MIS")
    broker = MockBroker(profile=prof, dry_run=False, ltps={"MMM": 100.0})
    reg = PositionRegistry("sess-c10-slm", 100000.0)
    reg.register(symbol="MMM", broker_profile="zer", qty=100, avg_price=100.0,
                 product="MIS", instrument_type="EQ", exchange="NSE",
                 client_order_id=coid)
    cfg = TradingSessionConfig(total_allocated_capital=100000.0,
                               order_product="MIS")
    gm = GTTManager("sess-c10-slm", cfg, {"zer": broker}, reg)
    pos = reg.get_open_positions()[0]
    slm_id = gm._place_protective_slm(pos, 95.0)
    assert slm_id
    client_tag = broker.slm_orders[-1]["client_tag"]
    return {
        "ledger_coid": _ledger_coid("sess-c10-slm", "MMM", "slm"),
        "identifier_reached": client_tag == compact_tag(coid),
        "broker_tag": client_tag,
        "expected_tag": compact_tag(coid),
    }


_PATH_RUNNERS = {
    "1_entry_place_one": _run_entry_place_one,
    "2_entry_iceberg_child": _run_entry_iceberg_child,
    "3_exit_single": _run_exit_single,
    "4_exit_iceberg_child": _run_exit_iceberg_child,
    "5_kill_first_exit": _run_kill_first_exit,
    "6_kill_retry": _run_kill_retry,
    "7_gtt_place": _run_gtt_place,
    "8_slm_place": _run_slm_place,
}


@pytest.mark.parametrize("path", list(_PATH_RUNNERS))
def test_item1_every_place_path_carries_tag_and_persists_coid(
        clean_positions, monkeypatch, path):
    """ITEM 1 — EACH of the 8 order-placing paths produces an order carrying OUR
    identifier AND persists a client_order_id IN THE LEDGER.

    MUTATION (drop the tag / coid on any ONE path → that path FAILS), e.g.:
      * path 1: remove `order.tag = compact_tag(client_order_id)` in
        session._place_one → broker_tag != expected_tag → FAILS.
      * path 2: remove `child_tag = compact_tag(child_coid)` in session._place_iceberg
        → FAILS.
      * path 3/5/6: drop `client_order_id=exit_coid` on the place_market_exit call /
        `client_order_id=...` on confirm_exit → identifier_reached / ledger_coid None
        → FAILS.
      * path 4: drop `client_order_id=child_coid` on the slice EXIT_PLACED append →
        ledger_coid None → FAILS.
      * path 7: remove the source='gtt' ledger append in gtt_manager.place_for_position
        → ledger_coid None → FAILS.
      * path 8: revert `client_tag` on place_protective_slm / remove the source='slm'
        ledger append → FAILS.
    """
    res = _PATH_RUNNERS[path](monkeypatch)
    # ALL 8 paths: a client_order_id is persisted in the durable ledger.
    assert res["ledger_coid"], f"{path}: no client_order_id in the ledger"
    assert str(res["ledger_coid"]).startswith("FAL-"), (path, res["ledger_coid"])
    # ALL 8 paths: the identifier reached the broker payload (tag / coid / gtt-id).
    assert res["identifier_reached"], f"{path}: identifier did not reach the broker"
    # Where the broker API SUPPORTS a tag, the payload carries compact_tag(coid).
    if res["expected_tag"] is not None:
        assert res["broker_tag"] == res["expected_tag"], (
            path, res["broker_tag"], res["expected_tag"])
        assert len(res["broker_tag"]) <= 20 and res["broker_tag"].isalnum()


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 2 — TAG-PRIMARY reconciliation + naked-detector.
# ══════════════════════════════════════════════════════════════════════════════

def _recon_session(monkeypatch, *, net_book, orders, holdings=None):
    def factory(p):
        return MockBroker(profile=p, dry_run=False, ltps={"AAA": 95.0},
                          net_book=net_book, orders=orders,
                          holdings=holdings or {})
    _patch_brokers(monkeypatch, factory)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def test_item2_reconciler_attributes_our_tagged_fill_foreign_invisible(
        clean_positions, monkeypatch):
    """A broker orderbook with OUR-tagged exit fill + a FOREIGN same-symbol fill:
    the reconciler attributes ONLY our fill (closes on it via RECONCILED_EXIT_BY_TAG),
    the foreign fill is invisible — EVEN THOUGH we never recorded the broker exit
    order-id on the row (only the exit_client_order_id / tag).

    MUTATION REVERT: remove the `_orderbook_exit_evidence_by_tag(...)` call from the
    deficit loop in position_reconciler.reconcile_broker_positions → our exit is not
    attributed (exit_order_id is NULL) → the deficit is unresolved →
    UNATTRIBUTED_CLOSE, the row stays OPEN → the CLOSED assertion FAILS."""
    exit_coid = order_ledger.make_client_order_id("x", "AAA", attempt=1)
    our_tag = compact_tag(exit_coid)
    # Broker fully flat for AAA today (quantity 0) but NO same-day sell volume in the
    # net row (so the fully-flat CLOSED_EXTERNAL_FLAT path does NOT fire) — the ONLY
    # way to attribute the close is our TAG in the orderbook.
    net_book = {"AAA": {"tradingsymbol": "AAA", "quantity": 0,
                        "exchange": "NSE", "product": "CNC"}}
    orders = [
        {"order_id": "OURS-1", "status": "COMPLETE", "filled_quantity": 100,
         "average_price": 95.0, "transaction_type": "SELL",
         "tradingsymbol": "AAA", "tag": our_tag},
        {"order_id": "FOREIGN-9", "status": "COMPLETE", "filled_quantity": 50,
         "average_price": 94.0, "transaction_type": "SELL",
         "tradingsymbol": "AAA", "tag": "manual-desk"},
    ]
    sess = _recon_session(monkeypatch, net_book=net_book, orders=orders)
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="AAA", broker_profile=prof, qty=100,
                           avg_price=100.0, product="CNC", instrument_type="EQ",
                           exchange="NSE")
    # We PLACED the exit (tag rode on it) but never recorded the broker order-id:
    # only the exit_client_order_id is persisted; exit_order_id stays NULL.
    sess.registry.set_exit_client_order_id("AAA", exit_coid, broker_profile=prof)

    actions = reconcile_broker_positions(sess)
    closed = [a for a in actions if a["symbol"] == "AAA"
              and a["action"] == "CLOSED_RECONCILED"]
    assert closed, actions
    assert closed[0]["close_reason"] == "RECONCILED_EXIT_BY_TAG"
    assert closed[0]["exit_order_id"] == "OURS-1"          # our fill, not the foreign
    assert abs(float(closed[0]["exit_price"]) - 95.0) < 1e-6
    # The FOREIGN fill was never attributed to us.
    assert not any(str(a.get("exit_order_id")) == "FOREIGN-9" for a in actions)
    assert _pos(sess.session_id, "AAA")["status"] == "CLOSED"


def test_item2_naked_detector_does_not_false_fire_on_foreign(
        clean_positions, monkeypatch):
    """A real broker position we have NO ownership evidence for (its orderbook order
    carries a FOREIGN tag, no CLOSED row of ours) is a manual holding → the naked
    detector stays SILENT (P7 invisibility). REVERT: n/a — this asserts a foreign
    same-symbol fill never false-fires the pager (the invisibility ITEM 2 relies on;
    the paired positive case — OUR tag DOES page — is in test_cluster6_naked)."""
    net_book = {"FOR": {"tradingsymbol": "FOR", "quantity": 100,
                        "exchange": "NSE", "product": "CNC"}}
    orders = [{"order_id": "F1", "status": "COMPLETE", "filled_quantity": 100,
               "average_price": 50.0, "transaction_type": "BUY",
               "tradingsymbol": "FOR", "tag": "someone-else"}]
    sess = _recon_session(monkeypatch, net_book=net_book, orders=orders)
    naked = detect_naked_positions(sess)
    assert naked == [], naked


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 3 — MANUAL-CONFLICT resolved by attribution (our qty = OUR fills).
# ══════════════════════════════════════════════════════════════════════════════

def test_item3_our_held_clamps_to_our_tracked_qty(clean_positions):
    """our_held_at_broker never exceeds OUR tracked qty even when the ACCOUNT net is
    inflated by a manual buy: our 100 + a manual 50 → broker net 150, our_held == 100.

    MUTATION REVERT: in registry.our_held_at_broker change
    `max(0, min(oq, net - other))` to `max(0, net - other)` (drop the `min(oq, ...)`
    clamp to our tracked qty) → returns 150 → FAILS."""
    assert our_held_at_broker("sess-x", "AAA", "EQ", our_qty=100,
                              broker_net=150) == 100


def test_item3_exit_clamps_to_our_fills_not_manual_inflated_net(
        clean_positions, monkeypatch):
    """OUR tagged entry 100 + a manual buy 50 (broker net 150 same symbol/account):
    our tracked position stays 100 (the manual buy never touches our row) AND the
    exit places OUR 100 — a manual same-symbol fill can neither inflate nor deflate
    OUR tracked position. End-to-end demonstration of the ITEM 3 invariant with
    DEFENCE IN DEPTH: TWO independent layers each cap the exit at our 100 —
      (1) the exit qty is sourced from the POSITION ROW qty (written from OUR fill),
          NEVER the account net, and
      (2) the our_held_at_broker clamp = min(our_qty, net) caps it again.
    Because both layers are present a SINGLE mutation cannot inflate the exit (verified:
    even sourcing qty from net_qty, the our_held clamp reduces it back to 100). The
    ISOLATED single-line mutation for the clamp layer is carried by
    test_item3_our_held_clamps_to_our_tracked_qty (min(oq,...) → net-other → FAILS)."""
    sess, broker, prof = _live_exit_session(monkeypatch, net={"AAA": 150})
    # Our tracked qty is defined by OUR fill (100), independent of the manual 50.
    assert _pos(sess.session_id, "AAA")["qty"] == 100
    pos = sess.registry.get_open_positions()[0]
    asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=None, kite_product="CNC", exec_cfg=sess.config))
    assert [q for _s, q in broker.exits] == [100]          # OUR 100, not 150


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 4 — PER-BROKER tag transmission (Upstox) + coid persisted regardless.
# ══════════════════════════════════════════════════════════════════════════════

class _FakeResp:
    def __init__(self, oid):
        self._oid = oid

    def raise_for_status(self):
        return None

    def json(self):
        return {"data": {"order_id": self._oid}}


def _upstox_live(monkeypatch, captured):
    from autotrade.broker.upstox import UpstoxBroker
    prof = BrokerProfile(profile_id="ups", broker_name="upstox",
                         allocated_capital=100000.0, order_product="CNC",
                         instrument_type="EQ")
    prof.access_token = "tok"
    b = UpstoxBroker(prof, dry_run=False)
    monkeypatch.setattr(b, "_live_allowed", lambda: True)
    monkeypatch.setattr(b, "_instrument_key", lambda *a, **k: "NSE_EQ|INEXXX")

    def fake_post(url, json=None, headers=None, timeout=None, proxies=None):
        captured["body"] = json
        return _FakeResp("U-1")

    monkeypatch.setattr("requests.post", fake_post)
    return b


def test_item4_upstox_transmits_client_tag_entry_and_exit(
        clean_positions, monkeypatch):
    """An adapter that SUPPORTS a tag field (Upstox v2 /order/place documents `tag`)
    transmits compact_tag(client_order_id) on BOTH entry and exit payloads.

    MUTATION REVERT: delete the `if _tag: body["tag"] = ...` (entry) / the
    `body["tag"] = compact_tag(...)` (exit) lines in broker/upstox.py → the payload
    carries no tag → FAILS."""
    coid = order_ledger.make_client_order_id("s", "AAA")
    tag = compact_tag(coid)

    cap = {}
    b = _upstox_live(monkeypatch, cap)
    order = Order(symbol="AAA", qty=10, product="CNC",
                  client_order_id=coid, tag=tag)
    asyncio.run(b.place_order(order))
    assert cap["body"]["tag"] == tag

    cap.clear()
    b = _upstox_live(monkeypatch, cap)
    asyncio.run(b.place_market_exit("AAA", 10, "EQ", kite_product="CNC",
                                    client_order_id=coid))
    assert cap["body"]["tag"] == tag


def test_item4_internal_coid_persisted_regardless_of_broker(
        clean_positions, monkeypatch):
    """OUR-side attribution works even when the broker CANNOT echo a tag: the exit
    path persists exit_client_order_id on the row + threads the client_order_id to
    the adapter, so reconciliation-by-recorded-order-id works no matter the broker.

    MUTATION REVERT: drop `registry.set_exit_client_order_id(...)` in
    session._exit_single_position_inner → the row's exit_client_order_id stays NULL →
    the assertion FAILS (attribution would fall back to symbol/qty)."""
    sess, broker, prof = _live_exit_session(monkeypatch)
    pos = sess.registry.get_open_positions()[0]
    asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=None, kite_product="CNC", exec_cfg=sess.config))
    row = _pos(sess.session_id, "AAA")
    # The coid is persisted internally (broker-agnostic) and the SAME coid was
    # threaded to the adapter — attribution holds even without a broker tag echo.
    assert row["exit_client_order_id"], "exit client_order_id not persisted"
    assert broker.exit_calls[-1]["client_order_id"] == row["exit_client_order_id"]
