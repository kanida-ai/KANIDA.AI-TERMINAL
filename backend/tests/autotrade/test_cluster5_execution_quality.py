"""SPRINT CLUSTER 5 — execution quality, latency & mark-staleness.

Every test here is MUTATION-VERIFIED: it passes with the fix in place and FAILS
when the specific fix is reverted (the revert is named in each test's docstring).
Additive + paper-safe: no real broker, no real Kite, no real orders.
"""
from __future__ import annotations

import asyncio
import time

import pytest

from autotrade.config import BrokerProfile, TradingSessionConfig
from autotrade.execution.orders import (Order, OrderTimeoutError,
                                        place_order_with_retry)
from autotrade.execution.quote_pricer import (plan_marketable_order,
                                              exit_circuit_locked_reason)
from tests.autotrade.mock_broker import MockBroker


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 1 — entries must not block the event loop (asyncio.to_thread) so legs
#          overlap under asyncio.gather AND wait_for can time out a hung place.
# REVERT: in autotrade/broker/zerodha.py place_order, replace
#   oid = await asyncio.to_thread(_retry_kite_call, lambda: kite.place_order(**p), ...)
# with the blocking
#   oid = _retry_kite_call(lambda: kite.place_order(**p), ...)
# → the coroutine never yields → gather serialises (overlap test fails) AND
#   wait_for cannot interrupt the blocking call (timeout test fails).
# ══════════════════════════════════════════════════════════════════════════════

class _FakeKite:
    VARIETY_REGULAR = "regular"
    EXCHANGE_NSE = "NSE"
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    PRODUCT_CNC = "CNC"
    PRODUCT_MIS = "MIS"
    PRODUCT_NRML = "NRML"

    def __init__(self, sleep_sec: float = 0.0):
        self.sleep_sec = sleep_sec
        self.calls = 0

    def place_order(self, **params):
        self.calls += 1
        if self.sleep_sec:
            time.sleep(self.sleep_sec)   # blocking HTTP stand-in
        return f"oid-{params.get('tradingsymbol')}-{self.calls}"


def _live_zerodha(sleep_sec: float) -> "object":
    from autotrade.broker.zerodha import ZerodhaBroker
    b = ZerodhaBroker(profile=BrokerProfile("p", "zerodha"), dry_run=False)
    b._kite = _FakeKite(sleep_sec=sleep_sec)
    # Bypass the live gates (token / preflight / master switch) — we are testing
    # ONLY the threading of the placement, not the guards.
    b._live_allowed = lambda: True
    b._token_abort_reason = lambda: None
    b._preflight_block_reason = lambda: None
    return b


def _order(symbol: str) -> Order:
    return Order(symbol=symbol, qty=1, product="CNC", order_type="MARKET")


def test_item1_entry_legs_overlap_not_serialise():
    """Two legs, each blocking 0.4s in the broker, must OVERLAP (~0.4s total) via
    asyncio.gather — not serialise (~0.8s). Proves place_order yields the loop."""
    broker = _live_zerodha(sleep_sec=0.4)

    async def _run():
        return await asyncio.gather(
            place_order_with_retry(_order("AAA"), broker, timeout_ms=5000),
            place_order_with_retry(_order("BBB"), broker, timeout_ms=5000))

    t0 = time.perf_counter()
    res = asyncio.run(_run())
    elapsed = time.perf_counter() - t0
    assert all(r.status == "PLACED" for r in res)
    # Overlapped ≈ 0.4s. Serial (blocking, mutation) ≈ 0.8s. Middle threshold.
    assert elapsed < 0.65, f"legs serialised ({elapsed:.2f}s) — place_order blocked the loop"


def test_item1_hung_placement_raises_ordertimeout():
    """A placement that blocks 3s with a 300ms wait_for must raise
    OrderTimeoutError — only possible if place_order runs in a thread so wait_for
    can time out. Mutation (blocking call) → the loop is wedged, no timeout, the
    order returns PLACED and this raises assertion instead."""
    broker = _live_zerodha(sleep_sec=3.0)

    async def _run():
        # Measure the time to RAISE (inside the loop) — asyncio.run()'s shutdown
        # would otherwise join the still-running to_thread worker (~3s), masking
        # the prompt timeout.
        t0 = time.perf_counter()
        try:
            await place_order_with_retry(_order("SLOW"), broker,
                                         max_retries=1, timeout_ms=300)
        except OrderTimeoutError:
            return time.perf_counter() - t0
        return None

    raised_after = asyncio.run(_run())
    # wait_for fired the timeout (only possible if place_order yielded the loop);
    # a blocking placement (mutation) returns PLACED → raised_after is None here.
    assert raised_after is not None, "no OrderTimeoutError — the placement blocked the loop"
    assert raised_after < 1.0


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 2 — mark-staleness abstain. A PROFIT kill must NOT fire on a stale mark
#          (a daily-close fallback), while the DOWNSIDE stop still fires.
# REVERT: set mark_staleness_abstain_sec=0 (or delete the `_marks_stale_for_profit`
# abstain block in session.tick) → the stale PROFIT_TARGET fires → the abstain
# assertion (kill NOT fired / mark_stale_abstain True) fails.
# ══════════════════════════════════════════════════════════════════════════════

from autotrade.session import (TradingSession, _marks_stale_for_profit,
                               set_fake_now)   # noqa: E402
from tests.autotrade.conftest import seed_signals   # noqa: E402
from falcon.db import falcon_conn   # noqa: E402


def _seed_ohlc_close(symbol: str, close: float):
    with falcon_conn() as con:
        con.execute("CREATE TABLE IF NOT EXISTS ohlc_daily "
                    "(symbol TEXT, trade_date TEXT, close REAL)")
        con.execute("DELETE FROM ohlc_daily WHERE symbol=?", (symbol,))
        con.execute("INSERT INTO ohlc_daily (symbol, trade_date, close) "
                    "VALUES (?,?,?)", (symbol, "2026-06-24", close))
        con.commit()


@pytest.fixture
def kill_broker(monkeypatch):
    """build_client → a MockBroker reading a shared, MUTABLE ltp map. Emptying the
    map makes get_ltp() return None so refresh_ltps falls back to ohlc_daily (a
    NON-live mark that ages → the staleness gate engages)."""
    import autotrade.broker.router as router_mod
    import autotrade.session as sess_mod
    shared = {}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=shared)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return shared


def _kill_cfg(**kw):
    base = dict(total_allocated_capital=10_000.0, top_n_stocks=1,
                sizing_mode="equal", order_product="CNC",
                strategy="portfolio_kill_switch", kill_switch_enabled=True,
                kill_switch_pct=0.01, per_position_gtt_enabled=False)
    base.update(kw)
    return TradingSessionConfig(**base)


def test_item2_marks_stale_helper():
    """Pure gate: a mark with a NULL live-stamp OR an old ltp_as_of is STALE; a
    fresh ltp_as_of is not; bound<=0 disables."""
    from datetime import datetime, timedelta, timezone
    IST = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(IST)
    fresh = now.isoformat()
    old = (now - timedelta(seconds=120)).isoformat()
    assert _marks_stale_for_profit([{"ltp": 100.0, "ltp_as_of": None}], 30) is True
    assert _marks_stale_for_profit([{"ltp": 100.0, "ltp_as_of": old}], 30) is True
    assert _marks_stale_for_profit([{"ltp": 100.0, "ltp_as_of": fresh}], 30) is False
    # bound 0 disables the gate entirely (pre-Cluster-5 behaviour).
    assert _marks_stale_for_profit([{"ltp": 100.0, "ltp_as_of": old}], 0) is False


def test_item2_stale_profit_abstains_then_fresh_fires(clean_positions, kill_broker):
    """Stale PROFIT mark (ohlc-close fallback) → kill ABSTAINS (flag set); a
    subsequent FRESH profit mark → kill FIRES. Mutation: mark_staleness_abstain_sec
    → 0 makes tick #1 fire (abstain assertion fails)."""
    set_fake_now(None)
    seed_signals([("SPROF", 1, 9.0, 100.0)])
    kill_broker["SPROF"] = 100.0                 # entry fill @100 → invested 10_000
    sess = TradingSession.create(_kill_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    sid = sess.session_id

    # TICK 1 — STALE profit: no live ltp; ohlc close 101 (+1% = the target). The
    # fallback mark has no live stamp → stale → ABSTAIN.
    kill_broker.clear()
    _seed_ohlc_close("SPROF", 101.0)
    r1 = asyncio.run(TradingSession.load(sid).tick())
    assert r1["mark_stale_abstain"] is True
    assert r1["kill_switch_fired"] is False
    assert TradingSession.load(sid)._current_status() == "RUNNING"

    # TICK 2 — FRESH profit mark (live ltp 101) → the SAME +1% now FIRES.
    kill_broker["SPROF"] = 101.0
    r2 = asyncio.run(TradingSession.load(sid).tick())
    assert r2["kill_switch_fired"] is True
    assert r2.get("mark_stale_abstain") is False


def test_item2_downside_stop_fires_even_when_stale(clean_positions, kill_broker):
    """A stale mark must NOT suppress the DOWNSIDE stop — LOSS_LIMIT fires on a
    stale ohlc-close loss. Mutation-independent guard on the abstain scope (only
    profit is abstained). If the gate wrongly abstained the loss side, this fails."""
    set_fake_now(None)
    seed_signals([("SLOSS", 1, 9.0, 100.0)])
    kill_broker["SLOSS"] = 100.0
    sess = TradingSession.create(_kill_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    sid = sess.session_id

    kill_broker.clear()
    _seed_ohlc_close("SLOSS", 98.0)             # -2% (stale) → LOSS_LIMIT
    r = asyncio.run(TradingSession.load(sid).tick())
    assert r["kill_switch_fired"] is True
    assert str(r["kill_reason"]).startswith("LOSS_LIMIT")


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 3 — ENTRY quote-freshness SLA. A stale ENTRY quote is NEVER priced off the
#          book (degrade to MARKET, or skip per policy). EXITS proceed but flag.
# REVERT: delete the staleness block at the top of plan_marketable_order → a stale
# ENTRY quote is priced into a LIMIT off the stale bid/ask → these asserts fail.
# ══════════════════════════════════════════════════════════════════════════════

def _sla_cfg(**kw):
    base = dict(total_allocated_capital=100_000.0, top_n_stocks=5,
                execution_mode="marketable_limit",
                entry_quote_max_age_sec=10.0)
    base.update(kw)
    return TradingSessionConfig(**base)


def _q(ltp, bid=None, ask=None, upper=None, lower=None, ts=0.0):
    return {"ltp": ltp, "bid": bid, "ask": ask, "upper_circuit": upper,
            "lower_circuit": lower, "ts": ts}


def test_item3_fresh_entry_quote_prices_limit_at_circuit():
    """A FRESH quote (ts≈now) locked at the upper circuit → a LIMIT queued at the
    circuit (the normal marketable-limit entry path is unaffected)."""
    cfg = _sla_cfg()
    now = time.time()
    q = _q(110.0, bid=109.9, ask=None, upper=110.0, lower=90.0, ts=now)
    plan = plan_marketable_order("BUY", "C", 10, q, 0.05, cfg, entry=True,
                                 now_ts=now, max_quote_age_sec=10.0)
    assert plan["ok"] is True and plan["order_type"] == "LIMIT"
    assert plan["price"] == 110.0


def test_item3_stale_entry_market_policy_degrades_not_priced():
    """policy 'market': a STALE entry quote degrades to a MARKET fallback flagged
    degraded_quote — it is NEVER priced into a LIMIT off the stale book."""
    cfg = _sla_cfg(entry_stale_quote_policy="market")
    now = time.time()
    q = _q(110.0, bid=109.9, ask=None, upper=110.0, lower=90.0, ts=now - 60)
    plan = plan_marketable_order("BUY", "C", 10, q, 0.05, cfg, entry=True,
                                 now_ts=now, max_quote_age_sec=10.0,
                                 stale_policy="market")
    assert plan["fallback_market"] is True
    assert plan["order_type"] == "MARKET"
    assert plan["price"] is None
    assert plan["degraded_quote"] is True


def test_item3_stale_entry_skip_policy():
    """policy 'skip': a STALE entry quote SKIPS the leg (no order, degraded flag)."""
    cfg = _sla_cfg(entry_stale_quote_policy="skip")
    now = time.time()
    q = _q(110.0, bid=109.9, ask=100.1, upper=110.0, lower=90.0, ts=now - 60)
    plan = plan_marketable_order("BUY", "C", 10, q, 0.05, cfg, entry=True,
                                 now_ts=now, max_quote_age_sec=10.0,
                                 stale_policy="skip")
    assert plan.get("skip") is True
    assert plan["ok"] is False
    assert plan["degraded_quote"] is True
    assert plan.get("price") is None


def test_item3_stale_exit_still_prices_but_flags():
    """An EXIT NEVER skips on a stale quote — it still returns a placeable order,
    carrying degraded_quote=True."""
    cfg = _sla_cfg()
    now = time.time()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0, ts=now - 60)
    plan = plan_marketable_order("SELL", "X", 10, q, 0.05, cfg, entry=False,
                                 now_ts=now, max_quote_age_sec=10.0)
    assert plan["ok"] is True and plan["order_type"] == "LIMIT"
    assert plan["degraded_quote"] is True


def test_item3_no_now_ts_is_backcompat():
    """Callers that don't pass now_ts (existing code / pure tests) are byte-for-byte
    unchanged — the SLA is inert."""
    cfg = _sla_cfg()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0, ts=0.0)
    plan = plan_marketable_order("SELL", "X", 10, q, 0.05, cfg, entry=False)
    assert plan["ok"] is True and plan["degraded_quote"] is False


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 4 — circuit-locked entry policy consistency + a DISTINCT stranded-at-circuit
#          exit signal.
# REVERT (exit signal): make exit_circuit_locked_reason() return None always (or
# remove the exhaustion circuit-probe in cancel_and_retry_exit) → the locked exit
# surfaces the generic reason → these asserts fail.
# ══════════════════════════════════════════════════════════════════════════════

def test_item4_entry_circuit_locked_policy_default_and_validate():
    cfg = TradingSessionConfig(total_allocated_capital=100_000.0)
    assert cfg.entry_circuit_locked_policy == "drop"     # documented policy (a)
    cfg.validate()
    TradingSessionConfig(total_allocated_capital=1.0,
                         entry_circuit_locked_policy="queue").validate()
    with pytest.raises(ValueError):
        TradingSessionConfig(total_allocated_capital=1.0,
                             entry_circuit_locked_policy="hold").validate()


def test_item4_exit_circuit_locked_reason_detection():
    """A long-exit SELL into a LOWER-circuit lock (no bid, ltp≤lower) → circuit
    locked; a healthy book → None; a short-cover BUY into an UPPER lock → locked."""
    lower_locked = {"ltp": 90.0, "bid": None, "ask": 90.5,
                    "upper_circuit": 110.0, "lower_circuit": 90.0}
    healthy = {"ltp": 100.0, "bid": 99.9, "ask": 100.1,
               "upper_circuit": 110.0, "lower_circuit": 90.0}
    upper_locked = {"ltp": 110.0, "bid": 109.9, "ask": None,
                    "upper_circuit": 110.0, "lower_circuit": 90.0}
    assert exit_circuit_locked_reason(lower_locked, "long") == "circuit_locked"
    assert exit_circuit_locked_reason(healthy, "long") is None
    assert exit_circuit_locked_reason(upper_locked, "short") == "circuit_locked"
    assert exit_circuit_locked_reason(upper_locked, "long") is None
    assert exit_circuit_locked_reason(None, "long") is None


class _FakeRegistry:
    def __init__(self):
        self.exit_failed = []

    def mark_exit_failed(self, symbol, reason, broker_profile=None, **kw):
        self.exit_failed.append((symbol, reason))

    def mark_closed(self, *a, **kw):
        pass


def test_item4_exit_retry_surfaces_circuit_locked_reason():
    """When a long exit can never fill because the stock is LOWER-circuit locked,
    the exhausted retry marks EXIT_FAILED with a DISTINCT 'circuit_locked' reason
    (not a generic 'cancel_and_retry exhausted')."""
    from autotrade.monitoring.exit_poller import cancel_and_retry_exit
    locked = {"ltp": 90.0, "bid": None, "ask": 90.5,
              "upper_circuit": 110.0, "lower_circuit": 90.0, "ts": time.time()}
    broker = MockBroker(BrokerProfile("p", "mock"), dry_run=False,
                        fail_symbols={"S"}, quotes={"S": locked})
    reg = _FakeRegistry()
    res = asyncio.run(cancel_and_retry_exit(
        session_id="sess-x", symbol="S", order_id="ord-old", qty=10,
        broker=broker, registry=reg, close_reason="EXIT_RETRY",
        max_retries=1, direction="long", instrument_type="EQ"))
    assert reg.exit_failed, "expected an EXIT_FAILED mark"
    _, reason = reg.exit_failed[-1]
    assert "circuit_locked" in reason
    assert res.get("reason") == "circuit_locked"


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 5 — marketable_limit is the SAFE default (config default; env still wins).
#   Covered by test_marketable_execution.test_default_execution_mode_is_marketable_limit
#   (kept there next to the execution suite). A focused restatement here:
# REVERT: config.py execution_mode default "marketable_limit" → "market" fails it.
# ══════════════════════════════════════════════════════════════════════════════

def test_item5_execution_mode_default_is_marketable_limit(monkeypatch):
    monkeypatch.delenv("FALCON_AUTOTRADE_EXECUTION_MODE", raising=False)
    assert TradingSessionConfig(
        total_allocated_capital=1.0).execution_mode == "marketable_limit"
    # Explicit market path still available.
    m = TradingSessionConfig(total_allocated_capital=1.0, execution_mode="market")
    m.validate()
    assert m.execution_mode == "market"


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 6 — the kill flatten resolves N legs from ONE batched orderbook fetch per
#          poll cycle (not N per-leg get_order_status scans).
# REVERT: in kill_switch.fire drop `status_provider=_snapshot_for(...).status` from
# the confirm_exit call (confirm falls back to broker.get_order_status per leg) →
# get_order_status_calls == N, get_orders_calls == 0 → the asserts flip and fail.
# ══════════════════════════════════════════════════════════════════════════════

from autotrade.monitoring.kill_switch import KillSwitchExecutor   # noqa: E402
from autotrade.monitoring.registry import PositionRegistry   # noqa: E402


def _make_session_row(session_id, cap):
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json) VALUES (?,?,?,?,?,?)""",
            (session_id, "2026-06-24T09:00:00", "RUNNING", "paper", cap, "{}"))
        con.commit()


def test_item6_kill_uses_one_orderbook_fetch_per_cycle(clean_positions):
    """An N-leg kill flatten issues ONE get_orders() (batched) and ZERO per-leg
    get_order_status() scans, resolving every leg from the shared snapshot."""
    import uuid
    sid = uuid.uuid4().hex
    cap = 1_000_000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    N = 4
    # A single broker holding N legs; its orderbook reports every exit COMPLETE so
    # each leg confirms on the first poll (one shared fetch).
    orderbook = []
    for i in range(N):
        sym = f"S{i}"
        reg.register(symbol=sym, broker_profile="zer", qty=10, avg_price=100.0)
        reg.update_ltp(sym, 100.0)
        orderbook.append({"order_id": f"exit-{sym}", "status": "COMPLETE",
                          "filled_quantity": 10, "average_price": 100.0})
    broker = MockBroker(BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={f"S{i}": 100.0 for i in range(N)},
                        orders=orderbook)
    cfg = TradingSessionConfig(total_allocated_capital=cap,
                               kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)
    res = asyncio.run(ks.fire("TEST"))
    assert res["n_exited_ok"] == N
    # ONE batched orderbook fetch served all N legs; NO per-leg get_order_status.
    assert broker.get_order_status_calls == 0, (
        f"per-leg get_order_status was used {broker.get_order_status_calls}× "
        "— batched snapshot not engaged")
    assert 1 <= broker.get_orders_calls <= 2, (
        f"expected ~1 batched get_orders per cycle, got {broker.get_orders_calls}")
