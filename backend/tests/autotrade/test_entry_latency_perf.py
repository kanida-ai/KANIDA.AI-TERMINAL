"""ENTRY/EXIT LATENCY REGRESSION (2026-07-16) — the F5 probe must not serialise the
basket, and MUST still catch a real double-fill conflict.

BACKGROUND (measured, not theoretical). SPRINT C9c FIX F5 (46f06e1, 2026-07-11) added
a fungible-account conflict probe to the entry path. It called `broker.get_pending_orders()`
per symbol, and the Zerodha adapter ran `self.kite.orders()` — a BLOCKING requests call —
directly inside that coroutine. That froze the asyncio event loop, so the
`asyncio.gather` over the entry legs SERIALISED: one full order-book round-trip per
symbol, back to back. Evidence from live sessions on 2026-07-15: consecutive
ORDER_CREATED rows (written immediately before each broker call) were 3.7-4.8s apart
instead of simultaneous, and the gap tracked the size of the day's order book
(0 orders → 0.85s; 38 orders → 3.67s). A 3-name CNC basket took 20.1s.

THE FIXES UNDER TEST:
  1. zerodha.get_pending_orders → asyncio.to_thread(self.kite.orders) — the loop stays
     free, so the gather actually overlaps (same precedent as place_order).
  2. ONE pending-order fetch per ACCOUNT per FIRE, threaded into every leg
     (deterministic hand-off, NOT a TTL cache — no freshness assumption enters the
     double-fill guard).

THE NON-NEGOTIABLE: a fast basket that lost its double-fill protection is far worse
than a slow one. The F5 fire/no-fire cases below are the real subject of this file;
the timing assertions are the secondary win.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade import alerts
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)

# Models the mid-day Kite order-book latency measured on 2026-07-15 (3.7-4.8s),
# scaled down so the suite stays fast. Serialised: N * PROBE_DELAY. Overlapped: ~1x.
PROBE_DELAY = 0.20
N_LEGS = 5


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


class _SlowBookBroker(MockBroker):
    """A broker whose pending-order book costs real wall-clock to fetch (like
    kite.orders() mid-day) and which COUNTS how many times it was fetched."""

    delay = PROBE_DELAY

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.book_fetches = 0

    async def get_pending_orders(self):
        self.book_fetches += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return list(self._pending)


def _wire(monkeypatch, broker_holder, *, pending=None, ltps=None, delay=PROBE_DELAY):
    def fake_build_client(profile, dry_run=True):
        mb = _SlowBookBroker(profile=profile, dry_run=False,
                            ltps=ltps, available_margin=10_000_000.0,
                            pending_orders=pending or [])
        mb.delay = delay
        broker_holder[profile.profile_id] = mb
        return mb
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)


def _cfg(n):
    return TradingSessionConfig(total_allocated_capital=1_000_000.0, top_n_stocks=n,
                                sizing_mode="equal", kill_switch_enabled=False)


# ═══════════════════════════════════════════════════════════════════════════
# (a) THE GATHER ACTUALLY OVERLAPS — wall-clock ~= 1x probe, not N x probe.
# ═══════════════════════════════════════════════════════════════════════════
def _time_fire(monkeypatch, delay, tag):
    syms = [f"{tag}{i}" for i in range(N_LEGS)]
    brokers = {}
    _wire(monkeypatch, brokers, ltps={s: 100.0 for s in syms}, delay=delay)
    seed_signals([(s, i + 1, 9.0, 100.0) for i, s in enumerate(syms)])
    sess = TradingSession.create(_cfg(N_LEGS), mode="live", user_id="u1")
    t0 = time.monotonic()
    res = asyncio.run(sess.start(when="now"))
    return time.monotonic() - t0, res, brokers


def test_entry_basket_does_not_serialise_on_the_pending_probe(clean_positions,
                                                              monkeypatch):
    """N legs must cost ~ONE probe of wall-clock, not N.

    Measured as a DELTA between an instant book and a slow book, so the fire's fixed
    overhead (DB, sizing, registration) cancels out and the assertion is not a
    machine-speed guess:

        serialised  → delta ≈ N * PROBE_DELAY
        overlapped  → delta ≈ 1 * PROBE_DELAY

    Revert either fix (bare self.kite.orders() in the coroutine, or drop the per-fire
    book) → delta blows out to N× → this FAILS.
    """
    monkeypatch.setattr(alerts, "send_urgent_deduped", lambda **kw: None)

    fast_s, fast_res, _ = _time_fire(monkeypatch, 0.0, "F")
    slow_s, slow_res, _ = _time_fire(monkeypatch, PROBE_DELAY, "S")

    assert fast_res["status"] == "RUNNING" and slow_res["status"] == "RUNNING"
    delta = slow_s - fast_s
    serial = N_LEGS * PROBE_DELAY
    assert delta < serial * 0.5, (
        f"entry basket SERIALISED on the pending probe: the slow book cost "
        f"+{delta:.2f}s for {N_LEGS} legs (serial ≈ {serial:.2f}s, overlapped ≈ "
        f"{PROBE_DELAY:.2f}s) — the asyncio.gather is not overlapping")


def test_pending_book_fetched_once_per_fire_not_once_per_leg(clean_positions,
                                                             monkeypatch):
    """FIX 2 (deterministic): ONE account book fetch per fire, regardless of N."""
    monkeypatch.setattr(alerts, "send_urgent_deduped", lambda **kw: None)
    syms = [f"S{i}" for i in range(N_LEGS)]
    brokers = {}
    _wire(monkeypatch, brokers, ltps={s: 100.0 for s in syms})
    seed_signals([(s, i + 1, 9.0, 100.0) for i, s in enumerate(syms)])
    sess = TradingSession.create(_cfg(N_LEGS), mode="live", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["n_placed"] == N_LEGS
    total = sum(b.book_fetches for b in brokers.values())
    assert total == 1, (f"expected ONE pending-book fetch for the whole fire, got "
                        f"{total} (linear in N = the 2026-07-15 regression)")


def test_paper_fire_does_not_fetch_the_pending_book(clean_positions, monkeypatch):
    """dry_run/paper must be byte-for-byte unchanged: no probe, no extra fetch."""
    monkeypatch.setattr(alerts, "send_urgent_deduped", lambda **kw: None)
    syms = [f"S{i}" for i in range(3)]
    brokers = {}

    def fake_build_client(profile, dry_run=True):
        mb = _SlowBookBroker(profile=profile, dry_run=True,
                             ltps={s: 100.0 for s in syms},
                             available_margin=10_000_000.0, pending_orders=[])
        brokers[profile.profile_id] = mb
        return mb
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    seed_signals([(s, i + 1, 9.0, 100.0) for i, s in enumerate(syms)])
    sess = TradingSession.create(_cfg(3), mode="paper", user_id="u1")

    asyncio.run(sess.start(when="now"))

    assert sum(b.book_fetches for b in brokers.values()) == 0


# ═══════════════════════════════════════════════════════════════════════════
# FIX 1 — the adapter must not BLOCK the event loop.
#
# This is the test that actually pins the 2026-07-15 root cause. MockBroker's probe
# is a normal `async def` that awaits, so it can NEVER reproduce the bug (it yields
# to the loop and overlaps regardless) — only the REAL adapter running a SYNCHRONOUS
# kite.orders() inside a coroutine freezes everything. So we drive ZerodhaBroker with
# a fake kite whose orders() does a BLOCKING time.sleep, exactly like requests does.
# ═══════════════════════════════════════════════════════════════════════════
class _BlockingKite:
    """kite.orders() = a BLOCKING HTTP call (what requests actually does)."""

    def __init__(self, delay, book=None):
        self.delay = delay
        self.book = book or []
        self.calls = 0

    def orders(self):
        self.calls += 1
        time.sleep(self.delay)          # BLOCKING — freezes the loop if not threaded
        return list(self.book)


def _zerodha_with_fake_kite(kite):
    from autotrade.broker.zerodha import ZerodhaBroker
    from autotrade.config import BrokerProfile
    prof = BrokerProfile(profile_id="p1", broker_name="zerodha")
    b = ZerodhaBroker(prof, dry_run=False)
    b._kite = kite                      # bypass the lazy authenticated build
    return b


def test_zerodha_pending_probe_does_not_block_the_event_loop():
    """N concurrent probes must overlap (~1x delay), not serialise (~Nx).

    Revert Fix 1 (`orders = self.kite.orders()` bare in the coroutine) → the loop is
    frozen for each call → elapsed ≈ N*delay → this FAILS. THIS is the 2026-07-15
    regression, reproduced.
    """
    delay, n = 0.25, 5
    kite = _BlockingKite(delay, book=[])
    b = _zerodha_with_fake_kite(kite)

    async def _run():
        t0 = time.monotonic()
        await asyncio.gather(*[b.get_pending_orders() for _ in range(n)])
        return time.monotonic() - t0

    elapsed = asyncio.run(_run())

    assert kite.calls == n
    serial = n * delay
    assert elapsed < serial * 0.5, (
        f"get_pending_orders BLOCKED the event loop: {n} concurrent probes took "
        f"{elapsed:.2f}s (serial ≈ {serial:.2f}s, threaded ≈ {delay:.2f}s) — a "
        f"blocking kite call inside a coroutine serialises the whole basket")


def test_zerodha_pending_probe_semantics_unchanged_through_the_thread():
    """to_thread must not alter the contract: same WORKING-status filter, same rows,
    and paper/dry_run still short-circuits without ever touching kite."""
    book = [{"order_id": "1", "status": "OPEN"},
            {"order_id": "2", "status": "COMPLETE"},
            {"order_id": "3", "status": "TRIGGER PENDING"},
            {"order_id": "4", "status": "REJECTED"},
            {"order_id": "5", "status": "PENDING"}]
    kite = _BlockingKite(0.0, book=book)
    live = _zerodha_with_fake_kite(kite)

    out = asyncio.run(live.get_pending_orders())
    assert [o["order_id"] for o in out] == ["1", "3", "5"]

    paper = _zerodha_with_fake_kite(kite)
    paper.dry_run = True
    before = kite.calls
    assert asyncio.run(paper.get_pending_orders()) == []
    assert kite.calls == before, "paper must never hit the broker"


def test_zerodha_pending_probe_still_fails_soft_on_broker_error():
    """A probe error must still return [] (never raise into a fire) — unchanged."""
    class _Boom:
        def orders(self):
            raise RuntimeError("kite down")
    b = _zerodha_with_fake_kite(_Boom())
    assert asyncio.run(b.get_pending_orders()) == []


# ═══════════════════════════════════════════════════════════════════════════
# (b) F5 STILL FIRES — the guard must be IDENTICAL through the per-fire book.
#     FIRE and NO-FIRE. This is the part that protects real money.
# ═══════════════════════════════════════════════════════════════════════════
def test_f5_still_refuses_foreign_conflict_through_the_prefetched_book(
        clean_positions, monkeypatch):
    """THE REGRESSION GUARD. A foreign manual BUY resting for the entry symbol must
    STILL refuse the leg + page — now that the book arrives pre-fetched.

    Revert: make the per-fire book bypass the probe (e.g. always pass []) → the entry
    places into a double-fill → this FAILS.
    """
    pages = []
    monkeypatch.setattr(alerts, "send_urgent_deduped",
                        lambda **kw: pages.append(kw))
    foreign = [{"tradingsymbol": "A", "transaction_type": "BUY", "quantity": 5,
                "order_id": "MANUAL-1", "status": "OPEN"}]
    brokers = {}
    _wire(monkeypatch, brokers, pending=foreign, ltps={"A": 100.0})
    seed_signals([("A", 1, 9.0, 100.0)])
    sess = TradingSession.create(_cfg(1), mode="live", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "FAILED"
    assert res["n_placed"] == 0
    for mb in brokers.values():
        assert mb.placed == [], "F5 BREACHED: placed into a foreign resting BUY"
    assert any(p.get("kind") == "MANUAL_CONFLICT" for p in pages)


def test_f5_refuses_only_the_conflicted_leg_in_a_mixed_basket(clean_positions,
                                                              monkeypatch):
    """Per-leg precision survives the SHARED book: the conflicted symbol is refused,
    the clean symbols still place. (A shared book must not smear one symbol's
    conflict across the basket, nor hide it.)"""
    monkeypatch.setattr(alerts, "send_urgent_deduped", lambda **kw: None)
    syms = ["A", "B", "C"]
    foreign = [{"tradingsymbol": "B", "transaction_type": "BUY", "quantity": 5,
                "order_id": "MANUAL-B", "status": "OPEN"}]
    brokers = {}
    _wire(monkeypatch, brokers, pending=foreign,
          ltps={s: 100.0 for s in syms})
    seed_signals([(s, i + 1, 9.0, 100.0) for i, s in enumerate(syms)])
    sess = TradingSession.create(_cfg(3), mode="live", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    # NOTE: res["n_placed"] is len(results) — it counts SKIPPED/FAILED legs too — so
    # the real safety property is what actually reached the BROKER.
    refused = [r for r in res["orders"] if r.get("manual_conflict")]
    assert [r["symbol"] for r in refused] == ["B"], \
        "exactly the conflicted leg must be refused"
    placed = [o for mb in brokers.values() for o in mb.placed]
    got = {getattr(o, "symbol", None) or (o.get("symbol") if isinstance(o, dict)
                                          else None) for o in placed}
    assert "B" not in got, "F5 BREACHED: placed the conflicted leg"
    assert {"A", "C"} <= got, "clean legs must still place"


def test_f5_no_fire_when_book_is_clean(clean_positions, monkeypatch):
    """NO-FIRE case: an empty book → no conflict → every leg places, no page."""
    pages = []
    monkeypatch.setattr(alerts, "send_urgent_deduped",
                        lambda **kw: pages.append(kw))
    syms = ["A", "B"]
    brokers = {}
    _wire(monkeypatch, brokers, pending=[], ltps={s: 100.0 for s in syms})
    seed_signals([(s, i + 1, 9.0, 100.0) for i, s in enumerate(syms)])
    sess = TradingSession.create(_cfg(2), mode="live", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 2
    assert not [p for p in pages if p.get("kind") == "MANUAL_CONFLICT"]


def test_f5_ignores_opposite_side_and_other_symbols(clean_positions, monkeypatch):
    """NO-FIRE: a resting SELL (opposite side) and an order on ANOTHER symbol are not
    same-side conflicts → the entry proceeds. Guards against the shared book making
    the probe over-eager and refusing legitimate entries."""
    monkeypatch.setattr(alerts, "send_urgent_deduped", lambda **kw: None)
    noise = [{"tradingsymbol": "A", "transaction_type": "SELL", "quantity": 5,
              "order_id": "M-SELL", "status": "OPEN"},
             {"tradingsymbol": "ZZZ", "transaction_type": "BUY", "quantity": 5,
              "order_id": "M-OTHER", "status": "OPEN"}]
    brokers = {}
    _wire(monkeypatch, brokers, pending=noise, ltps={"A": 100.0})
    seed_signals([("A", 1, 9.0, 100.0)])
    sess = TradingSession.create(_cfg(1), mode="live", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 1


# ═══════════════════════════════════════════════════════════════════════════
# The probe helper itself: pre-fetched book == self-fetched book (same verdict).
# ═══════════════════════════════════════════════════════════════════════════
def test_prefetched_book_yields_identical_verdict_and_zero_fetches():
    from autotrade.session import _foreign_same_side_pending
    book = [{"tradingsymbol": "A", "transaction_type": "BUY", "quantity": 5,
             "order_id": "MANUAL-1", "status": "OPEN"}]
    mb = _SlowBookBroker(profile=None, dry_run=False, pending_orders=book)

    self_fetched = asyncio.run(_foreign_same_side_pending(mb, "A", "BUY"))
    assert mb.book_fetches == 1
    handed_in = asyncio.run(
        _foreign_same_side_pending(mb, "A", "BUY", pending=book))

    assert mb.book_fetches == 1, "a handed-in book must issue NO broker round-trip"
    assert handed_in == self_fetched, "the verdict must be identical either way"
    assert handed_in and handed_in[0]["order_id"] == "MANUAL-1"


def test_prefetch_none_falls_back_to_fetching(clean_positions):
    """pending=None (the default, and the fetch-error fallback) → self-fetch, so
    every pre-existing caller is unchanged and the guard never silently no-ops."""
    from autotrade.session import _foreign_same_side_pending
    book = [{"tradingsymbol": "A", "transaction_type": "BUY", "quantity": 5,
             "order_id": "MANUAL-1", "status": "OPEN"}]
    mb = _SlowBookBroker(profile=None, dry_run=False, pending_orders=book)

    out = asyncio.run(_foreign_same_side_pending(mb, "A", "BUY", pending=None))

    assert mb.book_fetches == 1
    assert out and out[0]["order_id"] == "MANUAL-1"
