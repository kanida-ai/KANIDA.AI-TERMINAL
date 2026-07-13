"""QUERY-BEFORE-RETRY idempotency guard for the in-session order timeout retry.

Regression for the 2026-07-13 ZENSARTECH LIVE double-fill (session 44bebf96): a
confirmation TIMEOUT in place_order_with_retry did NOT prove the broker missed the
order (kite.place_order runs in an uncancellable worker thread), so the blind retry
placed a SECOND identical order (both COMPLETE → 2x position, one left untracked).

The fix: on TimeoutError, ASK the broker (find_recent_order) whether OUR order
already reached it and ADOPT it instead of re-placing — while NEVER treating an
inconclusive query as "absent" (that would re-introduce the double-place).
"""
import asyncio

import pytest

from autotrade.execution.orders import place_order_with_retry, OrderTimeoutError, Order
from autotrade.order_ledger import match_recent_order
from autotrade.config import BrokerProfile
from autotrade.broker.zerodha import ZerodhaBroker
from tests.autotrade.mock_broker import MockBroker


def _entry_order():
    o = Order(symbol="ZENSARTECH", qty=1654, transaction_type="BUY")
    o.tag = "ATdeadbeefcafe12"
    return o


# ── The core case: adopt-not-double-place ────────────────────────────────────
def test_timeout_adopts_existing_order_places_only_once():
    """place_order sleeps past the timeout (raises via wait_for) BUT the order
    already reached the broker → find_recent_order returns it → ADOPT: return the
    real broker_order_id and call place_order EXACTLY ONCE (no second placement).

    REVERT: delete the `if existing is not None:` adopt branch in
    place_order_with_retry (so a timeout always falls through to retry) → place_order
    is called TWICE → the `calls == 1` assertion FAILS (this is the double-fill).
    """
    class AdoptBroker(MockBroker):
        def __init__(self, **kw):
            super().__init__(**kw)
            self.calls = 0

        async def place_order(self, order):
            self.calls += 1
            await asyncio.sleep(0.3)          # exceeds the 50ms timeout below
            return await super().place_order(order)

        def find_recent_order(self, order):
            # OUR order IS at the broker (the submit succeeded; only our wait
            # timed out). Return the live orderbook row.
            return {"order_id": "260713220044356", "status": "COMPLETE",
                    "tag": order.tag, "tradingsymbol": order.symbol,
                    "transaction_type": "BUY", "quantity": order.qty}

    b = AdoptBroker(profile=None, ltps={"ZENSARTECH": 501.0})
    res = asyncio.run(place_order_with_retry(_entry_order(), b,
                                             max_retries=3, timeout_ms=50))
    assert b.calls == 1                        # NO second placement
    assert res.status == "PLACED"
    assert res.broker_order_id == "260713220044356"


# ── Genuine failure: order really didn't reach the broker → retry ────────────
def test_timeout_absent_order_retries():
    """place_order times out on the first attempt and find_recent_order returns
    None (definitively absent) → it is SAFE to retry; the 2nd attempt succeeds.
    place_order is called twice (the retry actually happens)."""
    class FlakyBroker(MockBroker):
        def __init__(self, **kw):
            super().__init__(**kw)
            self.calls = 0

        async def place_order(self, order):
            self.calls += 1
            if self.calls == 1:
                await asyncio.sleep(0.3)       # first attempt times out
            return await super().place_order(order)

        def find_recent_order(self, order):
            return None                        # queried OK, genuinely absent

    b = FlakyBroker(profile=None, ltps={"ZENSARTECH": 501.0})
    res = asyncio.run(place_order_with_retry(_entry_order(), b,
                                             max_retries=3, timeout_ms=50))
    assert b.calls == 2                        # retried once
    assert res.status in ("PLACED", "DRY_RUN")


# ── Inconclusive query is NOT a blind retry ──────────────────────────────────
def test_inconclusive_query_fails_leg_no_second_order():
    """place_order times out AND find_recent_order RAISES (broker query error /
    can't determine) → we must NOT place a second order; the leg fails.

    REVERT: change place_order_with_retry to treat an inconclusive query as absent
    (e.g. `except Exception: existing = None` then fall through to retry) → a 2nd
    place_order runs → `calls == 1` FAILS and OrderTimeoutError is NOT raised (the
    double-place is back).
    """
    class InconclusiveBroker(MockBroker):
        def __init__(self, **kw):
            super().__init__(**kw)
            self.calls = 0

        async def place_order(self, order):
            self.calls += 1
            await asyncio.sleep(0.3)
            return await super().place_order(order)

        def find_recent_order(self, order):
            raise ConnectionResetError(10054, "broker unreachable")

    b = InconclusiveBroker(profile=None, ltps={"ZENSARTECH": 501.0})
    with pytest.raises(OrderTimeoutError):
        asyncio.run(place_order_with_retry(_entry_order(), b,
                                           max_retries=3, timeout_ms=50))
    assert b.calls == 1                        # fail the leg, no second order


# ── Zerodha find_recent_order matching against a mock kite.orders() ──────────
class _FakeKite:
    def __init__(self, rows):
        self._rows = rows

    def orders(self):
        return self._rows


def _zerodha_live(rows):
    prof = BrokerProfile("z1", "zerodha", broker_account_id=None)
    b = ZerodhaBroker(prof, dry_run=False)
    b._kite = _FakeKite(rows)                   # bypass _build_kite
    b._live_allowed = lambda: True              # force the live query path
    return b


def test_zerodha_find_recent_order_matches_tag_symbol_txn_qty():
    o = _entry_order()
    rows = [
        {"order_id": "260713220044356", "status": "COMPLETE", "tag": o.tag,
         "tradingsymbol": "ZENSARTECH", "transaction_type": "BUY",
         "quantity": 1654},
        # non-matching noise: same symbol, different tag (a foreign order).
        {"order_id": "999", "status": "COMPLETE", "tag": "OTHERTAG",
         "tradingsymbol": "ZENSARTECH", "transaction_type": "BUY",
         "quantity": 1654},
    ]
    b = _zerodha_live(rows)
    found = b.find_recent_order(o)
    assert found is not None
    assert found["order_id"] == "260713220044356"


def test_zerodha_find_recent_order_ignores_other_symbol_and_mismatch():
    o = _entry_order()
    rows = [
        # right tag but WRONG symbol (never adopt the wrong instrument).
        {"order_id": "1", "status": "COMPLETE", "tag": o.tag,
         "tradingsymbol": "INFY", "transaction_type": "BUY", "quantity": 1654},
        # right tag+symbol but WRONG qty.
        {"order_id": "2", "status": "COMPLETE", "tag": o.tag,
         "tradingsymbol": "ZENSARTECH", "transaction_type": "BUY", "quantity": 10},
        # right tag+symbol+qty but WRONG side.
        {"order_id": "3", "status": "COMPLETE", "tag": o.tag,
         "tradingsymbol": "ZENSARTECH", "transaction_type": "SELL",
         "quantity": 1654},
        # right everything but REJECTED → not an adoptable success.
        {"order_id": "4", "status": "REJECTED", "tag": o.tag,
         "tradingsymbol": "ZENSARTECH", "transaction_type": "BUY",
         "quantity": 1654},
    ]
    b = _zerodha_live(rows)
    assert b.find_recent_order(o) is None


def test_zerodha_find_recent_order_paper_returns_none():
    """dry_run / not-live → None (no real book; place_order never times out in
    dry-run, so paper is byte-identical and never adopts)."""
    prof = BrokerProfile("z1", "zerodha", broker_account_id=None)
    b = ZerodhaBroker(prof, dry_run=True)
    assert b.find_recent_order(_entry_order()) is None


def test_zerodha_find_recent_order_api_error_is_inconclusive():
    """A Kite API error is RE-RAISED (inconclusive), NOT swallowed to None — so
    place_order_with_retry fails the leg rather than blind-retrying (false-negative
    is the dangerous direction)."""
    class _BoomKite:
        def orders(self):
            raise ConnectionResetError(10054, "broker unreachable")

    prof = BrokerProfile("z1", "zerodha", broker_account_id=None)
    b = ZerodhaBroker(prof, dry_run=False)
    b._kite = _BoomKite()
    b._live_allowed = lambda: True
    with pytest.raises(Exception):
        b.find_recent_order(_entry_order())


# ── The pure matcher ─────────────────────────────────────────────────────────
def test_match_recent_order_prefers_complete_over_open():
    tag = "ATx"
    rows = [
        {"order_id": "open1", "status": "OPEN", "tag": tag,
         "tradingsymbol": "A", "transaction_type": "BUY", "quantity": 5},
        {"order_id": "done1", "status": "COMPLETE", "tag": tag,
         "tradingsymbol": "A", "transaction_type": "BUY", "quantity": 5},
    ]
    m = match_recent_order(rows, tag=tag, symbol="A", txn="BUY", qty=5)
    assert m["order_id"] == "done1"


def test_match_recent_order_none_on_empty_or_no_tag():
    assert match_recent_order([], tag="ATx", symbol="A", txn="BUY", qty=5) is None
    assert match_recent_order(
        [{"tag": "ATx", "tradingsymbol": "A", "transaction_type": "BUY",
          "quantity": 5, "status": "OPEN"}],
        tag=None, symbol="A", txn="BUY", qty=5) is None
