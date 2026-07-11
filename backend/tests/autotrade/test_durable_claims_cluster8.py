"""SPRINT CLUSTER 8 ITEM 2 — cross-process DURABLE CLAIMS (C3 I3a) + adopt on the
kill/retry exit path.

The in-process fire_guard (_FIRED / _ENTRY_CLAIMED) and the exit single-flight are
backed by a SQLite compare-and-set + lease so a claim survives a restart and holds
across processes. cancel_and_retry_exit now mints+persists an exit client_order_id
and ADOPTS an already-placed tagged order instead of re-placing (exactly-once on the
kill path).

Each test PASSES with the fix and FAILS on the stated revert. Paper-safe.
"""
import asyncio

import pytest

from autotrade import durable_claims
from autotrade.config import BrokerProfile
from autotrade.monitoring import fire_guard
from autotrade import exit_gate
from autotrade import order_ledger
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.exit_poller import cancel_and_retry_exit
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn


def _row(sid, symbol):
    with falcon_conn() as con:
        r = con.execute("SELECT status, qty FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?", (sid, symbol)).fetchone()
    return dict(r) if r else None


# ══════════════════════════════════════════════════════════════════════════════
# CAS + lease authority
# ══════════════════════════════════════════════════════════════════════════════
def test_durable_claim_cas_one_winner(clean_positions):
    """Two claims of the SAME key → the FIRST wins, the SECOND (still-live lease)
    loses. The DB row is the shared authority across processes."""
    assert durable_claims.claim("k1", 100) is True
    assert durable_claims.claim("k1", 100) is False    # live lease held
    assert durable_claims.is_claimed("k1") is True
    durable_claims.release("k1")
    assert durable_claims.is_claimed("k1") is False
    assert durable_claims.claim("k1", 100) is True      # free again → wins


def test_durable_claim_expired_lease_is_takeable(clean_positions):
    """An EXPIRED lease is takeable by a new claimant; a LIVE one is not.

    MUTATION REVERT: drop the `AND leased_until < ?` expiry predicate from the
    takeover UPDATE in durable_claims.claim → a LIVE key would be re-claimable →
    the `claim(live) is False` assert FAILS."""
    durable_claims.claim("kexp", -1)                    # already expired
    assert durable_claims.is_claimed("kexp") is False
    assert durable_claims.claim("kexp", 100) is True    # takeover of expired
    # now it's LIVE → a re-claim must FAIL (not expired).
    assert durable_claims.claim("kexp", 100) is False


# ══════════════════════════════════════════════════════════════════════════════
# fire_guard — cross-restart / cross-process authority
# ══════════════════════════════════════════════════════════════════════════════
def test_fire_guard_fire_survives_restart(clean_positions):
    """claim_fire wins once; after a simulated RESTART (in-process _FIRED cleared)
    the DURABLE claim still blocks a second fire.

    MUTATION REVERT: in fire_guard.claim_fire replace the durable_claims.claim(...)
    with a plain `won = True` (in-process only) → after clearing _FIRED the second
    claim_fire yields True → `won2 is False` FAILS."""
    sid = "dc-fire"
    with fire_guard.claim_fire(sid) as won1:
        pass
    assert won1 is True
    with fire_guard._LOCK:                    # simulate a fresh process / restart
        fire_guard._FIRED.clear()
    with fire_guard.claim_fire(sid) as won2:
        pass
    assert won2 is False                       # durable claim survived the restart


def test_fire_guard_entry_survives_restart(clean_positions):
    """claim_entry wins once; after a simulated restart the durable claim blocks a
    second entry placement.

    MUTATION REVERT: replace durable_claims.claim in fire_guard.claim_entry with
    `won = True` → the post-restart claim_entry returns True → the assert FAILS."""
    sid = "dc-entry"
    assert fire_guard.claim_entry(sid) is True
    with fire_guard._LOCK:
        fire_guard._ENTRY_CLAIMED.clear()
    assert fire_guard.claim_entry(sid) is False


# ══════════════════════════════════════════════════════════════════════════════
# exit single-flight — cross-process authority
# ══════════════════════════════════════════════════════════════════════════════
def test_exit_flight_holds_across_processes(clean_positions):
    """begin_exit_flight wins; a second PROCESS (in-process set cleared) is still
    blocked by the durable claim; end_exit_flight releases it.

    MUTATION REVERT: remove the durable_claims.claim gate in
    exit_gate.begin_exit_flight → after clearing _INFLIGHT the second begin returns
    True → `won2 is False` FAILS."""
    sid, sym = "dc-flight", "A"
    assert exit_gate.begin_exit_flight(sid, sym) is True
    with exit_gate._INFLIGHT_LOCK:            # simulate another process
        exit_gate._INFLIGHT.clear()
    assert exit_gate.begin_exit_flight(sid, sym) is False   # durable claim blocks
    exit_gate.end_exit_flight(sid, sym)
    with exit_gate._INFLIGHT_LOCK:
        exit_gate._INFLIGHT.clear()
    assert exit_gate.begin_exit_flight(sid, sym) is True    # released → free
    exit_gate.end_exit_flight(sid, sym)


# ══════════════════════════════════════════════════════════════════════════════
# Part B — kill/retry adopts an existing tagged exit instead of re-placing.
# ══════════════════════════════════════════════════════════════════════════════
def test_kill_retry_adopts_existing_tagged_order(clean_positions):
    """The position carries a persisted exit client_order_id, and OUR tagged exit is
    ALREADY at the broker (COMPLETE). cancel_and_retry_exit ADOPTS it (confirms the
    existing fill) and places ZERO new orders.

    MUTATION REVERT: delete the `_adopted = await adopt_tagged_exit_if_present(...)
    → if _adopted is not None: return` block in cancel_and_retry_exit → it places a
    fresh retry exit → `broker.exits == []` FAILS (a duplicate exit)."""
    sid = "dc-adopt"
    reg = PositionRegistry(sid, 200000.0)
    reg.register(symbol="A", broker_profile="p1", qty=10, avg_price=100.0,
                 product="CNC", instrument_type="EQ")
    reg.update_ltp("A", 100.0, broker_profile="p1")
    coid = order_ledger.make_client_order_id(sid, "A", attempt=1)
    reg.set_exit_client_order_id("A", coid, broker_profile="p1")
    tag = order_ledger.compact_tag(coid)

    orders = [{"order_id": "adopted-A", "tag": tag, "transaction_type": "SELL",
               "status": "COMPLETE", "filled_quantity": 10, "average_price": 100.0}]
    broker = MockBroker(
        profile=BrokerProfile("p1", "mock"), dry_run=False, ltps={"A": 100.0},
        orders=orders,
        order_status={"adopted-A": {"status": "COMPLETE",
                                    "filled_quantity": 10, "average_price": 100.0}})

    res = asyncio.run(cancel_and_retry_exit(
        sid, "A", "stale-order", 10, broker, reg, max_wait_sec=1,
        poll_interval_sec=0.05, instrument_type="EQ", broker_profile="p1"))

    assert res["status"] == "COMPLETE"
    assert broker.exits == []                      # NO new exit placed — adopted
    assert _row(sid, "A")["status"] == "CLOSED"


def test_kill_retry_places_when_no_tagged_order(clean_positions):
    """No-fire complement: when NO tagged order exists at the broker (get_orders has
    none of our tag), the retry proceeds and places a fresh exit — adopt never
    suppresses a genuinely-needed retry."""
    sid = "dc-adopt-none"
    reg = PositionRegistry(sid, 200000.0)
    reg.register(symbol="A", broker_profile="p1", qty=10, avg_price=100.0,
                 product="CNC", instrument_type="EQ")
    reg.update_ltp("A", 100.0, broker_profile="p1")
    # broker with an EMPTY orderbook (no adoptable tag) and still-held net.
    broker = MockBroker(profile=BrokerProfile("p1", "mock"), dry_run=False,
                        ltps={"A": 100.0}, orders=[], net_positions={"A": 10})
    res = asyncio.run(cancel_and_retry_exit(
        sid, "A", "stale-order", 10, broker, reg, max_wait_sec=1,
        poll_interval_sec=0.05, instrument_type="EQ", broker_profile="p1"))
    assert any(e[0] == "A" for e in broker.exits)   # a fresh exit WAS placed
    # the retry placement carried OUR stable exit tag (query-before-place next time)
    assert any(c.get("client_order_id") for c in broker.exit_calls)
