"""SPRINT CLUSTER 7 — ICEBERG orders (slice large orders into child legs).

MUTATION-VERIFIED: each test asserts the SAFEGUARDED behaviour and FAILS when the
fix is reverted. The exact revert is stated in each test's docstring.

  CAP 1 — iceberg leg planner + config (pure).
  CAP 2 — ENTRY iceberg: N child orders → ONE aggregate position at the
          quantity-weighted-average fill; every child in the ledger.
  CAP 3 — ENTRY partial/reject robustness: a child reject / under-fill STOPS
          further children and registers the FILLED-so-far (never the intended
          qty — the phantom-fill class); no child filled → NO position.
  CAP 4 — preview surfaces iceberg intent (n_slices, slice_qty).

  (CAP 3 EXIT-iceberg — slicing a large kill/stop/square-off — is STAGED-NEXT and
  deliberately NOT wired here; see the delivery report.)

Paper/dry_run stays byte-identical for existing (non-large) orders: iceberg is
INERT unless iceberg_enabled AND qty > effective slice.
"""
import asyncio

import pytest

import autotrade.session as sess_mod
from autotrade import iceberg, order_ledger
from autotrade.broker.base import Pick
from autotrade.capital import CapitalAllocator
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn


def _prof(pid="zer", product="CNC", itype="EQ"):
    return BrokerProfile(profile_id=pid, broker_name="zerodha",
                         allocated_capital=1_000_000.0,
                         order_product=product, instrument_type=itype)


def _cfg(**kw):
    base = dict(total_allocated_capital=1_000_000.0, top_n_stocks=1,
                order_product="CNC", kill_switch_enabled=False,
                execution_mode="market")
    base.update(kw)
    return TradingSessionConfig(**base)


def _pos(session_id, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT * FROM autotrade_positions WHERE session_id=? AND symbol=?",
            (session_id, symbol)).fetchone()
    return dict(r) if r else None


def _created_events(session_id, symbol):
    return [e for e in order_ledger.get_events(session_id, symbol)
            if e["event_type"] == order_ledger.EV_ORDER_CREATED]


# ══════════════════════════════════════════════════════════════════════════════
# CAP 1 — leg planner + config
# ══════════════════════════════════════════════════════════════════════════════

def test_cap1_plan_iceberg_legs_examples_and_sum_preserved():
    """plan_iceberg_legs splits into ceil(total/slice) legs each <= slice, summing
    to total, remainder LAST; single leg when total <= slice.
    MUTATION REVERT: in iceberg.plan_iceberg_legs, drop the remainder append
    (`if rem: legs.append(rem)`) OR use `full = total // s + 1` (off-by-one) → the
    legs no longer sum to total → the sum asserts FAIL."""
    assert iceberg.plan_iceberg_legs(1000, 300) == [300, 300, 300, 100]
    assert iceberg.plan_iceberg_legs(250, 300) == [250]
    assert iceberg.plan_iceberg_legs(900, 300) == [300, 300, 300]
    for total, s in ((1000, 300), (250, 300), (900, 300), (5001, 500), (7, 3)):
        legs = iceberg.plan_iceberg_legs(total, s)
        assert sum(legs) == total                 # sum preserved
        assert all(0 < q <= s for q in legs)      # every leg within the slice
    assert iceberg.plan_iceberg_legs(0, 300) == []
    assert iceberg.plan_iceberg_legs(500, None) == [500]   # no cap → single leg


def test_cap1_effective_slice_respects_freeze_cap():
    """The effective slice is MIN(configured slice, exchange freeze cap): a slice
    LARGER than the freeze cap is clamped DOWN to the cap so legs never exceed the
    exchange freeze quantity.
    MUTATION REVERT: in iceberg.effective_slice_qty, stop appending freeze_cap_for
    to `caps` (ignore the cap) → eff becomes 5000 (the configured slice) → the
    legs-<=1000 assert FAILS."""
    cfg = _cfg(iceberg_enabled=True, iceberg_slice_qty=5000,
               iceberg_freeze_qty_default=1000)
    eff = iceberg.effective_slice_qty(cfg, "AAA", 3000, 100.0)
    assert eff == 1000                            # freeze cap wins (< 5000)
    legs = iceberg.plan_iceberg_legs(3000, eff)
    assert legs == [1000, 1000, 1000]
    assert all(q <= 1000 for q in legs)
    # per-symbol map overrides the default for that symbol
    cfg2 = _cfg(iceberg_enabled=True, iceberg_freeze_qty_default=1000,
                iceberg_freeze_qty_map={"BBB": 250})
    assert iceberg.effective_slice_qty(cfg2, "BBB", 3000, 100.0) == 250
    assert iceberg.effective_slice_qty(cfg2, "AAA", 3000, 100.0) == 1000


def test_cap1_slice_value_resolves_to_qty_via_price():
    """iceberg_slice_value (₹ notional) → floor(value/price) shares.
    MUTATION REVERT: n/a (arithmetic proof of the value→qty conversion)."""
    cfg = _cfg(iceberg_enabled=True, iceberg_slice_value=30_000.0)
    # ₹30,000 / ₹100 = 300 shares
    assert iceberg.effective_slice_qty(cfg, "AAA", 1000, 100.0) == 300
    assert iceberg.plan_for(cfg, "AAA", 1000, 100.0) == [300, 300, 300, 100]


def test_cap1_inert_when_disabled():
    """Disabled → no cap → a single leg (byte-for-byte unchanged).
    MUTATION REVERT: in effective_slice_qty, remove the `if not iceberg_enabled:
    return None` guard → a disabled config would still slice → the single-leg
    assert FAILS."""
    cfg = _cfg(iceberg_enabled=False, iceberg_slice_qty=300)
    assert iceberg.effective_slice_qty(cfg, "AAA", 1000, 100.0) is None
    assert iceberg.plan_for(cfg, "AAA", 1000, 100.0) == [1000]


def test_cap1_config_validation_and_roundtrip():
    """Config validates each source WHEN SET + requires a source when enabled +
    round-trips through to_dict/from_dict.
    MUTATION REVERT: remove the iceberg validation block in config.validate() →
    the enabled-without-source + slice_qty<1 asserts stop raising → FAIL."""
    with pytest.raises(ValueError):
        _cfg(iceberg_enabled=True).validate()          # enabled, no source
    with pytest.raises(ValueError):
        _cfg(iceberg_enabled=True, iceberg_slice_qty=0).validate()
    with pytest.raises(ValueError):
        _cfg(iceberg_enabled=True, iceberg_slice_value=-1.0).validate()
    good = _cfg(iceberg_enabled=True, iceberg_slice_qty=300,
                iceberg_freeze_qty_default=1000,
                iceberg_freeze_qty_map={"AAA": 500})
    good.validate()
    rt = TradingSessionConfig.from_dict(good.to_dict())
    assert rt.iceberg_enabled is True
    assert rt.iceberg_slice_qty == 300
    assert rt.iceberg_freeze_qty_default == 1000
    assert rt.iceberg_freeze_qty_map == {"AAA": 500}


# ══════════════════════════════════════════════════════════════════════════════
# CAP 2 — ENTRY iceberg: aggregate child fills into ONE position
# ══════════════════════════════════════════════════════════════════════════════

def test_cap2_entry_slices_children_one_position_weighted_avg(clean_positions):
    """A large ENTRY places N child orders (each <= slice), then registers ONE
    position with the TOTAL qty and the QUANTITY-WEIGHTED-AVERAGE fill; every child
    appears in the ledger.
    MUTATION REVERT: delete the iceberg branch in session._place_one (the
    `if iceberg_enabled and len(_ice_legs) > 1: return await self._place_iceberg`
    block) → a single order of the full qty is placed → n_children/4-orders asserts
    FAIL. (A wrong average — e.g. registering only the first child, or a simple
    mean 103.0 / last-price 106.0 instead of 102.4 — also fails the avg assert.)"""
    prof = _prof()
    broker = MockBroker(profile=prof, dry_run=False, ltps={"ICBAGG": 100.0},
                        fill_price_sequence={"ICBAGG": [100.0, 102.0, 104.0, 106.0]})
    cfg = _cfg(iceberg_enabled=True, iceberg_slice_qty=300)
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess._place_one(
        broker, prof, Pick("ICBAGG", 1), 1_000_000.0, CapitalAllocator(cfg),
        forced_qty=1000))
    assert res["status"] != "FAILED"
    assert res["iceberg"] is True
    assert res["n_children"] == 4
    assert res["qty"] == 1000
    # weighted avg = (300*100 + 300*102 + 300*104 + 100*106) / 1000 = 102.4
    assert res["price"] == pytest.approx(102.4, abs=1e-6)
    # 4 child orders, each <= the slice, summing to the total
    assert len(broker.placed) == 4
    assert all(o.qty <= 300 for o in broker.placed)
    assert sum(o.qty for o in broker.placed) == 1000
    # ONE aggregate position at the weighted avg + total qty
    row = _pos(sess.session_id, "ICBAGG")
    assert row is not None and row["qty"] == 1000
    assert row["avg_price"] == pytest.approx(102.4, abs=1e-6)
    # every child recorded in the ledger (distinct ORDER_CREATED intent per child)
    created = _created_events(sess.session_id, "ICBAGG")
    assert len(created) == 4
    assert len({e["client_order_id"] for e in created}) == 4


def test_cap2_inert_disabled_single_order_byte_identical(clean_positions):
    """With iceberg DISABLED a large order is placed as ONE order (byte-for-byte
    unchanged) — proving the slicing is opt-in.
    MUTATION REVERT: n/a — this is the control; paired with the slicing test it
    proves the branch only fires when enabled."""
    prof = _prof()
    broker = MockBroker(profile=prof, dry_run=False, ltps={"ICBINE": 100.0})
    cfg = _cfg(iceberg_enabled=False)
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess._place_one(
        broker, prof, Pick("ICBINE", 1), 1_000_000.0, CapitalAllocator(cfg),
        forced_qty=1000))
    assert len(broker.placed) == 1
    assert broker.placed[0].qty == 1000
    assert res["qty"] == 1000
    assert "n_children" not in res
    assert _pos(sess.session_id, "ICBINE")["qty"] == 1000


# ══════════════════════════════════════════════════════════════════════════════
# CAP 3 — ENTRY partial/reject robustness (phantom-fill guard)
# ══════════════════════════════════════════════════════════════════════════════

def test_cap3_entry_reject_midway_registers_filled_so_far(clean_positions):
    """A child rejecting mid-iceberg STOPS further children and registers the
    FILLED-so-far (a smaller REAL position), NEVER the intended qty.
    MUTATION REVERT: in session._place_iceberg change the partial-path
    `register_partial(..., filled_total, ...)` to `int(total_qty)` (register the
    intended qty) → the position shows 1000 not 600 → the qty==600 assert FAILS
    (this is exactly the phantom-fill the guard prevents)."""
    prof = _prof()
    # legs [300,300,300,100]; child0,1 fill, child2 rejects (reject_after=2).
    broker = MockBroker(profile=prof, dry_run=False, ltps={"ICBREJ": 100.0},
                        reject_after={"ICBREJ": 2})
    cfg = _cfg(iceberg_enabled=True, iceberg_slice_qty=300)
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess._place_one(
        broker, prof, Pick("ICBREJ", 1), 1_000_000.0, CapitalAllocator(cfg),
        forced_qty=1000))
    assert res["status"] == "PARTIAL"
    assert res["partial"] is True
    assert res["qty"] == 600                       # 2 children filled, not 1000
    row = _pos(sess.session_id, "ICBREJ")
    assert row is not None and row["qty"] == 600   # NOT the intended 1000
    # child3 was never placed (stopped at the reject)
    assert len(broker.placed) == 3


def test_cap3_entry_underfill_stops_and_registers_real(clean_positions):
    """A child that UNDER-fills (broker filled fewer than ordered) STOPS the
    iceberg and registers only the real filled qty.
    MUTATION REVERT: in _place_iceberg remove the `if child.get('under_fill'):
    break` → the loop keeps placing further children on top of an under-filled one
    → placed > 1 and qty != 200 → FAILS."""
    prof = _prof()
    # every placement of AAA fills only 200 of the 300 slice → under-fill on child0
    broker = MockBroker(profile=prof, dry_run=False, ltps={"ICBUND": 100.0},
                        partial_fills={"ICBUND": 200})
    cfg = _cfg(iceberg_enabled=True, iceberg_slice_qty=300)
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess._place_one(
        broker, prof, Pick("ICBUND", 1), 1_000_000.0, CapitalAllocator(cfg),
        forced_qty=1000))
    assert res["status"] == "PARTIAL"
    assert res["qty"] == 200
    assert len(broker.placed) == 1                 # stopped after the under-fill
    assert _pos(sess.session_id, "ICBUND")["qty"] == 200


def test_cap3_no_child_filled_no_phantom_position(clean_positions):
    """When the VERY FIRST child rejects (nothing filled) NO position is
    registered — never a phantom at the intended qty.
    MUTATION REVERT: in _place_iceberg remove the `if filled_total <= 0: return
    FAILED` early-out (fall through to register) → a 0-qty / intended-qty phantom
    row appears → the `row is None` assert FAILS."""
    prof = _prof()
    broker = MockBroker(profile=prof, dry_run=False, ltps={"ICBNIL": 100.0},
                        reject_after={"ICBNIL": 0})      # child0 rejects immediately
    cfg = _cfg(iceberg_enabled=True, iceberg_slice_qty=300)
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess._place_one(
        broker, prof, Pick("ICBNIL", 1), 1_000_000.0, CapitalAllocator(cfg),
        forced_qty=1000))
    assert res["status"] == "FAILED"
    assert _pos(sess.session_id, "ICBNIL") is None


# ══════════════════════════════════════════════════════════════════════════════
# CAP 4 — preview surfaces iceberg intent
# ══════════════════════════════════════════════════════════════════════════════

def test_cap4_preview_surfaces_iceberg_intent(clean_positions, monkeypatch):
    """The /preview response shows when a leg will iceberg (n_slices + slice_qty).
    MUTATION REVERT: remove the iceberg block in preview_session_sizing (the
    `if config.iceberg_enabled and len(_legs) > 1:` row-annotation) → the row has
    no `iceberg`/`n_slices` keys → the asserts FAIL."""
    ltps = {"ICBPRV": 100.0}
    monkeypatch.setattr(
        sess_mod, "build_client",
        lambda profile, dry_run=True: MockBroker(
            profile=profile, dry_run=True, ltps=ltps))
    seed_signals([("ICBPRV", 1, 9.0, 100.0)])
    cfg = _cfg(iceberg_enabled=True, iceberg_slice_qty=3000)
    out = sess_mod.preview_session_sizing(cfg, mode="paper")
    rows = [r for r in out["positions"]
            if r.get("symbol") == "ICBPRV" and r.get("status") != "SKIPPED"]
    assert rows, out["positions"]
    row = rows[0]
    assert row.get("iceberg") is True
    assert row["slice_qty"] == 3000
    assert row["n_slices"] == len(
        iceberg.plan_iceberg_legs(int(row["qty"]), 3000))
    assert row["n_slices"] > 1
