"""SPRINT CLUSTER 8 ITEM 3 — concentration / fat-finger limits + risk_basis label.

A per-ORDER cap on how much a single name may take (max_pct_per_name × capital, an
absolute ₹ notional cap, and a qty sanity cap) CLAMPS (default) or REFUSES a
breaching leg with a clear reason. An explicit risk_basis label + the ₹ thresholds
are surfaced in the preview/start response.

Each test PASSES with the fix and FAILS on the stated revert. Paper-safe.
"""
import asyncio

import pytest

import autotrade.session as sess_mod
from autotrade import risk_manager
from autotrade.risk_manager import check_concentration
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


# ══════════════════════════════════════════════════════════════════════════════
# check_concentration — pure decision
# ══════════════════════════════════════════════════════════════════════════════
def test_concentration_clamp_max_pct_per_name():
    """max_pct_per_name 0.4 × ₹1,000,000 = ₹400,000 cap; a ₹800,000 notional
    (8000×₹100) is CLAMPED to 4000 shares (default policy=clamp).

    MUTATION REVERT: in risk_manager.check_concentration delete the ₹-notional-cap
    clamp block (`if notional_cap is not None and ... qty = max_qty`) → qty stays
    8000 → `d.qty == 4000` and `d.clamped` FAIL."""
    cfg = _cfg(max_pct_per_name=0.4)
    d = check_concentration(cfg, "AAA", 8000, 100.0)
    assert d.refused is False
    assert d.clamped is True
    assert d.qty == 4000                       # 400,000 / 100
    assert d.cap_rs == pytest.approx(400_000.0)


def test_concentration_inert_when_unset():
    """No caps set (all None) → INERT: the qty passes through unchanged."""
    cfg = _cfg()
    d = check_concentration(cfg, "AAA", 8000, 100.0)
    assert d.qty == 8000 and d.clamped is False and d.refused is False


def test_concentration_refuse_policy():
    """policy='refuse' → a breaching leg is DROPPED (qty 0, refused)."""
    cfg = _cfg(max_pct_per_name=0.4, fatfinger_policy="refuse")
    d = check_concentration(cfg, "AAA", 8000, 100.0)
    assert d.refused is True and d.qty == 0


def test_concentration_abs_notional_and_qty_caps():
    """The absolute ₹ notional cap and the qty sanity cap both clamp."""
    d1 = check_concentration(_cfg(fatfinger_max_notional_per_order=250_000.0),
                             "AAA", 8000, 100.0)
    assert d1.qty == 2500                       # 250,000 / 100
    d2 = check_concentration(_cfg(fatfinger_max_qty_per_order=500),
                             "AAA", 8000, 100.0)
    assert d2.qty == 500 and d2.clamped is True


# ══════════════════════════════════════════════════════════════════════════════
# _place_one integration — clamp / refuse a live leg
# ══════════════════════════════════════════════════════════════════════════════
def test_place_one_clamps_concentration(clean_positions):
    """_place_one CLAMPS a leg breaching max_pct_per_name and registers the smaller
    REAL position (4000, not 8000).

    MUTATION REVERT: delete the `_conc = _check_conc(...)` concentration block in
    session._place_one → the full 8000 is placed/registered → `row['qty'] == 4000`
    FAILS."""
    prof = _prof()
    broker = MockBroker(profile=prof, dry_run=False, ltps={"CONC": 100.0})
    cfg = _cfg(max_pct_per_name=0.4)
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess._place_one(
        broker, prof, Pick("CONC", 1), 1_000_000.0, CapitalAllocator(cfg),
        forced_qty=8000))
    assert res["status"] != "FAILED"
    assert res["qty"] == 4000
    assert broker.placed[0].qty == 4000
    assert _pos(sess.session_id, "CONC")["qty"] == 4000


def test_place_one_refuses_whitelist_collapse(clean_positions):
    """A whitelist collapsing to ONE name that would take the whole book is REFUSED
    (policy=refuse) — NO order placed, NO phantom position.

    MUTATION REVERT: delete the concentration block in _place_one → the 8000-share
    order is placed → `broker.placed == []` and `row is None` FAIL."""
    prof = _prof()
    broker = MockBroker(profile=prof, dry_run=False, ltps={"ONLY": 100.0})
    cfg = _cfg(max_pct_per_name=0.4, fatfinger_policy="refuse")
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess._place_one(
        broker, prof, Pick("ONLY", 1), 1_000_000.0, CapitalAllocator(cfg),
        forced_qty=8000))
    assert res["status"] == "SKIPPED"
    assert res.get("concentration_refused") is True
    assert broker.placed == []
    assert _pos(sess.session_id, "ONLY") is None


def test_place_one_inert_by_default(clean_positions):
    """No caps set → _place_one places the full forced_qty (byte-for-byte control)."""
    prof = _prof()
    broker = MockBroker(profile=prof, dry_run=False, ltps={"NORM": 100.0})
    cfg = _cfg()
    sess = TradingSession.create(cfg, mode="live")
    asyncio.run(sess._place_one(
        broker, prof, Pick("NORM", 1), 1_000_000.0, CapitalAllocator(cfg),
        forced_qty=3000))
    assert broker.placed[0].qty == 3000
    assert _pos(sess.session_id, "NORM")["qty"] == 3000


# ══════════════════════════════════════════════════════════════════════════════
# preview surfaces risk_basis + ₹ thresholds
# ══════════════════════════════════════════════════════════════════════════════
def test_preview_surfaces_risk_basis_and_thresholds(clean_positions, monkeypatch):
    """/preview carries the risk_basis label and the ₹ concentration thresholds so
    the leverage math is unambiguous.

    MUTATION REVERT: remove the `'risk_basis'` + `'concentration_limits'` keys added
    to preview_session_sizing's return dict → the asserts on those keys FAIL."""
    monkeypatch.setattr(
        sess_mod, "build_client",
        lambda profile, dry_run=True: MockBroker(
            profile=profile, dry_run=True, ltps={"RBP": 100.0}))
    seed_signals([("RBP", 1, 9.0, 100.0)])
    cfg = _cfg(max_pct_per_name=0.4, fatfinger_max_notional_per_order=250_000.0,
               risk_basis="notional")
    out = sess_mod.preview_session_sizing(cfg, mode="paper")
    assert out["risk_basis"] == "notional"
    cl = out["concentration_limits"]
    assert cl["max_per_name_rs"] == pytest.approx(400_000.0)     # 0.4 × 1,000,000
    assert cl["max_notional_per_order_rs"] == pytest.approx(250_000.0)


def test_config_validation_and_roundtrip():
    """The new fields validate + round-trip through to_dict/from_dict.

    MUTATION REVERT: remove the risk_basis/fatfinger validation block in
    config.validate() → the bad-value asserts stop raising → FAIL."""
    with pytest.raises(ValueError):
        _cfg(max_pct_per_name=1.5).validate()
    with pytest.raises(ValueError):
        _cfg(fatfinger_policy="bogus").validate()
    with pytest.raises(ValueError):
        _cfg(risk_basis="fund").validate()
    good = _cfg(max_pct_per_name=0.35, fatfinger_max_notional_per_order=500_000.0,
                fatfinger_max_qty_per_order=1000, fatfinger_policy="refuse",
                risk_basis="capital")
    good.validate()
    rt = TradingSessionConfig.from_dict(good.to_dict())
    assert rt.max_pct_per_name == 0.35
    assert rt.fatfinger_max_notional_per_order == 500_000.0
    assert rt.fatfinger_max_qty_per_order == 1000
    assert rt.fatfinger_policy == "refuse"
    assert rt.risk_basis == "capital"
