"""P0 REAL-MONEY REGRESSION — a PAPER session must NEVER influence a LIVE
session's broker-net computation.

THE BUG (2026-07-15, live session b447b0d7f6dc, MAPMYINDIA):
`sibling_open_qty` had NO mode filter, and a paper session's position rows carry
IDENTICAL scope keys (broker_profile 'default'/'zerodha_default',
broker_account_id NULL, product 'MIS'). So the PAPER session a1e789a5eeda's 843
OPEN qty was subtracted from the LIVE broker net:

    our_held = max(0, min(706, 2030 − (1324 live sibling + 843 PAPER)))
             = max(0, min(706, −137)) = 0

→ "broker net qty is 0 … marking CLOSED, placing NO order" → position id 246
booked STOP_RECONCILED_FLAT with exit_order_id NULL. **706 real MAPMYINDIA MIS
shares (~₹8.33L notional) were never sold by Falcon.** Without the paper term:
2030 − 1324 = 706 = exactly right (verified against the live DB).

THE FIX: broker reality contains only LIVE fills, so the sibling subtraction (and
the reconciler's account invariant) is scoped to sessions with the SAME
autotrade_sessions.mode. These tests FAIL before the fix and pass after.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now, _exit_single_position
from autotrade.monitoring.registry import (sibling_open_qty, our_held_at_broker,
                                           session_mode)
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)

SYM = "MAPMYINDIA"
# The EXACT reproduction numbers from the 2026-07-15 incident.
BROKER_NET = 2030      # account net at the broker (live fills ONLY)
OURS = 706             # live session b447b0d7f6dc  → was NEVER SOLD
LIVE_SIB = 1324        # live session bc0b38fabbc9  → exited correctly
PAPER_SIB = 843        # paper session a1e789a5eeda → must be INVISIBLE here


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _patch(monkeypatch, net_positions, ltps=None):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False,
                        ltps=ltps or {}, net_positions=net_positions)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _mk(monkeypatch, mode, net_positions, ltps=None, build=True):
    """A session in the given mode, holding MIS equity (the incident's product)."""
    created = _patch(monkeypatch, net_positions, ltps)
    cfg = TradingSessionConfig(total_allocated_capital=3_000_000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product="MIS")
    sess = TradingSession.create(cfg, mode=mode)
    if build:
        sess._build_brokers()
    return sess, created


def _prof(sess):
    return (sess.config.broker_profiles[0].profile_id
            if sess.config.broker_profiles else "default")


def _reg(sess, qty, symbol=SYM, avg=1180.0, ltp=1170.0):
    prof = _prof(sess)
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg, product="MIS", instrument_type="EQ",
                           exchange="NSE", direction="long")
    sess.registry.update_ltp(symbol, ltp, broker_profile=prof)


def _row(sess, symbol=SYM):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT status, qty, close_reason, exit_order_id FROM "
            "autotrade_positions WHERE session_id=? AND symbol=?",
            (sess.session_id, symbol)).fetchone()
    return dict(r) if r else None


# ═══════════════════════════════════════════════════════════════════════════
# TEST 1 — THE MONEY TEST: exact reproduction of the MAPMYINDIA incident.
# ═══════════════════════════════════════════════════════════════════════════
def test_paper_sibling_does_not_zero_live_our_held_exact_incident(
        clean_positions, monkeypatch):
    """LIVE session (706) + LIVE sibling (1324) + PAPER sibling (843), broker net
    2030 → our_held MUST be 706 (2030 − 1324), NOT 0.

    Revert the fix (drop the mode filter in sibling_open_qty) → siblings sum to
    1324+843=2167 → max(0, min(706, −137)) = 0 → the `== 706` assert FAILS.
    """
    live, _ = _mk(monkeypatch, "live", {SYM: BROKER_NET}, ltps={SYM: 1170.0})
    _reg(live, OURS)
    live_sib, _ = _mk(monkeypatch, "live", {SYM: BROKER_NET}, build=False)
    _reg(live_sib, LIVE_SIB)
    paper, _ = _mk(monkeypatch, "paper", {SYM: BROKER_NET}, build=False)
    _reg(paper, PAPER_SIB)

    # The modes are what we think they are (the scope keys are otherwise IDENTICAL).
    assert session_mode(live.session_id) == "live"
    assert session_mode(paper.session_id) == "paper"
    assert _prof(live) == _prof(paper)          # same profile string

    # Only the LIVE sibling counts against a LIVE session's broker net.
    assert sibling_open_qty(live.session_id, SYM, "EQ", product="MIS") == LIVE_SIB
    assert our_held_at_broker(live.session_id, SYM, "EQ", OURS, BROKER_NET,
                              product="MIS") == OURS


def test_live_kill_places_the_real_exit_despite_paper_sibling(
        clean_positions, monkeypatch):
    """End-to-end money test: the live session's kill MUST place a real 706-share
    exit — never the "broker net qty is 0 … placing NO order" / RECONCILED_FLAT
    path that stranded the shares.

    Revert the fix → our_held==0 → no exit is placed → `exits == [(SYM, 706)]`
    FAILS (and the row books a *_RECONCILED_FLAT / EXIT_FAILED instead).
    """
    live, created = _mk(monkeypatch, "live", {SYM: BROKER_NET}, ltps={SYM: 1170.0})
    _reg(live, OURS)
    live_sib, _ = _mk(monkeypatch, "live", {SYM: BROKER_NET}, build=False)
    _reg(live_sib, LIVE_SIB)
    paper, _ = _mk(monkeypatch, "paper", {SYM: BROKER_NET}, build=False)
    _reg(paper, PAPER_SIB)

    asyncio.run(live.kill_switch.fire("STOP", gross_return=-0.05))

    broker = created[_prof(live)]
    # THE ASSERT THAT WOULD HAVE SAVED ₹8.33L: a real exit for OUR full 706.
    assert (SYM, OURS) in broker.exits, f"no 706-share exit placed: {broker.exits}"
    row = _row(live)
    assert row["status"] == "CLOSED"
    # NEITHER phantom-close path was taken.
    assert "RECONCILED_FLAT" not in str(row["close_reason"])
    assert row["exit_order_id"] not in (None, "")
    # The paper sibling was not touched by a live kill.
    assert _row(paper)["status"] == "OPEN"


def test_exit_single_position_path_also_immune(clean_positions, monkeypatch):
    """The per-stock-stop / retry path (_exit_single_position, session.py:1345) uses
    the same helper and must equally ignore the paper sibling."""
    live, created = _mk(monkeypatch, "live", {SYM: BROKER_NET}, ltps={SYM: 1170.0})
    _reg(live, OURS)
    live_sib, _ = _mk(monkeypatch, "live", {SYM: BROKER_NET}, build=False)
    _reg(live_sib, LIVE_SIB)
    paper, _ = _mk(monkeypatch, "paper", {SYM: BROKER_NET}, build=False)
    _reg(paper, PAPER_SIB)

    pos = live.registry.get_open_positions()[0]
    res = asyncio.run(_exit_single_position(
        session_id=live.session_id, position=pos, reason="PER_STOCK_STOP",
        brokers=live.brokers, registry=live.registry,
        gtt_manager=live.gtt_manager, kite_product="MIS"))

    assert res["status"] != "RECONCILED_FLAT"
    assert (SYM, OURS) in created[_prof(live)].exits


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2 — property-style: a paper sibling NEVER changes a live our_held.
# ═══════════════════════════════════════════════════════════════════════════
@pytest.mark.parametrize("net,ours,paper_qty", [
    (2030, 706, 843),      # the incident
    (706, 706, 1),         # a 1-share paper row must not shave our exit
    (706, 706, 10_000),    # an absurd paper row must not zero us
    (1000, 400, 600),      # paper exactly "fills" the remaining net
    (500, 700, 900),       # broker holds LESS than ours → clamp to net, not 0
    (0, 700, 900),         # genuinely flat → 0 regardless of paper
])
def test_paper_sibling_never_perturbs_live_our_held(clean_positions, monkeypatch,
                                                    net, ours, paper_qty):
    """For any combo, a LIVE session's our_held == the value it would have with NO
    paper session in the DB at all = max(0, min(ours, net))."""
    live, _ = _mk(monkeypatch, "live", {SYM: net})
    _reg(live, ours)
    baseline = our_held_at_broker(live.session_id, SYM, "EQ", ours, net,
                                  product="MIS")
    assert baseline == max(0, min(ours, net))

    paper, _ = _mk(monkeypatch, "paper", {SYM: net}, build=False)
    _reg(paper, paper_qty)
    after = our_held_at_broker(live.session_id, SYM, "EQ", ours, net,
                               product="MIS")
    assert after == baseline, (
        f"paper sibling qty={paper_qty} perturbed live our_held "
        f"{baseline} → {after}")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3 — the LIVE shared-login guard must NOT regress (the no-fire complement).
# ═══════════════════════════════════════════════════════════════════════════
def test_live_sibling_still_subtracted(clean_positions, monkeypatch):
    """A LIVE sibling IS in the broker net → it MUST still be subtracted.

    Cross-check on the incident's other leg (bc0b38fabbc9, 1324): with our 706 live
    sibling present its held = min(1324, 2030−706) = 1324 → it takes its exit
    (matching reality: id 247 exited with a real order-id). And the guard itself:
    when the live sibling's lot is ALL the broker has, our_held → 0 (no oversell).
    """
    live_sib, _ = _mk(monkeypatch, "live", {SYM: BROKER_NET})
    _reg(live_sib, LIVE_SIB)
    ours, _ = _mk(monkeypatch, "live", {SYM: BROKER_NET}, build=False)
    _reg(ours, OURS)
    paper, _ = _mk(monkeypatch, "paper", {SYM: BROKER_NET}, build=False)
    _reg(paper, PAPER_SIB)

    # bc0b38fabbc9's view: live sibling 706 subtracted, paper 843 ignored.
    assert sibling_open_qty(live_sib.session_id, SYM, "EQ", product="MIS") == OURS
    assert our_held_at_broker(live_sib.session_id, SYM, "EQ", LIVE_SIB, BROKER_NET,
                              product="MIS") == LIVE_SIB


def test_live_sibling_zeroes_us_and_blocks_oversell(clean_positions, monkeypatch):
    """THE GUARD (must not regress): the broker net holds ONLY the live sibling's
    lot → our_held == 0 → NO exit is placed (never sell into a sibling's shares).
    A paper row present alongside changes nothing."""
    live, created = _mk(monkeypatch, "live", {SYM: LIVE_SIB}, ltps={SYM: 1170.0})
    _reg(live, OURS)
    live_sib, _ = _mk(monkeypatch, "live", {SYM: LIVE_SIB}, build=False)
    _reg(live_sib, LIVE_SIB)
    paper, _ = _mk(monkeypatch, "paper", {SYM: LIVE_SIB}, build=False)
    _reg(paper, PAPER_SIB)

    assert our_held_at_broker(live.session_id, SYM, "EQ", OURS, LIVE_SIB,
                              product="MIS") == 0

    asyncio.run(live.kill_switch.fire("STOP", gross_return=-0.05))
    broker = created[_prof(live)]
    assert not any(s == SYM for s, _q in broker.exits)   # NO oversell
    assert _row(live_sib)["status"] == "OPEN"            # sibling untouched


def test_phantom_close_guard_still_fires(clean_positions, monkeypatch):
    """23ed40f's guard COEXISTS with the fix: when our_held==0 is inferred by a
    LIVE sibling subtraction while the broker STILL holds net!=0 and there is no
    confirmed fill of OUR order, the leg is kept OPEN (EXIT_FAILED) + paged —
    never a silent mark-price phantom close. The mode fix makes our_held CORRECT;
    this guard remains the backstop for the genuinely-ambiguous live case."""
    live, created = _mk(monkeypatch, "live", {SYM: LIVE_SIB}, ltps={SYM: 1170.0})
    _reg(live, OURS)
    live_sib, _ = _mk(monkeypatch, "live", {SYM: LIVE_SIB}, build=False)
    _reg(live_sib, LIVE_SIB)

    asyncio.run(live.kill_switch.fire("STOP", gross_return=-0.05))

    row = _row(live)
    # Kept OPEN-ish for retry, NOT silently closed at the mark.
    assert row["status"] == "EXIT_FAILED"
    assert "NOT phantom-closing" in str(row["close_reason"])
    assert not any(s == SYM for s, _q in created[_prof(live)].exits)


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4 — PAPER session scoping is correct / unaffected.
# ═══════════════════════════════════════════════════════════════════════════
def test_paper_session_scopes_to_paper_siblings(clean_positions, monkeypatch):
    """Symmetry: a PAPER session's sibling sum counts ONLY paper rows — a LIVE
    session's shares are not the paper book's."""
    paper_a, _ = _mk(monkeypatch, "paper", {SYM: BROKER_NET})
    _reg(paper_a, 100)
    paper_b, _ = _mk(monkeypatch, "paper", {SYM: BROKER_NET}, build=False)
    _reg(paper_b, 250)
    live, _ = _mk(monkeypatch, "live", {SYM: BROKER_NET}, build=False)
    _reg(live, OURS)

    # paper_a sees ONLY paper_b (250) — never the live 706.
    assert sibling_open_qty(paper_a.session_id, SYM, "EQ", product="MIS") == 250


def test_paper_broker_net_none_still_returns_none(clean_positions, monkeypatch):
    """Paper's broker_net is None → our_held_at_broker returns None → the caller
    runs its normal (synthetic) exit. Byte-for-byte unchanged."""
    paper, _ = _mk(monkeypatch, "paper", {SYM: BROKER_NET})
    _reg(paper, 100)
    assert our_held_at_broker(paper.session_id, SYM, "EQ", 100, None) is None


def test_unknown_session_id_keeps_legacy_unscoped_behaviour(clean_positions,
                                                            monkeypatch):
    """A session_id with NO autotrade_sessions row has no mode evidence → NO mode
    filter → byte-for-byte the pre-fix behaviour (protects synthetic callers and
    the existing cluster-9/10 unit tests)."""
    from autotrade.monitoring.registry import PositionRegistry
    reg = PositionRegistry("orphan-sess-A", 1_000_000.0)
    reg.register(symbol="ZZZ", broker_profile="p1", qty=100, avg_price=10.0,
                 product="MIS", instrument_type="EQ", exchange="NSE")
    assert session_mode("orphan-sess-A") is None
    assert sibling_open_qty("orphan-sess-B", "ZZZ", "EQ") == 100


# ═══════════════════════════════════════════════════════════════════════════
# TEST 5 — _account_open_positions_for mode scoping.
# ═══════════════════════════════════════════════════════════════════════════
def test_account_open_positions_mode_scoped(clean_positions, monkeypatch):
    """The reconciler's account invariant (db_held_all) must count LIVE rows only:
    broker_held is LIVE broker reality, so a paper row inflates the left side.

    Revert (drop the mode_scope filter) → the live query returns 2 rows /
    706+843=1549 → the `[OURS]` assert FAILS.
    """
    from autotrade.monitoring.position_reconciler import (
        _account_open_positions_for)

    live, _ = _mk(monkeypatch, "live", {SYM: BROKER_NET})
    _reg(live, OURS)
    paper, _ = _mk(monkeypatch, "paper", {SYM: BROKER_NET}, build=False)
    _reg(paper, PAPER_SIB)

    prof_scope = [_prof(live)]
    live_rows = _account_open_positions_for(SYM, "MIS", prof_scope, {},
                                            mode_scope="live", _mode_cache={})
    assert [int(r["qty"]) for r in live_rows] == [OURS]

    paper_rows = _account_open_positions_for(SYM, "MIS", prof_scope, {},
                                             mode_scope="paper", _mode_cache={})
    assert [int(r["qty"]) for r in paper_rows] == [PAPER_SIB]

    # mode_scope None (legacy caller) = unfiltered = the pre-fix behaviour.
    both = _account_open_positions_for(SYM, "MIS", prof_scope, {})
    assert sorted(int(r["qty"]) for r in both) == sorted([OURS, PAPER_SIB])
