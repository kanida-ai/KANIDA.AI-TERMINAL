"""BUG 2 (2026-07-15) — phantom-close of a still-held position on a SHARED Kite
login (₹5L session b447b0d7, MAPMYINDIA 706 qty).

MAPMYINDIA was the ONLY name in BOTH the ₹25L (1324) and ₹5L (706) sessions on
ONE shared login (broker held 2030). The ₹5L STOP fired while the ₹25L had
already sold its 1324 at the broker but its DB row was not yet marked closed. The
sibling-subtraction our_held = clamp(net − sibling) = clamp(706 − 1324) = 0 → the
kill/reconcile path concluded "our shares are flat" and marked the position
POSITION_CLOSED close_reason=STOP_RECONCILED_FLAT at a PHANTOM mark (₹1167.10),
exit_order_id NULL, NO exit ever placed. The 706 shares stayed live; the operator
sold them manually at 11:27.

Fix (kill_switch.fire reconcile-flat branch): a mark-price RECONCILED_FLAT now
requires the broker to be GENUINELY flat (net_qty == 0). When the broker STILL
holds shares (net_qty != 0) and there is NO confirmed close of OUR order, the
exit is left OPEN (mark_exit_failed → tick retries) and PAGES
PHANTOM_CLOSE_PREVENTED — never a silent mark-price close.

MUTATION-VERIFIED (test_shared_login_race_does_not_phantom_close):
  Change the guard `elif net_qty and float(net_qty) != 0:` back to
  `elif ltp_val and float(ltp_val) > 0:` (the old branch) → the position is
  marked CLOSED/STOP_RECONCILED_FLAT and this test's OPEN + no-alert asserts fail.
"""
import asyncio
import uuid

from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from tests.autotrade.mock_broker import MockBroker


def _sid():
    return uuid.uuid4().hex


def _session_row(sid, cap=500000.0):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json)
               VALUES (?,?,?,?,?,?)""",
            (sid, "2026-07-15T09:15:00", "RUNNING", "live", cap, "{}"))
        con.commit()


def _register(sid, symbol, qty, avg, ltp, product="MIS", direction="long"):
    reg = PositionRegistry(sid, 0)
    reg.register(symbol=symbol, broker_profile="zer", qty=qty, avg_price=avg,
                 product=product, instrument_type="EQ", direction=direction,
                 client_order_id="FAL-" + symbol)
    reg.update_ltp(symbol, ltp)
    return reg


def _cfg():
    return TradingSessionConfig(
        total_allocated_capital=500000.0, top_n_stocks=3, sizing_mode="equal",
        order_product="MIS", instrument_type="EQ",
        per_position_stop_pct=0.03, per_position_target_pct=0.06)


def _row(reg, symbol):
    for r in reg.get_all_positions():
        if r["symbol"] == symbol:
            return r
    return None


def _alerts(kind):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        return con.execute(
            "SELECT COUNT(*) AS n FROM autotrade_alerts WHERE kind=?",
            (kind,)).fetchone()["n"]


def test_shared_login_race_does_not_phantom_close(clean_positions):
    """THE MAPMYINDIA case: sibling's stale DB qty drives our_held to a FALSE 0
    while the broker STILL holds OUR 706 shares → must NOT phantom-close."""
    sib = _sid()          # the ₹25L session (already sold 1324 at broker; DB stale)
    ours = _sid()         # the ₹5L session (706 still genuinely held)
    _session_row(sib)
    _session_row(ours)
    # Stale sibling row: still OPEN in the DB at 1324 (broker already sold it).
    _register(sib, "MAPMYINDIA", 1324, 1180.0, 1167.10)
    reg = _register(ours, "MAPMYINDIA", 706, 1180.19, 1167.10)
    # Broker physically holds ONLY our 706 now (sibling's 1324 already sold).
    #   our_held = clamp(706 − 1324) = 0  → the reconcile-flat branch is entered,
    #   but net_qty (706) != 0 → the guard must prevent the phantom close.
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        net_positions={"MAPMYINDIA": 706})
    ks = KillSwitchExecutor(ours, _cfg(), {"zer": broker}, reg)
    asyncio.run(ks.fire("STOP", close_reason="STOP"))

    row = _row(reg, "MAPMYINDIA")
    assert row["status"] != "CLOSED"                     # NOT phantom-closed
    assert row["status"] == "EXIT_FAILED"                # kept, tick will retry
    assert (row["close_reason"] or "") != "STOP_RECONCILED_FLAT"
    assert row["exit_order_id"] is None
    assert ("MAPMYINDIA", 706) not in broker.exits       # NO exit placed
    assert _alerts("PHANTOM_CLOSE_PREVENTED") == 1       # paged


def test_genuinely_flat_still_reconciles_closed(clean_positions):
    """CONTROL: broker GENUINELY flat (net_qty == 0, no sibling) → the legitimate
    mark-price RECONCILED_FLAT still closes (no regression to the real flat case)."""
    ours = _sid()
    _session_row(ours)
    reg = _register(ours, "NUVOCO", 100, 350.0, 348.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        net_positions={"NUVOCO": 0})     # objectively gone
    ks = KillSwitchExecutor(ours, _cfg(), {"zer": broker}, reg)
    asyncio.run(ks.fire("STOP", close_reason="STOP"))

    row = _row(reg, "NUVOCO")
    assert row["status"] == "CLOSED"
    assert row["close_reason"] == "STOP_RECONCILED_FLAT"
    assert ("NUVOCO", 100) not in broker.exits           # closed via reconcile
    assert _alerts("PHANTOM_CLOSE_PREVENTED") == 0


def test_held_position_exits_normally(clean_positions):
    """CONTROL: a genuinely-held position (our_held > 0, no overlap) exits via a
    real market order and closes — the happy path is unchanged."""
    ours = _sid()
    _session_row(ours)
    reg = _register(ours, "KALYANKJIL", 200, 500.0, 495.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        net_positions={"KALYANKJIL": 200})   # fully held
    ks = KillSwitchExecutor(ours, _cfg(), {"zer": broker}, reg)
    asyncio.run(ks.fire("STOP", close_reason="STOP"))

    row = _row(reg, "KALYANKJIL")
    assert row["status"] == "CLOSED"
    assert ("KALYANKJIL", 200) in broker.exits           # real exit placed
    assert _alerts("PHANTOM_CLOSE_PREVENTED") == 0
