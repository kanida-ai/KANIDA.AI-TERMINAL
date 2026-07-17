"""FALCON INTRADAY MAGNIFIER — NO per-position broker stop, NO per-position target.

WHY (measured live 2026-07-17, ₹30L capital, 5× MIS):
The Magnifier spec (docs/strategy/FALCON_INTRADAY_MAGNIFIER.docx) defines exactly
ONE risk model: a BASKET trail on the CAPITAL basis (arm 6% / floor 2% /
giveback 5% / stop 3%) + the 15:29 square-off. It says NOTHING about per-position
stops or targets.

But ladder._make_child_config's MAGNIFIER branch never overrode
per_position_stop_pct (0.08) / per_position_target_pct (0.20), so the child
inherited the generic TradingSessionConfig defaults. Those defaults are fractions
of the position's CAPITAL and are correct at 1× CNC (8% of capital == 8% of
price). GTTManager converts capital→price via the session leverage L
(price_pct = capital_pct / L), so at the Magnifier's ~5× MIS an 8% capital stop
silently becomes a ~1.6% PRICE stop — a hair-trigger INSIDE a mid-cap's normal
intraday noise band.

What it cost, on ONE position:
  AEGISLOG entry 1325.84, broker SL-M at 1301.79 (-1.81% of price), fired 11:13
  → realised -₹56,050. The stock CLOSED at 1345.00 (ohlc_daily: O=1306.8 H=1358.0
  L=1293.0 C=1345.0). Had it held: +₹43,417. Cost of that one stop: -₹99,467.
  The day's high (1358) was ABOVE the trail's arm (~1341.7 = +6% of capital), so
  the INTENDED trail would have armed and locked profit. The BASKET stop (-3% of
  ₹30L = -₹90,000 across ALL names) would NOT have fired on AEGISLOG's -₹56k.
  The backtest HELD. The unmodelled execution stop SOLD.

The target side is the same defect: gtt_target for KALYANKJIL computed at 572.82;
KALYANKJIL closed at 577.00 and earned +₹159,523. A live GTT would have CAPPED
it. It survived only because gtt_id=None (MIS legs suppress the GTT) — luck of an
unrelated guard, not design.

THE MECHANISM (verified in source, a6a0f54):
  * GTTManager.place_for_position is the ONLY code path that places EITHER broker
    instrument — it is the sole caller of broker.place_gtt_oco AND of
    _place_protective_slm → broker.place_protective_slm.
  * place_for_position is reachable ONLY via GTTManager.backfill_missing().
  * backfill_missing is gated by config.per_position_gtt_enabled at EVERY call
    site (session.py:2638/3426/4895/5162, recovery.py:83) AND internally
    (gtt_manager.py:334 early-returns []).
  → per_position_gtt_enabled=False SKIPS placement entirely. That is the clean
    disable: no stop and no target ever reach the broker. It is strictly better
    than a "so wide it never fires" level — a number that never fires is still a
    number that CAN fire on a limit-down day.

BLAST RADIUS = the MAGNIFIER branch ONLY. intraday_basket / btst_oscillator /
positional children and every direct (non-ladder) session keep
per_position_gtt_enabled=True + the 8%/20% defaults — correct at their 1× products.
Pinned by the regression tests at the bottom.
"""
from datetime import datetime, timedelta, timezone

import pytest

from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.ladder import (
    LadderCampaign, CAMPAIGN_MAGNIFIER, CAMPAIGN_BTST, CAMPAIGN_POSITIONAL,
    MAGNIFIER_ARM_PCT, MAGNIFIER_FLOOR_PCT, MAGNIFIER_GIVEBACK_PCT,
    MAGNIFIER_STOP_PCT,
)
from autotrade.monitoring.gtt_manager import GTTManager
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring import trail_engine
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))

# Today's live numbers (2026-07-17), reproduced exactly.
AEGIS_ENTRY = 1325.84
AEGIS_QTY = 2266
CAPITAL_PER_NAME = 600000.0
AEGIS_KILLER_STOP = 1301.79   # the SL-M that actually fired, -₹56,050
AEGIS_CLOSE = 1345.00         # where it actually closed (+₹43,417 had it held)


class _SpyBroker(MockBroker):
    """Records every place_gtt_oco call. place_protective_slm is already recorded
    by MockBroker in .slm_orders. Together these are the ONLY two ways a
    per-position stop/target can reach a broker."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.gtt_placed = []

    def place_gtt_oco(self, symbol, qty, stop_price, target_price, last_price,
                      product="CNC", exchange="NSE", order_type="LIMIT",
                      stop_limit_price=None, direction="long"):
        self.gtt_placed.append({"symbol": symbol, "stop": stop_price,
                                "target": target_price})
        return f"GTT-{symbol}"


def _magnifier_child(**overrides):
    """The REAL Magnifier child config, straight from the production factory."""
    return LadderCampaign._make_child_config(
        CAPITAL_PER_NAME, "MIS", overrides, campaign_type=CAMPAIGN_MAGNIFIER)


def _aegis_registry(session_id, capital=CAPITAL_PER_NAME):
    """A one-name basket holding today's real AEGISLOG position. Σnotional /
    capital = (2266 × 1325.84) / 600,000 ≈ 5.0 → the Magnifier's 5× MIS leverage,
    which is exactly what divides the 8% capital stop down into the noise band."""
    reg = PositionRegistry(session_id, capital)
    reg.register(symbol="AEGISLOG", broker_profile="zer", qty=AEGIS_QTY,
                 avg_price=AEGIS_ENTRY, product="MIS", instrument_type="EQ")
    return reg


def _prof(cfg):
    return BrokerProfile(profile_id="zer", broker_name="zerodha",
                         allocated_capital=cfg.total_allocated_capital,
                         order_product=cfg.order_product)


def _gm(session_id, cfg, reg, broker):
    cfg.broker_profiles = [_prof(cfg)]
    return GTTManager(session_id, cfg, {"zer": broker}, reg)


# ══════════════════════════════════════════════════════════════════════════════
# THE FIX — a Magnifier child places NO per-position stop and NO target
# ══════════════════════════════════════════════════════════════════════════════

def test_magnifier_child_disables_per_position_gtt():
    """The single gate that skips ALL per-position broker placement."""
    cfg = _magnifier_child()
    assert cfg.strategy == "intraday_magnifier"
    assert cfg.per_position_gtt_enabled is False
    cfg.validate()   # config validation must still accept the Magnifier config


def test_magnifier_places_no_slm_and_no_gtt_on_todays_aegislog(clean_positions):
    """THE REGRESSION THAT COST ₹99,467.

    Today's exact position (entry 1325.84, qty 2266, ₹600,000/name → ~5× MIS)
    through the REAL production path: the real child factory → the real
    GTTManager → backfill_missing(). Assert the broker methods are NEVER called:
    no SL-M at 1301.79 (or anywhere), and no GTT (so no target cap either)."""
    cfg = _magnifier_child()
    reg = _aegis_registry("sess-mag-aegis")
    broker = _SpyBroker(profile=_prof(cfg), dry_run=False)
    gm = _gm("sess-mag-aegis", cfg, reg, broker)

    results = gm.backfill_missing()

    # The broker was never asked to place anything, by either mechanism.
    assert broker.slm_orders == [], (
        f"a per-position SL-M was placed: {broker.slm_orders} — this is the "
        f"unmodelled stop that sold AEGISLOG at {AEGIS_KILLER_STOP} for -₹56,050 "
        f"on a day it closed at {AEGIS_CLOSE}")
    assert broker.gtt_placed == [], (
        f"a per-position GTT was placed: {broker.gtt_placed} — the target leg "
        f"would CAP the upside (cf. KALYANKJIL +₹159,523)")
    assert results == []          # the gated path did no work at all

    # Nothing was persisted either — no stop/target level is recorded anywhere,
    # so nothing downstream can resurrect one.
    pos = reg.get_open_positions()[0]
    assert pos.get("slm_order_id") in (None, "")
    assert pos.get("gtt_id") in (None, "")
    assert pos.get("gtt_stop") in (None, 0, 0.0)
    assert pos.get("gtt_target") in (None, 0, 0.0)


def test_the_defect_is_real_the_old_config_DID_place_the_killer_stop(clean_positions):
    """PROOF THE FIX MATTERS (and that the test above isn't vacuous).

    Same basket, same leverage — but with the OLD inherited behaviour
    (per_position_gtt_enabled=True). The SL-M IS placed, and it lands inside
    AEGISLOG's normal intraday noise band: ~1.6% of price (8% capital / 5× MIS),
    within a rupee or two of the 1301.79 that actually fired. It is also ABOVE the
    day's LOW (1293.0) → it really would fire, on a day that closed at 1345.00."""
    cfg = _magnifier_child()
    cfg.per_position_gtt_enabled = True          # simulate the pre-fix config
    reg = _aegis_registry("sess-mag-old")
    broker = _SpyBroker(profile=_prof(cfg), dry_run=False)
    gm = _gm("sess-mag-old", cfg, reg, broker)

    gm.backfill_missing()

    assert len(broker.slm_orders) == 1, "pre-fix, the SL-M WAS placed"
    trig = broker.slm_orders[0]["trigger_price"]
    # ~1.6% below entry — leverage silently divided the 8% capital stop by ~5.
    assert 1300.0 < trig < 1310.0, trig
    assert (AEGIS_ENTRY - trig) / AEGIS_ENTRY == pytest.approx(0.016, abs=0.002)
    # It sits ABOVE the day's low (1293.0) → it fires. And below the close
    # (1345.0) → firing it turned a winner into a -₹56k loss.
    assert 1293.0 < trig < AEGIS_CLOSE
    # MIS legs suppress the GTT, so the TARGET was only ever recorded, not placed
    # — the upside cap survived on luck. Post-fix it isn't even computed.
    assert broker.gtt_placed == []


# ══════════════════════════════════════════════════════════════════════════════
# THE RISK MODEL MUST NOT REGRESS — the BASKET trail + square-off are the whole
# Magnifier risk model, so they carry all the weight now. Fire AND no-fire.
# ══════════════════════════════════════════════════════════════════════════════

def _mag_params():
    return trail_engine.params_from_config(_magnifier_child())


def _midday():
    """11:13 IST — the minute the AEGISLOG stop actually fired, and safely before
    the 15:29 square-off so the time branch never masks a trail assertion."""
    return datetime(2026, 7, 17, 11, 13, 0, tzinfo=IST)


def test_magnifier_trail_preset_is_unchanged():
    cfg = _magnifier_child()
    assert (cfg.arm_pct, cfg.floor_pct, cfg.trail_giveback_pct, cfg.stop_pct) == \
        (MAGNIFIER_ARM_PCT, MAGNIFIER_FLOOR_PCT,
         MAGNIFIER_GIVEBACK_PCT, MAGNIFIER_STOP_PCT)
    assert (cfg.arm_pct, cfg.floor_pct, cfg.trail_giveback_pct, cfg.stop_pct) == \
        (0.06, 0.02, 0.05, 0.03)
    assert cfg.trail_step_lock_enabled is False   # fixed-floor path


def test_basket_stop_does_NOT_fire_above_threshold():
    """NO-FIRE: -2.9% of capital is inside the -3% basket stop → HOLD."""
    d = trail_engine.decide(-0.029, trail_engine.TrailState(), _mag_params(),
                            now=_midday())
    assert d.action == "HOLD"


def test_basket_stop_FIRES_below_threshold():
    """FIRE: -3.1% of capital breaches the -3% basket stop → EXIT/STOP."""
    d = trail_engine.decide(-0.031, trail_engine.TrailState(), _mag_params(),
                            now=_midday())
    assert (d.action, d.reason) == ("EXIT", "STOP")


def test_aegislog_alone_would_not_have_tripped_the_basket_stop():
    """THE POINT OF THE WHOLE FIX. AEGISLOG's -₹56,050 on the ₹30L basket is
    -1.87% of capital — inside the -3% basket stop (-₹90,000). The risk model
    that was actually backtested says HOLD. It held to +₹43,417 by the close."""
    g = -56050.0 / 3000000.0
    assert g == pytest.approx(-0.01868, abs=1e-4)
    d = trail_engine.decide(g, trail_engine.TrailState(), _mag_params(),
                            now=_midday())
    assert d.action == "HOLD", "the backtested basket stop would NOT have fired"


def test_trail_arms_at_6pct():
    """NO-FIRE below arm, ARM at +6% of capital. AEGISLOG's high (1358) was above
    the ~+6% arm level, so the intended trail would have armed and locked profit."""
    p = _mag_params()
    assert trail_engine.decide(0.059, trail_engine.TrailState(), p,
                               now=_midday()).action == "HOLD"
    d = trail_engine.decide(0.061, trail_engine.TrailState(), p, now=_midday())
    assert d.action == "ARM"
    assert d.state.armed is True and d.state.peak == pytest.approx(0.061)


def test_armed_trail_giveback_and_floor_exits():
    p = _mag_params()
    armed = trail_engine.TrailState(armed=True, peak=0.12)
    # trigger = max(peak - giveback, floor) = max(0.07, 0.02) = 0.07.
    # (Binary float: 0.12-0.05 is 0.0699999...; stay off the exact boundary so
    # this pins BEHAVIOUR, not float representation.)
    assert trail_engine.decide(0.08, armed, p, now=_midday()).action == "HOLD"
    d = trail_engine.decide(0.0699, armed, p, now=_midday())
    assert (d.action, d.reason) == ("EXIT", "TRAIL_EXIT")
    assert d.trigger == pytest.approx(0.07)
    # FLOOR binds when peak-giveback drops below floor_pct (0.02): peak 0.065
    # → 0.015 < 0.02 → trigger clamps to the 2% floor.
    low = trail_engine.TrailState(armed=True, peak=0.065)
    assert trail_engine.decide(0.03, low, p, now=_midday()).action == "HOLD"
    d2 = trail_engine.decide(0.02, low, p, now=_midday())
    assert (d2.action, d2.reason) == ("EXIT", "FLOOR_EXIT")


def test_square_off_still_fires():
    cfg = _magnifier_child()
    assert cfg.square_off_enabled is True
    assert cfg.square_off_time == "15:29:00"
    p = _mag_params()
    # 15:28 → not yet; 15:29 → SQUARE_OFF regardless of a healthy +4% G.
    assert trail_engine.decide(
        0.04, trail_engine.TrailState(), p,
        now=datetime(2026, 7, 17, 15, 28, 0, tzinfo=IST)).action == "HOLD"
    d = trail_engine.decide(
        0.04, trail_engine.TrailState(), p,
        now=datetime(2026, 7, 17, 15, 29, 0, tzinfo=IST))
    assert (d.action, d.reason) == ("EXIT", "SQUARE_OFF")


# ══════════════════════════════════════════════════════════════════════════════
# BLAST RADIUS = EXACTLY ONE STRATEGY. Everything else keeps its per-position
# stop + target at the CURRENT values (correct at their 1× products).
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("campaign,strategy,product", [
    (CAMPAIGN_POSITIONAL, "intraday_basket", "CNC"),
    (CAMPAIGN_POSITIONAL, "intraday_basket", "MTF"),
    (CAMPAIGN_BTST, "btst_oscillator", "CNC"),
])
def test_other_ladder_children_keep_per_position_stop_and_target(
        campaign, strategy, product):
    cfg = LadderCampaign._make_child_config(500000.0, product, {},
                                        campaign_type=campaign)
    assert cfg.strategy == strategy
    assert cfg.per_position_gtt_enabled is True, (
        f"{campaign} must KEEP its per-position broker backstop")
    assert cfg.per_position_stop_pct == 0.08
    assert cfg.per_position_target_pct == 0.20
    cfg.validate()


def test_direct_non_ladder_session_is_unchanged():
    """A plain session built straight from TradingSessionConfig (no ladder) keeps
    the shipped defaults — this change touched no shared default."""
    cfg = TradingSessionConfig(total_allocated_capital=500000.0)
    assert cfg.per_position_gtt_enabled is True
    assert cfg.per_position_stop_pct == 0.08
    assert cfg.per_position_target_pct == 0.20
    # ...and a plain intraday_basket / tesla_short session too.
    for strat, prod, extra in (("intraday_basket", "MIS", {}),
                               ("tesla_short", "MIS", {"direction": "short"})):
        c = TradingSessionConfig(total_allocated_capital=500000.0,
                                 strategy=strat, order_product=prod,
                                 instrument_type="EQ", **extra)
        assert c.per_position_gtt_enabled is True
        assert c.per_position_stop_pct == 0.08
        assert c.per_position_target_pct == 0.20


def test_a_positional_child_DOES_place_its_stop(clean_positions):
    """The other side of the line: a 1× CNC positional child still gets a REAL
    broker stop+target. At L=1 the 8% capital stop == an 8% PRICE stop — a sane
    far-out disaster backstop, which is exactly why it stays."""
    cfg = LadderCampaign._make_child_config(500000.0, "CNC", {},
                                        campaign_type=CAMPAIGN_POSITIONAL)
    reg = PositionRegistry("sess-pos-keeps", 500000.0)
    reg.register(symbol="AAA", broker_profile="zer", qty=100, avg_price=1000.0,
                 product="CNC", instrument_type="EQ")
    broker = _SpyBroker(profile=_prof(cfg), dry_run=False)
    gm = _gm("sess-pos-keeps", cfg, reg, broker)

    gm.backfill_missing()

    # CNC → a real GTT-OCO, at 1× so capital% == price%: stop 8%, target 20%.
    assert len(broker.gtt_placed) == 1
    assert broker.gtt_placed[0]["stop"] == pytest.approx(920.0)
    assert broker.gtt_placed[0]["target"] == pytest.approx(1200.0)


def test_per_position_gtt_enabled_is_not_operator_overridable():
    """The ladder override whitelist cannot re-arm the per-position stop on a
    Magnifier campaign — per_position_gtt_enabled is not in it. (The two pct
    knobs ARE whitelisted but are inert while the gate is False.)"""
    assert "per_position_gtt_enabled" not in LadderCampaign.LADDER_CHILD_WHITELIST
    # Even if an operator saved those pcts, the Magnifier still places nothing.
    cfg = _magnifier_child(per_position_stop_pct=0.02,
                           per_position_target_pct=0.05)
    assert cfg.per_position_gtt_enabled is False
