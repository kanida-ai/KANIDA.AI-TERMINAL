"""BROKER-AGNOSTIC OMS — `broker_held` is the ADAPTER's book semantics, not a
KITE-tuned formula baked into the reconciler.

`broker_held = holdings(quantity + t1_quantity) + max(0, net)` is TRUE FOR KITE
(today's unsettled CNC buys sit in positions()['net'], reaching holdings only on
T+1) and was applied to ALL brokers. For a broker whose holdings book ALREADY
contains today's buys it DOUBLE-COUNTS them — a ~2x broker surplus, the suspected
source of 3 bogus CORP_ACTION_SUSPECTED alerts on Rupeezy CNC same-day buys.

The rule now lives on BrokerClient (broker_held_qty +
CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS) so each broker maps its OWN semantics. There
is NO `if broker == "..."` branch in the reconciler. The DEFAULT is the Kite rule,
so every existing adapter is byte-for-byte unchanged.
"""
import pytest

from autotrade.broker.zerodha import ZerodhaBroker
from autotrade.broker.rupeezy import RupeezyBroker
from tests.autotrade.mock_broker import MockBroker


class _Prof:
    profile_id = "p1"
    broker = "test"
    access_token = ""
    api_key = ""


class _KiteLike(MockBroker):
    """Holdings EXCLUDE today's buys (Kite) — inherits the BrokerClient default."""
    broker_name = "kitelike"


class _HoldingsInclusive(MockBroker):
    """Holdings ALREADY include today's buys — the adapter DECLARES it; the
    reconciler never learns the broker's name."""
    broker_name = "inclusive"
    CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS = True


def _b(cls):
    return cls(_Prof(), dry_run=True)


# ── the default (Kite) rule is preserved exactly ─────────────────────────────
@pytest.mark.parametrize("holdings,net,expect", [
    (35, 57, 92),     # LIVE 2026-07-06 AEGISLOG: holdings 35 + today's 57 buys.
    (35, -57, 35),    # LIVE 2026-07-08 AEGISLOG: other sessions' sells → still 35.
    (0, -12, 0),      # LIVE ACUTAAS fully sold → floors to 0 (never negative).
    (0, 0, 0),
    (100, 0, 100),    # settled overnight delivery, no day activity.
])
def test_default_cnc_rule_is_the_verified_kite_formula(holdings, net, expect):
    assert _b(_KiteLike).broker_held_qty(
        product="CNC", holdings_qty=holdings, net_qty=net) == expect
    # And the shipped Zerodha adapter declares exactly this.
    assert ZerodhaBroker.CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS is False


# ── the over-count this fixes ────────────────────────────────────────────────
def test_holdings_inclusive_broker_does_not_double_count_same_day_buy():
    """A same-day CNC buy of 57 that the broker ALREADY shows in holdings (92 =
    35 settled + 57 today) must read as 92 — NOT 92 + 57 = 149 (~the 2x surplus
    that fired the bogus CORP_ACTION_SUSPECTED alerts)."""
    kite_rule = _b(_KiteLike).broker_held_qty(
        product="CNC", holdings_qty=92, net_qty=57)
    assert kite_rule == 149                      # the BUG when misapplied
    own_rule = _b(_HoldingsInclusive).broker_held_qty(
        product="CNC", holdings_qty=92, net_qty=57)
    assert own_rule == 92                        # the broker's OWN semantics


def test_holdings_inclusive_sell_is_not_double_subtracted():
    """A CNC sell is already out of holdings → the negative net must not reduce it
    again (the same trap the Kite rule's max(0, net) avoids)."""
    assert _b(_HoldingsInclusive).broker_held_qty(
        product="CNC", holdings_qty=35, net_qty=-57) == 35
    assert _b(_HoldingsInclusive).broker_held_qty(
        product="CNC", holdings_qty=0, net_qty=-12) == 0


# ── non-CNC is universal across brokers ──────────────────────────────────────
@pytest.mark.parametrize("cls", [_KiteLike, _HoldingsInclusive])
@pytest.mark.parametrize("product,net,expect", [
    ("MIS", 706, 706),
    ("MIS", 0, 0),
    ("MTF", -630, 630),      # LIVE AARTIIND short MTF: |net| is the held size.
    ("NRML", -75, 75),
])
def test_non_cnc_is_abs_net_for_every_broker(cls, product, net, expect):
    """No holdings for MIS/NRML/MTF — |signed net| IS the exposure, and holdings_qty
    is ignored regardless of the adapter's CNC declaration."""
    assert _b(cls).broker_held_qty(
        product=product, holdings_qty=9999, net_qty=net) == expect


# ── shipped adapters ─────────────────────────────────────────────────────────
def test_every_adapter_inherits_the_hook():
    """Broker-agnostic: the OMS asks the ADAPTER, so every adapter must answer."""
    for cls in (ZerodhaBroker, RupeezyBroker):
        assert hasattr(cls, "broker_held_qty")
        assert isinstance(cls.CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS, bool)


def test_rupeezy_left_at_the_safe_default_pending_certification():
    """Rupeezy's holdings semantics are UNCERTIFIED (get_holdings is verified only
    against an all-zero live book), so the flag stays False = today's behaviour.
    Flipping it is a real-money reconciliation decision that needs ONE live
    non-zero holdings response + the same day's net. This test pins that we did
    NOT guess; flip it deliberately (with the evidence) when certifying."""
    assert RupeezyBroker.CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS is False


def test_reconciler_has_no_broker_name_branch():
    """The operator's HARD requirement: no `if broker == "zerodha"` in the OMS."""
    import inspect
    import io
    import tokenize
    from autotrade.monitoring import position_reconciler
    src = inspect.getsource(position_reconciler)
    # Strip comments + docstrings so this file's OWN prose about the banned
    # pattern doesn't trip it; judge the executable code only.
    code_lines = []
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            continue
        code_lines.append(tok.string)
    code = " ".join(code_lines)
    for needle in ('broker_name ==', 'broker ==', 'zerodha', 'rupeezy'):
        assert needle not in code, f"broker-specific branch found: {needle}"
