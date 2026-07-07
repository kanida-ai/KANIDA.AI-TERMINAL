"""Quote-driven marketable-LIMIT execution — pricer math + entry/exit wiring.

PHILOSOPHY under test: the execution layer ALWAYS PLACES an order and maximises
fill speed. The pricer NEVER skips — a circuit-locked stock is placed as a LIMIT
exactly AT the circuit (a valid, queued order that fills when the lock breaks,
the 2026-07-06 CEMPRO fix); with no usable price the caller uses a MARKET
fallback. execution_mode="market" (DEFAULT) is byte-for-byte the old MARKET path.
"""
from __future__ import annotations

import asyncio

import pytest

from autotrade.config import BrokerProfile, TradingSessionConfig
from autotrade.execution.quote_pricer import plan_marketable_order
from autotrade.execution.orders import build_order
from tests.autotrade.mock_broker import MockBroker


def _cfg(**kw) -> TradingSessionConfig:
    base = dict(total_allocated_capital=100_000.0,
                execution_mode="marketable_limit",
                marketable_buffer_pct=0.003, top_n_stocks=5)
    base.update(kw)
    return TradingSessionConfig(**base)


def _q(ltp, bid=None, ask=None, upper=None, lower=None, ts=0.0):
    return {"ltp": ltp, "bid": bid, "ask": ask,
            "upper_circuit": upper, "lower_circuit": lower, "ts": ts}


# ── Pricer math ──────────────────────────────────────────────────────────────

def test_buy_marketable_limit_is_ask_plus_buffer_tick_rounded():
    cfg = _cfg()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0)
    plan = plan_marketable_order("BUY", "X", 10, q, 0.05, cfg)
    assert plan["ok"] is True and plan["order_type"] == "LIMIT"
    # 100.1 * 1.003 = 100.4003 → floor to 0.05 tick = 100.40
    assert plan["price"] == 100.40


def test_sell_marketable_limit_is_bid_minus_buffer_tick_rounded():
    cfg = _cfg()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0)
    plan = plan_marketable_order("SELL", "X", 10, q, 0.05, cfg)
    assert plan["ok"] is True and plan["order_type"] == "LIMIT"
    # 99.9 * 0.997 = 99.6003 → ceil to 0.05 tick = 99.65
    assert plan["price"] == 99.65


def test_buy_capped_at_upper_circuit_when_ask_near_it():
    """Ask above the upper band → the LIMIT is capped AT the upper circuit
    (tick-floored so it never exceeds the band). No skip."""
    cfg = _cfg()
    q = _q(109.5, bid=109.4, ask=109.97, upper=110.0, lower=90.0)
    plan = plan_marketable_order("BUY", "X", 10, q, 0.10, cfg)
    assert plan["ok"] is True and plan["price"] == 110.0
    assert plan["price"] <= q["upper_circuit"]


def test_sell_floored_at_lower_circuit():
    cfg = _cfg()
    q = _q(90.5, bid=90.03, ask=90.6, upper=110.0, lower=90.0)
    plan = plan_marketable_order("SELL", "X", 10, q, 0.10, cfg)
    assert plan["ok"] is True and plan["price"] == 90.0
    assert plan["price"] >= q["lower_circuit"]


# ── THE CEMPRO CASE: locked at circuit → PLACED at the circuit (not skipped) ──

def test_buy_locked_at_upper_circuit_places_limit_at_circuit():
    """CEMPRO: a stock pinned at its UPPER circuit. Old behaviour = MARKET buy
    rejected "outside circuit limits". New behaviour = a LIMIT placed EXACTLY AT
    the upper circuit — a valid, queued order that fills when the lock breaks.
    NEVER a skip."""
    cfg = _cfg()
    q = _q(110.0, bid=109.95, ask=None, upper=110.0, lower=90.0)
    plan = plan_marketable_order("BUY", "CEMPRO", 10, q, 0.05, cfg)
    assert plan["ok"] is True
    assert plan.get("skip") is None  # no skip outcome exists any more
    assert plan["order_type"] == "LIMIT"
    assert plan["price"] == 110.0


def test_sell_locked_at_lower_circuit_places_limit_at_circuit():
    cfg = _cfg()
    q = _q(90.0, bid=None, ask=90.05, upper=110.0, lower=90.0)
    plan = plan_marketable_order("SELL", "X", 10, q, 0.05, cfg)
    assert plan["ok"] is True and plan["order_type"] == "LIMIT"
    assert plan["price"] == 90.0


# ── ALWAYS-PLACE guarantees (no skip states remain) ──────────────────────────

def test_stale_quote_is_still_used_never_skipped():
    """A stale quote (old ts) is STILL priced — staleness never skips."""
    cfg = _cfg()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0, ts=0.0)
    # now is implicitly time.time() (huge) → the quote is "very stale" but used.
    plan = plan_marketable_order("BUY", "X", 10, q, 0.05, cfg)
    assert plan["ok"] is True and plan["order_type"] == "LIMIT"


def test_wide_book_no_deviation_skip():
    """A price far from LTP (wide/locked book) is NOT skipped — it is capped by
    the circuit band only, and placed."""
    cfg = _cfg()
    # ask 30% above ltp but a tight circuit caps it at the band.
    q = _q(100.0, bid=99.0, ask=130.0, upper=105.0, lower=95.0)
    plan = plan_marketable_order("BUY", "X", 10, q, 0.05, cfg)
    assert plan["ok"] is True and plan["price"] == 105.0  # capped at upper


def test_ask_missing_falls_back_to_ltp_capped_by_circuit():
    cfg = _cfg()
    q = _q(100.0, bid=99.9, ask=None, upper=110.0, lower=90.0)
    plan = plan_marketable_order("BUY", "X", 10, q, 0.05, cfg)
    # 100.0 * 1.003 = 100.3 → tick 100.30
    assert plan["ok"] is True and plan["price"] == 100.30


def test_bid_missing_falls_back_to_ltp():
    cfg = _cfg()
    q = _q(100.0, bid=None, ask=100.1, upper=110.0, lower=90.0)
    plan = plan_marketable_order("SELL", "X", 10, q, 0.05, cfg)
    # 100.0 * 0.997 = 99.7 → ceil tick 99.70
    assert plan["ok"] is True and plan["price"] == 99.70


def test_quote_none_but_ltp_fallback_still_limits():
    cfg = _cfg()
    plan = plan_marketable_order("BUY", "X", 10, None, 0.05, cfg,
                                 ltp_fallback=100.0)
    assert plan["ok"] is True and plan["order_type"] == "LIMIT"
    assert plan["price"] == 100.30


def test_no_price_at_all_is_market_fallback_never_skip():
    cfg = _cfg()
    plan = plan_marketable_order("BUY", "X", 10, None, 0.05, cfg)
    assert plan["ok"] is False
    assert plan["fallback_market"] is True
    assert plan["order_type"] == "MARKET"
    assert plan.get("skip") is None


def test_invalid_side_is_market_fallback():
    cfg = _cfg()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0)
    plan = plan_marketable_order("HOLD", "X", 10, q, 0.05, cfg)
    assert plan["fallback_market"] is True and plan["order_type"] == "MARKET"


# ── Default execution_mode="market" is byte-for-byte the old MARKET path ─────

def test_default_execution_mode_builds_market_order_no_price():
    cfg = TradingSessionConfig(total_allocated_capital=100_000.0)  # default
    assert cfg.execution_mode == "market"
    broker = MockBroker(BrokerProfile("p1", "mock"), ltps={"X": 100.0})
    order = build_order("X", 10, cfg, broker)
    assert order.order_type == "MARKET"
    assert order.price is None


def test_config_validate_rejects_bad_execution_mode():
    cfg = TradingSessionConfig(total_allocated_capital=100_000.0,
                               execution_mode="nonsense")
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_validate_rejects_misscaled_buffer():
    cfg = TradingSessionConfig(total_allocated_capital=100_000.0,
                               execution_mode="marketable_limit",
                               marketable_buffer_pct=5.0)  # 500% — mis-scaled
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_round_trips_execution_fields():
    cfg = _cfg(marketable_buffer_pct=0.004)
    cfg.validate()
    d = cfg.to_dict()
    back = TradingSessionConfig.from_dict(d)
    assert back.execution_mode == "marketable_limit"
    assert back.marketable_buffer_pct == 0.004


# ── MockBroker get_quotes extension ──────────────────────────────────────────

def test_mock_broker_get_quotes_returns_injected_book():
    q = {"CEMPRO": _q(110.0, bid=109.9, ask=None, upper=110.0, lower=90.0),
         "INFY": _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0)}
    broker = MockBroker(BrokerProfile("p1", "mock"), quotes=q)
    out = broker.get_quotes(["CEMPRO", "INFY"])
    assert out["CEMPRO"]["upper_circuit"] == 110.0
    assert out["INFY"]["ask"] == 100.1


def test_mock_broker_get_quotes_none_when_unset():
    broker = MockBroker(BrokerProfile("p1", "mock"))
    assert broker.get_quotes(["X"]) is None  # safe sentinel → MARKET/fallback


# ── Live-session fire: one leg at upper circuit → LIMIT at circuit, no phantom ─

@pytest.fixture
def patched_quote_brokers(monkeypatch):
    """Patch build_client to return quote-aware MockBrokers (shared ltps + a
    shared quote book with one symbol LOCKED at its upper circuit)."""
    created = {}
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 110.0}
    shared_quotes = {
        "A": _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0),
        "B": _q(200.0, bid=199.5, ask=200.5, upper=220.0, lower=180.0),
        # C is LOCKED at its upper circuit (the CEMPRO case).
        "C": _q(110.0, bid=109.9, ask=None, upper=110.0, lower=90.0),
    }

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps,
                        quotes=shared_quotes)
        created[profile.profile_id] = mb
        return mb

    import autotrade.broker.router as router_mod
    import autotrade.session as sess_mod
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_fire_prices_limit_at_circuit_for_locked_leg_no_phantom(
        clean_positions, patched_quote_brokers):
    """A full basket fire in marketable_limit mode: the symbol LOCKED at its
    upper circuit is PLACED as a LIMIT AT the circuit (queued), the others as
    LIMIT at ask+buffer. No leg is skipped/dropped; panel == intended, no
    phantom position."""
    from autotrade.session import TradingSession
    from tests.autotrade.conftest import seed_signals
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 110.0)])
    cfg = _cfg(top_n_stocks=3, sizing_mode="equal")
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start())

    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 3          # NO leg skipped/dropped
    # No phantom: exactly the 3 open positions we intended.
    assert sess.status()["n_open_positions"] == 3

    broker = next(iter(patched_quote_brokers.values()))
    placed = {o.symbol: o for o in broker.placed}
    # Every leg is a marketable LIMIT.
    assert placed["A"].order_type == "LIMIT" and placed["A"].price == 100.40
    # B: ask 200.5 * 1.003 = 201.1015 → floor to 0.05 tick = 201.10.
    assert placed["B"].order_type == "LIMIT" and placed["B"].price == 201.10
    # The locked leg is placed AT the upper circuit (a queued LIMIT, not skipped).
    assert placed["C"].order_type == "LIMIT" and placed["C"].price == 110.0
    assert len(broker.placed) == 3


# ── Exit: marketable-limit threads exec_cfg; MARKET fallback keeps exiting ────

def test_exit_threads_exec_cfg_and_always_exits():
    """A paper exit in marketable_limit mode still PLACES (mock returns PLACED)
    and records exec_cfg — the exit never abandons the position."""
    cfg = _cfg()
    broker = MockBroker(BrokerProfile("p1", "mock"), dry_run=True,
                        ltps={"INFY": 100.0})

    async def _run():
        return await broker.place_market_exit("INFY", 10, "EQ",
                                              kite_product="CNC",
                                              direction="long", exec_cfg=cfg)

    res = asyncio.run(_run())
    assert res.status == "PLACED"           # the mock always places the exit
    assert broker.exit_calls[-1]["exec_cfg"] is cfg
    assert broker.exits == [("INFY", 10)]   # the exit fired
