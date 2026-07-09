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
    """A full basket fire in marketable_limit mode (ENTRY): the normal legs are
    genuine MARKET orders (fill at the touch on ANY gap — no tight limit a gap-up
    could jump), and the symbol LOCKED at its upper circuit is PLACED as a LIMIT
    AT the circuit (queued, the CEMPRO case). No leg is skipped/dropped; panel ==
    intended, no phantom position."""
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
    # A, B: normal book → genuine MARKET orders (guaranteed fill at the touch).
    assert placed["A"].order_type == "MARKET" and placed["A"].price is None
    assert placed["B"].order_type == "MARKET" and placed["B"].price is None
    # The locked leg is placed AT the upper circuit (a queued LIMIT, not skipped).
    assert placed["C"].order_type == "LIMIT" and placed["C"].price == 110.0
    assert len(broker.placed) == 3


# ── ENTRY mode (entry=True): guarantee the fill (2026-07-09 KALYANKJIL) ──────

def test_entry_buy_normal_book_is_market_order():
    """A normal book (a live ask below the upper circuit) → a genuine MARKET
    order, so a gap-up can never leave the entry unfilled (the KALYANKJIL miss:
    a 0.3%-through-touch LIMIT sat below a +2-3% opening gap and never filled)."""
    cfg = _cfg()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0)
    plan = plan_marketable_order("BUY", "X", 10, q, 0.05, cfg, entry=True)
    assert plan["fallback_market"] is True and plan["order_type"] == "MARKET"
    assert plan["price"] is None


def test_entry_buy_locked_at_upper_circuit_is_limit_at_circuit():
    """Locked at the upper circuit (no ask) → a LIMIT queued AT the circuit
    (a MARKET buy on a locked-up stock would be rejected 'outside circuit')."""
    cfg = _cfg()
    q = _q(110.0, bid=109.95, ask=None, upper=110.0, lower=90.0)
    plan = plan_marketable_order("BUY", "CEMPRO", 10, q, 0.05, cfg, entry=True)
    assert plan["ok"] is True and plan["order_type"] == "LIMIT"
    assert plan["price"] == 110.0


def test_entry_buy_ltp_at_band_is_limit_at_circuit():
    """LTP printing at the upper band (locked up) → LIMIT at the circuit even if
    a stale ask is present."""
    cfg = _cfg()
    q = _q(110.0, bid=109.9, ask=110.0, upper=110.0, lower=90.0)
    plan = plan_marketable_order("BUY", "X", 10, q, 0.05, cfg, entry=True)
    assert plan["order_type"] == "LIMIT" and plan["price"] == 110.0


def test_entry_sell_normal_book_is_market_order():
    """Short entry, normal book → MARKET (fills at the bid on a gap-down)."""
    cfg = _cfg()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0)
    plan = plan_marketable_order("SELL", "X", 10, q, 0.05, cfg, entry=True)
    assert plan["fallback_market"] is True and plan["order_type"] == "MARKET"


def test_entry_sell_locked_at_lower_circuit_is_limit():
    cfg = _cfg()
    q = _q(90.0, bid=None, ask=90.05, upper=110.0, lower=90.0)
    plan = plan_marketable_order("SELL", "X", 10, q, 0.05, cfg, entry=True)
    assert plan["order_type"] == "LIMIT" and plan["price"] == 90.0


def test_entry_no_price_at_all_is_market_fallback():
    cfg = _cfg()
    plan = plan_marketable_order("BUY", "X", 10, None, 0.05, cfg, entry=True)
    assert plan["fallback_market"] is True and plan["order_type"] == "MARKET"


def test_exit_pricing_unchanged_by_entry_default():
    """entry=False (the EXIT default) is byte-for-byte the old buffer pricing —
    the entry change must NOT touch the exit path."""
    cfg = _cfg()
    q = _q(100.0, bid=99.9, ask=100.1, upper=110.0, lower=90.0)
    plan = plan_marketable_order("BUY", "X", 10, q, 0.05, cfg)  # entry defaults False
    assert plan["order_type"] == "LIMIT" and plan["price"] == 100.40
    plan_s = plan_marketable_order("SELL", "X", 10, q, 0.05, cfg)
    assert plan_s["order_type"] == "LIMIT" and plan_s["price"] == 99.65


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


# ── PHANTOM-FILL FIX: an unfilled entry is cancelled + dropped, never marked ──

class _PendingThenTerminalBroker:
    """An entry order that is STILL PENDING while polled, then reports a terminal
    state ONLY after cancel_order_sync is called. `partial`/`avg` model a real
    partial fill that landed just before the cancel; 0 = fully unfilled."""

    def __init__(self, partial: int = 0, avg: float = 0.0):
        self.cancelled: list = []
        self._partial = partial
        self._avg = avg
        self._cancelled_flag = False

    def get_order_status(self, order_id: str) -> dict:
        if not self._cancelled_flag:
            # still working at the exchange — no fill yet
            return {"status": "OPEN", "filled_quantity": 0, "average_price": 0.0}
        if self._partial > 0:
            return {"status": "CANCELLED", "filled_quantity": self._partial,
                    "average_price": self._avg}
        return {"status": "CANCELLED", "filled_quantity": 0, "average_price": 0.0}

    def cancel_order_sync(self, order_id: str) -> bool:
        self._cancelled_flag = True
        self.cancelled.append(order_id)
        return True


def _live_session(monkeypatch):
    """A session forced onto the LIVE reconcile path (dry_run=False) with the
    KiteTicker postback stubbed out so the poll+cancel logic runs immediately."""
    from autotrade.session import TradingSession
    sess = TradingSession.create(_cfg(), mode="paper")
    sess.dry_run = False

    async def _no_postback(order_id, timeout=1.5):
        return None
    monkeypatch.setattr(sess, "_await_order_postback", _no_postback)
    return sess


def test_entry_reconcile_timeout_cancels_and_drops_when_zero_filled(monkeypatch):
    """The KALYANKJIL bug: a pending entry that never fills must be CANCELLED and
    the leg DROPPED — the reconcile signals 'rejected' so _place_one never books a
    phantom at the reference mark."""
    sess = _live_session(monkeypatch)
    broker = _PendingThenTerminalBroker(partial=0)
    rec = asyncio.run(sess._reconcile_entry_fill(
        broker, "ord-X", 10, max_wait_sec=0.2, poll_interval=0.05))
    assert rec == {"rejected": True, "status": "CANCELLED_UNFILLED"}
    assert broker.cancelled == ["ord-X"]    # we actively cancelled the stuck order


def test_entry_reconcile_timeout_registers_only_the_real_partial(monkeypatch):
    """If a partial actually filled before the cancel landed, register EXACTLY
    those shares at the real avg — never the full intended qty, never the mark."""
    sess = _live_session(monkeypatch)
    broker = _PendingThenTerminalBroker(partial=4, avg=101.0)
    rec = asyncio.run(sess._reconcile_entry_fill(
        broker, "ord-X", 10, max_wait_sec=0.2, poll_interval=0.05))
    assert rec == {"avg_price": 101.0, "filled_qty": 4}
    assert broker.cancelled == ["ord-X"]
