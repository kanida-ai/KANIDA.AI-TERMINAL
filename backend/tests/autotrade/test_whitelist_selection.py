"""Custom `symbol_whitelist` must win: every whitelisted name in today's ranked
Falcon set (up to the Top-50 selectable range) is sized/traded, IGNORING the
`top_n_stocks` cap. `top_n_stocks` still governs the DEFAULT (no-whitelist) top-N.

Regression for the "custom list silently truncated" bug: the pick loader in the
preview, warm-resolve and real-fire paths loaded only `max(top_n_stocks, 10)` and
then re-capped at `top_n_stocks`, so a whitelisted name at rank 11..50 (or beyond
the top-N) was silently dropped. All three paths now share
`session._resolve_falcon_selection(config)`.

All paper / dry-run — `router.build_client` is monkeypatched to MockBrokers (no
real Kite). "now" is frozen to a mid-session NSE trading day so the fire gate is
deterministic. The ranked set is seeded via `seed_signals` so the assertions
exercise the REAL `load_falcon_picks` + the REAL `_resolve_falcon_selection`.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import (
    TradingSession, set_fake_now, preview_session_sizing,
    _resolve_falcon_selection)
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)   # Thu, mid-session

# Ranked universe mirroring the live kanida_universe.db ordering the task cites:
# SWIGGY=2, WELCORP=6, LODHA=11, CEMPRO=15 (+ fillers so ranks 1..15 are dense).
# (symbol, rank, score, close_price)
_UNIVERSE = [
    ("TOP1", 1, 15.0, 100.0),
    ("SWIGGY", 2, 14.0, 400.0),
    ("TOP3", 3, 13.0, 100.0),
    ("MAPMYINDIA", 4, 12.0, 1600.0),
    ("TOP5", 5, 11.0, 100.0),
    ("WELCORP", 6, 10.0, 500.0),
    ("TOP7", 7, 9.0, 100.0),
    ("NAUKRI", 8, 8.0, 1200.0),
    ("TOP9", 9, 7.0, 100.0),
    ("TOP10", 10, 6.0, 100.0),
    ("LODHA", 11, 5.0, 1100.0),
    ("TOP12", 12, 4.0, 100.0),
    ("TOP13", 13, 3.0, 100.0),
    ("TOP14", 14, 2.0, 100.0),
    ("CEMPRO", 15, 1.0, 900.0),
]
_LTPS = {sym: close for (sym, _r, _s, close) in _UNIVERSE}


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


@pytest.fixture
def patched_brokers(monkeypatch):
    """Every broker is a MockBroker that knows every universe symbol's LTP so
    equal/cash sizing produces a real qty for each whitelisted name."""
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=dict(_LTPS))
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _seed():
    seed_signals([(sym, rank, score, close)
                  for (sym, rank, score, close) in _UNIVERSE])


def _open_symbols(session_id):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        return {r["symbol"] for r in con.execute(
            "SELECT symbol FROM autotrade_positions "
            "WHERE session_id=? AND status='OPEN'", (session_id,)).fetchall()}


def _preview_sized_symbols(result):
    return {p["symbol"] for p in result["positions"]
            if p.get("status") != "SKIPPED" and p.get("qty", 0) > 0}


# ── 1. A rank-11+ whitelisted name is sized in PREVIEW and traded in FIRE ──────

def test_whitelist_includes_rank11plus_preview_and_fire(clean_positions,
                                                        patched_brokers):
    """SWIGGY(2), WELCORP(6), LODHA(11), CEMPRO(15) whitelisted with top_n=5.
    LODHA(11) and CEMPRO(15) are BEYOND the old load depth (max(5,10)=10) — they
    must still be sized (preview) and traded (fire).

    MUTATION-REVERT: change `_resolve_falcon_selection` load depth back to
    `max(config.top_n_stocks, 10)` (i.e. drop the `50 if symbol_whitelist` branch)
    → LODHA(11)/CEMPRO(15) are never loaded → both asserts below fail."""
    _seed()
    wl = ["SWIGGY", "WELCORP", "LODHA", "CEMPRO"]
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0, top_n_stocks=5,
                               sizing_mode="equal", order_product="CNC",
                               kill_switch_enabled=False, symbol_whitelist=wl)

    # Direct helper: the deep load must surface the rank-11+ names, and the router
    # cap must equal the number of matched picks (all 4), not top_n_stocks (5).
    picks, cap = _resolve_falcon_selection(cfg)
    got = {p.symbol for p in picks}
    assert {"LODHA", "CEMPRO"} <= got, got
    assert cap == 4

    # PREVIEW (money-shape, places nothing).
    prev = preview_session_sizing(cfg, mode="paper")
    sized = _preview_sized_symbols(prev)
    assert {"LODHA", "CEMPRO"} <= sized, sized

    # FIRE (paper — real entry path).
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    opened = _open_symbols(sess.session_id)
    assert opened == set(wl), opened
    assert {"LODHA", "CEMPRO"} <= opened


# ── 2. A whitelist LARGER than top_n_stocks trades ALL of them (cap bypassed) ──

def test_whitelist_larger_than_topn_trades_all(clean_positions, patched_brokers):
    """top_n_stocks=2 but a 4-name whitelist (all within the top-10 so LOAD DEPTH
    is not the variable — this isolates the ROUTER CAP). All 4 must trade.

    MUTATION-REVERT: set the router cap back to `config.top_n_stocks` (i.e. return
    `config.top_n_stocks` instead of `len(picks)` in the whitelist branch) →
    route_picks caps the default branch at 2 by rank → only TOP1/SWIGGY trade →
    the `== 4` asserts fail."""
    _seed()
    wl = ["TOP1", "SWIGGY", "TOP3", "MAPMYINDIA"]   # ranks 1,2,3,4 — all in top-10
    cfg = TradingSessionConfig(total_allocated_capital=1_000_000.0, top_n_stocks=2,
                               sizing_mode="equal", order_product="CNC",
                               kill_switch_enabled=False, symbol_whitelist=wl)

    _picks, cap = _resolve_falcon_selection(cfg)
    assert cap == 4                      # NOT top_n_stocks (2)

    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 4
    assert _open_symbols(sess.session_id) == set(wl)


# ── 3. DEFAULT (no whitelist) is byte-identical: same top-N + same router cap ──

def test_default_no_whitelist_byte_identical(clean_positions, patched_brokers):
    """No whitelist → load depth = max(top_n_stocks, 10) (NOT 50) and router cap =
    top_n_stocks, so exactly the top-N by rank trade — unchanged behaviour.

    MUTATION-REVERT (deepen): if the helper loads 50 for the no-whitelist path,
    `len(picks) == 10` fails (15 seeded → 15 loaded).
    MUTATION-REVERT (uncap): if router cap becomes `len(picks)` for the
    no-whitelist path, `cap == 3` fails AND the fire opens 10 names not 3."""
    _seed()
    cfg = TradingSessionConfig(total_allocated_capital=900_000.0, top_n_stocks=3,
                               sizing_mode="equal", order_product="CNC",
                               kill_switch_enabled=False)

    picks, cap = _resolve_falcon_selection(cfg)
    assert cap == 3                      # == top_n_stocks (unchanged)
    assert len(picks) == 10              # max(top_n_stocks, 10), NOT 50

    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 3
    # Exactly the top-3 by rank — same set the pre-fix code produced.
    assert _open_symbols(sess.session_id) == {"TOP1", "SWIGGY", "TOP3"}


# ── 4. The "not in today's picks" warning is preserved for genuinely-absent names

def test_whitelist_absent_symbol_warns_and_is_dropped(clean_positions,
                                                      patched_brokers, caplog):
    """A whitelisted symbol that is NOT in the ranked top-50 is warned about and
    dropped; the present names still trade.

    MUTATION-REVERT: delete the `log.warning(... "not in today's picks" ...)` block
    in `_resolve_falcon_selection` → the caplog assertion fails."""
    _seed()
    wl = ["SWIGGY", "LODHA", "GHOSTSTOCK"]           # GHOSTSTOCK is not ranked
    cfg = TradingSessionConfig(total_allocated_capital=900_000.0, top_n_stocks=5,
                               sizing_mode="equal", order_product="CNC",
                               kill_switch_enabled=False, symbol_whitelist=wl)

    import logging
    with caplog.at_level(logging.WARNING, logger="kanida.autotrade.session"):
        picks, cap = _resolve_falcon_selection(cfg, log_ctx="test: ")
    got = {p.symbol for p in picks}
    assert got == {"SWIGGY", "LODHA"}                # GHOSTSTOCK dropped
    assert cap == 2
    msgs = [rec.getMessage() for rec in caplog.records]
    assert any("not in today's picks" in m and "GHOSTSTOCK" in m for m in msgs), msgs
