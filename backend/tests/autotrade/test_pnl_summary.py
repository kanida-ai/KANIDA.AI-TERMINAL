"""AutoTrade Performance-dashboard P&L aggregation tests.

Exercises the PURE service (autotrade.api.pnl_summary) against the isolated temp
DB from conftest.py — seeding sessions of each strategy (intraday, positional,
ladder w/ 2 children, killswitch loser) + closed positions across dates + a
second user, then asserting:

  (a) strategy derivation correct
  (b) net == gross − charges at position / strategy / total
  (c) time-bucketing by closed_at (out-of-window excluded)
  (d) user scoping (A can't see B; admin sees all)
  (e) ladder drill-down groups children by ladder_id (one campaign row)
  (f) segment/product tags correct
  (g) empty period → zeroed
  (h) CSV one row per closed trade with net = gross − charges

No real broker / Kite / orders.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from falcon.db import falcon_conn
from autotrade.api import pnl_summary as svc
from autotrade.charges import estimate_charges


# ── FIX 1: F&O charge model (segment-aware) ───────────────────────────────────

def test_equity_charges_unchanged_default():
    """EQ default is byte-identical to the prior equity model (has DP on CNC)."""
    ch = estimate_charges(product="CNC", buy_value=100000.0,
                          sell_value=110000.0, legs=2)
    ch2 = estimate_charges(product="CNC", buy_value=100000.0,
                           sell_value=110000.0, legs=2, instrument_type="EQ")
    assert ch == ch2
    assert ch["dp"] == 15.34                      # equity CNC delivery DP present
    # STT both sides 0.1%: 0.001*(210000) = 210
    assert ch["stt"] == 210.0


def test_futures_charges_sane_no_dp():
    """A ~₹40L futures notional must bill < ₹1,500 (not ~₹9k equity-model)."""
    # ₹42.85L notional per side (like PAGEIND26JULFUT): qty 200 @ 21425.
    buy = 200 * 21425.0     # 4,285,000
    sell = 200 * 21420.0    # 4,284,000
    fo = estimate_charges(product="NRML", buy_value=buy, sell_value=sell,
                          legs=2, instrument_type="FUT")
    eq = estimate_charges(product="NRML", buy_value=buy, sell_value=sell,
                          legs=2)   # WRONG equity model, for contrast
    assert fo["dp"] == 0.0                         # futures have NO DP
    # STT sell-side only 0.02%: 0.0002 * 4,284,000 = 856.8
    assert abs(fo["stt"] - 856.8) < 0.5
    # brokerage capped at ₹20/leg → ₹40 total
    assert fo["brokerage"] == 40.0
    assert fo["total"] < 1500.0                    # sane
    assert eq["total"] > 8000.0                    # the bug we fixed
    # the equity model overstates by an order of magnitude
    assert eq["total"] > 5 * fo["total"]


def test_options_charges_sane_no_dp():
    """Options: STT 0.1% sell premium only, ₹20 flat/leg, no DP."""
    buy = 750 * 120.0       # premium turnover 90,000
    sell = 750 * 130.0      # premium turnover 97,500
    op = estimate_charges(product="NRML", buy_value=buy, sell_value=sell,
                          legs=2, instrument_type="CE")
    assert op["dp"] == 0.0
    assert op["brokerage"] == 40.0                 # ₹20 flat × 2 legs
    # STT sell premium only 0.1%: 0.001 * 97,500 = 97.5
    assert abs(op["stt"] - 97.5) < 0.5
    # PE identical model
    op_pe = estimate_charges(product="NRML", buy_value=buy, sell_value=sell,
                             legs=2, instrument_type="PE")
    assert op_pe == op


def test_open_position_futures_only_buy_side():
    """OPEN futures (sell_value=0): no STT (sell-side only), one brokerage leg."""
    fo = estimate_charges(product="NRML", buy_value=1000000.0, sell_value=0.0,
                          legs=1, instrument_type="FUT")
    assert fo["stt"] == 0.0
    assert fo["brokerage"] == 20.0                 # one leg


def test_summary_futures_charges_via_instrument_type(clean_positions):
    """collect_trades must pass instrument_type → FUT position gets F&O charges."""
    cfg_sess = ("s_futx", USER_A)
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_sessions (session_id, created_at, started_at, "
            "status, mode, total_allocated_capital, invested_basis, config_json, "
            "ladder_id, user_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("s_futx", "2026-06-24T09:15:00+05:30", "2026-06-24T09:15:00+05:30",
             "CLOSED", "live", 200000.0, 200000.0,
             json.dumps({"strategy": "portfolio_kill_switch",
                         "order_product": "NRML",
                         "total_allocated_capital": 200000.0}),
             None, USER_A))
        con.commit()
    _mk_pos("s_futx", "PAGEINDFUT", qty=200, avg=21425.0, exitp=21420.0,
            instrument="FUT", close_reason="KILL_SWITCH")
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=USER_A, is_admin=False, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW)
    fut = next(t for t in trades if t["symbol"] == "PAGEINDFUT")
    # gross = (21420-21425)*200 = -1000
    assert fut["gross"] == -1000.0
    assert fut["charges"] < 1500.0                 # F&O model, not ~₹9k equity
    assert fut["net"] == round(fut["gross"] - fut["charges"], 2)

IST = timezone(timedelta(hours=5, minutes=30))
NOW = datetime(2026, 6, 25, 10, 0, tzinfo=IST)

USER_A = "101"
USER_B = "202"

# Everything in-window closes on Jun 24; the window is Jun 20..Jun 30.
WIN_FROM, WIN_TO = "2026-06-20", "2026-06-30"
IN_WINDOW = "2026-06-24T15:29:00+05:30"
OUT_WINDOW = "2026-05-01T15:29:00+05:30"
STARTED = "2026-06-24T09:15:00+05:30"


# ── seed helpers ──────────────────────────────────────────────────────────────

def _mk_session(session_id, user_id, *, strategy="intraday_basket",
                square_off=True, product="CNC", ladder_id=None, mode="live",
                invested_basis=100000.0, started_at=STARTED):
    cfg = {
        "strategy": strategy,
        "square_off_enabled": square_off,
        "order_product": product,
        "total_allocated_capital": invested_basis,
    }
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_sessions "
            "(session_id, created_at, started_at, status, mode, "
            " total_allocated_capital, invested_basis, config_json, "
            " ladder_id, user_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (session_id, started_at, started_at, "CLOSED", mode,
             invested_basis, invested_basis, json.dumps(cfg), ladder_id,
             user_id),
        )
        con.commit()


def _mk_ladder(ladder_id, user_id, *, total_capital, product="CNC"):
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_ladders "
            "(ladder_id, user_id, mode, total_capital, order_product, "
            " per_basket_capital, status, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (ladder_id, user_id, "live", total_capital, product,
             round(total_capital / 3.0, 2), "RUNNING", STARTED),
        )
        con.commit()


def _mk_pos(session_id, symbol, *, qty, avg, exitp, closed_at=IN_WINDOW,
            instrument="EQ", direction="long", close_reason="SQUARE_OFF",
            entry_oid="E1", exit_oid="X1", opened_at=STARTED, realised=None):
    if realised is None:
        realised = round((exitp - avg) * qty if direction == "long"
                         else (avg - exitp) * qty, 2)
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_positions "
            "(session_id, symbol, instrument_type, qty, avg_price, exit_price, "
            " realised_pnl, status, opened_at, closed_at, close_reason, "
            " direction, entry_order_id, exit_order_id) "
            "VALUES (?,?,?,?,?,?,?,'CLOSED',?,?,?,?,?,?)",
            (session_id, symbol, instrument, qty, avg, exitp, realised,
             opened_at, closed_at, close_reason, direction, entry_oid,
             exit_oid),
        )
        con.commit()


def _seed_full(clean):
    """The canonical multi-strategy fixture used by most tests."""
    # 1) intraday (user A, CNC, EQ) — 1 win + 1 loss in-window + 1 OUT-of-window.
    _mk_session("s_intra", USER_A, strategy="intraday_basket", square_off=True,
                product="CNC", invested_basis=200000.0)
    _mk_pos("s_intra", "AAA", qty=10, avg=100.0, exitp=110.0)          # +100 gross
    _mk_pos("s_intra", "BBB", qty=10, avg=100.0, exitp=95.0)           # -50 gross
    _mk_pos("s_intra", "OLD", qty=10, avg=100.0, exitp=200.0,
            closed_at=OUT_WINDOW)                                       # excluded

    # 2) positional (user A, MTF, EQ) — 1 win.
    _mk_session("s_pos", USER_A, strategy="intraday_basket", square_off=False,
                product="MTF", invested_basis=300000.0)
    _mk_pos("s_pos", "CCC", qty=20, avg=50.0, exitp=60.0)              # +200 gross

    # 3) ladder campaign L1 (user A, CNC) — 2 child sessions, 1 pos each.
    _mk_ladder("L1", USER_A, total_capital=150000.0, product="CNC")
    _mk_session("s_lad1", USER_A, strategy="intraday_basket", square_off=False,
                product="CNC", ladder_id="L1", invested_basis=100000.0)
    _mk_session("s_lad2", USER_A, strategy="intraday_basket", square_off=False,
                product="CNC", ladder_id="L1", invested_basis=100000.0,
                started_at="2026-06-25T09:15:00+05:30")
    _mk_pos("s_lad1", "DDD", qty=10, avg=100.0, exitp=105.0)           # +50 gross
    _mk_pos("s_lad2", "EEE", qty=10, avg=100.0, exitp=103.0,
            closed_at="2026-06-25T15:29:00+05:30")                     # +30 gross

    # 4) killswitch LOSER (user A, CNC, EQ).
    _mk_session("s_kill", USER_A, strategy="portfolio_kill_switch",
                product="CNC", invested_basis=150000.0)
    _mk_pos("s_kill", "FFF", qty=10, avg=100.0, exitp=90.0,
            close_reason="KILL_SWITCH")                                # -100 gross

    # 5) user B session (isolation).
    _mk_session("s_userb", USER_B, strategy="intraday_basket", square_off=True,
                product="CNC", invested_basis=100000.0)
    _mk_pos("s_userb", "GGG", qty=10, avg=100.0, exitp=120.0)          # +200 gross


# ── (a) strategy derivation ───────────────────────────────────────────────────

def test_derive_strategy_unit():
    assert svc.derive_strategy({"strategy": "intraday_basket",
                                "square_off_enabled": True}, None) == \
        ("intraday", "Intraday Basket")
    assert svc.derive_strategy({"strategy": "intraday_basket",
                                "square_off_enabled": False}, None) == \
        ("positional", "Positional Basket")
    # ladder_id wins over strategy
    assert svc.derive_strategy({"strategy": "intraday_basket",
                                "square_off_enabled": False}, "L1")[0] == "ladder"
    assert svc.derive_strategy({"strategy": "portfolio_kill_switch"}, None) == \
        ("killswitch", "Kill-Switch Basket")
    # legacy / missing strategy → killswitch default
    assert svc.derive_strategy({}, None)[0] == "killswitch"


def test_strategy_ids_present(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=None, is_admin=True,
                                    period="custom", from_date=WIN_FROM,
                                    to_date=WIN_TO, mode="live", now_ist=NOW)
    ids = {s["id"] for s in out["strategies"]}
    assert ids == {"intraday", "positional", "ladder", "killswitch"}


# ── (b) net == gross − charges everywhere ─────────────────────────────────────

def test_net_equals_gross_minus_charges(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=None, is_admin=True,
                                    period="custom", from_date=WIN_FROM,
                                    to_date=WIN_TO, mode="live", now_ist=NOW)
    # strategy level
    for s in out["strategies"]:
        assert s["net"] == round(s["gross"] - s["charges"], 2)
        # drill-down (session/campaign) level
        for d in s["sessions"]:
            assert d["net"] == round(d["gross"] - d["charges"], 2)
    # total level
    t = out["totals"]
    assert t["net"] == round(t["gross"] - t["charges"], 2)


def test_position_level_charge_matches_charges_module(clean_positions):
    _seed_full(clean_positions)
    # The intraday AAA position: CNC, qty10, avg100, exit110 → gross +100.
    ch = estimate_charges(product="CNC", buy_value=10 * 100.0,
                          sell_value=10 * 110.0, legs=2)
    expected_net = round(100.0 - ch["total"], 2)
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=None, is_admin=True, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW)
    aaa = next(t for t in trades if t["symbol"] == "AAA")
    assert aaa["gross"] == 100.0
    assert aaa["charges"] == round(ch["total"], 2)
    assert aaa["net"] == expected_net


# ── (c) time-bucketing by closed_at ───────────────────────────────────────────

def test_out_of_window_excluded(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=None, is_admin=True, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW)
    symbols = {t["symbol"] for t in trades}
    assert "OLD" not in symbols            # closed 2026-05-01, outside Jun window
    assert "AAA" in symbols


def test_widening_window_includes_old(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=None, is_admin=True, period="custom",
            from_date="2026-05-01", to_date="2026-06-30", mode="live",
            now_ist=NOW)
    assert "OLD" in {t["symbol"] for t in trades}


def test_yesterday_uses_prev_trading_day(clean_positions):
    _seed_full(clean_positions)
    # NOW = Thu 2026-06-25 → previous trading day = Wed 2026-06-24 (all in-window
    # positions except the Jun-25 ladder child close on Jun 24).
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=None, is_admin=True,
                                    period="yesterday", mode="live", now_ist=NOW)
    assert out["from"] == "2026-06-24"
    assert out["to"] == "2026-06-24"
    # The EEE ladder child closed Jun 25 → excluded from "yesterday".
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=None, is_admin=True, period="yesterday",
            mode="live", now_ist=NOW)
    syms = {t["symbol"] for t in trades}
    assert "DDD" in syms and "EEE" not in syms


# ── (d) user scoping ──────────────────────────────────────────────────────────

def test_user_a_cannot_see_user_b(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=USER_A, is_admin=False, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW)
    assert "GGG" not in {t["symbol"] for t in trades}   # user B's symbol
    assert "AAA" in {t["symbol"] for t in trades}


def test_user_b_sees_only_own(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=USER_B, is_admin=False, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW)
    assert {t["symbol"] for t in trades} == {"GGG"}


def test_admin_sees_all(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=None, is_admin=True, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW)
    syms = {t["symbol"] for t in trades}
    assert "GGG" in syms and "AAA" in syms


def test_admin_user_filter(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        trades, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=None, is_admin=True, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live",
            filter_user_id=USER_B, now_ist=NOW)
    assert {t["symbol"] for t in trades} == {"GGG"}


# ── (e) ladder drill-down ─────────────────────────────────────────────────────

def test_ladder_drilldown_groups_children(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=None, is_admin=True,
                                    period="custom", from_date=WIN_FROM,
                                    to_date=WIN_TO, mode="live", now_ist=NOW)
    lad = next(s for s in out["strategies"] if s["id"] == "ladder")
    assert lad["trades"] == 2
    # ONE campaign row aggregating both child sessions.
    assert len(lad["sessions"]) == 1
    row = lad["sessions"][0]
    assert row["kind"] == "campaign"
    assert row["id"] == "L1"
    assert row["trades"] == 2
    assert row["net"] == round(row["gross"] - row["charges"], 2)


# ── (f) segment / product tags ────────────────────────────────────────────────

def test_segment_product_tags(clean_positions):
    _seed_full(clean_positions)
    # add a futures NRML session for user A
    _mk_session("s_fut", USER_A, strategy="portfolio_kill_switch",
                product="NRML", invested_basis=100000.0)
    _mk_pos("s_fut", "NIFTYFUT", qty=50, avg=100.0, exitp=110.0,
            instrument="FUT")
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=None, is_admin=True,
                                    period="custom", from_date=WIN_FROM,
                                    to_date=WIN_TO, mode="live", now_ist=NOW)
    pos = next(s for s in out["strategies"] if s["id"] == "positional")
    assert pos["segment"] == "Equity"
    assert pos["products"] == ["MTF"]
    kill = next(s for s in out["strategies"] if s["id"] == "killswitch")
    # killswitch now has EQ (CNC) + FUT (NRML)
    assert "Futures" in kill["segments"]
    assert set(kill["products"]) == {"CNC", "NRML"}


# ── (g) empty period → zeroed ─────────────────────────────────────────────────

def test_empty_period_zeroed(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=None, is_admin=True,
                                    period="custom", from_date="2020-01-01",
                                    to_date="2020-01-31", mode="live",
                                    now_ist=NOW)
    assert out["strategies"] == []
    assert out["capital_deployed"] == 0.0
    assert out["totals"] == {"net": 0.0, "gross": 0.0, "charges": 0.0,
                             "trades": 0, "wins": 0, "losses": 0,
                             "win_rate": 0.0}


def test_no_sessions_for_user_zeroed(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id="999", is_admin=False,
                                    period="custom", from_date=WIN_FROM,
                                    to_date=WIN_TO, mode="live", now_ist=NOW)
    assert out["strategies"] == []
    assert out["totals"]["trades"] == 0


# ── (h) CSV export ────────────────────────────────────────────────────────────

def test_csv_one_row_per_trade_and_net(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        lines = list(svc.iter_csv_rows(
            con, viewer_user_id=USER_A, is_admin=False, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW))
    # header + data rows
    header = lines[0].strip().split(",")
    assert header[:5] == ["strategy", "session_or_campaign", "symbol",
                          "segment", "product"]
    data = [ln for ln in lines[1:] if ln.strip()]
    # user A in-window closed trades: AAA,BBB,CCC,DDD,EEE,FFF = 6
    assert len(data) == 6
    gi, ci, ni = header.index("gross_pnl"), header.index("charges"), header.index("net_pnl")
    for ln in data:
        cols = ln.strip().split(",")
        gross, charges, net = float(cols[gi]), float(cols[ci]), float(cols[ni])
        assert net == round(gross - charges, 2)


def test_csv_ladder_uses_campaign_id(clean_positions):
    _seed_full(clean_positions)
    with falcon_conn() as con:
        lines = list(svc.iter_csv_rows(
            con, viewer_user_id=USER_A, is_admin=False, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW))
    header = lines[0].strip().split(",")
    sc = header.index("session_or_campaign")
    sym = header.index("symbol")
    for ln in lines[1:]:
        if not ln.strip():
            continue
        cols = ln.strip().split(",")
        if cols[sym] in ("DDD", "EEE"):
            assert cols[sc] == "L1"     # ladder children carry the campaign id


# ── mode filter + capital_deployed ────────────────────────────────────────────

def test_mode_filter_paper_excluded(clean_positions):
    _mk_session("s_paper", USER_A, strategy="intraday_basket", square_off=True,
                product="CNC", mode="paper", invested_basis=100000.0)
    _mk_pos("s_paper", "PPP", qty=10, avg=100.0, exitp=110.0)
    _mk_session("s_live", USER_A, strategy="intraday_basket", square_off=True,
                product="CNC", mode="live", invested_basis=100000.0)
    _mk_pos("s_live", "LLL", qty=10, avg=100.0, exitp=110.0)
    with falcon_conn() as con:
        live_tr, _s, _f, _t = svc.collect_trades(
            con, viewer_user_id=USER_A, is_admin=False, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="live", now_ist=NOW)
        all_tr, _s2, _f2, _t2 = svc.collect_trades(
            con, viewer_user_id=USER_A, is_admin=False, period="custom",
            from_date=WIN_FROM, to_date=WIN_TO, mode="all", now_ist=NOW)
    assert {t["symbol"] for t in live_tr} == {"LLL"}
    assert {t["symbol"] for t in all_tr} == {"LLL", "PPP"}


def test_capital_deployed_counts_pools_once(clean_positions):
    _seed_full(clean_positions)
    # capital_deployed counts each POOL once:
    #   standalone total_allocated_capital: s_intra(200k)+s_pos(300k)+s_kill(150k)=650k
    #   ladder L1 total_capital (once, NOT 2×children): 150k
    #   → 800k. (Old per-session invested_basis sum would have been 850k.)
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=USER_A, is_admin=False,
                                    period="custom", from_date=WIN_FROM,
                                    to_date=WIN_TO, mode="live", now_ist=NOW)
    assert out["capital_deployed"] == 800000.0


def test_capital_deployed_ladder_counted_once_many_children(clean_positions):
    """A ladder with MANY children counts its total_capital ONCE."""
    _mk_ladder("LBIG", USER_A, total_capital=500000.0, product="CNC")
    for i in range(20):
        sid = f"lc{i}"
        _mk_session(sid, USER_A, strategy="intraday_basket", square_off=False,
                    product="CNC", ladder_id="LBIG", invested_basis=100000.0)
        _mk_pos(sid, f"S{i}", qty=10, avg=100.0, exitp=101.0)
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=USER_A, is_admin=False,
                                    period="custom", from_date=WIN_FROM,
                                    to_date=WIN_TO, mode="live", now_ist=NOW)
    # ONE pool of 500k — not 20×100k = 2,000k.
    assert out["capital_deployed"] == 500000.0


def test_capital_deployed_ladder_fallback_when_no_row(clean_positions):
    """No autotrade_ladders row → fall back to summing contributing children."""
    _mk_session("orphan1", USER_A, strategy="intraday_basket", square_off=False,
                product="CNC", ladder_id="LORPH", invested_basis=100000.0)
    _mk_session("orphan2", USER_A, strategy="intraday_basket", square_off=False,
                product="CNC", ladder_id="LORPH", invested_basis=100000.0)
    _mk_pos("orphan1", "O1", qty=10, avg=100.0, exitp=101.0)
    _mk_pos("orphan2", "O2", qty=10, avg=100.0, exitp=101.0)
    with falcon_conn() as con:
        out = svc.build_pnl_summary(con, viewer_user_id=USER_A, is_admin=False,
                                    period="custom", from_date=WIN_FROM,
                                    to_date=WIN_TO, mode="live", now_ist=NOW)
    # fallback = 100k + 100k children total_allocated_capital
    assert out["capital_deployed"] == 200000.0
