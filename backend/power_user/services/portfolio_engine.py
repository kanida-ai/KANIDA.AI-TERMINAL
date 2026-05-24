"""Co-Trader portfolio engine — Sprint 5d Phase 1.

Drives the 5 frozen virtual portfolios end-to-end:
  - reads daily engine signals (compute_top_n)
  - applies each portfolio's entry rule
  - manages SL / target / time / trail exits
  - writes positions + equity history + event log + portfolio_definitions seed

Idempotent on every callable surface:
  - `seed_portfolio_definitions()` upserts the 5 rows
  - `run_eod_for_date(date)` is safe to re-run for the same date (writes use
    INSERT OR IGNORE + UNIQUE constraints, equity rows REPLACE)
  - `backfill(from_date, to_date)` is safe to re-run end-to-end

Entry rule keys (matched against portfolio_defs.entry_rule):
  daily_top14                   — every weekday, top-14 by score
  daily_top14_tier_filtered     — every weekday, top-14 with tier-rule filter
  tuesday_top5                  — Tuesday only, top-5
  first_of_month_top14          — first trading day of month, top-14
  daily_top14_dca               — every weekday, top-14 but only deploy 1/7 capital/day

Exit rule keys:
  champion_trail                — -7% SL, arm +12% trail, 10d Donchian, 7d max hold
  fixed_target_no_trail         — -5% SL, +7% fixed target, 5d max hold
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .portfolio_defs import (
    ALL_PORTFOLIOS,
    OBSOLETE_SLUGS,
    PortfolioDef,
    VIRTUAL_CAPITAL_START,
    by_slug,
)

log = logging.getLogger("kanida.power_user.portfolio_engine")
IST = timezone(timedelta(hours=5, minutes=30))


# ──────────────────────────────────────────────────────────────────────────
# Definition table seeding (idempotent)
# ──────────────────────────────────────────────────────────────────────────

def seed_portfolio_definitions(con: sqlite3.Connection) -> int:
    """Upsert the 5 V3-locked personas. Returns how many rows were upserted.

    Side effect: deletes any obsolete-slug rows + their child positions/equity
    /event-log entries so the old (champion/confirmed/expiry-trader/concentrated/
    smooth) data doesn't drag along after the 2026-05-16 lock-down rename.
    """
    # ── Drop obsolete-slug definitions + their dependent rows in a single
    # transaction (positions FK to portfolio_definitions.id).
    for old_slug in OBSOLETE_SLUGS:
        row = con.execute("SELECT id FROM portfolio_definitions WHERE slug = ?", (old_slug,)).fetchone()
        if not row:
            continue
        old_id = row[0]
        con.execute("DELETE FROM portfolio_event_log       WHERE portfolio_id = ?", (old_id,))
        con.execute("DELETE FROM portfolio_equity_history  WHERE portfolio_id = ?", (old_id,))
        con.execute("DELETE FROM portfolio_positions       WHERE portfolio_id = ?", (old_id,))
        con.execute("DELETE FROM portfolio_definitions     WHERE id = ?",            (old_id,))
        log.info("seed_portfolio_definitions: dropped obsolete slug=%s id=%d", old_slug, old_id)

    n = 0
    for p in ALL_PORTFOLIOS:
        con.execute("""
            INSERT INTO portfolio_definitions (
                slug, name, tagline, entry_rule, exit_rule,
                virtual_capital_start, start_date, is_active, display_order,
                backtest_metrics_json, parameters_json, narrative_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(slug) DO UPDATE SET
                name                  = excluded.name,
                tagline               = excluded.tagline,
                entry_rule            = excluded.entry_rule,
                exit_rule             = excluded.exit_rule,
                display_order         = excluded.display_order,
                backtest_metrics_json = excluded.backtest_metrics_json,
                parameters_json       = excluded.parameters_json,
                narrative_json        = excluded.narrative_json,
                updated_at            = datetime('now')
        """, (
            p.slug, p.name, p.tagline, p.entry_rule, p.exit_rule,
            VIRTUAL_CAPITAL_START, "2026-01-01", p.display_order,
            p.backtest_metrics_json(), p.parameters_json(), p.narrative_json(),
        ))
        n += 1
    con.commit()
    return n


def wipe_and_reseed(con: sqlite3.Connection) -> Dict[str, int]:
    """Drop ALL portfolio_positions / portfolio_equity_history / portfolio_event_log
    rows, then re-seed the 5 definitions. Used during the V3 lock-down
    migration: capital model changed from %-of-equity → fixed-₹, so every
    existing position row was sized incorrectly and must be regenerated.

    Caller is expected to follow up with backfill().
    """
    n_pos = con.execute("DELETE FROM portfolio_positions").rowcount
    n_eq  = con.execute("DELETE FROM portfolio_equity_history").rowcount
    n_ev  = con.execute("DELETE FROM portfolio_event_log").rowcount
    con.commit()
    n_seeded = seed_portfolio_definitions(con)
    log.warning("wipe_and_reseed: dropped %d positions, %d equity rows, %d events; "
                "re-seeded %d portfolios", n_pos, n_eq, n_ev, n_seeded)
    return {"positions_dropped": n_pos, "equity_dropped": n_eq,
            "events_dropped":   n_ev, "portfolios_seeded": n_seeded}


# ──────────────────────────────────────────────────────────────────────────
# Signal fetching — wraps compute_top_n so backfill works even when
# falcon_signals_live is empty for a past date.
# ──────────────────────────────────────────────────────────────────────────

def _compute_signals_for_date(
    con: sqlite3.Connection, signal_date: str, top_n: int,
) -> List[Dict[str, Any]]:
    """Return the engine's top-N ranked picks for `signal_date`.

    Tries falcon_signals_live first (cheap). Falls back to compute_top_n
    (expensive but always works given falcon_features coverage).
    """
    rows = con.execute("""
        SELECT signal_date, entry_date, rank, symbol, sector, score, close_at_signal
          FROM falcon_signals_live
         WHERE signal_date = ?
         ORDER BY rank ASC
         LIMIT ?
    """, (signal_date, top_n)).fetchall()
    if rows:
        return [
            {
                "signal_date":     r["signal_date"],
                "entry_date":      r["entry_date"],
                "rank":            r["rank"],
                "symbol":          r["symbol"],
                "sector":          r["sector"],
                "score":           float(r["score"]),
                "close_at_signal": float(r["close_at_signal"]) if r["close_at_signal"] is not None else None,
            }
            for r in rows
        ]
    # Fallback: live compute via explainer. Wrapped in try/except so a missing
    # auxiliary table (pattern_candidates etc.) doesn't break exit-processing
    # for the day — the engine should still mark SL/TARGET/TIME exits even if
    # we can't open new entries today.
    try:
        from .explainer import compute_top_n, load_patterns
        patterns = load_patterns(con)
        raw = compute_top_n(con, signal_date, top_n=top_n, patterns=patterns)
    except sqlite3.OperationalError as e:
        log.debug("compute_top_n fallback unavailable for %s: %s", signal_date, e)
        return []
    except Exception as e:
        log.warning("compute_top_n failed for %s: %s", signal_date, e)
        return []
    entry_date = _next_trading_day(con, signal_date)
    return [
        {
            "signal_date":     signal_date,
            "entry_date":      entry_date,
            "rank":            i + 1,
            "symbol":          p["symbol"],
            "sector":          p.get("sector"),
            "score":           float(p["score"]),
            "close_at_signal": float(p["close_at_signal"]) if p.get("close_at_signal") is not None else None,
        }
        for i, p in enumerate(raw)
    ]


def _next_trading_day(con: sqlite3.Connection, after_date: str) -> Optional[str]:
    row = con.execute(
        "SELECT MIN(trade_date) FROM ohlc_daily WHERE trade_date > ?", (after_date,)
    ).fetchone()
    return row[0] if row and row[0] else None


def _trading_days_between(
    con: sqlite3.Connection, from_date: str, to_date: str,
) -> List[str]:
    rows = con.execute("""
        SELECT DISTINCT trade_date
          FROM ohlc_daily
         WHERE trade_date >= ? AND trade_date <= ?
         ORDER BY trade_date ASC
    """, (from_date, to_date)).fetchall()
    return [r[0] for r in rows]


# ──────────────────────────────────────────────────────────────────────────
# OHLC helpers — small per-symbol price queries (memoised per backfill batch)
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class OhlcRow:
    open_: float
    high:  float
    low:   float
    close: float


def _get_ohlc(con: sqlite3.Connection, symbol: str, date: str) -> Optional[OhlcRow]:
    row = con.execute("""
        SELECT open, high, low, close FROM ohlc_daily
         WHERE symbol = ? AND trade_date = ?
         LIMIT 1
    """, (symbol, date)).fetchone()
    if not row:
        return None
    return OhlcRow(open_=row[0], high=row[1], low=row[2], close=row[3])


def _donchian_10d_low(
    con: sqlite3.Connection, symbol: str, end_date: str,
) -> Optional[float]:
    """Lowest low over the last 10 sessions <= end_date (inclusive)."""
    rows = con.execute("""
        SELECT low FROM ohlc_daily
         WHERE symbol = ? AND trade_date <= ?
         ORDER BY trade_date DESC
         LIMIT 10
    """, (symbol, end_date)).fetchall()
    if not rows:
        return None
    return min(r[0] for r in rows)


# ──────────────────────────────────────────────────────────────────────────
# Position state queries
# ──────────────────────────────────────────────────────────────────────────

def _open_positions(con: sqlite3.Connection, portfolio_id: int) -> List[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    return con.execute("""
        SELECT * FROM portfolio_positions
         WHERE portfolio_id = ? AND exit_date IS NULL
         ORDER BY entry_date ASC
    """, (portfolio_id,)).fetchall()


def _portfolio_id_for_slug(con: sqlite3.Connection, slug: str) -> int:
    row = con.execute(
        "SELECT id FROM portfolio_definitions WHERE slug = ?", (slug,)
    ).fetchone()
    if not row:
        raise ValueError(f"portfolio not seeded: {slug}")
    return int(row[0])


def _cash_balance(
    con: sqlite3.Connection, portfolio_id: int, capital_start: float,
) -> float:
    """Compute current cash balance = starting capital
       - sum(capital committed in still-open positions)
       + sum(net proceeds on closed positions: exit_price * qty)
       - sum(entry cost on closed positions: capital_committed).

    For closed positions, net cash effect = exit_price * qty - entry_price * qty
    = pnl_rs (which we store at close), so closed-positions net effect == sum(pnl_rs).
    """
    row = con.execute("""
        SELECT
          COALESCE(SUM(CASE WHEN exit_date IS NULL THEN capital_committed END), 0) AS open_committed,
          COALESCE(SUM(CASE WHEN exit_date IS NOT NULL THEN pnl_rs END), 0)        AS realized_pnl
        FROM portfolio_positions WHERE portfolio_id = ?
    """, (portfolio_id,)).fetchone()
    open_committed = float(row[0])
    realized_pnl   = float(row[1])
    return capital_start - open_committed + realized_pnl


# ──────────────────────────────────────────────────────────────────────────
# ENTRY rule dispatch
# ──────────────────────────────────────────────────────────────────────────

def _is_first_trading_day_of_month(
    con: sqlite3.Connection, signal_date: str,
) -> bool:
    # signal_date is the date the SIGNAL was emitted. We want the entry_date
    # (next trading day) to be the first trading day of its month.
    entry = _next_trading_day(con, signal_date)
    if not entry:
        return False
    year, month, _ = entry.split("-")
    row = con.execute("""
        SELECT MIN(trade_date) FROM ohlc_daily
         WHERE substr(trade_date, 1, 7) = ?
    """, (f"{year}-{month}",)).fetchone()
    first_in_month = row[0] if row else None
    return first_in_month == entry


def _entries_for_portfolio(
    con: sqlite3.Connection,
    p:   PortfolioDef,
    signal_date: str,
    open_positions: List[sqlite3.Row],
    cash_balance:  float,
    capital_start: float,
) -> List[Dict[str, Any]]:
    """Return list of {symbol, sector, rank, score, entry_date, story} to OPEN today.

    V3 audit-ready (2026-05-16 lock): fixed rupees per trade — no compounding
    within year. Position sizing dispatched purely on params.entry_cadence and
    params.fixed_per_trade_rs / top_n. Integer shares only.
    """
    pars = p.params

    # ── Cadence gate
    if pars.entry_cadence == "tuesday_only":
        if datetime.fromisoformat(signal_date).weekday() != 1:   # 1 = Tuesday
            return []
    if pars.entry_cadence == "first_of_month":
        if not _is_first_trading_day_of_month(con, signal_date):
            return []

    # ── Fetch signals (top_n is locked per-persona, e.g. 10 for Weekly, 15 for BTST)
    signals = _compute_signals_for_date(con, signal_date, top_n=pars.top_n)
    if not signals:
        return []
    entry_date = signals[0]["entry_date"]
    if not entry_date:
        return []

    # ── Patient Trader: 15-min intraday filter approximation.
    # In production we don't yet have 1-min data in the prod DB, so we use a
    # rank-based proxy: keep only picks ranked top-7 (the engine's most-
    # conviction half). True 15-min vol/return gate will replace this when
    # /intraday-mining publish pipeline ships.
    if pars.intraday_filter == "wait_15min_volume_check":
        signals = [s for s in signals if _passes_15min_filter_proxy(s)]

    # ── Don't re-enter symbols already held (skip_if_held)
    if pars.skip_if_held:
        held = {row["symbol"] for row in open_positions}
        signals = [s for s in signals if s["symbol"] not in held]

    # ── Fixed rupees per trade — V3 audit semantic (no compound inflation)
    per_trade = float(pars.fixed_per_trade_rs)
    budget    = cash_balance

    out: List[Dict[str, Any]] = []
    for s in signals:
        if budget < per_trade * 0.5:    # not enough left for even a small position
            break
        # Build per-position spec
        price = s["close_at_signal"]
        if not price or price <= 0:
            # Fall back to next day's open
            ohlc = _get_ohlc(con, s["symbol"], entry_date)
            if not ohlc:
                continue
            price = ohlc.open_
        qty = int(per_trade // price)
        if qty <= 0:
            continue
        committed = qty * price
        if committed > budget:
            qty = int(budget // price)
            committed = qty * price
            if qty <= 0:
                continue
        out.append({
            "symbol":          s["symbol"],
            "sector":          s["sector"],
            "rank":            s["rank"],
            "score":           s["score"],
            "signal_date":     signal_date,
            "entry_date":      entry_date,
            "entry_price":     price,
            "qty":             qty,
            "capital_committed": committed,
        })
        budget -= committed
    return out


def _passes_15min_filter_proxy(signal: Dict[str, Any]) -> bool:
    """Patient Trader's 9:30 IST gate — proxy for the V3 backtest's 15-min
    confirmation rule:

      first_15_ret > 0  AND  first_15_vol > 5% × yesterday's full-day volume

    Production doesn't yet have 1-min data in the prod DB (87 M-row mining
    archive lives R&D-side), so we approximate by keeping picks ranked in
    the top half of the daily 14-pick budget — equivalent to the engine's
    higher-conviction half. Real intraday gate ships when the intraday rules
    publish pipeline lands (separate sprint, see engine_intraday_smart_entry
    memory note).
    """
    return signal["rank"] <= 7


# ──────────────────────────────────────────────────────────────────────────
# EXIT rule dispatch
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class ExitDecision:
    exit_reason: str          # SL | TARGET | TIME | TRAIL
    exit_price:  float
    update_trail_high_water: Optional[float] = None
    update_trail_active:     Optional[bool]  = None


def _decide_exit(
    con: sqlite3.Connection,
    pos: sqlite3.Row,
    p:   PortfolioDef,
    date: str,
) -> Optional[ExitDecision]:
    """Given an open position and today's OHLC, decide if it exits today.

    Order of evaluation per spec / falcon_engine_playbook:
      1. SL hit intraday      → exit at SL price
      2. Fixed target hit     → exit at target price
      3. Trail stop hit       → exit at trail level (after armed)
      4. Time exit            → close at today's close after hold_days_max
    """
    ohlc = _get_ohlc(con, pos["symbol"], date)
    if not ohlc:
        return None        # no data — can't decide today, carry forward

    sl_level     = pos["sl_level"]
    target_level = pos["target_level"]
    trail_hw     = pos["trail_high_water"]
    trail_armed  = bool(pos["trail_active"])
    entry_price  = pos["entry_price"]
    entry_date   = pos["entry_date"]
    pars         = p.params

    # 1. Stop loss — intraday low pierces SL
    if ohlc.low <= sl_level:
        return ExitDecision(exit_reason="SL", exit_price=sl_level)

    # 2. Fixed target (Expiry Trader only)
    if target_level is not None and ohlc.high >= target_level:
        return ExitDecision(exit_reason="TARGET", exit_price=target_level)

    # 3. Trail
    if pars.trail_trigger_pct is not None and pars.trail_method == "donchian_10d":
        # Arm trail when close >= entry × (1 + trigger%)
        trigger_price = entry_price * (1.0 + pars.trail_trigger_pct / 100.0)
        new_armed     = trail_armed
        new_trail_hw  = trail_hw

        if not trail_armed and ohlc.close >= trigger_price:
            new_armed    = True
            new_trail_hw = ohlc.close

        if new_armed:
            # Update trail high-water
            if new_trail_hw is None or ohlc.close > new_trail_hw:
                new_trail_hw = ohlc.close
            # Compute trail level = 10d Donchian low at THIS bar
            donch = _donchian_10d_low(con, pos["symbol"], date)
            if donch is not None and ohlc.low <= donch:
                return ExitDecision(
                    exit_reason="TRAIL",
                    exit_price=donch,
                    update_trail_high_water=new_trail_hw,
                    update_trail_active=True,
                )
            # No hit today, just record the trail-high-water update
            if new_armed != trail_armed or new_trail_hw != trail_hw:
                return ExitDecision(
                    exit_reason="__update_only__",   # sentinel, see caller
                    exit_price=0.0,
                    update_trail_high_water=new_trail_hw,
                    update_trail_active=new_armed,
                )

    # 4. Time exit — after holding for `hold_days_max` trading sessions
    sessions_held = _trading_sessions_between(con, entry_date, date)
    if sessions_held >= pars.hold_days_max:
        return ExitDecision(exit_reason="TIME", exit_price=ohlc.close)

    return None


def _trading_sessions_between(
    con: sqlite3.Connection, from_date: str, to_date: str,
) -> int:
    """Count of trading sessions on which the position has been HELD.
    Entry day counts as session 0 (no exit on entry day under these rules)."""
    row = con.execute("""
        SELECT COUNT(DISTINCT trade_date) FROM ohlc_daily
         WHERE trade_date > ? AND trade_date <= ?
    """, (from_date, to_date)).fetchone()
    return int(row[0]) if row else 0


# ──────────────────────────────────────────────────────────────────────────
# Daily EOD step — process exits, then entries, then write equity row
# ──────────────────────────────────────────────────────────────────────────

def run_eod_for_portfolio(
    con: sqlite3.Connection,
    p:   PortfolioDef,
    date: str,
) -> Dict[str, Any]:
    """Run one EOD cycle for a single portfolio on `date`.

    Idempotent: if the equity row for (portfolio, date) already exists, we
    re-process this date (positions are processed with INSERT OR IGNORE so
    duplicates can't form, and equity is REPLACE-d).
    """
    portfolio_id  = _portfolio_id_for_slug(con, p.slug)
    capital_start = VIRTUAL_CAPITAL_START

    # ── 1. Process exits on existing open positions ───────────────────
    open_positions = _open_positions(con, portfolio_id)
    n_closed = 0
    for pos in open_positions:
        decision = _decide_exit(con, pos, p, date)
        if decision is None:
            continue
        if decision.exit_reason == "__update_only__":
            # Trail armed / hw bumped, no exit
            con.execute("""
                UPDATE portfolio_positions
                   SET trail_active     = ?,
                       trail_high_water = ?
                 WHERE id = ?
            """, (1 if decision.update_trail_active else 0,
                  decision.update_trail_high_water, pos["id"]))
            continue
        # Real exit
        pnl_rs  = (decision.exit_price - pos["entry_price"]) * pos["qty"]
        pnl_pct = (decision.exit_price - pos["entry_price"]) / pos["entry_price"] * 100.0
        holding = _trading_sessions_between(con, pos["entry_date"], date)
        con.execute("""
            UPDATE portfolio_positions
               SET exit_date    = ?,
                   exit_price   = ?,
                   exit_reason  = ?,
                   pnl_rs       = ?,
                   pnl_pct      = ?,
                   holding_days = ?,
                   trail_active = ?,
                   trail_high_water = ?
             WHERE id = ?
        """, (date, decision.exit_price, decision.exit_reason,
              pnl_rs, pnl_pct, holding,
              1 if decision.update_trail_active else (pos["trail_active"] or 0),
              decision.update_trail_high_water if decision.update_trail_high_water is not None else pos["trail_high_water"],
              pos["id"]))
        event_type = f"EXIT_{decision.exit_reason}"
        reason_text = _exit_reason_text(decision.exit_reason, decision.exit_price, pos)
        con.execute("""
            INSERT INTO portfolio_event_log (portfolio_id, event_date, event_type, symbol, price, reason_text, position_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (portfolio_id, date, event_type, pos["symbol"], decision.exit_price, reason_text, pos["id"]))
        n_closed += 1

    # ── 2. Process new entries (use UPDATED cash after exits) ─────────
    open_positions = _open_positions(con, portfolio_id)
    cash_now = _cash_balance(con, portfolio_id, capital_start)
    entries = _entries_for_portfolio(con, p, signal_date=date,
                                       open_positions=open_positions,
                                       cash_balance=cash_now,
                                       capital_start=capital_start)
    n_opened = 0
    for e in entries:
        # Build SL/target levels off the entry price
        pars     = p.params
        sl_level = e["entry_price"] * (1.0 + pars.sl_pct / 100.0)
        target   = (e["entry_price"] * (1.0 + pars.target_pct / 100.0)
                    if pars.target_pct is not None else None)
        # Pre-fetch story for this pick (operator spec: "Why I bought X" expandable)
        story = _build_story(con, e)
        try:
            cur = con.execute("""
                INSERT OR IGNORE INTO portfolio_positions (
                    portfolio_id, signal_date, entry_date, symbol, sector,
                    rank_on_entry, score_on_entry, story_on_entry,
                    entry_price, qty, capital_committed,
                    sl_level, target_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                portfolio_id, e["signal_date"], e["entry_date"], e["symbol"], e["sector"],
                e["rank"], e["score"], story,
                e["entry_price"], e["qty"], e["capital_committed"],
                sl_level, target,
            ))
            if cur.rowcount:
                n_opened += 1
                pos_id = cur.lastrowid
                con.execute("""
                    INSERT INTO portfolio_event_log (portfolio_id, event_date, event_type, symbol, price, reason_text, position_id)
                    VALUES (?, ?, 'ENTER', ?, ?, ?, ?)
                """, (portfolio_id, e["entry_date"], e["symbol"], e["entry_price"],
                      f"Rank #{e['rank']} by score {round(e['score'])}. Capital committed: ₹{int(e['capital_committed']):,}.",
                      pos_id))
        except sqlite3.IntegrityError as ie:
            log.warning("portfolio_engine[%s]: skip duplicate entry %s on %s: %s",
                        p.slug, e["symbol"], e["signal_date"], ie)

    # ── 3. Compute end-of-day equity ──────────────────────────────────
    equity = _compute_equity_row(con, portfolio_id, date, capital_start, n_closed, n_opened)
    con.execute("""
        INSERT INTO portfolio_equity_history (
            portfolio_id, trade_date,
            cash_balance, deployed_capital, mtm_unrealized, total_equity,
            daily_pnl_rs, daily_pnl_pct, cumulative_return_pct, max_drawdown_pct,
            peak_equity, n_open_positions, n_closed_today, n_opened_today
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(portfolio_id, trade_date) DO UPDATE SET
            cash_balance         = excluded.cash_balance,
            deployed_capital     = excluded.deployed_capital,
            mtm_unrealized       = excluded.mtm_unrealized,
            total_equity         = excluded.total_equity,
            daily_pnl_rs         = excluded.daily_pnl_rs,
            daily_pnl_pct        = excluded.daily_pnl_pct,
            cumulative_return_pct= excluded.cumulative_return_pct,
            max_drawdown_pct     = excluded.max_drawdown_pct,
            peak_equity          = excluded.peak_equity,
            n_open_positions     = excluded.n_open_positions,
            n_closed_today       = excluded.n_closed_today,
            n_opened_today       = excluded.n_opened_today
    """, (
        portfolio_id, date,
        equity["cash_balance"], equity["deployed_capital"], equity["mtm_unrealized"],
        equity["total_equity"],
        equity["daily_pnl_rs"], equity["daily_pnl_pct"],
        equity["cumulative_return_pct"], equity["max_drawdown_pct"],
        equity["peak_equity"],
        equity["n_open_positions"], n_closed, n_opened,
    ))
    con.commit()
    return {"date": date, "slug": p.slug, "opened": n_opened, "closed": n_closed,
            "equity": equity["total_equity"],
            "return_pct": equity["cumulative_return_pct"]}


def _exit_reason_text(reason: str, exit_price: float, pos: sqlite3.Row) -> str:
    pct = (exit_price - pos["entry_price"]) / pos["entry_price"] * 100.0
    sign = "+" if pct >= 0 else "−"
    pct_abs = abs(pct)
    if reason == "SL":
        return f"Stop hit at ₹{exit_price:.2f} ({sign}{pct_abs:.2f}%). Cut and move on."
    if reason == "TARGET":
        return f"Target hit at ₹{exit_price:.2f} (+{pct:.2f}%). Booked."
    if reason == "TRAIL":
        return f"Trail stop hit at ₹{exit_price:.2f} ({sign}{pct_abs:.2f}%) — letting winners cool off."
    if reason == "TIME":
        return f"Hold window done. Closed at ₹{exit_price:.2f} ({sign}{pct_abs:.2f}%)."
    return f"Closed at ₹{exit_price:.2f} ({sign}{pct_abs:.2f}%)."


def _build_story(con: sqlite3.Connection, entry: Dict[str, Any]) -> str:
    """One-line trader-voice explanation for the pick. Uses the same explainer
    primitives as the Pick v1 payload so the language is consistent across
    /power/today and the portfolio dashboard."""
    try:
        from .explainer import build_pick_payload, compute_top_n, load_patterns
        # Cheaply recompute just the one pick we care about
        patterns = load_patterns(con)
        raw = compute_top_n(con, entry["signal_date"],
                            top_n=max(entry["rank"], 14), patterns=patterns)
        for i, p in enumerate(raw):
            if p["symbol"] == entry["symbol"]:
                payload = build_pick_payload(
                    rank=entry["rank"], pick=p, outcomes=None,
                    signal_date=entry["signal_date"], entry_date=entry["entry_date"],
                )
                return payload.get("story") or ""
    except Exception as e:
        log.debug("story build failed for %s: %s", entry["symbol"], e)
    return f"Top-{entry['rank']} by engine score on {entry['signal_date']}."


def _compute_equity_row(
    con: sqlite3.Connection, portfolio_id: int, date: str,
    capital_start: float, n_closed: int, n_opened: int,
) -> Dict[str, Any]:
    open_rows = con.execute("""
        SELECT symbol, qty, capital_committed FROM portfolio_positions
         WHERE portfolio_id = ? AND exit_date IS NULL
    """, (portfolio_id,)).fetchall()

    deployed = 0.0
    mtm      = 0.0
    for sym, qty, committed in open_rows:
        deployed += float(committed)
        ohlc = _get_ohlc(con, sym, date)
        if ohlc:
            mtm += (ohlc.close * qty) - float(committed)

    cash = _cash_balance(con, portfolio_id, capital_start)
    total_equity = cash + deployed + mtm

    # daily P&L: total_equity today - total_equity prior session
    prev = con.execute("""
        SELECT total_equity, peak_equity FROM portfolio_equity_history
         WHERE portfolio_id = ? AND trade_date < ?
         ORDER BY trade_date DESC LIMIT 1
    """, (portfolio_id, date)).fetchone()
    if prev:
        prev_equity = float(prev[0])
        peak        = max(float(prev[1] or capital_start), total_equity)
    else:
        prev_equity = capital_start
        peak        = max(capital_start, total_equity)

    daily_pnl_rs   = total_equity - prev_equity
    daily_pnl_pct  = (daily_pnl_rs / prev_equity) * 100.0 if prev_equity else 0.0
    cum_ret        = (total_equity / capital_start - 1.0) * 100.0
    dd_pct         = (total_equity / peak - 1.0) * 100.0 if peak else 0.0

    return {
        "cash_balance":          cash,
        "deployed_capital":      deployed,
        "mtm_unrealized":        mtm,
        "total_equity":          total_equity,
        "daily_pnl_rs":          daily_pnl_rs,
        "daily_pnl_pct":         daily_pnl_pct,
        "cumulative_return_pct": cum_ret,
        "max_drawdown_pct":      min(dd_pct, _existing_max_dd(con, portfolio_id, date)),
        "peak_equity":           peak,
        "n_open_positions":      len(open_rows),
    }


def _existing_max_dd(con: sqlite3.Connection, portfolio_id: int, date: str) -> float:
    row = con.execute("""
        SELECT MIN(max_drawdown_pct) FROM portfolio_equity_history
         WHERE portfolio_id = ? AND trade_date < ?
    """, (portfolio_id, date)).fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0


# ──────────────────────────────────────────────────────────────────────────
# Public entrypoints — used by main.py lifespan + scheduler
# ──────────────────────────────────────────────────────────────────────────

def run_eod_for_date(
    con: sqlite3.Connection, date: str,
) -> Dict[str, Any]:
    """Process all 5 portfolios for `date`. Returns a summary dict."""
    seed_portfolio_definitions(con)
    results = []
    for p in ALL_PORTFOLIOS:
        try:
            r = run_eod_for_portfolio(con, p, date)
            results.append(r)
        except Exception as e:
            log.exception("portfolio_engine[%s] EOD failed on %s: %s", p.slug, date, e)
            results.append({"slug": p.slug, "error": str(e)[:200], "date": date})
    return {"date": date, "portfolios": results}


def backfill(
    con: sqlite3.Connection, from_date: str, to_date: str,
) -> Dict[str, Any]:
    """Replay all 5 portfolios from `from_date` to `to_date` inclusive.
    Idempotent — re-running the same range is safe (UNIQUE constraints +
    REPLACE on equity rows make it converge to the same final state).
    """
    seed_portfolio_definitions(con)
    days = _trading_days_between(con, from_date, to_date)
    log.info("portfolio backfill: %d trading days from %s to %s",
              len(days), from_date, to_date)
    summary = {"days": len(days), "from": from_date, "to": to_date, "errors": []}
    for d in days:
        try:
            run_eod_for_date(con, d)
        except Exception as e:
            log.exception("backfill: day %s failed: %s", d, e)
            summary["errors"].append({"date": d, "reason": str(e)[:200]})
    return summary


# CLI entry: python -m power_user.services.portfolio_engine backfill 2026-01-01 2026-05-15
if __name__ == "__main__":
    import sys, os
    _BACKEND = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if _BACKEND not in sys.path:
        sys.path.insert(0, _BACKEND)
    from power_user.config import POWER_DB_PATH

    cmd = sys.argv[1] if len(sys.argv) > 1 else "backfill"
    if cmd == "backfill":
        f = sys.argv[2] if len(sys.argv) > 2 else "2026-01-01"
        t = sys.argv[3] if len(sys.argv) > 3 else datetime.now(IST).date().isoformat()
        c = sqlite3.connect(POWER_DB_PATH, timeout=60.0)
        c.row_factory = sqlite3.Row
        try:
            r = backfill(c, f, t)
            print(json.dumps(r, indent=2, default=str))
        finally:
            c.close()
    elif cmd == "seed":
        c = sqlite3.connect(POWER_DB_PATH, timeout=60.0)
        try:
            n = seed_portfolio_definitions(c)
            print(f"seeded {n} portfolios")
        finally:
            c.close()
    elif cmd == "wipe-and-reseed":
        # USE WITH CARE: wipes EVERY portfolio position/equity/event row,
        # reseeds the 5 V3-locked personas. Run before a fresh backfill when
        # the persona parameters change (e.g. the 2026-05-16 lock-down).
        c = sqlite3.connect(POWER_DB_PATH, timeout=60.0)
        c.row_factory = sqlite3.Row
        try:
            r = wipe_and_reseed(c)
            print(json.dumps(r, indent=2, default=str))
        finally:
            c.close()
    else:
        print(f"unknown command: {cmd}", file=sys.stderr)
        sys.exit(2)
