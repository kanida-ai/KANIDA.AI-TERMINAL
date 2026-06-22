"""Co-Trading virtual-portfolio simulation (single source of truth: the
falcon-top-10 walk-forward engine).

WHY A SEPARATE SERVICE
----------------------
`persona_simulator.simulate_persona("falcon-top-10")` is the canonical,
parity-locked backtest — BUT it resets capital to ₹5L every calendar year
(`yearly_reset`) and always starts 2021-01-01. The Co-Trading page needs a
DIFFERENT view of the SAME engine:

  • the USER's own starting capital (e.g. ₹2L, ₹10L, ₹50L), not ₹5L,
  • a CONTINUOUS single cash pool from a user-chosen start_date to today —
    NO yearly reset (capital compounds across year boundaries),
  • a point-in-time portfolio: open positions + closed trades + equity curve
    + an "actions" feed (entry / exit / trail / skip).

So this module REUSES the validated engine primitives from
`persona_engine_core` (data loaders, walk-forward signal generation, and the
EXACT entry/exit/trail/stop math) but runs them as ONE continuous pass with a
single cash pool. The trading RULES are not reinvented — they are lifted
verbatim from `simulate_year` (same -7% init stop, +12% trail trigger, 10-day
Donchian trail floor → higher of entry or 10-day low, gap-down exit at the
actual open, 7-trading-day max hold, integer shares, cash-only, no leverage,
skip already-held).

The ₹50k @ ₹5L fixed allocation is scaled proportionally to the user's
capital: per_trade = capital / 10 (top_n=10), so ₹5L → ₹50k, ₹10L → ₹1L, etc.

HONESTY (locked, same as the rest of the portal)
------------------------------------------------
This is a SIMULATION on END-OF-DAY data. It is NOT real fills — entries are
modelled at the next session's open (+ slippage), exits at modelled stop /
trail / time levels using actual OHLC. No look-ahead: a position can only be
entered on a date >= start_date and its signal_date is strictly before entry.

OUT OF SCOPE: real broker execution. This module NEVER imports or touches the
auto-trade path (falcon.trade.*, kite order placement). Read-only on market
data.
"""
from __future__ import annotations

import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from .. import config
from .persona_engine_core import (
    SLIP,
    FEE,
    PersonaRunConfig,
    build_sector_map,
    compute_year_signals,
    eligible_patterns_for_year,
    entry_filter_daily,
    load_all_bars,
    load_full_patterns,
    load_panel,
    trading_days,
)
from .persona_simulator import _resolve_rnd_db_path, PERSONA_CONFIGS

log = logging.getLogger("kanida.power_user.cotrade_sim")
IST = timezone(timedelta(hours=5, minutes=30))

PROD_DB = config.POWER_DB_PATH

# ── Locked falcon-top-10 rules (mirrors persona_simulator falcon-top-10 cfg) ──
# These are NOT new numbers — they are the same knobs the falcon-top-10 persona
# uses. We read them straight off PERSONA_CONFIGS so the two views can never
# drift. top_n=10, init_stop=-0.07, trail_trigger=0.12, trail_lookback=10,
# hold_days=7, sort_key="avg_lift", min_fires=10, group_by_signal_date=True.
_FT10 = PERSONA_CONFIGS["falcon-top-10"]
_FT10_CFG: PersonaRunConfig = _FT10["run_cfg"]
_FT10_BASE_CAPITAL = float(_FT10["cash_per_year"])          # ₹5,00,000
_FT10_SIM_START = _FT10["sim_start"]                        # "2021-01-01"

STYLE_FALCON_TOP_10 = "falcon-top-10"
VALID_STYLES = {STYLE_FALCON_TOP_10}

# ── Cache (style, capital, start, end) → result, ~10 min TTL ─────────────────
_CACHE: Dict[tuple, Dict[str, Any]] = {}
_CACHE_TTL_SECONDS = 600.0

# ── Last-simulated result per cache-key, for GET /portfolio convenience ──────
_LAST_RESULT: Dict[str, Any] = {"key": None, "result": None, "at": 0.0}


def _cache_key(style: str, capital: float, start: str, end: str) -> tuple:
    return (style, round(float(capital), 2), start, end)


def _today_ist() -> str:
    return datetime.now(IST).date().isoformat()


# ════════════════════════════════════════════════════════════════════════
# CONTINUOUS WALK-FORWARD SIMULATOR
# ════════════════════════════════════════════════════════════════════════
# This is the single-cash-pool continuous version of persona_engine_core's
# `simulate_year`. The per-day entry/exit logic is identical; the only
# structural differences are:
#   (1) ONE cash pool that compounds across years (no yearly reset),
#   (2) per-year walk-forward eligible patterns + signals are pre-computed
#       once for the whole window then merged into a single signal_date map,
#   (3) it records an "actions" feed (entry / exit / trail-arm / skip).

def _run_continuous(
    sigs_by_signal_date: Dict[str, List[Dict]],
    bars: Dict[str, List[Dict]],
    td_list: List[str],
    cfg: PersonaRunConfig,
    cash_start: float,
    sector_map: Dict[str, str],
) -> Dict[str, Any]:
    """Continuous single-pool walk-forward. Mirrors simulate_year's math."""
    sym_to_dates_idx = {s: {b["date"]: i for i, b in enumerate(bs)}
                        for s, bs in bars.items()}

    # Pre-compute entry candidates (signal_date → entry_date = next session).
    by_entry: Dict[str, List[Dict]] = defaultdict(list)
    for sd, group in sigs_by_signal_date.items():
        for s in group:
            sym = s["symbol"]
            if sym not in bars:
                continue
            bs = bars[sym]
            sd_idx = next((i for i, b in enumerate(bs) if b["date"] > sd), None)
            if sd_idx is None:
                continue
            ed = bs[sd_idx]["date"]
            if not cfg.entry_filter(ed):
                continue
            by_entry[ed].append({
                "symbol": sym, "signal_date": sd, "entry_date": ed,
                "score": s["score"], "n_fires": s["n_fires"],
                "_bars_start_idx": sd_idx,
            })

    cash = cash_start
    open_pos: List[Dict] = []
    closed: List[Dict] = []
    timeline: List[Dict] = []
    actions: List[Dict] = []
    peak_eq = cash_start
    max_dd_pct = 0.0

    for d in td_list:
        # ── 1. Exits on open positions (identical math to simulate_year) ──
        new_open = []
        for p in open_pos:
            sym = p["symbol"]
            bs = bars[sym]
            idx_map = sym_to_dates_idx[sym]
            if d not in idx_map:
                new_open.append(p)
                continue
            bar_idx = idx_map[d]
            day_bar = bs[bar_idx]
            cur_ret = day_bar["close"] / p["entry_px"] - 1
            if cur_ret > p["high_water"]:
                p["high_water"] = cur_ret

            init_stop_lvl = p["entry_px"] * (1 + cfg.init_stop)
            stop_lvl = init_stop_lvl
            armed = False
            if cfg.trail_trigger is not None:
                armed = p["high_water"] >= cfg.trail_trigger
                if armed:
                    window = bs[max(0, bar_idx - cfg.trail_lookback + 1): bar_idx + 1]
                    trail_stop = max(p["entry_px"], min(b["low"] for b in window))
                    stop_lvl = max(init_stop_lvl, trail_stop)
                    # Record a trail-arm action the first time the trail engages
                    # AND ratchets the stop above the initial stop.
                    if not p.get("_trail_logged") and stop_lvl > init_stop_lvl:
                        actions.append({
                            "date": d, "type": "trail", "symbol": sym,
                            "price": round(stop_lvl, 2),
                            "reason": f"trail armed (+{cfg.trail_trigger*100:.0f}% high), "
                                      f"stop → {round(stop_lvl, 2)}",
                        })
                        p["_trail_logged"] = True
            target_lvl = (p["entry_px"] * (1 + cfg.target)
                          if cfg.target is not None else None)

            exit_reason = None
            exit_px_raw = None
            if day_bar["low"] <= stop_lvl:
                exit_reason = "TRAIL_GIVEBACK" if armed else "INIT_STOP"
                exit_px_raw = min(stop_lvl, day_bar["open"])      # gap-down honoured
            elif target_lvl is not None and day_bar["high"] >= target_lvl:
                exit_reason = "TARGET"
                exit_px_raw = max(target_lvl, day_bar["open"])
            elif d == p["time_exit_date"]:
                exit_reason = "TIME_STOP"
                exit_px_raw = day_bar["close"]

            if exit_reason:
                exit_px = exit_px_raw * (1 - SLIP)
                gross_pnl = p["shares"] * (exit_px - p["entry_px"])
                fees = p["actual_deployed"] * FEE
                net_pnl = gross_pnl - fees
                exit_proceeds = p["shares"] * exit_px - fees
                cash += exit_proceeds
                trading_hold = bar_idx - p["entry_bar_idx"] + 1
                closed.append({**p, "exit_date": d, "exit_px": exit_px,
                               "gross_pnl": gross_pnl, "fees": fees,
                               "net_pnl": net_pnl, "exit_reason": exit_reason,
                               "trading_hold_days": trading_hold})
                actions.append({
                    "date": d, "type": "exit", "symbol": sym,
                    "price": round(exit_px, 2),
                    "reason": _exit_reason_human(exit_reason, net_pnl),
                })
            else:
                new_open.append(p)
        open_pos = new_open

        # ── 2. New entries (group_by_signal_date branch — falcon-top-10) ──
        held_syms = {p["symbol"] for p in open_pos}
        raw_cands = by_entry.get(d, [])
        if cfg.group_by_signal_date:
            by_sd_today: Dict[str, List[Dict]] = defaultdict(list)
            for c in raw_cands:
                by_sd_today[c["signal_date"]].append(c)
            cands_today = []
            for sd, grp in by_sd_today.items():
                grp.sort(key=lambda x: x["score"], reverse=True)
                cands_today.extend(grp[:cfg.top_n])
            cands_today.sort(key=lambda c: c["score"], reverse=True)
            accepted_count_cap = None
        else:
            cands_today = sorted(raw_cands, key=lambda c: c["score"],
                                 reverse=True)[:cfg.top_n_candidates]
            accepted_count_cap = cfg.top_n

        accepted = []
        for c in cands_today:
            if accepted_count_cap is not None and len(accepted) >= accepted_count_cap:
                break
            sym = c["symbol"]
            if sym in held_syms:
                actions.append({
                    "date": d, "type": "skip", "symbol": sym,
                    "price": None, "reason": "already held — not doubled up",
                })
                continue
            bs = bars[sym]
            bsi = c["_bars_start_idx"]
            if bsi >= len(bs):
                continue
            ep_raw = bs[bsi]["open"]
            if ep_raw <= 0:
                continue
            ep = ep_raw * (1 + SLIP)
            allocated = cfg.fixed_per_trade
            shares = math.floor(allocated / ep)
            if shares < 1:
                actions.append({
                    "date": d, "type": "skip", "symbol": sym,
                    "price": round(ep, 2),
                    "reason": "share price exceeds per-trade allocation",
                })
                continue
            actual_deployed = shares * ep
            if cash < actual_deployed:
                actions.append({
                    "date": d, "type": "skip", "symbol": sym,
                    "price": round(ep, 2),
                    "reason": "insufficient cash for a full lot",
                })
                continue
            cash -= actual_deployed
            time_exit_idx = min(bsi + cfg.hold_days - 1, len(bs) - 1)
            time_exit_date = bs[time_exit_idx]["date"]
            accepted.append({**c, "entry_px": ep, "entry_bar_idx": bsi,
                             "shares": shares, "allocated": allocated,
                             "actual_deployed": actual_deployed,
                             "unused_cash": allocated - actual_deployed,
                             "high_water": 0.0,
                             "time_exit_date": time_exit_date})
            held_syms.add(sym)
            actions.append({
                "date": d, "type": "entry", "symbol": sym,
                "price": round(ep, 2),
                "reason": f"Falcon Top 10 pick — {shares} sh @ ₹{round(ep, 2)} "
                          f"(₹{round(actual_deployed):,})",
            })
        open_pos.extend(accepted)

        # ── 3. EOD equity + drawdown ──────────────────────────────────────
        unrealized = 0.0
        for p in open_pos:
            sym = p["symbol"]
            if d in sym_to_dates_idx[sym]:
                bar = bars[sym][sym_to_dates_idx[sym][d]]
                mtm_px = bar["close"] * (1 - SLIP)
                unrealized += p["shares"] * (mtm_px - p["entry_px"])
        deployed = sum(p["actual_deployed"] for p in open_pos)
        equity = cash + deployed + unrealized
        peak_eq = max(peak_eq, equity)
        dd = (equity - peak_eq) / peak_eq * 100 if peak_eq > 0 else 0.0
        if dd < max_dd_pct:
            max_dd_pct = dd
        timeline.append({"date": d, "equity": equity, "cash": cash,
                         "deployed": deployed, "unrealized": unrealized,
                         "drawdown_pct": dd, "n_open": len(open_pos)})

    return {
        "closed_trades": closed,
        "open_positions": open_pos,
        "timeline": timeline,
        "actions": actions,
        "ending_cash": cash,
        "ending_equity": timeline[-1]["equity"] if timeline else cash_start,
        "peak_eq": peak_eq,
        "max_dd_pct": max_dd_pct,
        "sector_map": sector_map,
    }


def _exit_reason_human(exit_reason: str, net_pnl: float) -> str:
    sign = "profit" if net_pnl >= 0 else "loss"
    label = {
        "INIT_STOP": "stop-loss hit (-7%)",
        "TRAIL_GIVEBACK": "trailing stop hit",
        "TARGET": "target reached",
        "TIME_STOP": "7-day hold elapsed",
    }.get(exit_reason, exit_reason)
    return f"{label} — booked {sign}"


# ════════════════════════════════════════════════════════════════════════
# PUBLIC ENTRY POINT
# ════════════════════════════════════════════════════════════════════════

def simulate_cotrade(
    style: str,
    capital: float,
    start_date: str,
    end_date: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """Run the falcon-top-10 walk-forward for a USER capital + start date.

    Returns the Co-Trading portfolio payload (see module docstring). Cached
    by (style, capital, start, end) for ~10 minutes.

    Capital scales the fixed ₹50k @ ₹5L allocation proportionally:
    per_trade = capital / top_n (= capital / 10).
    """
    if style not in VALID_STYLES:
        raise ValueError(f"Unknown style: {style}. Valid: {sorted(VALID_STYLES)}")
    if not (capital and capital > 0):
        raise ValueError("capital must be a positive number")
    try:
        _sd = datetime.fromisoformat(start_date).date()
    except (TypeError, ValueError):
        raise ValueError("start_date must be YYYY-MM-DD")

    end_date = end_date or _today_ist()
    try:
        _ed = datetime.fromisoformat(end_date).date()
    except (TypeError, ValueError):
        raise ValueError("end_date must be YYYY-MM-DD")
    if _ed < _sd:
        raise ValueError("end_date must be on or after start_date")
    # Clamp the lower bound to the engine's data start (no signals before then).
    if start_date < _FT10_SIM_START:
        start_date = _FT10_SIM_START

    key = _cache_key(style, capital, start_date, end_date)
    now = time.time()
    if not force:
        hit = _CACHE.get(key)
        if hit and (now - hit["_at"]) < _CACHE_TTL_SECONDS:
            log.info("cotrade_sim: cache hit %s", key)
            _LAST_RESULT.update(key=key, result=hit["result"], at=now)
            return hit["result"]

    log.info("cotrade_sim: running %s capital=%.0f %s→%s", style, capital,
             start_date, end_date)
    t0 = time.time()

    # ── Per-trade allocation scaled from the locked ₹50k @ ₹5L base ──────────
    per_trade = capital / _FT10_CFG.top_n
    run_cfg = PersonaRunConfig(
        top_n=_FT10_CFG.top_n,
        top_n_candidates=_FT10_CFG.top_n_candidates,
        fixed_per_trade=per_trade,
        hold_days=_FT10_CFG.hold_days,
        init_stop=_FT10_CFG.init_stop,
        target=_FT10_CFG.target,
        trail_trigger=_FT10_CFG.trail_trigger,
        trail_lookback=_FT10_CFG.trail_lookback,
        entry_filter=entry_filter_daily,
        sort_key=_FT10_CFG.sort_key,
        min_fires=_FT10_CFG.min_fires,
        group_by_signal_date=_FT10_CFG.group_by_signal_date,
    )

    # ── Load data (RND patterns + PROD OHLC/features) ────────────────────────
    rnd_db = _resolve_rnd_db_path()
    all_pats = load_full_patterns(rnd_db)
    X, sym, dt = load_panel(PROD_DB, start_date, end_date)
    bars = load_all_bars(PROD_DB, "2020-12-01", end_date)   # pad for trail lookback
    sector_map = build_sector_map(PROD_DB)
    all_td = trading_days(PROD_DB, start_date, end_date)
    log.info("  patterns=%d panel=%d bars=%d td=%d",
             len(all_pats), X.shape[0], len(bars), len(all_td))

    # ── Walk-forward signals: per-year eligible patterns, single merged map ──
    years_arr = np.array([int(d[:4]) for d in dt], dtype=np.int32) if len(dt) else np.array([], dtype=np.int32)
    years_in_window = sorted({int(d[:4]) for d in all_td})
    merged_sigs_by_sd: Dict[str, List[Dict]] = defaultdict(list)
    for Y in years_in_window:
        elig = eligible_patterns_for_year(all_pats, Y)
        year_mask = years_arr == Y
        if not year_mask.any():
            continue
        sigs = compute_year_signals(X, sym, dt, year_mask, elig,
                                    min_fires=run_cfg.min_fires)
        if run_cfg.sort_key == "avg_lift":
            for s in sigs:
                s["score"] = s["avg_lift"]
        for s in sigs:
            merged_sigs_by_sd[s["signal_date"]].append(s)

    # ── Run continuous single-pool sim ───────────────────────────────────────
    r = _run_continuous(dict(merged_sigs_by_sd), bars, all_td, run_cfg,
                        capital, sector_map)

    result = _build_payload(r, style, capital, start_date, end_date, sector_map)
    result["_elapsed_seconds"] = round(time.time() - t0, 1)

    _CACHE[key] = {"_at": now, "result": result}
    _LAST_RESULT.update(key=key, result=result, at=now)
    log.info("cotrade_sim: done in %.1fs (open=%d closed=%d)",
             result["_elapsed_seconds"], result["summary"]["n_open"],
             result["summary"]["n_closed"])
    return result


def _build_payload(r: Dict[str, Any], style: str, capital: float,
                   start_date: str, end_date: str,
                   sector_map: Dict[str, str]) -> Dict[str, Any]:
    """Shape the raw continuous-sim output into the Co-Trading API payload."""
    closed = r["closed_trades"]
    open_pos = r["open_positions"]
    timeline = r["timeline"]

    current_value = r["ending_equity"]
    total_pnl = current_value - capital
    return_pct = (current_value / capital - 1) * 100 if capital else 0.0
    n_closed = len(closed)
    n_wins = sum(1 for t in closed if t["net_pnl"] > 0)
    win_rate = (n_wins / n_closed * 100) if n_closed else 0.0

    # Last-close lookup for open positions' MTM, from the timeline's final day.
    last_day = timeline[-1]["date"] if timeline else None

    positions: List[Dict[str, Any]] = []
    for t in closed:
        positions.append(_position_row(t, "closed", sector_map, last_day, capital))
    for p in open_pos:
        positions.append(_position_row(p, "open", sector_map, last_day, capital))

    summary = {
        "starting_rs": round(capital, 2),
        "current_value_rs": round(current_value, 2),
        "total_pnl_rs": round(total_pnl, 2),
        "return_pct": round(return_pct, 2),
        "max_dd_pct": round(r["max_dd_pct"], 2),
        "n_open": len(open_pos),
        "n_closed": n_closed,
        "cash_rs": round(r["ending_cash"], 2),
        "win_rate_pct": round(win_rate, 2),
        "n_trades": n_closed + len(open_pos),
    }

    equity = [{"date": tl["date"], "equity_rs": round(tl["equity"], 2)}
              for tl in timeline]

    # Actions newest-first for a feed.
    actions = sorted(r["actions"], key=lambda a: a["date"], reverse=True)

    return {
        "style": style,
        "start_date": start_date,
        "end_date": end_date,
        "as_of": last_day,
        "honesty": ("Simulation on end-of-day data — modelled entries/exits, "
                    "not real fills. Not investment advice."),
        "summary": summary,
        "positions": positions,
        "equity": equity,
        "actions": actions,
        "last_updated": datetime.now(IST).isoformat(),
    }


def _position_row(t: Dict[str, Any], status: str, sector_map: Dict[str, str],
                  last_day: Optional[str], capital: float) -> Dict[str, Any]:
    """One position row for the Co-Trading payload (open or closed)."""
    entry_px = float(t["entry_px"])
    deployed = float(t.get("actual_deployed") or (t["shares"] * entry_px))
    sl_level = entry_px * (1 + _FT10_CFG.init_stop)

    if status == "closed":
        exit_px = float(t["exit_px"])
        pnl_rs = float(t["net_pnl"])
        pnl_pct = (pnl_rs / deployed * 100) if deployed else 0.0
        last_close = round(exit_px, 2)
        exit_date = t.get("exit_date")
    else:
        # Open MTM: gross unrealized on the modelled last close (slip applied).
        exit_px = float(t["exit_px"]) if t.get("exit_px") is not None else entry_px
        pnl_rs = float(t.get("gross_pnl", t["shares"] * (exit_px - entry_px)))
        pnl_pct = (pnl_rs / deployed * 100) if deployed else 0.0
        last_close = round(exit_px, 2)
        exit_date = None

    return {
        "symbol": t["symbol"],
        "sector": sector_map.get(t["symbol"], "?"),
        "tier": None,                         # tier overlay is a frontend join (signal_tier)
        "entry_date": t["entry_date"],
        "entry_price": round(entry_px, 2),
        "qty": int(t["shares"]),
        "capital_rs": round(deployed, 2),
        "sl_level": round(sl_level, 2),
        "status": status,
        "exit_date": exit_date,
        "exit_price": (round(float(t["exit_px"]), 2)
                       if status == "closed" and t.get("exit_px") is not None else None),
        "pnl_rs": round(pnl_rs, 2),
        "pnl_pct": round(pnl_pct, 2),
        "hold_days": int(t.get("trading_hold_days", 0)),
        "last_close": last_close,
    }


def get_last_portfolio() -> Dict[str, Any]:
    """Return the most recently simulated Co-Trading portfolio, or {} if none.

    Convenience for GET /cotrade/portfolio. Full per-user DB persistence is a
    deliberate non-goal for now (see router note) — this serves the last
    in-memory result so the page has something to render after a /simulate.
    """
    res = _LAST_RESULT.get("result")
    return res or {}
