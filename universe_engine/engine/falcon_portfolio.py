"""
Falcon Engine Phase 4 — portfolio simulator using promoted patterns.

Walk-forward by year:
  - Year Y trading uses ONLY patterns where mined_year < Y
  - This ensures every signal is OOS by construction
  - Year 2023 uses 2022-mined patterns
  - Year 2024 uses 2022 + 2023-mined patterns
  - ...
  - Year 2026 uses 2022, 2023, 2024, 2025-mined patterns

Signal generation:
  - For each (stock, date), evaluate every eligible promoted pattern
  - Aggregate "fire count" and "score" (sum of OOS lift of firing patterns)
  - Stock with at least 1 fire qualifies; rank by score within day

Entry / exit:
  - Entry: next-day OPEN after signal
  - Exit: T+20 close (matches primary target horizon)
       OR -7% initial stop (intraday low)
       OR trailing stop: 10-day low once high-water >= +10%
  - One position per (symbol, signal_date) — no re-entries within open hold

Cash constraints:
  - ₹30L starting (configurable)
  - ₹1L per trade
  - Max 25 concurrent positions
  - Skip if cash < ₹1L OR open positions == cap
  - When position closes, cash + P&L returned to pot

Costs:
  - 30 bps round-trip
  - 5 bps slippage each side
"""
from __future__ import annotations
import json, sqlite3, statistics, time
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np


PER_TRADE   = 100_000.0
START_CAP   = 30_00_000.0
MAX_OPEN    = 25
HOLD_DAYS   = 20
INIT_STOP   = -0.07
TRAIL_TRIGGER = 0.10
TRAIL_LOOKBACK = 10
COST_BPS    = 30.0
SLIP_BPS    = 5.0


FEATURE_COLS = [
    "range_pct","close_loc","gap_pct","body_pct","upper_wick_pct","lower_wick_pct",
    "dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20","slope_sma_50",
    "rsi_14","roc_5","roc_20","roc_60",
    "vol_vs_20d","vol_5d_vs_20d","n_sub_75v_7d","n_sub_75v_20d",
    "atr_20_pct","atr_5_vs_20","n_sub_2_5_range_7d","n_sub_3_range_7d",
    "n_higher_lows_5d","n_higher_highs_5d",
    "dist_high_10","dist_high_20","dist_high_60","dist_high_120","dist_high_252",
    "weekly_close_vs_sma20","weekly_breakout_20w","weekly_range_pct","weekly_close_loc",
    "rs_sector_20d","rs_sector_60d","rs_market_20d","rs_market_60d",
]
FEATURE_IDX = {c: i for i, c in enumerate(FEATURE_COLS)}


# ─────────────────────────────────────────────────────────────────────────────
# Load all data into memory
# ─────────────────────────────────────────────────────────────────────────────

def load_panel_with_keys(db_path: Path):
    """Load joined feature panel + symbol/date/year metadata. Sorted by (date, symbol)."""
    con = sqlite3.connect(db_path, timeout=60.0)
    feat_select = ", ".join(f"f.{c}" for c in FEATURE_COLS)
    rows = con.execute(f"""
        SELECT f.symbol, f.trade_date, {feat_select}
        FROM falcon_features f
        ORDER BY f.trade_date, f.symbol
    """).fetchall()
    con.close()
    n = len(rows)
    n_feat = len(FEATURE_COLS)
    X = np.full((n, n_feat), np.nan, dtype=np.float64)
    symbols = np.empty(n, dtype=object)
    dates = np.empty(n, dtype=object)
    years = np.zeros(n, dtype=np.int32)
    for i, r in enumerate(rows):
        symbols[i] = r[0]
        dates[i] = r[1]
        years[i] = int(r[1][:4])
        X[i] = [v if v is not None else np.nan for v in r[2:]]
    return X, symbols, dates, years


def classify_pattern_families(rule):
    """Tag a rule with the feature families it uses. Used by V7.1 to drop
    the 'drawdown_bounce' cluster that empirically degrades performance."""
    fams = set()
    for f, op, th in rule:
        if f == "weekly_close_loc" and op == ">":     fams.add("weekly_close")
        if f == "atr_20_pct" and op == ">":           fams.add("high_atr")
        if f in ("dist_high_252", "dist_high_120") and op == "<=" and th < -10:
            fams.add("drawdown_bounce")
        if f == "weekly_range_pct" and op == ">":     fams.add("weekly_range")
    return fams


def load_promoted_patterns(con: sqlite3.Connection,
                              exclude_families: Optional[List[str]] = None):
    """Returns list of dicts with rule, mined_year, oos_lift, target.

    exclude_families: optional list of family tags to drop. V7.1 default uses
    ['drawdown_bounce'] — those patterns hurt OOS performance.
    """
    exclude_families = set(exclude_families or [])
    rows = con.execute("""
        SELECT c.pattern_id, c.mined_year, c.outcome_target, c.rule_json,
               p.classification, p.avg_oos_year_lift_pp
        FROM falcon_promoted_patterns p
        INNER JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
        WHERE p.classification IN ('universal', 'regime_dependent')
        ORDER BY p.avg_oos_year_lift_pp DESC
    """).fetchall()
    out = []
    for r in rows:
        rule = [(f, op, th) for f, op, th in json.loads(r[3])]
        fams = classify_pattern_families(rule)
        if exclude_families & fams:
            continue
        out.append({
            "pattern_id": r[0],
            "mined_year": r[1],
            "target":     r[2],
            "rule":       rule,
            "classification": r[4],
            "oos_lift":   r[5],
            "families":   fams,
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Vectorized rule mask
# ─────────────────────────────────────────────────────────────────────────────

def rule_mask(rule, X):
    mask = np.ones(X.shape[0], dtype=bool)
    for f, op, th in rule:
        idx = FEATURE_IDX.get(f)
        if idx is None: return np.zeros(X.shape[0], dtype=bool)
        col = X[:, idx]
        if op == "<=":
            mask &= (col <= th) & ~np.isnan(col)
        else:
            mask &= (col > th) & ~np.isnan(col)
    return mask


# ─────────────────────────────────────────────────────────────────────────────
# Compute per-row signal score with walk-forward eligibility
# ─────────────────────────────────────────────────────────────────────────────

def compute_signals(X, years, patterns):
    """For each row, compute (n_eligible_fires, sum_eligible_lift).
    Walk-forward: pattern mined in year M only contributes to rows where year > M."""
    n = X.shape[0]
    n_fires = np.zeros(n, dtype=np.int32)
    sum_lift = np.zeros(n, dtype=np.float64)
    print(f"[signals] Evaluating {len(patterns)} patterns × {n:,} rows ...")
    t0 = time.time()
    for i, p in enumerate(patterns):
        m = rule_mask(p["rule"], X)
        # Eligibility: only count this pattern's fire on rows where year > mined_year
        eligibility = years > int(p["mined_year"])
        eligible_fire = m & eligibility
        n_fires += eligible_fire.astype(np.int32)
        sum_lift += eligible_fire.astype(np.float64) * p["oos_lift"]
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(patterns)}] elapsed={time.time()-t0:.0f}s", flush=True)
    return n_fires, sum_lift


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio simulator
# ─────────────────────────────────────────────────────────────────────────────

def trading_days_after(con, sym, after, n):
    rows = con.execute("""SELECT trade_date FROM ohlc_daily
                          WHERE symbol=? AND trade_date>? ORDER BY trade_date LIMIT ?""",
                       (sym, after, n)).fetchall()
    return [r[0] for r in rows]


def get_bars(con, sym, dates):
    """Get OHLC bars for sym on listed dates (must be sorted)."""
    if not dates: return []
    placeholders = ",".join("?" * len(dates))
    rows = con.execute(f"""
        SELECT trade_date, open, high, low, close
        FROM ohlc_daily WHERE symbol=? AND trade_date IN ({placeholders})
        ORDER BY trade_date
    """, [sym] + list(dates)).fetchall()
    return [{"date":r[0],"open":r[1],"high":r[2],"low":r[3],"close":r[4]} for r in rows]


def all_trading_days(con, start, end):
    rows = con.execute("""SELECT DISTINCT trade_date FROM ohlc_daily
                          WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date""",
                       (start, end)).fetchall()
    return [r[0] for r in rows]


def simulate(db_path: Path, signals: List[Dict],
              starting_capital: float = START_CAP,
              max_open: int = MAX_OPEN,
              hold_days: int = HOLD_DAYS,
              top_n_per_day: Optional[int] = None) -> Dict:
    """signals: list of {symbol, signal_date, score, n_fires}.
       Returns trades + timeline + metrics."""
    if not signals:
        return {"trades":[], "timeline":[], "metrics": {}}

    con = sqlite3.connect(db_path, timeout=60.0)

    # Optionally cap signals to top-N per day by score
    by_day = defaultdict(list)
    for s in signals: by_day[s["signal_date"]].append(s)
    if top_n_per_day:
        for d, group in by_day.items():
            group.sort(key=lambda x: x["score"], reverse=True)
            by_day[d] = group[:top_n_per_day]

    # Build candidate trades — entry next day, hold T+hold_days
    candidates = []
    slip = SLIP_BPS / 10_000.0
    for d, group in by_day.items():
        for s in group:
            sym = s["symbol"]
            days_after = trading_days_after(con, sym, d, hold_days + 5)
            if len(days_after) < hold_days: continue
            entry_date = days_after[0]
            exit_date  = days_after[hold_days - 1]
            bars = get_bars(con, sym, days_after[:hold_days])
            if len(bars) < hold_days: continue
            ep_raw = bars[0]["open"]
            if ep_raw <= 0: continue
            candidates.append({
                "symbol": sym, "signal_date": d,
                "entry_date": entry_date, "exit_date": exit_date,
                "entry_price_raw": ep_raw,
                "bars": bars,
                "score": s["score"],
                "n_fires": s["n_fires"],
            })

    by_entry = defaultdict(list)
    for c in candidates: by_entry[c["entry_date"]].append(c)

    sim_start = min(c["entry_date"] for c in candidates) if candidates else None
    sim_end   = max(c["exit_date"]  for c in candidates) if candidates else None
    if not sim_start: return {"trades":[], "timeline":[], "metrics":{}}
    days = all_trading_days(con, sim_start, sim_end)

    cash = starting_capital
    open_pos = []
    closed = []
    skipped = []
    timeline = []
    peak_eq = starting_capital
    max_concurrent = 0

    for d in days:
        # Check exits for open positions
        new_open = []
        for p in open_pos:
            # Determine if today triggers an exit
            day_bar = next((b for b in p["bars"] if b["date"] == d), None)
            if day_bar is None:
                # Not a trading day for this symbol (shouldn't happen since bars are limited)
                new_open.append(p); continue

            # Update high-water and trail stop
            cur_ret = day_bar["close"] / p["avg_entry"] - 1
            if cur_ret > p["high_water"]:
                p["high_water"] = cur_ret

            # Initial stop check (intraday low)
            stop_level = p["avg_entry"] * (1 + INIT_STOP)
            # Trail stop after high-water >= +10%: use 10-day low
            if p["high_water"] >= TRAIL_TRIGGER:
                # Compute trailing stop from last 10 trading days' lows
                bar_idx = next(i for i, b in enumerate(p["bars"]) if b["date"] == d)
                trail_window = p["bars"][max(0, bar_idx - TRAIL_LOOKBACK + 1): bar_idx + 1]
                trail_stop = max(p["avg_entry"], min(b["low"] for b in trail_window))
                stop_level = max(stop_level, trail_stop)

            exit_reason = None
            exit_price = None
            if day_bar["low"] <= stop_level:
                exit_reason = "trail_stop" if p["high_water"] >= TRAIL_TRIGGER else "init_stop"
                # Gap-through fix: if open is below stop, real fill is at open (not stop level).
                # Otherwise fill at stop level.
                exit_price = min(stop_level, day_bar["open"])
            elif d == p["exit_date"]:
                exit_reason = "time_stop"
                exit_price = day_bar["close"]

            if exit_reason:
                ep = exit_price * (1 - slip)
                shares = PER_TRADE / p["avg_entry"]
                gross = shares * (ep - p["avg_entry"])
                fees = PER_TRADE * COST_BPS / 10_000.0
                net = gross - fees
                cash += PER_TRADE + net
                closed.append({**p, "exit_actual_date": d, "exit_actual_price": ep,
                                "exit_reason": exit_reason, "net_pnl": net,
                                "ret_pct": net / PER_TRADE * 100})
            else:
                new_open.append(p)
        open_pos = new_open

        # New entries
        open_symbols = {p["symbol"] for p in open_pos}
        for c in by_entry.get(d, []):
            if c["symbol"] in open_symbols:
                skipped.append({**c, "reason": "already_open"}); continue
            if cash < PER_TRADE:
                skipped.append({**c, "reason": "cash"}); continue
            if len(open_pos) >= max_open:
                skipped.append({**c, "reason": "max_open"}); continue
            ep = c["entry_price_raw"] * (1 + slip)
            cash -= PER_TRADE
            open_pos.append({**c, "avg_entry": ep, "high_water": 0.0})
            open_symbols.add(c["symbol"])

        # MTM
        unrealized = 0.0
        for p in open_pos:
            day_bar = next((b for b in p["bars"] if b["date"] == d), None)
            mtm_price = (day_bar["close"] if day_bar else p["avg_entry"]) * (1 - slip)
            shares = PER_TRADE / p["avg_entry"]
            unrealized += shares * (mtm_price - p["avg_entry"])
        equity = cash + len(open_pos) * PER_TRADE + unrealized
        if equity > peak_eq: peak_eq = equity
        max_concurrent = max(max_concurrent, len(open_pos))
        timeline.append({"date":d,"cash":cash,"deployed":len(open_pos)*PER_TRADE,
                          "unrealized":unrealized,"equity":equity,
                          "n_open":len(open_pos),"drawdown":equity-peak_eq,
                          "drawdown_pct":(equity-peak_eq)/peak_eq*100 if peak_eq>0 else 0})

    # Force close any still-open at end
    for p in open_pos:
        last_bar = p["bars"][-1] if p["bars"] else None
        if last_bar is None: continue
        ep = last_bar["close"] * (1 - slip)
        shares = PER_TRADE / p["avg_entry"]
        gross = shares * (ep - p["avg_entry"])
        fees = PER_TRADE * COST_BPS / 10_000.0
        net = gross - fees
        cash += PER_TRADE + net
        closed.append({**p, "exit_actual_date": sim_end, "exit_actual_price": ep,
                        "exit_reason": "sim_end", "net_pnl": net,
                        "ret_pct": net / PER_TRADE * 100})

    con.close()

    pnls = [t["net_pnl"] for t in closed]
    wins = [v for v in pnls if v > 0]
    losses = [v for v in pnls if v <= 0]
    final_eq = timeline[-1]["equity"] if timeline else starting_capital
    monthly = defaultdict(float)
    yearly = defaultdict(float)
    for t in closed:
        monthly[t["exit_actual_date"][:7]] += t["net_pnl"]
        yearly[t["exit_actual_date"][:4]] += t["net_pnl"]

    dd_min = min((tl["drawdown"] for tl in timeline), default=0)
    dd_pct = min((tl["drawdown_pct"] for tl in timeline), default=0)
    avg_util = statistics.mean([(tl["deployed"]/tl["equity"]) if tl["equity"]>0 else 0 for tl in timeline]) if timeline else 0

    return {
        "trades": closed, "skipped": skipped, "timeline": timeline,
        "metrics": {
            "starting_capital": starting_capital,
            "ending_equity": final_eq,
            "total_pnl": final_eq - starting_capital,
            "return_pct": (final_eq/starting_capital - 1) * 100,
            "trades_taken": len(closed),
            "trades_skipped": len(skipped),
            "skip_breakdown": {
                "cash":         sum(1 for s in skipped if s["reason"]=="cash"),
                "max_open":     sum(1 for s in skipped if s["reason"]=="max_open"),
                "already_open": sum(1 for s in skipped if s["reason"]=="already_open"),
            },
            "win_rate": (sum(1 for p in pnls if p>0)/len(pnls)*100) if pnls else 0,
            "avg_win": statistics.mean(wins) if wins else 0,
            "avg_loss": statistics.mean(losses) if losses else 0,
            "wlr": (statistics.mean(wins)/abs(statistics.mean(losses))) if (wins and losses) else None,
            "best": max(pnls) if pnls else 0,
            "worst": min(pnls) if pnls else 0,
            "max_dd": dd_min, "max_dd_pct": dd_pct,
            "max_concurrent": max_concurrent,
            "avg_util": avg_util,
            "monthly": dict(monthly),
            "yearly": dict(yearly),
        }
    }
