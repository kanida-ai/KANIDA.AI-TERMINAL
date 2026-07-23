"""
Multi-timeframe walk-forward — runs the same 107 long strategies across
1m, 5m, 15m, 30m bars. End-of-day forced exit (true intraday).

Key differences from daily walkforward.py:
  - timeframe parameter selects ohlc_{tf}min table
  - hold_cap measured in BARS, not days; default = 1 trading day per TF
  - intraday RS rank: replaced with daily RS (pre-computed once on daily,
    looked up at signal day) — keeps RS regime semantically meaningful
  - simulate_long walks bars on the SAME TF (not daily forward bars)
"""
from __future__ import annotations
import math, os, sqlite3, sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.strategies import ALL_LONG_STRATEGIES
from engine.simulator import cost_adjusted_pnl_pct
from engine.family import family_for, deflated_sharpe_ratio


# Bars per trading day per timeframe (NSE 9:15-15:30 = 6h15m = 375min)
BARS_PER_DAY = {
    "1min":  375,
    "5min":  75,
    "15min": 25,
    "30min": 13,
}

DEFAULTS = {
    "train_window_months": 6,           # less than daily — intraday has plenty of bars
    "embargo_days": 7,
    "rr": 2.0,
    "stop_min_pct": 0.5,                # smaller for intraday
    "stop_max_pct": 4.0,
    "atr_period": 14,
    "atr_mult": 1.5,
    "round_trip_bps": 30.0,
    "rs_lookback_days": 20,
    "rs_min_percentile": 0.67,
    # Promotion thresholds
    "min_trades_per_tf": 100,
    "min_pf_pass": 1.10,                # per-TF pass bar (lower than daily because we need cross-TF agreement)
    "min_wr_after_cost_pass": 0.38,
    "min_tfs_for_promotion": 2,         # must pass on ≥2 of 4 TFs
    "default_index_col": "in_nifty200",
    "n_io_workers": 16,
    "n_cpu_workers_min": 10,
    "n_cpu_workers_max": 48,
}


# ─────────────────────────────────────────────────────────────────────
# Bar loaders
# ─────────────────────────────────────────────────────────────────────

def load_bars_for_symbol(con: sqlite3.Connection, symbol: str, tf: str,
                         start_iso: str, end_iso: str) -> List[Dict]:
    """tf in {1min, 5min, 15min, 30min}. Returns list of dicts."""
    table = f"ohlc_{tf}"
    rows = con.execute(f"""
        SELECT bar_time, open, high, low, close, volume
        FROM {table}
        WHERE symbol = ? AND bar_time BETWEEN ? AND ?
        ORDER BY bar_time
    """, (symbol, start_iso + " 00:00:00", end_iso + " 23:59:59")).fetchall()
    return [{
        "bar_time": r[0], "trade_date": r[0][:10],
        "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5],
    } for r in rows]


def load_universe(con: sqlite3.Connection, index_col: str) -> List[str]:
    rows = con.execute(f"""
        SELECT symbol FROM universe_master
        WHERE is_active = 1 AND {index_col} = 1
        ORDER BY symbol
    """).fetchall()
    return [r[0] for r in rows]


def load_daily_close_map(con: sqlite3.Connection, symbols: List[str]) -> Dict[str, Dict[str, float]]:
    """For RS computation: load daily closes per symbol."""
    out = {}
    for sym in symbols:
        rows = con.execute(
            "SELECT trade_date, close FROM ohlc_daily WHERE symbol = ? ORDER BY trade_date",
            (sym,)
        ).fetchall()
        if rows:
            out[sym] = {r[0]: r[1] for r in rows}
    return out


def compute_rs_ranks(daily_closes: Dict[str, Dict[str, float]],
                     lookback: int = 20) -> Dict[str, Dict[str, float]]:
    """Daily RS percentile per (date, symbol). 1.0 = best, 0.0 = worst."""
    # Per-stock daily returns over lookback
    sym_dates = {sym: sorted(c.keys()) for sym, c in daily_closes.items()}
    # Aggregate by date
    by_date_returns = defaultdict(dict)
    for sym, dates in sym_dates.items():
        closes = daily_closes[sym]
        for i in range(lookback, len(dates)):
            past = closes[dates[i - lookback]]; now = closes[dates[i]]
            if past and past > 0:
                by_date_returns[dates[i]][sym] = (now - past) / past
    # Rank
    rank_map = defaultdict(dict)
    for d, sym_ret in by_date_returns.items():
        if not sym_ret: continue
        ranked = sorted(sym_ret.items(), key=lambda kv: kv[1])
        n = len(ranked)
        for idx, (sym, _) in enumerate(ranked):
            rank_map[d][sym] = idx / max(n - 1, 1)
    return rank_map


# ─────────────────────────────────────────────────────────────────────
# Worker — test ONE strategy on ONE TF
# ─────────────────────────────────────────────────────────────────────

def _worker_strategy_tf(args):
    """Test one strategy on one timeframe over a window. Returns trades + family."""
    strategy_name, tf, db_path, train_start, train_end, symbols, rs_rank_map, cfg = args

    sys_path = str(Path(__file__).resolve().parent.parent)
    if sys_path not in sys.path: sys.path.insert(0, sys_path)
    from engine.strategies import ALL_LONG_STRATEGIES as _STRATS
    strat_fn = next((fn for nm, fn in _STRATS if nm == strategy_name), None)
    if strat_fn is None: return strategy_name, tf, []

    try:
        import pandas as pd
    except ImportError:
        return strategy_name, tf, []

    con = sqlite3.connect(db_path, timeout=60.0)
    rs_min = cfg["rs_min_percentile"]
    bars_per_day = BARS_PER_DAY[tf]
    hold_cap_bars = bars_per_day            # 1 trading day forced exit
    rr = cfg["rr"]
    stop_lo = cfg["stop_min_pct"]; stop_hi = cfg["stop_max_pct"]
    cost_bps = cfg["round_trip_bps"]
    atr_p = cfg["atr_period"]; atr_m = cfg["atr_mult"]

    trades = []

    for symbol in symbols:
        # Load bars in training window
        bars = load_bars_for_symbol(con, symbol, tf, train_start, train_end)
        if len(bars) < bars_per_day * 5:           # need at least a week of bars
            continue
        df = pd.DataFrame(bars).rename(columns={
            "open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"
        })

        # Walk bars
        for i in range(60, len(bars) - 1):
            sd_str = bars[i]["trade_date"]
            # Daily RS filter (stamp by entry-day's daily rank)
            rs_pct = rs_rank_map.get(sd_str, {}).get(symbol)
            if rs_pct is None or rs_pct < rs_min:
                continue
            try:
                if not strat_fn(df, i):
                    continue
            except Exception:
                continue

            # Same-TF entry: enter at next bar's open
            entry_bar = bars[i + 1]
            if entry_bar.get("open", 0) <= 0:
                continue
            entry_price = float(entry_bar["open"])

            # ATR-based stop
            try:
                trs = []
                for j in range(max(1, i - atr_p + 2), i + 2):
                    if j >= len(bars): break
                    h = float(bars[j]["high"]); l = float(bars[j]["low"])
                    pc = float(bars[j-1]["close"])
                    trs.append(max(h - l, abs(h - pc), abs(l - pc)))
                a = sum(trs)/max(len(trs),1) if trs else 0
            except Exception:
                a = 0
            stop_pct = max(stop_lo, min(stop_hi, a / entry_price * 100 * atr_m if entry_price > 0 else stop_lo))
            target_pct = stop_pct * rr
            stop_p = entry_price * (1 - stop_pct/100)
            tgt_p  = entry_price * (1 + target_pct/100)

            # Walk forward bars on SAME TF, force-exit at hold_cap_bars
            fwd = bars[i+2 : i+2 + hold_cap_bars]
            if not fwd:
                continue

            exit_p = entry_price; exit_t = fwd[0]["bar_time"]; exit_r = "timeout"
            days_held = 0
            entry_day = entry_bar["trade_date"]
            for j, b in enumerate(fwd, start=1):
                bh = float(b["high"]); bl = float(b["low"])
                # Force same-day close at end of day
                if b["trade_date"] != entry_day:
                    # next day reached — exit at this day's open (or prev day close approx)
                    exit_p = float(fwd[j-2]["close"]) if j >= 2 else entry_price
                    exit_t = fwd[j-2]["bar_time"] if j >= 2 else b["bar_time"]
                    exit_r = "eod"
                    days_held = j - 1
                    break
                if bl <= stop_p:
                    exit_p = stop_p; exit_t = b["bar_time"]; exit_r = "sl"; days_held = j; break
                if bh >= tgt_p:
                    exit_p = tgt_p; exit_t = b["bar_time"]; exit_r = "tp"; days_held = j; break
            else:
                last = fwd[-1]
                exit_p = float(last["close"]); exit_t = last["bar_time"]; exit_r = "cap"
                days_held = len(fwd)

            pnl = (exit_p - entry_price) / entry_price * 100
            trades.append({
                "symbol": symbol, "strategy": strategy_name, "tf": tf,
                "signal_bar": bars[i]["bar_time"], "entry_bar": entry_bar["bar_time"],
                "entry": round(entry_price, 4),
                "stop": round(stop_p, 4), "target": round(tgt_p, 4),
                "exit_bar": exit_t, "exit": round(exit_p, 4),
                "exit_reason": exit_r, "bars_held": days_held,
                "pnl_pct": round(pnl, 4),
            })

    con.close()
    return strategy_name, tf, trades


# ─────────────────────────────────────────────────────────────────────
# Aggregator
# ─────────────────────────────────────────────────────────────────────

def aggregate_efficacy(trades: List[Dict], cost_bps: float) -> Dict[str, float]:
    if not trades: return {"n": 0}
    pnls_g = [t["pnl_pct"] for t in trades]
    pnls_n = [cost_adjusted_pnl_pct(p, cost_bps) for p in pnls_g]
    wins_n = [p for p in pnls_n if p > 0]
    losses_n = [p for p in pnls_n if p <= 0]
    pf_g = (sum(p for p in pnls_g if p > 0) / -sum(p for p in pnls_g if p < 0)
            if any(p < 0 for p in pnls_g) else float('inf'))
    pf_n = (sum(wins_n) / -sum(losses_n) if losses_n else float('inf'))
    # Sharpe (rough — per-trade)
    mu = sum(pnls_n) / len(pnls_n)
    var = sum((x-mu)**2 for x in pnls_n) / len(pnls_n)
    sd = math.sqrt(var) if var > 0 else 1e-9
    sharpe_per_trade = mu / sd if sd > 0 else 0
    return {
        "n": len(trades),
        "wins": len(wins_n),
        "wr_after_cost": len(wins_n)/len(trades),
        "pf_gross": pf_g,
        "pf_net": pf_n,
        "avg_gross": sum(pnls_g)/len(trades),
        "avg_net":   sum(pnls_n)/len(trades),
        "sharpe_per_trade": sharpe_per_trade,
    }


def passes_tf(eff: Dict, cfg: Dict) -> bool:
    if eff.get("n", 0) < cfg["min_trades_per_tf"]: return False
    if eff.get("pf_net", 0) < cfg["min_pf_pass"]:  return False
    if eff.get("wr_after_cost", 0) < cfg["min_wr_after_cost_pass"]: return False
    return True


# ─────────────────────────────────────────────────────────────────────
# Top-level driver: test all strategies × all TFs
# ─────────────────────────────────────────────────────────────────────

def cpu_worker_count(cfg: Dict) -> int:
    cores = os.cpu_count() or 4
    return max(cfg["n_cpu_workers_min"], min(cfg["n_cpu_workers_max"], cores * 4))


def run_intraday_walkforward(con: sqlite3.Connection,
                              tfs: List[str],
                              test_year: int, test_month: int,
                              cfg: Dict[str, Any] = None) -> Dict[str, Any]:
    cfg = {**DEFAULTS, **(cfg or {})}

    # Training window
    test_start = date(test_year, test_month, 1)
    train_end  = test_start - timedelta(days=cfg["embargo_days"] + 1)
    y = train_end.year; m = train_end.month - cfg["train_window_months"]
    while m <= 0: m += 12; y -= 1
    train_start = date(y, m, 1)
    print(f"[intraday-wf] test {test_year}-{test_month:02d} | train {train_start} → {train_end}", flush=True)

    universe = load_universe(con, cfg["default_index_col"])
    print(f"[intraday-wf] universe: {len(universe)} stocks", flush=True)

    # RS rank map (daily, applied at signal-day)
    print(f"[intraday-wf] computing daily RS ranks...", flush=True)
    daily_closes = load_daily_close_map(con, universe)
    rs_rank_map = compute_rs_ranks(daily_closes, lookback=cfg["rs_lookback_days"])
    print(f"[intraday-wf] RS map: {len(rs_rank_map)} dates", flush=True)

    db_path = str(Path(con.execute("PRAGMA database_list").fetchall()[0][2]))

    n_workers = cpu_worker_count(cfg)
    print(f"[intraday-wf] CPU workers: {n_workers}", flush=True)

    # Test each TF separately
    results: Dict[str, Dict[str, Any]] = {}     # tf -> strategy -> efficacy
    all_trades: Dict[str, Dict[str, List]] = {}  # tf -> strategy -> trades

    for tf in tfs:
        print(f"\n[intraday-wf] === TIMEFRAME: {tf} ===", flush=True)
        # Quick check: does this TF have data?
        cnt = con.execute(f"SELECT COUNT(*) FROM ohlc_{tf}").fetchone()[0]
        print(f"[intraday-wf] {tf} table has {cnt:,} bars", flush=True)
        if cnt == 0:
            print(f"[intraday-wf] SKIP {tf}: no data")
            continue

        args_list = [
            (name, tf, db_path, train_start.isoformat(), train_end.isoformat(),
             universe, rs_rank_map, cfg)
            for name, _ in ALL_LONG_STRATEGIES
        ]

        results[tf] = {}
        all_trades[tf] = {}
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            futs = {ex.submit(_worker_strategy_tf, a): a[0] for a in args_list}
            done = 0
            for f in as_completed(futs):
                name = futs[f]
                try:
                    _name, _tf, trades = f.result()
                except Exception as e:
                    print(f"  [{name}] ERROR: {e}", flush=True); continue
                eff = aggregate_efficacy(trades, cost_bps=cfg["round_trip_bps"])
                results[tf][name] = {**eff, "family": family_for(name)}
                all_trades[tf][name] = trades
                done += 1
                tag = "PASS" if passes_tf(eff, cfg) else "—"
                if done <= 10 or done % 20 == 0 or tag == "PASS":
                    print(f"  [{tf}][{done:>3}/{len(args_list)}] {name:<32} "
                          f"n={eff.get('n',0):>5}  PF_net={eff.get('pf_net',0):.2f}  "
                          f"WR={eff.get('wr_after_cost',0)*100:.0f}%  {tag}", flush=True)

    return {
        "test_year": test_year, "test_month": test_month,
        "train_start": train_start.isoformat(), "train_end": train_end.isoformat(),
        "tfs_tested": list(results.keys()),
        "efficacy": results,
        "trades": all_trades,
        "config": cfg,
    }


def cross_tf_classify(efficacy: Dict[str, Dict[str, Dict]],
                      cfg: Dict) -> Dict[str, Dict[str, Any]]:
    """For each strategy, count how many TFs it passed on. Promote / reject / no-go."""
    all_strats = set()
    for tf_eff in efficacy.values():
        all_strats.update(tf_eff.keys())

    out = {}
    for s in all_strats:
        passed_tfs = []
        per_tf = {}
        for tf, tf_eff in efficacy.items():
            eff = tf_eff.get(s, {})
            per_tf[tf] = eff
            if passes_tf(eff, cfg):
                passed_tfs.append(tf)
        n_pass = len(passed_tfs)
        if n_pass >= cfg["min_tfs_for_promotion"]:
            cls = "promoted"
        elif n_pass == 1:
            cls = "rejected_overfit"
        elif n_pass == 0:
            cls = "no_go"
        else:
            cls = "no_go"
        out[s] = {"classification": cls, "n_pass": n_pass,
                  "passed_tfs": passed_tfs, "per_tf": per_tf,
                  "family": family_for(s)}
    return out
