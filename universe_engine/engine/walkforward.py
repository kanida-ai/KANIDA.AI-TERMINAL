"""
Pipeline A — universe-level walk-forward strategy efficacy.

For each test month:
  1. Define training window = trailing TRAIN_WINDOW_MONS ending EMBARGO_DAYS before test-month start.
  2. For each strategy, scan every (ticker, bar) in the training window across the entire universe.
     For every fire, simulate the trade (TP/SL/cap, blind + smart), accumulate stats POOLED across stocks.
     Universe-level efficacy per strategy (this train window).
  3. Bless strategies that meet the universe thresholds (PF, WR after cost, pos-months %, max consecutive losses).
  4. For the test month, replay only blessed strategies' fires and log every trade.

Multi-worker: parallelise across (strategy × stock_chunk) pairs.
"""
from __future__ import annotations
import json, math, os, sqlite3, sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.strategies import ALL_LONG_STRATEGIES
from engine.simulator import (
    simulate_signal, cost_adjusted_pnl_pct, is_win_after_cost,
)
from engine.regime import compute_realised_vol_map, regime_for, classify as regime_classify


# Default config (overridden by CLI / config file)
DEFAULTS = {
    "train_window_months": 18,
    "embargo_days": 28,
    "rr": 2.0,                              # was 1.5; raised per Path-2 reasoning
    "atr_period": 14,
    "atr_mult": 1.5,
    "stop_min_pct": 1.5,
    "stop_max_pct": 8.0,
    "hold_cap_days": 12,
    "smart_max_above_open_pct": 3.0,
    "round_trip_bps": 30.0,
    "min_trades_universe": 200,
    "min_pf_blessed": 1.20,
    "min_wr_after_cost_blessed": 0.40,
    "min_pos_months_pct_blessed": 0.55,
    "max_consec_loss_months_blessed": 4,
    # ── per-stock RS filter (replaces broad NIFTY regime gate) ──
    "rs_lookback_days": 20,                 # 20-day cross-sectional return rank
    "rs_min_percentile": 0.67,              # top 33% only
    "use_broad_nifty_gate": False,          # broad regime gate is OFF; per-stock RS replaces it
    "vol_low_max": 10.7,                    # kept for reporting only
    "vol_high_min": 13.4,
    "default_index_col": "in_nifty200",
    "min_adv_inr_cr": 100.0,
    "n_io_workers": 50,
    "n_cpu_workers_min": 10,
    "n_cpu_workers_max": 50,
}


# ─────────────────────────────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────────────────────────────

def load_universe(con: sqlite3.Connection, index_col: str) -> List[Dict[str, Any]]:
    rows = con.execute(f"""
        SELECT symbol, sector
        FROM universe_master
        WHERE is_active = 1 AND {index_col} = 1
        ORDER BY symbol
    """).fetchall()
    return [{"symbol": r[0], "sector": r[1]} for r in rows]


def load_ohlc_for_symbols(con: sqlite3.Connection, symbols: List[str]) -> Dict[str, List[Dict]]:
    out = {}
    for sym in symbols:
        rows = con.execute("""
            SELECT trade_date, open, high, low, close, volume
            FROM ohlc_daily
            WHERE symbol = ? AND quality_flag != 'rejected'
            ORDER BY trade_date
        """, (sym,)).fetchall()
        if rows:
            out[sym] = [{
                "trade_date": r[0], "open": r[1], "high": r[2],
                "low": r[3], "close": r[4], "volume": r[5],
            } for r in rows]
    return out


def attach_rs_ranks(ohlc_by_sym: Dict[str, List[Dict]], lookback: int = 20) -> None:
    """
    Cross-sectional RS rank: for each trading day, rank each stock's
    `lookback`-day return within the universe. Attach `rs_pct` (0.0–1.0,
    1.0 = best) to each bar in-place. Bars with insufficient history get rs_pct=None.
    """
    # Step 1: compute per-stock per-date 'lookback'-day return
    by_date: Dict[str, Dict[str, float]] = {}
    for sym, bars in ohlc_by_sym.items():
        for i in range(lookback, len(bars)):
            past = bars[i - lookback]["close"]; now = bars[i]["close"]
            if past and past > 0:
                ret = (now - past) / past
                by_date.setdefault(bars[i]["trade_date"], {})[sym] = ret

    # Step 2: rank within each date and back-attach
    rank_lookup: Dict[str, Dict[str, float]] = {}
    for d, sym_ret in by_date.items():
        if not sym_ret: continue
        ranked = sorted(sym_ret.items(), key=lambda kv: kv[1])
        n = len(ranked)
        for rank_idx, (sym, _) in enumerate(ranked):
            # Percentile rank: 0.0 worst, 1.0 best
            rank_lookup.setdefault(d, {})[sym] = rank_idx / max(n - 1, 1)

    # Step 3: stamp bars
    for sym, bars in ohlc_by_sym.items():
        for b in bars:
            d = b["trade_date"]
            b["rs_pct"] = rank_lookup.get(d, {}).get(sym)


# ─────────────────────────────────────────────────────────────────────
# Training-window slicing & ATR-friendly windows
# ─────────────────────────────────────────────────────────────────────

def training_window(test_year: int, test_month: int, train_months: int,
                    embargo_days: int) -> Tuple[str, str]:
    test_start = date(test_year, test_month, 1)
    train_end  = test_start - timedelta(days=embargo_days + 1)
    y = train_end.year; m = train_end.month - train_months
    while m <= 0: m += 12; y -= 1
    train_start = date(y, m, 1)
    return train_start.isoformat(), train_end.isoformat()


def restrict_to_window(bars: List[Dict], start_iso: str, end_iso: str) -> List[Dict]:
    return [b for b in bars if start_iso <= b["trade_date"] <= end_iso]


# ─────────────────────────────────────────────────────────────────────
# Worker — test ONE strategy across a chunk of stocks for one window
# ─────────────────────────────────────────────────────────────────────

def _worker_one_strategy(args):
    """Execute one strategy across N stocks within a (start_iso, end_iso) window.
    Returns aggregated trade stats for that strategy on that window."""
    strategy_name, ohlc_chunk_iter, train_start, train_end, cfg = args
    # Re-import in child process
    sys_path = str(Path(__file__).resolve().parent.parent)
    if sys_path not in sys.path:
        sys.path.insert(0, sys_path)
    from engine.strategies import ALL_LONG_STRATEGIES as _STRATS
    from engine.simulator import simulate_signal as _sim
    strat_fn = next((fn for nm, fn in _STRATS if nm == strategy_name), None)
    if strat_fn is None:
        return strategy_name, []

    # Convert ohlc to pandas DataFrame on the fly per stock (strategies expect pandas)
    try:
        import pandas as pd
    except ImportError:
        return strategy_name, []

    trades = []   # list of dicts (each trade)
    rr = cfg["rr"]; atr_p = cfg["atr_period"]; atr_m = cfg["atr_mult"]
    stop_lo = cfg["stop_min_pct"]; stop_hi = cfg["stop_max_pct"]
    cap = cfg["hold_cap_days"]; smart_above = cfg["smart_max_above_open_pct"]
    rs_min = cfg.get("rs_min_percentile", 0.67)

    for symbol, bars_full in ohlc_chunk_iter.items():
        # Restrict to training window for SIGNAL detection; allow forward bars to extend slightly past
        # for simulation. We still cut at the test start to avoid leakage of test-month bars into training.
        bars = [b for b in bars_full if b["trade_date"] <= train_end]
        if len(bars) < 60:
            continue
        df = pd.DataFrame(bars)
        # strategies use Title-cased columns
        df = df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"})

        for i in range(60, len(bars) - 1):  # ensure entry bar at i+1 exists
            if bars[i]["trade_date"] < train_start:
                continue
            # ── PER-STOCK RS FILTER (replaces broad NIFTY regime gate) ──
            rs = bars[i].get("rs_pct")
            if rs is None or rs < rs_min:
                continue
            try:
                if not strat_fn(df, i):
                    continue
            except Exception:
                continue

            entry_bar = bars[i + 1]
            # forward bars after entry for simulation; need up to cap+5 bars
            fwd = bars_full[bars_full.index(entry_bar) + 1 : bars_full.index(entry_bar) + 1 + cap + 5] \
                if entry_bar in bars_full else []
            # The above index() is O(n); cheaper to find by date
            try:
                eidx = next(j for j, b in enumerate(bars_full) if b["trade_date"] == entry_bar["trade_date"])
            except StopIteration:
                continue
            fwd = bars_full[eidx + 1 : eidx + 1 + cap + 5]
            if not fwd:
                continue

            outcome = _sim(
                entry_bar=entry_bar,
                forward_bars_after_entry=fwd,
                prev_bars_for_atr=bars_full,
                signal_day_idx=i,
                rr=rr, atr_period=atr_p, atr_mult=atr_m,
                stop_min_pct=stop_lo, stop_max_pct=stop_hi,
                hold_cap_days=cap,
                smart_max_above_open_pct=smart_above,
            )
            if not outcome.blind.entry_taken:
                continue
            trades.append({
                "symbol": symbol,
                "signal_date": bars[i]["trade_date"],
                "entry_date":  entry_bar["trade_date"],
                "blind_entry": outcome.blind.entry_price,
                "blind_exit_price": outcome.blind.exit_price,
                "blind_exit_reason": outcome.blind.exit_reason,
                "blind_days": outcome.blind.days_held,
                "blind_pnl_pct": outcome.blind.pnl_pct,
                "smart_taken": outcome.smart.entry_taken,
                "smart_pnl_pct": outcome.smart.pnl_pct if outcome.smart.entry_taken else None,
            })
    return strategy_name, trades


# ─────────────────────────────────────────────────────────────────────
# Aggregator — universe-level efficacy from pooled trades
# ─────────────────────────────────────────────────────────────────────

def aggregate_efficacy(trades: List[Dict], cost_bps: float) -> Dict[str, float]:
    if not trades:
        return {"n": 0}
    pnls_gross = [t["blind_pnl_pct"] for t in trades]
    pnls_net   = [cost_adjusted_pnl_pct(p, cost_bps) for p in pnls_gross]
    wins_net   = [p for p in pnls_net if p > 0]
    losses_net = [p for p in pnls_net if p <= 0]
    pf_gross   = (sum(p for p in pnls_gross if p > 0) / -sum(p for p in pnls_gross if p < 0)
                  if any(p < 0 for p in pnls_gross) else float('inf'))
    pf_net     = (sum(wins_net) / -sum(losses_net) if losses_net else float('inf'))
    # per-month pos rate
    by_m = defaultdict(float)
    for t, p_net in zip(trades, pnls_net):
        m = t["entry_date"][:7]; by_m[m] += p_net
    pos_months  = sum(1 for v in by_m.values() if v > 0)
    total_months = len(by_m)
    # consecutive losing months
    sorted_m = sorted(by_m.keys())
    longest = cur = 0
    for m in sorted_m:
        if by_m[m] <= 0: cur += 1; longest = max(longest, cur)
        else: cur = 0
    return {
        "n": len(trades),
        "wins_after_cost": len(wins_net),
        "wr_after_cost":  len(wins_net) / len(trades),
        "pf_gross":       pf_gross,
        "pf_net":         pf_net,
        "avg_gross":      sum(pnls_gross)/len(trades),
        "avg_net":        sum(pnls_net)/len(trades),
        "pos_months":     pos_months,
        "total_months":   total_months,
        "pos_months_pct": pos_months / max(1, total_months),
        "max_consec_loss_months": longest,
    }


def is_blessed(eff: Dict, cfg: Dict) -> bool:
    if eff.get("n", 0) < cfg["min_trades_universe"]:
        return False
    if eff.get("pf_net", 0) < cfg["min_pf_blessed"]:
        return False
    if eff.get("wr_after_cost", 0) < cfg["min_wr_after_cost_blessed"]:
        return False
    if eff.get("pos_months_pct", 0) < cfg["min_pos_months_pct_blessed"]:
        return False
    if eff.get("max_consec_loss_months", 0) > cfg["max_consec_loss_months_blessed"]:
        return False
    return True


# ─────────────────────────────────────────────────────────────────────
# Top-level runner
# ─────────────────────────────────────────────────────────────────────

def cpu_worker_count(cfg: Dict) -> int:
    cores = os.cpu_count() or 4
    return max(cfg["n_cpu_workers_min"], min(cfg["n_cpu_workers_max"], cores * 4))


def run_walkforward(con: sqlite3.Connection, test_year: int, test_month: int,
                    cfg: Dict[str, Any] = None) -> Dict[str, Any]:
    """Single-month walk-forward: train on trailing window, test on (test_year, test_month).
    Returns a result dict with blessed strategies + test-month trade ledger.
    """
    cfg = {**DEFAULTS, **(cfg or {})}
    train_start, train_end = training_window(
        test_year, test_month, cfg["train_window_months"], cfg["embargo_days"]
    )
    print(f"[walkfwd] test {test_year}-{test_month:02d}  | train {train_start} → {train_end}", flush=True)

    universe = load_universe(con, cfg["default_index_col"])
    print(f"[walkfwd] universe: {len(universe)} stocks (index col = {cfg['default_index_col']})", flush=True)
    ohlc_all = load_ohlc_for_symbols(con, [u["symbol"] for u in universe])
    print(f"[walkfwd] ohlc loaded for {len(ohlc_all)} stocks", flush=True)

    # Per-stock cross-sectional RS rank (replaces broad NIFTY regime gate)
    print(f"[walkfwd] computing per-stock RS ranks (lookback={cfg['rs_lookback_days']}d)...", flush=True)
    attach_rs_ranks(ohlc_all, lookback=cfg["rs_lookback_days"])
    print(f"[walkfwd] RS filter: top {(1-cfg['rs_min_percentile'])*100:.0f}% only "
          f"(min percentile = {cfg['rs_min_percentile']})", flush=True)

    # NIFTY regime calendar (kept for reporting only; not gating signals)
    vol_map = compute_realised_vol_map(con, lookback_days=60, symbol="NIFTY50")

    # ── Mining: parallel across strategies ──
    n_workers = cpu_worker_count(cfg)
    print(f"[walkfwd] mining all strategies × universe in {n_workers} workers...", flush=True)

    strategies = ALL_LONG_STRATEGIES
    args_list = [(name, ohlc_all, train_start, train_end, cfg) for name, _ in strategies]

    eff_per_strategy: Dict[str, Dict[str, float]] = {}
    blessed_set = set()
    blessed_trades_summary = {}

    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_worker_one_strategy, a): a[0] for a in args_list}
        done = 0
        for f in as_completed(futs):
            name = futs[f]
            try:
                _name, trades = f.result()
            except Exception as e:
                print(f"  [{name}] ERROR: {e}", flush=True)
                continue
            eff = aggregate_efficacy(trades, cost_bps=cfg["round_trip_bps"])
            eff_per_strategy[name] = eff
            blessed = is_blessed(eff, cfg)
            if blessed:
                blessed_set.add(name)
                blessed_trades_summary[name] = eff
            done += 1
            tag = "BLESSED" if blessed else "—"
            print(f"  [{done:>3}/{len(strategies)}] {name:<35} n={eff.get('n',0):>5}  "
                  f"PF_net={eff.get('pf_net',0):.2f}  WR_net={eff.get('wr_after_cost',0)*100:.0f}%  "
                  f"pos_mo={eff.get('pos_months_pct',0)*100:.0f}%  {tag}", flush=True)

    print(f"[walkfwd] {len(blessed_set)} of {len(strategies)} strategies blessed", flush=True)

    # ── Replay test month with blessed strategies ──
    print(f"[walkfwd] replaying test month {test_year}-{test_month:02d} with blessed strategies...", flush=True)

    # For replay, we need bars in the test month
    test_prefix = f"{test_year}-{test_month:02d}"
    test_trades = []
    import pandas as pd
    rs_min = cfg.get("rs_min_percentile", 0.67)

    for symbol, bars in ohlc_all.items():
        if not bars or len(bars) < 60: continue
        df = pd.DataFrame(bars).rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"})

        for i in range(60, len(bars) - 1):
            sd = bars[i]["trade_date"]
            if not sd.startswith(test_prefix): continue
            # Per-stock RS filter (same as training)
            rs = bars[i].get("rs_pct")
            if rs is None or rs < rs_min:
                continue
            entry_bar = bars[i + 1]
            try:
                eidx = next(j for j, b in enumerate(bars) if b["trade_date"] == entry_bar["trade_date"])
            except StopIteration:
                continue
            fwd = bars[eidx + 1 : eidx + 1 + cfg["hold_cap_days"] + 5]
            if not fwd: continue

            for name in blessed_set:
                strat_fn = next((fn for nm, fn in ALL_LONG_STRATEGIES if nm == name), None)
                if strat_fn is None: continue
                try:
                    if not strat_fn(df, i): continue
                except Exception:
                    continue
                # Regime
                regime = regime_for(vol_map, sd, cfg["vol_low_max"], cfg["vol_high_min"])
                outcome = simulate_signal(
                    entry_bar=entry_bar,
                    forward_bars_after_entry=fwd,
                    prev_bars_for_atr=bars,
                    signal_day_idx=i,
                    rr=cfg["rr"], atr_period=cfg["atr_period"], atr_mult=cfg["atr_mult"],
                    stop_min_pct=cfg["stop_min_pct"], stop_max_pct=cfg["stop_max_pct"],
                    hold_cap_days=cfg["hold_cap_days"],
                    smart_max_above_open_pct=cfg["smart_max_above_open_pct"],
                )
                if not outcome.blind.entry_taken: continue
                test_trades.append({
                    "symbol": symbol,
                    "strategy": name,
                    "signal_date": sd,
                    "entry_date":  entry_bar["trade_date"],
                    "rs_pct": round(bars[i].get("rs_pct") or 0, 3),
                    "regime": regime,
                    "blind_entry":  outcome.blind.entry_price,
                    "blind_target": outcome.blind.target_price,
                    "blind_stop":   outcome.blind.stop_price,
                    "blind_exit_date": outcome.blind.exit_date,
                    "blind_exit_price": outcome.blind.exit_price,
                    "blind_exit_reason": outcome.blind.exit_reason,
                    "blind_days":   outcome.blind.days_held,
                    "blind_pnl_pct": outcome.blind.pnl_pct,
                    "smart_taken":  outcome.smart.entry_taken,
                    "smart_entry":  outcome.smart.entry_price if outcome.smart.entry_taken else None,
                    "smart_exit_price": outcome.smart.exit_price if outcome.smart.entry_taken else None,
                    "smart_exit_reason": outcome.smart.exit_reason if outcome.smart.entry_taken else "rejected_smart",
                    "smart_days":   outcome.smart.days_held if outcome.smart.entry_taken else 0,
                    "smart_pnl_pct": outcome.smart.pnl_pct if outcome.smart.entry_taken else None,
                })

    return {
        "test_year": test_year, "test_month": test_month,
        "train_start": train_start, "train_end": train_end,
        "universe_size": len(universe),
        "blessed_strategies": sorted(blessed_set),
        "efficacy_per_strategy": eff_per_strategy,
        "test_trades": test_trades,
    }
