"""
Rolling-origin walk-forward with recency weighting.

For each test month from 2021-07 to 2026-04:
  - Training window = trailing 18 months ending 4 weeks before test month start
  - Recency weights = monthly exponential decay, lambda = 0.95
  - Mine pattern library on weighted training data
  - Replay test month: long-only, overlap >= 0.85, blind next-open entry, engine TP/SL
  - Record full trade ledger

Output a single concatenated OOS trade set + monthly / stock / atom-family summaries
+ no-cap and day-12-cap variants.
"""
from __future__ import annotations
import json, sqlite3, sys, math, os, csv
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from itertools import combinations
from pathlib import Path
from statistics import pstdev

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.outcome_first.features import build_behavior_rows, forward_outcomes
from engine.outcome_first.trust import trusted_probability
from engine.backtest.run_backtest import simulate_trade, RR_RATIO

DB = ROOT / "data" / "db" / "kanida_quant.db"
CONFIG = json.loads((ROOT / "config" / "prototype_config.json").read_text())

# Run parameters (defaults agreed)
MIN_OVERLAP_USER  = 0.85
RECENCY_LAMBDA    = 0.95           # monthly exponential decay
EMBARGO_DAYS      = 28             # 4-week embargo
TRAIN_WINDOW_MONS = 18             # trailing months
CAPITAL_PER_TRADE = 100_000.0
COST_BPS_RT       = 30
HOLD_CAP_DAYS     = 12
N_WORKERS         = min(os.cpu_count() or 4, 8)

OUT_LEDGER_CSV = ROOT / "walkfwd_ledger.csv"
OUT_REPORT_MD  = ROOT / "WALKFWD_REPORT.md"
OUT_TRADES_JSON = ROOT / "scripts" / "_walkfwd_trades.json"


# =========================================================================
# Weighted learner — copy of engine.outcome_first.learner with recency wts
# =========================================================================

def _candidate_patterns(atoms, max_size):
    priority_prefixes = (
        "trend_20:", "flow:", "volatility:", "ma_position:", "breakout_state:",
        "volume:", "candle:", "sr_state:", "range_state:", "ma_slope:", "gap_state:",
    )
    atoms = [a for a in atoms if a.startswith(priority_prefixes)]
    patterns = [(a,) for a in atoms]
    for size in range(2, min(max_size, len(atoms)) + 1):
        for combo in combinations(atoms, size):
            families = {c.split(":", 1)[0] for c in combo}
            if len(families) != len(combo):
                continue
            if "trend_20" not in families and "flow" not in families:
                continue
            patterns.append(combo)
    return patterns


def _direction_targets(targets):
    return [("rally", t) for t in targets] + [("fall", t) for t in targets]


def _hit(outcome, direction, target):
    if direction == "rally":
        return int(outcome["max_forward_return"] >= target)
    return int(outcome["min_forward_return"] <= -target)


def _directional_return(forward_return, direction):
    return forward_return if direction == "rally" else -forward_return


def _avg_clip(values, config):
    max_abs = float(config["outcome_first"]["max_abs_return_for_scoring"])
    clipped = [max(-max_abs, min(max_abs, v)) for v in values]
    return sum(clipped) / len(clipped) if clipped else 0.0


def _tier(score, probability, lift, occurrences):
    if score >= 0.72 and probability >= 0.45 and lift >= 1.8 and occurrences >= 15:
        return "high_conviction"
    if score >= 0.56 and probability >= 0.30 and lift >= 1.25:
        return "medium"
    return "exploratory"


def recency_weight(bar_date_str: str, anchor_date: date, lam: float) -> float:
    """λ^(months_old). bar_date < anchor_date typically."""
    bd = date.fromisoformat(bar_date_str[:10])
    months_old = max(0, (anchor_date.year - bd.year) * 12 + (anchor_date.month - bd.month))
    return lam ** months_old


def _mine_patterns_weighted(all_rows, market, ticker, direction, target, window,
                            baseline_w, max_pattern_size, min_occurrences, config):
    """Weighted version: probability = weighted_hits / weighted_occurrences,
    but min_occurrences gate is on RAW count to maintain support threshold."""
    pat_wcount = defaultdict(float)
    pat_whits  = defaultdict(float)
    pat_rcount = defaultdict(int)
    pat_rhits  = defaultdict(int)
    pat_rets   = defaultdict(list)
    pat_dates  = defaultdict(list)

    for rec in all_rows:
        w = rec["weight"]
        atoms = sorted(set(rec["atoms"]))
        patterns = _candidate_patterns(atoms, max_pattern_size)
        for pattern in patterns:
            pat_wcount[pattern] += w
            pat_whits[pattern]  += w * int(rec["hit"])
            pat_rcount[pattern] += 1
            pat_rhits[pattern]  += int(rec["hit"])
            pat_rets[pattern].append(float(rec["directional_return"]))
            pat_dates[pattern].append(rec["date"])

    out = []
    for pattern, w_count in pat_wcount.items():
        raw_count = pat_rcount[pattern]
        if raw_count < min_occurrences:
            continue
        w_hits = pat_whits[pattern]
        raw_hits = pat_rhits[pattern]
        if raw_hits < max(3, min_occurrences // 3):
            continue
        probability = (w_hits / w_count) if w_count > 0 else 0.0
        trust = trusted_probability(int(round(raw_hits)), raw_count, baseline_w)
        lift = probability / baseline_w if baseline_w else 0.0
        if lift < float(config["outcome_first"]["baseline_lift_floor"]):
            continue
        rets = pat_rets[pattern]
        recent_n = int(config["outcome_first"]["recent_occurrences"])
        recent_returns = rets[-recent_n:]
        recent_hits = [1 if r >= target else 0 for r in recent_returns]
        avg_ret = _avg_clip(rets, config)
        recent_probability = (sum(recent_hits)/len(recent_hits)) if recent_hits else probability
        stability = 1.0 / (1.0 + min((pstdev(rets) if len(rets) > 1 else 0.0)/0.10, 3.0))
        complexity_penalty = max(0, len(pattern) - 2) * 0.04
        support_factor = min(1.0, math.log10(raw_count + 1) / math.log10(max(min_occurrences, 2) + 1))
        score = (0.24*probability + 0.24*min(3.0, lift)/3.0
                 + 0.18*min(1.0, max(avg_ret,0)/max(target,0.01))
                 + 0.14*support_factor + 0.10*recent_probability + 0.10*stability
                 - complexity_penalty)
        out.append({
            "market": market, "ticker": ticker, "direction": direction,
            "target_move": target, "forward_window": window,
            "pattern_size": len(pattern), "behavior_pattern": " + ".join(pattern),
            "occurrences": raw_count, "hits": raw_hits,
            "weighted_occurrences": round(w_count, 3), "weighted_hits": round(w_hits, 3),
            "baseline_probability": baseline_w,
            "raw_probability": probability, "lift": lift,
            "trusted_probability": trust["trusted_probability"],
            "opportunity_score": max(0.0, min(1.0, score)),
            "tier": _tier(score, probability, lift, raw_count),
        })
    return out


def learn_weighted(ohlcv_by_stock, config, anchor_date, lam):
    """Outer driver, parallel across stock chunks."""
    opts = config["outcome_first"]
    windows = [int(x) for x in opts["forward_windows"]]
    targets = [float(x) for x in opts["move_targets"]]
    primary_windows = set(int(x) for x in opts["primary_windows"])
    primary_targets = set(float(x) for x in opts["primary_targets"])
    min_bars = int(opts["min_history_bars"])
    max_pattern_size = int(opts["max_pattern_size"])
    min_occurrences = int(opts["min_pattern_occurrences"])
    max_per_stock_target = int(opts["max_patterns_per_stock_target"])

    learned = []
    for (market, ticker), rows in ohlcv_by_stock.items():
        if len(rows) < min_bars:
            continue
        brows = build_behavior_rows(rows)
        if not brows:
            continue
        behavior_by_date = {r["trade_date"]: r for r in brows}
        outcomes = forward_outcomes(rows, windows)

        stock_rows = []
        for window in windows:
            for direction, target in _direction_targets(targets):
                if window not in primary_windows or target not in primary_targets:
                    continue
                if direction != "rally":   # LONG ONLY
                    continue
                all_rows = []
                hits_w_sum = 0.0; weight_sum = 0.0
                for d, feat in behavior_by_date.items():
                    out = outcomes.get((d, window))
                    if not out:
                        continue
                    h = _hit(out, direction, target)
                    dret = _directional_return(out["forward_return"], direction)
                    w = recency_weight(d, anchor_date, lam)
                    all_rows.append({"date": d, "hit": h, "weight": w,
                                     "directional_return": dret,
                                     "atoms": feat["behavior_atoms"]})
                    hits_w_sum += w * h
                    weight_sum += w
                if len(all_rows) < min_bars // 2:
                    continue
                baseline_w = (hits_w_sum / weight_sum) if weight_sum > 0 else 0.0
                if baseline_w <= 0:
                    continue
                stats = _mine_patterns_weighted(all_rows, market, ticker, direction,
                                                target, window, baseline_w,
                                                max_pattern_size, min_occurrences, config)
                stock_rows.extend(stats)

        grouped = defaultdict(list)
        for r in stock_rows:
            grouped[(r["direction"], r["target_move"], r["forward_window"])].append(r)
        for grp in grouped.values():
            grp.sort(key=lambda r: (-r["opportunity_score"], -r["lift"], -r["occurrences"], r["pattern_size"]))
            learned.extend(grp[:max_per_stock_target])
    return learned


def _chunk_learn(args):
    chunk, cfg, anchor_iso, lam = args
    sys_path = str(Path(__file__).resolve().parent.parent)
    if sys_path not in sys.path:
        sys.path.insert(0, sys_path)
    return learn_weighted(chunk, cfg, date.fromisoformat(anchor_iso), lam)


# =========================================================================
# Replay test month
# =========================================================================

def replay_month(ohlcv_by_stock, pats_by_stock, year, month):
    """Replay all long signals with overlap>=0.85 in this calendar month."""
    test_prefix = f"{year}-{month:02d}"
    out = []
    for (market, ticker), ohlcv_rows in ohlcv_by_stock.items():
        stock_pats = pats_by_stock.get((market, ticker), [])
        if not stock_pats: continue
        brows = build_behavior_rows(ohlcv_rows)
        if not brows: continue
        raw_idx = {str(r["trade_date"]): i for i, r in enumerate(ohlcv_rows)}

        cands = []
        for pat in stock_pats:
            atoms = set(s.strip() for s in str(pat.get("behavior_pattern","")).split("+") if s.strip())
            if not atoms: continue
            try:
                tm = float(pat["target_move"]); fw = int(pat["forward_window"])
                opp = float(pat.get("opportunity_score") or 0)
            except: continue
            for brow in brows:
                sd = str(brow["trade_date"])
                if not sd.startswith(test_prefix): continue
                live_atoms = set(brow.get("behavior_atoms") or [])
                if not live_atoms: continue
                ov = len(atoms & live_atoms) / len(atoms)
                if ov < MIN_OVERLAP_USER: continue
                rsi = raw_idx.get(sd)
                if rsi is None or rsi+1 >= len(ohlcv_rows): continue
                cands.append({"ticker": ticker, "sig_date": sd,
                              "target_move": tm, "forward_window": fw,
                              "opp_score": opp, "overlap": ov,
                              "raw_sig_idx": rsi,
                              "pat_str": pat.get("behavior_pattern",""),
                              "claimed_prob": float(pat.get("raw_probability") or 0)})

        groups = defaultdict(list)
        for c in cands:
            groups[(c["ticker"], c["sig_date"])].append(c)
        deduped = []
        for g in groups.values():
            g.sort(key=lambda x: -x["opp_score"])
            deduped.append((g[0], len(g)))
        deduped.sort(key=lambda x: x[0]["sig_date"])

        last_entry = None
        for cand, mc in deduped:
            cd = max(5, cand["forward_window"]//2)
            try: sigd = date.fromisoformat(cand["sig_date"][:10])
            except: continue
            if last_entry and (sigd-last_entry).days < cd: continue
            rsi = cand["raw_sig_idx"]
            entry_raw = ohlcv_rows[rsi+1]
            ep = float(entry_raw["open"] or 0)
            ed = str(entry_raw["trade_date"])
            if ep <= 0: continue
            fwd = ohlcv_rows[rsi+2 : rsi+2+cand["forward_window"]]
            if not fwd: continue
            sim = simulate_trade(ep, fwd, "rally", cand["target_move"])
            if sim["exit_price"] <= 0: continue
            tp_p = ep * (1 + cand["target_move"])
            sl_p = ep * (1 - cand["target_move"]/RR_RATIO)
            pnl = ((sim["exit_price"]-ep)/ep*100) if ep>0 else 0

            # Day-12 cap variant
            cap_pnl = pnl; cap_exit_date = sim["exit_date"]; cap_reason = sim["exit_reason"]; cap_days = sim["days"]
            if sim["days"] > HOLD_CAP_DAYS and len(fwd) >= HOLD_CAP_DAYS:
                cap_bar = fwd[HOLD_CAP_DAYS-1]
                cap_close = float(cap_bar["close"] or 0)
                cap_pnl = (cap_close - ep)/ep*100 if ep>0 else 0
                cap_exit_date = str(cap_bar["trade_date"])
                cap_reason = f"cap{HOLD_CAP_DAYS}"
                cap_days = HOLD_CAP_DAYS

            out.append({
                "test_year": year, "test_month": month,
                "ticker": ticker,
                "signal_date": cand["sig_date"], "entry_date": ed,
                "entry_price": round(ep,2),
                "target_price": round(tp_p,2), "stop_price": round(sl_p,2),
                "pattern_target_move_pct": round(cand["target_move"]*100,1),
                "overlap": round(cand["overlap"],3),
                "multi_pattern_count": mc,
                "claimed_prob": round(cand["claimed_prob"],3),
                "pattern": cand["pat_str"],
                "exit_date": sim["exit_date"], "exit_price": round(sim["exit_price"],2),
                "exit_reason": sim["exit_reason"], "days_held": sim["days"],
                "pnl_pct": round(pnl,3),
                "mfe_pct": round(sim["mfe"]*100,2), "mae_pct": round(sim["mae"]*100,2),
                "cap_exit_date": cap_exit_date, "cap_exit_reason": cap_reason,
                "cap_days_held": cap_days, "cap_pnl_pct": round(cap_pnl,3),
            })
            last_entry = date.fromisoformat(ed[:10])
    return out


# =========================================================================
# Driver
# =========================================================================

def load_ohlcv(con):
    rows = con.execute("""
        SELECT market, ticker, trade_date, open, high, low, close, volume
        FROM ohlc_daily WHERE quality_flag != 'rejected'
        ORDER BY market, ticker, trade_date
    """).fetchall()
    by_stock = defaultdict(list)
    for r in rows:
        by_stock[(r[0], r[1])].append({
            "market": r[0], "ticker": r[1], "trade_date": r[2],
            "open": r[3], "high": r[4], "low": r[5],
            "close": r[6], "volume": r[7],
        })
    return by_stock


def cut_window(full, start_iso, end_iso):
    """Filter each stock's bars to start_iso <= trade_date <= end_iso."""
    out = {}
    for k, rows in full.items():
        kept = [r for r in rows if start_iso <= r["trade_date"] <= end_iso]
        if kept:
            out[k] = kept
    return out


def iter_test_months():
    """Generate (year, month) test cells from earliest feasible to 2026-04."""
    # Earliest test month: need 18 months training + 4-week embargo before it.
    # OHLC starts 2020-01-01. Earliest training start = 2020-01-01.
    # Earliest training end = 2020-01-01 + 18mo - 1 day = 2021-06-30.
    # Plus embargo 4 weeks -> earliest test month start = 2021-07-29.
    # Round to first full calendar month after that: Aug 2021.
    # (Conservative; gives ~57 months Aug-2021..Apr-2026.)
    cur = date(2021, 8, 1)
    end = date(2026, 4, 1)
    while cur <= end:
        yield cur.year, cur.month
        m = cur.month + 1; y = cur.year
        if m > 12: m = 1; y += 1
        cur = date(y, m, 1)


def training_window_for(test_year, test_month):
    """Return (start_iso, end_iso) for the 18-mo training window with 4-wk embargo."""
    test_start = date(test_year, test_month, 1)
    train_end = test_start - timedelta(days=EMBARGO_DAYS) - timedelta(days=1)
    # 18 months back from train_end
    y = train_end.year; m = train_end.month - TRAIN_WINDOW_MONS
    while m <= 0: m += 12; y -= 1
    train_start = date(y, m, 1)
    return train_start.isoformat(), train_end.isoformat()


def main():
    print(f"Loading OHLC ...", flush=True)
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
    full = load_ohlcv(con)
    print(f"  {len(full)} stocks loaded", flush=True)
    con.close()

    all_trades = []
    months = list(iter_test_months())
    print(f"Walk-forward across {len(months)} test months "
          f"({months[0][0]}-{months[0][1]:02d} to {months[-1][0]}-{months[-1][1]:02d})", flush=True)

    for i, (yr, mo) in enumerate(months, 1):
        ts_start, ts_end = training_window_for(yr, mo)
        anchor = date.fromisoformat(ts_end)
        train = cut_window(full, ts_start, ts_end)
        n_train_stocks = len(train)
        n_train_bars = sum(len(v) for v in train.values())
        t0 = datetime.now()

        # Parallel mining
        items = list(train.items())
        chunk_size = max(1, (len(items) + N_WORKERS - 1) // N_WORKERS)
        chunks = [dict(items[k:k+chunk_size]) for k in range(0, len(items), chunk_size)]
        patterns = []
        with ProcessPoolExecutor(max_workers=N_WORKERS) as ex:
            futs = [ex.submit(_chunk_learn, (c, CONFIG, ts_end, RECENCY_LAMBDA)) for c in chunks]
            for f in as_completed(futs):
                patterns.extend(f.result())

        pats_by_stock = defaultdict(list)
        for p in patterns:
            pats_by_stock[(p["market"], p["ticker"])].append(p)

        # Replay test month using full OHLC (so forward bars in test month exist)
        month_trades = replay_month(full, pats_by_stock, yr, mo)
        all_trades.extend(month_trades)

        elapsed = (datetime.now() - t0).total_seconds()
        wins = sum(1 for t in month_trades if t["exit_reason"]=="tp")
        n = len(month_trades)
        avg = (sum(t["pnl_pct"] for t in month_trades)/n) if n else 0
        print(f"  [{i:>2}/{len(months)}] test {yr}-{mo:02d} | train {ts_start}..{ts_end} ({n_train_stocks}st {n_train_bars}b) | "
              f"{len(patterns)} pats | {n} trades | WR {wins/max(1,n)*100:.0f}% | avg {avg:+.2f}% | {elapsed:.0f}s", flush=True)

    # Save trades
    OUT_TRADES_JSON.write_text(json.dumps(all_trades, indent=1, default=str))
    print(f"\nSaved {len(all_trades)} OOS trades -> {OUT_TRADES_JSON}", flush=True)

    # CSV ledger
    fields = ["test_year","test_month","signal_date","entry_date","ticker",
              "entry_price","target_price","stop_price","pattern_target_move_pct",
              "overlap","multi_pattern_count","claimed_prob","pattern",
              "exit_date","exit_price","exit_reason","days_held",
              "pnl_pct","mfe_pct","mae_pct",
              "cap_exit_date","cap_exit_reason","cap_days_held","cap_pnl_pct"]
    with open(OUT_LEDGER_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(fields)
        for t in all_trades:
            w.writerow([t.get(k,"") for k in fields])
    print(f"Saved CSV -> {OUT_LEDGER_CSV}", flush=True)
    print("\nDone mining & replay. Run scripts/build_walkfwd_report.py to build the report.", flush=True)


if __name__ == "__main__":
    main()
