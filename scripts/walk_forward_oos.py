"""
WALK-FORWARD OOS TEST
=====================
Train pattern_library on 2024+2025 only (mask 2026 from learner).
Replay backtest entry/exit on 2026 signal dates only.
Compare to the in-sample 2026 numbers from the existing trade_log.

Reads OHLC + config from the live DB but does NOT modify any tables.
Prints a comparison report.
"""
from __future__ import annotations
import json, sqlite3, sys, math
from collections import defaultdict
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.outcome_first.features import build_behavior_rows
from engine.outcome_first.learner import learn_outcome_patterns
from engine.backtest.run_backtest import (
    simulate_trade, post_exit_stats, classify_bucket,
    MIN_OVERLAP, RR_RATIO, BACKTEST_YEARS,
)
import os
os.environ.setdefault("PYTHONUNBUFFERED", "1")
from concurrent.futures import ProcessPoolExecutor, as_completed

DB = ROOT / "data" / "db" / "kanida_quant.db"
CONFIG = json.loads((ROOT / "config" / "prototype_config.json").read_text())

TRAIN_CUTOFF = "2025-12-31"   # patterns mined only on bars up to this date
TEST_YEAR    = "2026"          # signals emitted only in this year


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


def _chunk_learn(args):
    chunk, config = args
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from engine.outcome_first.learner import learn_outcome_patterns as _learn
    return _learn(chunk, config)


def cut_to_train(ohlcv_by_stock, cutoff):
    out = {}
    for k, rows in ohlcv_by_stock.items():
        kept = [r for r in rows if r["trade_date"] <= cutoff]
        if kept:
            out[k] = kept
    return out


def replay_backtest(ohlcv_by_stock, patterns_by_stock, year):
    """Identical entry/exit logic to run_backtest, but signals filtered to `year`
    and patterns supplied externally."""
    all_trades = []
    for (market, ticker), ohlcv_rows in ohlcv_by_stock.items():
        stock_pats = patterns_by_stock.get((market, ticker), [])
        if not stock_pats: continue
        behavior_rows = build_behavior_rows(ohlcv_rows)
        if not behavior_rows: continue
        raw_idx = {str(r["trade_date"]): i for i, r in enumerate(ohlcv_rows)}

        # collect candidates (signal_date in test year)
        cands = []
        for pat in stock_pats:
            atoms = set(s.strip() for s in str(pat.get("behavior_pattern","")).split("+") if s.strip())
            if not atoms: continue
            try:
                tm = float(pat["target_move"]); direction = str(pat["direction"])
                fw = int(pat["forward_window"]); opp = float(pat.get("opportunity_score") or 0)
            except: continue
            for brow in behavior_rows:
                sd = str(brow["trade_date"])
                if sd[:4] != year: continue
                live_atoms = set(brow.get("behavior_atoms") or [])
                if not live_atoms: continue
                ov = len(atoms & live_atoms) / len(atoms)
                if ov < MIN_OVERLAP: continue
                rsi = raw_idx.get(sd)
                if rsi is None or rsi+1 >= len(ohlcv_rows): continue
                cands.append({
                    "ticker": ticker, "sig_date": sd, "direction": direction,
                    "pat_str": pat.get("behavior_pattern",""),
                    "target_move": tm, "forward_window": fw, "opp_score": opp,
                    "overlap": ov, "raw_sig_idx": rsi, "tier": pat.get("tier","")
                })

        # dedup per (ticker, sig_date, direction): keep highest opp_score
        groups = defaultdict(list)
        for c in cands:
            groups[(c["ticker"], c["sig_date"], c["direction"])].append(c)
        deduped = []
        for g in groups.values():
            g.sort(key=lambda x: -x["opp_score"])
            deduped.append((g[0], len(g)))
        deduped.sort(key=lambda x: x[0]["sig_date"])

        last_entry = {}
        for cand, mc in deduped:
            d = cand["direction"]
            cd = max(5, cand["forward_window"]//2)
            le = last_entry.get(d)
            try: sigd = date.fromisoformat(cand["sig_date"][:10])
            except: continue
            if le and (sigd-le).days < cd: continue
            rsi = cand["raw_sig_idx"]
            entry_raw = ohlcv_rows[rsi+1]
            ep = float(entry_raw["open"] or 0)
            ed = str(entry_raw["trade_date"])
            if ep <= 0: continue
            fwd = ohlcv_rows[rsi+2 : rsi+2+cand["forward_window"]]
            if not fwd: continue
            sim = simulate_trade(ep, fwd, d, cand["target_move"])
            if sim["exit_price"] <= 0: continue
            exi = raw_idx.get(sim["exit_date"], rsi+1+sim["days"])
            mpi, p5 = post_exit_stats(ohlcv_rows, exi, d, sim["exit_price"])
            pnl = ((sim["exit_price"]-ep)/ep*100 if d=="rally" else (ep-sim["exit_price"])/ep*100)
            bucket = classify_bucket(sim["exit_reason"], sim["days"], cand["forward_window"],
                                     sim["mfe"], sim["mae"], mpi, p5)
            all_trades.append({
                "ticker": ticker, "direction": "long" if d=="rally" else "short",
                "signal_date": cand["sig_date"], "entry_date": ed,
                "entry_price": ep, "exit_price": sim["exit_price"],
                "exit_reason": sim["exit_reason"], "days": sim["days"],
                "pnl_pct": pnl, "bucket": bucket, "tier": cand["tier"],
                "target_move": cand["target_move"], "forward_window": cand["forward_window"],
            })
            last_entry[d] = date.fromisoformat(ed[:10])
    return all_trades


def summarise(label, trades):
    n = len(trades)
    if n == 0:
        print(f"{label}: 0 trades"); return
    wins = sum(1 for t in trades if t["exit_reason"] == "tp")
    avg = sum(t["pnl_pct"] for t in trades)/n
    bk = defaultdict(lambda: {"n":0,"w":0,"sum":0.0})
    for t in trades:
        b = bk[t["bucket"]]
        b["n"] += 1; b["sum"] += t["pnl_pct"]
        if t["exit_reason"]=="tp": b["w"] += 1

    # daily equity for Sharpe / MDD
    by_day = defaultdict(list)
    for t in trades:
        by_day[t["entry_date"][:10]].append(t["pnl_pct"]/100)
    days = sorted(by_day.keys())
    dr = [sum(by_day[d])/len(by_day[d]) for d in days]
    mu = sum(dr)/len(dr) if dr else 0
    sd = (sum((x-mu)**2 for x in dr)/len(dr))**0.5 if dr else 0
    sh = (mu/sd*math.sqrt(252)) if sd>0 else 0
    eq=1.0; peak=1.0; mdd=0.0
    for r in dr:
        eq*=(1+r); peak=max(peak,eq); mdd=min(mdd,(eq-peak)/peak)

    print(f"\n{label}")
    print(f"  total      : {n}")
    print(f"  WR (TP)    : {wins/n*100:5.1f}%  ({wins}/{n})")
    print(f"  avg pnl    : {avg:+.3f}% / trade")
    print(f"  Sharpe(ann): {sh:+.2f}  (gross, no costs)")
    print(f"  Max DD     : {mdd*100:+.1f}%")
    print(f"  cum equity : {(eq-1)*100:+.1f}%  ({len(days)} trading days)")
    print(f"  Buckets:")
    for b, s in sorted(bk.items()):
        print(f"    {b:<10} n={s['n']:>4}  WR={s['w']/s['n']*100:5.1f}%  avg={s['sum']/s['n']:+.2f}%")
    # cost sensitivity
    print(f"  Net cum @ cost (round-trip):")
    for c_bps in (0,5,10,15,20):
        c = c_bps/10000
        eq=1.0
        for r in dr: eq*=(1+r-c)
        avg_t = avg - c_bps/100
        print(f"    {c_bps:>2} bps -> cum {(eq-1)*100:+6.1f}%  avg/trade {avg_t:+.3f}%")


def main():
    print(f"Walk-forward OOS test")
    print(f"  Train cutoff : <= {TRAIN_CUTOFF}  (patterns mined on bars up to this date only)")
    print(f"  Test year    : {TEST_YEAR}")
    print(f"  Config       : {CONFIG.get('outcome_first',{}).get('forward_windows','?')} fwd / {CONFIG.get('outcome_first',{}).get('move_targets','?')} targets")

    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    print("\nLoading OHLCV ...")
    full_ohlcv = load_ohlcv(con)
    print(f"  {len(full_ohlcv)} stocks, full window")

    print(f"\nMasking OHLCV to <= {TRAIN_CUTOFF} for the learner ...")
    train_ohlcv = cut_to_train(full_ohlcv, TRAIN_CUTOFF)
    train_bars = sum(len(v) for v in train_ohlcv.values())
    full_bars  = sum(len(v) for v in full_ohlcv.values())
    print(f"  train: {train_bars} bars  vs  full: {full_bars} bars")

    print("\nMining patterns on TRAIN-ONLY data (parallel) ...", flush=True)
    items = list(train_ohlcv.items())
    n_workers = min(os.cpu_count() or 4, 8)
    chunk_size = max(1, (len(items) + n_workers - 1) // n_workers)
    chunks = [dict(items[i:i+chunk_size]) for i in range(0, len(items), chunk_size)]
    print(f"  {len(items)} stocks, {n_workers} workers, {len(chunks)} chunks", flush=True)
    patterns = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_chunk_learn, (c, CONFIG)): i for i, c in enumerate(chunks)}
        for f in as_completed(futs):
            r = f.result()
            patterns.extend(r)
            print(f"  chunk {futs[f]+1}/{len(chunks)} done -> {len(r)} patterns (total {len(patterns)})", flush=True)
    print(f"  {len(patterns)} patterns mined", flush=True)
    pats_by_stock = defaultdict(list)
    for p in patterns:
        pats_by_stock[(p["market"], p["ticker"])].append(p)

    # Save patterns to a JSON for inspection
    out_pat = ROOT / "scripts" / "_oos_patterns.json"
    out_pat.write_text(json.dumps([{k:p[k] for k in ("market","ticker","direction","target_move","forward_window","behavior_pattern","occurrences","hits","raw_probability","lift","tier","opportunity_score")} for p in patterns], indent=2))
    print(f"  saved -> {out_pat}")

    print(f"\nReplaying backtest on {TEST_YEAR} signals using OOS patterns + full OHLCV ...")
    oos_trades = replay_backtest(full_ohlcv, pats_by_stock, TEST_YEAR)
    print(f"  {len(oos_trades)} trades emitted")

    # Save trades
    out_t = ROOT / "scripts" / "_oos_trades.json"
    out_t.write_text(json.dumps(oos_trades, indent=2, default=str))

    # In-sample 2026 from the existing trade_log
    is_trades = []
    for r in con.execute("""
      SELECT ticker, direction, signal_date, entry_date, entry_price, exit_price,
             exit_reason, days_held days, pnl_pct, notes
      FROM trade_log WHERE trade_type='backtest' AND substr(signal_date,1,4)=?
    """, (TEST_YEAR,)):
        n = json.loads(r["notes"])
        is_trades.append({
            "ticker": r["ticker"], "direction": r["direction"],
            "signal_date": r["signal_date"], "entry_date": r["entry_date"],
            "entry_price": r["entry_price"], "exit_price": r["exit_price"],
            "exit_reason": r["exit_reason"], "days": r["days"],
            "pnl_pct": r["pnl_pct"], "bucket": n.get("bucket","?"),
            "tier": n.get("tier",""), "target_move": 0, "forward_window": 0,
        })

    print("\n" + "="*60)
    print(f"COMPARISON: 2026 IN-SAMPLE  vs  2026 OUT-OF-SAMPLE")
    print("="*60)
    summarise(f"IN-SAMPLE 2026 (patterns mined incl. 2026)", is_trades)
    summarise(f"OUT-OF-SAMPLE 2026 (patterns mined on <= {TRAIN_CUTOFF})", oos_trades)

    con.close()

if __name__ == "__main__":
    main()
