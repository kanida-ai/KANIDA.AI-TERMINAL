"""
1-MINUTE TICK-LEVEL BACKTEST — the new source of truth, built for the SUB-MINUTE milestone (<1 min run,
5 min hard ceiling). Architecture: signals stay daily (already mined); the 1-minute bars only refine the
EXACT fill time + price of each order. Parallel-load per worker (indexed range query ~0.7s each), then
pure-numpy vectorized fills — no per-trade DB hits, no minute-by-minute Python loops.

Fill model (real intraday):
  entry  = 09:15 opening bar (the day's first 1-min open)
  MIS / MIS-Daily short exit = 15:20 square-off bar (actual traded price/time)
  single (CNC/NRML/MIS) exit = first minute the target is touched (exact time+price) else 15:30 close
Emits reports/sellable_tradelogs_1min.csv (+ _positions) and per-worker aggregates. P&L now reflects
actual intraday fills, so it supersedes the daily-model numbers.
Run: PYTHONIOENCODING=utf-8 python arena/backtest_1min.py [ALL|SYM ...]
"""
import os, sys, json, sqlite3, time, csv
os.environ.setdefault("OMP_NUM_THREADS", "1")
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "arena")); sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
from mine_phase1 import apply_rule
import features as FE
import routing

KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db"); REP = ROOT / "reports"
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
CAP = 1_000_000.0; POS_FRAC = 0.15; MARGIN = POS_FRAC * CAP; SELECT_MAX_YEAR = 2024; MIN_OCC_PRE = 15


def load_1min(symbol):
    """Per-date intraday arrays for 2025-26. Returns {date: dict(hm, o,h,l,c)} + ordered date list."""
    con = sqlite3.connect(KDB)
    df = pd.read_sql("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? "
                     "AND bar_time>=? AND bar_time<? ORDER BY bar_time", con,
                     params=[symbol, "2025-01-01", "2027-01-01"])
    con.close()
    if df.empty: return {}
    d = df["bar_time"].str[:10].values; hm = df["bar_time"].str[11:16].values
    o = df["open"].values; h = df["high"].values; l = df["low"].values; c = df["close"].values
    out = {}; cut = np.where(d[1:] != d[:-1])[0] + 1; bounds = [0, *cut.tolist(), len(d)]
    for a, b in zip(bounds[:-1], bounds[1:]):
        out[d[a]] = {"hm": hm[a:b], "o": o[a:b], "h": h[a:b], "l": l[a:b], "c": c[a:b]}
    return out


def px_at(day, target_hm="15:20"):
    """(price, hm) at the square-off bar (== target_hm, else the last bar <= it, else last)."""
    hm = day["hm"]; idx = np.where(hm == target_hm)[0]
    if len(idx): i = idx[0]
    else:
        le = np.where(hm <= target_hm)[0]; i = le[-1] if len(le) else len(hm) - 1
    return float(day["c"][i]), hm[i]


def sel_kept(frame, rows, is_fno):
    yr = frame["year"].values; net = {}
    for t in set(t for t, _ in rows):
        d, pct, w = TGT[t]; net[t] = routing.net_roc_series(frame, d, pct, w, is_fno)[0]
    pre = yr <= SELECT_MAX_YEAR; masks = {}
    for t, rj in rows:
        m = apply_rule(frame, [tuple(c) for c in json.loads(rj)]).values
        masks.setdefault(t, np.zeros(len(frame), bool)); masks[t] |= m
    kept = []
    for t in masks:
        r = net[t][pre & masks[t]]; r = r[np.isfinite(r)]
        if len(r) >= MIN_OCC_PRE and r.mean() > 0: kept.append((r.mean(), t, TGT[t][2], masks[t]))
    kept.sort(reverse=True, key=lambda x: x[0])
    return kept, net


def bt_worker(args):
    symbol, is_fno = args
    con = sqlite3.connect(SNR)
    rows = [(t, rj) for t, rj in con.execute("SELECT target,rule_json FROM unified_patterns WHERE symbol=?", (symbol,)).fetchall() if t in TGT]
    con.close()
    if not rows: return symbol, [], None
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return symbol, [], None
    kept, net = sel_kept(frame, rows, is_fno)
    if not kept: return symbol, [], None
    m1 = load_1min(symbol)
    if not m1: return symbol, [], None
    O = frame["_o"].values; yr = frame["year"].values; idx = frame.index; n = len(O)
    dstr = np.array([str(x.date()) for x in idx])
    te = np.where((yr == 2025) | (yr == 2026))[0]; out = []; day = 0; pid = 0
    while day < len(te):
        gi = te[day]; chosen = None
        for pe, t, w, mask in kept:
            if mask[gi] and np.isfinite(net[t][gi]): chosen = t; break
        if chosen is None: day += 1; continue
        t = chosen; d, pct, w = TGT[t]; product, mode = routing.route(d, w, is_fno); long = (d == "up")
        r = routing.sim_roc(O, frame["_h"].values, frame["_l"].values, frame["_c"].values, gi, d, pct, w, product, mode)
        if r is None: day += 1; continue
        exi = r[2]; pid += 1
        if mode == "daily":                                            # MIS-Daily: one intraday short per day
            leg = 0
            for x in range(gi + 1, min(gi + 1 + w, n)):
                ds = dstr[x]; dd = m1.get(ds)
                if dd is None or len(dd["o"]) == 0: continue
                leg += 1
                entry = float(dd["o"][0]); ex_px, ex_hm = px_at(dd, "15:20")
                out.append(mk(symbol, pid, leg, w, dstr[gi], ds, "09:15", entry, ds, ex_hm, ex_px,
                              long, "MIS", "MIS-Daily", "intraday square-off", exi - gi))
            day += max(1, exi - gi)
        else:                                                          # single: enter 09:15, exit at target minute / close
            eday = dstr[gi + 1]; dd = m1.get(eday)
            if dd is None or len(dd["o"]) == 0: day += max(1, exi - gi); continue
            entry = float(dd["o"][0]); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
            ex_px = ex_hm = ex_date = None; reason = "time-exit"
            for x in range(gi + 1, min(gi + 1 + w, n)):
                dx = m1.get(dstr[x])
                if dx is None: continue
                hit = np.where(dx["h"] >= tgt)[0] if long else np.where(dx["l"] <= tgt)[0]
                if len(hit): ex_px = tgt; ex_hm = dx["hm"][hit[0]]; ex_date = dstr[x]; reason = "target-hit"; break
            if ex_px is None:                                          # time-exit: square-off (MIS) or close (else)
                lastd = m1.get(dstr[exi]) or dd
                if product == "MIS": ex_px, ex_hm = px_at(lastd, "15:20")
                else: ex_px, ex_hm = float(lastd["c"][-1]), "15:30"
                ex_date = dstr[exi]
            out.append(mk(symbol, pid, 1, 1, dstr[gi], eday, "09:15", entry, ex_date, ex_hm, ex_px,
                          long, product, product, reason, exi - gi))
            day += max(1, exi - gi)
    return symbol, out, len(kept)


def mk(sym, pid, leg, tot, sig, ed, et, entry, xd, xt, xpx, long, product, label, reason, hold):
    lev = routing.LEV[product]; cost = routing.COST[product]; notional = MARGIN * lev
    qty = int(notional / entry) if entry > 0 else 0
    gross = (xpx / entry - 1) * 100 if long else (1 - xpx / entry) * 100
    gpnl = notional * gross / 100; crs = notional * cost / 100; net = gpnl - crs
    return {"symbol": sym, "position_id": pid, "leg_no": leg, "total_legs": tot, "signal_date": sig,
            "entry_date": ed, "entry_time": et, "entry_price": round(entry, 2), "exit_date": xd,
            "exit_time": xt, "exit_price": round(float(xpx), 2), "direction": "Long" if long else "Short",
            "order_type": label, "qty": qty, "leverage": lev, "notional_rs": round(notional),
            "gross_move_pct": round(float(gross), 3), "gross_pnl_rs": round(float(gpnl)),
            "cost_rs": round(crs), "net_pnl_rs": round(float(net)), "hold": int(hold),
            "net_return_on_margin_pct": round(float(net / MARGIN * 100), 2), "exit_reason": reason}


def sellable():
    a = REP / "expectancy_scorecard.csv"; b = REP / "expectancy_scorecard_NEW.csv"
    sc = b if (b.exists() and (not a.exists() or b.stat().st_mtime > a.stat().st_mtime)) else a
    d = pd.read_csv(sc); return sorted(d[(d.sustained == True) & (d.min_netexp >= 1.0)].symbol)


def main():
    import multiprocessing as mp
    T0 = time.time(); args = sys.argv[1:] or ["ALL"]
    kc = sqlite3.connect(KDB); fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = sellable() if args[0].upper() == "ALL" else args
    tasks = [(s, int(fno.get(s, 0))) for s in syms]
    allrows = []; agg = {}
    with mp.get_context("spawn").Pool(min(8, len(tasks))) as pool:
        for sym, orders, keptn in pool.imap_unordered(bt_worker, tasks):
            if orders: allrows.extend(orders); agg[sym] = sum(o["net_pnl_rs"] for o in orders)
    cols = list(allrows[0].keys()) if allrows else []
    out = REP / "sellable_tradelogs_1min.csv"
    try: fh = open(out, "w", newline="", encoding="utf-8")
    except PermissionError: out = REP / "sellable_tradelogs_1min_fixed.csv"; fh = open(out, "w", newline="", encoding="utf-8")
    with fh: w = csv.DictWriter(fh, fieldnames=cols); w.writeheader(); w.writerows(allrows)
    (REP / "worker_pnl_1min.json").write_text(json.dumps(agg), encoding="utf-8")
    dt = time.time() - T0
    print(f"1-MIN BACKTEST: {len(agg)} workers, {len(allrows):,} orders, combined net P&L Rs{sum(agg.values()):,.0f}")
    print(f"  -> {out.name} in {dt:.1f}s  [{'PASS' if dt < 60 else 'OVER'} sub-minute milestone]")


if __name__ == "__main__":
    main()
