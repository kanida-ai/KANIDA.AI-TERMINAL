"""
AUDITABLE per-worker TRADE LOG — every field needed to justify the P&L. Replays the exact sellable book
(same selection + non-overlap as the dashboard) and emits, for every EXECUTED order:
  position_id, leg, signal_date, entry_date, entry_price, exit_date, exit_price, direction, order_type,
  qty, leverage, notional_rs, gross_move_pct, gross_pnl_rs, cost_rs, net_pnl_rs, net_return_on_margin_pct,
  hold_days, exit_reason
MIS-Daily campaigns are expanded into their per-day intraday legs (each a real order). Sum of net_pnl_rs
reconciles to the worker's total P&L. Daily-resolution engine -> dates+prices (no intraday clock times).
Run: PYTHONIOENCODING=utf-8 python arena/trade_log.py SYM [SYM ...]   -> reports/<SYM>_tradelog.csv
"""
import sys, json, sqlite3, csv
from pathlib import Path
import numpy as np
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "arena")); sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
from mine_phase1 import apply_rule
import features as FE
import routing

SNR = str(ROOT / "db" / "KANIDA_SNR.db"); KDB = str(ROOT / "db" / "kanida.db"); REP = ROOT / "reports"
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
CAP = 1_000_000.0; POS_FRAC = 0.15; MARGIN = POS_FRAC * CAP; SELECT_MAX_YEAR = 2024; MIN_OCC_PRE = 15
COLS = ["position_id", "leg_no", "total_legs", "signal_date", "entry_date", "entry_time", "entry_price",
        "exit_date", "exit_time", "exit_price", "direction", "order_type", "qty", "leverage", "notional_rs",
        "gross_move_pct", "gross_pnl_rs", "cost_rs", "net_pnl_rs", "net_return_on_margin_pct", "hold_days", "exit_reason"]


def row(pid, leg_no, total_legs, sig_i, ent_i, exit_i, exit_px, reason, long, product, label, O, dates):
    """One detailed order row. `product` sets leverage/cost (MIS/CNC/NRML); `label` is what's displayed.
    leg_no/total_legs are plain integers so spreadsheets never misread them as dates."""
    lev = routing.LEV[product]; cost_pct = routing.COST[product]
    entry = float(O[ent_i]); notional = MARGIN * lev; qty = int(notional / entry) if entry > 0 else 0
    gross = (exit_px / entry - 1) * 100 if long else (1 - exit_px / entry) * 100
    gross_pnl = notional * gross / 100; cost_rs = notional * cost_pct / 100; net = gross_pnl - cost_rs
    # execution-model clock times (daily-bar engine): enter at OPEN 09:15; MIS/MIS-Daily square off ~15:20;
    # multi-day exits at target (exact minute not modeled at daily resolution) or CLOSE 15:30.
    entry_time = "09:15"
    exit_time = ("15:20" if label in ("MIS", "MIS-Daily") else
                 ("intraday-target" if reason == "target-hit" else "15:30"))
    return {"position_id": pid, "leg_no": leg_no, "total_legs": total_legs, "signal_date": str(dates[sig_i].date()),
            "entry_date": str(dates[ent_i].date()), "entry_time": entry_time, "entry_price": round(entry, 2),
            "exit_date": str(dates[exit_i].date()), "exit_time": exit_time, "exit_price": round(float(exit_px), 2),
            "direction": "Long" if long else "Short", "order_type": label,
            "qty": qty, "leverage": lev, "notional_rs": round(notional),
            "gross_move_pct": round(float(gross), 3), "gross_pnl_rs": round(float(gross_pnl)),
            "cost_rs": round(float(cost_rs)), "net_pnl_rs": round(float(net)),
            "net_return_on_margin_pct": round(float(net / MARGIN * 100), 2),
            "hold_days": int(exit_i - ent_i), "exit_reason": reason}


def build_log(symbol, con, is_fno):
    rows = [(t, rj) for t, rj in con.execute("SELECT target,rule_json FROM unified_patterns WHERE symbol=?", (symbol,)).fetchall() if t in TGT]
    if not rows: return []
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return []
    O, H, L, C = frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values
    dates = frame.index; yr = frame["year"].values; n = len(O)
    net = {}
    for t in set(t for t, _ in rows):
        d, pct, w = TGT[t]; net[t] = routing.net_roc_series(frame, d, pct, w, is_fno)[0]
    pre = yr <= SELECT_MAX_YEAR; kept = []
    for t, rj in rows:                                         # PER-PATTERN (matches the dashboard exactly)
        conds = [tuple(c) for c in json.loads(rj)]; mask = apply_rule(frame, conds).values
        r = net[t][pre & mask]; r = r[np.isfinite(r)]
        if len(r) >= MIN_OCC_PRE and r.mean() > 0:
            kept.append((r.mean(), t, TGT[t][2], mask))
    if not kept: return []
    kept.sort(reverse=True, key=lambda x: x[0])
    te = np.where((yr == 2025) | (yr == 2026))[0]; out = []; day = 0; pid = 0
    while day < len(te):
        gi = te[day]; chosen = None
        for pe, t, w, mask in kept:
            if mask[gi] and np.isfinite(net[t][gi]): chosen = (t, w); break
        if chosen is None: day += 1; continue
        t, w = chosen; d, pct, _ = TGT[t]; product, mode = routing.route(d, w, is_fno); long = (d == "up")
        pid += 1
        if mode == "daily":                                    # expand MIS-Daily into per-day intraday legs
            leg = 0; last = gi
            for x in range(gi + 1, min(gi + 1 + w, n)):
                if not (O[x] > 0): break
                leg += 1; last = x
                out.append(row(pid, leg, w, gi, x, x, C[x], "intraday square-off", long, "MIS", "MIS-Daily", O, dates))
            day += max(1, last - gi)
        else:
            entry = O[gi + 1]; tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
            ex = None; off = 1; reason = "time-exit"
            for k, x in enumerate(range(gi + 1, min(gi + 1 + w, n)), 1):
                off = k
                if long and H[x] >= tgt: ex = tgt; reason = "target-hit"; break
                if (not long) and L[x] <= tgt: ex = tgt; reason = "target-hit"; break
            if ex is None: ex = C[gi + off]
            out.append(row(pid, 1, 1, gi, gi + 1, gi + off, ex, reason, long, product, product, O, dates))
            day += max(1, off)
    return out


POSCOLS = ["position_id", "signal_date", "entry_date", "entry_time", "exit_date", "exit_time", "direction",
           "order_type", "n_orders", "gross_pnl_rs", "cost_rs", "net_pnl_rs", "return_on_margin_pct", "win", "exit_reason"]


def rollup_positions(log):
    """Collapse leg-level orders into one row per POSITION (= one dashboard 'trade')."""
    from collections import OrderedDict
    pos = OrderedDict()
    for r in log:
        pos.setdefault(r["position_id"], []).append(r)
    out = []
    for pid, legs in pos.items():
        f, l = legs[0], legs[-1]; net = sum(x["net_pnl_rs"] for x in legs)
        out.append({"position_id": pid, "signal_date": f["signal_date"], "entry_date": f["entry_date"],
                    "entry_time": f["entry_time"], "exit_date": l["exit_date"], "exit_time": l["exit_time"],
                    "direction": f["direction"], "order_type": f["order_type"],
                    "n_orders": len(legs), "gross_pnl_rs": sum(x["gross_pnl_rs"] for x in legs),
                    "cost_rs": sum(x["cost_rs"] for x in legs), "net_pnl_rs": net,
                    "return_on_margin_pct": round(net / MARGIN * 100, 2), "win": 1 if net > 0 else 0,
                    "exit_reason": l["exit_reason"]})
    return out


def write_csv(path, cols, rows, desc):
    try:
        fh = open(path, "w", newline="", encoding="utf-8")
    except PermissionError:
        path = path.with_name(path.stem + "_fixed" + path.suffix)
        fh = open(path, "w", newline="", encoding="utf-8")
    with fh:
        w = csv.DictWriter(fh, fieldnames=cols); w.writeheader(); w.writerows(rows)
    print(f"  wrote {path.name}  ({desc}, {len(rows):,} rows)")


def sellable_symbols():
    import pandas as pd
    a = REP / "expectancy_scorecard.csv"; b = REP / "expectancy_scorecard_NEW.csv"
    sc = b if (b.exists() and (not a.exists() or b.stat().st_mtime > a.stat().st_mtime)) else a
    d = pd.read_csv(sc)
    return sorted(d[(d.sustained == True) & (d.min_netexp >= 1.0)].symbol)


def main():
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    REP.mkdir(exist_ok=True)
    args = sys.argv[1:] or ["KEC"]
    if args[0].upper() == "ALL":                                    # combined file for all sellable workers
        import json
        dash = {c["symbol"]: c for c in json.load(open(REP / "worker_cards.json"))}   # for reconciliation
        syms = sellable_symbols()
        allrows = []; allpos = []; total = 0; nrows = 0; done = 0; bad = []
        for s in syms:
            log = build_log(s, con, int(fno.get(s, 0)))
            if not log: continue
            for r in log:
                r2 = {"symbol": s}; r2.update(r); allrows.append(r2)
            for p in rollup_positions(log):
                p2 = {"symbol": s}; p2.update(p); allpos.append(p2)
            spnl = sum(r["net_pnl_rs"] for r in log)
            disp = round(spnl / CAP * 100, 1)                                            # log return at dashboard precision
            exp = dash.get(s, {}).get("total_return")                                    # dashboard's shown return
            if exp is None or abs(disp - exp) > 0.05: bad.append((s, disp, exp))         # QC: match at 0.1% precision
            total += spnl; nrows += len(log); done += 1
            if done % 15 == 0: print(f"  ...{done}/{len(syms)} workers", flush=True)
        write_csv(REP / "sellable_tradelogs.csv", ["symbol"] + COLS, allrows, "leg/order-level")
        write_csv(REP / "sellable_positions.csv", ["symbol"] + POSCOLS, allpos, "position-level (matches dashboard)")
        print(f"\nSELLABLE: {done} workers | {len(allpos):,} positions (= dashboard 'trades') | {nrows:,} orders (legs)")
        print(f"combined net P&L Rs{total:,.0f} | RECONCILIATION vs dashboard: {done - len(bad)}/{done} workers match." +
              ("" if not bad else f" MISMATCHES: {bad[:10]}"))
    else:
        for s in args:
            log = build_log(s, con, int(fno.get(s, 0)))
            if not log:
                print(f"{s}: no trades"); continue
            p = REP / f"{s}_tradelog.csv"
            with open(p, "w", newline="", encoding="utf-8") as f:
                wtr = csv.DictWriter(f, fieldnames=COLS); wtr.writeheader(); wtr.writerows(log)
            tot = sum(r["net_pnl_rs"] for r in log); pos = len(set(r["position_id"] for r in log))
            print(f"{s}: {pos} positions / {len(log)} orders | net P&L Rs{tot:,.0f} | +{tot/CAP*100:.1f}% on Rs10L -> {p.name}")
    con.close()


if __name__ == "__main__":
    main()
