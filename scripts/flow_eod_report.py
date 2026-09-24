"""EOD flow report + VALIDATION over the full day's mkt_orderflow_1min.

Answers: does 'ride the flow' actually pay? At several decision points through the day it
ranks the cash universe by flow (move-so-far CONFIRMED by book imbalance + volume), forms a
long (top-decile) / short (bottom-decile) basket, and measures the return from that point to
15:29 — so we see whether up-push names kept rising and down-push names kept falling. Plus a
per-stock table (flow read -> EOD outcome -> paid off?) and the divergence outcomes.

Writes docs/ops/FLOW_EOD_<date>.xlsx + a short text summary. Auto-runs post-close daily; also
runnable intraday for a partial read.
"""
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
IST = timezone(timedelta(hours=5, minutes=30))
NAMED = {"AEGISVOPAK", "CUB", "NAUKRI", "RITES", "WELCORP"}
DECISIONS = ["09:30", "09:45", "10:15", "10:45", "11:30", "12:30", "13:30", "14:15"]
EXIT = "15:29"


def load(day):
    c = sqlite3.connect(str(DB))
    df = pd.read_sql_query(
        "SELECT symbol,segment,substr(bar_time,12,5) hm,open,close,volume,total_buy_qty,total_sell_qty,oi "
        "FROM mkt_orderflow_1min WHERE bar_time>=? AND bar_time<=? ",
        c, params=(day + " 09:15", day + " 15:35"))
    c.close()
    return df


def zsc(s):
    s = s.astype(float); sd = s.std()
    return (s - s.mean()) / sd if sd > 0 else s * 0


def flow_frame(cash, upto_hm):
    """per-stock cumulative flow state using bars up to upto_hm."""
    d = cash[cash.hm <= upto_hm]
    g = d.groupby("symbol")
    opn = g.open.first(); px = g.close.last()
    vol = g.volume.sum()
    imb = d.assign(bp=d.total_buy_qty / (d.total_buy_qty + d.total_sell_qty)).groupby("symbol").bp.mean() * 100
    f = pd.DataFrame({"open": opn, "px": px, "vol": vol, "imb": imb}).dropna()
    f["move"] = (f.px / f.open - 1) * 100
    f["score"] = zsc(f.move) + zsc(f.imb - 50) + 0.5 * zsc(np.log1p(f.vol))
    return f


def main():
    day = datetime.now(IST).strftime("%Y-%m-%d")
    df = load(day)
    if df.empty:
        print("no data for", day); return
    cash = df[df.segment == "CASH"].copy()
    oi = df[df.segment == "FUT"].groupby("symbol").oi.agg(["first", "last"])
    oichg = ((oi["last"] - oi["first"]) / oi["first"] * 100).rename("oichg")
    exit_px = cash[cash.hm == EXIT].set_index("symbol").close
    have_exit = not exit_px.empty
    last_hm = cash.hm.max()

    # ---- validation: ride-the-flow long-short spread at each decision point ----
    val = []
    for D in DECISIONS:
        if D > last_hm:
            continue
        f = flow_frame(cash, D)
        ref = exit_px if have_exit else cash[cash.hm == last_hm].set_index("symbol").close
        f = f.join(ref.rename("exitpx")).dropna(subset=["exitpx"])
        if len(f) < 40:
            continue
        f["fwd"] = (f.exitpx / f.px - 1) * 100
        q = f.score.quantile([0.1, 0.9])
        longs = f[f.score >= q[0.9]]; shorts = f[f.score <= q[0.1]]
        lr = longs.fwd.mean(); sr = -shorts.fwd.mean()
        val.append(dict(decision=D, n=len(f), long_ret=round(lr, 3), short_ret=round(sr, 3),
                        spread=round(lr + sr, 3), long_hit=round((longs.fwd > 0).mean() * 100, 0),
                        short_hit=round((shorts.fwd < 0).mean() * 100, 0)))
    val = pd.DataFrame(val)

    # ---- per-stock day summary (flow read -> outcome) using a mid-morning read (10:15) ----
    ref_hm = "10:15" if "10:15" <= last_hm else last_hm
    f = flow_frame(cash, ref_hm).join(oichg)
    ref = exit_px if have_exit else cash[cash.hm == last_hm].set_index("symbol").close
    f = f.join(ref.rename("exitpx"))
    f["day_ret"] = (f.exitpx / f.open - 1) * 100
    f["fwd_ret"] = (f.exitpx / f.px - 1) * 100
    f["read"] = np.where((f.move > 0) & (f.imb > 52), "UP-PUSH(long)",
                np.where((f.move < 0) & (f.imb < 48), "DOWN-PUSH(short)",
                np.where((f.move > 0) & (f.imb < 45), "DISTRIBUTION(fade)",
                np.where((f.move < 0) & (f.imb > 55), "ABSORPTION(bounce?)", "neutral"))))
    def paid(r):
        if r.read.startswith("UP-PUSH"): return r.fwd_ret > 0
        if r.read.startswith("DOWN-PUSH"): return r.fwd_ret < 0
        if r.read.startswith("DISTRIBUTION"): return r.fwd_ret < 0
        if r.read.startswith("ABSORPTION"): return r.fwd_ret > 0
        return None
    f["paid_off"] = f.apply(paid, axis=1)

    # ---- text summary ----
    print(f"FLOW EOD REPORT {day}  (data to {last_hm}{' — FINAL' if have_exit else ' — partial'})")
    print("\n=== Does riding the flow pay? long(top-decile) − short(bottom-decile) → to 15:29 ===")
    if not val.empty:
        print(val.to_string(index=False))
        print(f"  avg spread across decision points: {val.spread.mean():+.3f}%")
    for rd in ["UP-PUSH(long)", "DOWN-PUSH(short)", "DISTRIBUTION(fade)", "ABSORPTION(bounce?)"]:
        sub = f[f.read == rd]
        if len(sub):
            po = sub.paid_off.mean() * 100
            print(f"  {rd:<22} n={len(sub):>3}  avg fwd(to close) {sub.fwd_ret.mean():+.2f}%  paid-off {po:.0f}%")
    print("\n=== your 5 named ===")
    for s in sorted(NAMED):
        if s in f.index:
            r = f.loc[s]
            print(f"  {s:<12} read={r.read:<20} day {r.day_ret:+.2f}%  fwd(from 10:15) {r.fwd_ret:+.2f}%  paid={r.paid_off}")

    # ---- Excel ----
    out = ROOT / "docs" / "ops" / f"FLOW_EOD_{day}.xlsx"
    cols = ["read", "move", "imb", "vol", "oichg", "day_ret", "fwd_ret", "paid_off"]
    per_stock = f.reset_index()[["symbol"] + cols].sort_values("day_ret", ascending=False)
    try:
        with pd.ExcelWriter(out) as w:
            (val if not val.empty else pd.DataFrame([{"note": "insufficient data"}])).to_excel(w, "Ride-the-flow validation", index=False)
            per_stock.to_excel(w, "Per-stock flow", index=False)
        print(f"\n[*] WROTE {out}")
    except PermissionError:
        print("\n[!] report file open — close it to rewrite")


if __name__ == "__main__":
    main()
