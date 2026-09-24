"""Minute-level impulse study for two contrasting July-8 stocks: NAUKRI (failed morning
spike) vs JIOFIN (sustained distribution). Merges clean OHLCV (ohlc_1min) with order-flow
book imbalance (mkt_orderflow_1min, CASH). Labels impulses/fades/sweeps, computes the
behavioral features, and dumps a JSON series for charting."""
import sqlite3, json
from pathlib import Path
import numpy as np
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
DAY = "2026-07-08"


def pull(con, sym):
    px = con.execute("SELECT substr(bar_time,12,5) hm, open,high,low,close,volume FROM ohlc_1min "
                     "WHERE symbol=? AND bar_time BETWEEN ? AND ? ORDER BY bar_time",
                     (sym, DAY + " 09:15:00", DAY + " 15:29:00")).fetchall()
    of = {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT substr(bar_time,12,5) hm, total_buy_qty, total_sell_qty FROM mkt_orderflow_1min "
        "WHERE symbol=? AND segment='CASH' AND bar_time BETWEEN ? AND ? ORDER BY bar_time",
        (sym, DAY + " 09:15", DAY + " 15:30"))}
    hm = [r[0] for r in px]; o = px[0][1]
    close = np.array([r[4] for r in px], float); high = np.array([r[2] for r in px], float)
    low = np.array([r[3] for r in px], float); vol = np.array([r[5] for r in px], float)
    cumret = (close / o - 1) * 100
    hiret = (high / o - 1) * 100; loret = (low / o - 1) * 100
    relvol = vol / np.median(vol[vol > 0])
    buypct = np.array([(of.get(t, (np.nan, np.nan))[0] /
                        max(1, (of.get(t, (0, 0))[0] + of.get(t, (0, 0))[1]))) * 100
                       if t in of else np.nan for t in hm])
    return dict(sym=sym, hm=hm, o=o, close=close, cumret=cumret, hiret=hiret, loret=loret,
                vol=vol, relvol=relvol, buypct=buypct)


def analyse(d):
    cr = d["cumret"]; rv = d["relvol"]; hm = d["hm"]
    peak_i = int(np.argmax(cr)); trough_i = int(np.argmin(cr))
    # initial impulse = move in first 5 min
    init5 = cr[min(5, len(cr) - 1)]
    # time to +0.5/+1/+2 and -0.5/-1/-2
    def t_to(th, up=True):
        idx = np.where(cr >= th)[0] if up else np.where(cr <= th)[0]
        return hm[idx[0]] if len(idx) else "-"
    # volume-leads-price: correlation of relvol[t] with |cumret change| at t+1..t+3
    dcr = np.abs(np.diff(cr, prepend=cr[0]))
    lead = np.corrcoef(rv[:-3], dcr[3:])[0, 1] if len(rv) > 5 else np.nan
    # big-volume candle before the main move
    bigvol_i = int(np.argmax(rv))
    return dict(peak=(hm[peak_i], round(float(cr[peak_i]), 2)), trough=(hm[trough_i], round(float(cr[trough_i]), 2)),
                init5=round(float(init5), 2), close_ret=round(float(cr[-1]), 2),
                mae_before_close=round(float(cr[:].min()), 2), mfe=round(float(cr.max()), 2),
                t_up05=t_to(0.5), t_up1=t_to(1.0), t_up2=t_to(2.0),
                t_dn05=t_to(-0.5, False), t_dn1=t_to(-1.0, False), t_dn2=t_to(-2.0, False),
                vol_leads_price_corr=round(float(lead), 2), bigvol_bar=(hm[bigvol_i], round(float(rv[bigvol_i]), 1)),
                avg_buypct=round(float(np.nanmean(d["buypct"])), 1))


def main():
    con = sqlite3.connect(str(DB), timeout=60); con.execute("PRAGMA query_only=1")
    out = {}
    for sym in ["NAUKRI", "JIOFIN"]:
        d = pull(con, sym); a = analyse(d)
        out[sym] = dict(hm=d["hm"], cumret=[round(x, 3) for x in d["cumret"]],
                        hiret=[round(x, 3) for x in d["hiret"]], loret=[round(x, 3) for x in d["loret"]],
                        relvol=[round(x, 2) for x in d["relvol"]],
                        buypct=[None if np.isnan(x) else round(x, 1) for x in d["buypct"]], analysis=a)
        print(f"\n===== {sym}  ({DAY})  open {d['o']} -> close {round(float(d['close'][-1]),2)} ({a['close_ret']:+.2f}%) =====")
        print(f"  Initial impulse (first 5 min): {a['init5']:+.2f}%   |  intraday peak {a['peak'][1]:+.2f}% @ {a['peak'][0]}   trough {a['trough'][1]:+.2f}% @ {a['trough'][0]}")
        print(f"  Time to +0.5/+1/+2%: {a['t_up05']} / {a['t_up1']} / {a['t_up2']}    Time to -0.5/-1/-2%: {a['t_dn05']} / {a['t_dn1']} / {a['t_dn2']}")
        print(f"  Order-flow book buy%: avg {a['avg_buypct']}%  (>50 = buy-heavy book)   |  volume-leads-price corr: {a['vol_leads_price_corr']}")
        print(f"  Biggest-volume minute: {a['bigvol_bar'][0]} ({a['bigvol_bar'][1]}x median)")
    con.close()
    json.dump(out, open(ROOT / "docs" / "ops" / "_example_july8.json", "w"))
    print("\n[*] series -> docs/ops/_example_july8.json")


if __name__ == "__main__":
    main()
