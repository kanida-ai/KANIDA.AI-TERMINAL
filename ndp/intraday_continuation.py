# -*- coding: utf-8 -*-
"""Intraday CONDITIONAL continuation analysis for one stock (1-min, 2024-05+). Once price reaches a
milestone from the open (enter in that direction), analyse the PATH AFTER entry: how much additional move
was available (MFE from entry), the max pullback before continuing (MAE before the extreme), how often it
continued to further milestones, and how often it reversed. This sets targets / trailing stops / exits."""
import os, sqlite3, sys
import numpy as np, pandas as pd
MDB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "universe_engine", "data", "db", "kanida_universe.db")
UP = [0.5, 0.7, 1.0, 1.5]; DN = [-0.5, -0.7, -1.0, -1.5]


def analyse(sym):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]
    b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    sess = [(d, g.sort_values("bar_time")) for d, g in b.groupby("date")]
    sess = [(d, g) for d, g in sess if len(g) >= 60]

    def side(miles, sign):
        rows = []
        for M in miles[:-1]:                                       # entry milestones (not the last, need a 'next')
            recs = []
            for d, g in sess:
                o = g.open.iloc[0]; H = g.high.values; L = g.low.values
                lvl = o*(1+M/100)
                if sign > 0:
                    hit = np.where(H >= lvl)[0]
                else:
                    hit = np.where(L <= lvl)[0]
                if len(hit) == 0: continue
                t = hit[0]; entry = lvl
                Ha = H[t:]; La = L[t:]
                if sign > 0:                                        # LONG
                    peak_i = int(np.argmax(Ha)); mfe = (Ha.max()/entry-1)*100         # additional upside after entry
                    pull = (entry - La[:peak_i+1].min())/entry*100 if peak_i > 0 else max(0.0, (entry-La[0])/entry*100)
                    reached = {m: (Ha.max() >= o*(1+m/100)) for m in miles if m > M}
                    reversed_ = La.min() <= o                       # fell back to open
                else:                                               # SHORT
                    trough_i = int(np.argmin(La)); mfe = (1-La.min()/entry)*100        # additional downside after entry
                    pull = (Ha[:trough_i+1].max()-entry)/entry*100 if trough_i > 0 else max(0.0, (Ha[0]-entry)/entry*100)
                    reached = {m: (La.min() <= o*(1+m/100)) for m in miles if m < M}
                    reversed_ = Ha.max() >= o                       # bounced back to open
                recs.append(dict(mfe=mfe, pull=pull, reversed=reversed_, **{f"c{m}": reached[m] for m in reached}))
            df = pd.DataFrame(recs)
            if df.empty: continue
            row = {"entry@": f"{M:+.1f}%", "n": len(df)}
            for m in [x for x in miles if (x > M if sign > 0 else x < M)]:
                row[f"cont_{m:+.1f}%"] = round(df[f"c{m}"].mean()*100)
            row["addl_move"] = round(df.mfe.mean(), 2)             # avg additional favourable move after entry
            row["max_pullbk"] = round(df.pull.mean(), 2)          # avg max pullback before continuing
            row["reversed%"] = round(df["reversed"].mean()*100)   # fell back to open
            rows.append(row)
        return pd.DataFrame(rows)
    return side(UP, +1), side(DN, -1), len(sess)


def run(sym):
    up, dn, n = analyse(sym)
    print("="*82); print(f"INTRADAY CONDITIONAL CONTINUATION — {sym}  (1-min, {n} sessions 2024-05->2026-07)"); print("="*82)
    print("\nUPSIDE — reach +X% from open, enter LONG, then what? (all measured AFTER entry, from the entry level)")
    print(up.to_string(index=False))
    print("\nDOWNSIDE — reach -X% from open, enter SHORT, then what?")
    print(dn.to_string(index=False))
    print("\n  addl_move  = avg ADDITIONAL favourable move available after entry (MFE from entry, %)")
    print("  max_pullbk = avg max pullback/adverse move BEFORE the extreme (sets the trailing-stop room, %)")
    print("  reversed%  = share that fell all the way back to the open")
    out = os.path.expanduser("~")+f"/Downloads/INTRADAY_CONT_{sym}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        up.to_excel(w, "upside", index=False); dn.to_excel(w, "downside", index=False)
    print(f"\nsaved -> {out}")


if __name__ == "__main__":
    run(sys.argv[1].upper() if len(sys.argv) > 1 else "ICICIBANK")
