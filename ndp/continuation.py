# -*- coding: utf-8 -*-
"""Continuation probability curve for one stock. Given it reached milestone X% (open->high), what is the
conditional probability it extends to the next milestones, how much it pulls back before continuing, and
how often it fails. Continuation = daily OHLC (full 2022-2026, exact since reaching +1% implies passing
+0.5%). Pullback = 1-minute path (2024-05+ only)."""
import os, sqlite3, sys
import numpy as np, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
UP = [0.5, 0.7, 1.0, 1.5, 2.0]; DN = [-0.5, -0.7, -1.0, -1.5, -2.0]


def continuation_daily(sym):
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    g = pd.read_sql_query("SELECT open,high,low FROM ohlc_daily WHERE symbol=? AND trade_date>='2022-01-01' "
                          "AND trade_date<='2026-12-31' ORDER BY trade_date", con, params=[sym]); con.close()
    o, h, l = g.open.values, g.high.values, g.low.values
    oh = (h/o-1)*100; ol = (l/o-1)*100
    def tbl(exc, miles, sign):
        rows = []
        for i, Mi in enumerate(miles):
            reached = (exc*sign >= abs(Mi)) if sign > 0 else (exc <= Mi)
            nreach = reached.sum()
            if nreach == 0: continue
            row = {"reached": f"{Mi:+.1f}%", "n": int(nreach), "P(reach)": round(nreach/len(exc)*100)}
            for Mj in miles[i+1:]:
                cont = (exc >= Mj) if sign > 0 else (exc <= Mj)
                row[f"->{Mj:+.1f}%"] = round(cont.sum()/nreach*100)
            nxt = miles[i+1] if i+1 < len(miles) else None
            if nxt is not None:
                contn = (exc >= nxt) if sign > 0 else (exc <= nxt)
                row["failed%"] = round((1-contn.sum()/nreach)*100)
            rows.append(row)
        return pd.DataFrame(rows)
    return tbl(oh, UP, +1), tbl(ol, DN, -1), len(o)


def pullback_1min(sym):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low FROM ohlc_1min WHERE symbol=? ORDER BY bar_time",
                          con, params=[sym]); con.close()
    if b.empty: return None, None
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]
    b = b[(b.hm >= "09:15") & (b.hm <= "15:30")]
    def side(miles, sign):
        acc = {round(m, 1): [] for m in miles[:-1]}
        for d, g in b.groupby("date"):
            g = g.sort_values("bar_time"); o = g.open.iloc[0]
            exc_h = (g.high.values/o-1)*100; exc_l = (g.low.values/o-1)*100
            for i in range(len(miles)-1):
                Mi, Mj = miles[i], miles[i+1]
                if sign > 0:
                    ti = np.argmax(exc_h >= Mi) if (exc_h >= Mi).any() else -1
                    tj = np.argmax(exc_h >= Mj) if (exc_h >= Mj).any() else -1
                    if ti == -1 or tj == -1 or tj <= ti: continue
                    dip = exc_l[ti:tj+1].min(); pb = Mi - dip                     # how far below Mi it dipped
                else:
                    ti = np.argmax(exc_l <= Mi) if (exc_l <= Mi).any() else -1
                    tj = np.argmax(exc_l <= Mj) if (exc_l <= Mj).any() else -1
                    if ti == -1 or tj == -1 or tj <= ti: continue
                    rip = exc_h[ti:tj+1].max(); pb = rip - Mi
                if pb > 0: acc[round(Mi, 1)].append(pb)
        return {k: (round(np.mean(v), 3), len(v)) for k, v in acc.items() if v}
    return side(UP, +1), side(DN, -1)


def run(sym):
    up, dn, ndays = continuation_daily(sym)
    print("="*74); print(f"CONTINUATION CURVE — {sym}  (2022-2026, {ndays} days)  ·  once it reaches X%, what next?"); print("="*74)
    print("\nUPSIDE — of days that reached each milestone (open->high), % that extended further:")
    print(up.to_string(index=False))
    print("\nDOWNSIDE — of days that reached each milestone (open->low), % that extended further:")
    print(dn.to_string(index=False))
    pu, pd_ = pullback_1min(sym)
    print("\n" + "-"*74)
    print("AVERAGE PULLBACK before continuing (1-minute path, 2024-05+ only) — trailing-stop breathing room:")
    if pu:
        print("  UP  : " + "   ".join(f"after {k:+.1f}% -> pullback {v[0]:.2f}% (n={v[1]})" for k, v in pu.items()))
        print("  DOWN: " + "   ".join(f"after {k:+.1f}% -> rip {v[0]:.2f}% (n={v[1]})" for k, v in pd_.items()))
    out = os.path.expanduser("~") + f"/Downloads/CONTINUATION_{sym}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        up.to_excel(w, "upside_continuation", index=False); dn.to_excel(w, "downside_continuation", index=False)
    print(f"\nsaved -> {out}")


if __name__ == "__main__":
    run(sys.argv[1].upper() if len(sys.argv) > 1 else "ICICIBANK")
