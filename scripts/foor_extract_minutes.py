"""Phase 1.5 Stage B data — the individual OPENING MINUTE BARS (09:15-09:20) per stock-day,
so the Layer-2 footprint can detect sweep -> absorption (volume at the extreme) -> immediate
reaction (reversal vs continuation), minute by minute. Deep equity 1-min, F&O universe.
"""
import sqlite3, pickle
from pathlib import Path
import pandas as pd

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "docs" / "ops" / "_foor_minutes.pkl"
WIN_LO, WIN_HI = "2024-05-13 00:00:00", "2026-07-04 00:00:00"
BARS = ["09:15", "09:16", "09:17", "09:18", "09:19", "09:20"]

_cols = []
for b in BARS:
    t = b.replace(":", "")
    _cols.append(f"max(CASE WHEN substr(bar_time,12,5)='{b}' THEN open   END) o{t}")
    _cols.append(f"max(CASE WHEN substr(bar_time,12,5)='{b}' THEN high   END) h{t}")
    _cols.append(f"max(CASE WHEN substr(bar_time,12,5)='{b}' THEN low    END) l{t}")
    _cols.append(f"max(CASE WHEN substr(bar_time,12,5)='{b}' THEN close  END) c{t}")
    _cols.append(f"max(CASE WHEN substr(bar_time,12,5)='{b}' THEN volume END) v{t}")
Q = ("SELECT substr(bar_time,1,10) date, " + ", ".join(_cols) + ", "
     "max(CASE WHEN substr(bar_time,12,5)='15:29' THEN close END) eod, "
     "max(CASE WHEN substr(bar_time,12,5) BETWEEN '09:20' AND '15:29' THEN high END) postH, "
     "min(CASE WHEN substr(bar_time,12,5) BETWEEN '09:20' AND '15:29' THEN low  END) postL, "
     "max(high) fullH, min(low) fullL, count(*) nbar "
     "FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<? GROUP BY date")


def main():
    con = sqlite3.connect(str(DB))
    onemin = set(x[0] for x in con.execute("SELECT DISTINCT symbol FROM ohlc_1min"))
    fo = [x[0] for x in con.execute("SELECT symbol FROM fo_stock_master WHERE fo_eligible=1").fetchall()] \
        or [x[0] for x in con.execute("SELECT symbol FROM fo_stock_master").fetchall()]
    ALIAS = {"ZOMATO": "ETERNAL"}
    syms = sorted({ALIAS.get(s, s) for s in fo} & onemin)
    print(f"[min] extracting opening minute bars for {len(syms)} F&O stocks...", flush=True)
    parts = []
    for k, s in enumerate(syms, 1):
        g = pd.read_sql_query(Q, con, params=(s, WIN_LO, WIN_HI))
        g = g[g.nbar >= 300]
        if len(g):
            g.insert(0, "symbol", s); parts.append(g)
        if k % 50 == 0:
            print(f"  [{k}/{len(syms)}] {s}", flush=True)
    con.close()
    panel = pd.concat(parts, ignore_index=True)
    pickle.dump(panel, open(OUT, "wb"), protocol=4)
    print(f"[min] WROTE {OUT}  {len(panel):,} stock-days | {panel.symbol.nunique()} stocks | "
          f"{panel.date.min()}..{panel.date.max()}  ({OUT.stat().st_size/1e6:.0f} MB)")


if __name__ == "__main__":
    main()
