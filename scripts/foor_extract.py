"""Phase 1 — opening-range feature panel on DEEP EQUITY 1-min (F&O stocks' cash).
Per (symbol, date): opening-window aggregates at checkpoints 09:18 / 09:20 / 09:22, the
rest-of-day target, and post-checkpoint MFE/MAE. Cached to _foor_panel.pkl. Walk-forward
baselines + signal derivation happen in the analysis step (no lookahead there).
"""
import sqlite3, pickle
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "docs" / "ops" / "_foor_panel.pkl"
WIN_LO, WIN_HI = "2024-05-13 00:00:00", "2026-07-04 00:00:00"

Q = """
SELECT substr(bar_time,1,10) date,
  max(CASE WHEN substr(bar_time,12,5)='09:15' THEN open END) o0915,
  -- checkpoint 09:18 (3-min window 09:15-09:17)
  max(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:17' THEN high END) H18,
  min(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:17' THEN low  END) L18,
  sum(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:17' THEN volume END) V18,
  max(CASE WHEN substr(bar_time,12,5)='09:17' THEN close END) C18,
  -- checkpoint 09:20 (5-min window 09:15-09:19)
  max(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:19' THEN high END) H20,
  min(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:19' THEN low  END) L20,
  sum(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:19' THEN volume END) V20,
  max(CASE WHEN substr(bar_time,12,5)='09:19' THEN close END) C20,
  -- checkpoint 09:22 (7-min window 09:15-09:21)
  max(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:21' THEN high END) H22,
  min(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:21' THEN low  END) L22,
  sum(CASE WHEN substr(bar_time,12,5) BETWEEN '09:15' AND '09:21' THEN volume END) V22,
  max(CASE WHEN substr(bar_time,12,5)='09:21' THEN close END) C22,
  -- rest-of-day + MFE/MAE from the 09:20 checkpoint (primary)
  max(CASE WHEN substr(bar_time,12,5)='15:29' THEN close END) eod,
  max(CASE WHEN substr(bar_time,12,5) BETWEEN '09:20' AND '15:29' THEN high END) postH,
  min(CASE WHEN substr(bar_time,12,5) BETWEEN '09:20' AND '15:29' THEN low  END) postL,
  max(high) fullH, min(low) fullL, sum(volume) dayV, count(*) nbar
FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<? GROUP BY date
"""


def main():
    con = sqlite3.connect(str(DB))
    onemin = set(x[0] for x in con.execute("SELECT DISTINCT symbol FROM ohlc_1min"))
    fo = [x[0] for x in con.execute("SELECT symbol FROM fo_stock_master WHERE fo_eligible=1").fetchall()] \
        or [x[0] for x in con.execute("SELECT symbol FROM fo_stock_master").fetchall()]
    ALIAS = {"ZOMATO": "ETERNAL"}
    syms = sorted({ALIAS.get(s, s) for s in fo} & onemin)
    print(f"[foor] extracting opening panel for {len(syms)} F&O stocks (cash 1-min)...", flush=True)
    parts = []
    for k, s in enumerate(syms, 1):
        g = pd.read_sql_query(Q, con, params=(s, WIN_LO, WIN_HI))
        g = g[g.nbar >= 300]                      # full sessions only
        if len(g):
            g.insert(0, "symbol", s); parts.append(g)
        if k % 50 == 0:
            print(f"  [{k}/{len(syms)}] {s} rows-so-far~{sum(len(p) for p in parts):,}", flush=True)
    con.close()
    panel = pd.concat(parts, ignore_index=True)
    pickle.dump(panel, open(OUT, "wb"), protocol=4)
    print(f"[foor] WROTE {OUT}  {len(panel):,} stock-days | {panel.symbol.nunique()} stocks | "
          f"{panel.date.min()}..{panel.date.max()}  ({OUT.stat().st_size/1e6:.0f} MB)")


if __name__ == "__main__":
    main()
