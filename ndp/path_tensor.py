# -*- coding: utf-8 -*-
"""Path tensor (NDP spec 5.5) for one stock. Precompute per (symbol, entry_date, entry_time, horizon):
entry VWAP, exit price, MFE/MAE long+short, touch times, gap, first-minute range (slippage proxy).
INTRADAY horizon from 1-min. Entry fill = VWAP of first 1-2 min of the entry window (no naked open print).
Writes ndp.db::path_tensor. Read-only on the 65GB source DB.
"""
import os, sqlite3, argparse
import numpy as np, pandas as pd
SRC = "universe_engine/data/db/kanida_universe.db"
NDB = "ndp/ndp.db"
ENTRY_TIMES = ["09:15", "09:20", "09:30", "09:45", "10:00"]

DDL = """
CREATE TABLE IF NOT EXISTS path_tensor (
  symbol TEXT NOT NULL, signal_date TEXT NOT NULL, entry_date TEXT NOT NULL,
  entry_time TEXT NOT NULL, horizon TEXT NOT NULL,
  entry_price REAL NOT NULL, exit_price REAL NOT NULL, exit_ts TEXT NOT NULL,
  mfe_long REAL NOT NULL, mae_long REAL NOT NULL, mfe_short REAL NOT NULL, mae_short REAL NOT NULL,
  time_to_mfe_mins INTEGER, time_to_mae_mins INTEGER,
  touch_005_ts TEXT, touch_010_ts TEXT,
  gap_open_pct REAL NOT NULL, first_min_range_pct REAL NOT NULL,
  data_completeness REAL NOT NULL,
  PRIMARY KEY (symbol, signal_date, entry_time, horizon) );
CREATE INDEX IF NOT EXISTS ix_pt ON path_tensor(symbol, entry_date);
"""

def build(sym):
    con = sqlite3.connect("file:" + SRC + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]
    b = b[(b.hm >= "09:15") & (b.hm <= "15:30")].reset_index(drop=True)
    sessions = sorted(b.date.unique())
    prev_close = {}
    out = []
    for i, d in enumerate(sessions):
        g = b[b.date == d].sort_values("bar_time").reset_index(drop=True)
        if len(g) < 60: continue
        pc = prev_close.get("last")
        expected = 375  # 09:15-15:29
        completeness = min(1.0, len(g) / expected)
        o0915 = g.iloc[0].open
        gap = (o0915 / pc - 1) * 100 if pc else 0.0
        for et in ENTRY_TIMES:
            gi = g[g.hm >= et]
            if len(gi) < 5: continue
            e2 = gi.iloc[:2]
            entry = (e2.close * e2.volume).sum() / max(e2.volume.sum(), 1)
            if entry <= 0: entry = gi.iloc[0].open
            fmr = (gi.iloc[0].high - gi.iloc[0].low) / entry * 100
            path = gi.iloc[1:]                       # after entry window
            if len(path) < 2: continue
            hi_i = path.high.values; lo_i = path.low.values; ts = path.bar_time.values
            mfe_l = (hi_i.max() / entry - 1) * 100; mae_l = (lo_i.min() / entry - 1) * 100
            mfe_s = -(lo_i.min() / entry - 1) * 100; mae_s = -(hi_i.max() / entry - 1) * 100
            t_mfe = int(np.argmax(hi_i)); t_mae = int(np.argmin(lo_i))
            up005 = np.where(hi_i / entry - 1 >= 0.005)[0]; up010 = np.where(hi_i / entry - 1 >= 0.010)[0]
            t005 = ts[up005[0]] if len(up005) else None; t010 = ts[up010[0]] if len(up010) else None
            exitp = g.iloc[-1].close; exitts = g.iloc[-1].bar_time
            out.append((sym, d, d, et, "INTRADAY", float(entry), float(exitp), exitts,
                        float(mfe_l), float(mae_l), float(mfe_s), float(mae_s),
                        t_mfe, t_mae, t005, t010, float(gap), float(fmr), float(completeness)))
        prev_close["last"] = g.iloc[-1].close
    os.makedirs("ndp", exist_ok=True)
    db = sqlite3.connect(NDB); db.executescript(DDL)
    db.execute("DELETE FROM path_tensor WHERE symbol=?", [sym])
    db.executemany("INSERT OR REPLACE INTO path_tensor VALUES (" + ",".join(["?"]*19) + ")", out)
    db.commit()
    n = db.execute("SELECT COUNT(*) FROM path_tensor WHERE symbol=?", [sym]).fetchone()[0]
    nd = db.execute("SELECT COUNT(DISTINCT signal_date) FROM path_tensor WHERE symbol=?", [sym]).fetchone()[0]
    db.close()
    print(f"path_tensor built: {sym}  rows={n:,}  sessions={nd}  entry_times={len(ENTRY_TIMES)}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("symbol", nargs="?", default="ICICIBANK")
    build(ap.parse_args().symbol.upper())
