"""
Free enrichment for kanida.db (run AFTER the backfill + ATP so writes never collide):
  1. Kite instrument METADATA -> instrument_labels: company_name, lot_size, tick_size
     (from db/kite_meta_cache.json; Kite's dump is the only free extra it offers).
  2. EVENT MARKERS -> event_markers table: abnormal-move / volume-spike days derived from
     our own daily OHLC. NOTE: Kite prices are split/bonus-ADJUSTED, so splits/bonuses are
     invisible (no gap). These markers flag likely results/news/ex-dividend days — they are
     NOT labeled corporate actions (that needs a non-Kite feed).

Idempotent; uses a long busy_timeout so it politely waits if anything else is mid-write.
Run: PYTHONIOENCODING=utf-8 python enrich.py
"""
import json, sqlite3
from datetime import datetime
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
CACHE = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kite_meta_cache.json")


def log(m): print(f"{datetime.now():%H:%M:%S} {m}", flush=True)


def conn():
    c = sqlite3.connect(str(DB), timeout=180)
    c.execute("PRAGMA busy_timeout=180000"); c.execute("PRAGMA journal_mode=WAL")
    return c


def enrich_metadata():
    meta = json.load(open(CACHE))
    c = conn()
    cols = [r[1] for r in c.execute("PRAGMA table_info(instrument_labels)").fetchall()]
    for col, typ in [("company_name", "TEXT"), ("lot_size", "INTEGER"), ("tick_size", "REAL")]:
        if col not in cols:
            c.execute(f"ALTER TABLE instrument_labels ADD COLUMN {col} {typ}")
    c.commit()
    syms = [r[0] for r in c.execute("SELECT symbol FROM instrument_labels").fetchall()]
    upd = []
    for s in syms:
        m = meta.get(s)
        if m:
            upd.append((m.get("name"), m.get("lot_size"), m.get("tick_size"), s))
    c.executemany("UPDATE instrument_labels SET company_name=?, lot_size=?, tick_size=? WHERE symbol=?", upd)
    c.commit()
    n = c.execute("SELECT count(*) FROM instrument_labels WHERE company_name IS NOT NULL AND company_name!=''").fetchone()[0]
    log(f"metadata: enriched {len(upd)} symbols · {n} now have company_name/lot/tick")
    c.close()


def event_markers():
    c = conn()
    c.execute("""CREATE TABLE IF NOT EXISTS event_markers (
        symbol TEXT, event_date TEXT, day_ret_pct REAL, gap_pct REAL, vol_ratio REAL,
        atr20_pct REAL, flags TEXT, PRIMARY KEY (symbol, event_date))""")
    c.commit()
    d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily ORDER BY symbol,bar_time", c)
    d["date"] = d["bar_time"].str[:10]
    added = 0
    for s, g in d.groupby("symbol", sort=False):
        g = g.reset_index(drop=True)
        pc = g["close"].shift(1)
        day_ret = (g["close"] / pc - 1) * 100
        gap = (g["open"] / pc - 1) * 100
        tr = np.maximum(g["high"] - g["low"], np.maximum((g["high"] - pc).abs(), (g["low"] - pc).abs()))
        atr = (tr.rolling(20).mean() / g["close"] * 100)
        volr = g["volume"] / g["volume"].rolling(20).mean()
        rows = []
        for i in range(len(g)):
            dr, gp, ar, vr = day_ret.iloc[i], gap.iloc[i], atr.iloc[i], volr.iloc[i]
            if pd.isna(dr) or pd.isna(ar):
                continue
            flags = []
            if abs(dr) >= max(8.0, 3 * ar): flags.append("big_move")
            if abs(gp) >= max(5.0, 2.5 * ar): flags.append("gap")
            if vr >= 4: flags.append("vol_spike")
            if flags:
                rows.append((s, g["date"].iloc[i], round(float(dr), 2), round(float(gp), 2),
                             round(float(vr), 2) if pd.notna(vr) else None, round(float(ar), 2), ",".join(flags)))
        if rows:
            c.executemany("INSERT OR IGNORE INTO event_markers VALUES (?,?,?,?,?,?,?)", rows)
            c.commit(); added += len(rows)
    tot = c.execute("SELECT count(*) FROM event_markers").fetchone()[0]
    log(f"event_markers: {added} flagged this run · {tot} total (big_move / gap / vol_spike days)")
    c.close()


if __name__ == "__main__":
    log("enrich start")
    enrich_metadata()
    event_markers()
    log("ENRICH COMPLETE")
