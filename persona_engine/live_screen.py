"""
LIVE daily morning screen. Run after 10:00 IST on a trading day.

Fetches today's 9:15->10:00 1-min bars from Kite for the F&O universe, ranks by morning
volatility, and prints the top-5 with the 10:00 price + breakout/target/stop levels for
an intraday volatility-breakout trade (exit by 15:15, no carryover).

Usage:  python -m persona_engine.live_screen [YYYY-MM-DD]   (default: today IST)
Saves to table live_intraday_screen + a CSV.
"""
from __future__ import annotations
import datetime as dt
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from persona_engine import db, universe

NPICK = 5
BRK, TGT, STOP = 0.25, 1.0, 0.5
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")

SCHEMA = """CREATE TABLE IF NOT EXISTS live_intraday_screen (
    screen_date TEXT, rank INTEGER, symbol TEXT, morning_vol REAL, px_1000 REAL,
    long_trigger REAL, short_trigger REAL, target_up REAL, target_dn REAL,
    stop_long REAL, stop_short REAL, created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY(screen_date, symbol))"""


def fetch_morning(k, tok, symbols, date):
    frm = dt.datetime.combine(date, dt.time(9, 15))
    to = dt.datetime.combine(date, dt.time(10, 0))
    out = []
    for s in symbols:
        if s not in tok:
            continue
        try:
            bars = k.historical_data(tok[s], frm, to, "minute")
        except Exception:
            continue
        if len(bars) < 10:
            continue
        c = pd.Series([b["close"] for b in bars])
        vol = c.pct_change().std()*100
        px1000 = bars[-1]["close"]
        out.append((s, vol, px1000))
    return pd.DataFrame(out, columns=["symbol", "m_volat", "px_1000"])


def run(date=None):
    con = db.connect()
    con.execute(SCHEMA)
    fo = universe.get_fo_universe(con)
    d = date or dt.datetime.now(IST).date()
    k = universe.get_kite()
    inst = k.instruments("NSE")
    tok = {r["tradingsymbol"]: r["instrument_token"]
           for r in inst if r.get("segment") == "NSE" and r.get("instrument_type") == "EQ"}
    df = fetch_morning(k, tok, fo, d)
    if df.empty:
        print(f"No morning data for {d} (market not open yet / holiday?).")
        con.close(); return
    df = df.sort_values("m_volat", ascending=False).head(NPICK).reset_index(drop=True)
    df["rank"] = df.index + 1
    df["long_trigger"] = (df["px_1000"]*(1+BRK/100)).round(2)
    df["short_trigger"] = (df["px_1000"]*(1-BRK/100)).round(2)
    df["target_up"] = (df["px_1000"]*(1+TGT/100)).round(2)
    df["target_dn"] = (df["px_1000"]*(1-TGT/100)).round(2)
    df["stop_long"] = (df["long_trigger"]*(1-STOP/100)).round(2)
    df["stop_short"] = (df["short_trigger"]*(1+STOP/100)).round(2)
    df["px_1000"] = df["px_1000"].round(2); df["m_volat"] = df["m_volat"].round(3)

    rows = [(str(d), int(r["rank"]), r["symbol"], r["m_volat"], r["px_1000"],
             r["long_trigger"], r["short_trigger"], r["target_up"], r["target_dn"],
             r["stop_long"], r["stop_short"]) for _, r in df.iterrows()]
    con.executemany("INSERT OR REPLACE INTO live_intraday_screen(screen_date,rank,symbol,"
                    "morning_vol,px_1000,long_trigger,short_trigger,target_up,target_dn,"
                    "stop_long,stop_short) VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT/f"live_screen_{d}.csv", index=False)

    print(f"\n==== LIVE INTRADAY SCREEN — {d} (enter ~10:00, exit by 15:15, no carryover) ====")
    print(f"{'#':2s} {'SYMBOL':12s} {'10:00':>9s} {'LongTrig':>9s} {'ShortTrig':>9s} "
          f"{'Tgt+1%':>9s} {'Tgt-1%':>9s} {'StopL':>9s} {'StopS':>9s}")
    for _, r in df.iterrows():
        print(f"{int(r['rank']):<2d} {r['symbol']:12s} {r['px_1000']:>9.2f} {r['long_trigger']:>9.2f} "
              f"{r['short_trigger']:>9.2f} {r['target_up']:>9.2f} {r['target_dn']:>9.2f} "
              f"{r['stop_long']:>9.2f} {r['stop_short']:>9.2f}")
    print("\nRule: whichever trigger hits first -> ride to the +/-1% target; hard stop as shown; "
          "force-exit at 15:15. Saved -> live_intraday_screen + CSV.")
    con.close()


if __name__ == "__main__":
    d = dt.date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
    print("LIVE_DONE")
