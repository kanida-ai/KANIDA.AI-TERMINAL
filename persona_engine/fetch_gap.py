"""
Backfill the recent gap from Kite: daily (2026-05-08+) and 1-min (2026-05-12+) up to
today, for the F&O universe. Inserts into ohlc_daily / ohlc_1min (INSERT OR IGNORE).
"""
from __future__ import annotations
import datetime as dt
import time
from persona_engine import db, universe

DAILY_FROM = dt.date(2026, 5, 8)
MIN_FROM = dt.date(2026, 5, 12)
TO = dt.date(2026, 6, 23)


def run():
    con = db.connect()
    fo = universe.get_fo_universe(con)              # cached F&O list
    k = universe.get_kite()
    inst = k.instruments("NSE")
    tok = {r["tradingsymbol"]: r["instrument_token"]
           for r in inst if r.get("segment") == "NSE" and r.get("instrument_type") == "EQ"}
    have = [s for s in fo if s in tok]
    miss = [s for s in fo if s not in tok]
    print(f"F&O symbols: {len(fo)} | matched tokens: {len(have)} | unmatched: {len(miss)}", flush=True)
    if miss:
        print("  unmatched (skipped):", miss[:20])

    nd = nm = 0
    for i, s in enumerate(have):
        t = tok[s]
        # daily
        for attempt in range(2):
            try:
                d = k.historical_data(t, DAILY_FROM, TO, "day")
                rows = [(s, str(c["date"])[:10], c["open"], c["high"], c["low"],
                         c["close"], int(c["volume"]), "ok") for c in d]
                con.executemany("INSERT OR IGNORE INTO ohlc_daily"
                                "(symbol,trade_date,open,high,low,close,volume,quality_flag) "
                                "VALUES (?,?,?,?,?,?,?,?)", rows)
                nd += len(rows); break
            except Exception as e:
                if attempt == 1: print(f"  {s} daily ERR {e}", flush=True)
                time.sleep(1)
        # minute
        for attempt in range(2):
            try:
                m = k.historical_data(t, MIN_FROM, TO, "minute")
                rows = [(s, str(c["date"])[:19], c["open"], c["high"], c["low"],
                         c["close"], int(c["volume"])) for c in m]
                con.executemany("INSERT OR IGNORE INTO ohlc_1min"
                                "(symbol,bar_time,open,high,low,close,volume) "
                                "VALUES (?,?,?,?,?,?,?)", rows)
                nm += len(rows); break
            except Exception as e:
                if attempt == 1: print(f"  {s} min ERR {e}", flush=True)
                time.sleep(1)
        con.commit()
        if i % 25 == 0:
            print(f"  {i+1}/{len(have)} {s}: daily+={nd} min+={nm}", flush=True)
        time.sleep(0.3)   # rate limit

    print(f"\nDONE inserted daily rows={nd} minute rows={nm}")
    print("ohlc_daily max:", con.execute("SELECT MAX(trade_date) FROM ohlc_daily WHERE symbol='RELIANCE'").fetchone()[0])
    print("ohlc_1min max:", con.execute("SELECT MAX(bar_time) FROM ohlc_1min WHERE symbol='RELIANCE'").fetchone()[0])
    print("new daily trading dates:", [r[0] for r in con.execute(
        "SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date>='2026-05-08' ORDER BY trade_date")])
    con.close()


if __name__ == "__main__":
    run()
    print("FETCH_DONE")
