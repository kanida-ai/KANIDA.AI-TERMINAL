"""
Backfill daily / 5-min / 1-min OHLC+volume from 2020-01-01 up to each symbol's CURRENT
earliest stored bar (i.e. fill the pre-2022 gap only). Idempotent (INSERT OR IGNORE) and
resumable — the fetch window shrinks to what's still missing on each run. Stocks listed
after 2020 simply return nothing before their listing.

Run: PYTHONIOENCODING=utf-8 python backfill_2020.py
"""
import os, sys, sqlite3, time
from datetime import datetime, date, timedelta
from pathlib import Path

ENG = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ENG / "universe_engine")); sys.path.insert(0, str(ENG / "backend"))
from engine.data_fetch import get_kite, get_latest_access_token, RateLimiter  # noqa
from kiteconnect import KiteConnect  # noqa

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
START = date(2020, 1, 1)
PLAN = [("day", "ohlc_daily", 2000), ("5minute", "ohlc_5min", 100), ("minute", "ohlc_1min", 60)]


def log(m): print(f"{datetime.now():%H:%M:%S} {m}", flush=True)


def fetch_chunk(kite, token, cs, ce, interval, rl, retries=5):
    delay = 1.0; last = None
    for _ in range(retries):
        rl.wait()
        try:
            return kite.historical_data(token, datetime.combine(cs, datetime.min.time()),
                                        datetime.combine(ce, datetime.max.time()), interval)
        except Exception as e:
            msg = str(e)
            if "TokenException" in msg or "access_token" in msg.lower() or "api_key" in msg.lower():
                raise RuntimeError("AUTH:" + msg)
            last = msg; time.sleep(delay); delay = min(delay * 2, 16)
    log(f"    chunk {cs}..{ce} failed: {last}"); return []


def chunks(s, e, maxd):
    cur = s
    while cur <= e:
        ce = min(cur + timedelta(days=maxd - 1), e); yield cur, ce; cur = ce + timedelta(days=1)


def main():
    kite = get_kite()
    log(f"token OK: {kite.profile().get('user_name')}")
    nse = kite.instruments("NSE")
    eq = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "NSE"}
    idx = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "INDICES"}
    con = sqlite3.connect(str(DB), timeout=90)
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_daily ORDER BY symbol").fetchall()]
    tokmap = {}
    for s in syms:
        t = eq.get(s) or idx.get(s)
        if t: tokmap[s] = t
    log(f"symbols in DB: {len(syms)} | resolved tokens: {len(tokmap)} | backfilling {START} -> existing-earliest")
    rl = RateLimiter(rps=3.0)
    for interval, table, maxd in PLAN:
        log(f"=== {table} ({interval}) ===")
        added_tot = 0
        for n, s in enumerate(sorted(tokmap), 1):
            token = tokmap[s]
            row = con.execute(f"SELECT min(bar_time) FROM {table} WHERE symbol=?", (s,)).fetchone()[0]
            end = (date.fromisoformat(row[:10]) - timedelta(days=1)) if row else date.today()
            if START > end:
                continue                      # already have 2020+ (or earlier)
            for cs, ce in chunks(START, end, maxd):
                try:
                    bars = fetch_chunk(kite, token, cs, ce, interval, rl)
                except RuntimeError as e:
                    log(f"AUTH ERROR, stopping: {e}"); con.close(); return
                if not bars:
                    continue
                payload = [(s, token, b["date"].strftime("%Y-%m-%d %H:%M:%S"),
                            float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
                            int(b["volume"])) for b in bars]
                con.executemany(
                    f"INSERT OR IGNORE INTO {table} (symbol,instrument_token,bar_time,open,high,low,close,volume) "
                    f"VALUES (?,?,?,?,?,?,?,?)", payload)
                con.commit(); added_tot += con.total_changes
            if n % 25 == 0:
                log(f"  {table}: {n}/{len(tokmap)} (last {s})")
        r = con.execute(f"SELECT min(bar_time), max(bar_time), count(*) FROM {table}").fetchone()
        log(f"  {table} DONE: range {r[0]} .. {r[1]} | rows {r[2]:,}")
    con.close()
    log("BACKFILL COMPLETE")


if __name__ == "__main__":
    main()
