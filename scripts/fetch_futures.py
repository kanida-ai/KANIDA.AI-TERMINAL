"""Fetch daily / 5-min / 1-min OHLC + Volume + OI for every ACTIVE NSE stock-futures
contract into kanida.db (ohlc_futures_daily / _5min / _1min). Kite exposes only
currently-active contracts (each with data from its introduction ~3 months before
expiry), so this yields the trailing ~3 months of REAL per-contract futures. Index
futures excluded (stocks only). Idempotent + retry; oi=True.
"""
import os, sys, sqlite3, time
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

ENG = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ENG / "universe_engine")); sys.path.insert(0, str(ENG / "backend"))
from engine.data_fetch import get_kite, get_latest_access_token, RateLimiter  # noqa
from kiteconnect import KiteConnect  # noqa

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
START, END = date(2022, 1, 1), date.today()
IDX = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYIT", "BANKEX", "SENSEX"}
PLAN = [("day", "ohlc_futures_daily", 2000), ("5minute", "ohlc_futures_5min", 100),
        ("minute", "ohlc_futures_1min", 60)]

SCHEMA = """
CREATE TABLE IF NOT EXISTS {t} (
    symbol           TEXT NOT NULL,   -- underlying (RELIANCE)
    tradingsymbol    TEXT NOT NULL,   -- contract (RELIANCE26AUGFUT)
    expiry           TEXT NOT NULL,
    instrument_token INTEGER,
    bar_time         TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL,
    volume INTEGER, oi INTEGER,
    PRIMARY KEY (tradingsymbol, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_{t}_sym_dt ON {t}(symbol, bar_time);
CREATE INDEX IF NOT EXISTS idx_{t}_ts_dt  ON {t}(tradingsymbol, bar_time);
"""


def fetch_chunk(kite, token, cs, ce, interval, rl, retries=5):
    delay = 1.0; last = None
    for _ in range(retries):
        rl.wait()
        try:
            return kite.historical_data(token, datetime.combine(cs, datetime.min.time()),
                                        datetime.combine(ce, datetime.max.time()), interval, oi=True)
        except Exception as e:
            msg = str(e)
            if "TokenException" in msg or "access_token" in msg.lower():
                raise RuntimeError("AUTH:" + msg)
            last = msg; time.sleep(delay); delay = min(delay * 2, 16)
    print(f"    chunk {cs}..{ce} failed: {last}"); return []


def _chunks(s, e, maxd):
    cur = s
    while cur <= e:
        ce = min(cur + timedelta(days=maxd - 1), e); yield cur, ce; cur = ce + timedelta(days=1)


def main():
    kite = KiteConnect(api_key=os.environ["KITE_API_KEY"]); kite.set_access_token(get_latest_access_token())
    nfo = kite.instruments("NFO")
    fut = [i for i in nfo if i.get("instrument_type") == "FUT" and i.get("name") not in IDX]
    contracts = [(i["name"], i["tradingsymbol"], str(i["expiry"]), i["instrument_token"]) for i in fut]
    print(f"contracts: {len(contracts)} across {len({c[0] for c in contracts})} underlyings", flush=True)

    con = sqlite3.connect(str(DB), timeout=90)
    for _, table, _m in PLAN:
        con.executescript(SCHEMA.format(t=table))
    con.commit()
    rl = RateLimiter(rps=3.0)

    for interval, table, maxd in PLAN:
        print(f"--- {table} ({interval}) ---", flush=True)
        for n, (sym, ts, exp, token) in enumerate(contracts, 1):
            have = con.execute(f"SELECT max(bar_time) FROM {table} WHERE tradingsymbol=?", (ts,)).fetchone()[0]
            fstart = (date.fromisoformat(have[:10]) + timedelta(days=1)) if have else START
            if fstart > END:
                continue
            for cs, ce in _chunks(fstart, END, maxd):
                try:
                    bars = fetch_chunk(kite, token, cs, ce, interval, rl)
                except RuntimeError as e:
                    print("AUTH ERROR, stopping:", e); con.close(); return
                if not bars:
                    continue
                payload = [(sym, ts, exp, token, b["date"].strftime("%Y-%m-%d %H:%M:%S"),
                            float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
                            int(b["volume"]), int(b.get("oi", 0))) for b in bars]
                con.executemany(
                    f"INSERT OR IGNORE INTO {table} (symbol,tradingsymbol,expiry,instrument_token,"
                    f"bar_time,open,high,low,close,volume,oi) VALUES (?,?,?,?,?,?,?,?,?,?,?)", payload)
                con.commit()
            if n % 50 == 0 or n == len(contracts):
                print(f"  {table}: {n}/{len(contracts)} (last {ts})", flush=True)

    # summary
    print("\n=== FUTURES SUMMARY ===", flush=True)
    for _, table, _m in PLAN:
        r = con.execute(f"SELECT count(DISTINCT tradingsymbol), count(*), min(bar_time), max(bar_time) FROM {table}").fetchone()
        print(f"  {table:19} contracts={r[0]:>4} rows={r[1]:>10,} range={r[2]} .. {r[3]}", flush=True)
    con.close()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
