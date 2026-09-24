"""Add INDEX futures (NIFTY/BANKNIFTY/FINNIFTY/MIDCPNIFTY/NIFTYNXT50) 1-min + OI to
ohlc_futures_1min from Kite. Reuses the stock-futures fetch machinery. Idempotent."""
import sys, sqlite3, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "universe_engine")); sys.path.insert(0, str(ROOT / "backend"))
from fetch_futures_1min import _fetch_one, SCHEMA, START, END, DB
from engine.data_fetch import RateLimiter
from engine.oi_fetch import get_active_fut_contracts

IDX = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"]
con = sqlite3.connect(str(DB)); con.executescript(SCHEMA); con.commit(); con.close()
by = get_active_fut_contracts(IDX)
rl = RateLimiter(rps=3.0)
args = [(sym, c, START, END, str(DB), rl) for sym in IDX for c in by.get(sym, [])]
print(f"[idxfut] {len(args)} index-future contracts | window {START}..{END}", flush=True)
summ = {"done": 0, "rows": 0, "auth": 0}
t0 = time.time()
with ThreadPoolExecutor(max_workers=6) as ex:
    futs = {ex.submit(_fetch_one, a): a[1]["tradingsymbol"] for a in args}
    for f in as_completed(futs):
        r = f.result(); summ["done"] += 1; summ["rows"] += r.get("rows", 0)
        if r["status"] == "auth_error": summ["auth"] += 1
        print(f"  {futs[f]:<20} rows={r.get('rows',0):,} status={r['status']}", flush=True)
print(f"[idxfut] DONE contracts={summ['done']} rows={summ['rows']:,} auth_err={summ['auth']} in {time.time()-t0:.0f}s")
con = sqlite3.connect(str(DB))
for sym in IDX:
    row = con.execute("SELECT count(*), count(distinct tradingsymbol), min(substr(bar_time,1,10)), max(substr(bar_time,1,10)) "
                      "FROM ohlc_futures_1min WHERE symbol=?", (sym,)).fetchone()
    print(f"  {sym}: {row[0]:,} rows | {row[1]} contracts | {row[2]}..{row[3]}")
con.close()
