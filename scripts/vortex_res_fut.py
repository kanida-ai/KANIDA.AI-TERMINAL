"""Vortex: (a) do coarser intraday resolutions (5/15/30/60-min) go back further than 1-min?
(b) instrument master coverage (NFO futures) + a futures historical fetch."""
import os, sys, time, csv, io
from pathlib import Path
from datetime import datetime, timezone, timedelta
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
exec(Path(ROOT / "scripts" / "vortex_probe.py").read_text().split("print(\"== 1)")[0])
import requests
IST = timezone(timedelta(hours=5, minutes=30))
def ep(y, m, d): return int(datetime(y, m, d, tzinfo=IST).timestamp())

def depth(exch, token, a, b, res, lbl):
    r = get("/data/history", {"exchange": exch, "token": token, "from": a, "to": b, "resolution": res})
    try:
        j = r.json(); t = j.get("t") or []
    except Exception:
        print(f"  {lbl}: err"); return
    if t:
        print(f"  {lbl}: {len(t)} bars | {datetime.fromtimestamp(t[0],IST):%Y-%m-%d %H:%M} .. {datetime.fromtimestamp(t[-1],IST):%Y-%m-%d %H:%M}")
    else:
        print(f"  {lbl}: s={j.get('s')}")

print("== coarser-resolution lookback (RELIANCE 2885): does 15/30/60-min go back further than 1-min? ==")
for res in ["5", "15", "30", "60"]:
    for y in [2024, 2022, 2020]:
        depth("NSE_EQ", "2885", ep(y, 6, 1), ep(y, 6, 10), res, f"res{res}-min {y}")

print("\n== instrument master (static.rupeezy.in/master.csv) ==")
raw = requests.get("https://static.rupeezy.in/master.csv", timeout=60).text
rdr = list(csv.reader(io.StringIO(raw)))
print(f"  rows: {len(rdr)} | header: {rdr[0][:12]}")
# find columns: look for a RELIANCE FUT row + a cash row
hdr = rdr[0]
def col(name):
    for i, h in enumerate(hdr):
        if name.lower() in h.lower(): return i
    return None
print(f"  sample data row: {rdr[1][:12]}")
# count NFO FUT + find a RELIANCE future
nfo_fut = [r for r in rdr[1:] if len(r) > 3 and any('FUT' in str(x).upper() for x in r) and any('RELIANCE' in str(x).upper() for x in r)]
print(f"  RELIANCE-FUT-like rows: {len(nfo_fut)}")
for r in nfo_fut[:4]: print("   ", r[:10])
