"""Deeper Vortex probe: (1) index & F&O 1-min lookback depth, (2) FULL response fields of
/data/history (is there buy/sell qty / OI beyond OHLCV?), (3) live /data/quote depth
(buy qty / sell qty / bid-ask). Read-only."""
import os, sys, json, csv, io
from pathlib import Path
from datetime import datetime, timezone, timedelta
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
exec(Path(ROOT / "scripts" / "vortex_probe.py").read_text().split("print(\"== 1)")[0])
import requests
IST = timezone(timedelta(hours=5, minutes=30))
def ep(y, m, d): return int(datetime(y, m, d, tzinfo=IST).timestamp())

# ---- master: find index + futures tokens ----
rows = list(csv.reader(io.StringIO((ROOT / "data" / "config" / "rupeezy_master.csv").read_text(encoding="utf-8"))))
hdr = rows[0]; ci = {h: i for i, h in enumerate(hdr)}
exchs = {}
for r in rows[1:]:
    if len(r) > ci['exchange']: exchs[r[ci['exchange']]] = exchs.get(r[ci['exchange']], 0) + 1
print("exchanges in master:", dict(sorted(exchs.items(), key=lambda x: -x[1])[:12]))
def find(pred, n=4):
    out = []
    for r in rows[1:]:
        if len(r) > ci['security_desc'] and pred(r): out.append(r)
        if len(out) >= n: break
    return out
nifty = find(lambda r: r[ci['symbol']] in ('NIFTY', 'NIFTY 50', 'NIFTY50') and 'IDX' in r[ci['instrument_name']].upper())
if not nifty: nifty = find(lambda r: 'NIFTY' in r[ci['symbol']].upper() and 'IDX' in r[ci['instrument_name']].upper())
print("NIFTY index candidates:", [(r[ci['token']], r[ci['exchange']], r[ci['symbol']], r[ci['instrument_name']]) for r in nifty])
relfut = find(lambda r: r[ci['security_desc']] == 'RELIANCE26JULFUT')
print("RELIANCE Jul future:", [(r[ci['token']], r[ci['exchange']], r[ci['security_desc']]) for r in relfut])

IDX_TOK = nifty[0][ci['token']] if nifty else None
IDX_EXCH = nifty[0][ci['exchange']] if nifty else None
FUT_TOK = relfut[0][ci['token']] if relfut else None
FUT_EXCH = relfut[0][ci['exchange']] if relfut else None

print("\n== FULL /data/history response fields (cash 1-min, recent) ==")
r = get("/data/history", {"exchange": "NSE_EQ", "token": "2885", "from": ep(2026, 6, 29), "to": ep(2026, 7, 4), "resolution": "1"})
j = r.json(); print("  keys:", list(j.keys()))
if j.get("t"): print("  one bar (all arrays at idx -1):", {k: (v[-1] if isinstance(v, list) and v else v) for k, v in j.items()})

print("\n== INDEX 1-min lookback (NIFTY) ==")
if IDX_TOK:
    for y in [2026, 2024, 2022, 2020, 2018]:
        rr = get("/data/history", {"exchange": IDX_EXCH, "token": IDX_TOK, "from": ep(y, 6, 2), "to": ep(y, 6, 6), "resolution": "1"})
        try:
            jj = rr.json(); t = jj.get("t") or []
            print(f"  NIFTY 1-min {y}: {len(t)} bars" + (f" {datetime.fromtimestamp(t[0],IST):%Y-%m-%d}..{datetime.fromtimestamp(t[-1],IST):%Y-%m-%d}" if t else f" s={jj.get('s')}"))
        except Exception as e: print("  err", e)

print("\n== F&O FUTURE 1-min lookback (RELIANCE Jul fut) ==")
if FUT_TOK:
    for y in [2026, 2025, 2024]:
        rr = get("/data/history", {"exchange": FUT_EXCH, "token": FUT_TOK, "from": ep(y, 5, 2), "to": ep(y, 5, 6), "resolution": "1"})
        try:
            jj = rr.json(); t = jj.get("t") or []
            print(f"  RELfut 1-min {y}: {len(t)} bars, keys={list(jj.keys())}" + (f" {datetime.fromtimestamp(t[0],IST):%Y-%m-%d}" if t else f" s={jj.get('s')}"))
        except Exception as e: print("  err", e)

print("\n== LIVE /data/quote — does it carry buy qty / sell qty / depth? ==")
for path, params in [("/data/quote", {"exchange": "NSE_EQ", "token": "2885"}),
                     ("/data/quote", {"exchange": "NSE_EQ", "token": "2885", "mode": "full"}),
                     ("/data/quotes", {"exchange": "NSE_EQ", "token": "2885"})]:
    rr = get(path, params)
    if rr is not None and rr.status_code == 200:
        try:
            jj = rr.json(); print(f"  {path} {params.get('mode','')}: keys={list(jj.keys())} | data keys={list((jj.get('data') or {}).keys())[:20]}")
        except Exception: pass
