"""Re-probe with CORRECT tokens: NIFTY spot (26000 NSE_EQ), RELIANCE NSE future (61284
NSE_FO). Pin the exact 1-min earliest date for cash. Crack the live /data/quotes format
to see if buy-qty / sell-qty / market depth are available."""
import os, sys, json
from pathlib import Path
from datetime import datetime, timezone, timedelta
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
exec(Path(ROOT / "scripts" / "vortex_probe.py").read_text().split("print(\"== 1)")[0])
IST = timezone(timedelta(hours=5, minutes=30))
def ep(y, m, d): return int(datetime(y, m, d, tzinfo=IST).timestamp())
def hist(exch, tok, a, b, res="1"):
    r = get("/data/history", {"exchange": exch, "token": tok, "from": a, "to": b, "resolution": res})
    try: return r.json()
    except Exception: return {}
def show(lbl, j):
    t = j.get("t") or []
    if t: print(f"  {lbl}: {len(t)} bars | {datetime.fromtimestamp(t[0],IST):%Y-%m-%d %H:%M} .. {datetime.fromtimestamp(t[-1],IST):%Y-%m-%d %H:%M}")
    else: print(f"  {lbl}: s={j.get('s')}")

print("== NIFTY SPOT index (26000 NSE_EQ) 1-min lookback ==")
for y in [2026, 2024, 2022, 2020]:
    show(f"NIFTY spot {y}", hist("NSE_EQ", "26000", ep(y, 6, 2), ep(y, 6, 6)))

print("\n== RELIANCE NSE future (61284 NSE_FO) 1-min ==")
for lbl, y, m in [("recent", 2026, 6), ("2025", 2025, 5)]:
    show(f"RELfut NSE {lbl}", hist("NSE_FO", "61284", ep(y, m, 1), ep(y, m, 6)))

print("\n== PIN cash 1-min earliest (RELIANCE 2885), 5-day windows ending at various dates ==")
for y, m, d in [(2026, 1, 15), (2026, 2, 15), (2026, 3, 1), (2026, 3, 20), (2026, 4, 1), (2026, 4, 10)]:
    show(f"cash to {y}-{m:02d}-{d:02d}", hist("NSE_EQ", "2885", ep(y, m, max(1, d - 5)), ep(y, m, d)))

print("\n== LIVE /data/quotes — find the format + whether it carries buy/sell qty & depth ==")
import requests
for params in [{"q": "NSE_EQ-2885"}, {"q": "NSE_EQ-2885", "mode": "full"}, {"instruments": "NSE_EQ-2885"},
               {"q": ["NSE_EQ-2885"]}, {"exchange": "NSE_EQ", "token": "2885", "mode": "full"}]:
    try:
        r = requests.get(f"{BASE}/data/quotes", headers=H, params=params, timeout=20, proxies=PROX)
        body = r.text[:400]
        print(f"  /data/quotes {params} -> {r.status_code} {body[:360]}")
    except Exception as e:
        print(f"  /data/quotes {params} -> EXC {e}")
