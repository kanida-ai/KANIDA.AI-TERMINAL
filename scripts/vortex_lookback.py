"""How deep is Vortex historical 1-min (and daily), and does it cap per request?
The whole point: does Vortex beat Kite (Kite = ~2yr 1-min, 60-day/request cap; ~3mo futures)."""
import os, sys, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
exec(Path(ROOT / "scripts" / "vortex_probe.py").read_text().split("print(\"== 1)")[0])  # reuse creds/get()

IST = timezone(timedelta(hours=5, minutes=30))
def ep(y, m, d, hh=0, mm=0): return int(datetime(y, m, d, hh, mm, tzinfo=IST).timestamp())

def probe(exch, token, a, b, res, lbl):
    r = get("/data/history", {"exchange": exch, "token": token, "from": a, "to": b, "resolution": res})
    if r is None: return
    try:
        j = r.json()
    except Exception:
        print(f"  {lbl}: non-json"); return
    s = j.get("s")
    t = j.get("t") or []
    if s == "ok" and t:
        f = datetime.fromtimestamp(t[0], IST).strftime("%Y-%m-%d %H:%M")
        l = datetime.fromtimestamp(t[-1], IST).strftime("%Y-%m-%d %H:%M")
        print(f"  {lbl}: {len(t)} bars | {f} .. {l}")
    else:
        print(f"  {lbl}: s={s} (no data)")

print("== 1-MIN lookback depth (RELIANCE NSE_EQ token 2885), 3-day windows ==")
for y in [2025, 2024, 2023, 2022, 2021, 2020, 2018, 2016]:
    probe("NSE_EQ", "2885", ep(y, 6, 2), ep(y, 6, 5), "1", f"1-min {y}")

print("\n== MAX SINGLE-REQUEST SPAN for 1-min (does it cap like Kite's 60 days?) ==")
for lbl, a, b in [("1 month", ep(2026, 5, 1), ep(2026, 6, 1)),
                  ("6 months", ep(2025, 12, 1), ep(2026, 6, 1)),
                  ("2 years", ep(2024, 6, 1), ep(2026, 6, 1))]:
    probe("NSE_EQ", "2885", a, b, "1", f"span {lbl}")

print("\n== DAILY (1D) lookback depth ==")
for y in [2015, 2010, 2005]:
    probe("NSE_EQ", "2885", ep(y, 1, 1), ep(2026, 7, 1), "1D", f"daily since {y}")
