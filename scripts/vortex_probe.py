"""Probe the Rupeezy Vortex API (READ-ONLY) with the existing ACTIVE token to discover:
  (1) token validity (funds), (2) instrument-master source, (3) historical-data endpoint +
  its lookback/resolution limits. No orders, no account changes — data reads only.
"""
import os, sys, json, time
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")


def load_env(p):
    if not Path(p).exists(): return
    for line in Path(p).read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env(ROOT / "config" / ".env")
sys.path.insert(0, str(ROOT / "backend"))
import requests
from autotrade.vault import get_decrypted_creds

ACC = None
import sqlite3
c = sqlite3.connect(str(ROOT / "data" / "db" / "kanida_universe.db"))
row = c.execute("SELECT broker_account_id, user_id FROM broker_accounts WHERE broker='rupeezy'").fetchone()
c.close()
ACC, UID = row[0], str(row[1])
creds = get_decrypted_creds(ACC, UID)
tok = getattr(creds, "access_token", None)
xkey = getattr(creds, "api_secret", None)
appid = getattr(creds, "api_key", None)
print(f"creds: token={'yes('+str(len(tok))+'ch)' if tok else 'NONE'} x-api-key={'yes' if xkey else 'NONE'} app_id={appid}")
BASE = os.environ.get("RUPEEZY_API_BASE", "https://vortex-api.rupeezy.in/v2").rstrip("/")
H = {"Authorization": f"Bearer {tok}", "x-api-key": xkey or "", "Content-Type": "application/json"}
try:
    from services.kite_auth import _kite_proxies
    PROX = _kite_proxies() or None
except Exception:
    PROX = None
print(f"base={BASE} | proxy={'on' if PROX else 'off'}\n")


def get(path, params=None, tag=""):
    try:
        r = requests.get(f"{BASE}{path}", headers=H, params=params, timeout=30, proxies=PROX)
        body = r.text[:300]
        print(f"  GET {path} {params or ''} -> {r.status_code}  {body[:220]}")
        return r
    except Exception as e:
        print(f"  GET {path} -> EXC {type(e).__name__}: {e}")
        return None


print("== 1) token check (funds) ==")
get("/user/funds", tag="funds")

print("\n== 2) instrument master discovery ==")
for p in ["/data/instruments", "/data/instruments/NSE_EQ", "/instruments", "/data/instrument"]:
    r = get(p)
    if r is not None and r.status_code == 200 and len(r.text) > 500:
        print(f"     ^ looks like the master ({len(r.text)} bytes)")

print("\n== 3) historical-data probe ==")
now = int(time.time()); two_yr = now - 730 * 86400; five_d = now - 5 * 86400
# try a known liquid token guess is risky; first see if history accepts a symbol; try RELIANCE common tokens
for res in ["1", "day", "1D"]:
    for tokguess in ["2885", "738561"]:   # 2885=RELIANCE NSE series id (common), 738561=kite token (unlikely)
        r = get("/data/history", {"exchange": "NSE_EQ", "token": tokguess, "from": five_d, "to": now, "resolution": res})
        if r is not None and r.status_code == 200 and ('"s"' in r.text or '"t"' in r.text or "candle" in r.text.lower()):
            print(f"     ^ HISTORY WORKS: res={res} token={tokguess}")
            break
