"""Find correct Kite tradingsymbols — exact and fuzzy search."""
import os, sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

env = ROOT / "config" / ".env"
for line in env.read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line: continue
    k, v = line.split("=", 1)
    os.environ[k.strip()] = v.strip().strip('"').strip("'")

from kiteconnect import KiteConnect
kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
kite.set_access_token(os.environ["KITE_ACCESS_TOKEN"])

instruments = kite.instruments("NSE")
nse_eq = {i["tradingsymbol"]: i for i in instruments if i["segment"] == "NSE" and i["instrument_type"] == "EQ"}
print(f"NSE EQ instruments: {len(nse_eq)}\n")

EXACT = ["TATAMOTORS", "ZOMATO", "MCDOWELL-N", "LTIM", "LTIMINDTREE", "ETERNAL", "UNITDSPR", "TMCV"]
for s in EXACT:
    found = nse_eq.get(s)
    print(f"  {s:<20} -> {'FOUND: ' + found['name'] if found else 'NOT FOUND'}")

print("\n--- Name-based search ---")
NAMES = {
    "TATAMOTORS": "TATA MOTORS",
    "ZOMATO":     "ZOMATO",
    "MCDOWELL-N": "MCDOWELL",
    "LTIM":       "LTIMINDTREE",
}
for want, name_frag in NAMES.items():
    matches = [(s, i["name"]) for s, i in nse_eq.items() if name_frag.upper() in i.get("name","").upper()]
    print(f"  {want}: {matches[:5] if matches else 'NOT FOUND'}")
