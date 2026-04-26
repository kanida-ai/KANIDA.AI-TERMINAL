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
instr = kite.instruments("NSE")
keywords = ["MINDTREE", "LTIM", "LTI", "MCDOWELL", "UNITED SPIRITS", "MCDO"]
for i in instr:
    if i["instrument_type"] == "EQ":
        sym  = i["tradingsymbol"].upper()
        name = i.get("name", "").upper()
        for k2 in keywords:
            if k2 in sym or k2 in name:
                print(f"{i['tradingsymbol']:<25} {i['name']}")
                break
