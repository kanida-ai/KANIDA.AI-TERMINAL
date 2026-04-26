import os, sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

env = ROOT / "config" / ".env"
for line in env.read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    os.environ[k.strip()] = v.strip().strip('"').strip("'")

from kiteconnect import KiteConnect
kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
kite.set_access_token(os.environ["KITE_ACCESS_TOKEN"])
try:
    profile = kite.profile()
    print("TOKEN OK -- logged in as:", profile["user_name"])
    print("  Email:", profile.get("email",""))
    print("  Broker:", profile.get("broker",""))
except Exception as e:
    print("TOKEN EXPIRED or INVALID:", e)
    print()
    print("To get a fresh token:")
    print(f"  1. Visit: https://kite.trade/connect/login?api_key={os.environ['KITE_API_KEY']}&v=3")
    print("  2. Log in and copy the request_token from the redirect URL")
    print("  3. Share it here and I will exchange it for a new access token")
