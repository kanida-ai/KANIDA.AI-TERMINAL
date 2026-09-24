"""
Refresh Kite Connect access token via OAuth request_token exchange.
Run once per trading day.

Usage:
    1. Visit: https://kite.trade/connect/login?api_key=<KITE_API_KEY>&v=3
    2. Login to Zerodha (you're already logged in via portal — should be one-click)
    3. From the redirect URL, copy the `request_token` value
       (it'll be in the address bar like: ...request_token=ABC123...)
    4. python scripts/refresh_kite_token.py <request_token>

The script:
  - Exchanges request_token + api_secret → access_token via Kite API
  - Writes the new access_token to:
      • kite_tokens table in kanida_quant.db  (single source of truth)
      • config/.env                           (legacy mirror, for backwards compat)
"""
from __future__ import annotations
import os, re, sqlite3, sys
from datetime import datetime, date
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent.parent
ENV      = ROOT / "config" / ".env"
MAIN_DB  = ROOT / "data" / "db" / "kanida_quant.db"


def load_env():
    if not ENV.exists():
        sys.exit(f"ERROR: {ENV} not found")
    for line in ENV.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def update_env_token(new_token: str):
    text = ENV.read_text()
    if "KITE_ACCESS_TOKEN=" in text:
        text = re.sub(r"^KITE_ACCESS_TOKEN=.*$",
                      f"KITE_ACCESS_TOKEN={new_token}",
                      text, count=1, flags=re.MULTILINE)
    else:
        text += f"\nKITE_ACCESS_TOKEN={new_token}\n"
    ENV.write_text(text)
    print(f"  Updated {ENV} -> KITE_ACCESS_TOKEN={new_token}")


def main():
    if len(sys.argv) < 2:
        sys.exit("Usage: python scripts/refresh_kite_token.py <request_token>")
    request_token = sys.argv[1].strip()

    load_env()
    api_key    = os.environ["KITE_API_KEY"]
    api_secret = os.environ["KITE_API_SECRET"]

    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=api_key)
    try:
        sess = kite.generate_session(request_token, api_secret=api_secret)
    except Exception as e:
        sys.exit(f"ERROR generating session: {e}")

    new_token = sess["access_token"]
    print(f"  New access token (first 8 chars): {new_token[:8]}...")

    # Write to kite_tokens table (single source of truth)
    if MAIN_DB.exists():
        con = sqlite3.connect(str(MAIN_DB))
        con.execute("""
            INSERT INTO kite_tokens (access_token, token_date, set_by, created_at)
            VALUES (?, ?, ?, ?)
        """, (new_token, date.today().isoformat(),
              "universe_engine_refresh",
              datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        con.commit()
        con.close()
        print(f"  Wrote new row to kite_tokens table in {MAIN_DB.name}")
    else:
        print(f"  WARN: {MAIN_DB} not found; only updating .env")

    # Mirror to .env (backwards compat for code still reading env)
    update_env_token(new_token)

    # Probe
    kite.set_access_token(new_token)
    try:
        margins = kite.margins()
        print(f"  Probe OK — margins endpoint reachable.")
    except Exception as e:
        print(f"  WARN probe failed: {e}")
    print("\nToken refreshed. Now run:")
    print("  python scripts/setup_intraday.py --months 6 --workers 16 --rps 5")


if __name__ == "__main__":
    main()
