"""
Save a Kite access token to the central kite_tokens table.
Single source of truth for all services (main engine + universe engine + admin portal).

Usage:
    python scripts/save_kite_token.py <ACCESS_TOKEN>
"""
from __future__ import annotations
import os, sqlite3, sys
from datetime import datetime, date
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent.parent
MAIN_DB  = ROOT / "data" / "db" / "kanida_quant.db"


def main():
    if len(sys.argv) < 2:
        sys.exit("Usage: python scripts/save_kite_token.py <ACCESS_TOKEN>")
    new_token = sys.argv[1].strip()
    if len(new_token) < 16:
        sys.exit("ERROR: token looks too short — paste the full token")

    # Probe first
    # Load API key from .env
    env = ROOT / "config" / ".env"
    for line in env.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
    kite.set_access_token(new_token)
    try:
        kite.margins()
        print(f"  Probe OK — token is valid")
    except Exception as e:
        sys.exit(f"  Probe FAILED — token is invalid: {e}")

    # Write to kite_tokens
    con = sqlite3.connect(MAIN_DB)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO kite_tokens (access_token, token_date, set_by, created_at)
        VALUES (?, ?, ?, ?)
    """, (new_token, date.today().isoformat(),
          "universe_engine_cli",
          datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    con.commit()
    print(f"  Wrote new row to kite_tokens (token starts {new_token[:8]}...)")

    # Also mirror into config/.env for backwards compat with code that still reads env
    text = env.read_text()
    import re
    if "KITE_ACCESS_TOKEN=" in text:
        text = re.sub(r"^KITE_ACCESS_TOKEN=.*$",
                      f"KITE_ACCESS_TOKEN={new_token}", text,
                      count=1, flags=re.MULTILINE)
    else:
        text += f"\nKITE_ACCESS_TOKEN={new_token}\n"
    env.write_text(text)
    print(f"  Mirrored to {env}")
    con.close()
    print("\nDone. All services will pick up the new token automatically.")


if __name__ == "__main__":
    main()
