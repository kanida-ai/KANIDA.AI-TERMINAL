"""Minimal live-token test: fetch 1-min for one symbol, last few days. Read-only (no DB write)."""
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
ROOT = Path(__file__).resolve().parent.parent
ENG = ROOT / "universe_engine"
sys.path.insert(0, str(ENG))
from engine.data_fetch import get_kite, get_instrument_tokens

try:
    kite = get_kite()
    toks = get_instrument_tokens(kite, ["RELIANCE"])
    print("instrument token RELIANCE:", toks)
    it = toks["RELIANCE"]
    end = date.today()
    raw = kite.historical_data(it,
        from_date=datetime.combine(end - timedelta(days=6), datetime.min.time()),
        to_date=datetime.combine(end, datetime.max.time()), interval="minute")
    print(f"rows returned: {len(raw)}")
    if raw:
        print("first bar:", raw[0]["date"], raw[0]["close"])
        print("last  bar:", raw[-1]["date"], raw[-1]["close"])
    print("TOKEN_OK")
except Exception as e:
    print("TOKEN_FAIL:", type(e).__name__, str(e)[:200])
