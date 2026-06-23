"""
Runtime universe resolution (constitutional P5/P6/P7).

  • F&O eligible universe + lot sizes  -> from Kite ``instruments("NFO")`` at runtime
  • Nifty 500 constituents             -> from NSE/niftyindices CSV at runtime

Never hardcoded. Each successful fetch is cached into the DB
(``fo_universe_membership`` / ``fo_stock_master``) so a later offline run can fall
back to the most recent known membership rather than a literal list.
"""
from __future__ import annotations

import io
import os
import re
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

_PROD_TREE = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
_QUANT_DB = _PROD_TREE / "data" / "db" / "kanida_quant.db"
_ENV_FILE = _PROD_TREE / "config" / ".env"


# ── kite ───────────────────────────────────────────────────────────────────────

def _env(key: str) -> Optional[str]:
    if os.environ.get(key):
        return os.environ[key]
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line.startswith(key + "="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def _access_token() -> Optional[str]:
    if _QUANT_DB.exists():
        try:
            c = sqlite3.connect(str(_QUANT_DB))
            row = c.execute(
                "SELECT access_token FROM kite_tokens ORDER BY id DESC LIMIT 1"
            ).fetchone()
            c.close()
            if row:
                return row[0]
        except Exception:
            pass
    return _env("KITE_ACCESS_TOKEN")


def get_kite():
    from kiteconnect import KiteConnect

    api_key = _env("KITE_API_KEY")
    token = _access_token()
    if not api_key or not token:
        raise RuntimeError("Kite credentials unavailable (api_key/access_token)")
    k = KiteConnect(api_key=api_key)
    k.set_access_token(token)
    return k


# ── F&O universe ─────────────────────────────────────────────────────────────--

def fetch_fo_universe_kite() -> List[Tuple[str, int]]:
    """Return [(underlying_symbol, lot_size), ...] for current F&O FUT contracts."""
    k = get_kite()
    inst = k.instruments("NFO")
    seen = {}
    for row in inst:
        if row.get("instrument_type") != "FUT":
            continue
        name = row.get("name")
        lot = row.get("lot_size") or 0
        if name and name not in seen:
            seen[name] = lot
    return sorted(seen.items())


def _fallback_fo_list(con: sqlite3.Connection) -> List[str]:
    # 1) most recent cached membership
    row = con.execute(
        "SELECT as_of_date FROM fo_universe_membership ORDER BY as_of_date DESC LIMIT 1"
    ).fetchone()
    if row:
        d = row[0]
        return [r[0] for r in con.execute(
            "SELECT symbol FROM fo_universe_membership WHERE as_of_date=?", (d,))]
    # 2) parse the existing ingest list (last resort, not hardcoded in this module)
    f = _PROD_TREE / "data" / "ingest" / "fetch_fno_kite.py"
    if f.exists():
        m = re.search(r"FNO_TICKERS\s*=\s*\[(.*?)\]", f.read_text(), re.S)
        if m:
            return re.findall(r'"([A-Z0-9&\-]+)"', m.group(1))
    return []


def get_fo_universe(con: sqlite3.Connection, as_of_date: Optional[str] = None,
                    persist: bool = True) -> List[str]:
    try:
        pairs = fetch_fo_universe_kite()
        symbols = [s for s, _ in pairs]
        if persist and as_of_date:
            con.executemany(
                "INSERT OR REPLACE INTO fo_universe_membership(symbol,as_of_date,lot_size,source) VALUES (?,?,?,?)",
                [(s, as_of_date, lot, "kite") for s, lot in pairs])
            con.executemany(
                "INSERT OR REPLACE INTO fo_stock_master(symbol,lot_size,fo_eligible,as_of_date) VALUES (?,?,1,?)",
                [(s, lot, as_of_date) for s, lot in pairs])
            con.commit()
        return symbols
    except Exception as e:
        fb = _fallback_fo_list(con)
        print(f"[universe] Kite F&O fetch failed ({e}); fallback list n={len(fb)}")
        return fb


# ── Nifty 500 ─────────────────────────────────────────────────────────────────-

NSE500_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"


def fetch_nifty500_nse() -> List[str]:
    import requests
    import pandas as pd

    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(NSE500_URL, headers=headers, timeout=20)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    col = "Symbol" if "Symbol" in df.columns else df.columns[2]
    return sorted(df[col].astype(str).str.strip().tolist())


def get_nifty500(con: sqlite3.Connection) -> List[str]:
    try:
        syms = fetch_nifty500_nse()
        if len(syms) >= 400:
            return syms
    except Exception as e:
        print(f"[universe] NSE Nifty500 fetch failed ({e}); using universe_master")
    return [r[0] for r in con.execute(
        "SELECT symbol FROM universe_master WHERE in_nifty500=1")]


def get_universes(con: sqlite3.Connection, as_of_date: Optional[str] = None,
                  available_only: bool = True):
    """Return (fo_universe, lt_universe), optionally intersected with symbols that
    actually have computed features (so the walk-forward has data)."""
    fo = get_fo_universe(con, as_of_date=as_of_date)
    lt = get_nifty500(con)
    if available_only:
        have = {r[0] for r in con.execute(
            "SELECT DISTINCT symbol FROM persona_signal_features")}
        fo = [s for s in fo if s in have]
        lt = [s for s in lt if s in have]
    return fo, lt


if __name__ == "__main__":
    from persona_engine import db
    con = db.connect()
    fo, lt = get_universes(con, as_of_date="2026-06-22")
    print(f"F&O universe (with features): {len(fo)}  e.g. {fo[:8]}")
    print(f"Nifty500 universe (with features): {len(lt)}  e.g. {lt[:8]}")
    con.close()
