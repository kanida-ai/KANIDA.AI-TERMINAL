"""Build/refresh mkt_reference — static reference for the mkt_ cluster: segment, quote key,
sector, lot size, margin, market cap (static, monthly), expiry. Idempotent (rebuild anytime).
Sector from falcon_sectors, lot/margin from fo_stock_master, market_cap left for the monthly
NSE refresh (populate_market_cap()). Run standalone or from the daily automation."""
import sys, sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "universe_engine")); sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scripts"))
from engine.data_fetch import get_kite
import mkt_poller as P

DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
IST = timezone(timedelta(hours=5, minutes=30))

SCHEMA = """
CREATE TABLE IF NOT EXISTS mkt_reference (
    symbol TEXT, segment TEXT, instrument_key TEXT,
    sector TEXT, sub_sector TEXT, company TEXT,
    lot_size INTEGER, total_margin REAL, market_cap REAL,
    expiry TEXT, updated_at TEXT,
    PRIMARY KEY (symbol, segment)
);
"""


def build():
    kite = get_kite()
    univ = P.build_universe(kite)                      # (symbol, segment, key)
    # front-month expiry for futures (from the same contract discovery)
    from engine.oi_fetch import get_active_fut_contracts
    fut_u = [u[0] for u in univ if u[1] == "FUT"]
    contracts = get_active_fut_contracts(fut_u)
    exp = {u: sorted(cs, key=lambda c: c["expiry"])[0]["expiry"] for u, cs in contracts.items() if cs}

    con = sqlite3.connect(str(DB)); con.executescript(SCHEMA)
    sectors = {r[0]: (r[1], r[2], r[3]) for r in con.execute("SELECT symbol,sector,sub_sector,company FROM falcon_sectors")}
    lots = {r[0]: (r[1], r[2]) for r in con.execute("SELECT symbol,lot_size,total_margin FROM fo_stock_master")}
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
    rows = []
    for sym, seg, key in univ:
        sec = sectors.get(sym, (None, None, None))
        lot = lots.get(sym, (None, None))
        rows.append((sym, seg, key, sec[0], sec[1], sec[2], lot[0], lot[1], None,
                     exp.get(sym) if seg == "FUT" else None, now))
    con.execute("DELETE FROM mkt_reference")           # full rebuild
    con.executemany(f"INSERT OR REPLACE INTO mkt_reference VALUES ({','.join('?'*11)})", rows)
    con.commit()
    # report
    seg = dict(con.execute("SELECT segment,count(*) FROM mkt_reference GROUP BY segment").fetchall())
    withsec = con.execute("SELECT count(*) FROM mkt_reference WHERE sector IS NOT NULL").fetchone()[0]
    withlot = con.execute("SELECT count(*) FROM mkt_reference WHERE lot_size IS NOT NULL").fetchone()[0]
    con.close()
    print(f"[mkt_reference] {len(rows)} rows {seg} | with sector={withsec} | with lot={withlot} | "
          f"market_cap=0 (awaiting monthly NSE refresh)")


if __name__ == "__main__":
    build()
