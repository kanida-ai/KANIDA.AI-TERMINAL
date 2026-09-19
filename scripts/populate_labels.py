"""Build kanida.db `instrument_labels`: one row per stock/index with sector,
sub-sector, company, F&O flag, and Nifty index-membership flags.

Sources:
  * universe_master (kanida_universe.db) — 508 Nifty-500 symbols + index flags + sector
  * falcon_sectors  (kanida_universe.db) — sub_sector + company
  * Kite instruments("NSE")             — instrument_token, INDICES list
  * Kite instruments("NFO") FUT names   — F&O-underlying flag
"""
import os, sys, sqlite3
from pathlib import Path
from datetime import datetime

ENG = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ENG / "universe_engine")); sys.path.insert(0, str(ENG / "backend"))
from engine.data_fetch import get_kite  # noqa
UNIV = ENG / "data" / "db" / "kanida_universe.db"
DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")

SCHEMA = """
DROP TABLE IF EXISTS instrument_labels;
CREATE TABLE instrument_labels (
    symbol           TEXT PRIMARY KEY,
    exchange         TEXT,
    instrument_type  TEXT,          -- STOCK | INDEX
    kite_token       INTEGER,
    sector           TEXT,
    sub_sector       TEXT,
    company          TEXT,
    in_nifty50  INTEGER DEFAULT 0,
    in_nifty100 INTEGER DEFAULT 0,
    in_nifty200 INTEGER DEFAULT 0,
    in_nifty500 INTEGER DEFAULT 0,
    is_fno      INTEGER DEFAULT 0,
    is_active   INTEGER DEFAULT 1,
    updated_at  TEXT
);
"""

INDEX_FNO = {"NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "NIFTY MIDCAP SELECT",
             "NIFTY NEXT 50"}  # indices that have listed derivatives

# universe_master carries a few stale tradingsymbols (recent NSE renames). Map old
# -> current Kite symbol so they resolve + fetch. Verified against Kite name field.
RENAME = {
    "ZOMATO": "ETERNAL",       # Zomato rebranded to Eternal (2025)
    "MCDOWELL-N": "UNITDSPR",  # United Spirits symbol change
    "TATAMOTORS": "TMCV",      # demerged; TMCV retains "TATA MOTORS" (TMPV added separately)
}
# extra current symbols to add to the fetch universe (demerger spin-offs)
EXTRA = [("TMPV", "NSE", "Auto")]
# DUMMYVEDL* are Vedanta-demerger placeholders; these 4 are not in Kite's master.
DROP = {"DUMMYVEDL1", "DUMMYVEDL2", "DUMMYVEDL3", "DUMMYVEDL4"}


def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    kite = get_kite()
    nse = kite.instruments("NSE")
    eq_tok = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "NSE"}
    idx = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "INDICES"}
    fno = {i["name"] for i in kite.instruments("NFO") if i.get("instrument_type") == "FUT"}

    U = sqlite3.connect(str(UNIV))
    umaster = U.execute("SELECT symbol, exchange, sector, in_nifty50, in_nifty100, in_nifty200, "
                        "in_nifty500, is_active FROM universe_master").fetchall()
    # falcon_sectors is the fuller sector source (503 rows); keyed by the CURRENT symbol
    fsec = {r[0]: (r[1], r[2], r[3]) for r in
            U.execute("SELECT symbol, sector, sub_sector, company FROM falcon_sectors").fetchall()}
    U.close()

    con = sqlite3.connect(str(DB), timeout=60)
    con.executescript(SCHEMA)

    rows = []
    for sym, exch, sector, n50, n100, n200, n500, active in umaster:
        if sym in DROP:
            continue
        cur = RENAME.get(sym, sym)                      # store under current symbol
        fs, ss, comp = fsec.get(cur) or fsec.get(sym) or (None, None, None)
        rows.append((cur, exch or "NSE", "STOCK", eq_tok.get(cur), fs or sector, ss, comp,
                     n50, n100, n200, n500, 1 if cur in fno else 0, active, now))
    for sym, exch, sector in EXTRA:
        fs, ss, comp = fsec.get(sym, (sector, None, None))
        rows.append((sym, exch, "STOCK", eq_tok.get(sym), fs or sector, ss, comp,
                     0, 0, 0, 1, 1 if sym in fno else 0, 1, now))
    for name, tok in idx.items():
        rows.append((name, "NSE", "INDEX", tok, None, None, name,
                     0, 0, 0, 0, 1 if name in INDEX_FNO else 0, 1, now))

    con.executemany(
        "INSERT INTO instrument_labels (symbol,exchange,instrument_type,kite_token,sector,sub_sector,"
        "company,in_nifty50,in_nifty100,in_nifty200,in_nifty500,is_fno,is_active,updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(symbol) DO UPDATE SET exchange=excluded.exchange, instrument_type=excluded.instrument_type, "
        "kite_token=excluded.kite_token, sector=excluded.sector, sub_sector=excluded.sub_sector, "
        "company=excluded.company, in_nifty50=excluded.in_nifty50, in_nifty100=excluded.in_nifty100, "
        "in_nifty200=excluded.in_nifty200, in_nifty500=excluded.in_nifty500, is_fno=excluded.is_fno, "
        "is_active=excluded.is_active, updated_at=excluded.updated_at", rows)
    con.commit()

    tot = con.execute("SELECT count(*) FROM instrument_labels").fetchone()[0]
    st = con.execute("SELECT count(*) FROM instrument_labels WHERE instrument_type='STOCK'").fetchone()[0]
    ix = con.execute("SELECT count(*) FROM instrument_labels WHERE instrument_type='INDEX'").fetchone()[0]
    fn = con.execute("SELECT count(*) FROM instrument_labels WHERE is_fno=1").fetchone()[0]
    notok = con.execute("SELECT count(*) FROM instrument_labels WHERE instrument_type='STOCK' AND kite_token IS NULL").fetchone()[0]
    print(f"instrument_labels: {tot} rows ({st} stocks, {ix} indices) | is_fno={fn} | stocks_without_kite_token={notok}")
    print("sectors:", [r for r in con.execute("SELECT sector, count(*) FROM instrument_labels WHERE instrument_type='STOCK' GROUP BY sector ORDER BY 2 DESC LIMIT 8").fetchall()])
    if notok:
        print("  unresolved stocks:", [r[0] for r in con.execute("SELECT symbol FROM instrument_labels WHERE instrument_type='STOCK' AND kite_token IS NULL").fetchall()])
    con.close()


if __name__ == "__main__":
    main()
