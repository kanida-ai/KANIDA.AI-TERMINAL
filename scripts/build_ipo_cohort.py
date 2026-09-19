"""
Build the IPO cohort WITHOUT touching the Kite key (so it can't impact the running backfill).
Source = NSE's public equity master list (EQUITY_L.csv) which carries DATE OF LISTING for every
listed stock. We take everything listed >= 2015 as the IPO cohort, compute the deterministic
lock-in event dates (SEBI), flag which are already in kanida.db / Nifty-500, and store in
KANIDA_SNR.db.ipo_cohort. The from-listing PRICE history is fetched separately (Kite), chained
to run AFTER the backfill completes.

Run: PYTHONIOENCODING=utf-8 python build_ipo_cohort.py
"""
import io, sqlite3, time
from datetime import datetime, date, timedelta
from pathlib import Path
import requests, pandas as pd

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
COHORT_FLOOR = date(2015, 1, 1)
URLS = ["https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv"]


def log(m): print(f"{datetime.now():%H:%M:%S} {m}", flush=True)


def nse_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                      "Accept": "text/csv,*/*", "Accept-Language": "en-US,en;q=0.9",
                      "Referer": "https://www.nseindia.com/"})
    try:
        s.get("https://www.nseindia.com", timeout=20)
    except Exception:
        pass
    return s


def fetch_equity_list():
    s = nse_session()
    for url in URLS:
        for attempt in range(3):
            try:
                r = s.get(url, timeout=30)
                if r.status_code == 200 and "SYMBOL" in r.text[:200]:
                    return pd.read_csv(io.StringIO(r.text))
                log(f"  {url} -> {r.status_code}; retry"); time.sleep(2); s = nse_session()
            except Exception as e:
                log(f"  {url} err {str(e)[:60]}; retry"); time.sleep(2); s = nse_session()
    raise RuntimeError("could not fetch EQUITY_L.csv")


def parse_dt(x):
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try: return datetime.strptime(str(x).strip(), fmt).date()
        except Exception: pass
    return None


def main():
    df = fetch_equity_list()
    df.columns = [c.strip() for c in df.columns]
    log(f"EQUITY_L.csv: {len(df)} listed stocks | cols {list(df.columns)[:8]}")
    sym_col = "SYMBOL"; name_col = "NAME OF COMPANY"; ser_col = "SERIES"; list_col = "DATE OF LISTING"
    df["listing_date"] = df[list_col].map(parse_dt)
    coh = df[df["listing_date"].notna() & (df["listing_date"] >= COHORT_FLOOR)].copy()
    log(f"listed >= {COHORT_FLOOR}: {len(coh)} IPO-cohort candidates (of {len(df)})")

    kc = sqlite3.connect(KDB)
    have = set(r[0] for r in kc.execute("SELECT DISTINCT symbol FROM ohlc_daily").fetchall())
    n500 = set(r[0] for r in kc.execute("SELECT symbol FROM instrument_labels WHERE in_nifty500=1").fetchall())
    sect = {r[0]: r[1] for r in kc.execute("SELECT symbol,sector FROM instrument_labels").fetchall()}
    kc.close()

    rows = []
    for _, r in coh.iterrows():
        sym = str(r[sym_col]).strip(); ld = r["listing_date"]
        rows.append((sym, str(r.get(name_col, "")).strip(), str(r.get(ser_col, "")).strip(),
                     ld.isoformat(),
                     (ld + timedelta(days=30)).isoformat(),      # anchor 50% lock
                     (ld + timedelta(days=90)).isoformat(),      # anchor remainder
                     (ld + timedelta(days=180)).isoformat(),     # pre-issue 6-month lock
                     1 if sym in have else 0, 1 if sym in n500 else 0, sect.get(sym, "")))

    con = sqlite3.connect(SNR)
    con.execute("DROP TABLE IF EXISTS ipo_cohort")
    con.execute("""CREATE TABLE ipo_cohort(
        symbol TEXT PRIMARY KEY, company TEXT, series TEXT, listing_date TEXT,
        anchor_lock_30d TEXT, anchor_lock_90d TEXT, preipo_lock_180d TEXT,
        in_kanida_db INTEGER, is_nifty500 INTEGER, sector TEXT, prices_fetched INTEGER DEFAULT 0)""")
    con.executemany("INSERT OR REPLACE INTO ipo_cohort "
                    "(symbol,company,series,listing_date,anchor_lock_30d,anchor_lock_90d,preipo_lock_180d,"
                    "in_kanida_db,is_nifty500,sector) VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    by_year = pd.read_sql("SELECT substr(listing_date,1,4) yr, count(*) n, sum(in_kanida_db) have "
                          "FROM ipo_cohort GROUP BY yr ORDER BY yr", con)
    tot = con.execute("SELECT count(*), sum(in_kanida_db), sum(is_nifty500) FROM ipo_cohort").fetchone()
    con.close()
    log(f"ipo_cohort stored: {tot[0]} IPOs | already in kanida.db: {tot[1]} | in Nifty-500: {tot[2]} | "
        f"NEED price fetch: {tot[0]-tot[1]}")
    log("by listing year (n | already have data):\n" + by_year.to_string(index=False))
    log("IPO COHORT BUILT (no Kite calls; backfill unaffected)")


if __name__ == "__main__":
    main()
