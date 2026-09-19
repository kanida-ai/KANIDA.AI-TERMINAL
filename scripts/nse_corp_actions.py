"""
Fetch NSE corporate actions (public feed) 2020->2026 into kanida.db `corp_actions`,
classify each (dividend/split/bonus/rights/buyback/demerger/agm/other), then cross-tag
`event_markers` with the corp-action type when a flagged big-move/gap/vol day lands on
(±1 calendar day) a corp-action ex-date.

Run: PYTHONIOENCODING=utf-8 python nse_corp_actions.py
"""
import sqlite3, time, re
from datetime import datetime, date, timedelta
from pathlib import Path
import requests

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
START, END = date(2020, 1, 1), date(2026, 8, 1)


def log(m): print(f"{datetime.now():%H:%M:%S} {m}", flush=True)


def session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-actions"})
    s.get("https://www.nseindia.com", timeout=20)
    return s


def classify(subj):
    u = (subj or "").upper()
    if "BONUS" in u: return "bonus"
    if "SPLIT" in u or "SUB-DIVISION" in u or "SUB DIVISION" in u or "FACE VALUE" in u: return "split"
    if "RIGHTS" in u: return "rights"
    if "BUY" in u and "BACK" in u: return "buyback"
    if "DEMERGER" in u or "SCHEME OF ARRANGEMENT" in u or "ARRANGEMENT" in u: return "demerger"
    if "DIVIDEND" in u or "DISTRIBUTION" in u: return "dividend"
    if "ANNUAL GENERAL" in u or re.search(r"\bAGM\b", u): return "agm"
    if "MEETING" in u: return "meeting"
    return "other"


def parse_dt(s):
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try: return datetime.strptime(s.strip(), fmt).date()
        except Exception: pass
    return None


def main():
    con = sqlite3.connect(str(DB), timeout=90)
    universe = set(r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_daily").fetchall())
    con.execute("""CREATE TABLE IF NOT EXISTS corp_actions(
        symbol TEXT, ex_date TEXT, action_type TEXT, subject TEXT, series TEXT, face_value TEXT,
        source TEXT DEFAULT 'NSE', PRIMARY KEY(symbol, ex_date, subject))""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_ca_sym ON corp_actions(symbol, ex_date)")
    s = session()
    seen = {}; total = 0; cur = START; nreq = 0
    while cur < END:
        nxt = min(cur + timedelta(days=60), END)
        f, t = cur.strftime("%d-%m-%Y"), nxt.strftime("%d-%m-%Y")
        try:
            r = s.get("https://www.nseindia.com/api/corporates-corporateActions",
                      params={"index": "equities", "from_date": f, "to_date": t}, timeout=30)
            rows = r.json() if r.status_code == 200 else []
            if not isinstance(rows, list): rows = rows.get("data", [])
        except Exception as e:
            log(f"  {f}..{t}: ERR {str(e)[:60]}; re-seeding"); s = session(); time.sleep(2); continue
        for x in rows:
            sym = x.get("symbol"); ex = parse_dt(x.get("exDate", "") or "")
            if not sym or not ex or sym not in universe:
                continue
            subj = (x.get("subject") or "").strip()
            key = (sym, ex.isoformat(), subj)
            seen[key] = (sym, ex.isoformat(), classify(subj), subj, x.get("series", ""), str(x.get("faceVal", "")))
        total += len(rows); nreq += 1
        if nreq % 10 == 0:
            log(f"  fetched through {t}: {len(seen)} universe actions so far ({total} raw)"); s = session()
        time.sleep(1.3); cur = nxt + timedelta(days=1)
    con.executemany("INSERT OR IGNORE INTO corp_actions(symbol,ex_date,action_type,subject,series,face_value) "
                    "VALUES (?,?,?,?,?,?)", list(seen.values()))
    con.commit()
    by = con.execute("SELECT action_type, count(*) FROM corp_actions GROUP BY action_type ORDER BY 2 DESC").fetchall()
    log(f"corp_actions: {con.execute('SELECT count(*) FROM corp_actions').fetchone()[0]} rows | by type: {dict(by)}")

    # cross-tag event_markers with corp-action type (±1 day)
    cols = [c[1] for c in con.execute("PRAGMA table_info(event_markers)").fetchall()]
    if "corp_action" not in cols:
        con.execute("ALTER TABLE event_markers ADD COLUMN corp_action TEXT")
    ca = {}
    for sym, ex, at in con.execute("SELECT symbol, ex_date, action_type FROM corp_actions").fetchall():
        ca.setdefault(sym, {})[ex] = at
    tagged = 0
    for eid, sym, ed in con.execute("SELECT rowid, symbol, event_date FROM event_markers").fetchall():
        m = ca.get(sym)
        if not m: continue
        d0 = parse_dt(ed) or (datetime.strptime(ed[:10], "%Y-%m-%d").date() if ed else None)
        if not d0: continue
        hit = None
        for off in (0, -1, 1):
            k = (d0 + timedelta(days=off)).isoformat()
            if k in m: hit = m[k]; break
        if hit:
            con.execute("UPDATE event_markers SET corp_action=? WHERE rowid=?", (hit, eid)); tagged += 1
    con.commit()
    log(f"event_markers cross-tagged with a corp action: {tagged}")
    con.close()
    log("NSE CORP-ACTIONS COMPLETE")


if __name__ == "__main__":
    main()
