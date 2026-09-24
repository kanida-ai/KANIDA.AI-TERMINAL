"""Live ORDER-FLOW poller (Vortex). Every minute during market hours, snapshot buy/sell
quantity + 5-level depth + LTP/volume/ATP for the F&O universe (cash + front-month future)
into fo_orderflow_1min. This data is LIVE-ONLY (no historical backfill exists), so this
builds the order-flow history going FORWARD.

Usage:
  python orderflow_poller.py --once     # single snapshot (test; works even when market closed)
  python orderflow_poller.py            # loop every minute 09:15-15:30 IST on trading days
"""
import os, sys, json, csv, io, time, argparse, sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta, date

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
IST = timezone(timedelta(hours=5, minutes=30))
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
UDB = ROOT / "data" / "db" / "kanida_universe.db"        # portal DB (broker_accounts)
BATCH = 50


def load_env(p):
    if not Path(p).exists(): return
    for line in Path(p).read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env(ROOT / "config" / ".env")
sys.path.insert(0, str(ROOT / "backend"))
import requests
from autotrade.vault import get_decrypted_creds
try:
    from services.kite_auth import _kite_proxies
    PROX = _kite_proxies() or None
except Exception:
    PROX = None
BASE = os.environ.get("RUPEEZY_API_BASE", "https://vortex-api.rupeezy.in/v2").rstrip("/")

SCHEMA = """
CREATE TABLE IF NOT EXISTS fo_orderflow_1min (
    symbol TEXT NOT NULL, segment TEXT NOT NULL, instrument TEXT NOT NULL,
    bar_time TEXT NOT NULL,
    ltp REAL, volume INTEGER, atp REAL,
    total_buy_qty INTEGER, total_sell_qty INTEGER,
    bid1 REAL, bid1_qty INTEGER, ask1 REAL, ask1_qty INTEGER,
    depth_buy_qty INTEGER, depth_sell_qty INTEGER,
    PRIMARY KEY (instrument, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_of_sym_dt ON fo_orderflow_1min(symbol, bar_time);
"""


def _creds():
    con = sqlite3.connect(str(UDB)); r = con.execute(
        "SELECT broker_account_id, user_id FROM broker_accounts WHERE broker='rupeezy' AND status='ACTIVE' LIMIT 1").fetchone()
    con.close()
    if not r:
        raise RuntimeError("no ACTIVE rupeezy account")
    c = get_decrypted_creds(r[0], str(r[1]))
    return getattr(c, "access_token", None), getattr(c, "api_secret", None)


def build_universe():
    """cash (NSE_EQ) + front-month future (NSE_FO) for each F&O stock + index futures."""
    m = json.loads((ROOT / "data" / "config" / "rupeezy_instruments.json").read_text())
    eq = m["NSE_EQ"]
    # front-month futures from master.csv (nearest expiry >= today)
    rows = list(csv.reader(io.StringIO((ROOT / "data" / "config" / "rupeezy_master.csv").read_text(encoding="utf-8"))))
    hdr = rows[0]; ci = {h: i for i, h in enumerate(hdr)}
    today = datetime.now(IST).strftime("%Y%m%d")
    front = {}
    for r in rows[1:]:
        if len(r) <= ci["security_desc"] or r[ci["exchange"]] != "NSE_FO": continue
        if "FUT" not in r[ci["instrument_name"]].upper(): continue
        exp = r[ci["expiry_date"]]
        if exp < today: continue
        u = r[ci["symbol"]]
        if u not in front or exp < front[u][1]:
            front[u] = (r[ci["token"]], exp, r[ci["security_desc"]])
    con = sqlite3.connect(str(DB))
    fo = [x[0] for x in con.execute("SELECT symbol FROM fo_stock_master WHERE fo_eligible=1").fetchall()] \
        or [x[0] for x in con.execute("SELECT symbol FROM fo_stock_master").fetchall()]
    con.close()
    IDXN = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}
    univ = []
    for s in fo:
        if s in IDXN:  # index → future only
            if s in front: univ.append((s, "FUT", f"NSE_FO-{front[s][0]}"))
            continue
        if s in eq: univ.append((s, "CASH", f"NSE_EQ-{eq[s]}"))
        if s in front: univ.append((s, "FUT", f"NSE_FO-{front[s][0]}"))
    for s in IDXN:  # ensure index futures included
        if s in front and (s, "FUT", f"NSE_FO-{front[s][0]}") not in univ:
            univ.append((s, "FUT", f"NSE_FO-{front[s][0]}"))
    return univ


def _sum_qty(levels):
    return int(sum(int(l.get("quantity", 0)) for l in (levels or [])))


def poll_once(univ):
    tok, xkey = _creds()
    H = {"Authorization": f"Bearer {tok}", "x-api-key": xkey or "", "Content-Type": "application/json"}
    bar = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
    by_inst = {u[2]: u for u in univ}
    rows = []
    for i in range(0, len(univ), BATCH):
        chunk = univ[i:i + BATCH]
        params = [("q", u[2]) for u in chunk] + [("mode", "full")]
        try:
            r = requests.get(f"{BASE}/data/quotes", headers=H, params=params, timeout=25, proxies=PROX)
            data = (r.json() or {}).get("data") or {}
        except Exception as e:
            print(f"  chunk {i} error: {e}"); continue
        for inst, q in data.items():
            u = by_inst.get(inst)
            if not u or not isinstance(q, dict): continue
            d = q.get("depth") or {}
            rows.append((u[0], u[1], inst, bar,
                         q.get("last_trade_price"), q.get("volume"), q.get("average_trade_price"),
                         q.get("total_buy_quantity"), q.get("total_sell_quantity"),
                         (d.get("buy") or [{}])[0].get("price"), (d.get("buy") or [{}])[0].get("quantity"),
                         (d.get("sell") or [{}])[0].get("price"), (d.get("sell") or [{}])[0].get("quantity"),
                         _sum_qty(d.get("buy")), _sum_qty(d.get("sell"))))
    if rows:
        con = sqlite3.connect(str(DB), timeout=60)
        con.executescript(SCHEMA)
        con.executemany("INSERT OR IGNORE INTO fo_orderflow_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        con.commit(); con.close()
    return bar, len(rows)


def market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5: return False, "weekend"
    hm = now.hour * 60 + now.minute
    if hm < 9 * 60 + 15: return False, "pre-open"
    if hm > 15 * 60 + 30: return False, "closed"
    return True, "open"


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--once", action="store_true"); a = ap.parse_args()
    con = sqlite3.connect(str(DB)); con.executescript(SCHEMA); con.commit(); con.close()
    univ = build_universe()
    print(f"[orderflow] universe: {len(univ)} instruments "
          f"({sum(1 for u in univ if u[1]=='CASH')} cash + {sum(1 for u in univ if u[1]=='FUT')} fut)")
    if a.once:
        bar, n = poll_once(univ)
        print(f"[orderflow] ONE snapshot @ {bar}: stored {n} rows")
        con = sqlite3.connect(str(DB))
        for r in con.execute("SELECT symbol,segment,ltp,total_buy_qty,total_sell_qty,depth_buy_qty,depth_sell_qty "
                             "FROM fo_orderflow_1min WHERE bar_time=? ORDER BY symbol LIMIT 6", (bar,)):
            imb = (r[3] or 0) / ((r[3] or 0) + (r[4] or 1)) if (r[3] or r[4]) else 0
            print(f"    {r[0]:<12} {r[1]:<5} ltp={r[2]} buyQ={r[3]:,} sellQ={r[4]:,} buy%={imb*100:.0f}")
        con.close()
        return
    # start-gate: exit fast outside the run window so scheduled (30-min) triggers
    # don't leave idle processes; the ~09:00 trigger idles briefly then runs all day.
    now = datetime.now(IST); hm = now.hour * 60 + now.minute
    if now.weekday() >= 5 or hm < 8 * 60 + 50 or hm > 15 * 60 + 30:
        print(f"[orderflow] outside run window ({now:%Y-%m-%d %H:%M} IST) — exiting"); return
    print("[orderflow] looping every minute 09:15-15:30 IST (Ctrl-C to stop)")
    last = None
    while True:
        ok, why = market_open()
        if not ok:
            if why == "closed": print("[orderflow] market closed — exiting"); break
            time.sleep(15); continue   # pre-open (08:50-09:15) short idle
        cur = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        if cur != last:
            bar, n = poll_once(univ); last = cur
            print(f"[orderflow] {bar}: {n} instruments", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
