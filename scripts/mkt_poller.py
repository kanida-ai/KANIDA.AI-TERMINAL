"""LIVE market-data poller (Zerodha/Kite) -> mkt_orderflow_1min (mkt_ cluster).
Polls quote() every ~5s and AGGREGATES into a true 1-minute bar per instrument:
  OHLC (open=first tick, high/low=running, close=last), minute VOLUME (cumulative delta),
  plus the order-flow snapshot at bar close: OI, total buy/sell qty, and full 5-LEVEL depth.
Flushed once per minute. Forward-only (no historical order-flow/depth exists).

Universe = 499 cash (NSE) + 215 futures (NFO stock+index) + 28 index SPOTs.
Indices have no book -> only OHLC/close populate. 20-level depth needs 5paisa (not Kite).

Run: `python mkt_poller.py --once` (one bar, test) or no-arg (session loop).
"""
import sys, sqlite3, time, argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "universe_engine")); sys.path.insert(0, str(ROOT / "backend"))
from engine.data_fetch import get_kite
from engine.oi_fetch import get_active_fut_contracts

DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
IST = timezone(timedelta(hours=5, minutes=30))
IDXN = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"]
BATCH = 480
POLL_SEC = 5
INDICES = [
    "NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "NIFTY MID SELECT", "NIFTY NEXT 50",
    "NIFTY 100", "NIFTY 200", "NIFTY 500", "INDIA VIX", "NIFTY AUTO", "NIFTY IT", "NIFTY FMCG",
    "NIFTY PHARMA", "NIFTY METAL", "NIFTY ENERGY", "NIFTY REALTY", "NIFTY MEDIA", "NIFTY INFRA",
    "NIFTY PSU BANK", "NIFTY PVT BANK", "NIFTY CONSUMPTION", "NIFTY COMMODITIES", "NIFTY MNC",
    "NIFTY CPSE", "NIFTY PSE", "NIFTY HEALTHCARE", "NIFTY OIL AND GAS", "NIFTY CONSR DURBL",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS mkt_orderflow_1min (
    symbol TEXT, segment TEXT, instrument_key TEXT, bar_time TEXT,
    open REAL, high REAL, low REAL, close REAL, volume INTEGER, atp REAL, oi INTEGER,
    total_buy_qty INTEGER, total_sell_qty INTEGER,
    b1p REAL,b1q INTEGER,b2p REAL,b2q INTEGER,b3p REAL,b3q INTEGER,b4p REAL,b4q INTEGER,b5p REAL,b5q INTEGER,
    a1p REAL,a1q INTEGER,a2p REAL,a2q INTEGER,a3p REAL,a3q INTEGER,a4p REAL,a4q INTEGER,a5p REAL,a5q INTEGER,
    PRIMARY KEY (instrument_key, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_mktof_sym_dt ON mkt_orderflow_1min(symbol, bar_time);
CREATE TABLE IF NOT EXISTS mkt_control (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
"""


def poller_enabled():
    """Admin Start/Stop flag (mkt_control.poller_enabled). Default = enabled."""
    try:
        con = sqlite3.connect(str(DB), timeout=10)
        r = con.execute("SELECT value FROM mkt_control WHERE key='poller_enabled'").fetchone()
        con.close()
        return r is None or str(r[0]) != "0"
    except Exception:
        return True


def build_universe(kite):
    con = sqlite3.connect(str(DB))
    cash = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_1min").fetchall()]
    fo = [r[0] for r in con.execute("SELECT symbol FROM fo_stock_master WHERE fo_eligible=1").fetchall()] \
        or [r[0] for r in con.execute("SELECT symbol FROM fo_stock_master").fetchall()]
    con.close()
    contracts = get_active_fut_contracts([s for s in fo if s not in IDXN] + IDXN)
    univ = []
    for s in cash:
        if s not in IDXN:
            univ.append((s, "CASH", f"NSE:{s}"))
    for u, cs in contracts.items():
        if cs:
            univ.append((u, "FUT", f"NFO:{sorted(cs, key=lambda c: c['expiry'])[0]['tradingsymbol']}"))
    for ix in INDICES:
        univ.append((ix, "INDEX", f"NSE:{ix}"))
    seen = set(); out = []
    for x in univ:
        if x[2] not in seen:
            seen.add(x[2]); out.append(x)
    return out


def _lvl(levels, i):
    l = levels or []
    return (l[i]["price"], l[i]["quantity"]) if i < len(l) else (None, None)


def snapshot(kite, univ):
    """One quote() sweep -> {key: dict(ltp, cumvol, atp, oi, tbq, tsq, bids[5], asks[5])}."""
    keys = [u[2] for u in univ]; snap = {}
    for i in range(0, len(keys), BATCH):
        try:
            data = kite.quote(keys[i:i + BATCH])
        except Exception as e:
            print(f"  quote chunk {i} error: {str(e)[:80]}", flush=True); continue
        for key, q in (data or {}).items():
            if not isinstance(q, dict):
                continue
            d = q.get("depth") or {}
            snap[key] = dict(ltp=q.get("last_price"), cumvol=q.get("volume"), atp=q.get("average_price"),
                             oi=q.get("oi"), tbq=q.get("buy_quantity"), tsq=q.get("sell_quantity"),
                             bids=[_lvl(d.get("buy"), j) for j in range(5)],
                             asks=[_lvl(d.get("sell"), j) for j in range(5)])
    return snap


def flush(bars, prev_vol, minute):
    """Write completed 1-min bars. minute volume = cumulative delta since last flush."""
    rows = []
    for key, agg in bars.items():
        cv = agg["cumvol"]
        mvol = (cv - prev_vol.get(key, 0)) if cv is not None else None
        if cv is not None:
            prev_vol[key] = cv
        b, a = agg["bids"], agg["asks"]
        rows.append((agg["sym"], agg["seg"], key, minute,
                     agg["o"], agg["h"], agg["l"], agg["c"], mvol, agg["atp"], agg["oi"],
                     agg["tbq"], agg["tsq"],
                     b[0][0], b[0][1], b[1][0], b[1][1], b[2][0], b[2][1], b[3][0], b[3][1], b[4][0], b[4][1],
                     a[0][0], a[0][1], a[1][0], a[1][1], a[2][0], a[2][1], a[3][0], a[3][1], a[4][0], a[4][1]))
    if rows:
        con = sqlite3.connect(str(DB), timeout=60); con.executescript(SCHEMA)
        con.executemany(f"INSERT OR IGNORE INTO mkt_orderflow_1min VALUES ({','.join('?'*33)})", rows)
        con.commit(); con.close()
    return len(rows)


def update_bars(bars, snap, univ_map):
    """Fold a snapshot into the in-progress minute bars."""
    for key, s in snap.items():
        ltp = s["ltp"]
        if ltp is None:
            continue
        if key not in bars:
            sym, seg = univ_map[key]
            bars[key] = dict(sym=sym, seg=seg, o=ltp, h=ltp, l=ltp, c=ltp, **{k: s[k] for k in
                             ("cumvol", "atp", "oi", "tbq", "tsq", "bids", "asks")})
        else:
            b = bars[key]; b["h"] = max(b["h"], ltp); b["l"] = min(b["l"], ltp); b["c"] = ltp
            for k in ("cumvol", "atp", "oi", "tbq", "tsq", "bids", "asks"):
                b[k] = s[k]


def phase():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return "weekend"
    hm = now.hour * 60 + now.minute
    if hm < 8 * 60 + 50:
        return "too-early"
    if hm < 9 * 60 + 15:
        return "pre-open"
    if hm <= 15 * 60 + 30:
        return "open"
    return "closed"


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--once", action="store_true")
    ap.add_argument("--force", action="store_true"); a = ap.parse_args()
    con = sqlite3.connect(str(DB)); con.executescript(SCHEMA); con.commit(); con.close()
    kite = get_kite()
    univ = build_universe(kite); univ_map = {u[2]: (u[0], u[1]) for u in univ}
    print(f"[mkt] universe: {len(univ)} instruments "
          f"({sum(1 for u in univ if u[1]=='CASH')} cash + {sum(1 for u in univ if u[1]=='FUT')} fut + "
          f"{sum(1 for u in univ if u[1]=='INDEX')} index) @ {datetime.now(IST):%Y-%m-%d %H:%M IST}", flush=True)
    if a.once:
        minute = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        bars = {}; update_bars(bars, snapshot(kite, univ), univ_map)
        n = flush(bars, {}, minute)
        print(f"[mkt] ONE bar @ {minute}: stored {n} rows", flush=True)
        con = sqlite3.connect(str(DB))
        for r in con.execute("SELECT symbol,segment,close,oi,total_buy_qty,total_sell_qty,b1p,a1p FROM mkt_orderflow_1min "
                             "WHERE bar_time=? ORDER BY symbol LIMIT 6", (minute,)):
            tb, ts = (r[4] or 0), (r[5] or 0); imb = tb / (tb + ts) * 100 if (tb + ts) else 0
            print(f"    {r[0]:<12}{r[1]:<5} close={r[2]} oi={r[3]} buyQ={tb:,} sellQ={ts:,} buy%={imb:.0f} bid1={r[6]} ask1={r[7]}")
        con.close(); return
    # session loop: poll every POLL_SEC, aggregate, flush each completed minute
    bars = {}; prev_vol = {}; cur_min = None
    while True:
        p = phase()
        if p in ("weekend", "too-early", "closed"):
            if bars and cur_min:
                flush(bars, prev_vol, cur_min)
            print(f"[mkt] {p} — exiting", flush=True); break
        if p == "pre-open":
            time.sleep(10); continue
        if not poller_enabled():                         # admin Stop
            if bars and cur_min:
                flush(bars, prev_vol, cur_min)
            print("[mkt] disabled via admin control — exiting", flush=True); break
        now_min = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        if cur_min is None:
            cur_min = now_min
        if now_min != cur_min:
            n = flush(bars, prev_vol, cur_min); print(f"[mkt] {cur_min}: {n} instruments", flush=True)
            bars = {}; cur_min = now_min
        update_bars(bars, snapshot(kite, univ), univ_map)
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
