"""LIVE TICK capture (Zerodha KiteTicker websocket, FULL mode) -> mkt_trades_1min.

The 5-second quote poller sees the book; this sees the TRADES. From the tick stream we
reconstruct, per instrument per minute:
  - traded volume split into BUY vs SELL (Lee-Ready: trade vs bid/ask midpoint, tick-rule
    fallback) -> was the minute buyer- or seller-driven,
  - trade-size footprint: avg per tick, the largest single-tick volume (block proxy), and
    the largest last_traded_quantity (biggest single print) -> one whale vs a retail crowd.

Universe = CASH + FUT (indices don't trade). Self-gates 09:15-15:30 IST, respects the admin
Start/Stop flag, auto-reconnects. Runs in parallel with the quote poller (both WAL + busy_timeout).
"""
import os, sys, sqlite3, time, threading, argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "universe_engine")); sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scripts"))
from engine.data_fetch import get_kite, get_latest_access_token
from mkt_poller import build_universe, phase, poller_enabled
from kiteconnect import KiteTicker

DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
IST = timezone(timedelta(hours=5, minutes=30))

SCHEMA = """
CREATE TABLE IF NOT EXISTS mkt_trades_1min (
    symbol TEXT, segment TEXT, instrument_key TEXT, bar_time TEXT,
    n_ticks INTEGER, volume INTEGER, buy_vol INTEGER, sell_vol INTEGER, buy_vol_pct REAL,
    avg_tick_vol REAL, max_tick_vol INTEGER, max_trade_qty INTEGER,
    PRIMARY KEY (instrument_key, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_mkttr_sym_dt ON mkt_trades_1min(symbol, bar_time);
"""

# shared state (mutated in the ticker thread)
_state = {}      # token -> dict(pv=prev cumulative volume, pp=prev price)
_bucket = {}     # token -> dict(n,vol,buy,sell,maxtick,maxq)
_curmin = {"m": None}
_tokmap = {}     # token -> (symbol, segment, key)
_lock = threading.Lock()


def _now_min():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M")


def _classify(price, bid, ask, prev):
    mid = (bid + ask) / 2 if (bid and ask) else None
    if mid is not None and price is not None:
        if price > mid:
            return "buy"
        if price < mid:
            return "sell"
    if prev is None or price is None:
        return "buy"
    return "buy" if price >= prev else "sell"


def flush_minute(minute):
    with _lock:
        if not _bucket:
            return 0
        items = list(_bucket.items()); _bucket.clear()
    rows = []
    for tok, bk in items:
        info = _tokmap.get(tok)
        if not info or bk["vol"] <= 0:
            continue
        sym, seg, key = info
        bp = bk["buy"] / bk["vol"] * 100 if bk["vol"] else None
        rows.append((sym, seg, key, minute, bk["n"], bk["vol"], bk["buy"], bk["sell"],
                     round(bp, 1) if bp is not None else None,
                     round(bk["vol"] / bk["n"], 1) if bk["n"] else None, bk["maxtick"], bk["maxq"]))
    if rows:
        con = sqlite3.connect(str(DB), timeout=60); con.execute("PRAGMA busy_timeout=60000")
        con.executescript(SCHEMA)
        con.executemany(f"INSERT OR IGNORE INTO mkt_trades_1min VALUES ({','.join(['?']*12)})", rows)
        con.commit(); con.close()
    return len(rows)


def on_ticks(ws, ticks):
    m = _now_min()
    if _curmin["m"] is None:
        _curmin["m"] = m
    if m != _curmin["m"]:
        done = _curmin["m"]; _curmin["m"] = m
        n = flush_minute(done)
        print(f"[tick] {done}: {n} instruments", flush=True)
    with _lock:
        for t in ticks:
            tok = t.get("instrument_token")
            vol = t.get("volume_traded"); price = t.get("last_price"); ltq = t.get("last_traded_quantity") or 0
            d = t.get("depth") or {}
            bid = (d.get("buy") or [{}])[0].get("price"); ask = (d.get("sell") or [{}])[0].get("price")
            st = _state.get(tok)
            dvol = (vol - st["pv"]) if (st and vol is not None and vol >= st["pv"]) else 0
            direction = _classify(price, bid, ask, st["pp"] if st else None)
            bk = _bucket.setdefault(tok, dict(n=0, vol=0, buy=0, sell=0, maxtick=0, maxq=0))
            bk["n"] += 1; bk["vol"] += dvol
            bk["buy" if direction == "buy" else "sell"] += dvol
            if dvol > bk["maxtick"]:
                bk["maxtick"] = dvol
            if ltq > bk["maxq"]:
                bk["maxq"] = ltq
            _state[tok] = dict(pv=vol, pp=price)


def on_connect(ws, response):
    toks = list(_tokmap.keys())
    ws.subscribe(toks)
    ws.set_mode(ws.MODE_FULL, toks)
    print(f"[tick] connected + subscribed {len(toks)} instruments (FULL mode)", flush=True)


def on_error(ws, code, reason):
    print(f"[tick] error {code}: {reason}", flush=True)


def on_close(ws, code, reason):
    print(f"[tick] closed {code}: {reason}", flush=True)


def watchdog(kws):
    """Stop the socket when the session ends or admin disables the poller."""
    while True:
        p = phase()
        if p in ("closed", "weekend", "too-early") or not poller_enabled():
            flush_minute(_curmin["m"] or _now_min())
            print(f"[tick] {p if p!='open' else 'disabled'} — stopping", flush=True)
            try:
                kws.close()
            except Exception:
                pass
            os._exit(0)
        time.sleep(5)


def build_tokmap():
    kite = get_kite()
    univ = [u for u in build_universe(kite) if u[1] in ("CASH", "FUT")]   # indices don't trade
    keys = [u[2] for u in univ]; bykey = {u[2]: (u[0], u[1]) for u in univ}
    for i in range(0, len(keys), 400):
        for key, info in kite.ltp(keys[i:i + 400]).items():
            if key in bykey:
                _tokmap[info["instrument_token"]] = (bykey[key][0], bykey[key][1], key)
    return len(_tokmap)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--test-secs", type=int, default=0); a = ap.parse_args()
    try:                                                # best-effort; flush() creates it if locked now
        con = sqlite3.connect(str(DB), timeout=30); con.execute("PRAGMA busy_timeout=30000")
        con.executescript(SCHEMA); con.commit(); con.close()
    except sqlite3.OperationalError:
        print("[tick] DB busy (backfill?) — table will be created on first flush", flush=True)
    n = build_tokmap()
    print(f"[tick] resolved {n} instrument tokens (cash+fut) @ {datetime.now(IST):%Y-%m-%d %H:%M IST}", flush=True)
    if not a.test_secs:
        while phase() == "pre-open":
            time.sleep(15)
        if phase() != "open":
            print(f"[tick] {phase()} — not starting", flush=True); return
    kws = KiteTicker(os.environ["KITE_API_KEY"], get_latest_access_token())
    kws.on_ticks = on_ticks; kws.on_connect = on_connect; kws.on_error = on_error; kws.on_close = on_close
    if a.test_secs:                                     # connection smoke test
        threading.Thread(target=lambda: (time.sleep(a.test_secs), flush_minute(_curmin["m"] or _now_min()),
                                         kws.close(), os._exit(0)), daemon=True).start()
    else:
        threading.Thread(target=watchdog, args=(kws,), daemon=True).start()
    kws.connect()


if __name__ == "__main__":
    main()
