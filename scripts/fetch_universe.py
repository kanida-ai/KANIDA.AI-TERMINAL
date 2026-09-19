"""Fetch daily / 5-min / 1-min OHLCV for the ENTIRE labelled universe
(instrument_labels: 497 stocks + 136 indices) into kanida.db, 2022-01-01 -> today.

Reuses the hardened primitives in fetch_ohlc.py (retry, set-difference gap repair)
and adds:
  * universe pulled from instrument_labels (symbol -> kite_token), stocks + indices;
  * token AUTO-REFRESH: on a KiteAuthError (daily expiry mid-run) it re-mints via the
    engine's auth_worker and rebuilds the client, then continues — so a multi-hour
    run survives the ~7:30am IST token rollover;
  * progress logging (flushed) so it can be tailed while running in the background.

Resumable + idempotent: re-running continues from each symbol's last stored bar and
repairs interior holes. Safe to kill and restart.
"""
import os, sys, sqlite3, subprocess, time
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fetch_ohlc as F  # hardened primitives
from kiteconnect import KiteConnect

START = date(2022, 1, 1)
END = date.today()
AUTH_WORKER = F.ENGINE_ROOT / "scripts" / "auth_worker.py"


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def build_client():
    from engine.data_fetch import get_latest_access_token
    k = KiteConnect(api_key=os.environ["KITE_API_KEY"])
    k.set_access_token(get_latest_access_token())
    k.profile()  # verify live
    return k


def refresh_token():
    log("AUTH: token rejected -> running auth_worker to re-mint ...")
    try:
        subprocess.run([sys.executable, str(AUTH_WORKER)], timeout=300,
                       capture_output=True, text=True)
    except Exception as e:
        log(f"AUTH: auth_worker error: {e}")
    time.sleep(2)


def universe(con):
    return con.execute(
        "SELECT symbol, kite_token, instrument_type FROM instrument_labels "
        "WHERE kite_token IS NOT NULL ORDER BY (instrument_type='INDEX'), symbol").fetchall()


def with_auth_retry(fn, get_client):
    """Run fn(client); on KiteAuthError re-mint + rebuild + retry (up to 3x)."""
    client = get_client[0]
    for _ in range(3):
        try:
            return fn(client)
        except F.KiteAuthError:
            refresh_token()
            client = build_client()
            get_client[0] = client
    return fn(client)  # last attempt, let it raise if still bad


def run():
    con = sqlite3.connect(str(F.DB), timeout=120)
    for _, table, _m in F.PLAN:
        con.executescript(F.SCHEMA.format(t=table))
    con.commit()

    uni = universe(con)
    log(f"universe: {len(uni)} instruments "
        f"({sum(1 for _,_,t in uni if t=='STOCK')} stocks, {sum(1 for _,_,t in uni if t=='INDEX')} indices)")
    holder = [build_client()]
    rl = F.RateLimiter(rps=3.0)

    # ---- forward fetch: daily (fast) first, then 5-min, then 1-min ----
    for interval, table, maxd in F.PLAN:
        log(f"=== forward fetch {table} ({interval}) ===")
        for i, (sym, token, _typ) in enumerate(uni):
            with_auth_retry(
                lambda k, s=sym, t=token: F.forward_fetch(con, k, s, t, interval, table, maxd, START, END, rl),
                holder)
            if i % 25 == 0 or i == len(uni) - 1:
                log(f"  {table}: {i+1}/{len(uni)} (last {sym})")

    # ---- gap repair for intraday, vs each symbol's own daily calendar ----
    for interval, table, maxd in F.PLAN:
        if table not in F.INTRADAY:
            continue
        log(f"=== repair {table} ===")
        for i, (sym, token, _typ) in enumerate(uni):
            ref = F._days_present(con, "ohlc_daily", sym)
            if not ref:
                continue
            left = with_auth_retry(
                lambda k, s=sym, t=token, r=ref: F.repair(con, k, s, t, interval, table, maxd, r, rl),
                holder)
            if left and len(left) > 3:  # >3 missing after repair may = suspension/no-data
                log(f"  {sym} {table}: {len(left)} day(s) still missing (likely no Kite data)")
            if i % 50 == 0 or i == len(uni) - 1:
                log(f"  repair {table}: {i+1}/{len(uni)}")

    con.close()
    log("UNIVERSE FETCH COMPLETE")


if __name__ == "__main__":
    run()
