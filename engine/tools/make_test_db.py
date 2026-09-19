"""Build a small SQLite database that mimics the real one, for testing.

Deliberately uses AWKWARD column names (trade_date, tradingsymbol, o/h/l/c/v,
qty) so the ColumnMap machinery gets exercised rather than accidentally
working because everything was already called `close`.

The intraday path is generated FIRST and the daily bar is derived from it, so
daily and intraday are exactly consistent -- which is what you should verify
about your own two tables before trusting any of this.

    python tools/make_test_db.py --out data/test_market.db --symbols 6 --days 60
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from state_engine import data as sedata   # noqa: E402

SESSION_MINUTES = 375   # 09:15 -> 15:30


def build(out: str, n_symbols: int, n_days: int, seed: int = 5) -> None:
    rng = np.random.default_rng(seed)
    daily = sedata.make_synthetic_panel(n_symbols=n_symbols, n_days=n_days, seed=seed)

    # U-shaped intraday volume profile
    x = np.linspace(0, 1, SESSION_MINUTES)
    profile = 0.6 + 1.8 * (x - 0.5) ** 2
    profile = profile / profile.sum()

    minute_rows, daily_rows = [], []
    for (sym, sec), grp in daily.groupby(["symbol", "sector"], sort=False):
        for r in grp.itertuples(index=False):
            o, c = float(r.open), float(r.close)
            day_sigma = max(abs(c - o) / max(o, 1e-9), 0.004)

            # Brownian bridge from open to close
            steps = rng.normal(0, day_sigma / np.sqrt(SESSION_MINUTES), SESSION_MINUTES)
            path = np.cumsum(steps)
            path = path - np.linspace(0, path[-1], SESSION_MINUTES)      # pin both ends
            logp = np.log(o) + np.linspace(0, np.log(c / o), SESSION_MINUTES) + path
            price = np.exp(logp)
            price[0], price[-1] = o, c

            wig = np.abs(rng.normal(0, day_sigma * 0.12, SESSION_MINUTES))
            hi = price * (1 + wig)
            lo = price * (1 - wig)
            op = np.concatenate([[o], price[:-1]])
            hi = np.maximum(hi, np.maximum(op, price))
            lo = np.minimum(lo, np.minimum(op, price))

            vol = np.maximum(rng.poisson(float(r.volume) * profile), 1)
            ts = pd.Timestamp(r.date) + pd.Timedelta(hours=9, minutes=15) + \
                pd.to_timedelta(np.arange(SESSION_MINUTES), unit="m")

            minute_rows.append(pd.DataFrame({
                "ts": ts, "tradingsymbol": sym, "open_price": op, "high_price": hi,
                "low_price": lo, "close_price": price, "qty": vol,
            }))
            daily_rows.append({
                "trade_date": pd.Timestamp(r.date).date().isoformat(),
                "tradingsymbol": sym, "o": o, "h": float(hi.max()),
                "l": float(lo.min()), "c": c, "v": int(vol.sum()), "sector_name": sec,
            })

    mins = pd.concat(minute_rows, ignore_index=True)
    mins["ts"] = mins["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    eod = pd.DataFrame(daily_rows)

    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    if os.path.exists(out):
        os.remove(out)
    con = sqlite3.connect(out)
    eod.to_sql("eod_bars", con, index=False)
    mins.to_sql("minute_bars", con, index=False)
    con.execute("CREATE INDEX ix_eod ON eod_bars (tradingsymbol, trade_date)")
    con.execute("CREATE INDEX ix_min ON minute_bars (tradingsymbol, ts)")
    con.commit()
    con.close()

    size = os.path.getsize(out) / 1e6
    print(f"wrote {out}  ({size:.1f} MB)")
    print(f"  eod_bars    : {len(eod):,} rows")
    print(f"  minute_bars : {len(mins):,} rows")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/test_market.db")
    p.add_argument("--symbols", type=int, default=6)
    p.add_argument("--days", type=int, default=60)
    p.add_argument("--seed", type=int, default=5)
    a = p.parse_args()
    build(a.out, a.symbols, a.days, a.seed)
