"""
ARENA FAST CORE — shared high-performance foundation for ALL specialist agents.
- Payload cache: 1-min bars per stock -> cache/bars/<SYM>.npz (built ONCE; every agent reuses it).
- numba-JIT intraday trail sim (C-speed, path-dependent).
- Vectorized portfolio slot allocation + fund metrics.
Goal: after the one-time cache build, any agent's full-universe backtest runs in seconds.

Build cache once:  python scripts/arena_fast.py build
"""
from __future__ import annotations
import os, sys, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
import numba
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db")
CACHE = ROOT / "cache" / "bars"; CACHE.mkdir(parents=True, exist_ok=True)


def universe():
    con = sqlite3.connect(SNR)
    s = sorted(set(r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall())); con.close()
    return s


def build_one(sym):
    p = CACHE / f"{sym}.npz"
    if p.exists():
        return 0
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time",
                           con, params=(sym,))
    d = pd.read_sql_query("SELECT bar_time,high FROM ohlc_daily WHERE symbol=? ORDER BY bar_time", con, params=(sym,))
    con.close()
    if df.empty or d.empty:
        np.savez(p, empty=np.array([1])); return 0
    date = df["bar_time"].str[:10].str.replace("-", "").astype(np.int32).values
    hm = (df["bar_time"].str[11:13].astype(int) * 100 + df["bar_time"].str[14:16].astype(int)).astype(np.int16).values
    o = df["open"].values.astype(np.float32); h = df["high"].values.astype(np.float32)
    l = df["low"].values.astype(np.float32); c = df["close"].values.astype(np.float32); v = df["volume"].values.astype(np.float32)
    cut = np.where(date[1:] != date[:-1])[0] + 1
    day_start = np.array([0, *cut.tolist()], dtype=np.int64)
    day_id = date[day_start]
    dd = d["bar_time"].str[:10].str.replace("-", "").astype(np.int32).values
    p20 = d["high"].rolling(20).max().shift(1).values.astype(np.float32)
    p20map = dict(zip(dd.tolist(), p20.tolist()))
    day_p20 = np.array([p20map.get(int(x), np.nan) for x in day_id], dtype=np.float32)
    np.savez(p, date=date, hm=hm, o=o, h=h, l=l, c=c, v=v, day_start=day_start, day_id=day_id, day_p20=day_p20)
    return 1


def build_cache():
    import multiprocessing as mp, time
    syms = universe(); t0 = time.time()
    with mp.get_context("spawn").Pool(8) as pool:
        r = pool.map(build_one, syms)
    print(f"cache built: {sum(r)} new / {len(syms)} symbols in {time.time()-t0:.0f}s -> {CACHE}")


def load(sym):
    p = CACHE / f"{sym}.npz"
    if not p.exists():
        return None
    z = np.load(p)
    if "date" not in z:
        return None
    return {k: z[k] for k in z.files}


@numba.njit(cache=True, fastmath=True)
def trail_long(h, l, c, i0, endi, lev, arm, floor, give, hard):
    """Path-dependent intraday LONG trail. Returns (cap_pct, exit_idx). Adverse extreme checked before peak update."""
    entry = c[i0]
    if entry <= 0.0:
        return 0.0, i0
    peak = 0.0; armed = False
    for j in range(i0 + 1, endi + 1):
        lo = lev * (l[j] - entry) / entry * 100.0
        hi = lev * (h[j] - entry) / entry * 100.0
        if not armed:
            if lo <= -hard:
                return -hard, j
        else:
            thr = floor if floor > (peak - give) else (peak - give)
            if lo <= thr:
                return thr, j
        if hi >= arm:
            armed = True
        if hi > peak:
            peak = hi
    return lev * (c[endi] - entry) / entry * 100.0, endi


def _r(x, nd=1, default=0.0):
    try:
        x = float(x)
        return round(x, nd) if np.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def metrics(dayret, cap0=1_500_000.0):
    if len(dayret) == 0:
        return {}
    idx = pd.to_datetime(dayret.index)
    dr = np.clip(dayret.values, -99.9, None)                 # a portfolio can't lose >100% in a day
    eq = pd.Series(np.maximum(cap0 * np.cumprod(1 + dr / 100.0), 1.0), index=idx)
    yrs = (idx[-1] - idx[0]).days / 365.25
    cagr = ((max(eq.iloc[-1], 1.0) / cap0) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
    peak = eq.cummax(); dd = float(((eq - peak) / peak * 100).min())
    me = eq.resample("ME").last()
    mon = (me.pct_change().replace([np.inf, -np.inf], np.nan).dropna() * 100)
    roll = (me.pct_change(12).replace([np.inf, -np.inf], np.nan).dropna() * 100)
    sh = (dayret.mean() / dayret.std() * np.sqrt(252)) if dayret.std() > 0 else 0
    pm = (float((mon > 0).mean() * 100) if len(mon) else 0.0)
    return {"cagr": _r(cagr), "total_ret": _r(eq.iloc[-1] / cap0 * 100 - 100),
            "max_dd": _r(dd), "calmar": _r(cagr / -dd, 2) if dd < 0 else None,
            "avg_month": _r(mon.mean(), 2), "pct_pos_months": round(pm) if np.isfinite(pm) else 0,
            "worst_month": _r(mon.min()), "best_month": _r(mon.max()),
            "roll12_med": _r(roll.median()) if len(roll) else None,
            "roll12_min": _r(roll.min()) if len(roll) else None,
            "sharpe": _r(sh, 2), "years": _r(yrs, 2),
            "start": str(idx[0].date()), "end": str(idx[-1].date()),
            "monthly": [(str(k)[:7], _r(x)) for k, x in mon.items()]}


def slot_portfolio(ev, n_slots):
    """ev: DataFrame [date, entry_hm, exit_hm, cap_pct, symbol] -> daily portfolio return% (each slot = 1/N)."""
    daily = {}
    for date, day in ev.groupby("date", sort=True):
        day = day.sort_values("entry_hm")
        slots = []; held = set(); taken = 0.0
        for eh, xh, cap, sym in zip(day.entry_hm.values, day.exit_hm.values, day.cap_pct.values, day.symbol.values):
            slots = [x for x in slots if x > eh]
            if len(slots) < n_slots and sym not in held:
                slots.append(xh); held.add(sym); taken += cap
        daily[date] = taken / n_slots
    return pd.Series(daily).sort_index()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        build_cache()
    else:
        print("usage: python scripts/arena_fast.py build")
