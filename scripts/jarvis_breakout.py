"""
AGENT JARVIS — breakout specialist, v1 FIXED-RULE baseline (no learning yet; that's v2).
Cross-stock. Each day, intraday, a stock "breaks out" when price first crosses its prior-20-day high
(after 09:20, before 14:30). Jarvis holds a ROTATING book of up to N_SLOTS breakouts: a free slot is
filled by the next qualifying breakout (score >= threshold), long, managed by an intraday trail, squared
off 15:20; when it exits the slot frees for the next. Compounding fund accounting -> CAGR / monthly /
rolling / drawdown. LEAK-FREE: signal & score use only info up to the breakout minute; prior-20d-high
uses only prior days. v1 is a fixed strategy (no fitting) so there's no train/test split to leak.

Run: python scripts/jarvis_breakout.py            (writes reports/jarvis_v1.json)
"""
from __future__ import annotations
import os, sys, json, sqlite3, time
os.environ.setdefault("OMP_NUM_THREADS", "1")
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))
import backtest_1min as B
KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db"); REP = ROOT / "reports"
CAP0 = 1_500_000.0; N_SLOTS = 15; LEV = 5.0; COST_SLIP = 0.80    # capital-% round-trip
ARM, FLOOR, GIVE, HARD = 6.0, 2.0, 4.0, 3.0                       # intraday capital-% trail
START, END = "2023-01-01", "2026-12-31"
BREAK_FROM, BREAK_TO, SQ = "09:20", "14:30", "15:20"
VOL_SURGE_MIN = 1.5                                              # breakout-bar vol >= 1.5x trailing avg


def load_1min_vol(sym):
    """Per-date intraday arrays incl. volume over START..END (own loader — B.load_1min is 2025-26 & no volume)."""
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? "
                           "AND bar_time>=? AND bar_time<? ORDER BY bar_time", con,
                           params=[sym, START + " 00:00:00", "2027-01-01"])
    con.close()
    if df.empty:
        return {}
    d = df["bar_time"].str[:10].values; hm = df["bar_time"].str[11:16].values
    o = df["open"].values; h = df["high"].values; l = df["low"].values; c = df["close"].values; v = df["volume"].values
    out = {}; cut = np.where(d[1:] != d[:-1])[0] + 1; bounds = [0, *cut.tolist(), len(d)]
    for a, b in zip(bounds[:-1], bounds[1:]):
        out[d[a]] = {"hm": hm[a:b], "o": o[a:b], "h": h[a:b], "l": l[a:b], "c": c[a:b], "v": v[a:b]}
    return out


def sim_long(hm, o, h, l, c, i0):
    entry = float(c[i0])
    if entry <= 0:
        return None
    peak = 0.0; armed = False
    for j in range(i0 + 1, len(hm)):
        if hm[j] >= SQ:
            return (5.0 * (float(c[j]) - entry) / entry * 100.0), hm[j], "square-off"
        lo = 5.0 * (float(l[j]) - entry) / entry * 100.0        # adverse extreme (conservative first)
        hi = 5.0 * (float(h[j]) - entry) / entry * 100.0
        if not armed:
            if lo <= -HARD:
                return -HARD, hm[j], "hard-stop"
        else:
            thr = max(FLOOR, peak - GIVE)
            if lo <= thr:
                return thr, hm[j], "trail"
        if hi >= ARM:
            armed = True
        peak = max(peak, hi)
    return (5.0 * (float(c[-1]) - entry) / entry * 100.0), hm[-1], "eod"


def events_for(sym):
    """All breakout trades for one symbol: list of (date, entry_hm, exit_hm, cap_pct, score)."""
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    d = pd.read_sql_query("SELECT bar_time,high FROM ohlc_daily WHERE symbol=? ORDER BY bar_time", con, params=(sym,))
    if d.empty:
        con.close(); return []
    d["date"] = d["bar_time"].str[:10]
    d["p20h"] = d["high"].rolling(20).max().shift(1)             # prior-20d high (excludes today)
    p20 = dict(zip(d["date"], d["p20h"]))
    con.close()
    m1 = load_1min_vol(sym)
    if not m1:
        return []
    out = []
    for date, g in m1.items():
        if date < START or date > END:
            continue
        lvl = p20.get(date)
        if lvl is None or not np.isfinite(lvl):
            continue
        hm = g["hm"]; o = g["o"]; h = g["h"]; l = g["l"]; c = g["c"]; v = g["v"]
        win = np.where((hm >= BREAK_FROM) & (hm <= BREAK_TO) & (h >= lvl))[0]
        if len(win) == 0:
            continue
        i0 = int(win[0])                                          # first breakout bar
        if i0 < 3:
            continue
        avgv = float(np.mean(v[:i0])) if i0 > 0 else 0.0
        surge = float(v[i0] / avgv) if avgv > 0 else 0.0
        if surge < VOL_SURGE_MIN:
            continue
        r = sim_long(hm, o, h, l, c, i0)
        if r is None:
            continue
        cap = r[0] - COST_SLIP
        out.append((sym, date, str(hm[i0]), str(r[1]), float(cap), surge))
    return out


def portfolio(events):
    """Chronological slot fill -> daily portfolio return% (compounding). events: DataFrame sorted by date,entry_hm."""
    daily = {}
    for date, day in events.groupby("date", sort=True):
        rows = day.sort_values("entry_hm").itertuples(index=False)
        slots = []            # list of exit_hm for occupied slots
        held = set(); taken = []
        for e in rows:
            slots = [x for x in slots if x > e.entry_hm]         # free slots whose trade already exited
            if len(slots) < N_SLOTS and e.symbol not in held:
                slots.append(e.exit_hm); held.add(e.symbol); taken.append(e.cap_pct)
        daily[date] = sum(taken) / N_SLOTS if taken else 0.0     # each slot = 1/N of capital
    return pd.Series(daily).sort_index()


def metrics(dayret):
    idx = pd.to_datetime(dayret.index)
    eq = CAP0 * np.cumprod(1 + dayret.values / 100.0)
    eq = pd.Series(eq, index=idx)
    yrs = (idx[-1] - idx[0]).days / 365.25
    cagr = ((eq.iloc[-1] / CAP0) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
    peak = eq.cummax(); dd = ((eq - peak) / peak * 100).min()
    mon = eq.resample("ME").last().pct_change().dropna() * 100
    roll = eq.resample("ME").last().pct_change(12).dropna() * 100
    trades = int((dayret != 0).sum())
    sharpe = (dayret.mean() / dayret.std() * np.sqrt(252)) if dayret.std() > 0 else 0
    return {"cagr": round(float(cagr), 1), "total_ret": round(float(eq.iloc[-1] / CAP0 - 1) * 100, 1),
            "max_dd": round(float(dd), 1), "calmar": round(float(cagr / -dd), 2) if dd < 0 else None,
            "avg_month": round(float(mon.mean()), 2), "pct_pos_months": round(float((mon > 0).mean() * 100)),
            "worst_month": round(float(mon.min()), 1), "best_month": round(float(mon.max()), 1),
            "roll12_min": round(float(roll.min()), 1) if len(roll) else None,
            "roll12_med": round(float(roll.median()), 1) if len(roll) else None,
            "trading_days": trades, "sharpe": round(float(sharpe), 2), "years": round(float(yrs), 2),
            "start": str(idx[0].date()), "end": str(idx[-1].date()),
            "monthly": [(str(k)[:7], round(float(v), 1)) for k, v in mon.items()]}


def main():
    import multiprocessing as mp
    t0 = time.time()
    cache = REP / "jarvis_events.csv"
    if cache.exists() and "--fresh" not in sys.argv:
        df = pd.read_csv(cache, dtype={"entry_hm": str, "exit_hm": str, "date": str})
        print(f"  loaded {len(df):,} cached breakout events from {cache.name}")
    else:
        con = sqlite3.connect(SNR)
        syms = sorted(set(r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall())); con.close()
        with mp.get_context("spawn").Pool(8) as pool:
            allev = pool.map(events_for, syms)
        rows = [e for lst in allev for e in lst]
        df = pd.DataFrame(rows, columns=["symbol", "date", "entry_hm", "exit_hm", "cap_pct", "surge"])
        df.to_csv(cache, index=False)
    dayret = portfolio(df)
    m = metrics(dayret)
    m["n_breakout_trades_universe"] = len(df)
    m["n_slots"] = N_SLOTS
    (REP / "jarvis_v1.json").write_text(json.dumps(m), encoding="utf-8")
    dt = time.time() - t0
    print(f"\n===== AGENT JARVIS v1 (fixed-rule breakout portfolio, {N_SLOTS} slots) — {dt:.0f}s =====")
    print(f"  universe breakout trades detected: {len(df):,}  across {df['date'].nunique()} trading days")
    print(f"  window: {m['start']} .. {m['end']} ({m['years']}y)")
    print(f"  CAGR: {m['cagr']}%   total: {m['total_ret']:+}%   max DD: {m['max_dd']}%   Calmar: {m['calmar']}")
    print(f"  avg month: {m['avg_month']}%   +months: {m['pct_pos_months']}%   worst month: {m['worst_month']}%   best: {m['best_month']}%")
    print(f"  rolling-12m: median {m['roll12_med']}%  min {m['roll12_min']}%   Sharpe: {m['sharpe']}   active days: {m['trading_days']}")
    print("  written: reports/jarvis_v1.json")


if __name__ == "__main__":
    main()
