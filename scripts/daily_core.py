"""
DAILY CORE — shared fast daily-bar foundation for the UNLEVERAGED (1x CNC) edge hunt.
Loads the 441 tradeable universe (+ NIFTY 50) adjusted daily OHLCV ONCE into a parquet cache,
then any lab (mean-reversion, momentum, gap, pairs) reads it in <1s.

Facts (verified): ohlc_daily is SPLIT/BONUS ADJUSTED (no fake -90% gaps on split dates); dividends
are NOT adjusted (small, conservative -> understates total return slightly).

CAVEAT baked in: the 441-universe = today's liquid pattern-scanned names => SURVIVORSHIP-BIASED.
Every result here is optimistic vs a true point-in-time universe. We mitigate with point-in-time
listing checks (require full lookback history) but cannot fully remove it. Flagged in all reports.

Build once:  python scripts/daily_core.py build
"""
from __future__ import annotations
import sys, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db")
CACHE = ROOT / "cache"; CACHE.mkdir(parents=True, exist_ok=True)
PANEL = CACHE / "daily_panel.parquet"          # long-form: symbol,date,o,h,l,c,v
INDEX = "NIFTY 50"

# --- honest cost model (CNC delivery, India) ---------------------------------
# STT 0.1% buy + 0.1% sell = 0.20%; exch+GST+stamp+sebi ~ 0.03%; brokerage ~0 (Zerodha CNC).
# => ~0.23% statutory round-trip. Add slippage. Base = 0.30% RT; stress = 0.50% RT.
COST_RT = 0.30
COST_RT_STRESS = 0.50


def universe():
    con = sqlite3.connect("file:" + Path(SNR).as_posix() + "?mode=ro", uri=True)
    s = sorted(set(r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()))
    con.close()
    return s


def build_panel():
    import time
    t0 = time.time()
    syms = universe()
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    qmarks = ",".join("?" * len(syms))
    df = pd.read_sql_query(
        f"SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily "
        f"WHERE symbol IN ({qmarks}) OR symbol=? ORDER BY symbol,bar_time",
        con, params=(*syms, INDEX))
    con.close()
    df["date"] = pd.to_datetime(df["bar_time"].str[:10])
    df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
    df = df[["symbol", "date", "o", "h", "l", "c", "v"]]
    # drop obviously bad rows
    df = df[(df.c > 0) & (df.o > 0) & (df.h > 0) & (df.l > 0)].reset_index(drop=True)
    df.to_parquet(PANEL, index=False)
    print(f"panel built: {df.symbol.nunique()} symbols, {len(df):,} rows, "
          f"{df.date.min().date()}..{df.date.max().date()} in {time.time()-t0:.0f}s -> {PANEL}")


def load_panel():
    if not PANEL.exists():
        build_panel()
    return pd.read_parquet(PANEL)


def wide(field="c", min_rows=260):
    """Wide matrix: index=trading dates (union), columns=symbols, values=field. Index (NIFTY 50) included."""
    df = load_panel()
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    w = df.pivot(index="date", columns="symbol", values=field).sort_index()
    # keep symbols with enough history
    good = w.columns[(w.notna().sum() >= min_rows)]
    return w[good]


def wide_all():
    """All fields as a dict of wide frames + index close series."""
    df = load_panel()
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    fields = {f: df.pivot(index="date", columns="symbol", values=f).sort_index() for f in ["o", "h", "l", "c", "v"]}
    idx = fields["c"][INDEX].copy() if INDEX in fields["c"].columns else None
    for f in fields:
        if INDEX in fields[f].columns:
            fields[f] = fields[f].drop(columns=[INDEX])
    return fields, idx


# --- shared metrics on an equity curve (business-day or event indexed) --------
def curve_metrics(eq, cap0=None, freq_per_year=252):
    eq = eq.dropna()
    if len(eq) < 3:
        return {}
    if cap0 is None:
        cap0 = eq.iloc[0]
    idx = pd.to_datetime(eq.index)
    yrs = (idx[-1] - idx[0]).days / 365.25
    total = eq.iloc[-1] / cap0 - 1
    cagr = (eq.iloc[-1] / cap0) ** (1 / yrs) - 1 if yrs > 0 and eq.iloc[-1] > 0 else -1
    peak = eq.cummax(); dd = float(((eq - peak) / peak).min())
    rets = eq.pct_change(fill_method=None).dropna()
    sh = (rets.mean() / rets.std() * np.sqrt(freq_per_year)) if rets.std() > 0 else 0
    mo = eq.resample("ME").last().pct_change(fill_method=None).dropna()
    return {"years": round(yrs, 2), "total_%": round(total * 100, 1), "CAGR_%": round(cagr * 100, 2),
            "maxDD_%": round(dd * 100, 1), "calmar": round(cagr / -dd, 2) if dd < 0 else None,
            "sharpe": round(sh, 2),
            "sharpe_m": round(mo.mean() / mo.std() * np.sqrt(12), 2) if mo.std() > 0 else 0,
            "pct_pos_mo": round((mo > 0).mean() * 100) if len(mo) else None,
            "worst_mo_%": round(mo.min() * 100, 1) if len(mo) else None,
            "start": str(idx[0].date()), "end": str(idx[-1].date())}


def slot_fund(trades, N, cap0=1_000_000.0):
    """
    Compounding CNC fund = N independent equal slots (serial sub-accounts), each starting cap0/N.
    A new trade is assigned to the earliest-free slot (fewer than N open => there is room). Each slot
    compounds its own SERIAL stream of realised trade returns; it is idle (flat, cash earns 0) between
    trades. Total equity = sum of the N slot equities. This reconciles total vs CAGR exactly and models
    a real N-book equal-weight portfolio without the cash-sizing ratchet.

    trades: iterable of (entry_date, exit_date, ret_net) with ret_net fractional net return.
    Returns (summary_dict, daily_equity_series). Intra-trade DD not marked (realised at exit) -> DD is a
    mild UNDER-estimate; acceptable for cross-strategy ranking.
    """
    tl = sorted(((pd.Timestamp(a), pd.Timestamp(b), c) for a, b, c in trades), key=lambda x: x[0])
    slot_free = [pd.Timestamp("1900-01-01")] * N     # when each slot becomes available
    slot_eq = [cap0 / N] * N                          # each slot's compounding equity
    steps = []                                        # (exit_date, delta_equity) realised increments
    steps.append((pd.Timestamp("1900-01-02"), cap0))  # seed
    taken = 0
    for ed, xd, rf in tl:
        s = min(range(N), key=lambda k: slot_free[k])
        if slot_free[s] > ed:                         # no slot free at entry -> trade skipped (capacity)
            continue
        pnl = slot_eq[s] * rf
        slot_eq[s] += pnl
        slot_free[s] = xd
        steps.append((xd, pnl))
        taken += 1
    st = pd.DataFrame(steps, columns=["date", "delta"]).groupby("date")["delta"].sum().sort_index()
    eq = st.cumsum()
    eq = eq[~eq.index.duplicated(keep="last")].resample("D").ffill().dropna()
    eq = eq[eq.index >= tl[0][0]] if tl else eq
    m = curve_metrics(eq, cap0=cap0)
    m.update({"N": N, "taken": taken, "generated": len(tl)})
    return m, eq


def reconcile(m):
    """True if total return and CAGR are mutually consistent (guards the old jarvis metrics bug)."""
    if not m or m.get("CAGR_%") is None:
        return False
    implied = (1 + m["CAGR_%"] / 100) ** m["years"] - 1
    return abs(implied - m["total_%"] / 100) < max(0.05, 0.02 * abs(m["total_%"] / 100))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        build_panel()
    else:
        p = load_panel()
        print(f"panel: {p.symbol.nunique()} symbols, {len(p):,} rows, {p.date.min().date()}..{p.date.max().date()}")
