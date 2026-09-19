"""
LEO OVERNIGHT hot-sector agent (1x CNC) — built on the SPLIT/BONUS-ADJUSTED DAILY panel (clean), NOT the
raw 1-min cache (which had unadjusted corp-action gaps that corrupted overnight returns).

Thesis (proven by leo_intraday_diag): the drift is OVERNIGHT (+0.30%/day) while the intraday session
bleeds. So at the CLOSE, rank sectors by TODAY's move, buy the strongest names in the leading sector(s),
capture the overnight (and multi-day) move.

Leak-free: signal = today's return (known at close t); ENTER close t; EXIT next open (overnight) or
close t+N. Costs 0.23% round-trip (CNC STT+charges). Safety: drop any |overnight|>25% (residual bad print).
Benchmarks: buy-ALL overnight, buy-ALL hold-N. OOS era split. Survivorship caveat: same 441 universe.

Run: python scripts/leo_overnight.py
"""
from __future__ import annotations
import sys, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC
KDB = str(ROOT / "db" / "kanida.db")
COST_RT = 0.23 / 100
MIN_PRICE = 20.0; MIN_DVOL = 2e7


def build_long():
    fields, _ = DC.wide_all()
    o, c, v = fields["o"], fields["c"], fields["v"]
    dvol = (c * v).rolling(20).mean()
    nxt_o = o.shift(-1)
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    lab = pd.read_sql_query("SELECT symbol,sector FROM instrument_labels", con); con.close()
    sec = lab.dropna(subset=["sector"]).set_index("symbol")["sector"]
    # long frame
    L = pd.DataFrame({
        "ret_day": (c / o - 1.0).stack(),
        "overnight": (nxt_o / c - 1.0).stack(),
        "c": c.stack(), "dvol": dvol.stack(),
    }).reset_index()
    L.columns = ["date", "symbol", "ret_day", "overnight", "c", "dvol"]
    L["sector"] = L["symbol"].map(sec)
    L = L.dropna(subset=["sector", "ret_day"])
    L = L[(L["c"] > MIN_PRICE) & (L["dvol"] > MIN_DVOL)]              # liquid, tradeable
    L["day"] = L["date"].dt.strftime("%Y%m%d").astype(int)
    # sector rank by median day-return
    sm = L.groupby(["day", "sector"])["ret_day"].agg(["median", "size"])
    sm = sm[sm["size"] >= 3].reset_index()
    sm["srank"] = sm.groupby("day")["median"].rank(ascending=False, method="first")
    L = L.merge(sm[["day", "sector", "srank"]], on=["day", "sector"], how="inner")
    return L, c, o


def metrics(eq):
    eq = eq.sort_index()
    idx = pd.to_datetime(eq.index.astype(int).astype(str), format="%Y%m%d"); eq.index = idx
    yrs = (idx[-1] - idx[0]).days / 365.25
    cagr = eq.iloc[-1] ** (1 / yrs) - 1 if yrs > 0 and eq.iloc[-1] > 0 else -1
    peak = eq.cummax(); dd = float(((eq - peak) / peak).min())
    r = eq.pct_change(fill_method=None).dropna()
    sh = r.mean() / r.std() * np.sqrt(252) if r.std() > 0 else 0
    return {"CAGR_%": round(cagr * 100, 1), "maxDD_%": round(dd * 100, 1),
            "calmar": round(cagr / -dd, 2) if dd < 0 else None, "sharpe": round(sh, 2), "yrs": round(yrs, 1)}


def overnight_series(L, topsec=1, nstock=3, cost=COST_RT):
    d = L[L["overnight"].abs() <= 0.25]                               # safety clip on residual bad prints
    lead = d[d["srank"] <= topsec].copy()
    lead["prank"] = lead.groupby("day")["ret_day"].rank(ascending=False, method="first")
    picks = lead[lead["prank"] <= nstock]
    daily = picks.groupby("day")["overnight"].mean() - cost
    return daily, (1 + daily).cumprod()


def holdN_series(L, c, N, topsec=1, nstock=3, cost=COST_RT):
    days = np.sort(L["day"].unique()); entry = days[::N]
    cl = c.copy(); cl.index = cl.index.strftime("%Y%m%d").astype(int)
    ret = {}
    for i in range(len(entry) - 1):
        t, te = entry[i], entry[i + 1]
        rows = L[L["day"] == t]
        lead = rows[rows["srank"] <= topsec].copy()
        lead["prank"] = lead["ret_day"].rank(ascending=False, method="first")
        picks = lead[lead["prank"] <= nstock]["symbol"].tolist()
        if not picks or t not in cl.index or te not in cl.index:
            continue
        ce = cl.loc[t, picks].astype(float); cx = cl.loc[te, picks].astype(float)
        val = (ce > 0) & (cx > 0)
        ret[te] = float(np.nanmean(np.where(val, cx.values / ce.values - 1.0, np.nan))) - cost
    s = pd.Series(ret).dropna().sort_index()
    return s, (1 + s).cumprod()


def bench_overnight_all(L, cost=COST_RT):
    d = L[L["overnight"].abs() <= 0.25]
    daily = d.groupby("day")["overnight"].mean() - cost
    return daily, (1 + daily).cumprod()


def main():
    L, c, o = build_long()
    # sanity: raw overnight drift on clean data
    allov = L[L["overnight"].abs() <= 0.25]["overnight"]
    print(f"clean overnight drift (liquid universe): {allov.mean()*100:+.3f}%/night gross "
          f"(x250 ~ {allov.mean()*250*100:+.0f}%/yr), median {allov.median()*100:+.3f}%\n")

    print("=== LEO OVERNIGHT hot-sector (buy close -> sell next open), net 0.23% RT ===")
    print(f"  {'variant':<26}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}{'Shrp':>6}")
    best = None
    for topsec in [1, 2, 3]:
        for nstock in [2, 3, 5]:
            daily, eq = overnight_series(L, topsec, nstock)
            m = metrics(eq)
            if best is None or (m["CAGR_%"] or -9) > best[0]["CAGR_%"]:
                best = (m, topsec, nstock, daily)
            if nstock == 3:
                print(f"  {'overnight top'+str(topsec)+'sec 3stk':<26}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe']:>6}")
    _, beq = bench_overnight_all(L); mb = metrics(beq)
    print(f"  {'BENCH overnight buy-all':<26}{mb['CAGR_%']:>7}%{mb['maxDD_%']:>7}%{str(mb['calmar']):>7}{mb['sharpe']:>6}")

    print("\n=== MULTI-DAY hold (buy close -> sell close+N, rebalance every N days), net 0.23% RT ===")
    print(f"  {'variant':<26}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}{'Shrp':>6}")
    for N in [2, 3, 5, 10]:
        s, eq = holdN_series(L, c, N, topsec=1, nstock=3)
        m = metrics(eq)
        print(f"  {'hold '+str(N)+'d top1sec 3stk':<26}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe']:>6}")

    m, topsec, nstock, daily = best
    print(f"\n=== BEST overnight: top{topsec}sec {nstock}stk  CAGR {m['CAGR_%']}%  DD {m['maxDD_%']}%  "
          f"Calmar {m['calmar']}  Sharpe {m['sharpe']} ===")
    print(f"  avg net {daily.mean()*100:+.3f}%/night, {(daily>0).mean()*100:.0f}% nights +, {len(daily)} nights traded")
    print("  OOS era split:")
    print(f"  {'era':<16}{'nights':>7}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}{'avgNight%':>10}")
    for lo, hi, lab in [(20150101, 20200101, "2015-2019"), (20200101, 20230101, "2020-2022"), (20230101, 20270101, "2023-2026 OOS")]:
        sub = daily[(daily.index >= lo) & (daily.index < hi)]
        if len(sub) > 100:
            mm = metrics((1 + sub).cumprod())
            print(f"  {lab:<16}{len(sub):>7}{mm['CAGR_%']:>7}%{mm['maxDD_%']:>7}%{str(mm['calmar']):>7}{sub.mean()*100:>+9.3f}%")


if __name__ == "__main__":
    main()
