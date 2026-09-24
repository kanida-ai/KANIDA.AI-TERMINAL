"""ROBUSTNESS v2 — tail-short (201-500) multi-year + the REAL down-phase test.
2022 excluded: the full-depth ranking only reaches ~rank 137 then (thin early pattern library) — no 201-500 tail.
Down-phase = actual MARKET corrections (universe median monthly return < -5%): 2024-10, 2025-01/02, 2026-01/03.
Q: in those market crashes, does Falcon (long) lose AND does the tail-short PROFIT (real hedge), or also lose?
Read-only; Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
LEV, FRIC = 5, 0.15; cost_d = FRIC / 100 * LEV / 20
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
df = pd.read_sql_query("SELECT signal_date, rank, nd_intraday_ret FROM falcon_full_ranking WHERE signal_date>='2023-01-01' AND nd_intraday_ret IS NOT NULL", rc)
rc.close(); df["yr"] = df.signal_date.str[:4]
# market monthly (universe median close-to-close) -> down-months
oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
od = pd.read_sql_query("SELECT trade_date, symbol, close FROM ohlc_daily WHERE trade_date>='2022-12-01'", oc); oc.close()
piv = od.pivot_table(index="trade_date", columns="symbol", values="close")
mret = piv.pct_change(fill_method=None).median(axis=1)
mm = (mret.groupby(mret.index.str[:7]).apply(lambda x: (1 + x.fillna(0)).prod() - 1) * 100)
down_months = set(mm[mm < -5].index)
def series(sub):
    f = sub[sub["rank"] <= 10].groupby("signal_date").nd_intraday_ret.mean() / 100 * LEV - cost_d
    t = (-sub[sub["rank"] >= 201].groupby("signal_date").nd_intraday_ret.mean()) / 100 * LEV - cost_d
    idx = sorted(set(f.index) & set(t.index)); return f[idx], t[idx]
def rdd(s): tot = s.sum() * 100; c = s.cumsum(); d = (c.cummax() - c).max() * 100; return tot, d, (tot / d if d else 0)

print("=" * 96 + "\nPER-YEAR (Falcon=Top-10 long 5x, tail-short=201-500 short 5x). '*' = year had a market correction month\n" + "=" * 96)
print(f"{'year':<7}{'tail edge/tr':>13}{'corr(F,tail)':>13}{'Falcon r/DD':>13}{'F+30%tail r/DD':>16}{'maxDD alone→combo':>19}")
for y in ["2023", "2024", "2025", "2026"]:
    sub = df[df.yr == y]; f, t = series(sub)
    star = "*" if any(m.startswith(y) for m in down_months) else " "
    edge = (-sub[sub["rank"] >= 201].nd_intraday_ret).mean(); corr = np.corrcoef(f, t)[0, 1]
    ft, fd, frdd = rdd(f); ct, cd, crdd = rdd(0.7 * f + 0.3 * t)
    print(f"{y+star:<7}{edge:>+12.3f}%{corr:>+13.2f}{frdd:>13.2f}{crdd:>16.2f}{f'{fd:.0f}%→{cd:.0f}%':>19}")

print("\n" + "=" * 96 + "\nDOWN-PHASE TEST — MARKET corrections (universe median month < -5%): does the tail-short HEDGE?\n" + "=" * 96)
f, t = series(df); fm = f.groupby(f.index.str[:7]).sum() * 100; tm = t.groupby(t.index.str[:7]).sum() * 100
dmask = f.index.str[:7].isin(down_months)
print(f"correlation(Falcon, tail-short) daily — ALL {np.corrcoef(f,t)[0,1]:+.2f} | market-DOWN months {np.corrcoef(f[dmask],t[dmask])[0,1]:+.2f} | UP months {np.corrcoef(f[~dmask],t[~dmask])[0,1]:+.2f}")
print(f"\n  {'market-down month':<20}{'market%':>9}{'Falcon%':>9}{'tail-short%':>13}{'hedge?':>9}")
hedged = 0; dm_in = sorted(m for m in down_months if m in fm.index)
for m in dm_in:
    hh = tm[m] > 0; hedged += hh
    print(f"  {m:<20}{mm[m]:>+9.1f}{fm[m]:>+9.1f}{tm[m]:>+13.1f}{'✓ PROFIT' if hh else '✗ also lost':>9}")
print(f"\n  tail-short profited in {hedged}/{len(dm_in)} market-correction months; avg tail-short in those months {tm[dm_in].mean():+.1f}%")
ok = (np.corrcoef(f[dmask], t[dmask])[0, 1] < 0.3) and (tm[dm_in].mean() > 0) and hedged >= len(dm_in) * 0.6
print("\nVERDICT:", "SURVIVES — hedge holds across market corrections (negative corr persists, tail-short profits when the market falls) -> OK to wire into the governed pipeline"
      if ok else "FAIR-WEATHER / MIXED — the tail-short does NOT reliably profit in market corrections; do NOT rely on it as a hedge (still a diversifier by low correlation, but not crash protection).")
