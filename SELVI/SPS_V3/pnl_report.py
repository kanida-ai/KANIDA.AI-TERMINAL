"""
Trader-language P&L view of the vault-confirmed basket over Jan-2026 -> today
(the sealed out-of-sample period). Daily + monthly + month-over-month, capital
deployed, trades/day, returns. At 1x and 5x. Modeled costs (0.13%) — pre real
slippage (that's the next test).

Run:  PYTHONIOENCODING=utf-8 python pnl_report.py
"""
import json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
import finalize
from finalize import load_min, load_atr, day_pack, VAULT

HERE = Path(__file__).resolve().parent
CAP = 30_000.0

port = json.load(open(HERE / "portfolio.json"))
syms = port["list"]
con = sqlite3.connect(str(HERE / "scan_results.db"))
specs, thrs = {}, {}
for s in syms:
    row = con.execute("SELECT spec, thr FROM champ WHERE sym=?", (s,)).fetchone()
    specs[s] = json.loads(row[0]); thrs[s] = row[1]
con.close()

# per-stock per-date net return on the sealed vault
rows = []
for s in syms:
    atr = load_atr(s)
    vault = day_pack(load_min(s, *VAULT, unlock=True))
    ser = finalize.sim_series(s, vault, atr, specs[s], thrs[s])
    for d, net in ser.items():
        rows.append({"date": d, "sym": s, "net": net})
df = pd.DataFrame(rows)
df["date"] = pd.to_datetime(df["date"])
df["month"] = df["date"].dt.strftime("%Y-%m")
df["pnl_1x"] = CAP * df["net"]              # Rs P&L per trade, 1x
df["pnl_5x"] = CAP * 5 * df["net"]          # Rs P&L per trade, 5x (MIS margin=CAP, notional=5*CAP)

# ---- daily aggregation ----
daily = df.groupby("date").agg(trades=("net", "size"),
                               stocks=("sym", "nunique"),
                               pnl_1x=("pnl_1x", "sum"),
                               pnl_5x=("pnl_5x", "sum")).reset_index()
daily["deployed"] = CAP * daily["stocks"]                    # capital at work that day (margin)
daily["ret_deployed_1x"] = daily["pnl_1x"] / daily["deployed"]
daily["ret_deployed_5x"] = daily["pnl_5x"] / daily["deployed"]
n_days = len(daily); total_alloc = CAP * len(syms)

# equity / drawdown (1x and 5x, on Rs P&L)
def dd(series):
    eq = series.cumsum(); return (eq - eq.cummax()).min()

# ---- monthly ----
mon = df.groupby("month").agg(trades=("net", "size"),
                              pnl_1x=("pnl_1x", "sum"),
                              pnl_5x=("pnl_5x", "sum")).reset_index()
mdays = daily.groupby(daily["date"].dt.strftime("%Y-%m")).size()
mon["days"] = mon["month"].map(mdays)
mon["trades_per_day"] = (mon["trades"] / mon["days"]).round(1)
# monthly return on avg deployed
mon_dep = daily.groupby(daily["date"].dt.strftime("%Y-%m"))["deployed"].mean()
mon["avg_deployed"] = mon["month"].map(mon_dep)
mon["ret_1x_pct"] = mon["pnl_1x"] / mon["avg_deployed"] / mon["days"] * 100   # avg %/day on deployed
mon["ret_5x_pct"] = mon["pnl_5x"] / mon["avg_deployed"] / mon["days"] * 100

L = []
def p(s): L.append(s)
p("=" * 78)
p("SELVI ORBlate BASKET — P&L VIEW  (sealed OOS: Jan-2026 → 2026-07-29)")
p(f"basket {len(syms)} stocks · Rs{CAP:,.0f}/sleeve · total allocated Rs{total_alloc:,.0f} · modeled cost 0.13% (PRE real slippage)")
p("=" * 78)
p("\n--- HEADLINE ---")
p(f"trading days           : {n_days}")
p(f"total trades           : {int(daily['trades'].sum()):,}  ({daily['trades'].mean():.1f}/day avg, range {int(daily['trades'].min())}-{int(daily['trades'].max())})")
p(f"avg stocks live/day    : {daily['stocks'].mean():.1f} of {len(syms)}  (capital deployed ~Rs{daily['deployed'].mean():,.0f}/day, {daily['deployed'].mean()/total_alloc*100:.0f}% of allocated)")
p(f"win-day rate           : {(daily['pnl_1x']>0).mean()*100:.1f}%")
p("")
p(f"{'':22}{'1x (unlevered)':>20}{'5x (MIS)':>20}")
p(f"{'total P&L':22}{('Rs'+format(daily['pnl_1x'].sum(),',.0f')):>20}{('Rs'+format(daily['pnl_5x'].sum(),',.0f')):>20}")
p(f"{'avg P&L / day':22}{('Rs'+format(daily['pnl_1x'].mean(),',.0f')):>20}{('Rs'+format(daily['pnl_5x'].mean(),',.0f')):>20}")
p(f"{'avg return/day (dep.)':22}{format(daily['ret_deployed_1x'].mean()*100,'+.3f')+'%':>20}{format(daily['ret_deployed_5x'].mean()*100,'+.3f')+'%':>20}")
p(f"{'best day':22}{('Rs'+format(daily['pnl_1x'].max(),',.0f')):>20}{('Rs'+format(daily['pnl_5x'].max(),',.0f')):>20}")
p(f"{'worst day':22}{('Rs'+format(daily['pnl_1x'].min(),',.0f')):>20}{('Rs'+format(daily['pnl_5x'].min(),',.0f')):>20}")
p(f"{'max drawdown (Rs)':22}{('Rs'+format(dd(daily.set_index('date')['pnl_1x']),',.0f')):>20}{('Rs'+format(dd(daily.set_index('date')['pnl_5x']),',.0f')):>20}")
p(f"{'return on ALLOCATED':22}{format(daily['pnl_1x'].sum()/total_alloc*100,'+.1f')+'%':>20}{format(daily['pnl_5x'].sum()/total_alloc*100,'+.1f')+'%':>20}")

p("\n--- MONTHLY (month-over-month) ---")
p(f"{'month':8}{'days':>5}{'trades':>8}{'t/day':>7}{'P&L 1x':>12}{'P&L 5x':>13}{'ret/day 1x':>12}{'ret/day 5x':>12}")
prev = None
for _, r in mon.iterrows():
    mom = "" if prev is None else f"  (MoM {((r['pnl_1x']-prev)/abs(prev)*100):+.0f}%)" if prev != 0 else ""
    p(f"{r['month']:8}{int(r['days']):>5}{int(r['trades']):>8}{r['trades_per_day']:>7}"
      f"{('Rs'+format(r['pnl_1x'],',.0f')):>12}{('Rs'+format(r['pnl_5x'],',.0f')):>13}"
      f"{format(r['ret_1x_pct'],'+.3f')+'%':>12}{format(r['ret_5x_pct'],'+.3f')+'%':>12}{mom}")
    prev = r["pnl_1x"]

out = "\n".join(L)
(HERE / "pnl_report.txt").write_text(out, encoding="utf-8")
print(out)
