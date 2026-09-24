"""FALCON BTST OSCILLATOR campaign — run the exact AutoTrade-panel config for Jan 2026 + Jul 2026, daily P&L.

CONFIG (as specified):
  Universe   : Falcon Top-15 -> HIGH-TIER filtered. Zero high-tier that day => NO ENTRY (never fall back).
  Entry      : split 50:50 @ 09:15 open + 09:16 -> blended cost (approx = 09:15 open; daily OHLC only).
  Product    : CNC delivery (overnight hold).
  Max hold   : 2 sessions -> buy Day-1 open, sell Day-2 close (1 overnight).
  Trail      : OFF (pure hold to exit).
  Hard stop  : -6% on blended cost (intraday low breach on Day-1 or Day-2 -> exit at -6%).
  Leverage   : 1x. Cost ~0.30% round-trip, netted.
  Capital    : Rs10L pool = 2 x Rs5L sleeves, continuous daily roll, max active Rs10L.
               -> each trading day deploys ONE Rs5L basket, equal-weighted across the day's high-tier names.

Signal REBUILT via production engine (falcon_signal_replay). JULY uses stored live-era features (leak-free,
matches UI 10/10); JANUARY uses week-to-date recomputed features (leak-free; backfill leak removed).
Read-only. -> daily log printed + Excel to Downloads.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
TOPN = 15; SLEEVE = 500000.0; POOL = 1000000.0; COST = 0.30; STOP = -6.0
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}


def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and np.isfinite(turn_pct or np.nan) and turn_pct >= 0.75: return "AVOID"
    if sret <= 2 and np.isfinite(twoday or np.nan) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and np.isfinite(rng or np.nan) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and np.isfinite(trend3_20 or np.nan) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and np.isfinite(turn_pct or np.nan) and turn_pct < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"


con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-11-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-08-15' ORDER BY symbol,trade_date", con)
con.close()

o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}; SYM = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); v = g.volume.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c / pc - 1) * 100; rng = (h - l) / pc * 100; twoday = (c / c2 - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    tp = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values): TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
    SYM[s] = dict(o=g.open.values.astype(float), h=h, l=l, c=c, idx={d: i for i, d in enumerate(g.trade_date)}, dates=list(g.trade_date))
PIT = pd.concat(rec, ignore_index=True)
FCleak = feat
FCpit = feat.drop(columns=WEEKLY).merge(PIT, on=["symbol", "trade_date"], how="left")


def high_tier_basket(fc, day):
    """Rebuild Top-15, return the high-tier names (production engine)."""
    fd = fc[fc.trade_date == day]
    if fd.empty: return []
    syms = fd.symbol.values
    X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    yr = int(day[:4]); elig = [p for p in pats if int(p["mined_year"]) < yr]
    fire = np.zeros(len(syms), np.int32); score = np.zeros(len(syms))
    for p in elig:
        m = FR.rule_mask(p["rule"], X)
        if not m.any(): continue
        fire += m.astype(np.int32); score += m.astype(np.float64) * p["oos_lift"]
    cands = [{"symbol": syms[i], "n_fires": int(fire[i]), "score": float(score[i])}
             for i in range(len(syms)) if fire[i] >= 10]
    cands.sort(key=lambda c: -c["score"])
    ranked = sorted(cands[:100], key=lambda c: -(c["score"] / max(c["n_fires"], 1)))[:TOPN]
    out = []
    for c in ranked:
        al = c["score"] / max(c["n_fires"], 1); tf = TF.get((c["symbol"], day), (np.nan,) * 5)
        if classify(tf[0], tf[2], tf[1], al, tf[3], tf[4]) in HIGH:
            out.append(c["symbol"])
    return out


def position_ret(sym, sigday):
    """buy Day-1 open, sell Day-2 close; -6% disaster stop on Day-1/Day-2 low. Net %."""
    S = SYM.get(sym); i = S["idx"].get(sigday) if S else None
    if i is None or i + 2 >= len(S["dates"]): return None, None, None
    e = S["o"][i + 1]
    if e <= 0: return None, None, None
    stop_px = e * (1 + STOP / 100.0)
    if S["l"][i + 1] <= stop_px: return STOP - COST, S["dates"][i + 1], S["dates"][i + 2]   # stop on Day-1
    if S["l"][i + 2] <= stop_px: return STOP - COST, S["dates"][i + 1], S["dates"][i + 2]   # stop on Day-2
    return (S["c"][i + 2] / e - 1) * 100 - COST, S["dates"][i + 1], S["dates"][i + 2]


months = {"January": ("2026-01-01", "2026-01-31", FCpit), "July": ("2026-07-01", "2026-07-31", FCleak)}
xl = {}
for mname, (lo, hi, fc) in months.items():
    sigdays = sorted(fc[(fc.trade_date >= lo) & (fc.trade_date <= hi)].trade_date.unique())
    print("=" * 92); print(f"  FALCON BTST OSCILLATOR  ·  {mname} 2026  ·  high-tier only · Rs5L/basket · 2-session CNC hold")
    print("=" * 92)
    print(f"  {'signal':<11}{'entry':<11}{'exit':<11}{'names':>6}{'basket%':>9}{'P&L Rs':>12}{'cum Rs':>13}   stops")
    rows = []; cum = 0.0
    for d in sigdays:
        basket = high_tier_basket(fc, d)
        if not basket:
            print(f"  {d:<11}{'—':<11}{'—':<11}{0:>6}{'NO ENTRY (0 high-tier)':>30}")
            rows.append(dict(signal_date=d, entry_date="", exit_date="", n_names=0, basket_ret_pct=None, pnl_rs=0, cum_pnl_rs=round(cum), stops=0)); continue
        rets = []; edate = xdate = ""; nstop = 0
        for s in basket:
            r, ed, xd = position_ret(s, d)
            if r is None: continue
            rets.append(r); edate, xdate = ed, xd
            if r <= STOP - COST + 1e-9: nstop += 1
        if not rets:
            rows.append(dict(signal_date=d, entry_date="", exit_date="", n_names=0, basket_ret_pct=None, pnl_rs=0, cum_pnl_rs=round(cum), stops=0)); continue
        br = float(np.mean(rets)); pnl = SLEEVE * br / 100.0; cum += pnl
        print(f"  {d:<11}{edate:<11}{xdate:<11}{len(rets):>6}{br:>+9.2f}{pnl:>+12,.0f}{cum:>+13,.0f}{('   '+str(nstop)+' stop') if nstop else ''}")
        rows.append(dict(signal_date=d, entry_date=edate, exit_date=xdate, n_names=len(rets),
                         basket_ret_pct=round(br, 3), pnl_rs=round(pnl), cum_pnl_rs=round(cum), stops=nstop))
    df = pd.DataFrame(rows); xl[mname] = df
    traded = df[df.n_names > 0]
    tot = traded.pnl_rs.sum(); wr = (traded.basket_ret_pct > 0).mean() * 100 if len(traded) else 0
    print("  " + "-" * 90)
    print(f"  baskets traded {len(traded)}/{len(sigdays)}  ·  NO-ENTRY days {int((df.n_names==0).sum())}  ·  basket win-rate {wr:.0f}%")
    print(f"  avg basket {traded.basket_ret_pct.mean():+.2f}%  ·  best {traded.basket_ret_pct.max():+.2f}%  ·  worst {traded.basket_ret_pct.min():+.2f}%  ·  stop-fires {int(traded.stops.sum())}")
    print(f"  TOTAL realized P&L  Rs{tot:+,.0f}   ·   return on Rs10L pool  {tot/POOL*100:+.2f}%   ·   avg names/basket {traded.n_names.mean():.1f}\n")

out = os.path.join(os.path.expanduser("~"), "Downloads", "FALCON_BTST_OSCILLATOR_JAN_JUL_2026.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    for mname, df in xl.items(): df.to_excel(w, f"{mname}_daily", index=False)
print(f"Excel (daily P&L, both months) -> {out}")
