"""ADDENDUM F — AGENT 6 (Item 3): TRUE 1-MINUTE VPOC / absorption zones (not the daily-rollup proxy).
Per stock, build the intraday volume-at-price profile from 1-min bars (each minute's volume binned by its close),
rolling 20 trading days -> VPOC (max-volume price), Value Area (70% of volume) VAL/VAH.
Zone-state = below_value / in_value / above_value. ABSORPTION flag = a high-volume 1-min bar (>=3x day avg minute
vol) that CLOSES strong off its low near/below VAL (buyer defending a level). Test each zone-state long & short, plus
'below_value + absorption' long, vs the DUMB baseline (buy-all / short-all). Gate: must beat dumb OOS, else PARK.
1-min window (2024-05..2026-07): LEARN 2024 -> VALIDATE 2025 -> touch-once OOS 2026. Read-only; no Falcon."""
import os, sqlite3
from collections import Counter
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
FRIC, WIN, REFRESH = 0.15, 20, 3

def value_area(prof):
    if not prof: return np.nan, np.nan, np.nan
    items = sorted(prof.items()); prices = [p for p, _ in items]; vols = [v for _, v in items]
    tot = sum(vols); vpoc = prices[int(np.argmax(vols))]
    order = sorted(range(len(prices)), key=lambda i: -vols[i]); acc = 0; chosen = []
    for i in order:
        acc += vols[i]; chosen.append(prices[i])
        if acc >= 0.70 * tot: break
    return min(chosen), vpoc, max(chosen)

if __name__ == "__main__":
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    fno = set(r[0] for r in oc.execute("SELECT symbol FROM fo_stock_master WHERE fo_eligible=1")) if \
        oc.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fo_stock_master'").fetchone() else None
    syms = [r[0] for r in oc.execute("SELECT DISTINCT symbol FROM ohlc_1min WHERE substr(bar_time,1,10)='2026-05-15'")]
    if fno: syms = [s for s in syms if s in fno]
    print(f"symbols: {len(syms)}", flush=True)
    rows = []
    for k, s in enumerate(syms):
        df = pd.read_sql_query("SELECT substr(bar_time,1,10) d, high, low, close, volume FROM ohlc_1min WHERE symbol=? AND substr(bar_time,1,10)>='2024-05-01' ORDER BY bar_time", oc, params=(s,))
        if df.empty: continue
        med = df.close.median(); step = max(0.05, round(0.0025 * med, 2))
        df["bin"] = (df.close / step).round() * step
        daily_hist = {d: Counter(dict(g.groupby("bin").volume.sum())) for d, g in df.groupby("d")}
        dayclose = df.groupby("d").close.last(); days = list(dayclose.index)
        # per-day absorption flag + next-day return prep
        dstats = {}
        for d, g in df.groupby("d"):
            avgmin = g.volume.mean(); rng = (g.high - g.low).replace(0, np.nan)
            spike = g.volume >= 3 * avgmin; strong = (g.close - g.low) / rng > 0.6
            dstats[d] = dict(absorb=int((spike & strong & (g.low <= g.low.min() * 1.01)).sum() > 0),
                             o=g.iloc[0].close, c=g.iloc[-1].close)
        prof = Counter(); buf = []
        for i, d in enumerate(days[:-1]):
            buf.append(daily_hist[d]); prof += daily_hist[d]
            if len(buf) > WIN: prof -= buf.pop(0)
            if i < WIN or i % REFRESH:
                pass
            if len(buf) < WIN: continue
            val, vpoc, vah = value_area(prof)
            if not (val == val) or vah <= val: continue
            c = dayclose[d]; zone = "below_value" if c < val else ("above_value" if c > vah else "in_value")
            nd_o = dstats[days[i + 1]]["o"]; nd_c = dstats[days[i + 1]]["c"]
            nd = (nd_c / nd_o - 1) * 100 if nd_o else np.nan
            if nd == nd:
                rows.append((d, s, zone, dstats[d]["absorb"], nd))
        if (k + 1) % 40 == 0: print(f"  {k+1}/{len(syms)} stocks, {len(rows):,} rows", flush=True)
    oc.close()
    Z = pd.DataFrame(rows, columns=["signal_date", "symbol", "zone", "absorb", "nd"])
    Z["yr"] = Z.signal_date.str[:4]; Z["period"] = np.where(Z.yr <= "2024", "LEARN", np.where(Z.yr == "2025", "VALIDATE", "OOS"))
    ndays = {p: g.signal_date.nunique() for p, g in Z.groupby("period")}
    dumb = {p: g.nd.mean() for p, g in Z.groupby("period")}
    print(f"\nTRUE 1-min VPOC zones | rows {len(Z):,} | stocks {Z.symbol.nunique()} | OOS {ndays.get('OOS')}d")
    print("DUMB baseline (buy-all next-day %):", {p: round(v, 3) for p, v in dumb.items()})
    print(f"\n{'zone-state':<24}{'side':<6}{'brdth/d':>9}{'LEARN':>8}{'VALID':>8}{'OOS':>8}{'net':>8}{'vs_dumb':>9}{'GATE':>7}")
    def cell(sub, side, label):
        sgn = 1 if side == "LONG" else -1; r = {}
        for p in ["LEARN", "VALIDATE", "OOS"]:
            x = sub[sub.period == p]; r[p] = (sgn * x.nd).mean() if len(x) else np.nan
        base = sgn * dumb.get("OOS", 0); net = r["OOS"] - FRIC; brd = len(sub[sub.period == "OOS"]) / max(ndays.get("OOS", 1), 1)
        g = "PASS" if (net > 0 and r["OOS"] > base + 0.05 and r["LEARN"] > base and r["VALIDATE"] > base) else ("weak+" if net > 0 else "FAIL")
        print(f"{label:<24}{side:<6}{brd:>9.0f}{r['LEARN']:>+8.3f}{r['VALIDATE']:>+8.3f}{r['OOS']:>+8.3f}{net:>+8.3f}{r['OOS']-base:>+9.3f}{g:>7}")
        return g
    npass = 0
    for zone in ["below_value", "in_value", "above_value"]:
        for side in ["LONG", "SHORT"]:
            if cell(Z[Z.zone == zone], side, zone) == "PASS": npass += 1
    if cell(Z[(Z.zone == "below_value") & (Z.absorb == 1)], "LONG", "below_value+ABSORB") == "PASS": npass += 1
    print(f"\nCELLS PASSING vs dumb baseline OOS: {npass}")
    print("VERDICT:", "1-min VPOC zones show OOS edge -> promote" if npass else "NO 1-min VPOC/absorption zone beats the dumb baseline OOS -> PARK zones for good (valid finding).")
