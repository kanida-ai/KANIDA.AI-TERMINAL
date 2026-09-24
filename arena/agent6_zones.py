"""ADDENDUM F — AGENT 6: Positioning Zone Engine. NO Falcon signals. Per-stock, independent behavioral memory.
Infer price ZONES from volume-at-price (a volume profile), trade the LEVEL (where), long+short.
  Zone map (per stock, trailing 60d, vol-weighted percentiles of typical price):
    VAL = 15th vol-pct, VPOC = 50th, VAH = 85th  -> zone-state = below_value / in_value / above_value.
  Two executors: LEG A long every stock 09:15->EOD, LEG B short every stock. Calibrate which zone-state favours
  which side. MANDATORY GATE: zone-filtered legs must beat the DUMB baseline (buy-all / short-all) OOS, else park.
Protocol LEARN 2022-24 -> VALIDATE 2025 -> touch-once OOS 2026. Daily-rollup zone proxy (1-min VPOC = a refinement).
Reads ohlc_daily read-only; no Falcon. Writes results pickle for the report."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
OHLC = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
FRICTION = 0.15; REFRESH = 5; WIN = 60

def vw_pcts(typ, vol):
    o = np.argsort(typ); t = typ[o]; v = vol[o]; cv = np.cumsum(v); tot = cv[-1]
    if tot <= 0: return np.nan, np.nan, np.nan
    return (np.interp(0.15 * tot, cv, t), np.interp(0.50 * tot, cv, t), np.interp(0.85 * tot, cv, t))

if __name__ == "__main__":
    oc = sqlite3.connect("file:" + OHLC.replace("\\", "/") + "?mode=ro", uri=True)
    fno = set(r[0] for r in oc.execute("SELECT symbol FROM fo_stock_master WHERE fo_eligible=1")) if \
        oc.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fo_stock_master'").fetchone() else None
    df = pd.read_sql_query("SELECT symbol, trade_date, open, high, low, close, volume FROM ohlc_daily WHERE trade_date>='2021-06-01' ORDER BY symbol, trade_date", oc)
    oc.close()
    rows = []
    for s, g in df.groupby("symbol"):
        g = g.reset_index(drop=True)
        if len(g) < WIN + 60 or np.median(g.close * g.volume) < 2e7: continue
        if fno and s not in fno: continue
        typ = ((g.high + g.low + g.close) / 3).values; vol = g.volume.values; c = g.close.values; o = g.open.values; dt = g.trade_date.values
        val = vah = vpoc = np.nan
        for i in range(WIN, len(g) - 1):
            if (i - WIN) % REFRESH == 0:
                val, vpoc, vah = vw_pcts(typ[i - WIN:i], vol[i - WIN:i])
            if not (val == val) or vah <= val: continue
            zone = "below_value" if c[i] < val else ("above_value" if c[i] > vah else "in_value")
            nd = (c[i + 1] / o[i + 1] - 1) * 100 if o[i + 1] else np.nan   # next-day 09:15->EOD
            if nd == nd:
                rows.append((dt[i], s, zone, nd))
    Z = pd.DataFrame(rows, columns=["signal_date", "symbol", "zone", "nd"])
    Z["yr"] = Z.signal_date.str[:4]
    Z["period"] = np.where(Z.yr <= "2024", "LEARN", np.where(Z.yr == "2025", "VALIDATE", "OOS"))
    ndays = {p: g.signal_date.nunique() for p, g in Z.groupby("period")}
    dumb = {p: {"LONG": g.nd.mean(), "SHORT": -g.nd.mean()} for p, g in Z.groupby("period")}
    print(f"Agent 6 Zone Engine | rows {len(Z):,} | stocks {Z.symbol.nunique()} | OOS {ndays.get('OOS')}d")
    print("DUMB baseline (buy-all / short-all, next-day %):", {p: {k: round(v, 3) for k, v in d.items()} for p, d in dumb.items()})
    print(f"\n{'zone-state':<14}{'side':<6}{'breadth/d':>10}{'LEARN':>9}{'VALID':>9}{'OOS':>9}{'OOS_net':>9}{'vs_dumb':>9}{'GATE':>7}")
    res = []
    for zone in ["below_value", "in_value", "above_value"]:
        zz = Z[Z.zone == zone]
        for side, sgn in [("LONG", 1), ("SHORT", -1)]:
            r = {"zone": zone, "side": side}
            for p in ["LEARN", "VALIDATE", "OOS"]:
                sub = zz[zz.period == p]; r[p] = (sgn * sub.nd).mean() if len(sub) else np.nan
            breadth = len(zz[zz.period == "OOS"]) / max(ndays.get("OOS", 1), 1)
            net = r["OOS"] - FRICTION; base = dumb.get("OOS", {}).get(side, 0)
            passes = (net > 0) and (r["OOS"] > base + 0.05) and (r["LEARN"] > base) and (r["VALIDATE"] > base)
            gate = "PASS" if passes else ("weak+" if net > 0 else "FAIL")
            res.append({**r, "breadth": breadth, "net": net, "vs_dumb": r["OOS"] - base, "gate": gate})
            print(f"{zone:<14}{side:<6}{breadth:>10.0f}{r['LEARN']:>+9.3f}{r['VALIDATE']:>+9.3f}{r['OOS']:>+9.3f}{net:>+9.3f}{r['OOS']-base:>+9.3f}{gate:>7}")
    R = pd.DataFrame(res)
    # monthly OOS for the passing cells
    monthly = {}
    for _, r in R[R.gate == "PASS"].iterrows():
        zz = Z[(Z.zone == r.zone) & (Z.period == "OOS")].copy(); sgn = 1 if r.side == "LONG" else -1
        zz["ym"] = zz.signal_date.str[:7]
        monthly[f"Zone-{r.zone}-{r.side}"] = (sgn * zz.groupby("ym").nd.mean()).round(3).to_dict()
    pas = R[R.gate == "PASS"]
    print(f"\nCELLS PASSING vs dumb baseline OOS: {len(pas)}/6")
    for _, r in pas.iterrows(): print(f"   {r.zone} {r.side}: OOS net {r.net:+.3f}%/trade, +{r.vs_dumb:.3f}pp vs dumb, breadth {r.breadth:.0f}/day")
    if not len(pas): print("   -> NO zone-state beats the dumb baseline OOS. Zones are hindsight lines here -> PARK (valid finding).")
    import pickle
    pickle.dump({"matrix": R.to_dict("records"), "dumb": dumb, "monthly": monthly, "ndays": ndays},
                open(os.path.join(ROOT, "arena", "agent6_results.pkl"), "wb"))
    print("\nsaved -> arena/agent6_results.pkl")
