"""Does the STRENGTH of the accumulation footprint discriminate winners from fizzlers?
For every phase-detector match, measure footprint-intensity features (dry-up depth, absorption volume at the low,
hold tightness, higher-lows slope, volume-expansion on the turn) and test whether STRONGER footprints -> bigger
forward moves, BOTH raw and BETA-NEUTRAL (forward run minus the equal-weight universe over the same window).
This sidesteps the saturated 20% bull base rate: instead of 'does the pattern beat random', it asks 'does a better
footprint mean a better outcome' — the real test of whether reading the pattern well adds value. READ-ONLY."""
import os, sqlite3
import numpy as np, pandas as pd
from scipy.stats import spearmanr
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
BASE, FWD = 75, 130
DRY, VUP, NEAR = 0.65, 1.3, 1.30

def load():
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol, trade_date, high, low, close, volume FROM ohlc_daily WHERE trade_date>='2016-01-01' ORDER BY symbol, trade_date", oc)
    oc.close(); return df

if __name__ == "__main__":
    df = load()
    # LIQUID universe only, for a fair equal-weight market benchmark (fix: earlier used median of all junk)
    liq = [s for s, g in df.groupby("symbol") if len(g) >= 300 and np.median(g.close * g.volume) >= 2e7 and np.median(g.close) >= 10]
    piv = df[df.symbol.isin(liq)].pivot_table(index="trade_date", columns="symbol", values="close")
    mkt_ret = piv.pct_change(fill_method=None).mean(axis=1)    # equal-weight LIQUID universe daily return
    mkt_cum = (1 + mkt_ret.fillna(0)).cumprod()
    dates = list(mkt_cum.index); di = {d: i for i, d in enumerate(dates)}

    rows = []
    for s, g in df.groupby("symbol"):
        g = g.reset_index(drop=True); c = g.close.values; lo = g.low.values; hi = g.high.values; v = g.volume.values.astype(float); td = g.trade_date.values
        if len(g) < 300 or np.median(c * v) < 2e7 or np.median(c) < 10: continue
        va25 = pd.Series(v).rolling(25).mean().values; last = -999
        for A in range(BASE + 130, len(g) - FWD):
            if A - last < 40 or not ("2024" <= td[A][:4] <= "2026"): continue
            b0 = A - BASE; bl = lo[b0:A].min(); bli = b0 + int(np.argmin(lo[b0:A]))
            if bli > b0 + BASE * 0.7 or lo[bli + 1:A].min() < bl * 0.96 or lo[A - 15:A].min() < bl * 1.005: continue
            baseline = np.median(v[b0 - 120:b0 - 20]); dryv = np.min(va25[b0:A])
            if not (baseline > 0 and dryv <= DRY * baseline) or v[A - 15:A].mean() < VUP * dryv or c[A] > bl * NEAR: continue
            last = A
            # intensity features
            absorb_x = v[max(bli - 5, b0):bli + 6].max() / max(dryv, 1)          # peak vol near the low / dry-up
            vol_expand = v[A - 10:A].mean() / max(dryv, 1)                        # demand appearing on the turn
            dryup_depth = dryv / baseline                                         # lower = deeper contraction
            hold_tight = (hi[bli:A].max() - bl) / bl                              # base width from the low
            hl_slope = (lo[A - 10:A].min() - lo[bli:bli + 10].min()) / bl         # higher-lows rise
            fwd = hi[A + 1:A + 1 + FWD].max() / c[A] - 1                          # peak run
            fwd_p2p = c[A + FWD] / c[A] - 1 if A + FWD < len(c) else np.nan        # point-to-point at 26wk
            if td[A] in di and di[td[A]] + FWD < len(dates):
                mret = mkt_cum.iloc[di[td[A]] + FWD] / mkt_cum.iloc[di[td[A]]] - 1  # market point-to-point
            else: mret = 0.0
            rows.append(dict(symbol=s, arm=td[A], fwd=fwd * 100, fwd_p2p=fwd_p2p * 100,
                             resid=(fwd_p2p - mret) * 100, mret=mret * 100,           # HONEST beta-neutral (p2p vs p2p)
                             absorb_x=absorb_x, vol_expand=vol_expand, dryup_depth=dryup_depth,
                             hold_tight=hold_tight, hl_slope=hl_slope))
    R = pd.DataFrame(rows).dropna(subset=["fwd_p2p"])
    print(f"matches {len(R)} | peak-run {R.fwd.mean():+.0f}% | point-to-point 26wk {R.fwd_p2p.mean():+.1f}% | market p2p {R.mret.mean():+.1f}% | BETA-NEUTRAL {R.resid.mean():+.1f}%")
    print("\n[INTENSITY vs OUTCOME] Spearman corr of each footprint feature with forward run & beta-neutral resid:")
    print(f"{'feature':<14}{'corr_fwd':>10}{'corr_resid':>12}{'dir_expected':>14}")
    exp = {"absorb_x": "+", "vol_expand": "+", "dryup_depth": "-", "hold_tight": "-", "hl_slope": "+"}
    for f in ["absorb_x", "vol_expand", "dryup_depth", "hold_tight", "hl_slope"]:
        cf = spearmanr(R[f], R.fwd).correlation; cr = spearmanr(R[f], R.resid).correlation
        print(f"{f:<14}{cf:>+10.3f}{cr:>+12.3f}{exp[f]:>14}")
    print("\n[QUARTILE TEST] outcome by ABSORPTION-strength quartile (absorb_x): does stronger absorption pay?")
    R["q"] = pd.qcut(R.absorb_x, 4, labels=["Q1-weak", "Q2", "Q3", "Q4-strong"])
    print(f"{'quartile':<11}{'n':>5}{'mean_fwd':>10}{'hit+40%':>9}{'beta-neutral':>14}")
    for q, gq in R.groupby("q"):
        print(f"{str(q):<11}{len(gq):>5}{gq.fwd.mean():>+9.0f}%{(gq.fwd>=40).mean()*100:>8.0f}%{gq.resid.mean():>+13.1f}%")
    # composite intensity score
    z = lambda x: (x - x.mean()) / x.std()
    R["score"] = z(R.absorb_x) + z(R.vol_expand) - z(R.dryup_depth) - z(R.hold_tight) + z(R.hl_slope)
    R["sq"] = pd.qcut(R.score, 4, labels=["weak", "Q2", "Q3", "strong"])
    print("\n[COMPOSITE footprint score] outcome by overall footprint strength:")
    print(f"{'tier':<9}{'n':>5}{'mean_fwd':>10}{'hit+40%':>9}{'beta-neutral':>14}")
    for q, gq in R.groupby("sq"):
        print(f"{str(q):<9}{len(gq):>5}{gq.fwd.mean():>+9.0f}%{(gq.fwd>=40).mean()*100:>8.0f}%{gq.resid.mean():>+13.1f}%")
