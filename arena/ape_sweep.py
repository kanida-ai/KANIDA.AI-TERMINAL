"""APE — LIQUIDITY-SWEEP detector, tested PROPERLY (the operator's model).
SIGNAL (point-in-time): a liquidity sweep = a high-volume day that PUSHES PRICE DOWN to a new low —
    vol >= 2.5x trailing-60d avg volume  AND  makes a new 60-day low  AND  closes down.
OUTCOME: expansion = forward max close within ~26 weeks (130 trading days) >= +40% (also report +30/+50).
HONEST HARNESS (fixes every prior flaw):
  - FULL 2016-2026 daily history -> CROSS-CYCLE (bull AND bear), not one bull window.
  - WHOLE liquid universe, winners AND failed sweeps -> no survivorship of outcomes.
  - PRECISION vs BASE RATE (P(expansion|sweep) / P(expansion|any day)) with bootstrap CI.
  - BY YEAR -> does the edge hold across regimes?
  - BETA-NEUTRAL -> does the sweep predict RESIDUAL (market-demeaned) forward return?
  - SWEEP+HOLD variant -> does requiring the sweep-low to hold 20d improve it?
READ-ONLY on data."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
FWD, VOLX, LOWN = 130, 2.5, 60           # forward window (trading days), volume multiple, new-low lookback
RNG = np.random.RandomState(13)

def load():
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol, trade_date, high, low, close, volume FROM ohlc_daily "
                           "WHERE trade_date >= '2016-01-01' ORDER BY symbol, trade_date", oc)
    oc.close(); return df

def build(df):
    rows = []
    for s, g in df.groupby("symbol"):
        g = g.reset_index(drop=True)
        if len(g) < 300: continue
        c = g.close.values; lo = g.low.values; v = g.volume.values.astype(float); dt = g.trade_date.values
        turn = np.median(c * v)
        if turn < 2e7 or np.median(c) < 10: continue           # liquid + not penny
        vavg = pd.Series(v).rolling(60).mean().shift(1).values
        low60 = pd.Series(lo).rolling(LOWN).min().shift(1).values
        pc = pd.Series(c).shift(1).values
        n = len(c); fwd = np.full(n, np.nan); holdlow = np.full(n, np.nan)
        for i in range(n - FWD):
            fwd[i] = c[i + 1:i + 1 + FWD].max() / c[i] - 1
            holdlow[i] = lo[i + 1:i + 21].min()                # min low over next 20d (for the HOLD variant)
        sweep = (v >= VOLX * vavg) & (lo <= low60) & (c < pc)
        for i in range(60, n - FWD):
            if not (fwd[i] == fwd[i]): continue
            rows.append(dict(symbol=s, d=str(dt[i]), yr=str(dt[i])[:4], px=c[i], sweep=bool(sweep[i]),
                             held=bool(sweep[i] and holdlow[i] >= lo[i] * 0.97), fwd=fwd[i]))
    return pd.DataFrame(rows)

def prec(mask, E, thr):
    exp = (E.fwd >= thr)
    return exp[mask].mean() if mask.sum() else np.nan

if __name__ == "__main__":
    print("loading ohlc_daily 2016-2026 ...", flush=True)
    E = build(load())
    print(f"observations {len(E):,} | symbols {E.symbol.nunique()} | sweeps {int(E.sweep.sum()):,} ({E.sweep.mean():.2%} of days)", flush=True)
    sw = E.sweep.values; base = E[~sw]; swe = E[sw]

    print("\n[EXPANSION PRECISION] P(fwd >= thr within ~26wk)  —  sweep vs any-day base rate:")
    print(f"{'threshold':<12}{'base_rate':>10}{'sweep':>9}{'held':>9}{'LIFT_sweep':>12}{'LIFT_held':>11}")
    for thr in [0.30, 0.40, 0.50]:
        br = (E.fwd >= thr).mean(); ps = prec(E.sweep, E, thr); ph = prec(E.held, E, thr)
        print(f"+{int(thr*100)}% / 26wk{'':<2}{br:>10.1%}{ps:>9.1%}{ph:>9.1%}{ps/br:>11.2f}x{ph/br:>10.2f}x")

    # bootstrap CI on the +40% lift (resample sweep observations)
    thr = 0.40; br = (E.fwd >= thr).mean(); boot = []
    swi = np.where(sw)[0]
    for _ in range(1000):
        idx = RNG.choice(swi, len(swi)); boot.append((E.fwd.values[idx] >= thr).mean() / br)
    lo_, hi_ = np.percentile(boot, [2.5, 97.5])
    print(f"\n+40%/26wk sweep LIFT = {prec(E.sweep,E,0.40)/br:.2f}x   95% CI [{lo_:.2f}, {hi_:.2f}]  (CI>1 => real)")

    print("\n[CROSS-CYCLE] +40%/26wk precision by YEAR (does it hold in bear years too?):")
    print(f"{'year':<7}{'n_sweep':>9}{'base_rate':>10}{'sweep_prec':>11}{'lift':>7}")
    for y, gy in E.groupby("yr"):
        if gy.sweep.sum() < 15: continue
        br_y = (gy.fwd >= 0.40).mean(); ps_y = (gy[gy.sweep].fwd >= 0.40).mean()
        print(f"{y:<7}{int(gy.sweep.sum()):>9}{br_y:>10.1%}{ps_y:>11.1%}{(ps_y/br_y if br_y else float('nan')):>6.2f}x")

    print("\n[FORWARD RETURN] mean / median forward max-return (sweep vs base):")
    print(f"   sweep : mean {swe.fwd.mean():+.1%}  median {swe.fwd.median():+.1%}")
    print(f"   base  : mean {base.fwd.mean():+.1%}  median {base.fwd.median():+.1%}")

    # beta-neutral: residual = fwd minus the same-month cross-sectional mean
    E["ym"] = E.d.str[:7]; E["resid"] = E.fwd - E.groupby("ym").fwd.transform("mean")
    print("\n[BETA-NEUTRAL] residual forward return (market-demeaned):")
    print(f"   sweep residual mean {E[E.sweep].resid.mean():+.2%}  vs base {E[~E.sweep].resid.mean():+.2%}"
          f"  -> {'sweep beats market' if E[E.sweep].resid.mean() > 0 else 'no residual edge (just beta)'}")
    print("\nVERDICT: real IF sweep lift CI>1 AND it holds across years AND residual mean > 0.")
