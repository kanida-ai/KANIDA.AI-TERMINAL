"""A: exhaustion-reversal FADE (short) — refine the >5x-volume climax signal, test as a fade.
B: sequential PATH labeling — reconstruct the first-hour 1-min path, label archetypes
   (clean trend / sweep-reclaim / impulse-fade / chop / down), test forward outcome per shape.
Both hindsight-free: features from [09:15,10:15], outcome from (10:15, close]. Control = base rate."""
import sqlite3, pickle
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
CACHE = ROOT / "docs" / "ops" / "_cash_intraday_v1.pkl"


def load_cache():
    df = pickle.load(open(CACHE, "rb"))
    for c in ["early_ret", "early_range", "early_dip", "fwd_ret", "fwd_mfe", "fwd_mae"]:
        if c not in df:
            df["early_ret"] = df.pt / df.o915 - 1; df["early_dip"] = df.elo / df.o915 - 1
            df["fwd_ret"] = df.eod / df.pt - 1; df["fwd_mfe"] = df.hi_after / df.pt - 1
            df["fwd_mae"] = df.lo_after / df.pt - 1
    med = df.groupby("symbol")["vol_early"].transform("median")
    df["relvol"] = df.vol_early / med.replace(0, np.nan)
    df["fade_from_hi"] = df.ehi / df.pt - 1     # at 10:15, how far below the early high (>0 = rolling over)
    return df.dropna(subset=["relvol", "fwd_ret"])


def partA(df):
    print("=" * 78); print("A — EXHAUSTION-REVERSAL FADE (short at 10:15, profit if it falls)"); print("=" * 78)
    base_short = -df.fwd_ret.mean() * 100
    print(f"CONTROL short-all: mean {base_short:+.3f}% | P(profitable short)={ (df.fwd_ret<0).mean()*100:.1f}% | "
          f"avg favourable drop {-df.fwd_mae.mean()*100:.2f}%\n")
    print(f"{'setup':<42}{'n':>7}{'shortMean%':>11}{'P(prof)':>9}{'avgDrop%':>9}{'avgAdv%':>9}")
    def row(lab, m):
        d = df[m]
        if len(d) < 40: return
        print(f"{lab:<42}{len(d):>7}{-d.fwd_ret.mean()*100:>10.3f}%{(d.fwd_ret<0).mean()*100:>8.1f}%"
              f"{-d.fwd_mae.mean()*100:>8.2f}%{d.fwd_mfe.mean()*100:>8.2f}%")
    row("climax-up: earlyRet>+2% & relvol 3-5x", (df.early_ret > 0.02) & (df.relvol > 3) & (df.relvol <= 5))
    row("climax-up: earlyRet>+2% & relvol >5x", (df.early_ret > 0.02) & (df.relvol > 5))
    row("  + already rolling over (>=0.5% off hi)", (df.early_ret > 0.02) & (df.relvol > 5) & (df.fade_from_hi >= 0.005))
    row("extreme vol >5x, any direction", (df.relvol > 5))
    row("extreme vol >5x & early UP", (df.relvol > 5) & (df.early_ret > 0.01))
    row("extreme vol >5x & early DOWN (fade the drop=long)", (df.relvol > 5) & (df.early_ret < -0.01))
    print()


def sample_paths(df, n_dates=60, seed=42):
    rng = np.random.default_rng(seed)
    alld = sorted(df.d.unique())
    pick = sorted(rng.choice(alld, size=min(n_dates, len(alld)), replace=False).tolist())
    con = sqlite3.connect(str(DB), timeout=120); con.execute("PRAGMA query_only=1")
    frames = []
    for d in pick:
        q = ("SELECT symbol, substr(bar_time,12,5) hm, open, high, low, close, volume "
             "FROM ohlc_1min WHERE bar_time BETWEEN ? AND ? ")
        frames.append(pd.read_sql_query(q, con, params=(d + " 09:15:00", d + " 10:15:00")).assign(d=d))
    con.close()
    return pd.concat(frames, ignore_index=True)


def label_path(g):
    g = g.sort_values("hm")
    o = g.open.iloc[0]
    if o <= 0: return None
    r = (g.close.values / o - 1)
    peak, trough, end = r.max(), r.min(), r[-1]
    dr = np.diff(r)
    nchg = int((np.sign(dr[:-1]) != np.sign(dr[1:])).sum())
    swept = (trough <= -0.005) and (end > 0.002)
    faded = (peak >= 0.01) and (end < peak - 0.007)
    if end <= -0.005:
        arch = "DOWN"
    elif swept:
        arch = "SWEEP_RECLAIM"
    elif faded:
        arch = "IMPULSE_FADE"
    elif end >= 0.005 and trough > -0.003:
        arch = "CLEAN_TREND_UP"
    elif abs(end) < 0.005 and nchg >= 20:
        arch = "CHOP"
    else:
        arch = "MIXED"
    return pd.Series(dict(arch=arch, peak=peak, trough=trough, end=end, nchg=nchg))


def partB(df):
    print("=" * 78); print("B — SEQUENTIAL PATH ARCHETYPES (first-hour shape -> forward outcome)"); print("=" * 78)
    paths = sample_paths(df)
    lab = paths.groupby(["symbol", "d"]).apply(label_path).reset_index()
    m = lab.merge(df[["symbol", "d", "fwd_ret", "fwd_mfe", "fwd_mae", "relvol"]], on=["symbol", "d"], how="inner")
    base_mfe = (df.fwd_mfe >= 0.02).mean() * 100
    print(f"sample: {len(m):,} stock-days across {paths.d.nunique()} days | base P(MFE>=2%)={base_mfe:.1f}% | base fwd>0={(df.fwd_ret>0).mean()*100:.1f}%\n")
    print(f"{'archetype':<18}{'n':>7}{'%samp':>7}{'meanFwd%':>10}{'P(MFE>=2%)':>12}{'P(fwd>0)':>10}{'lift(MFE)':>10}")
    for a in ["CLEAN_TREND_UP", "SWEEP_RECLAIM", "IMPULSE_FADE", "MIXED", "CHOP", "DOWN"]:
        d = m[m.arch == a]
        if len(d) < 40: continue
        pm = (d.fwd_mfe >= 0.02).mean() * 100
        print(f"{a:<18}{len(d):>7}{len(d)/len(m)*100:>6.1f}%{d.fwd_ret.mean()*100:>9.3f}%{pm:>11.1f}%"
              f"{(d.fwd_ret>0).mean()*100:>9.1f}%{pm-base_mfe:>+9.1f}")


def main():
    df = load_cache()
    print(f"[*] cache: {len(df):,} stock-days, {df.symbol.nunique()} symbols\n")
    partA(df)
    partB(df)


if __name__ == "__main__":
    main()
