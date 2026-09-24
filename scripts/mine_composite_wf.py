"""Composite intraday signal + WALK-FORWARD validation. Legs derived from the mining:
  LONG  = clean-trend-up (earlyRet>=+0.5%, didn't dip >0.3%) AND moderate relvol 1.3-2.5x
  SHORT = exhaustion (earlyRet>+2%, relvol>5x, already rolling over >=0.5% off the early high)
Entry 10:15, hold to close. Net of 0.10% round-trip. Control = base rate.
Anti-overfit: report EACH YEAR + IS(2024-25)/OOS(2026) separately — the edge must be stable,
not just present in aggregate. Signal definitions are generic (not fitted numbers)."""
import pickle
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
CACHE = ROOT / "docs" / "ops" / "_cash_intraday_v1.pkl"
COST = 0.10  # % round-trip


def load():
    df = pickle.load(open(CACHE, "rb"))
    df["early_ret"] = df.pt / df.o915 - 1
    df["early_dip"] = df.elo / df.o915 - 1
    df["fwd_ret"] = df.eod / df.pt - 1
    df["fwd_mae"] = df.lo_after / df.pt - 1
    df["fwd_mfe"] = df.hi_after / df.pt - 1
    df["fade_from_hi"] = df.ehi / df.pt - 1
    med = df.groupby("symbol")["vol_early"].transform("median")
    df["relvol"] = df.vol_early / med.replace(0, np.nan)
    df["year"] = df.d.str[:4]
    return df.dropna(subset=["relvol", "fwd_ret"])


def stat(net):
    net = np.asarray(net)
    if len(net) == 0: return None
    w = net[net > 0]; l = net[net < 0]
    pf = w.sum() / -l.sum() if l.sum() < 0 else 99
    return dict(n=len(net), mean=net.mean(), win=(net > 0).mean() * 100, total=net.sum(),
                pf=pf, worst=net.min())


def leg_report(df, mask, direction, name):
    print(f"\n{name}  (direction={direction})")
    print(f"  {'period':<12}{'n':>7}{'mean%':>8}{'win%':>7}{'total%':>9}{'PF':>6}")
    for per, m in [("2024", df.year == "2024"), ("2025", df.year == "2025"), ("2026 (OOS)", df.year == "2026"),
                   ("IS 24-25", df.year != "2026"), ("ALL", df.d == df.d)]:
        sel = mask & m
        gross = df.loc[sel, "fwd_ret"].values * (1 if direction == "long" else -1) * 100
        net = gross - COST
        s = stat(net)
        if s:
            print(f"  {per:<12}{s['n']:>7}{s['mean']:>8.3f}{s['win']:>6.1f}%{s['total']:>9.0f}{s['pf']:>6.2f}")


def main():
    df = load()
    print(f"[*] {len(df):,} stock-days | base fwd>0 {(df.fwd_ret>0).mean()*100:.1f}% | base mean {df.fwd_ret.mean()*100:+.3f}%")

    LONG = (df.early_ret >= 0.005) & (df.early_dip > -0.003) & (df.relvol >= 1.3) & (df.relvol <= 2.5)
    SHORT = (df.early_ret > 0.02) & (df.relvol > 5) & (df.fade_from_hi >= 0.005)
    print(f"[*] LONG signals: {LONG.sum():,} ({LONG.sum()/df.d.nunique():.1f}/day)   "
          f"SHORT signals: {SHORT.sum():,} ({SHORT.sum()/df.d.nunique():.1f}/day)")

    leg_report(df, LONG, "long", "LONG LEG (clean-trend-up x moderate volume)")
    leg_report(df, SHORT, "short", "SHORT LEG (exhaustion up-climax rolling over)")

    # ---- combined DAILY strategy (equal-weight all signals that day, net) ----
    df["signal"] = 0; df.loc[LONG, "signal"] = 1; df.loc[SHORT, "signal"] = -1
    sig = df[df.signal != 0].copy()
    sig["net"] = (sig.fwd_ret * sig.signal) * 100 - COST
    daily = sig.groupby("d").agg(n=("net", "size"), dayret=("net", "mean")).reset_index()
    daily["year"] = daily.d.str[:4]
    print("\n" + "=" * 70)
    print("COMBINED DAILY STRATEGY (equal-weight long+short signals at 10:15, net):")
    print(f"  {'period':<12}{'days':>6}{'sig/day':>9}{'meanDay%':>10}{'dayWin%':>9}{'total%':>9}{'worstDay%':>11}")
    for per, m in [("2024", daily.year == "2024"), ("2025", daily.year == "2025"), ("2026 (OOS)", daily.year == "2026"),
                   ("ALL", daily.d == daily.d)]:
        d = daily[m]
        if len(d) == 0: continue
        print(f"  {per:<12}{len(d):>6}{d.n.mean():>9.1f}{d.dayret.mean():>10.3f}{(d.dayret>0).mean()*100:>8.1f}%"
              f"{d.dayret.sum():>9.0f}{d.dayret.min():>10.2f}%")
    print("\n  (meanDay% = avg of that day's signal net returns; a day with no signal is skipped)")
    print(f"  return per capital-HOUR (hold 10:15->close ~5.25h): "
          f"{daily.dayret.mean()/5.25:.3f}%/h  | @5x margin {daily.dayret.mean()/5.25*5:.2f}%/h")


if __name__ == "__main__":
    main()
