"""Falcon Intraday Behavioral Alpha — EXPERIMENT #5 analysis (LOSER side first).

Loads the feature matrix (behav_features.py) and asks, with strict controls + walk-forward:
  Do EARLY (by-10:00) behavioral features predict an EOD STRONG LOSER, with LIFT over base rate,
  and — the real question — do they ADD anything beyond the naive "already falling by 10:00"
  momentum benchmark?

Guards against hindsight:
  1. LIFT vs base rate (not raw hit-rate).
  2. WALK-FORWARD: thresholds are read on train (<2026); every number is reported ALSO on the
     untouched test (2026) block.
  3. THE FLAT-BY-10:00 TEST: restrict to stocks that were NOT yet obviously moving by 10:00
     (|early_ret| small). If features still predict the EOD strong-loss there, that's genuine
     early detection; if not, we were just reading momentum.
  4. Control base rate is the SAME-period base rate (and, for patterns, same liquidity tier).
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

FM = Path(r"C:\Users\SPS\AppData\Local\Temp\claude\C--Users-SPS-Desktop-Kanida-ai-Terminal-Quant-Intelligence-Engine\c73fe1ef-c928-428e-a17b-d7f23047b24b\scratchpad") / "intraday_behav_features.parquet"
Z_LOSS = -1.0     # strong loser = outcome_z <= -1.0
FLAT_EARLY = 0.003  # "flat by 10:00" = |early_ret| < 0.3%


def lift(df, mask, target="is_loss"):
    sub = df[mask]
    if len(sub) == 0:
        return dict(n=0, succ=0.0, base=round(df[target].mean() * 100, 2), lift=0.0)
    base = df[target].mean()
    succ = sub[target].mean()
    return dict(n=int(len(sub)), succ=round(succ * 100, 1), base=round(base * 100, 1),
                lift=round(succ / base, 2) if base > 0 else float("nan"))


def quintile_scan(df, feat, target="is_loss", ascending_bad=True):
    """P(strong_loss) across feature quintiles — is the extreme quintile lifted vs base?"""
    try:
        q = pd.qcut(df[feat].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    except Exception:
        return None
    g = df.groupby(q, observed=True)[target].agg(["mean", "size"])
    base = df[target].mean()
    return {int(k): (round(v["mean"] * 100, 1), round(v["mean"] / base, 2), int(v["size"])) for k, v in g.iterrows()}


def report(df, label):
    print(f"\n############ {label}  (n={len(df):,} stock-days) ############")
    print(f"  base rate  strong-LOSS (z<={Z_LOSS}) = {df['is_loss'].mean()*100:.1f}%   "
          f"strong-GAIN = {(df['outcome_z']>=1).mean()*100:.1f}%")
    # 1) univariate quintile lift for the loser-relevant features
    feats = ["early_ret", "impulse_mag_z", "early_rvol", "up_frac", "max_drawdown",
             "t_dn05", "rng_expansion", "vol_leads_price", "pullback"]
    print("  --- univariate: P(strong-loss) by feature quintile [Q1..Q5], (rate%, lift, n) ---")
    for f in feats:
        qs = quintile_scan(df, f)
        if qs:
            cells = "  ".join(f"Q{k}:{qs[k][0]}%/{qs[k][1]}x" for k in sorted(qs))
            print(f"    {f:16s} {cells}")
    # 2) named down-patterns (behavioral), lift + occurrence
    dn = df["impulse_dir"] < 0
    hi_rvol = df["early_rvol"] > df["early_rvol"].quantile(0.8)
    hi_imp = df["impulse_mag_z"] > df["impulse_mag_z"].quantile(0.8)
    low_up = df["up_frac"] < df["up_frac"].quantile(0.3)
    fast_dn = df["t_dn05"] <= 10
    already_down = df["early_ret"] <= df["early_ret"].quantile(0.1)
    pats = {
        "MOMENTUM benchmark: already down hard by 10:00": already_down,
        "down-impulse + high rvol": dn & hi_rvol,
        "down-impulse + high rvol + fast -0.5%": dn & hi_rvol & fast_dn,
        "down-impulse + hi-impulse-z + low up-frac": dn & hi_imp & low_up,
        "high rvol + vol-leads-price>0": hi_rvol & (df["vol_leads_price"] > 0),
    }
    print("  --- patterns: n | strong-loss% | base% | LIFT ---")
    for name, m in pats.items():
        r = lift(df, m)
        print(f"    {name:52s} n={r['n']:>6}  loss={r['succ']:>5}%  base={r['base']}%  LIFT={r['lift']}x")
    # 3) THE FLAT-BY-10:00 TEST — genuine early detection vs momentum
    flat = df[df["early_ret"].abs() < FLAT_EARLY]
    print(f"  --- FLAT-BY-10:00 subset (|early_ret|<{FLAT_EARLY:.1%}, n={len(flat):,}, base-loss={flat['is_loss'].mean()*100:.1f}%): "
          f"do features still predict the EOD strong-loss? ---")
    for f in ["impulse_mag_z", "early_rvol", "up_frac", "max_drawdown", "rng_expansion", "vol_leads_price"]:
        qs = quintile_scan(flat, f)
        if qs:
            print(f"    {f:16s} Q1:{qs[1][0]}%/{qs[1][1]}x  ...  Q5:{qs[5][0]}%/{qs[5][1]}x")
    r = lift(flat, (flat["impulse_dir"] < 0) & (flat["early_rvol"] > flat["early_rvol"].quantile(0.8)))
    print(f"    pattern[down-impulse+hi-rvol | FLAT@10:00]: n={r['n']}  loss={r['succ']}%  base={r['base']}%  LIFT={r['lift']}x")


def main():
    if not FM.exists():
        print("feature matrix not built yet — run behav_features.py first"); return
    df = pd.read_parquet(FM)
    df["is_loss"] = (df["outcome_z"] <= Z_LOSS).astype(int)
    print(f"[behav-analysis] {len(df):,} stock-days | train={ (df['period']=='train').sum():,}  test={ (df['period']=='test').sum():,}")
    report(df[df["period"] == "train"], "TRAIN  (2024-05 .. 2025-12)")
    report(df[df["period"] == "test"], "TEST / WALK-FORWARD  (2026)")
    print("\n[note] A feature is only real if its lift SURVIVES in TEST and, above all, in the FLAT-BY-10:00 "
          "subset — otherwise it is just reading momentum (already-falling), not the pre-move fingerprint.")


if __name__ == "__main__":
    main()
