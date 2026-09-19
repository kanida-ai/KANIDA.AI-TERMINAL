"""
MR DIAGNOSTICS + HONESTY CHECKS. Is the mean-reversion CAGR real or an artifact?
  1) trade-return distribution + left tail (MR should have a LEFT tail, not a right fat tail).
  2) profit concentration (is it a few lucky recoveries? => fragile / survivorship-driven).
  3) SURVIVORSHIP probe: split edge by whether the name is a big long-run winner vs laggard.
  4) risk controls that are NOT overfit: index-regime gate (NIFTY>200ma) + per-trade hard stop.
Run: python scripts/mr_diag.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC, mr_lab as M


def sim_trades_gated(D, kind, exit_rule, idx_ok, hard_stop=None, cost_rt=DC.COST_RT):
    """Like mr_lab.sim_trades but with (a) index-regime gate at entry, (b) optional intra-trade hard stop.
    idx_ok: dict date->bool (NIFTY above 200ma on the signal day). Returns (entry,exit,ret,hold,symbol)."""
    out = []
    for sym, df in D.items():
        sig = M.signal(df, kind) & (df["c"] > df["ma200"])
        idx = np.where(sig.values)[0]
        c = df["c"].values; o = df["o"].values; l = df["l"].values; ma5 = df["ma5"].values
        dates = df["date"].values; n = len(c); last_exit = -1
        for k in idx:
            if k <= last_exit or k + 1 >= n or not (o[k + 1] > 0):
                continue
            if idx_ok is not None and not idx_ok.get(pd.Timestamp(dates[k]), True):
                continue
            entry = o[k + 1]; stop = entry * (1 - hard_stop / 100) if hard_stop else None
            end = min(k + 1 + M.MAXHOLD, n); xd = None; expx = None
            for d in range(k + 1, end):
                if stop is not None and l[d] <= stop:      # intra-trade hard stop (fills at stop)
                    xd = d; expx = stop; break
                hit = False
                if exit_rule == "ma5x":
                    hit = c[d] > ma5[d]
                elif exit_rule == "tgt4":
                    hit = c[d] >= entry * 1.04 or d - k >= 10
                if hit or d == end - 1:
                    xd = d; expx = c[d]; break
            if xd is None:
                continue
            out.append((dates[k + 1], dates[xd], expx / entry - 1 - cost_rt / 100, xd - k, sym))
    return out


def main():
    t0 = time.time()
    panel = DC.load_panel()
    idxc = panel[panel.symbol == DC.INDEX].set_index("date")["c"].sort_index()
    idx_ma200 = idxc.rolling(200).mean()
    idx_ok = (idxc > idx_ma200)
    idx_ok_map = {pd.Timestamp(d): bool(v) for d, v in idx_ok.items()}
    panel = panel[panel.symbol != DC.INDEX]
    D = {s: M.prep(g.sort_values("date").reset_index(drop=True)) for s, g in panel.groupby("symbol", sort=False)}

    # long-run winner classification for survivorship probe (full-sample total return per symbol)
    tot = {s: (df["c"].values[-1] / df["c"].values[0] - 1) for s, df in D.items() if len(df) > 250}
    med_tot = np.median(list(tot.values()))
    print(f"universe long-run median total return = {med_tot*100:.0f}%  (survivor universe: all still liquid today)\n")

    for kind, ex in [("down3", "tgt4"), ("rsi2_5", "ma5x")]:
        tr = M.sim_trades(D, kind, ex)
        r = np.array([t[2] for t in tr])
        # need symbol per trade -> recompute with symbol
        trs = sim_trades_gated(D, kind, ex, None)
        rr = np.array([t[2] for t in trs]); syms = [t[4] for t in trs]
        print(f"=== {kind}/{ex}  n={len(rr)}  avg {rr.mean()*100:+.3f}%  median {np.median(rr)*100:+.3f}% ===")
        print(f"  tails: p01 {np.percentile(rr,1)*100:+.1f}%  p05 {np.percentile(rr,5)*100:+.1f}%  "
              f"p95 {np.percentile(rr,95)*100:+.1f}%  p99 {np.percentile(rr,99)*100:+.1f}%  worst {rr.min()*100:+.1f}%")
        # profit concentration
        pos = np.sort(rr[rr > 0])[::-1]; tot_profit = pos.sum()
        top5 = pos[:max(1, len(pos)//20)].sum()
        print(f"  profit concentration: top 5% of winners = {top5/tot_profit*100:.0f}% of gross profit "
              f"(breakout was 56%; lower = more robust)")
        # survivorship probe: edge among long-run WINNER names vs LAGGARD names
        wr = np.array([tot.get(s, 0) > med_tot for s in syms])
        print(f"  edge in long-run WINNERS {rr[wr].mean()*100:+.3f}% (n={wr.sum()})  vs LAGGARDS "
              f"{rr[~wr].mean()*100:+.3f}% (n={(~wr).sum()})  <- if only winners have edge => survivorship-driven")

    print("\n=== RISK CONTROLS (down3/tgt4, N=20): index-regime gate + hard stop ===")
    print(f"  {'variant':<28}{'taken':>7}{'total%':>10}{'CAGR%':>7}{'maxDD%':>8}{'Calmar':>7}{'Shrp':>6}")
    variants = [
        ("baseline", None, None),
        ("+NIFTY>200ma gate", idx_ok_map, None),
        ("+hard stop 8%", None, 8.0),
        ("+gate +hard stop 8%", idx_ok_map, 8.0),
        ("+hard stop 12%", None, 12.0),
    ]
    for name, gate, hs in variants:
        trs = sim_trades_gated(D, "down3", "tgt4", gate, hard_stop=hs)
        m, _ = DC.slot_fund([(a, b, c) for a, b, c, _, _ in trs], 20)
        print(f"  {name:<28}{m['taken']:>7}{m['total_%']:>10}{m['CAGR_%']:>6}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>6}")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
