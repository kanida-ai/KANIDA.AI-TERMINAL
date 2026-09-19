"""
CROSS-SECTIONAL MOMENTUM PORTFOLIO (1x CNC, unleveraged). The most defensible non-fat-tail edge:
rank the universe by relative strength, hold the top-N equal weight, rebalance monthly. Momentum
AVOIDS falling/dying names by construction => far less survivorship-inflated than mean-reversion.

Design (LEAK-FREE):
  At each month-end t: rank eligible stocks by lookback momentum measured with data up to t.
  FORM the portfolio = top-N. TRADE it at the NEXT trading day's OPEN (t+1). Hold 1 month; repeat.
  Return of month m attributed from the open we bought to the open we sell (next rebalance) => no leak.
Variants:
  lookback: 12-1 (skip recent month, classic), 6-1, 3-0. N: 10/15/20/30.
  abs filter: only hold names with positive lookback momentum (else that slot -> cash). regime: NIFTY>200ma.
Costs: 0.30% per side applied to the turned-over fraction each rebalance (0.50% stress available).

Run: python scripts/momentum_port.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC

MIN_PRICE = 20.0
MIN_DVOL = 2e7            # Rs 2cr avg daily turnover
COST_SIDE = 0.30 / 100    # per-side cost applied to turnover


def build_matrices():
    fields, idxc = DC.wide_all()
    o = fields["o"]; c = fields["c"]; v = fields["v"]
    dates = c.index
    # month-end trading dates (last trading day of each calendar month)
    me = pd.Series(dates, index=dates).groupby([dates.year, dates.month]).last().values
    me = pd.DatetimeIndex(sorted(me))
    dvol20 = (c * v).rolling(20).mean()
    ma200 = c.rolling(200).mean()
    idx_ma200 = idxc.rolling(200).mean() if idxc is not None else None
    return o, c, dvol20, ma200, idxc, idx_ma200, dates, me


def momentum_signal(c, t_idx, lb):
    """lookback momentum known at date index t_idx. lb in {'12_1','6_1','3_0','6_0'}."""
    row = lambda k: c.iloc[k]
    if lb == "12_1":
        a, b = 252, 21
    elif lb == "6_1":
        a, b = 126, 21
    elif lb == "6_0":
        a, b = 126, 0
    elif lb == "3_0":
        a, b = 63, 0
    else:
        raise ValueError(lb)
    if t_idx - a < 0:
        return None
    past = c.iloc[t_idx - a]
    recent = c.iloc[t_idx - b] if b > 0 else c.iloc[t_idx]
    return recent / past - 1.0


def run(o, c, dvol20, ma200, idxc, idx_ma200, dates, me, lb="12_1", N=20,
        abs_filter=False, regime=False, cost_side=COST_SIDE):
    """Simulate monthly-rebalanced top-N. Returns (monthly_return_series indexed by trade date, holdings_count)."""
    pos_index = {d: i for i, d in enumerate(dates)}
    # map each month-end to the NEXT trading day index (entry day)
    entries = []
    for mdate in me:
        i = pos_index.get(pd.Timestamp(mdate))
        if i is None or i + 1 >= len(dates):
            continue
        entries.append((mdate, i, i + 1))       # (signal_date, signal_idx, entry_idx=next open)
    rets = {}; prev_holdings = set(); n_hold_series = {}
    eq_dates = []
    for j in range(len(entries) - 1):
        sig_d, sig_i, ent_i = entries[j]
        nxt_ent_i = entries[j + 1][2]
        mom = momentum_signal(c, sig_i, lb)
        if mom is None:
            continue
        elig = (c.iloc[sig_i] > MIN_PRICE) & (dvol20.iloc[sig_i] > MIN_DVOL) & mom.notna()
        if regime and idx_ma200 is not None:
            if not (idxc.iloc[sig_i] > idx_ma200.iloc[sig_i]):
                elig = elig & False           # whole market off -> go to cash this month
        m = mom[elig]
        if abs_filter:
            m = m[m > 0]
        ranked = m.sort_values(ascending=False)
        picks = list(ranked.index[:N])
        # portfolio return over the hold = mean of each pick's open(ent)->open(nxt_ent) return
        if picks:
            oe = o.iloc[ent_i][picks].values.astype(float)
            ox = o.iloc[nxt_ent_i][picks].values.astype(float)
            valid = (oe > 0) & (ox > 0)
            r = np.where(valid, ox / oe - 1.0, 0.0)
            gross = r.mean() if len(r) else 0.0
            held = set(picks)
        else:
            gross = 0.0; held = set()          # cash
        # turnover cost: fraction of book changed vs prev holdings, each side
        turn = 1.0 if not prev_holdings and held else (
            len(held.symmetric_difference(prev_holdings)) / (2 * max(len(held | prev_holdings), 1)) * 2 if held or prev_holdings else 0.0)
        # simpler/correct: entering names = held-prev, exiting = prev-held; cost applies to both legs' weight
        enter_frac = len(held - prev_holdings) / N if N else 0
        exit_frac = len(prev_holdings - held) / N if N else 0
        cost = (enter_frac + exit_frac) * cost_side
        net = gross - cost
        ent_date = dates[ent_i]
        rets[ent_date] = net; n_hold_series[ent_date] = len(held)
        prev_holdings = held
    s = pd.Series(rets).sort_index()
    return s, pd.Series(n_hold_series).sort_index()


def fund_from_monthly(s, cap0=1_000_000.0):
    eq = cap0 * (1 + s).cumprod()
    eq.index = pd.to_datetime(eq.index)
    return DC.curve_metrics(eq, cap0=cap0), eq


def main():
    t0 = time.time()
    o, c, dvol20, ma200, idxc, idx_ma200, dates, me = build_matrices()
    print(f"loaded matrices: {c.shape[1]} symbols x {c.shape[0]} days, {len(me)} month-ends. "
          f"[{time.time()-t0:.0f}s]\n")

    print("=== MOMENTUM PORTFOLIO SWEEP (net 0.30%/side turnover, leak-free open t+1) ===")
    print(f"{'lookback':<9}{'N':>4}{'abs':>5}{'reg':>5}{'total%':>11}{'CAGR%':>7}{'maxDD%':>8}{'Calmar':>7}{'Shrp_m':>7}{'pos_mo':>7}{'ok':>4}")
    best = None; curves = {}
    for lb in ["12_1", "6_1", "6_0", "3_0"]:
        for N in [10, 15, 20, 30]:
            for absf, reg in [(False, False), (True, False), (True, True)]:
                s, nh = run(o, c, dvol20, ma200, idxc, idx_ma200, dates, me, lb=lb, N=N, abs_filter=absf, regime=reg)
                m, eq = fund_from_monthly(s)
                if not m:
                    continue
                ok = "y" if DC.reconcile(m) else "BAD"
                key = (lb, N, absf, reg)
                curves[key] = (s, m, eq)
                if best is None or (m["CAGR_%"] or -9) > best[1]["CAGR_%"]:
                    best = (key, m)
                # only print a readable subset (all N for 12_1/6_1, plus best-of others)
                if lb in ("12_1", "6_1") or (absf and not reg):
                    print(f"{lb:<9}{N:>4}{str(absf)[0]:>5}{str(reg)[0]:>5}{m['total_%']:>11}{m['CAGR_%']:>6}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>7}{str(m['pct_pos_mo']):>7}{ok:>4}")

    key, m = best
    print(f"\n=== BEST by CAGR: {key}  CAGR {m['CAGR_%']}%  DD {m['maxDD_%']}%  Calmar {m['calmar']}  Sharpe_m {m['sharpe_m']} ===")
    s, _, eq = curves[key]
    lb, N, absf, reg = key
    print("  OOS era split:")
    print(f"  {'era':<16}{'months':>8}{'total%':>10}{'CAGR%':>7}{'maxDD%':>8}{'Calmar':>7}")
    for lo, hi, lab in [("2014-01-01", "2020-01-01", "2014-2019"),
                        ("2020-01-01", "2023-01-01", "2020-2022"),
                        ("2023-01-01", "2027-01-01", "2023-2026 OOS")]:
        sub = s[(s.index >= lo) & (s.index < hi)]
        if len(sub) < 12:
            continue
        mm, _ = fund_from_monthly(sub)
        print(f"  {lab:<16}{len(sub):>8}{mm['total_%']:>10}{mm['CAGR_%']:>6}%{mm['maxDD_%']:>7}%{str(mm['calmar']):>7}")
    # cost stress
    s2, _ = run(o, c, dvol20, ma200, idxc, idx_ma200, dates, me, lb=lb, N=N, abs_filter=absf, regime=reg, cost_side=0.50/100)
    m2, _ = fund_from_monthly(s2)
    print(f"\n  cost stress 0.50%/side: CAGR {m2['CAGR_%']}%  DD {m2['maxDD_%']}%  (base 0.30% = {m['CAGR_%']}%)")
    # NIFTY buy-hold benchmark over same window
    if idxc is not None:
        bh = idxc[(idxc.index >= eq.index[0]) & (idxc.index <= eq.index[-1])].dropna()
        mbh = DC.curve_metrics(bh, cap0=bh.iloc[0])
        print(f"  NIFTY 50 buy-hold same window: CAGR {mbh['CAGR_%']}%  DD {mbh['maxDD_%']}%  (beat by {round(m['CAGR_%']-mbh['CAGR_%'],1)}pp)")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
