"""
LEO + VOL-SIZING. Momentum crashes when its OWN volatility spikes (Barroso & Santa-Clara). Since Leo is
unleveraged (exposure capped at 100%, idle cash earns 0), vol-sizing can only ever DE-RISK. We test:

  base       : always 100% invested (plain Leo).
  voltarget  : exposure_t = min(1, target_vol / realized_vol_{t-1}); realized_vol = trailing-6mo annualized
               std of Leo's monthly returns, shifted 1 month (causal / leak-free).
  vix_scale  : exposure_t = f(INDIA VIX percentile at t-1); cut exposure when fear is elevated.
  combo      : min(voltarget, vix_scale).

All overlays are applied to Leo's realised monthly return stream (net_t = exposure_t * leo_t), exposure
decided from info available BEFORE month t. We report CAGR / DD / Calmar / Sharpe / avg-exposure and the
OOS era split. Goal: cut the -30% momentum-crash drawdown while keeping return -> higher Calmar.

Run: python scripts/leo_volsize.py
"""
from __future__ import annotations
import sys, sqlite3, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC, leo as LEO
KDB = str(ROOT / "db" / "kanida.db")


def vix_percentile_series():
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT bar_time,close FROM ohlc_daily WHERE symbol='INDIA VIX' ORDER BY bar_time", con)
    con.close()
    df["date"] = pd.to_datetime(df["bar_time"].str[:10])
    vix = df.drop_duplicates("date").set_index("date")["close"].astype(float)
    return vix.rolling(252).apply(lambda w: (w[-1] >= w).mean() * 100, raw=True)   # 0..100


def apply_overlay(leo_m, exposure):
    exposure = exposure.reindex(leo_m.index).clip(0, 1)
    net = leo_m * exposure
    eq = 1e6 * (1 + net).cumprod()
    m = DC.curve_metrics(eq, cap0=1e6)
    m["avg_exp"] = round(float(exposure.mean()), 2)
    return m, net, exposure


def met_of(net):
    eq = 1e6 * (1 + net).cumprod(); return DC.curve_metrics(eq, cap0=1e6)


def main():
    t0 = time.time()
    o, c, dvol20, sec = LEO.load()
    cfg = dict(lb=126, freq="M", topk=3, nper=12)          # Leo's best config
    leo_m = LEO.run(o, c, dvol20, sec, **cfg)              # monthly return stream
    base = met_of(leo_m)

    # realized-vol targeting (causal): trailing 6-mo annualized std, shifted 1
    rv = leo_m.rolling(6).std().shift(1) * np.sqrt(12)
    # VIX percentile at each rebalance date (shifted 1 month -> use last month's fear)
    vixpct = vix_percentile_series().reindex(leo_m.index, method="ffill").shift(1)

    print("=== LEO + VOL-SIZING (Leo best config 126d/M/top3/12) — leak-free overlays ===")
    print(f"  {'variant':<22}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}{'Shrp':>6}{'avgExp':>7}")
    results = {}
    # base
    print(f"  {'base (always 100%)':<22}{base['CAGR_%']:>7}%{base['maxDD_%']:>7}%{str(base['calmar']):>7}{base['sharpe_m']:>6}{'1.0':>7}")
    results["base"] = (base, leo_m, pd.Series(1.0, index=leo_m.index))
    # vol-target sweep
    ann_vol = (leo_m.std() * np.sqrt(12))
    for tv in [0.15, 0.20, 0.25, 0.30]:
        exp = (tv / rv).clip(upper=1.0)
        exp = exp.fillna(1.0)
        m, net, e = apply_overlay(leo_m, exp)
        results[f"voltarget {int(tv*100)}%"] = (m, net, e)
        print(f"  {'voltarget '+str(int(tv*100))+'%':<22}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>6}{m['avg_exp']:>7}")
    # vix scaling: exposure = clip((100 - vixpct)/40, floor, 1) -> full when calm, down as fear rises
    for floor in [0.3, 0.5]:
        exp = ((100 - vixpct) / 40).clip(lower=floor, upper=1.0).fillna(1.0)
        m, net, e = apply_overlay(leo_m, exp)
        results[f"vix_scale f{floor}"] = (m, net, e)
        print(f"  {'vix_scale floor '+str(floor):<22}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>6}{m['avg_exp']:>7}")
    # combo: min(best voltarget, vix)
    exp_vt = (0.20 / rv).clip(upper=1.0).fillna(1.0)
    exp_vx = ((100 - vixpct) / 40).clip(lower=0.3, upper=1.0).fillna(1.0)
    exp_c = pd.concat([exp_vt, exp_vx], axis=1).min(axis=1)
    m, net, e = apply_overlay(leo_m, exp_c)
    results["combo (vt20+vix)"] = (m, net, e)
    print(f"  {'combo (vt20+vix)':<22}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>6}{m['avg_exp']:>7}")

    # pick best by Calmar
    best = max(results.items(), key=lambda kv: (kv[1][0].get("calmar") or -9))
    bname, (bm, bnet, bexp) = best
    print(f"\n=== BEST by Calmar: '{bname}'  CAGR {bm['CAGR_%']}%  DD {bm['maxDD_%']}%  Calmar {bm['calmar']}  "
          f"(base was CAGR {base['CAGR_%']}% DD {base['maxDD_%']}% Calmar {base['calmar']}) ===")
    print("  OOS era split (best overlay vs base):")
    print(f"  {'era':<16}{'base CAGR/DD':>18}{'sized CAGR/DD':>20}")
    for lo, hi, lab in [("2014-01-01", "2020-01-01", "2014-2019"), ("2020-01-01", "2023-01-01", "2020-2022"),
                        ("2023-01-01", "2027-01-01", "2023-2026 OOS")]:
        b = leo_m[(leo_m.index >= lo) & (leo_m.index < hi)]
        s = bnet[(bnet.index >= lo) & (bnet.index < hi)]
        if len(b) >= 8:
            mb, ms = met_of(b), met_of(s)
            print(f"  {lab:<16}{str(mb['CAGR_%'])+'% / '+str(mb['maxDD_%'])+'%':>18}"
                  f"{str(ms['CAGR_%'])+'% / '+str(ms['maxDD_%'])+'%':>20}")
    # current exposure signal
    cur_exp = bexp.dropna().iloc[-1] if len(bexp.dropna()) else 1.0
    print(f"\n  Leo's CURRENT vol-sized exposure = {cur_exp*100:.0f}% invested "
          f"(realized_vol={rv.dropna().iloc[-1]*100:.0f}%, VIX pct={vixpct.dropna().iloc[-1]:.0f})")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
