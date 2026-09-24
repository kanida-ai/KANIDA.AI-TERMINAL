"""APE — STEP 3 (+Falcon overlay): TRADE + CAPACITY + GOVERNANCE, reported MONTHLY.
Adds the C4.4 Falcon-overlay feature family (presence frequency through the base, avg-lift level, avg-lift TREND into
the late base) from falcon_signal_day_study (2021-2026) — point-in-time (signal_date <= detection day). Turns the
accumulation probability into delivery trades (target +1.5x / structure-break / time-stop), measures P&L of EVERY flag
(winners AND ~98%), the capacity ladder (Rs1cr..50cr), and governs vs the random-base no-signal control. Output is a
MONTHLY view by entry month. Anti-lookahead + survivorship-safe. READ-ONLY on data."""
import os, sqlite3, importlib.util
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
spec = importlib.util.spec_from_file_location("ape", os.path.join(ROOT, "arena", "ape.py"))
A = importlib.util.module_from_spec(spec); spec.loader.exec_module(A)
BASE_LEN, FWD, STEP, TARGET, DIST_MAX, TIGHT_MAX = A.BASE_LEN, A.FWD, A.STEP, A.TARGET, A.DIST_MAX, A.TIGHT_MAX
DELIV_COST, ACC_DAYS = 0.004, 15
FCOLS = A.FCOLS + ["fal_presence", "fal_lift", "fal_trend"]      # + Falcon-overlay family

def load_falcon():
    c = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol, signal_date, avg_lift FROM falcon_signal_day_study WHERE avg_lift IS NOT NULL", c)
    c.close()
    FAL = {}
    for s, g in df.groupby("symbol"):
        g = g.sort_values("signal_date")
        FAL[s] = (g.signal_date.values.astype(str), g.avg_lift.values.astype(float))
    return FAL

def episodes_trade(T, FAL):
    rows = []
    for s, g in T.groupby("symbol"):
        g = g.reset_index(drop=True)
        if len(g) < BASE_LEN + FWD + 5: continue
        vol = g.volume.values; close = g.close.values; low = g.low.values; high = g.high.values; dts = g.d.values.astype(str)
        vmean60 = pd.Series(vol).rolling(60).mean().values; vstd60 = pd.Series(vol).rolling(60).std().values
        fdates, flifts = FAL.get(s, (np.array([], dtype=str), np.array([])))
        for t in range(BASE_LEN, len(g) - FWD, STEP):
            b0 = t - BASE_LEN; base_low = low[b0:t].min(); base_high = high[b0:t].max()
            if base_low <= 0: continue
            dist = close[t] / base_low - 1; tight = base_high / base_low - 1
            if dist > DIST_MAX or tight > TIGHT_MAX: continue
            bs = g.iloc[b0:t]; px = close[t]; d_det = dts[t]; d_start = dts[b0]; d_mid = dts[b0 + BASE_LEN // 2]
            vz = (vol[b0:t] - vmean60[b0:t]) / np.where(vstd60[b0:t] > 0, vstd60[b0:t], 1)
            cstr = bs.cstr.values; near_low = low[b0:t] < base_low * 1.05
            rmin = pd.Series(low[b0:t]).rolling(10).min().dropna().values
            flat = np.abs(np.diff(close[b0:t]) / close[b0:t-1]) < 0.01
            # Falcon overlay (point-in-time: signal_date in [base_start, detection])
            i0 = np.searchsorted(fdates, d_start); i1 = np.searchsorted(fdates, d_det, side="right")
            fpres = (i1 - i0) / BASE_LEN
            if i1 > i0:
                lif = flifts[i0:i1]; flift = float(lif.mean()); mid = np.searchsorted(fdates, d_mid)
                e = flifts[i0:max(mid, i0)]; la = flifts[max(mid, i0):i1]
                ftrend = (float(la.mean()) if len(la) else 0) - (float(e.mean()) if len(e) else 0)
            else: flift = ftrend = 0.0
            ret = None
            for k in range(FWD):
                if high[t + k] >= TARGET * px: ret = TARGET - 1; break
                if close[t + k] < base_low: ret = close[t + k] / px - 1; break
            if ret is None: ret = close[t + FWD - 1] / px - 1
            rows.append(dict(symbol=s, d=d_det, px=px, adv=float((close[b0:t] * vol[b0:t]).mean()),
                             fwd_ret=ret - DELIV_COST, label=int(high[t:t + FWD].max() >= TARGET * px),
                             f_dryup=vol[t-20:t].mean() / max(vol[b0:t].mean(), 1), f_expand=vol[t-5:t].mean() / max(vol[b0:t].mean(), 1),
                             f_absorption=float(((vz > 1.5) & (cstr > 0.5) & near_low).mean()),
                             f_hlslope=(np.polyfit(range(len(rmin)), rmin, 1)[0] / px if len(rmin) > 5 else 0),
                             f_vcp=(high[t-20:t].max() - low[t-20:t].min()) / max(base_high - base_low, 1e-9),
                             f_dist=dist, f_tight=tight, f_churn=(float(vz[1:][flat].mean()) if flat.any() else 0.0),
                             f_buypress=bs.up_ratio.mean(), f_cvv=bs.cvv.mean(), f_cstr=cstr.mean(),
                             f_intoclose=bs.last_hr_share.mean(), f_intoclose_rec=g.last_hr_share.values[t-20:t].mean(),
                             f_mvoltrend=vol[t-20:t].mean() / max(vol[t-60:t-40].mean(), 1),
                             fal_presence=fpres, fal_lift=flift, fal_trend=ftrend))
    return pd.DataFrame(rows)

def cap_haircut(fwd_ret, adv, pos_value):
    part = (pos_value / ACC_DAYS) / max(adv, 1); return fwd_ret - 2 * min(part, 0.5) * 0.02

if __name__ == "__main__":
    T = pd.read_pickle(A.CACHE); FAL = load_falcon()
    print(f"Falcon signal history: {len(FAL)} symbols", flush=True)
    E = episodes_trade(T, FAL).replace([np.inf, -np.inf], np.nan).dropna(subset=FCOLS + ["fwd_ret", "adv"]).sort_values("d").reset_index(drop=True)
    print(f"episodes {len(E):,} | winners {int(E.label.sum())} | base rate {E.label.mean():.1%} | Falcon present in {(E.fal_presence>0).mean():.0%} of episodes", flush=True)
    cut = sorted(E.d.unique())[int(len(E.d.unique()) * 0.6)]
    tr, te = E[E.d < cut], E[E.d >= cut].copy()
    sc = StandardScaler().fit(tr[FCOLS]); clf = LogisticRegression(max_iter=1000, C=0.5).fit(sc.transform(tr[FCOLS]), tr.label)
    te["p"] = clf.predict_proba(sc.transform(te[FCOLS]))[:, 1]
    te["ym"] = te.d.str[:7]
    thr = te.p.quantile(0.90); te["flag"] = te.p >= thr; flagged = te[te.flag]

    # ---------- OOS precision (detection) with Falcon overlay ----------
    te_base = te.label.mean(); prec10 = flagged.label.mean()
    print("\n" + "=" * 96); print("APE + FALCON-OVERLAY  (out-of-sample, winners AND failures)"); print("=" * 96)
    print(f"TEST {te.d.min()}..{te.d.max()}  | detection top-decile precision {prec10:.1%} vs base {te_base:.1%}  -> LIFT {prec10/max(te_base,1e-9):.2f}x")

    # ---------- MONTHLY VIEW of the trade P&L (by entry month) ----------
    print("\nMONTHLY VIEW — APE accumulation book vs random-base, by ENTRY month (8-wk hold, delivery 1x, net cost):")
    print(f"{'month':<9}{'APE_n':>6}{'APE_ret%':>9}{'APE_win%':>9}{'rand_ret%':>10}{'edge_pp':>8}")
    for ym in sorted(te.ym.unique()):
        ap = flagged[flagged.ym == ym]; rnd = te[te.ym == ym]
        if len(ap) == 0: continue
        ar = ap.fwd_ret.mean() * 100; rr = rnd.fwd_ret.mean() * 100
        print(f"{ym:<9}{len(ap):>6}{ar:>+9.2f}{(ap.fwd_ret>0).mean()*100:>9.0f}{rr:>+10.2f}{ar-rr:>+8.2f}")
    ape_m, rnd_m = flagged.fwd_ret.mean() * 100, te.fwd_ret.mean() * 100
    print(f"{'ALL':<9}{len(flagged):>6}{ape_m:>+9.2f}{(flagged.fwd_ret>0).mean()*100:>9.0f}{rnd_m:>+10.2f}{ape_m-rnd_m:>+8.2f}")

    # ---------- capacity ladder ----------
    print("\n[CAPACITY LADDER] APE book net mean return by capital level:")
    print(f"{'capital':>10}{'perPos_Rs':>13}{'net_mean%':>11}{'vs_random':>10}")
    for cap, tag in [(1e7, "1cr"), (5e7, "5cr"), (2e8, "20cr"), (5e8, "50cr")]:
        posval = cap / min(max(len(flagged), 1), 30)
        nm = np.mean([cap_haircut(r, a, posval) for r, a in zip(flagged.fwd_ret.values, flagged.adv.values)]) * 100
        print(f"{tag:>10}{posval:>13,.0f}{nm:>+11.2f}{nm-rnd_m:>+10.2f}")

    # ---------- did Falcon overlay help? coefficients ----------
    coef = dict(zip(FCOLS, clf.coef_[0]))
    print("\nFALCON-OVERLAY coefficients (+ = predicts breakout):",
          f"presence {coef['fal_presence']:+.2f}, lift {coef['fal_lift']:+.2f}, trend {coef['fal_trend']:+.2f}")

    # ---------- governance ----------
    edge = ape_m - rnd_m
    verdict = "PROMOTE — beats no-signal control on return AND win-rate, OOS" if (edge > 0 and (flagged.fwd_ret>0).mean() > (te.fwd_ret>0).mean() and len(flagged) >= 20) else "continue-testing / reject"
    print("\n[GOVERNANCE] champion = random-base control @ 8-wk horizon.  APE", f"{ape_m:+.2f}%/trade vs {rnd_m:+.2f}%  -> EDGE {edge:+.2f}pp")
    print("VERDICT:", verdict, "| vs Magnifier: different product (multi-week 1x, HIGH-CAPACITY), not an intraday-return contest.")
    print("\nNOTE: 2024-26 = mostly bull; episodes overlap (effective N < shown).")
