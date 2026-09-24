"""APE (Accumulation Pattern Engine) — STEP 2: episodes, honest labels, detection model, precision-vs-base-rate.
Implements the operator's Addendum-C teaching. Detects institutional-accumulation footprints in the LATE BASE from
multi-timescale features (intraday-hour volume + daily/weekly/monthly), and asks the ONE honest question: of all
bases showing the footprint, what fraction actually delivered +1.5x within 8 weeks, OUT-OF-SAMPLE, vs the base rate?
Designed against the two traps (Addendum C3): (1) LABEL LEAKAGE — features strictly point-in-time (<= detection day),
label forward-looking used ONLY at train time; (2) SURVIVORSHIP — every structural base is an episode (winners AND
failures), never only the ones that rallied. Governed: champion = base rate; APE promoted only if OOS precision beats
it. Interpretable model (logistic) so every signal is explainable. READ-ONLY on data."""
import os
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
CACHE = os.path.join(ROOT, "arena", "ape_daily_features.pkl")
BASE_LEN, FWD, STEP, TARGET = 60, 40, 5, 1.5           # ~12-wk base, 8-wk forward, weekly scan, +1.5x
DIST_MAX, TIGHT_MAX = 0.35, 1.20                       # still in base (<=35% above low), base range <=120%

def episodes(T):
    rows = []
    for s, g in T.groupby("symbol"):
        g = g.reset_index(drop=True)
        if len(g) < BASE_LEN + FWD + 5: continue
        vol = g.volume.values; close = g.close.values; low = g.low.values; high = g.high.values
        vmean60 = pd.Series(vol).rolling(60).mean().values; vstd60 = pd.Series(vol).rolling(60).std().values
        for t in range(BASE_LEN, len(g) - FWD, STEP):
            b0 = t - BASE_LEN; base_low = low[b0:t].min(); base_high = high[b0:t].max()
            if base_low <= 0: continue
            dist = close[t] / base_low - 1; tight = base_high / base_low - 1
            if dist > DIST_MAX or tight > TIGHT_MAX: continue           # structural base filter (still in a tight base)
            bs = g.iloc[b0:t]
            # --- point-in-time features (data <= t only) ---
            vd = vol[t-20:t].mean() / max(vol[b0:t].mean(), 1)          # dry-up (recent vs base avg)
            ve = vol[t-5:t].mean() / max(vol[b0:t].mean(), 1)           # recent expansion
            vz = (vol[b0:t] - vmean60[b0:t]) / np.where(vstd60[b0:t] > 0, vstd60[b0:t], 1)
            cstr = bs.cstr.values; near_low = low[b0:t] < base_low * 1.05
            absorption = float(((vz > 1.5) & (cstr > 0.5) & near_low).mean())   # Dec-HFCL tell: vol spike at low, closes strong
            rmin = pd.Series(low[b0:t]).rolling(10).min().dropna().values
            hl_slope = np.polyfit(range(len(rmin)), rmin, 1)[0] / close[t] if len(rmin) > 5 else 0   # higher-lows slope
            vcp = (high[t-20:t].max() - low[t-20:t].min()) / max(base_high - base_low, 1e-9)          # range contraction
            flat = np.abs(np.diff(close[b0:t]) / close[b0:t-1]) < 0.01
            churn = float(vz[1:][flat].mean()) if flat.any() else 0.0    # turnover while flat
            rows.append(dict(symbol=s, d=g.d.iloc[t], px=close[t],
                             f_dryup=vd, f_expand=ve, f_absorption=absorption, f_hlslope=hl_slope, f_vcp=vcp,
                             f_dist=dist, f_tight=tight, f_churn=churn,
                             f_buypress=bs.up_ratio.mean(), f_cvv=bs.cvv.mean(), f_cstr=cstr.mean(),
                             f_intoclose=bs.last_hr_share.mean(), f_intoclose_rec=g.last_hr_share.values[t-20:t].mean(),
                             f_mvoltrend=vol[t-20:t].mean() / max(vol[t-60:t-40].mean(), 1),
                             label=int(close[t:t+FWD].max() >= TARGET * close[t])))   # forward label (train/eval ONLY)
    return pd.DataFrame(rows)

FCOLS = ["f_dryup", "f_expand", "f_absorption", "f_hlslope", "f_vcp", "f_dist", "f_tight", "f_churn",
         "f_buypress", "f_cvv", "f_cstr", "f_intoclose", "f_intoclose_rec", "f_mvoltrend"]

if __name__ == "__main__":
    T = pd.read_pickle(CACHE)
    print(f"loaded {len(T):,} symbol-days, {T.symbol.nunique()} symbols ({T.d.min()}..{T.d.max()})", flush=True)
    E = episodes(T).dropna(subset=FCOLS).replace([np.inf, -np.inf], np.nan).dropna(subset=FCOLS)
    E = E.sort_values("d").reset_index(drop=True)
    base_rate = E.label.mean()
    print(f"episodes: {len(E):,}  (winners {int(E.label.sum())}, failures {int((1-E.label).sum())})  BASE RATE = {base_rate:.1%}")

    # walk-forward: expanding train, test on the later OOS window
    cut = E.d.quantile(0.6) if False else sorted(E.d.unique())[int(len(E.d.unique()) * 0.6)]
    tr, te = E[E.d < cut], E[E.d >= cut]
    sc = StandardScaler().fit(tr[FCOLS])
    clf = LogisticRegression(max_iter=1000, C=0.5).fit(sc.transform(tr[FCOLS]), tr.label)
    te = te.copy(); te["p"] = clf.predict_proba(sc.transform(te[FCOLS]))[:, 1]
    te_base = te.label.mean()
    print("\n" + "=" * 92); print("APE — ACCUMULATION DETECTION (out-of-sample, winners AND failures)"); print("=" * 92)
    print(f"train {tr.d.min()}..{tr.d.max()} ({len(tr)} eps)  |  TEST {te.d.min()}..{te.d.max()} ({len(te)} eps, base rate {te_base:.1%})")
    print(f"\n{'threshold':<26}{'flagged':>8}{'precision':>11}{'base':>7}{'LIFT':>7}")
    for tag, q in [("top 30% prob", 0.70), ("top 20% prob", 0.80), ("top 10% prob", 0.90), ("top 5% prob", 0.95)]:
        thr = te.p.quantile(q); sel = te[te.p >= thr]
        if len(sel) < 5: continue
        prec = sel.label.mean()
        print(f"{tag:<26}{len(sel):>8}{prec:>10.1%}{te_base:>7.1%}{prec/max(te_base,1e-9):>6.2f}x")
    coef = sorted(zip(FCOLS, clf.coef_[0]), key=lambda x: -abs(x[1]))
    print("\nWHAT THE FOOTPRINT WEIGHS (logistic coefficients, + = predicts breakout):")
    for f, c in coef: print(f"   {f:<18}{c:+.2f}")

    # ---- governance verdict: champion = base rate ----
    top = te[te.p >= te.p.quantile(0.90)]; prec10 = top.label.mean() if len(top) else 0
    lift = prec10 / max(te_base, 1e-9)
    verdict = "PROMOTE (real signal — precision beats base rate OOS)" if (lift >= 1.5 and len(top) >= 10) else \
              ("continue-testing (edge present but thin/marginal)" if lift > 1.15 else "REJECT (no lift over base rate — footprint is hindsight noise)")
    print("\n[GOVERNANCE] champion = base rate.  APE top-decile precision", f"{prec10:.1%} vs base {te_base:.1%}  -> LIFT {lift:.2f}x")
    print("VERDICT:", verdict)
    print("\nNOTE: 2024-2026 (1-min era) = mostly bull cycle; cross-cycle validation needs deeper 1-min history. "
          "Falcon-overlay family not yet added (pos_picks = 2026 only).")
