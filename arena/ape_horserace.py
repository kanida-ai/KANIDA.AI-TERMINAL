"""APE — decisive test, PART B: the CASE-CONTROL HORSE-RACE.
The single load-bearing question: do 1-MINUTE microstructure EVENTS carry accumulation signal that daily data does
NOT? Everything in the APE thesis rides on this. Harness (per the multi-agent synthesis, all traps designed out):
  - EPISODE-LEVEL, de-overlapped (one base per stock per non-overlapping 40-day block) -> ~independent episodes.
  - CASE-CONTROL: winners (+1.5x/8wk) vs matched controls (same month x price-band, did NOT break out).
  - INCREMENTAL AUC: logistic DAILY-only vs DAILY+MICRO on a PURGED/EMBARGOED temporal holdout -> does micro ADD AUC?
  - BETA/SECTOR-NEUTRAL: residualize forward return within (month x sector); does micro predict the RESIDUAL?
  - Bootstrap CIs on the AUC delta.  VERDICT: build the full architecture only if micro beats daily, beta-neutral.
READ-ONLY on data."""
import os, sqlite3
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
BASE_LEN, FWD, STEP, TARGET, DIST_MAX, TIGHT_MAX = 60, 40, 40, 1.5, 0.35, 1.20   # STEP=40 -> non-overlapping episodes
RNG = np.random.RandomState(11)
DAILY_COLS = ["dryup", "vcp", "dist", "tight", "hlslope", "buypress", "cvv", "cstr", "intoclose", "mvoltrend"]
MICRO_COLS = ["absorb_total", "absorb_density", "absorb_trend", "late_imb_mean", "delta_mean", "spike_total"]

def sectors():
    c = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    m = {r[0]: (r[1] or "NA") for r in c.execute("SELECT symbol, sector FROM universe_master")}; c.close(); return m

def build_episodes():
    D = pd.read_pickle(os.path.join(ROOT, "arena", "ape_daily_features.pkl"))
    Mi = pd.read_pickle(os.path.join(ROOT, "arena", "ape_micro_features.pkl"))
    M = D.merge(Mi, on=["symbol", "d"], how="left")
    for c in ["n_absorb", "n_spike", "late_imb", "delta"]: M[c] = M[c].fillna(0.0)
    SEC = sectors(); rows = []
    for s, g in M.groupby("symbol"):
        g = g.reset_index(drop=True)
        if len(g) < BASE_LEN + FWD + 5: continue
        close = g.close.values; low = g.low.values; high = g.high.values; vol = g.volume.values
        nab = g.n_absorb.values; nsp = g.n_spike.values; li = g.late_imb.values; dl = g.delta.values
        for t in range(BASE_LEN, len(g) - FWD, STEP):
            b0 = t - BASE_LEN; blow = low[b0:t].min(); bhigh = high[b0:t].max()
            if blow <= 0: continue
            dist = close[t] / blow - 1; tight = bhigh / blow - 1
            if dist > DIST_MAX or tight > TIGHT_MAX: continue
            bs = g.iloc[b0:t]; rmin = pd.Series(low[b0:t]).rolling(10).min().dropna().values
            rows.append(dict(symbol=s, d=g.d.iloc[t], sector=SEC.get(s, "NA"), px=close[t],
                             label=int(high[t:t+FWD].max() >= TARGET * close[t]),
                             fwd_ret=close[t:t+FWD].max() / close[t] - 1,
                             # daily-only footprint
                             dryup=vol[t-20:t].mean() / max(vol[b0:t].mean(), 1), vcp=(high[t-20:t].max()-low[t-20:t].min())/max(bhigh-blow,1e-9),
                             dist=dist, tight=tight, hlslope=(np.polyfit(range(len(rmin)), rmin, 1)[0]/close[t] if len(rmin) > 5 else 0),
                             buypress=bs.up_ratio.mean(), cvv=bs.cvv.mean(), cstr=bs.cstr.mean(),
                             intoclose=bs.last_hr_share.mean(), mvoltrend=vol[t-20:t].mean()/max(vol[t-60:t-40].mean(), 1),
                             # 1-min microstructure EVENTS aggregated over the base
                             absorb_total=nab[b0:t].sum(), absorb_density=nab[b0:t].mean(),
                             absorb_trend=nab[t-20:t].mean() - nab[b0:b0+20].mean(),
                             late_imb_mean=li[b0:t].mean(), delta_mean=dl[b0:t].mean(), spike_total=nsp[b0:t].sum()))
    return pd.DataFrame(rows).replace([np.inf, -np.inf], np.nan).dropna(subset=DAILY_COLS + MICRO_COLS).sort_values("d").reset_index(drop=True)

def auc_split(E, cols):
    cut = sorted(E.d.unique())[int(len(E.d.unique()) * 0.6)]
    emb = pd.Timestamp(cut) - pd.Timedelta(days=60)                      # embargo: drop train episodes within 60d of cut
    tr = E[E.d < emb.strftime("%Y-%m-%d")]; te = E[E.d >= cut]
    if te.label.sum() < 5 or tr.label.sum() < 5: return None
    sc = StandardScaler().fit(tr[cols]); clf = LogisticRegression(max_iter=1000, class_weight="balanced").fit(sc.transform(tr[cols]), tr.label)
    p = clf.predict_proba(sc.transform(te[cols]))[:, 1]
    return te.label.values, p

if __name__ == "__main__":
    E = build_episodes()
    print(f"de-overlapped episodes: {len(E):,}  | winners {int(E.label.sum())} ({E.label.mean():.1%}) | {E.d.min()}..{E.d.max()}")

    # ---------- TEST 1: case-control (winners vs matched controls, same month x price-decile) ----------
    E["ym"] = E.d.str[:7]; E["pxb"] = pd.qcut(E.px, 10, labels=False, duplicates="drop")
    win = E[E.label == 1]; ctrl_pool = E[E.label == 0]
    matched = []
    for _, w in win.iterrows():
        c = ctrl_pool[(ctrl_pool.ym == w.ym) & (ctrl_pool.pxb == w.pxb)]
        if len(c): matched.append(c.sample(min(3, len(c)), random_state=RNG))
    ctrl = pd.concat(matched) if matched else ctrl_pool
    print("\n[TEST 1] CASE-CONTROL (median micro-event level, winners vs matched controls):")
    print(f"{'feature':<16}{'winners':>10}{'controls':>10}{'ratio':>8}")
    for f in ["absorb_density", "absorb_trend", "late_imb_mean", "delta_mean", "spike_total"]:
        wv, cv = win[f].median(), ctrl[f].median()
        print(f"{f:<16}{wv:>10.4f}{cv:>10.4f}{(wv/cv if cv else float('nan')):>8.2f}")

    # ---------- TEST 2: incremental AUC (daily-only vs daily+micro, purged holdout) ----------
    rd = auc_split(E, DAILY_COLS); rb = auc_split(E, DAILY_COLS + MICRO_COLS); rm = auc_split(E, MICRO_COLS)
    print("\n[TEST 2] INCREMENTAL AUC on purged/embargoed holdout (episode-level):")
    if rd and rb:
        y, pd_ = rd; _, pb = rb; _, pm = rm
        ad, ab, am = roc_auc_score(y, pd_), roc_auc_score(y, pb), roc_auc_score(y, pm)
        boot = []
        for _ in range(1000):
            idx = RNG.randint(0, len(y), len(y))
            if y[idx].sum() == 0 or y[idx].sum() == len(idx): continue
            boot.append(roc_auc_score(y[idx], pb[idx]) - roc_auc_score(y[idx], pd_[idx]))
        lo, hi = np.percentile(boot, [2.5, 97.5])
        print(f"   AUC  daily-only {ad:.3f} | micro-only {am:.3f} | daily+micro {ab:.3f}")
        print(f"   INCREMENTAL AUC (micro over daily) = {ab-ad:+.3f}   95% CI [{lo:+.3f}, {hi:+.3f}]")
        verdict_auc = "ADDS signal (CI excludes 0)" if lo > 0 else "does NOT add signal (CI includes 0)"
        print(f"   -> micro {verdict_auc}")
    else:
        verdict_auc = "insufficient"; print("   insufficient winners in holdout")

    # ---------- TEST 3: beta/sector-neutral residual — does micro predict RESIDUAL forward return? ----------
    E["resid"] = E.fwd_ret - E.groupby(["ym", "sector"]).fwd_ret.transform("mean")
    print("\n[TEST 3] BETA/SECTOR-NEUTRAL — correlation of micro events with RESIDUAL fwd return (Spearman):")
    for f in ["absorb_density", "absorb_trend", "late_imb_mean", "delta_mean"]:
        c = E[[f, "resid"]].corr(method="spearman").iloc[0, 1]
        print(f"   {f:<16}{c:+.3f}")

    print("\n" + "=" * 90)
    print("VERDICT: micro-over-daily on the purged holdout ->", verdict_auc)
    print("If micro adds AUC (CI>0) AND shows residual (beta-neutral) predictivity -> the 1-min edge is REAL -> build")
    print("the full per-stock/event/sequence architecture. If not -> the accumulation thesis is dead at this resolution.")
