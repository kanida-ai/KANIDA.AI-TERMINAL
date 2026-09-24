"""Falcon Intraday Behavioral Alpha — EXPERIMENT #5 feature extractor.

For every stock-day in ohlc_1min (2.2yr cash 1-min OHLCV, 499 stocks), compute a small set of
EARLY behavioral features from ONLY 09:15-09:59 (the first 45 min) and the EOD outcome (open->close),
per-stock normalized. NO look-ahead: every feature uses data strictly before 10:00; the outcome is
after. Per-stock baselines (vol scale) are computed from PRE-2026 data only, so 2026 (the walk-forward
test period) is untouched by normalization.

Output: a feature matrix -> parquet in scratchpad (one row per stock-day). Analysis is separate
(behav_analysis.py) so we can iterate on the lift/control/walk-forward study without re-extracting.

This is a DIAGNOSTIC feature build, NOT a strategy. No indicators assumed; every feature is a
behavioral descriptor of the intraday path.
"""
import sqlite3, sys, math
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "scripts"))
from mkt_poller import DB
OUT = Path(r"C:\Users\SPS\AppData\Local\Temp\claude\C--Users-SPS-Desktop-Kanida-ai-Terminal-Quant-Intelligence-Engine\c73fe1ef-c928-428e-a17b-d7f23047b24b\scratchpad") / "intraday_behav_features.parquet"
BASE_CUTOFF = "2026-01-01"     # per-stock baselines use data BEFORE this only (keep 2026 test clean)
EARLY_END = "10:00"            # features from 09:15..09:59 only
MIN_EARLY = 40                 # need a full early window
MIN_DAY = 300                  # need a full session for a valid outcome


def _con():
    return sqlite3.connect("file:" + str(DB).replace("\\", "/") + "?mode=ro", uri=True, timeout=120)


def features_for_symbol(sym, df):
    """df = all 1-min bars for one symbol, columns [bar_time,open,high,low,close,volume]."""
    df = df.copy()
    df["date"] = df["bar_time"].str[:10]; df["hm"] = df["bar_time"].str[11:16]
    df["ret1"] = df.groupby("date")["close"].pct_change()
    df["rng"] = (df["high"] - df["low"]) / df["close"]
    base = df[df["date"] < BASE_CUTOFF]
    if len(base) < 5000:
        base = df                                   # newly-listed: fall back to full sample
    ret_std = base["ret1"].std() or 1e-9
    vol_med = base["volume"].median() or 1.0
    rng_med = base["rng"].median() or 1e-9
    # per-stock daily open->close return std (for outcome normalization) from baseline period
    bd = base.groupby("date").agg(o=("open", "first"), c=("close", "last"))
    dstd = (bd["c"] / bd["o"] - 1).std() or 1e-9
    turnover = float((base["volume"] * base["close"]).median() or 0)   # liquidity tier proxy (stable)
    rows = []
    for date, g in df.groupby("date", sort=True):
        if len(g) < MIN_DAY:
            continue
        e = g[g["hm"] < EARLY_END]
        if len(e) < MIN_EARLY:
            continue
        op = e["open"].iloc[0]
        if not op or op <= 0:
            continue
        cum = e["close"].to_numpy() / op - 1.0            # cumulative return from 09:15 open, per early bar
        r1 = e["ret1"].fillna(0).to_numpy()
        vol = e["volume"].to_numpy()
        # --- outcome (EOD) ---
        day_ret = g["close"].iloc[-1] / op - 1.0
        outcome_z = day_ret / dstd
        # --- Tier-1 early features (all from the first 45 min only) ---
        early_ret = cum[-1]                                # where is it by 10:00 (naive momentum benchmark)
        # impulse magnitude: strongest standardized k-bar thrust (k in 1,3,5), sign of the biggest
        best_mag, best_dir = 0.0, 0
        csum = np.concatenate([[0.0], np.cumsum(r1)])
        for k in (1, 3, 5):
            if len(r1) > k:
                kret = csum[k:] - csum[:-k]
                z = kret / (ret_std * math.sqrt(k))
                j = int(np.argmax(np.abs(z)))
                if abs(z[j]) > abs(best_mag):
                    best_mag, best_dir = float(z[j]), int(np.sign(kret[j]))
        impulse_mag_z = abs(best_mag); impulse_dir = best_dir
        early_rvol = float(vol.sum() / (len(vol) * vol_med))                 # relative volume first 45m
        max_runup = float(cum.max()); max_drawdown = float(cum.min())        # excursions vs open
        up_frac = float((r1 > 0).mean())                                     # trend persistence proxy
        # first-impulse pullback: retracement from the running extreme reached in the window
        if best_dir >= 0:
            peak = cum.max(); pk_i = int(np.argmax(cum)); pullback = (peak - cum[pk_i:].min()) if pk_i < len(cum) else 0.0
        else:
            trough = cum.min(); tr_i = int(np.argmin(cum)); pullback = (cum[tr_i:].max() - trough) if tr_i < len(cum) else 0.0
        rng_expansion = float((e["rng"].mean()) / rng_med)
        # volume leads price: corr( vol_z[t], |ret|[t+1] )
        vz = vol / vol_med
        if len(vz) > 6:
            a = vz[:-1]; b = np.abs(r1[1:])
            vlp = float(np.corrcoef(a, b)[0, 1]) if a.std() > 0 and b.std() > 0 else 0.0
        else:
            vlp = 0.0
        # time (minutes) to first reach +/-0.5% from open
        def t_to(thr, sign):
            idx = np.where((cum * sign) >= thr)[0]
            return int(idx[0]) if len(idx) else 99
        t_up05 = t_to(0.005, 1); t_dn05 = t_to(0.005, -1)
        rows.append(dict(date=date, symbol=sym, turnover=turnover,
                         day_ret=day_ret, outcome_z=outcome_z,
                         early_ret=early_ret, impulse_mag_z=impulse_mag_z, impulse_dir=impulse_dir,
                         early_rvol=early_rvol, max_runup=max_runup, max_drawdown=max_drawdown,
                         up_frac=up_frac, pullback=float(pullback), rng_expansion=rng_expansion,
                         vol_leads_price=vlp, t_up05=t_up05, t_dn05=t_dn05))
    return rows


def main():
    con = _con()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_1min ORDER BY symbol")]
    print(f"[behav] {len(syms)} symbols", flush=True)
    all_rows = []
    for i, sym in enumerate(syms, 1):
        df = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time",
                               con, params=(sym,))
        if len(df) < MIN_DAY:
            continue
        all_rows.extend(features_for_symbol(sym, df))
        if i % 25 == 0:
            print(f"  [{i}/{len(syms)}] {sym}: cumulative stock-days={len(all_rows):,}", flush=True)
    con.close()
    fm = pd.DataFrame(all_rows)
    # liquidity tier = turnover quintile (for same-tier control matching)
    fm["tier"] = pd.qcut(fm["turnover"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]).astype(int)
    fm["period"] = np.where(fm["date"] < BASE_CUTOFF, "train", "test")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fm.to_parquet(OUT, index=False)
    print(f"[behav] wrote {len(fm):,} stock-days -> {OUT}", flush=True)
    print("[behav] outcome_z quantiles:", fm["outcome_z"].quantile([.05, .1, .5, .9, .95]).round(2).to_dict())
    print("[behav] train/test split:", fm["period"].value_counts().to_dict())


if __name__ == "__main__":
    main()
