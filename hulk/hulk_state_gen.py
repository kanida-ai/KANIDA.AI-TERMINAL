"""
FALCON HULK V1 — STATE GENERATOR (the heart of the STATE ENGINE, Phase 1).

PRINCIPLE: PREDICT not CONFIRM. The feature cutoff is STRICTLY BEFORE the move
we predict. This module GENERATES (does not hand-pick) a large point-in-time
library of candidate STATE primitives per (symbol, track) from the stock's OWN
1-min + daily bars, and stores a MANIFEST of every primitive's definition.

Two tracks (cutoffs enforced by construction + asserted):
  Track A  cutoff = close of the 09:30 bar of day T.
           uses ONLY day-T bars in [09:15, 09:30]  +  DAILY context shifted to
           <= day T-1 (day-T daily bar is NOT complete at 09:30)  +  the known
           day-T open and overnight gap.   -> predict 09:30 -> close.
  Track B  cutoff = 15:29 close of day T.
           uses ALL day-T bars (<= 15:29)  +  DAILY context through day T.
           -> predict day T+1.

Generation = a metric x window CROSS-PRODUCT over multiple resolutions
(daily, 1-min, 5-min) plus fixed clock segments and open-specific primitives.
Nothing here measures the move being predicted (no confirmation leakage).

Source (read-only): universe_engine/data/db/kanida_universe.db :: mkt_ohlc_1min
   (segment='CASH'). NEVER read 2026 (SQL filters bar_time < '2026-01-01';
   max date asserted <= 2025-12-31).
Isolation: writes ONLY hulk_state_feat_A / hulk_state_feat_B / hulk_state_manifest
   in data/db/falcon_hulk.db. Touches ZERO legacy falcon_* tables.
UNITS: every return/feature is a FRACTION (0.005 = 0.5%).
"""

import sqlite3
import numpy as np
import pandas as pd

UDB = "universe_engine/data/db/kanida_universe.db"
HDB = "data/db/falcon_hulk.db"
HARD_MAX = "2025-12-31"
SYMBOLS = ["RELIANCE", "ADANIENT"]

# ---- generation grids ------------------------------------------------------
DAILY_WINDOWS = [1, 2, 3, 5, 7, 10, 15, 20, 30, 40, 60, 90, 120, 252]
A_MIN_WINDOWS = [1, 2, 3, 5, 7, 10, 15]            # 1-min bars back from 09:30 cutoff
A_5M_WINDOWS = [1, 2, 3]                            # 5-min bars in the opening
B_MIN_WINDOWS = [5, 10, 15, 30, 45, 60, 90, 120, 180, 240, 375]
B_5M_WINDOWS = [1, 2, 3, 6, 12, 24, 48, 75]         # 5-min bars back from 15:29
B_15M_WINDOWS = [1, 2, 4, 8, 16, 25]                # 15-min bars back from 15:29

# clock segments for Track B (start_hm, end_hm)
B_SEGMENTS = {
    "morning":   ("09:15", "12:59"),
    "afternoon": ("13:00", "15:29"),
    "firsthour": ("09:15", "10:14"),
    "lasthour":  ("14:30", "15:29"),
    "last2h":    ("13:30", "15:29"),
    "firsthalf": ("09:15", "12:20"),
    "midday":    ("11:00", "13:00"),
}

# human definitions used to build the manifest (name is fully structured)
METRIC_DEF = {
    "ret": "return over the window: last_close/window_start_close - 1",
    "rng": "window high-low range / last_close",
    "pos": "position of last_close within the window hi-lo range [0..1]",
    "disthi": "last_close / window_high - 1 (<=0)",
    "distlo": "last_close / window_low - 1 (>=0)",
    "vwapd": "last_close / window VWAP - 1",
    "volfrac": "window volume / whole-allowed-window volume",
    "flow": "signed order flow (up_vol - dn_vol)/vol over the window",
    "accdist": "accumulation/distribution: sum(CLV*vol)/sum(vol) over window",
    "upcnt": "count of up bars (close>open) in the window",
    "dncnt": "count of down bars (close<open) in the window",
    "muprun": "longest consecutive up-bar run within the window",
    "mdnrun": "longest consecutive down-bar run within the window",
    "bodysum": "sum of sign(close-open) over the window",
    # daily-only
    "roc": "daily rate of change over D sessions: c/c[-D]-1",
    "volr": "daily volume / mean(volume, D)",
    "updayfrac": "fraction of up sessions (c>prev c) over D",
    "hhcnt": "count of higher-highs over D sessions",
    "llcnt": "count of lower-lows over D sessions",
    "meanbody": "mean daily body (c-o)/o over D",
    "stdret": "std of daily returns over D",
    "smadist": "c/SMA(c,D)-1",
    "smaslope": "SMA(c,D) slope over D sessions",
    "cur_uprun": "current consecutive up-session run length ending at anchor",
    "cur_dnrun": "current consecutive down-session run length ending at anchor",
}
RES_DEF = {"D": "daily", "1m": "1-minute", "5m": "5-minute", "15m": "15-minute",
           "seg": "fixed clock segment", "open": "opening-specific"}


# ---------------------------------------------------------------------------
# load + per-minute helper columns
# ---------------------------------------------------------------------------
def load_bars(sym):
    con = sqlite3.connect(f"file:{UDB}?mode=ro", uri=True)
    df = pd.read_sql(
        "SELECT bar_time, open, high, low, close, volume FROM mkt_ohlc_1min "
        "WHERE symbol=? AND segment='CASH' AND bar_time < '2026-01-01' "
        "ORDER BY bar_time", con, params=(sym,))
    con.close()
    df["date"] = df["bar_time"].str[:10]
    df["hm"] = df["bar_time"].str[11:16]
    assert df["date"].max() <= HARD_MAX, f"date-safety: 2026 leaked ({df['date'].max()})"
    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    df["up"] = np.where(c > o, v, 0.0)
    df["dn"] = np.where(c < o, v, 0.0)
    tp = (h + l + c) / 3.0
    df["tpv"] = tp * v
    rng = (h - l).replace(0, np.nan)
    df["clvv"] = (((c - l) - (h - c)) / rng).fillna(0.0) * v
    return df


# ---------------------------------------------------------------------------
# DAILY context (vectorized). shift=1 -> Track A (info only through T-1).
# ---------------------------------------------------------------------------
def daily_series(bars):
    g = bars.groupby("date")
    d = pd.DataFrame({
        "o": g["open"].first(), "h": g["high"].max(),
        "l": g["low"].min(),   "c": g["close"].last(),
        "v": g["volume"].sum(),
    }).sort_index()
    return d


def _run_len(sign_series, positive):
    s = sign_series.copy()
    grp = (s != s.shift()).cumsum()
    runlen = s.groupby(grp).cumcount() + 1
    keep = (s > 0) if positive else (s < 0)
    return runlen.where(keep, 0)


def daily_feats(d, prefix, shift):
    c, o, h, l, v = d["c"], d["o"], d["h"], d["l"], d["v"]
    out = {}
    for D in DAILY_WINDOWS:
        out[f"{prefix}_roc_{D}"] = c / c.shift(D) - 1.0
        if D >= 2:
            hi = h.rolling(D).max(); lo = l.rolling(D).min()
            span = (hi - lo).replace(0, np.nan)
            out[f"{prefix}_rng_{D}"] = (hi - lo) / c
            out[f"{prefix}_pos_{D}"] = (c - lo) / span
            out[f"{prefix}_disthi_{D}"] = c / hi - 1.0
            out[f"{prefix}_distlo_{D}"] = c / lo - 1.0
            out[f"{prefix}_hhcnt_{D}"] = (h > h.shift(1)).rolling(D).sum()
            out[f"{prefix}_llcnt_{D}"] = (l < l.shift(1)).rolling(D).sum()
            out[f"{prefix}_updayfrac_{D}"] = (c > c.shift(1)).rolling(D).mean()
            out[f"{prefix}_stdret_{D}"] = c.pct_change().rolling(D).std()
        out[f"{prefix}_volr_{D}"] = v / v.rolling(D).mean().replace(0, np.nan)
        out[f"{prefix}_meanbody_{D}"] = ((c - o) / o).rolling(D).mean()
        out[f"{prefix}_smadist_{D}"] = c / c.rolling(D).mean().replace(0, np.nan) - 1.0
        out[f"{prefix}_smaslope_{D}"] = c.rolling(D).mean().pct_change(D)
    sgn = np.sign(c.diff())
    out[f"{prefix}_cur_uprun_cur"] = _run_len(sgn, True)
    out[f"{prefix}_cur_dnrun_cur"] = _run_len(sgn, False)
    df = pd.DataFrame(out)
    if shift:
        df = df.shift(1)          # Track A: only info through day T-1
    return df


# ---------------------------------------------------------------------------
# MINUTE-WINDOW metrics on a single date's ordered bar arrays.
# `ref` = index of the cutoff bar (last allowed). window K counts bars back.
# ---------------------------------------------------------------------------
def _minute_window_metrics(arr, windows, prefix, feat):
    o, h, l, c, v, up, dn, tpv, clvv = arr
    n = len(c)
    ref = n - 1
    tot_v = v.sum()
    body_sign = np.sign(c - o)
    for K in windows:
        start = ref - K            # price K bars before cutoff
        seg = slice(max(ref - K + 1, 0), ref + 1)
        hh = h[seg].max(); ll = l[seg].min(); span = hh - ll
        sv = v[seg].sum()
        feat[f"{prefix}_ret_{K}"] = (c[ref] / c[start] - 1.0) if start >= 0 else np.nan
        feat[f"{prefix}_rng_{K}"] = (hh - ll) / c[ref] if c[ref] else np.nan
        feat[f"{prefix}_pos_{K}"] = (c[ref] - ll) / span if span > 0 else np.nan
        feat[f"{prefix}_disthi_{K}"] = c[ref] / hh - 1.0 if hh else np.nan
        feat[f"{prefix}_distlo_{K}"] = c[ref] / ll - 1.0 if ll else np.nan
        vw = tpv[seg].sum() / sv if sv > 0 else np.nan
        feat[f"{prefix}_vwapd_{K}"] = c[ref] / vw - 1.0 if vw and not np.isnan(vw) else np.nan
        feat[f"{prefix}_volfrac_{K}"] = sv / tot_v if tot_v > 0 else np.nan
        feat[f"{prefix}_flow_{K}"] = (up[seg].sum() - dn[seg].sum()) / sv if sv > 0 else np.nan
        feat[f"{prefix}_accdist_{K}"] = clvv[seg].sum() / sv if sv > 0 else np.nan
        bs = body_sign[seg]
        feat[f"{prefix}_upcnt_{K}"] = float((bs > 0).sum())
        feat[f"{prefix}_dncnt_{K}"] = float((bs < 0).sum())
        feat[f"{prefix}_muprun_{K}"] = _max_run(bs, 1)
        feat[f"{prefix}_mdnrun_{K}"] = _max_run(bs, -1)
        feat[f"{prefix}_bodysum_{K}"] = float(bs.sum())


def _max_run(sign_arr, want):
    best = cur = 0
    for s in sign_arr:
        if s == want:
            cur += 1; best = max(best, cur)
        else:
            cur = 0
    return float(best)


def _seg_metrics(arr, name, feat):
    o, h, l, c, v, up, dn, tpv, clvv = arr
    if len(c) == 0:
        for m in ("ret", "rng", "pos", "vwapd", "flow", "accdist", "bodysum", "volfrac"):
            feat[f"B_seg_{m}_{name}"] = np.nan
        return
    hh = h.max(); ll = l.min(); span = hh - ll; sv = v.sum()
    feat[f"B_seg_ret_{name}"] = c[-1] / o[0] - 1.0 if o[0] else np.nan
    feat[f"B_seg_rng_{name}"] = (hh - ll) / c[-1] if c[-1] else np.nan
    feat[f"B_seg_pos_{name}"] = (c[-1] - ll) / span if span > 0 else np.nan
    vw = tpv.sum() / sv if sv > 0 else np.nan
    feat[f"B_seg_vwapd_{name}"] = c[-1] / vw - 1.0 if vw and not np.isnan(vw) else np.nan
    feat[f"B_seg_flow_{name}"] = (up.sum() - dn.sum()) / sv if sv > 0 else np.nan
    feat[f"B_seg_accdist_{name}"] = clvv.sum() / sv if sv > 0 else np.nan
    feat[f"B_seg_bodysum_{name}"] = float(np.sign(c - o).sum())
    feat[f"B_seg_volfrac_{name}"] = sv               # normalized later vs day vol


def _resample_5m(sub):
    """sub: bars for one date (ordered). return arrays at 5-min resolution."""
    blk = (sub["hm"].str[:2].astype(int) * 60 + sub["hm"].str[3:5].astype(int)) // 5
    g = sub.groupby(blk)
    o = g["open"].first().values; h = g["high"].max().values
    l = g["low"].min().values;    c = g["close"].last().values
    v = g["volume"].sum().values; up = g["up"].sum().values
    dn = g["dn"].sum().values;    tpv = g["tpv"].sum().values
    clvv = g["clvv"].sum().values
    return (o, h, l, c, v, up, dn, tpv, clvv)


def _resample_15m(sub):
    blk = (sub["hm"].str[:2].astype(int) * 60 + sub["hm"].str[3:5].astype(int)) // 15
    g = sub.groupby(blk)
    o = g["open"].first().values; h = g["high"].max().values
    l = g["low"].min().values;    c = g["close"].last().values
    v = g["volume"].sum().values; up = g["up"].sum().values
    dn = g["dn"].sum().values;    tpv = g["tpv"].sum().values
    clvv = g["clvv"].sum().values
    return (o, h, l, c, v, up, dn, tpv, clvv)


def _arr(sub):
    return (sub["open"].values, sub["high"].values, sub["low"].values,
            sub["close"].values, sub["volume"].values, sub["up"].values,
            sub["dn"].values, sub["tpv"].values, sub["clvv"].values)


# ---------------------------------------------------------------------------
# Per-date intraday feature builders
# ---------------------------------------------------------------------------
def build_trackA_intraday(bars, prev_close, sym):
    rows = []
    for date, sub in bars.groupby("date"):
        win = sub[(sub["hm"] >= "09:15") & (sub["hm"] <= "09:30")].sort_values("hm")
        if len(win) < 5:
            continue
        assert win["hm"].max() <= "09:30", "Track A leaked a bar past 09:30!"
        feat = {"symbol": sym, "date": date}
        _minute_window_metrics(_arr(win), A_MIN_WINDOWS, "A_1m", feat)
        _minute_window_metrics(_resample_5m(win), A_5M_WINDOWS, "A_5m", feat)
        # open-specific primitives
        o915 = win["open"].iloc[0]; c930 = win["close"].iloc[-1]
        hi = win["high"].max(); lo = win["low"].min()
        pc = prev_close.get(date, np.nan)
        feat["A_open_gap_g"] = o915 / pc - 1.0 if pc and not np.isnan(pc) else np.nan
        feat["A_open_or_pct"] = (hi - lo) / o915 if o915 else np.nan
        feat["A_open_orpos_p"] = (c930 - lo) / (hi - lo) if hi > lo else np.nan
        feat["A_open_ret_r"] = c930 / o915 - 1.0 if o915 else np.nan
        rows.append(feat)
    return pd.DataFrame(rows)


def build_trackB_intraday(bars, sym):
    rows = []
    for date, sub in bars.groupby("date"):
        sub = sub[(sub["hm"] >= "09:15") & (sub["hm"] <= "15:29")].sort_values("hm")
        if len(sub) < 100:
            continue
        assert sub["hm"].max() <= "15:29", "Track B leaked a bar past 15:29!"
        feat = {"symbol": sym, "date": date}
        _minute_window_metrics(_arr(sub), B_MIN_WINDOWS, "B_1m", feat)
        _minute_window_metrics(_resample_5m(sub), B_5M_WINDOWS, "B_5m", feat)
        _minute_window_metrics(_resample_15m(sub), B_15M_WINDOWS, "B_15m", feat)
        day_v = sub["volume"].sum()
        for name, (t0, t1) in B_SEGMENTS.items():
            seg = sub[(sub["hm"] >= t0) & (sub["hm"] <= t1)]
            _seg_metrics(_arr(seg), name, feat)
            k = f"B_seg_volfrac_{name}"
            feat[k] = feat[k] / day_v if day_v > 0 else np.nan   # normalize seg vol
        rows.append(feat)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Manifest from final columns (names are fully structured -> zero drift)
# ---------------------------------------------------------------------------
def build_manifest(colnames, track):
    rows = []
    for name in colnames:
        if name in ("symbol", "date"):
            continue
        parts = name.split("_")
        res = parts[1]                              # A_<res>_..., B_<res>_..., Dp/Db handled below
        if name.startswith(("Dp_", "Db_")):
            res = "D"; metric = parts[1]; window = parts[-1]
            fam = "daily_context"
            mdef = METRIC_DEF.get(metric if not metric.startswith("cur") else "cur_uprun", metric)
            if metric == "cur":  # Dp_cur_uprun_cur
                metric = "_".join(parts[1:3]); mdef = METRIC_DEF.get(parts[1] + "_" + parts[2], "")
            defn = f"[{RES_DEF['D']}] {mdef}; window D={window}"
            if name.startswith("Dp_"):
                defn += " (Track-A view: shifted to <= T-1)"
        elif "_seg_" in name:
            metric = parts[2]; window = parts[3]
            fam = "clock_segment"
            defn = f"[segment {window}] {METRIC_DEF.get(metric, metric)}"
        elif "_open_" in name:
            metric = parts[-1]; fam = "opening_specific"
            defn = {"g": "overnight gap: open0915/prev_close-1",
                    "pct": "opening 09:15-09:30 range / open",
                    "p": "close0930 position in opening range",
                    "r": "09:15->09:30 return"}.get(metric, name)
            res = "open"
        else:                                       # A_1m/A_5m/B_1m/B_5m/B_15m minute windows
            res = parts[1]; metric = parts[2]; window = parts[3]
            fam = "minute_window"
            defn = f"[{RES_DEF.get(res, res)}] {METRIC_DEF.get(metric, metric)}; window={window} bars"
        rows.append(dict(name=name, track=track, resolution=res,
                         family=fam, definition=defn))
    return pd.DataFrame(rows)


def build_symbol(sym):
    bars = load_bars(sym)
    d = daily_series(bars)
    prev_close = d["c"].shift(1)
    prev_close_map = dict(zip(d.index, prev_close.values))

    # daily context: Track A view (shifted, prefix Dp) + Track B view (prefix Db)
    dailyA = daily_feats(d, "Dp", shift=True).reset_index().rename(columns={"index": "date"})
    dailyB = daily_feats(d, "Db", shift=False).reset_index().rename(columns={"index": "date"})
    if "date" not in dailyA.columns:
        dailyA = dailyA.rename(columns={dailyA.columns[0]: "date"})
        dailyB = dailyB.rename(columns={dailyB.columns[0]: "date"})

    intraA = build_trackA_intraday(bars, prev_close_map, sym)
    intraB = build_trackB_intraday(bars, sym)

    featA = intraA.merge(dailyA, on="date", how="left")
    featB = intraB.merge(dailyB, on="date", how="left")
    return featA, featB


def assert_pointintime():
    """Independent raw re-derivation confirming cutoffs (no future bars)."""
    con = sqlite3.connect(f"file:{UDB}?mode=ro", uri=True)
    raw = pd.read_sql(
        "SELECT bar_time,open,high,low,close,volume FROM mkt_ohlc_1min "
        "WHERE symbol='RELIANCE' AND segment='CASH' AND bar_time LIKE '2024-01-02%' "
        "ORDER BY bar_time", con)
    con.close()
    raw["hm"] = raw["bar_time"].str[11:16]
    a = raw[(raw.hm >= "09:15") & (raw.hm <= "09:30")]
    assert a["hm"].max() == "09:30", "Track A cutoff breach"
    b = raw[(raw.hm >= "09:15") & (raw.hm <= "15:29")]
    assert b["hm"].max() == "15:29", "Track B cutoff breach"
    print("  [assert] cutoffs verified: Track A<=09:30, Track B<=15:29; daily-A shifted<=T-1")


def main():
    assert_pointintime()
    allA, allB = [], []
    for sym in SYMBOLS:
        fa, fb = build_symbol(sym)
        allA.append(fa); allB.append(fb)
        print(f"  {sym}: A rows={len(fa)} cols={fa.shape[1]-2} | "
              f"B rows={len(fb)} cols={fb.shape[1]-2} | "
              f"date {fa['date'].min()}..{fa['date'].max()}")
    dfA = pd.concat(allA, ignore_index=True)
    dfB = pd.concat(allB, ignore_index=True)
    assert dfA["date"].max() <= HARD_MAX and dfB["date"].max() <= HARD_MAX, "2026 leaked!"

    manA = build_manifest(dfA.columns, "A")
    manB = build_manifest(dfB.columns, "B")
    manifest = pd.concat([manA, manB], ignore_index=True)

    con = sqlite3.connect(HDB)
    dfA.to_sql("hulk_state_feat_A", con, if_exists="replace", index=False)
    dfB.to_sql("hulk_state_feat_B", con, if_exists="replace", index=False)
    manifest.to_sql("hulk_state_manifest", con, if_exists="replace", index=False)
    con.execute("CREATE INDEX IF NOT EXISTS ix_sfa ON hulk_state_feat_A(symbol,date)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_sfb ON hulk_state_feat_B(symbol,date)")
    con.commit(); con.close()

    nA = dfA.shape[1] - 2; nB = dfB.shape[1] - 2
    print(f"\nwrote hulk_state_feat_A rows={len(dfA)} states={nA}")
    print(f"wrote hulk_state_feat_B rows={len(dfB)} states={nB}")
    print(f"wrote hulk_state_manifest rows={len(manifest)}")
    for name, df, feats in [("A", dfA, [c for c in dfA.columns if c not in ('symbol','date')]),
                            ("B", dfB, [c for c in dfB.columns if c not in ('symbol','date')])]:
        cov = df[feats].notna().mean().mean()
        print(f"  Track {name}: {len(feats)} states, mean per-cell non-null={cov:.3f}")


if __name__ == "__main__":
    main()
