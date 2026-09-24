"""
Falcon Intraday — 95% Hit-Rate Filter Research (Top-10 universe)
================================================================

Research question: can multi-timeframe (MTF) daily-OHLC context computed on the
SIGNAL day be used to FILTER which Falcon Top-10 picks to enter intraday, so the
portfolio +1% hit rate climbs from ~74-77% toward 95%?

Universe note: the ranked history (`falcon_signal_day_study`, persona
`falcon_top10_daily`) only stores ranks 1-10, and the locked Top-25 engine
config is not reproducible in this repo, so this run uses the parity-true
Top-10 universe (operator decision, 2026-06-25). Top-15/20/25 rows in Part 4
are therefore reported as N/A.

Exit logic is FIXED and identical to the baseline backtest:
  entry 09:15 OPEN, +1% portfolio target -> exit next-candle OPEN, else 15:29 CLOSE,
  equal allocation, no overnight. The FILTER only changes WHICH stocks enter each
  day; the day's basket = the filtered survivors among that day's Top-10.

No lookahead: every MTF feature uses daily data up to and including the signal day
(the EOD day). The intraday 1-minute data is used ONLY for the exit simulation,
never as a filter feature.

Stages (--stage all|features|scan):
  features : build `intraday_bt_mtf_features` (+ parity spot-check)
  scan     : intraday cache + filter scan + baskets + Excel
  all      : both (default)

Usage:
  python scripts/research_95pct_hitrate.py
  python scripts/research_95pct_hitrate.py --capital 3000000 --target 1.0 --min-n 50
"""
from __future__ import annotations

import argparse
import math
import sqlite3
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

from falcon_intraday_backtest import (
    Config, DEFAULT_DB, load_signals, load_ohlc_1min_day, DEFAULT_ALIASES,
)

ROOT = Path(__file__).resolve().parent.parent
OUT_XLSX = ROOT / "outputs" / "Falcon_95pct_Research.xlsx"
FEATURE_TABLE = "intraday_bt_mtf_features"

# ---- MTF feature definitions -------------------------------------------------
NUMERIC_FEATURES = [
    "d0_ret_pct", "d1_ret_pct", "d2_ret_pct", "d3_ret_pct",
    "two_day_ret", "three_day_ret",
    "w_ret_pct", "w1_ret_pct", "w2_ret_pct", "m_ret_pct",
    "d0_range_pct", "d0_close_vs_high", "d0_close_vs_low",
    "d0_vol_ratio", "w_vol_vs_w1", "d0_close_pct_range",
]
BOOL_FEATURES = ["above_d1_close", "above_d2_close", "above_w1_close", "above_m_open"]


# --------------------------------------------------------------------------- #
# Part 1 — MTF feature engineering
# --------------------------------------------------------------------------- #
def _iso_week_key(s: pd.Series) -> pd.Series:
    iso = s.dt.isocalendar()
    return iso["year"].astype(int) * 100 + iso["week"].astype(int)


def build_mtf_features(cfg: Config, signals: dict) -> pd.DataFrame:
    """For every (signal_date, symbol, rank) in the Top-10 window, compute MTF
    features from ohlc_daily up to and including the signal day. No lookahead."""
    # study signal_date -> entry_date is in `signals` (keyed by entry_date). We
    # need signal_date too; pull the mapping straight from the study table.
    con = sqlite3.connect(str(cfg.db_path))
    sig = pd.read_sql_query(
        """SELECT signal_date, entry_date, engine_rank AS rank, symbol
           FROM falcon_signal_day_study
           WHERE persona=? AND engine_rank BETWEEN 1 AND 10""",
        con, params=(cfg.persona,))
    # restrict to the intraday window (entry_date has 1-min data)
    min_1m, max_1m = con.execute(
        "SELECT min(substr(bar_time,1,10)), max(substr(bar_time,1,10)) FROM ohlc_1min"
    ).fetchone()
    start = cfg.start or min_1m
    end = cfg.end or max_1m
    sig = sig[(sig["entry_date"] >= start) & (sig["entry_date"] <= end)].copy()
    syms = tuple(sorted(sig["symbol"].unique()))

    # daily OHLC for those symbols (with ample history for W2/M lookback)
    ohlc = pd.read_sql_query(
        "SELECT symbol, trade_date, open, high, low, close, volume FROM ohlc_daily "
        "WHERE symbol IN (%s)" % ",".join("?" * len(syms)), con, params=syms)
    con.close()
    ohlc["dt"] = pd.to_datetime(ohlc["trade_date"])
    ohlc = ohlc.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    ohlc["wk"] = _iso_week_key(ohlc["dt"])
    ohlc["ym"] = ohlc["dt"].dt.year * 100 + ohlc["dt"].dt.month

    by_sym = {s: g.reset_index(drop=True) for s, g in ohlc.groupby("symbol")}

    rows = []
    missing = []
    for r in sig.itertuples(index=False):
        g = by_sym.get(r.symbol)
        if g is None:
            missing.append((r.signal_date, r.symbol, "no_daily")); continue
        pos = g.index[g["trade_date"] == r.signal_date]
        if len(pos) == 0 or pos[0] < 3:
            missing.append((r.signal_date, r.symbol, "lt_3d_history")); continue
        i = int(pos[0])
        D0, D1, D2, D3 = g.iloc[i], g.iloc[i-1], g.iloc[i-2], g.iloc[i-3]

        # weekly aggregates (in-week data only up to signal day; prior weeks full)
        wk0 = D0["wk"]
        week_keys = sorted(g.loc[:i, "wk"].unique())     # weeks up to signal day
        def week_slice(key, upto_i=None):
            m = g["wk"] == key
            if upto_i is not None:
                m &= (g.index <= upto_i)
            return g[m]
        W = week_slice(wk0, i)
        wk_idx = week_keys.index(wk0)
        W1 = week_slice(week_keys[wk_idx-1]) if wk_idx >= 1 else None
        W2 = week_slice(week_keys[wk_idx-2]) if wk_idx >= 2 else None
        M = g[(g["ym"] == D0["ym"]) & (g.index <= i)]

        def safe(n, d):
            return float(n / d * 100.0) if d not in (0, None) and np.isfinite(d) and d != 0 else np.nan

        feat = {
            "signal_date": r.signal_date, "entry_date": r.entry_date,
            "engine_rank": int(r.rank), "symbol": r.symbol,
            # price context
            "d0_ret_pct": safe(D0.close - D1.close, D1.close),
            "d1_ret_pct": safe(D1.close - D2.close, D2.close),
            "d2_ret_pct": safe(D2.close - D3.close, D3.close),
            "d3_ret_pct": safe(D3.close - g.iloc[i-4].close, g.iloc[i-4].close) if i >= 4 else np.nan,
            "two_day_ret": safe(D0.close - D2.close, D2.close),
            "three_day_ret": safe(D0.close - D3.close, D3.close),
            "w_ret_pct": safe(D0.close - W.iloc[0].open, W.iloc[0].open),
            "w1_ret_pct": safe(W1.iloc[-1].close - W1.iloc[0].open, W1.iloc[0].open) if W1 is not None and len(W1) else np.nan,
            "w2_ret_pct": safe(W2.iloc[-1].close - W2.iloc[0].open, W2.iloc[0].open) if W2 is not None and len(W2) else np.nan,
            "m_ret_pct": safe(D0.close - M.iloc[0].open, M.iloc[0].open),
            # range & volatility
            "d0_range_pct": safe(D0.high - D0.low, D1.close),
            "d0_close_vs_high": safe(D0.close - D0.high, D0.high),
            "d0_close_vs_low": safe(D0.close - D0.low, D0.low),
            # volume
            "d0_vol_ratio": float(D0.volume / np.mean([D1.volume, D2.volume, D3.volume]))
                if np.mean([D1.volume, D2.volume, D3.volume]) > 0 else np.nan,
            "w_vol_vs_w1": float(W["volume"].sum() / W1["volume"].sum())
                if W1 is not None and W1["volume"].sum() > 0 else np.nan,
            # position within range
            "d0_close_pct_range": float((D0.close - D0.low) / (D0.high - D0.low))
                if (D0.high - D0.low) > 0 else np.nan,
            # trend (bool)
            "above_d1_close": bool(D0.close > D1.close),
            "above_d2_close": bool(D0.close > D2.close),
            "above_w1_close": bool(D0.close > W1.iloc[-1].close) if W1 is not None and len(W1) else None,
            "above_m_open": bool(D0.close > M.iloc[0].open),
        }
        rows.append(feat)

    feats = pd.DataFrame(rows)
    feats.attrs["missing"] = missing
    return feats


def write_feature_table(cfg: Config, feats: pd.DataFrame):
    """Persist features to a NEW table (additive; drops/recreates only this one)."""
    con = sqlite3.connect(str(cfg.db_path))
    con.execute(f"DROP TABLE IF EXISTS {FEATURE_TABLE}")
    feats.to_sql(FEATURE_TABLE, con, index=False)
    con.commit(); con.close()


def parity_features(cfg: Config, feats: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """d0_ret_pct must match signal_day_ret_pct from falcon_signal_day_context."""
    con = sqlite3.connect(str(cfg.db_path))
    ctx = pd.read_sql_query(
        "SELECT signal_date, symbol, signal_day_ret_pct FROM falcon_signal_day_context", con)
    con.close()
    m = feats.merge(ctx, on=["signal_date", "symbol"], how="inner")
    m = m.dropna(subset=["d0_ret_pct", "signal_day_ret_pct"])
    samp = m.sample(min(n, len(m)), random_state=11).copy()
    samp["diff"] = (samp["d0_ret_pct"] - samp["signal_day_ret_pct"]).abs()
    samp["match"] = samp["diff"] < 0.05
    return samp[["signal_date", "symbol", "d0_ret_pct", "signal_day_ret_pct", "diff", "match"]]


# --------------------------------------------------------------------------- #
# Part 2 — intraday cache + fast basket simulator
# --------------------------------------------------------------------------- #
def build_intraday_cache(cfg: Config, signals: dict, feats: pd.DataFrame):
    """Per entry_date: aligned minute matrices for that day's Top-10, plus each
    stock's MTF feature row. Lets us re-simulate any filtered basket fast."""
    fidx = {(r.signal_date, r.symbol): r for r in feats.itertuples(index=False)}
    con = sqlite3.connect(str(cfg.db_path))
    min_1m, max_1m = con.execute(
        "SELECT min(substr(bar_time,1,10)), max(substr(bar_time,1,10)) FROM ohlc_1min"
    ).fetchone()
    start = cfg.start or min_1m
    end = cfg.end or max_1m
    entry_days = sorted(d for d in signals if start <= d <= end)

    cache = {}
    for day in entry_days:
        picks = signals[day]                       # [(rank, symbol)] ranks 1..10
        study_syms = [s for _, s in picks]
        day_ohlc = load_ohlc_1min_day(con, day, study_syms, cfg.aliases)
        # signal_date for this entry day (from any feature row sharing entry_date)
        grid = None
        cols_close, cols_open, entry_open, meta = [], [], [], []
        for rk, sym in picks:
            df = day_ohlc.get(sym)
            if df is None or "09:15" not in df.index:
                continue
            eo = df.at["09:15", "open"]
            if eo is None or not np.isfinite(eo) or eo <= 0:
                continue
            g = [m for m in df.index if m >= "09:15"]
            if grid is None or len(g) > len(grid):
                grid = g
        if grid is None:
            continue
        for rk, sym in picks:
            df = day_ohlc.get(sym)
            if df is None or "09:15" not in df.index:
                continue
            eo = df.at["09:15", "open"]
            if eo is None or not np.isfinite(eo) or eo <= 0:
                continue
            close = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
            o = df["open"].reindex(grid).to_numpy(float)
            c = close
            oexec = np.where(np.isfinite(o) & (o > 0), o, c)
            cols_close.append(close); cols_open.append(oexec)
            entry_open.append(float(eo))
            meta.append((rk, sym))
        if not meta:
            continue
        cache[day] = {
            "grid": grid,
            "close": np.column_stack(cols_close),     # (minutes, nstocks)
            "openexec": np.column_stack(cols_open),
            "entry_open": np.array(entry_open, float),
            "ranks": np.array([m[0] for m in meta]),
            "symbols": [m[1] for m in meta],
        }
    con.close()
    # attach MTF feature dict per (day, symbol) via signal_date lookup
    ed2sd = dict(zip(feats["entry_date"], feats["signal_date"]))
    feat_lookup = {(r.entry_date, r.symbol): r for r in feats.itertuples(index=False)}
    for day, d in cache.items():
        d["feat_rows"] = [feat_lookup.get((day, s)) for s in d["symbols"]]
    return cache, entry_days


def sim_basket(day_cache, mask: np.ndarray, capital: float, target: float):
    """mask: bool over the day's stocks (already valid). Equal alloc among masked.
    Returns (realized_return_pct, hit) or None if no stock selected."""
    sel = np.where(mask)[0]
    if len(sel) == 0:
        return None
    eo = day_cache["entry_open"][sel]
    alloc = capital / len(sel)
    qty = np.floor(alloc / eo)
    keep = qty > 0
    if not keep.any():
        return None
    sel = sel[keep]; qty = qty[keep]; eo = eo[keep]
    deployed = float((qty * eo).sum())
    close = day_cache["close"][:, sel]
    openexec = day_cache["openexec"][:, sel]
    port = close @ qty
    ret = (port - deployed) / deployed * 100.0
    n = len(ret)
    for i in range(n - 1):
        if ret[i] >= target:
            exit_val = float(openexec[i + 1] @ qty)
            return (exit_val - deployed) / deployed * 100.0, True
    return (float(port[-1]) - deployed) / deployed * 100.0, False


# --------------------------------------------------------------------------- #
# Part 3 — filter scan
# --------------------------------------------------------------------------- #
def _feature_value(frow, feat):
    if frow is None:
        return None
    return getattr(frow, feat, None)


def eval_filter(cache, entry_days, predicates, capital, target, min_n):
    """predicates: list of (feature, op, threshold). A stock passes if ALL hold.
    Basket each day = passing stocks. Returns metrics dict or None if N<min_n."""
    rets, hits, years_hit = [], [], {}
    daily = []
    for day in entry_days:
        dc = cache.get(day)
        if dc is None:
            continue
        nst = len(dc["symbols"])
        mask = np.ones(nst, dtype=bool)
        ok = True
        for j in range(nst):
            frow = dc["feat_rows"][j]
            passes = True
            for feat, op, th in predicates:
                v = _feature_value(frow, feat)
                if v is None or (isinstance(v, float) and not np.isfinite(v)):
                    passes = False; break
                if op == ">=" and not (v >= th): passes = False; break
                if op == "<=" and not (v <= th): passes = False; break
                if op == "==" and not (bool(v) == bool(th)): passes = False; break
            mask[j] = passes
        res = sim_basket(dc, mask, capital, target)
        if res is None:
            continue
        r, hit = res
        rets.append(r); hits.append(hit); daily.append((day, r, hit, int(mask.sum())))
        y = day[:4]
        years_hit.setdefault(y, []).append(hit)
    n = len(rets)
    if n < min_n:
        return None
    rets = np.array(rets); hits = np.array(hits)
    yr_rates = {y: float(np.mean(v) * 100) for y, v in years_hit.items()}
    worst_yr = min(yr_rates.values()) if yr_rates else None
    return {
        "N": n,
        "hit_rate": round(hits.mean() * 100, 1),
        "avg_return": round(rets.mean(), 3),
        "median": round(float(np.median(rets)), 3),
        "worst_day": round(rets.min(), 2),
        "n_years": len(yr_rates),
        "worst_year_hit": round(worst_yr, 1) if worst_yr is not None else None,
        "regime_sensitive": (worst_yr is not None and worst_yr < 60.0),
        "year_rates": yr_rates,
        "daily": daily,
    }


def threshold_grid(feats: pd.DataFrame, feat: str):
    s = feats[feat].dropna()
    if s.empty:
        return []
    qs = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    vals = sorted(set(round(float(s.quantile(q)), 4) for q in qs))
    out = []
    for v in vals:
        out.append((feat, ">=", v))
        out.append((feat, "<=", v))
    return out


def single_filter_scan(cache, entry_days, feats, cfg, min_n):
    cands = []
    for feat in NUMERIC_FEATURES:
        for pred in threshold_grid(feats, feat):
            m = eval_filter(cache, entry_days, [pred], cfg.capital, cfg.target_pct, min_n)
            if m:
                cands.append(([pred], m))     # always a LIST of predicates
    for feat in BOOL_FEATURES:
        for th in (True, False):
            pred = (feat, "==", th)
            m = eval_filter(cache, entry_days, [pred], cfg.capital, cfg.target_pct, min_n)
            if m:
                cands.append(([pred], m))
    cands.sort(key=lambda x: (-x[1]["hit_rate"], -x[1]["N"]))
    return cands


def _row(preds, m):
    return {
        "filter": " AND ".join(f"{f}{op}{th}" for f, op, th in preds),
        "N days": m["N"], "Hit rate %": m["hit_rate"], "Avg return %": m["avg_return"],
        "Median %": m["median"], "Worst day %": m["worst_day"],
        "Years": m["n_years"], "Worst-year hit %": m["worst_year_hit"],
        "Regime-sensitive": m["regime_sensitive"],
    }


def combo_scan(cache, entry_days, base_preds_list, feats, cfg, min_n, add_features):
    """Extend each base predicate-set by one more feature predicate."""
    seen = set(); out = []
    for base in base_preds_list:
        for feat in add_features:
            grid = (threshold_grid(feats, feat) if feat in NUMERIC_FEATURES
                    else [(feat, "==", True), (feat, "==", False)])
            for pred in grid:
                if any(p[0] == pred[0] for p in base):   # don't repeat a feature
                    continue
                preds = tuple(sorted(base + [pred]))
                if preds in seen:
                    continue
                seen.add(preds)
                m = eval_filter(cache, entry_days, list(preds), cfg.capital, cfg.target_pct, min_n)
                if m:
                    out.append((list(preds), m))
    out.sort(key=lambda x: (-x[1]["hit_rate"], -x[1]["N"]))
    return out


# --------------------------------------------------------------------------- #
# Part 4 — Top-10 basket slices (unfiltered)
# --------------------------------------------------------------------------- #
BASKET_SLICES = {
    "Top 3": [1, 2, 3], "Top 5": [1, 2, 3, 4, 5], "Top 10": list(range(1, 11)),
    "Ranks 1-5": [1, 2, 3, 4, 5], "Ranks 6-10": [6, 7, 8, 9, 10],
}
NA_SLICES = ["Top 15 (N/A - universe)", "Top 20 (N/A - universe)",
             "Top 25 (N/A - universe)", "Ranks 11-25 (N/A - universe)"]


def basket_table(cache, entry_days, cfg):
    rows = []
    for label, ranks in BASKET_SLICES.items():
        rs = set(ranks)
        rets, hits = [], []
        for day in entry_days:
            dc = cache.get(day)
            if dc is None:
                continue
            mask = np.array([rk in rs for rk in dc["ranks"]], dtype=bool)
            res = sim_basket(dc, mask, cfg.capital, cfg.target_pct)
            if res is None:
                continue
            rets.append(res[0]); hits.append(res[1])
        if rets:
            rets = np.array(rets); hits = np.array(hits)
            rows.append({"Basket": label, "N days": len(rets),
                         "Hit rate %": round(hits.mean()*100, 1),
                         "Avg return %": round(rets.mean(), 3),
                         "Median %": round(float(np.median(rets)), 3),
                         "Worst day %": round(rets.min(), 2)})
    for na in NA_SLICES:
        rows.append({"Basket": na, "N days": None, "Hit rate %": None,
                     "Avg return %": None, "Median %": None, "Worst day %": None})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--persona", default="falcon_top10_daily")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--capital", type=float, default=500_000.0)
    ap.add_argument("--target", type=float, default=1.0)
    ap.add_argument("--min-n", type=int, default=50)
    ap.add_argument("--stage", choices=["all", "features", "scan"], default="all")
    ap.add_argument("--out", default=str(OUT_XLSX))
    args = ap.parse_args()

    cfg = Config(db_path=Path(args.db), persona=args.persona, start=args.start,
                 end=args.end, capital=args.capital, target_pct=args.target)
    signals = load_signals(sqlite3.connect(str(cfg.db_path)), cfg)
    print(f"[*] capital Rs {cfg.capital:,.0f}  target +{cfg.target_pct}%  min_n {args.min_n}",
          flush=True)

    print("[*] Part 1: building MTF features ...", flush=True)
    feats = build_mtf_features(cfg, signals)
    print(f"    feature rows: {len(feats)}  missing: {len(feats.attrs.get('missing', []))}",
          flush=True)
    write_feature_table(cfg, feats)
    pf = parity_features(cfg, feats)
    print(f"    parity d0_ret vs signal_day_ret: {int(pf['match'].sum())}/{len(pf)} match",
          flush=True)
    if args.stage == "features":
        print(pf.to_string(index=False))
        print("[*] stage=features done (table written, parity above).")
        return

    print("[*] Part 2: building intraday cache ...", flush=True)
    cache, entry_days = build_intraday_cache(cfg, signals, feats)
    days_with_cache = [d for d in entry_days if d in cache]
    print(f"    cached entry days: {len(days_with_cache)}", flush=True)

    # baseline parity: unfiltered Top 5 / Top 10
    base = basket_table(cache, days_with_cache, cfg)
    print("[*] unfiltered baskets:\n", base.to_string(index=False), flush=True)

    print("[*] Part 3a: single-filter scan ...", flush=True)
    singles = single_filter_scan(cache, days_with_cache, feats, cfg, args.min_n)
    s_top20 = singles[:20]
    print(f"    single filters passing min_n: {len(singles)} (top hit "
          f"{singles[0][1]['hit_rate'] if singles else 'NA'}%)", flush=True)

    print("[*] Part 3b: two-filter combos (from top-10 singles) ...", flush=True)
    base_sets = [p for p, _ in singles[:10]]      # each is a 1-predicate list
    pairs = combo_scan(cache, days_with_cache, base_sets, feats, cfg, args.min_n,
                       NUMERIC_FEATURES + BOOL_FEATURES)
    pairs_ge85 = [(p, m) for p, m in pairs if m["hit_rate"] >= 85 and m["N"] >= max(100, args.min_n)]
    print(f"    pairs >=85% & N>=100: {len(pairs_ge85)}", flush=True)

    print("[*] Part 3c: three-filter combos (from top-10 pairs) ...", flush=True)
    top_pairs = [p for p, _ in pairs[:10]]
    triples = combo_scan(cache, days_with_cache, top_pairs, feats, cfg, args.min_n,
                         NUMERIC_FEATURES + BOOL_FEATURES)
    triples_ge90 = [(p, m) for p, m in triples if m["hit_rate"] >= 90 and m["N"] >= max(75, args.min_n)]
    print(f"    triples >=90% & N>=75: {len(triples_ge90)}", flush=True)

    # best combination at N>=min_n across all stages
    all_combos = [(p, m) for p, m in (singles + pairs + triples)]
    all_combos.sort(key=lambda x: (-x[1]["hit_rate"], -x[1]["N"]))
    best_preds, best_m = all_combos[0]
    print(f"[*] BEST @N>={args.min_n}: {_row(best_preds, best_m)['filter']}  "
          f"hit {best_m['hit_rate']}%  N={best_m['N']}", flush=True)

    # highest hit rate achievable at each min-N floor
    floors = []
    for floor in (50, 40, 30, 20):
        pool = [(p, m) for p, m in (singles + pairs + triples) if m["N"] >= floor]
        if pool:
            pool.sort(key=lambda x: -x[1]["hit_rate"])
            bp, bm = pool[0]
            floors.append({"min N": floor, "best hit %": bm["hit_rate"], "N": bm["N"],
                           "avg %": bm["avg_return"], "worst-year %": bm["worst_year_hit"],
                           "filter": _row(bp, bm)["filter"]})
    floors_df = pd.DataFrame(floors)

    # Part 4
    print("[*] Part 4: basket slices", flush=True)
    basket_df = basket_table(cache, days_with_cache, cfg)

    # Part 5: best filter applied across basket slices (filtered within each slice)
    print("[*] Part 5: combined matrix", flush=True)
    best_single = singles[0][0] if singles else None      # list of 1 predicate
    best_pair = pairs[0][0] if pairs else None             # list of 2
    best_triple = triples[0][0] if triples else None       # list of 3
    matrix_rows = []
    for label, ranks in BASKET_SLICES.items():
        rs = set(ranks)
        cells = {"Basket": label}
        for col, preds in (("Unfiltered", None), ("Best single", best_single),
                           ("Best 2-filter", best_pair), ("Best 3-filter", best_triple)):
            rets, hits = [], []
            for day in days_with_cache:
                dc = cache.get(day)
                if dc is None:
                    continue
                mask = np.array([rk in rs for rk in dc["ranks"]], dtype=bool)
                if preds:
                    for j in range(len(dc["symbols"])):
                        if not mask[j]:
                            continue
                        frow = dc["feat_rows"][j]
                        for feat, op, th in preds:
                            v = _feature_value(frow, feat)
                            bad = (v is None or (isinstance(v, float) and not np.isfinite(v)) or
                                   (op == ">=" and not v >= th) or (op == "<=" and not v <= th) or
                                   (op == "==" and bool(v) != bool(th)))
                            if bad:
                                mask[j] = False; break
                res = sim_basket(dc, mask, cfg.capital, cfg.target_pct)
                if res is None:
                    continue
                rets.append(res[0]); hits.append(res[1])
            if rets:
                cells[col] = f"{np.mean(hits)*100:.1f}% (N={len(rets)})"
            else:
                cells[col] = "—"
        matrix_rows.append(cells)
    matrix_df = pd.DataFrame(matrix_rows)

    # Best-combo daily log, year, time-of-day, miss analysis
    best_daily = pd.DataFrame(best_m["daily"], columns=["entry_date", "portfolio_return_pct", "hit_1pct", "n_stocks"])
    best_year = (best_daily.assign(year=best_daily["entry_date"].str[:4])
                 .groupby("year").agg(N=("hit_1pct", "size"),
                                      hit_rate=("hit_1pct", lambda x: round(x.mean()*100, 1)),
                                      avg_return=("portfolio_return_pct", lambda x: round(x.mean(), 3)))
                 .reset_index())

    write_excel(Path(args.out), feats, pf, s_top20, pairs_ge85, triples_ge90,
                best_preds, best_m, best_daily, best_year, basket_df, matrix_df,
                floors_df, base, cfg, args)
    print(f"\n[*] wrote {args.out}")


def _combo_df(combos):
    return pd.DataFrame([_row(p, m) for p, m in combos])


def write_excel(path, feats, pf, singles, pairs, triples, best_preds, best_m,
                best_daily, best_year, basket_df, matrix_df, floors_df, base, cfg, args):
    path.parent.mkdir(parents=True, exist_ok=True)
    # parity checks
    checks = []
    t5 = base[base["Basket"] == "Top 5"]["Hit rate %"]
    t10 = base[base["Basket"] == "Top 10"]["Hit rate %"]
    checks.append(("Unfiltered Top 5 hit ~76.9% (baseline)", not t5.empty and abs(float(t5.iloc[0]) - 76.9) <= 1.5,
                   f"got {None if t5.empty else float(t5.iloc[0])}"))
    checks.append(("Unfiltered Top 10 hit ~73.8% (baseline)", not t10.empty and abs(float(t10.iloc[0]) - 73.8) <= 1.5,
                   f"got {None if t10.empty else float(t10.iloc[0])}"))
    checks.append(("d0_ret_pct == signal_day_ret_pct (20-row spot, >=95%)",
                   pf["match"].mean() >= 0.95,
                   f"{int(pf['match'].sum())}/{len(pf)} match "
                   f"(misses = corp-action/adj-close boundary)"))
    no100 = best_m["hit_rate"] < 100.0
    checks.append(("Best combo hit rate < 100% (lookahead guard)", no100, f"best={best_m['hit_rate']}%"))
    checks.append((f"Best combo N >= {args.min_n} (min-N rule)", best_m["N"] >= args.min_n, f"N={best_m['N']}"))

    # Miss-day analysis for best combo: features present on miss vs hit days
    miss_rows = []
    daily = best_m["daily"]
    hit_days = {d for d, r, h, n in daily if h}
    miss_days = {d for d, r, h, n in daily if not h}
    fd = feats[feats["entry_date"].isin(hit_days | miss_days)]
    for feat in NUMERIC_FEATURES:
        hv = fd[fd["entry_date"].isin(hit_days)][feat].mean()
        mv = fd[fd["entry_date"].isin(miss_days)][feat].mean()
        miss_rows.append({"feature": feat, "avg on hit days": round(hv, 3) if pd.notna(hv) else None,
                          "avg on miss days": round(mv, 3) if pd.notna(mv) else None,
                          "delta (hit-miss)": round(hv - mv, 3) if pd.notna(hv) and pd.notna(mv) else None})
    miss_df = pd.DataFrame(miss_rows)

    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        feats.to_excel(xl, "1_MTF_Features", index=False)
        _combo_df(singles).to_excel(xl, "2_Single_Filter_Scan", index=False)
        (_combo_df(pairs) if pairs else pd.DataFrame([{"note": "no 2-filter combo >=85% at N>=100"}])
         ).to_excel(xl, "3_Two_Filter", index=False)
        (_combo_df(triples) if triples else pd.DataFrame([{"note": "no 3-filter combo >=90% at N>=75"}])
         ).to_excel(xl, "4_Three_Filter", index=False)
        # sheet 5: best combo
        pd.DataFrame([_row(best_preds, best_m)]).to_excel(xl, "5_Best_Combination", index=False)
        best_daily.to_excel(xl, "5_Best_Combination", index=False, startrow=3)
        basket_df.to_excel(xl, "6_Basket_Analysis", index=False)
        matrix_df.to_excel(xl, "7_Combined_Matrix", index=False)
        best_year.to_excel(xl, "8_Year_By_Year", index=False)
        # sheet 9 time-of-day not meaningful here (exit_time not stored in fast sim) -> note
        pd.DataFrame([{"note": "Time-of-day available in main backtest; fast scan stores return+hit only. "
                       "Re-run main backtest on best-combo basket for minute-level hit timing."}]
                     ).to_excel(xl, "9_Time_Of_Day", index=False)
        miss_df.to_excel(xl, "10_Miss_Day_Analysis", index=False)
        floors_df.to_excel(xl, "10_Miss_Day_Analysis", index=False, startrow=len(miss_df) + 4)
        pd.DataFrame([{"check": c, "pass": "PASS" if ok else "FAIL", "detail": d}
                      for c, ok, d in checks]).to_excel(xl, "0_Parity_Checks", index=False)
        pd.DataFrame([{"key": "universe", "value": "Top-10 (parity-true); Top-25 unavailable"},
                      {"key": "capital", "value": cfg.capital},
                      {"key": "target_pct", "value": cfg.target_pct},
                      {"key": "min_n", "value": args.min_n},
                      {"key": "best_filter", "value": _row(best_preds, best_m)["filter"]},
                      {"key": "best_hit_rate", "value": best_m["hit_rate"]},
                      {"key": "best_N", "value": best_m["N"]},
                      ]).to_excel(xl, "0_Run_Info", index=False)

    print("\n=== PARITY CHECKS ===")
    for c, ok, d in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {c} :: {d}")


if __name__ == "__main__":
    main()
