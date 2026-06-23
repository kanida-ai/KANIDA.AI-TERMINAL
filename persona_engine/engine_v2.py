"""
v2 — TWO-STAGE engine (user direction 2026-06-22).

  Stage 1 (EOD, day T)      : rank the F&O universe and take a SHORTLIST of N
                              long-watch + N short-watch names. Uses the EOD model
                              plus D-1/D-2 lag + volume features.
  Stage 2 (next morning T+1): CONFIRM/narrow the shortlist to the final Top-10 using
                              the opening signal (gap, 9:15→9:45 momentum, early vol).

We report THREE overlap measurements per day, kept rigorously distinct:

  • NAME_OC    : final-10 chosen at 09:45 vs actual top-10 by 09:15→close (spec metric).
                A *screening* number — it credits the 09:15–09:45 move that has
                already happened, so it is NOT fully predictive. Flagged as such.
  • CAP_0945   : final-10 chosen at 09:45 vs actual top-10 by 09:45→close (the
                CAPTURABLE return if you enter at 09:45). The honest tradeable number.
  • GAP_OC     : final-10 chosen at 09:15 by gap only vs actual top-10 by open→close
                (capturable from the open). Works on full history (no intraday needed).

Stage-1-only (EOD, fully predictive) overlap is also reported for reference.
"""
from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from persona_engine import db, model, outcomes
from persona_engine.features import INDEX_SYMBOL

SHORTLIST = 30


def _next_date_map(dates: List[str]) -> Dict[str, str]:
    s = sorted(dates)
    return {s[i]: s[i + 1] for i in range(len(s) - 1)}


def _add_lags(feats: pd.DataFrame) -> pd.DataFrame:
    feats = feats.sort_values(["symbol", "trade_date"])
    g = feats.groupby("symbol", group_keys=False)
    feats["sig_ret_lag1"] = g["sig_ret_pct"].shift(1)
    feats["sig_ret_lag2"] = g["sig_ret_pct"].shift(2)
    feats["vol_ratio_lag1"] = g["vol_ratio_20d"].shift(1)
    return feats


def _centered_rank(s):
    return (s.rank(pct=True) - 0.5) * 2.0


def _overlap(pred_syms, actual_top):
    return len(set(pred_syms) & set(actual_top))


def run_v2(con, fo_universe, start="2022-01-01", end=None,
           intraday_only=False, verbose=True) -> Dict:
    feats = pd.read_sql_query(
        "SELECT * FROM persona_signal_features WHERE symbol IN (%s)"
        % ",".join("?" * len(fo_universe)), con, params=fo_universe)
    feats = _add_lags(feats)
    # NEW non-price event/conviction features (earnings flags + delivery)
    try:
        ev = pd.read_sql_query(
            "SELECT symbol,trade_date,earn_next1,earn_recent2,deliv_pct,deliv_z20,accum "
            "FROM persona_event_features WHERE symbol IN (%s)"
            % ",".join("?" * len(fo_universe)), con, params=fo_universe)
        feats = feats.merge(ev, on=["symbol", "trade_date"], how="left")
    except Exception:
        for c in ("earn_next1", "earn_recent2", "deliv_pct", "deliv_z20", "accum"):
            feats[c] = np.nan
    openf = pd.read_sql_query(
        "SELECT * FROM persona_open_features WHERE symbol IN (%s)"
        % ",".join("?" * len(fo_universe)), con, params=fo_universe)

    if start:
        feats = feats[feats["trade_date"] >= start]
    if end:
        feats = feats[feats["trade_date"] <= end]

    w_long = dict(model.BASELINE_WEIGHTS["FO_LONG"])
    w_short = dict(model.BASELINE_WEIGHTS["FO_SHORT"])

    all_dates = sorted(set(feats["trade_date"]) | set(openf["trade_date"]))
    nxt = _next_date_map(all_dates)
    feats_by_date = {d: g.set_index("symbol") for d, g in feats.groupby("trade_date")}
    open_by_date = {d: g.set_index("symbol") for d, g in openf.groupby("trade_date")}

    acc = {k: [] for k in ["eod_long", "eod_short", "name_oc_long", "name_oc_short",
                           "cap0945_long", "cap0945_short", "gapoc_long", "gapoc_short"]}

    for T in sorted(feats_by_date.keys()):
        Tn = nxt.get(T)
        if Tn is None or Tn not in open_by_date:
            continue
        eod = feats_by_date[T]
        if len(eod) < 20:
            continue
        om = open_by_date[Tn]  # opening features on the outcome morning

        # ---- Stage 1: EOD scores + shortlist ----
        long_s = pd.Series(0.0, index=eod.index)
        short_s = pd.Series(0.0, index=eod.index)
        for f, wv in w_long.items():
            if f in eod:
                long_s += wv * _centered_rank(eod[f]).fillna(0)
        for f, wv in w_short.items():
            if f in eod:
                short_s += wv * _centered_rank(eod[f]).fillna(0)
        # add a magnitude tilt (both books want tail names) + earnings-tomorrow boost
        mag = pd.Series(0.0, index=eod.index)
        for f in ["atr_20_pct", "vol_ratio_20d"]:
            if f in eod:
                mag += _centered_rank(eod[f]).fillna(0)
        # (earnings-tomorrow boost was tested here; it diluted the top-10 — only ~19%
        #  of results-day stocks become movers — so it is not applied.)
        long_s = long_s + 1.5 * mag
        short_s = short_s + 1.5 * mag
        long_watch = long_s.sort_values(ascending=False).head(SHORTLIST).index
        short_watch = short_s.sort_values(ascending=False).head(SHORTLIST).index

        # ---- actual universe ranking on the morning ----
        om_oc = om.dropna(subset=["oc_full"])
        if len(om_oc) < 20:
            continue
        actual_gain_oc = om_oc.sort_values("oc_full", ascending=False).head(10).index
        actual_lose_oc = om_oc.sort_values("oc_full", ascending=True).head(10).index

        has_intra = om["has_intraday"].fillna(0).astype(int).max() == 1
        if intraday_only and not has_intra:
            continue

        # ---- Stage 1-only overlap (EOD prediction, fully predictive) ----
        eod_long10 = long_s.sort_values(ascending=False).head(10).index
        eod_short10 = short_s.sort_values(ascending=False).head(10).index
        acc["eod_long"].append((T, _overlap(eod_long10, actual_gain_oc)))
        acc["eod_short"].append((T, _overlap(eod_short10, actual_lose_oc)))

        # ---- Stage 2: GAP confirm (capturable from open), full history ----
        def confirm(watch, col, asc):
            sub = om.reindex(watch).dropna(subset=[col])
            if sub.empty:
                return []
            return sub.sort_values(col, ascending=asc).head(10).index.tolist()

        gap_long10 = confirm(long_watch, "gap_pct", asc=False)
        gap_short10 = confirm(short_watch, "gap_pct", asc=True)
        acc["gapoc_long"].append((T, _overlap(gap_long10, actual_gain_oc)))
        acc["gapoc_short"].append((T, _overlap(gap_short10, actual_lose_oc)))

        if has_intra:
            # ---- Stage 2: 9:45 momentum confirm ----
            name_long10 = confirm(long_watch, "ret_o_0945", asc=False)
            name_short10 = confirm(short_watch, "ret_o_0945", asc=True)
            # NAME vs open->close (spec metric; screening, look-ahead-flagged)
            acc["name_oc_long"].append((T, _overlap(name_long10, actual_gain_oc)))
            acc["name_oc_short"].append((T, _overlap(name_short10, actual_lose_oc)))
            # CAP vs 9:45->close (honest capturable)
            om_cap = om.dropna(subset=["ret_0945_close"])
            if len(om_cap) >= 20:
                act_gain_cap = om_cap.sort_values("ret_0945_close", ascending=False).head(10).index
                act_lose_cap = om_cap.sort_values("ret_0945_close", ascending=True).head(10).index
                acc["cap0945_long"].append((T, _overlap(name_long10, act_gain_cap)))
                acc["cap0945_short"].append((T, _overlap(name_short10, act_lose_cap)))

    summary = _summ(acc)
    if verbose:
        _print(summary)
    return {"summary": summary}


def _summ(acc):
    out = {}
    for k, pairs in acc.items():
        if not pairs:
            out[k] = {}
            continue
        df = pd.DataFrame(pairs, columns=["date", "h"])
        df["year"] = df["date"].str[:4]
        out[k] = {"overall_pct": round(df["h"].mean() * 10, 1),
                  "avg": round(df["h"].mean(), 2),
                  "by_year": (df.groupby("year")["h"].mean() * 10).round(1).to_dict(),
                  "n": len(df)}
    return out


def _print(s):
    print("\n=========== V2 TWO-STAGE SUMMARY (random ~4.7%) ===========")
    rows = [
        ("eod_long", "Stage-1 EOD long  (predictive)"),
        ("eod_short", "Stage-1 EOD short (predictive)"),
        ("gapoc_long", "GAP-confirm long  vs open->close [capturable@open]"),
        ("gapoc_short", "GAP-confirm short vs open->close [capturable@open]"),
        ("cap0945_long", "9:45-confirm long  vs 9:45->close [CAPTURABLE]"),
        ("cap0945_short", "9:45-confirm short vs 9:45->close [CAPTURABLE]"),
        ("name_oc_long", "9:45-NAME long  vs open->close [screen; look-ahead]"),
        ("name_oc_short", "9:45-NAME short vs open->close [screen; look-ahead]"),
    ]
    for k, lbl in rows:
        v = s.get(k, {})
        if not v:
            print(f"  {lbl:52s}: (no data)")
            continue
        print(f"  {lbl:52s}: {v['overall_pct']:4.1f}%  (avg {v['avg']}/10, n={v['n']})")
        print(f"      by year: {v['by_year']}")
    print("===========================================================\n")


if __name__ == "__main__":
    from persona_engine import universe
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    run_v2(con, fo, start="2022-01-01", end=None, verbose=True)
    con.close()
