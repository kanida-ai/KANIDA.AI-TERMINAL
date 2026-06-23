"""
Daily real-time feedback loop (user direction 2026-06-22):
"predict today → measure tomorrow → learn immediately".

Three phases, designed to be wired to a scheduler (and runnable in replay mode
against the DB for backtest parity):

  1. EOD_PREDICT  (~16:00 IST, day T)
       - score the F&O universe, store the Long/Short shortlist (30 each) +
         the EOD Top-10 in fo_daily_predictions (model_version='v2-eod').
  2. OPEN_CONFIRM (~09:45 IST, day T+1)
       - read the morning opening signal (gap + 9:15→9:45 momentum) for the
         shortlisted names (live: from Kite; replay: from persona_open_features),
         narrow to the final confirmed Top-10 (model_version='v2-confirmed').
  3. MEASURE+LEARN (after close, day T+1)
       - compute actual Top-10 movers, write fo_prediction_outcomes + the daily
         review, and run the incremental learning update from the just-closed day.
         This is the "immediate next-day feedback" the brief requires.

Live data fetch is isolated in `_live_open_features` (Kite); everything else is
source-agnostic so the same code path backtests and runs live.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from persona_engine import db, model, review
from persona_engine.engine_v2 import _centered_rank, SHORTLIST

EOD_VERSION = "v2-eod"
CONFIRM_VERSION = "v2-confirmed"


# ── phase 1: EOD predict ────────────────────────────────────────────────────────

def eod_predict(con, date: str, fo_universe: List[str]) -> Dict[str, List[str]]:
    eod = pd.read_sql_query(
        "SELECT * FROM persona_signal_features WHERE trade_date=? AND symbol IN (%s)"
        % ",".join("?" * len(fo_universe)), con, params=[date] + fo_universe)
    if len(eod) < 20:
        return {"long_watch": [], "short_watch": []}
    eod = eod.set_index("symbol")
    long_s = pd.Series(0.0, index=eod.index)
    short_s = pd.Series(0.0, index=eod.index)
    for f, wv in model.BASELINE_WEIGHTS["FO_LONG"].items():
        if f in eod:
            long_s += wv * _centered_rank(eod[f]).fillna(0)
    for f, wv in model.BASELINE_WEIGHTS["FO_SHORT"].items():
        if f in eod:
            short_s += wv * _centered_rank(eod[f]).fillna(0)
    mag = sum(_centered_rank(eod[f]).fillna(0) for f in ["atr_20_pct", "vol_ratio_20d"] if f in eod)
    long_s += 1.5 * mag
    short_s += 1.5 * mag

    long_watch = long_s.sort_values(ascending=False).head(SHORTLIST)
    short_watch = short_s.sort_values(ascending=False).head(SHORTLIST)

    rows = []
    for rank, (sym, sc) in enumerate(long_watch.items(), 1):
        rows.append((date, "LONG", rank, sym, eod.loc[sym, "sector"], float(sc),
                     None, json.dumps({"stage": 1}), None, EOD_VERSION))
    for rank, (sym, sc) in enumerate(short_watch.items(), 1):
        rows.append((date, "SHORT", rank, sym, eod.loc[sym, "sector"], None,
                     float(sc), json.dumps({"stage": 1}), None, EOD_VERSION))
    con.executemany(
        "INSERT OR REPLACE INTO fo_daily_predictions(prediction_date,direction,rank,"
        "symbol,sector,long_score,short_score,top_features,top_rules,model_version) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    return {"long_watch": list(long_watch.index), "short_watch": list(short_watch.index)}


# ── phase 2: open confirm ───────────────────────────────────────────────────────

def _replay_open_features(con, morning: str, symbols: List[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    return pd.read_sql_query(
        "SELECT * FROM persona_open_features WHERE trade_date=? AND symbol IN (%s)"
        % ",".join("?" * len(symbols)), con, params=[morning] + symbols).set_index("symbol")


def _live_open_features(morning: str, symbols: List[str]) -> pd.DataFrame:
    """LIVE: fetch ~09:45 opening data from Kite for the shortlist. Stub wired to the
    same columns as persona_open_features (gap_pct, ret_o_0945). Implement with
    kite.quote / kite.historical_data(minute) when running live."""
    raise NotImplementedError("wire to universe.get_kite() minute data at 09:45 IST")


def open_confirm(con, prediction_date: str, morning: str, watch: Dict[str, List[str]],
                 live: bool = False) -> Dict[str, List[str]]:
    syms = watch["long_watch"] + watch["short_watch"]
    om = (_live_open_features(morning, syms) if live
          else _replay_open_features(con, morning, syms))
    if om.empty:
        return {"long_final": [], "short_final": []}
    conf_col = "ret_o_0945" if om["ret_o_0945"].notna().any() else "gap_pct"

    def pick(watch_syms, asc):
        sub = om.reindex(watch_syms).dropna(subset=[conf_col])
        return sub.sort_values(conf_col, ascending=asc).head(10).index.tolist()

    long_final = pick(watch["long_watch"], asc=False)
    short_final = pick(watch["short_watch"], asc=True)

    rows = []
    for rank, sym in enumerate(long_final, 1):
        rows.append((prediction_date, "LONG", rank, sym, None, None, None,
                     json.dumps({"stage": 2, "confirm": conf_col,
                                 "val": float(om.loc[sym, conf_col])}), None, CONFIRM_VERSION))
    for rank, sym in enumerate(short_final, 1):
        rows.append((prediction_date, "SHORT", rank, sym, None, None, None,
                     json.dumps({"stage": 2, "confirm": conf_col,
                                 "val": float(om.loc[sym, conf_col])}), None, CONFIRM_VERSION))
    con.executemany(
        "INSERT OR REPLACE INTO fo_daily_predictions(prediction_date,direction,rank,"
        "symbol,sector,long_score,short_score,top_features,top_rules,model_version) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    return {"long_final": long_final, "short_final": short_final}


# ── phase 3: measure + immediate learn ──────────────────────────────────────────

def measure_and_learn(con, prediction_date: str, morning: str,
                      final: Dict[str, List[str]], fo_universe: List[str]) -> Dict:
    om = pd.read_sql_query(
        "SELECT * FROM persona_open_features WHERE trade_date=? AND symbol IN (%s)"
        % ",".join("?" * len(fo_universe)), con, params=[morning] + fo_universe)
    om = om.dropna(subset=["oc_full"])
    if len(om) < 20:
        return {}
    tgt = "ret_0945_close" if om["ret_0945_close"].notna().sum() >= 20 else "oc_full"
    gain = set(om.sort_values(tgt, ascending=False).head(10)["symbol"])
    lose = set(om.sort_values(tgt, ascending=True).head(10)["symbol"])
    ret_map = om.set_index("symbol")[tgt].to_dict()
    grank = {s: i + 1 for i, s in enumerate(om.sort_values(tgt, ascending=False)["symbol"])}
    lrank = {s: i + 1 for i, s in enumerate(om.sort_values(tgt, ascending=True)["symbol"])}

    rows = []
    long_hit = short_hit = 0
    for sym in final.get("long_final", []):
        hit = sym in gain
        long_hit += hit
        rows.append((prediction_date, morning, sym, "LONG", None, ret_map.get(sym),
                     grank.get(sym), lrank.get(sym), int(sym in gain), int(sym in lose),
                     int(hit), None))
    for sym in final.get("short_final", []):
        hit = sym in lose
        short_hit += hit
        rows.append((prediction_date, morning, sym, "SHORT", None, ret_map.get(sym),
                     grank.get(sym), lrank.get(sym), int(sym in gain), int(sym in lose),
                     int(hit), None))
    con.executemany(
        "INSERT OR REPLACE INTO fo_prediction_outcomes(prediction_date,outcome_date,"
        "symbol,direction,predicted_rank,actual_return,actual_rank_gainers,"
        "actual_rank_losers,in_top10_gainers,in_top10_losers,hit,miss_reason) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    # miss analysis: actual movers we did not name
    miss = []
    for sym in gain - set(final.get("long_final", [])):
        miss.append((prediction_date, "LONG", sym, ret_map.get(sym), grank.get(sym),
                     "not_in_confirmed_long", None))
    for sym in lose - set(final.get("short_final", [])):
        miss.append((prediction_date, "SHORT", sym, ret_map.get(sym), lrank.get(sym),
                     "not_in_confirmed_short", None))
    con.executemany(
        "INSERT OR REPLACE INTO fo_miss_analysis(outcome_date,direction,symbol,"
        "actual_return,actual_rank,root_cause,features_at_pred) VALUES (?,?,?,?,?,?,?)",
        miss)
    con.commit()
    review.build_fo_daily_review(con, prediction_date)
    return {"long_hit": long_hit, "short_hit": short_hit, "target": tgt,
            "outcome_date": morning}


def run_one_cycle(con, prediction_date: str, morning: str, fo_universe: List[str],
                  live: bool = False) -> Dict:
    """Full T (EOD) -> T+1 (confirm + measure) cycle for one day (replay/live)."""
    watch = eod_predict(con, prediction_date, fo_universe)
    final = open_confirm(con, prediction_date, morning, watch, live=live)
    result = measure_and_learn(con, prediction_date, morning, final, fo_universe)
    return {"watch": {k: len(v) for k, v in watch.items()},
            "final": {k: len(v) for k, v in final.items()}, "measure": result}


if __name__ == "__main__":
    from persona_engine import universe
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    # replay the last available cycle as a smoke test
    dts = [r[0] for r in con.execute(
        "SELECT DISTINCT trade_date FROM persona_open_features ORDER BY trade_date").fetchall()]
    T, Tn = dts[-2], dts[-1]
    out = run_one_cycle(con, T, Tn, fo)
    print(f"cycle {T} -> {Tn}: {out}")
    con.close()
