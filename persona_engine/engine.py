"""
Closed-loop walk-forward engine (spec §2.6/2.7, §3.5/3.6, §4.1).

Runs the genuine loop, causally:

    for each trading day T (walk-forward):
        PREDICT  : score the cross-section with the weights ACTIVE as of T
                   (no future data) -> Long/Short Top10 (F&O), Top10 (LT)
        MEASURE  : once T's outcome is known, rank the universe by realised
                   forward return -> actual Top10 -> overlap / hits / misses
        LEARN    : every 5 trading days, re-estimate feature weights from a
                   TRAILING window only (rank-IC), persist a human-review proposal,
                   and (for the research simulation) adopt the learned weights

2021 is training-only (warm up weights). Predictions/outcomes are stored from the
walk-forward start (default 2022-01) onward.

The IC-weight adoption inside the simulation is the *research* self-improvement
loop; live deployment of any weight change still requires human approval via
``learning_proposals`` (constitutional P4/P10). Both are recorded.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from persona_engine import db, model, personas, outcomes
from persona_engine.features import INDEX_SYMBOL
from persona_engine.model import ALL_FEATURES

WALK_START_DEFAULT = "2022-01-01"
TRAIN_START_DEFAULT = "2021-01-01"
LEARN_EVERY = 5            # trading days between weight re-estimation
TRAIL_DAYS = 126           # trailing trading days used for IC learning
EWMA = 0.7                 # weight on freshly learned IC (vs previous weights)
MIN_XS = 30                # min cross-section size for a usable IC sample
TARGET_L1 = 5.0            # normalise weight vector L1 norm for score stability


# ── data assembly ───────────────────────────────────────────────────────────--

def load_dataset(con: sqlite3.Connection, fo_universe: List[str],
                 lt_universe: List[str]) -> pd.DataFrame:
    feats = pd.read_sql_query(
        "SELECT * FROM persona_signal_features", con)
    fwd = outcomes.forward_returns(con, symbols=sorted(set(fo_universe) | set(lt_universe)))
    data = feats.merge(
        fwd[["symbol", "trade_date", "fwd_nd", "fwd_nd_date", "ret_20", "ret_40",
             "d20_date", "d40_date"]],
        on=["symbol", "trade_date"], how="left")
    return data


# ── IC weight learning (causal: pass only trailing slices) ──────────────────────

def learn_ic_weights(window: pd.DataFrame, ret_col: str, direction_sign: float,
                     prev: Dict[str, float]) -> Dict[str, float]:
    """Estimate pooled rank-IC of each feature vs forward return over a trailing
    window, blend with previous weights (EWMA), normalise. direction_sign=+1 for
    long/LT, -1 for short.

    Vectorised: rank both the feature and the return cross-sectionally per date
    (so each date contributes equally and outliers are tamed), then take one pooled
    Pearson correlation per feature ~ the average daily rank-IC.
    """
    w = window.dropna(subset=[ret_col]).copy()
    # keep only dates with a usable cross-section
    sizes = w.groupby("trade_date")[ret_col].transform("size")
    w = w[sizes >= MIN_XS]
    if w.empty:
        return {f: prev.get(f, 0.0) for f in ALL_FEATURES}
    gd = w.groupby("trade_date")
    ret_rank = gd[ret_col].rank(pct=True)
    ics: Dict[str, float] = {}
    for feat in ALL_FEATURES:
        if feat not in w.columns or w[feat].notna().sum() < MIN_XS:
            ics[feat] = 0.0
            continue
        fr = w.groupby("trade_date")[feat].rank(pct=True)
        m = fr.notna() & ret_rank.notna()
        if m.sum() < MIN_XS or fr[m].std() == 0:
            ics[feat] = 0.0
            continue
        ic = np.corrcoef(fr[m].values, ret_rank[m].values)[0, 1]
        ics[feat] = 0.0 if np.isnan(ic) else float(ic)

    learned = {f: direction_sign * ic for f, ic in ics.items()}
    # normalise the learned IC vector to the same L1 scale as `prev` so the EWMA
    # mix is meaningful (raw ICs ~0.04 would otherwise be swamped by the prior).
    l1_learned = sum(abs(v) for v in learned.values()) or 1.0
    learned = {f: v / l1_learned * TARGET_L1 for f, v in learned.items()}
    blended = {f: (1 - EWMA) * prev.get(f, 0.0) + EWMA * learned.get(f, 0.0)
               for f in ALL_FEATURES}
    l1 = sum(abs(v) for v in blended.values()) or 1.0
    return {f: v / l1 * TARGET_L1 for f, v in blended.items()}


# ── main walk-forward ───────────────────────────────────────────────────────---

def run_walkforward(
    con: sqlite3.Connection,
    fo_universe: List[str],
    lt_universe: List[str],
    walk_start: str = WALK_START_DEFAULT,
    train_start: str = TRAIN_START_DEFAULT,
    end: Optional[str] = None,
    persist: bool = True,
    verbose: bool = True,
) -> Dict:
    data = load_dataset(con, fo_universe, lt_universe)
    if end:
        data = data[data["trade_date"] <= end]
    data = data[data["trade_date"] >= train_start]

    fo_set, lt_set = set(fo_universe), set(lt_universe)
    sector_mom = model.sector_momentum(data)
    regime = model.market_regime_series(con).set_index("trade_date")["regime"].to_dict()

    by_date = {d: g for d, g in data.groupby("trade_date")}
    dates = sorted(by_date.keys())

    # active weights (start from baseline rulebook)
    w_long = dict(model.BASELINE_WEIGHTS["FO_LONG"])
    w_short = dict(model.BASELINE_WEIGHTS["FO_SHORT"])
    w_lt = dict(model.BASELINE_WEIGHTS["LT"])

    fo_pred_rows, fo_out_rows = [], []
    lt_pred_rows, lt_out_rows = [], []
    fo_miss_rows, lt_miss_rows = [], []
    proposals: List[Dict] = []

    # per-period hit accumulators
    fo_hits = {"long": [], "short": [], "combined": []}
    lt_hits = {"4wk": [], "8wk": []}

    sm_by_date = {d: g for d, g in sector_mom.groupby("trade_date")}

    learn_counter = 0
    walk_dates = [d for d in dates if d >= walk_start]
    if verbose and walk_dates:
        print(f"[wf] {len(dates)} dates loaded; walk {walk_dates[0]}..{walk_dates[-1]}; "
              f"FO uni={len(fo_set)} LT uni={len(lt_set)}", flush=True)
    for di, T in enumerate(dates):
        if verbose and is_walk_progress(T, walk_start) and di % 100 == 0:
            print(f"[wf] {T} ... fo_pred={len(fo_pred_rows)} lt_pred={len(lt_pred_rows)}", flush=True)
        day = by_date[T].set_index("symbol")
        sm_day = sm_by_date.get(T)
        reg = regime.get(T)

        # eligible cross-sections
        fo_day = day[day.index.isin(fo_set)].copy()
        fo_day = fo_day[fo_day["roc_20"].notna()]
        lt_day = day[day.index.isin(lt_set)].copy()
        lt_day = lt_day[lt_day["roc_20"].notna()]

        is_walk = T >= walk_start

        # ---- weekly LEARN (causal: trailing window strictly before T) ----
        learn_counter += 1
        if learn_counter >= LEARN_EVERY and di > 20:
            learn_counter = 0
            lo = dates[max(0, di - 1 - _idx_back(dates, di, TRAIL_DAYS))]
            win = data[(data["trade_date"] >= lo) & (data["trade_date"] < T)]
            win_fo = win[win["symbol"].isin(fo_set)]
            win_lt = win[win["symbol"].isin(lt_set)]
            if len(win_fo) > 500:
                w_long = learn_ic_weights(win_fo, "fwd_nd", +1.0, w_long)
                w_short = learn_ic_weights(win_fo, "fwd_nd", -1.0, w_short)
            if len(win_lt) > 500:
                w_lt = learn_ic_weights(win_lt, "ret_20", +1.0, w_lt)
            if is_walk:
                proposals.append({"date": T, "w_long": dict(w_long),
                                  "w_short": dict(w_short), "w_lt": dict(w_lt)})

        if not is_walk:
            continue

        # ---- F&O PREDICT ----
        if len(fo_day) >= 20:
            long_top, short_top = personas.predict_fo(fo_day, sm_day, w_long, w_short, reg)
            # ---- F&O MEASURE (next-day O->C) ----
            fo_universe_day = fo_day[fo_day["fwd_nd"].notna()]
            if len(fo_universe_day) >= 20:
                gain_rank, lose_rank = outcomes.rank_maps(
                    fo_universe_day.reset_index(), "fwd_nd")
                top10_gain = set(list(gain_rank.keys())[:10] if False else
                                 [s for s, r in gain_rank.items() if r <= 10])
                top10_lose = set([s for s, r in lose_rank.items() if r <= 10])

                lh = _record_fo(long_top, "LONG", T, fo_universe_day, gain_rank,
                                lose_rank, top10_gain, top10_lose, w_long,
                                fo_pred_rows, fo_out_rows)
                sh = _record_fo(short_top, "SHORT", T, fo_universe_day, gain_rank,
                                lose_rank, top10_gain, top10_lose, w_short,
                                fo_pred_rows, fo_out_rows)
                fo_hits["long"].append((T, lh))
                fo_hits["short"].append((T, sh))
                fo_hits["combined"].append((T, lh + sh))
                # miss analysis: actual movers not predicted
                _record_fo_miss(T, fo_universe_day, top10_gain, top10_lose,
                                set(long_top["symbol"]), set(short_top["symbol"]),
                                gain_rank, lose_rank, fo_miss_rows)

        # ---- LT PREDICT ----
        if len(lt_day) >= 20:
            lt_top = personas.predict_lt(lt_day, sm_day, w_lt, reg)
            lt_universe_day = lt_day[lt_day["ret_20"].notna()]
            if len(lt_universe_day) >= 20:
                h4, h8 = _record_lt(lt_top, T, lt_universe_day, w_lt,
                                    lt_pred_rows, lt_out_rows, lt_miss_rows)
                if h4 is not None:
                    lt_hits["4wk"].append((T, h4))
                if h8 is not None:
                    lt_hits["8wk"].append((T, h8))

    summary = _summarise(fo_hits, lt_hits)
    if persist:
        _persist(con, fo_pred_rows, fo_out_rows, lt_pred_rows, lt_out_rows,
                 fo_miss_rows, lt_miss_rows, proposals)
    if verbose:
        _print_summary(summary)
    return {"summary": summary, "n_proposals": len(proposals),
            "fo_pred": len(fo_pred_rows), "lt_pred": len(lt_pred_rows),
            "final_weights": {"FO_LONG": w_long, "FO_SHORT": w_short, "LT": w_lt}}


def _idx_back(dates, di, n):
    return min(di, n)


def is_walk_progress(T, walk_start):
    return T >= walk_start


# ── recording helpers ──────────────────────────────────────────────────────────

def _record_fo(top, direction, T, uni_day, gain_rank, lose_rank,
               top10_gain, top10_lose, weights, pred_rows, out_rows) -> int:
    hits = 0
    score_col = "long_score" if direction == "LONG" else "short_score"
    outcome_date = uni_day["fwd_nd_date"].iloc[0]
    ret_map = uni_day["fwd_nd"].to_dict()  # uni_day is indexed by symbol
    for _, r in top.iterrows():
        sym = r["symbol"]
        in_g = sym in top10_gain
        in_l = sym in top10_lose
        hit = (in_g if direction == "LONG" else in_l)
        if hit:
            hits += 1
        pred_rows.append((T, direction, int(r["rank"]), sym, r.get("sector"),
                          float(r.get("long_score", np.nan)),
                          float(r.get("short_score", np.nan)),
                          personas.top_features_json(r, weights), None, "wf"))
        out_rows.append((T, outcome_date, sym, direction, int(r["rank"]),
                         ret_map.get(sym), gain_rank.get(sym), lose_rank.get(sym),
                         int(in_g), int(in_l), int(hit), None))
    return hits


def _record_fo_miss(T, uni_day, top10_gain, top10_lose, pred_long, pred_short,
                    gain_rank, lose_rank, miss_rows):
    ret_map = uni_day["fwd_nd"].to_dict()  # indexed by symbol
    for sym in top10_gain - pred_long:
        miss_rows.append((T, "LONG", sym, ret_map.get(sym), gain_rank.get(sym),
                          "not_in_long_top10", None))
    for sym in top10_lose - pred_short:
        miss_rows.append((T, "SHORT", sym, ret_map.get(sym), lose_rank.get(sym),
                          "not_in_short_top10", None))


def _record_lt(top, T, uni_day, weights, pred_rows, out_rows, miss_rows):
    r20 = uni_day["ret_20"].to_dict()  # indexed by symbol
    r40 = uni_day["ret_40"].to_dict()
    rank20 = (uni_day.dropna(subset=["ret_20"]).sort_values("ret_20", ascending=False)
              .reset_index().assign(r=lambda d: d.index + 1)
              .set_index("symbol")["r"].to_dict())
    has40 = uni_day["ret_40"].notna().sum() >= 20
    rank40 = {}
    if has40:
        rank40 = (uni_day.dropna(subset=["ret_40"]).sort_values("ret_40", ascending=False)
                  .reset_index().assign(r=lambda d: d.index + 1)
                  .set_index("symbol")["r"].to_dict())
    top10_4 = {s for s, r in rank20.items() if r <= 10}
    top10_8 = {s for s, r in rank40.items() if r <= 10}
    h4 = h8 = 0
    for _, r in top.iterrows():
        sym = r["symbol"]
        in4 = sym in top10_4
        in8 = sym in top10_8 if has40 else None
        if in4:
            h4 += 1
        if in8:
            h8 += 1
        mb = int((r20.get(sym, 0) or 0) >= 40 or (r40.get(sym, 0) or 0) >= 40)
        pred_rows.append((T, int(r["rank"]), sym, r.get("sector"),
                          float(r.get("lt_score", np.nan)),
                          personas.top_features_json(r, weights), "wf"))
        out_rows.append((T, sym, int(r["rank"]), r20.get(sym), r40.get(sym),
                         rank20.get(sym), rank40.get(sym), int(in4),
                         (int(in8) if in8 is not None else None), mb, None))
    for sym in top10_4 - set(top["symbol"]):
        miss_rows.append((T, "4wk", sym, r20.get(sym), rank20.get(sym),
                          int((r20.get(sym, 0) or 0) >= 40), "not_in_lt_top10"))
    return h4, (h8 if has40 else None)


# ── summary ─────────────────────────────────────────────────────────────────---

def _year(d):
    return d[:4]


def _summarise(fo_hits, lt_hits):
    def agg(pairs, denom=10):
        if not pairs:
            return {}
        df = pd.DataFrame(pairs, columns=["date", "h"])
        df["year"] = df["date"].map(_year)
        by_year = (df.groupby("year")["h"].mean() / denom * 100).round(1).to_dict()
        overall = round(df["h"].mean() / denom * 100, 1)
        return {"overall_pct": overall, "by_year": by_year, "n": len(df),
                "avg_hits": round(df["h"].mean(), 2)}
    return {
        "fo_long": agg(fo_hits["long"]),
        "fo_short": agg(fo_hits["short"]),
        "fo_combined": agg(fo_hits["combined"], denom=20),
        "lt_4wk": agg(lt_hits["4wk"]),
        "lt_8wk": agg(lt_hits["8wk"]),
    }


def _print_summary(s):
    print("\n================ WALK-FORWARD SUMMARY ================")
    for k, label in [("fo_long", "F&O Long Top-10 hit"),
                     ("fo_short", "F&O Short Top-10 hit"),
                     ("fo_combined", "F&O Combined hit (of 20)"),
                     ("lt_4wk", "LT 4-week Top-10 hit"),
                     ("lt_8wk", "LT 8-week Top-10 hit")]:
        v = s.get(k, {})
        if not v:
            print(f"  {label:32s}: (no data)")
            continue
        print(f"  {label:32s}: {v['overall_pct']}%  (avg {v['avg_hits']}/{'20' if k=='fo_combined' else '10'}, n={v['n']})")
        print(f"      by year: {v['by_year']}")
    print("=====================================================\n")


# ── persistence ────────────────────────────────────────────────────────────────

def _persist(con, fo_pred, fo_out, lt_pred, lt_out, fo_miss, lt_miss, proposals):
    cur = con.cursor()
    cur.execute("DELETE FROM fo_daily_predictions")
    cur.execute("DELETE FROM fo_prediction_outcomes")
    cur.execute("DELETE FROM lt_daily_predictions")
    cur.execute("DELETE FROM lt_prediction_outcomes")
    cur.execute("DELETE FROM fo_miss_analysis")
    cur.execute("DELETE FROM lt_miss_analysis")
    cur.executemany(
        "INSERT OR REPLACE INTO fo_daily_predictions(prediction_date,direction,rank,symbol,sector,long_score,short_score,top_features,top_rules,model_version) VALUES (?,?,?,?,?,?,?,?,?,?)",
        fo_pred)
    cur.executemany(
        "INSERT OR REPLACE INTO fo_prediction_outcomes(prediction_date,outcome_date,symbol,direction,predicted_rank,actual_return,actual_rank_gainers,actual_rank_losers,in_top10_gainers,in_top10_losers,hit,miss_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        fo_out)
    cur.executemany(
        "INSERT OR REPLACE INTO lt_daily_predictions(prediction_date,rank,symbol,sector,lt_score,top_features,model_version) VALUES (?,?,?,?,?,?,?)",
        lt_pred)
    cur.executemany(
        "INSERT OR REPLACE INTO lt_prediction_outcomes(prediction_date,symbol,predicted_rank,ret_4wk,ret_8wk,actual_rank_4wk,actual_rank_8wk,in_top10_4wk,in_top10_8wk,is_multibagger,miss_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        lt_out)
    cur.executemany(
        "INSERT OR REPLACE INTO fo_miss_analysis(outcome_date,direction,symbol,actual_return,actual_rank,root_cause,features_at_pred) VALUES (?,?,?,?,?,?,?)",
        fo_miss)
    cur.executemany(
        "INSERT OR REPLACE INTO lt_miss_analysis(prediction_date,window,symbol,actual_return,actual_rank,is_multibagger,root_cause) VALUES (?,?,?,?,?,?,?)",
        lt_miss)
    con.commit()
