"""
Explainable review output (spec §2.8 daily F&O, §3.6 LT weekly).

Generates the plain-English "what I predicted / what actually happened / what I got
right / what I missed / what I learned" record the spec mandates, from the persisted
predictions + outcomes + miss analysis. Stored in fo_daily_review / lt_weekly_review.
"""
from __future__ import annotations

import json
import sqlite3
from typing import List, Optional

import pandas as pd


def _names(con, sql, params):
    return [r[0] for r in con.execute(sql, params).fetchall()]


def build_fo_daily_review(con: sqlite3.Connection, prediction_date: str) -> Optional[dict]:
    preds = pd.read_sql_query(
        "SELECT direction,rank,symbol FROM fo_daily_predictions WHERE prediction_date=? ORDER BY direction,rank",
        con, params=[prediction_date])
    if preds.empty:
        return None
    outs = pd.read_sql_query(
        "SELECT * FROM fo_prediction_outcomes WHERE prediction_date=?",
        con, params=[prediction_date])
    miss = pd.read_sql_query(
        "SELECT direction,symbol,actual_return,actual_rank FROM fo_miss_analysis WHERE outcome_date=? ORDER BY direction,actual_rank",
        con, params=[prediction_date])

    longs = preds[preds.direction == "LONG"]["symbol"].tolist()
    shorts = preds[preds.direction == "SHORT"]["symbol"].tolist()
    outcome_date = outs["outcome_date"].iloc[0] if not outs.empty else None

    long_hit = int(((outs.direction == "LONG") & (outs.hit == 1)).sum())
    short_hit = int(((outs.direction == "SHORT") & (outs.hit == 1)).sum())
    long_correct = outs[(outs.direction == "LONG") & (outs.hit == 1)]["symbol"].tolist()

    missed_winners = miss[miss.direction == "LONG"]["symbol"].tolist()
    missed_losers = miss[miss.direction == "SHORT"]["symbol"].tolist()

    # Full actual Top-10 = the picks we got right (with their rank) UNION the movers
    # we missed (from miss_analysis), ordered by actual rank.
    def _full_actual(direction, rank_col):
        hit_rows = outs[(outs.direction == direction) & (outs.hit == 1)]
        ranks = {r["symbol"]: r[rank_col] for _, r in hit_rows.iterrows()}
        msub = miss[miss.direction == direction]
        for _, r in msub.iterrows():
            ranks[r["symbol"]] = r["actual_rank"]
        return [s for s, _ in sorted(ranks.items(), key=lambda kv: (kv[1] is None, kv[1]))][:10]

    actual_gainers = _full_actual("LONG", "actual_rank_gainers")
    actual_losers = _full_actual("SHORT", "actual_rank_losers")

    review = {
        "prediction_date": prediction_date,
        "outcome_date": outcome_date,
        "predicted_long": longs,
        "predicted_short": shorts,
        "actual_top10_gainers": actual_gainers,
        "actual_top10_losers": actual_losers,
        "long_hit_rate": f"{long_hit} of 10 ({long_hit*10}%)",
        "short_hit_rate": f"{short_hit} of 10 ({short_hit*10}%)",
        "combined_hit_rate": f"{long_hit+short_hit} of 20",
        "long_matched": long_correct,
        "missed_winners": missed_winners[:10],
        "missed_losers": missed_losers[:10],
    }

    text = (
        f"F&O review for {prediction_date} (outcome {outcome_date}):\n"
        f"  Predicted LONG : {', '.join(longs)}\n"
        f"  Predicted SHORT: {', '.join(shorts)}\n"
        f"  Actual top gainers: {', '.join(actual_gainers)}\n"
        f"  Actual top losers : {', '.join(actual_losers)}\n"
        f"  Long hit {long_hit}/10; Short hit {short_hit}/10; Combined {long_hit+short_hit}/20.\n"
        f"  Matched longs: {', '.join(long_correct) or '—'}\n"
        f"  Missed winners: {', '.join(missed_winners[:6]) or '—'}\n"
    )

    con.execute(
        "INSERT OR REPLACE INTO fo_daily_review(prediction_date,outcome_date,review_json,review_text,long_hit,short_hit,combined_hit) VALUES (?,?,?,?,?,?,?)",
        (prediction_date, outcome_date, json.dumps(review), text,
         long_hit, short_hit, long_hit + short_hit))
    con.commit()
    return review


def build_all_fo_reviews(con: sqlite3.Connection) -> int:
    dates = _names(con, "SELECT DISTINCT prediction_date FROM fo_daily_predictions ORDER BY prediction_date", [])
    n = 0
    for d in dates:
        if build_fo_daily_review(con, d):
            n += 1
    return n


def build_lt_weekly_review(con: sqlite3.Connection, week_ending: str,
                           dates_in_week: List[str]) -> Optional[dict]:
    qs = ",".join("?" * len(dates_in_week))
    outs = pd.read_sql_query(
        f"SELECT * FROM lt_prediction_outcomes WHERE prediction_date IN ({qs})",
        con, params=dates_in_week)
    if outs.empty:
        return None
    h4 = outs["in_top10_4wk"].mean() * 100
    h8 = outs["in_top10_8wk"].dropna().mean() * 100 if outs["in_top10_8wk"].notna().any() else None
    mb = outs[outs.is_multibagger == 1]["symbol"].unique().tolist()
    review = {
        "week_ending": week_ending,
        "n_predictions": int(len(outs)),
        "avg_4wk_hit_pct": round(float(h4), 1),
        "avg_8wk_hit_pct": (round(float(h8), 1) if h8 is not None else None),
        "multibaggers_captured": mb,
        "avg_ret_4wk": round(float(outs["ret_4wk"].mean()), 2),
        "avg_ret_8wk": (round(float(outs["ret_8wk"].mean()), 2)
                        if outs["ret_8wk"].notna().any() else None),
    }
    text = (
        f"LT weekly review (week ending {week_ending}):\n"
        f"  4-wk Top-10 hit: {review['avg_4wk_hit_pct']}%  | 8-wk: {review['avg_8wk_hit_pct']}%\n"
        f"  Avg 4-wk fwd return of picks: {review['avg_ret_4wk']}%\n"
        f"  Multibaggers captured: {', '.join(mb) or '—'}\n"
    )
    con.execute(
        "INSERT OR REPLACE INTO lt_weekly_review(week_ending,review_json,review_text,hit_4wk,hit_8wk) VALUES (?,?,?,?,?)",
        (week_ending, json.dumps(review), text, review["avg_4wk_hit_pct"],
         review["avg_8wk_hit_pct"]))
    con.commit()
    return review
