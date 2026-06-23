"""What happened May-12 -> today: run the volatility TOUCH screen on the freshly-fetched
days and show per-day results + the picks."""
from __future__ import annotations
import sys
import pandas as pd
from persona_engine import db, universe
from persona_engine.touch_tradelog import build, summarize

START = sys.argv[1] if len(sys.argv) > 1 else "2026-05-12"


def run(con, fo):
    picks = build(con, fo)
    picks = picks[picks["date"] >= START]
    if picks.empty:
        print("no data in window"); return
    daily, monthly, yearly, overall = summarize(picks)
    print(f"=== WINDOW {picks['date'].min()} .. {picks['date'].max()} ({daily.shape[0]} trading days) ===")
    print(overall.T.to_string(header=False))
    print("\n=== PER-DAY (touch of 5) ===")
    print("date        @1% @2% @3%  | top-5 most-volatile-morning picks (max_up% / max_dn%)")
    for _, r in daily.iterrows():
        day = picks[picks["date"] == r["date"]].sort_values("rk")
        names = ", ".join(f"{x.symbol}({x.up_touch:+.1f}/{x.dn_touch:+.1f})" for x in day.itertuples())
        print(f"{r['date']}  {int(r['touch1'])}   {int(r['touch2'])}   {int(r['touch3'])}  | {names}")


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("RECENT_DONE")
