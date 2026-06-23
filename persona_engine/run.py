"""
CLI orchestrator for the persona engine.

Usage (from the worktree root, with the project python):
  python -m persona_engine.run init-schema
  python -m persona_engine.run build-features [--start 2021-01-01] [--end YYYY-MM-DD]
  python -m persona_engine.run walkforward [--start 2022-01-01] [--end YYYY-MM-DD]
  python -m persona_engine.run learn [--week-ending YYYY-MM-DD]
  python -m persona_engine.run reviews
  python -m persona_engine.run status
"""
from __future__ import annotations

import argparse

from persona_engine import db, schema, model, features, universe, engine, learn, review


def cmd_init_schema(args):
    con = db.connect()
    schema.create_all(con)
    model.seed_baseline_weights(con)
    print("schema created + baseline weights seeded ->", db.resolve_db_path())
    con.close()


def cmd_build_features(args):
    con = db.connect()
    syms = [r[0] for r in con.execute(
        "SELECT symbol FROM universe_master WHERE in_nifty500=1")]
    feats = features.build_features(con, start=args.start, end=args.end,
                                    symbols=syms, write=True)
    print(f"features built: {len(feats)} rows -> persona_signal_features")
    con.close()


def cmd_walkforward(args):
    con = db.connect()
    model.seed_baseline_weights(con)
    fo, lt = universe.get_universes(con, as_of_date=args.as_of)
    res = engine.run_walkforward(con, fo, lt, walk_start=args.start,
                                 train_start=args.train_start, end=args.end,
                                 persist=not args.no_persist, verbose=True)
    print("proposals (research weight vectors):", res["n_proposals"])
    con.close()


def cmd_learn(args):
    con = db.connect()
    fo, lt = universe.get_universes(con, as_of_date=args.as_of)
    out = learn.run_learning(con, fo, lt, week_ending=args.week_ending,
                             walk_start=args.start)
    print(f"learning: {out['n_proposals']} proposals, {out['n_active']} ACTIVE")
    for tag in ("fo_long", "fo_short", "lt"):
        print(f"\n  top {tag} rules:")
        for p in out[tag][:6]:
            print(f"    {p['rule_name']:34s} hit={p['hit_rate']:5.1f}% "
                  f"n={p['n_occurrences']:5d} yrs={p['n_years']} "
                  f"reg={p['regimes_tested']} sec={p['n_sectors']} "
                  f"worst={p['worst_year_hr']:.1f}% [{p['status']}]")
    con.close()


def cmd_reviews(args):
    con = db.connect()
    n = review.build_all_fo_reviews(con)
    print(f"built {n} F&O daily reviews")
    con.close()


def cmd_status(args):
    con = db.connect()

    def c(t):
        try:
            return con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            return "—"
    print("DB:", db.resolve_db_path())
    for t in ["persona_signal_features", "fo_daily_predictions",
              "fo_prediction_outcomes", "lt_daily_predictions",
              "lt_prediction_outcomes", "fo_miss_analysis", "learning_proposals",
              "fo_daily_review", "lt_weekly_review"]:
        print(f"  {t:28s}: {c(t)}")
    con.close()


def main():
    p = argparse.ArgumentParser(prog="persona_engine.run")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init-schema"); s.set_defaults(fn=cmd_init_schema)

    s = sub.add_parser("build-features")
    s.add_argument("--start", default="2021-01-01")
    s.add_argument("--end", default=None)
    s.set_defaults(fn=cmd_build_features)

    s = sub.add_parser("walkforward")
    s.add_argument("--start", default="2022-01-01")
    s.add_argument("--train-start", default="2021-01-01")
    s.add_argument("--end", default=None)
    s.add_argument("--as-of", default="2026-06-22")
    s.add_argument("--no-persist", action="store_true")
    s.set_defaults(fn=cmd_walkforward)

    s = sub.add_parser("learn")
    s.add_argument("--week-ending", default="2026-06-22")
    s.add_argument("--start", default="2022-01-01")
    s.add_argument("--as-of", default="2026-06-22")
    s.set_defaults(fn=cmd_learn)

    s = sub.add_parser("reviews"); s.set_defaults(fn=cmd_reviews)
    s = sub.add_parser("status"); s.set_defaults(fn=cmd_status)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
