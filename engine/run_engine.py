"""Command-line entry point.

Examples (Anaconda Prompt or PowerShell, from inside this folder):

    python run_engine.py demo
    python run_engine.py leakcheck
    python run_engine.py run --csv data/panel.csv --target 0.01 --stop 0.005
    python run_engine.py run --csv data/panel.csv --no-graph          # A/B the graph layer
    python run_engine.py signals --csv data/panel.csv
    python run_engine.py ab --csv data/panel.csv                      # graph on vs off

SQLite workflow (daily 2022-2026 + intraday May 2024-2026):

    python run_engine.py inspect-db     --db market.db
    python run_engine.py build-intraday --db market.db --bar-minutes 5
    python run_engine.py calibrate      --db market.db
    python run_engine.py run            --db market.db
    python run_engine.py run            --db market.db --exact-labels
"""
from __future__ import annotations

import argparse
import os
import sys

from datetime import datetime

import pandas as pd

from state_engine import Config, data, leakcheck, sqlite_io, intraday
from state_engine import baseline as bl
from state_engine import attribution as attr
from state_engine import barriers as bar
from state_engine import overlay as ov
from state_engine.registry import StateRegistry
from state_engine import basket as bk
from state_engine import replicate as rep
from state_engine import pilot as pl
from state_engine.pipeline import run_pipeline, prepare_panel, todays_signals

OUT = "outputs"


def _cfg(a) -> Config:
    c = Config()
    if a.target is not None:
        c.target_pct = a.target
    if a.stop is not None:
        c.stop_pct = a.stop
    if a.horizon is not None:
        c.horizon_days = a.horizon
    if a.label_mode:
        c.label_mode = a.label_mode
    if a.state_method:
        c.state_method = a.state_method
    if a.train_months is not None:
        c.train_months_min = a.train_months
    if a.side:
        c.side = a.side
    if a.wilson_z:
        c.wilson_z = a.wilson_z
    if a.min_support:
        c.min_support_train = a.min_support
    if a.jobs:
        c.n_jobs = a.jobs
    if a.timeframes:
        c.use_timeframes = True
    if a.structure:
        c.structure = a.structure
    if a.cost is not None:
        c.cost_pct = a.cost
    if a.no_graph:
        c.use_graph = False
    if a.no_interactions:
        c.use_interactions = False
    if a.state_features is not None:
        c.state_features = a.state_features
    return c


def _db(a) -> sqlite_io.DBConfig:
    if a.db_config:
        return sqlite_io.DBConfig.from_json(a.db_config)
    db = sqlite_io.suggest_config(a.db)
    print(f"  auto-detected: daily={db.daily_table!r} intraday={db.intraday_table!r}")
    print("  (if that is wrong, write a db_config.json and pass --db-config)")
    return db


def _cache_path(a) -> str:
    return a.intraday_cache or (
        f"data/intraday_{a.bar_minutes}min_"
        f"{int((a.target or 0.01) * 10000)}_{int((a.stop or 0.005) * 10000)}.parquet")


def _spec(a) -> intraday.IntradaySpec:
    return intraday.IntradaySpec(
        target_pct=a.target if a.target is not None else 0.01,
        stop_pct=a.stop if a.stop is not None else 0.005,
        bar_minutes=a.bar_minutes,
        ambiguous_policy=a.ambiguous,
    )


def _load(a) -> pd.DataFrame:
    if a.db:
        db = _db(a)
        print(f"loading daily panel from {a.db}")
        raw = sqlite_io.load_daily(db, start=a.start, end=a.end)
        if getattr(a, "drop_bad_bars", False):
            raw = data.clean(raw)
        if not getattr(a, "keep_indices", False):
            stocks, idx = data.split_universe(raw)
            if len(idx):
                raw = stocks
        return data._validate(raw)
    if a.csv:
        print(f"loading {a.csv}")
        return data.load_csv(a.csv)
    print(f"generating synthetic panel ({a.symbols} symbols x {a.days} days)")
    return data.make_synthetic_panel(n_symbols=a.symbols, n_days=a.days, seed=a.seed)


def _save(res: dict, tag: str) -> None:
    os.makedirs(OUT, exist_ok=True)
    for k in ("folds", "lifecycle", "trades"):
        df = res.get(k)
        if isinstance(df, pd.DataFrame) and not df.empty:
            p = os.path.join(OUT, f"{tag}_{k}.csv")
            df.to_csv(p, index=False)
            print(f"  wrote {p}  ({len(df):,} rows)")


def main() -> int:
    p = argparse.ArgumentParser(description="State-discovery research engine")
    p.add_argument("command", choices=["demo", "run", "leakcheck", "signals", "ab",
                                       "inspect-db", "build-intraday", "calibrate",
                                       "index-db", "baseline", "attribute",
                                       "evidence", "both-sides", "tune-barriers", "overlay", "check-data", "basket", "replicate", "pilot"])
    p.add_argument("--keep-indices", action="store_true",
                   help="keep zero-volume index symbols in the tradeable universe")
    p.add_argument("--drop-bad-bars", action="store_true",
                   help="drop malformed bars and broken-feed symbols")
    p.add_argument("--side", choices=["long", "short"],
                   help="which side to mine; long and short are mined separately")
    p.add_argument("--symbol", help="single-symbol baseline report")
    p.add_argument("--db", help="path to the SQLite database")
    p.add_argument("--db-config", help="db_config.json with explicit table/column names")
    p.add_argument("--intraday-cache", help="path to the collapsed intraday cache")
    p.add_argument("--bar-minutes", type=int, default=5,
                   help="resolution for first-touch resolution (5 is usually enough)")
    p.add_argument("--ambiguous", default="loss", choices=["loss", "win", "drop"],
                   help="how to treat both barriers touched inside one bar")
    p.add_argument("--exact-labels", action="store_true",
                   help="use intraday first-touch labels (restricts the sample)")
    p.add_argument("--start"); p.add_argument("--end")
    p.add_argument("--symbols-limit", type=int,
                   help="use only the N most liquid symbols (fast iteration)")
    p.add_argument("--csv", help="long panel CSV: date,symbol,open,high,low,close,volume[,sector]")
    p.add_argument("--symbols", type=int, default=40)
    p.add_argument("--days", type=int, default=1400)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--target", type=float)
    p.add_argument("--stop", type=float)
    p.add_argument("--horizon", type=int)
    p.add_argument("--label-mode", choices=["conservative", "optimistic", "close"])
    p.add_argument("--state-method", choices=["grid", "tree"])
    p.add_argument("--state-features", type=int)
    p.add_argument("--train-months", type=int)
    p.add_argument("--entry-step", type=int, default=15,
                   help="minutes between candidate entry times in the scan")
    p.add_argument("--intraday-table",
                   help="override the intraday table, e.g. ohlc_1min")
    p.add_argument("--pilot-symbols", help="comma separated, default ADANIENT,CARTRADE")
    p.add_argument("--top-n", type=int,
                   help="names the model picks per day (default: your median)")
    p.add_argument("--policy", default="hold",
                   choices=["hold", "arm1.0_floor0.50", "arm1.0_floor0.75",
                            "arm0.75_floor0.50", "arm1.0_give0.50",
                            "trail0.75", "trail0.50"])
    p.add_argument("--capital", type=float, default=50000.0,
                   help="rupees allocated per position")
    p.add_argument("--jobs", type=int, default=1,
                   help="parallel workers (folds are independent)")
    p.add_argument("--concentration", action="store_true",
                   help="report whether each state is broad or a few stocks")
    p.add_argument("--timeframes", action="store_true",
                   help="add weekly and monthly point-in-time features")
    p.add_argument("--run-id", help="label this research run in the state registry")
    p.add_argument("--dna-universe", action="store_true",
                   help="also show DNA against the whole universe (stock selection)")
    p.add_argument("--split-date", default="2026-01-01",
                   help="holdout boundary for the pre-specified rule tests")
    p.add_argument("--filter-support", type=int, default=20)
    p.add_argument("--filter-bins", type=int, default=4)
    p.add_argument("--filter-features", type=int, default=1)
    p.add_argument("--filter-train-months", type=int, default=8)
    p.add_argument("--trades", help="CSV trade log: trade_date,symbol,stock_ret_pct")
    p.add_argument("--wilson-z", type=float,
                   help="one-sided confidence for promotion: 1.64=90%%, 2.33=99%%")
    p.add_argument("--min-support", type=int,
                   help="minimum occurrences in TRAIN before a state can promote")
    p.add_argument("--structure", choices=["bracket", "hybrid"],
                   help="hybrid = disaster stop only, exit at the close")
    p.add_argument("--cost", type=float, help="round-trip cost, e.g. 0.0011")
    p.add_argument("--no-graph", action="store_true")
    p.add_argument("--no-interactions", action="store_true")
    p.add_argument("--tag", default="run")
    a = p.parse_args()

    cfg = _cfg(a)

    # ---- database-only commands ----
    if a.command == "inspect-db":
        if not a.db:
            print("need --db"); return 1
        sqlite_io.inspect_db(a.db)
        db = sqlite_io.suggest_config(a.db)
        os.makedirs(OUT, exist_ok=True)
        db.to_json(os.path.join(OUT, "db_config.json"))
        print(f"\n  wrote a starter mapping to {OUT}/db_config.json - CHECK IT, then")
        print("  pass it back with --db-config.")
        return 0

    if a.command == "basket":
        if not (a.db and a.trades):
            print("need --db and --trades"); return 1
        db = _db(a)
        t = ov.load_trade_log(a.trades, cost=cfg.cost_pct)
        ov.summarise_log(t)
        print(f"\n  pulling {a.bar_minutes}-min bars for the traded symbol-days only")
        paths = bk.load_trade_paths(db, t, bar_minutes=a.bar_minutes)
        b = bk.build_basket(paths)
        os.makedirs(OUT, exist_ok=True)
        prof = bk.basket_profile(b, target=(a.target or 0.01),
                                 bar_minutes=a.bar_minutes)
        prof.to_csv(os.path.join(OUT, "basket_profile.csv"), index=False)
        bk.entry_timing(paths, bar_minutes=a.bar_minutes).to_csv(
            os.path.join(OUT, "basket_entry_timing.csv"), index=False)
        bk.simulate_policies(b, cost=cfg.cost_pct).to_csv(
            os.path.join(OUT, "basket_policies.csv"), index=False)
        bk.walk_forward_policy(b, cost=cfg.cost_pct).to_csv(
            os.path.join(OUT, "basket_wf_policy.csv"), index=False)
        tl, mth = bk.simulate_book(paths, b, policy=a.policy,
                                   capital_per_stock=a.capital,
                                   cost=cfg.cost_pct)
        tl.to_csv(os.path.join(OUT, f"book_trades_{a.policy}.csv"), index=False)
        mth.to_csv(os.path.join(OUT, f"book_monthly_{a.policy}.csv"), index=False)
        print(f"\n  wrote {OUT}/book_trades_{a.policy}.csv and "
              f"book_monthly_{a.policy}.csv")
        return 0

    if a.command == "pilot":
        if not a.db:
            print("need --db"); return 1
        db = _db(a)
        if a.intraday_table:
            db.intraday_table = a.intraday_table
        syms = ([x.strip() for x in a.pilot_symbols.split(",")]
                if a.pilot_symbols else ("ADANIENT", "CARTRADE"))
        pcfg = pl.PilotConfig(symbols=syms, notional=a.capital or 25000.0,
                              cost_pct=cfg.cost_pct, bar_minutes=a.bar_minutes,
                              start=a.start or "2022-01-01",
                              end=a.end or "2025-12-31")
        os.makedirs(OUT, exist_ok=True)
        print(f"\nTWO-STOCK PILOT   {', '.join(pcfg.symbols)}   "
              f"{pcfg.start} -> {pcfg.end}   Rs {pcfg.notional:,.0f}/stock")
        print("  2026 is deliberately excluded and stays untouched as holdout.")
        bars = pl.load_bars(db, pcfg)
        tr, summ = pl.intraday_baseline(bars, pcfg)
        tr.to_csv(os.path.join(OUT, "pilot_trades.csv"), index=False)
        summ.to_csv(os.path.join(OUT, "pilot_intraday.csv"), index=False)
        pl.threshold_profile(tr, pcfg).to_csv(
            os.path.join(OUT, "pilot_thresholds.csv"), index=False)
        pl.directional_scorecard(summ, tr, pcfg).to_csv(
            os.path.join(OUT, "pilot_scorecard.csv"), index=False)
        daily = sqlite_io.load_daily(db, symbols=list(pcfg.symbols),
                                     start=pcfg.start, end=pcfg.end)
        pl.multi_session(daily, pcfg).to_csv(
            os.path.join(OUT, "pilot_multisession.csv"), index=False)
        pl.leverage_view(summ, pcfg).to_csv(
            os.path.join(OUT, "pilot_leverage.csv"), index=False)
        pl.overnight_split(daily, pcfg).to_csv(
            os.path.join(OUT, "pilot_overnight.csv"), index=False)
        es = pl.entry_scan(bars, pcfg, step_min=a.entry_step)
        if not es.empty:
            es.to_csv(os.path.join(OUT, "pilot_entry_scan.csv"), index=False)
        print(f"\n  wrote 6 CSVs to {OUT}/")
        return 0

    if a.command == "check-data":
        if not a.db:
            print("need --db"); return 1
        data.diagnose(sqlite_io.load_daily(_db(a), start=a.start, end=a.end))
        return 0

    if a.command == "index-db":
        if not a.db:
            print("need --db"); return 1
        print("creating indexes (one-off, may take minutes on a large table)")
        sqlite_io.ensure_indexes(_db(a))
        return 0

    if a.command == "build-intraday":
        if not a.db:
            print("need --db"); return 1
        intraday.build_cache(_db(a), _spec(a), _cache_path(a),
                             start=a.start, end=a.end)
        return 0

    raw = _load(a)

    if a.symbols_limit:
        turnover = (raw["close"] * raw["volume"]).groupby(raw["symbol"]).median()
        keep = set(turnover.nlargest(a.symbols_limit).index)
        raw = raw[raw["symbol"].isin(keep)].reset_index(drop=True)
        print(f"  restricted to the {a.symbols_limit} most liquid symbols")

    cache = None
    if a.db or a.intraday_cache:
        try:
            cache = intraday.load_cache(_cache_path(a))
            print(f"  intraday cache    : {len(cache):,} rows")
        except FileNotFoundError:
            if a.command == "calibrate" or a.exact_labels:
                print("  no intraday cache - run `build-intraday` first")
                return 1

    if a.command == "replicate":
        if not a.trades:
            print("need --trades"); return 1
        from state_engine.pipeline import prepare_panel as _pp
        t = ov.load_trade_log(a.trades, cost=cfg.cost_pct)
        panel, feats = _pp(raw, cfg, True, intraday_cache=cache)
        r = rep.replicate(panel, feats, t, cfg, top_n=a.top_n)
        os.makedirs(OUT, exist_ok=True)
        r["folds"].to_csv(os.path.join(OUT, "replicate_folds.csv"), index=False)
        if not r["picks"].empty:
            r["picks"].to_csv(os.path.join(OUT, "replicate_picks.csv"), index=False)
            print(f"\n  wrote {OUT}/replicate_picks.csv - the model's own daily basket")
        return 0

    if a.command == "overlay":
        if not a.trades:
            print("need --trades path\\to\\tradelog.csv"); return 1
        from state_engine.pipeline import prepare_panel as _pp
        t = ov.load_trade_log(a.trades, cost=cfg.cost_pct)
        ov.summarise_log(t)
        panel, feats = _pp(raw, cfg, True, intraday_cache=cache)
        os.makedirs(OUT, exist_ok=True)
        dna = ov.characterise(panel, feats, t, within_symbols=True)
        if not dna.empty:
            dna.to_csv(os.path.join(OUT, "signal_dna_within.csv"), index=False)
        if a.dna_universe:
            dna_u = ov.characterise(panel, feats, t, within_symbols=False)
            if not dna_u.empty:
                dna_u.to_csv(os.path.join(OUT, "signal_dna_universe.csv"), index=False)
        try:
            ov.exit_audit(panel, t, cost=cfg.cost_pct)
            ov.stop_sweep(panel, t, cost=cfg.cost_pct)
        except Exception as e:
            print(f"  exit audit skipped: {e}")
        ov.winner_loser_dna(panel, feats, t)
        sep = ov.rank_separators(panel, feats, t)
        if not sep.empty:
            sep.to_csv(os.path.join(OUT, "separators.csv"), index=False)
        ov.monotone_null(panel, feats, t)
        pair = ov.greedy_pair(panel, feats, t)
        if not pair.empty:
            pair.to_csv(os.path.join(OUT, "pair_rules.csv"), index=False)
        hyp = ov.test_signal_contradictions(panel, t, cfg, split_date=a.split_date)
        if not hyp.empty:
            hyp.to_csv(os.path.join(OUT, "hypothesis_tests.csv"), index=False)

        # ---- persist everything discovered, so rediscovery can be measured ----
        reg = StateRegistry(os.path.join(OUT, "state_registry.json"))
        run_id = a.run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
        found = []
        if not hyp.empty:
            for _, r in hyp.iterrows():
                found.append({"state": f"{r['feature']}[{r['keep']}]",
                              "n": r.get("n_folds", 0) * 30,
                              "win_rate": r.get("win_kept"), "ret": r.get("ret_kept"),
                              "folds": r.get("n_folds"),
                              "folds_positive": r.get("folds_better"),
                              "holdout_ret": r.get("holdout_ret_kept")})
        if not pair.empty:
            for _, r in pair.head(15).iterrows():
                found.append({"state": r["rule"], "n": r["n"],
                              "win_rate": r["win"], "ret": r["ret"],
                              "folds": 0, "folds_positive": 0, "holdout_ret": None})
        if found:
            reg.record_run(run_id, {
                "symbols": int(raw["symbol"].nunique()),
                "trades": int(len(t)), "bins": a.filter_bins,
                "train_months": a.filter_train_months,
                "interactions": not a.no_interactions,
            }, pd.DataFrame(found))
            reg.save()
            reg.report()
        r = ov.filter_states(panel, feats, t, cfg,
                             min_support=a.filter_support, n_bins=a.filter_bins,
                             state_features=a.filter_features,
                             train_months=a.filter_train_months)
        if not r["folds"].empty:
            r["folds"].to_csv(os.path.join(OUT, "filter_folds.csv"), index=False)
        return 0

    if a.command == "tune-barriers":
        bar.run_tuning(data.build_index(raw), out_dir=OUT, cost=a.cost)
        return 0

    if a.command == "baseline":
        panel = data.build_index(raw)
        if a.symbol:
            bl.print_symbol_report(panel, a.symbol)
        else:
            bl.run_baseline(panel, out_dir=OUT)
        return 0

    if a.command in ("attribute", "evidence", "both-sides"):
        from state_engine.pipeline import prepare_panel as _pp
        sides = ["long", "short"] if a.command == "both-sides" else [cfg.side]
        for side in sides:
            c = _cfg(a); c.side = side
            print("\n" + "#" * 68 + f"\n# SIDE: {side.upper()}\n" + "#" * 68)
            panel, feats = _pp(raw, c, True, intraday_cache=cache,
                               exact_labels=a.exact_labels)
            if a.command == "evidence":
                ev = attr.todays_evidence(panel, feats, c)
                if ev.empty:
                    print("  nothing firing")
                else:
                    cols = ["symbol", "symbol_base", "p_state", "n_states",
                            "n_confirming", "graph_confirms", "p_final",
                            "total_lift", "signal"]
                    print(ev[cols].to_string(index=False))
                    os.makedirs(OUT, exist_ok=True)
                    ev.to_csv(os.path.join(OUT, f"evidence_{side}.csv"), index=False)
            else:
                r = attr.layered_walkforward(panel, feats, c)
                os.makedirs(OUT, exist_ok=True)
                for k in ("folds", "states", "summary"):
                    if not r[k].empty:
                        r[k].to_csv(os.path.join(OUT, f"attr_{side}_{k}.csv"), index=False)
                if a.concentration:
                    conc = attr.state_concentration(panel, feats, c)
                    if not conc.empty:
                        conc.to_csv(os.path.join(
                            OUT, f"concentration_{side}.csv"), index=False)
                if not r["states"].empty:
                    sc = attr.print_scorecard(r["states"])
                    sc.to_csv(os.path.join(OUT, f"scorecard_{side}.csv"), index=False)
                if not r["summary"].empty:
                    print("\n  top states by OOS lift over the SYMBOL base rate:")
                    cols = ["state", "n_test", "sym_base", "L1_test", "L2_test",
                            "state_lift_oos", "graph_lift_oos"]
                    print(r["summary"][cols].head(8).to_string(index=False))
        return 0

    if a.command == "calibrate":
        from state_engine.pipeline import prepare_panel as _pp
        panel, _ = _pp(raw, cfg, True, intraday_cache=cache)
        intraday.calibrate_labels(panel)
        return 0

    if a.command == "leakcheck":
        return 0 if leakcheck.run_all(raw, cfg) else 1

    if a.command in ("demo", "run"):
        if a.command == "demo":
            print("\n[demo] running the leakage audit first, then the full engine")
            if not leakcheck.run_all(raw, cfg):
                print("\nAUDIT FAILED - results below are not trustworthy")
        res = run_pipeline(raw, cfg, intraday_cache=cache, exact_labels=a.exact_labels)
        _save(res, a.tag)
        os.makedirs(OUT, exist_ok=True)
        cfg.to_json(os.path.join(OUT, f"{a.tag}_config.json"))
        return 0

    if a.command == "signals":
        panel, feats = prepare_panel(raw, cfg, True, cache, a.exact_labels)
        sig = todays_signals(panel, feats, cfg)
        if sig.empty:
            print("\nno qualifying states for the latest date")
        else:
            print("\nSIGNALS FOR NEXT SESSION (execute at open, no recomputation)")
            print(sig.to_string(index=False))
            os.makedirs(OUT, exist_ok=True)
            sig.to_csv(os.path.join(OUT, "signals.csv"), index=False)
        return 0

    if a.command == "ab":
        print("\n" + "#" * 68 + "\n# A: graph layer OFF\n" + "#" * 68)
        cfg_a = _cfg(a); cfg_a.use_graph = False
        res_a = run_pipeline(raw, cfg_a, intraday_cache=cache, exact_labels=a.exact_labels)
        print("\n" + "#" * 68 + "\n# B: graph layer ON\n" + "#" * 68)
        cfg_b = _cfg(a); cfg_b.use_graph = True
        res_b = run_pipeline(raw, cfg_b, intraday_cache=cache, exact_labels=a.exact_labels)

        def m(r):
            f = r["folds"].dropna(subset=["oos_lift"])
            return f["oos_lift"].mean(), int((f["oos_lift"] > 0).sum()), len(f)

        (la, pa, na), (lb, pb, nb) = m(res_a), m(res_b)
        print("\n" + "=" * 68)
        print("INCREMENTAL VALUE OF THE GRAPH LAYER")
        print("=" * 68)
        print(f"  no graph : mean OOS lift {la:+.4f}   positive folds {pa}/{na}")
        print(f"  graph    : mean OOS lift {lb:+.4f}   positive folds {pb}/{nb}")
        print(f"  delta    : {lb - la:+.4f}")
        print("  -> only keep the graph layer if this delta is positive AND stable.")
        _save(res_a, "ab_nograph")
        _save(res_b, "ab_graph")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
