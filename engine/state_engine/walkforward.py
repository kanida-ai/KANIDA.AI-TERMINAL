"""Walk-forward validation and the state lifecycle.

Expanding window, by calendar month:

    train [start .. M]      -> test M+1
    train [start .. M+1]    -> test M+2
    ...

Within every fold, the following are fitted on TRAIN ONLY and then applied
frozen to TEST: bin edges, feature scores, feature selection, tree structure,
state statistics. Nothing about the test month informs any of them.

Lifecycle: candidate -> validated -> production -> retired. A state must keep
earning its place; consecutive out-of-sample failures retire it. This turns
overfitting from a worry into a test that can fail.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from .config import Config
from .states import fit_state_model, state_stats, promote_states


@dataclass
class LifecycleRecord:
    state: str
    signature_features: tuple
    status: str = "candidate"
    folds_seen: int = 0
    consec_pass: int = 0
    consec_fail: int = 0
    oos_n: int = 0
    oos_hits: float = 0.0
    history: list = field(default_factory=list)

    @property
    def oos_hit_rate(self) -> float:
        return self.oos_hits / self.oos_n if self.oos_n else float("nan")


def month_folds(dates: pd.Series, cfg: Config) -> list[tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    """Returns (train_start, train_end_exclusive, test_end_exclusive) per fold."""
    months = pd.PeriodIndex(dates.dt.to_period("M").unique(), freq="M").sort_values()
    folds = []
    for i in range(cfg.train_months_min, len(months) - cfg.test_months + 1, cfg.test_months):
        train_end = months[i].to_timestamp()
        test_end = months[min(i + cfg.test_months, len(months) - 1)].to_timestamp()
        if cfg.expanding:
            train_start = months[0].to_timestamp()
        else:
            j = max(0, i - cfg.rolling_train_months)
            train_start = months[j].to_timestamp()
        if test_end <= train_end:
            continue
        folds.append((train_start, train_end, test_end))
    return folds


def run_walkforward(panel: pd.DataFrame, features: list[str], cfg: Config,
                    verbose: bool = True) -> dict:
    folds = month_folds(panel["date"], cfg)
    if not folds:
        raise RuntimeError("not enough history for even one fold; lower train_months_min")

    registry: dict[str, LifecycleRecord] = {}
    fold_rows, oos_trades = [], []

    for fi, (tr_s, tr_e, te_e) in enumerate(folds, 1):
        train = panel[(panel["date"] >= tr_s) & (panel["date"] < tr_e)]
        test = panel[(panel["date"] >= tr_e) & (panel["date"] < te_e)]
        train = train[train["y_hit"].notna()]
        test = test[test["y_hit"].notna()]
        if len(train) < cfg.min_support_train * 5 or test.empty:
            continue

        model, ranked = fit_state_model(train, features, cfg)
        if model is None:
            continue

        tr_sig = model.assign(train)
        stats_tr = state_stats(train, tr_sig, cfg, cfg.min_support_train)
        good = promote_states(stats_tr, cfg)
        if good.empty:
            fold_rows.append({"fold": fi, "train_end": tr_e, "test_end": te_e,
                              "n_states": 0, "oos_n": 0, "oos_hit_rate": np.nan,
                              "oos_base": float(test["y_hit"].mean()), "oos_lift": np.nan})
            continue

        te_sig = model.assign(test)
        te_eval = test.assign(_sig=te_sig)
        te_eval = te_eval[te_eval["_sig"].isin(set(good["state"]))]

        base_oos = float(test["y_hit"].mean())
        sel_n = len(te_eval)
        sel_hr = float(te_eval["y_hit"].mean()) if sel_n else np.nan

        # ---- per-state OOS verdict and lifecycle update ----
        if sel_n:
            gg = te_eval.groupby("_sig", observed=True)["y_hit"]
            per_state = pd.DataFrame({"n": gg.size(), "hits": gg.sum(), "hr": gg.mean()})
            for st, row in per_state.iterrows():
                key = f"{model.describe()['method']}::{st}"
                rec = registry.get(key)
                if rec is None:
                    rec = LifecycleRecord(state=st, signature_features=tuple(model.features))
                    registry[key] = rec
                rec.folds_seen += 1
                rec.oos_n += int(row["n"])
                rec.oos_hits += float(row["hits"])
                passed = (row["n"] >= cfg.min_support_test) and (row["hr"] - base_oos >= cfg.min_lift)
                if passed:
                    rec.consec_pass += 1
                    rec.consec_fail = 0
                elif row["n"] >= cfg.min_support_test:
                    rec.consec_fail += 1
                    rec.consec_pass = 0
                rec.history.append({"fold": fi, "n": int(row["n"]), "hr": float(row["hr"]),
                                    "base": base_oos, "passed": bool(passed)})
                if rec.consec_fail >= cfg.retire_after:
                    rec.status = "retired"
                elif rec.consec_pass >= cfg.production_after:
                    rec.status = "production"
                elif rec.consec_pass >= cfg.promote_after:
                    rec.status = "validated"
                else:
                    rec.status = "candidate" if rec.status != "retired" else "retired"

            oos_trades.append(te_eval[["date", "symbol", "_sig", "y_hit", "y_mfe",
                                       "y_mae", "y_expectancy"]].assign(fold=fi))

        fold_rows.append({
            "fold": fi, "train_end": tr_e, "test_end": te_e,
            "n_states": len(good), "oos_n": sel_n, "oos_hit_rate": sel_hr,
            "oos_base": base_oos, "oos_lift": (sel_hr - base_oos) if sel_n else np.nan,
            "top_features": ",".join(model.features),
        })
        if verbose:
            print(f"  fold {fi:>3} | train<{tr_e.date()} test<{te_e.date()} "
                  f"| states {len(good):>4} | oos n {sel_n:>5} "
                  f"| hr {sel_hr:.3f} vs base {base_oos:.3f} "
                  f"| lift {(sel_hr - base_oos) if sel_n else float('nan'):+.3f}")

    folds_df = pd.DataFrame(fold_rows)
    trades_df = pd.concat(oos_trades, ignore_index=True) if oos_trades else pd.DataFrame()
    life_df = pd.DataFrame([{
        "state": r.state, "status": r.status, "folds_seen": r.folds_seen,
        "consec_pass": r.consec_pass, "consec_fail": r.consec_fail,
        "oos_n": r.oos_n, "oos_hit_rate": r.oos_hit_rate,
        "features": ",".join(r.signature_features),
    } for r in registry.values()])
    if not life_df.empty:
        life_df = life_df.sort_values(["status", "oos_n"], ascending=[True, False])

    return {"folds": folds_df, "lifecycle": life_df, "trades": trades_df}


def summarise(res: dict, cfg: Config) -> str:
    f, life, tr = res["folds"], res["lifecycle"], res["trades"]
    lines = ["", "=" * 68, "WALK-FORWARD REPORT CARD", "=" * 68]
    if f.empty:
        return "\n".join(lines + ["no usable folds"])

    valid = f.dropna(subset=["oos_lift"])
    n_pos = int((valid["oos_lift"] > 0).sum())
    lines += [
        f"folds run                 : {len(f)}",
        f"folds with any signal     : {len(valid)}",
        f"folds with positive lift  : {n_pos} / {len(valid)}",
        f"mean OOS lift             : {valid['oos_lift'].mean():+.4f}",
        f"median OOS lift           : {valid['oos_lift'].median():+.4f}",
        f"pooled OOS hit rate       : {tr['y_hit'].mean():.4f}" if not tr.empty else "pooled OOS hit rate       : n/a",
        f"pooled OOS base rate      : {valid['oos_base'].mean():.4f}",
        f"pooled OOS signals        : {int(valid['oos_n'].sum())}",
    ]
    if not tr.empty:
        lines += [
            f"mean OOS expectancy/trade : {tr['y_expectancy'].mean():+.5f}",
            f"mean OOS MFE / MAE        : {tr['y_mfe'].mean():+.4f} / {tr['y_mae'].mean():+.4f}",
        ]
    if not life.empty:
        counts = life["status"].value_counts().to_dict()
        lines += ["", "state lifecycle:"]
        for k in ["production", "validated", "candidate", "retired"]:
            lines.append(f"  {k:<12}: {counts.get(k, 0)}")
        prod = life[life["status"].isin(["production", "validated"])].head(10)
        if not prod.empty:
            lines += ["", "top surviving states (OOS):"]
            for _, r in prod.iterrows():
                lines.append(f"  [{r['status']:<10}] n={int(r['oos_n']):<5} "
                             f"hr={r['oos_hit_rate']:.3f}  {r['state'][:70]}")
    lines += ["", "SUCCESS CRITERION: this engine is judged on the COUNT of states that",
              "survive out of sample, not on a backtest curve.", "=" * 68]
    return "\n".join(lines)
