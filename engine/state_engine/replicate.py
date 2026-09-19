"""Can the engine PREDICT the signal, rather than describe it after the fact?

WHAT EVERY EARLIER MODULE DID
-----------------------------
`overlay` took your trades as given. It asked what characterised them, whether
a filter improved them, what the exits cost. Useful, but all of it started from
"you already decided to buy these nine stocks."

That is description, not prediction. It could never produce a signal on a day
you had not already traded.

WHAT THIS DOES
--------------
Trains on day T's features to predict whether your signal will fire on T+1, for
every symbol in the universe -- including the thousands of symbol-days you did
NOT trade. Then, walk-forward, it picks its own basket each morning and holds it
to the close, so its returns are directly comparable to yours.

    can it identify your signal days?      precision at the top N
    do its OWN picks make money?           mean next-day return of its basket
    does it beat picking at random?        against the universe base rate

TWO INDEPENDENT VERDICTS FALL OUT OF ONE RUN
--------------------------------------------
If the model reproduces your signal well, the engine has learned to generate it
and can run without you.

If it reproduces it poorly, there are two possible reasons and they matter very
differently. Either your signal uses information the daily panel does not
contain -- news, fundamentals, intraday microstructure -- which is fine and
means the engine simply cannot see what you see. Or your signal used data that
did not exist at the previous close, in which case no causal model could ever
reproduce it, and the +1.3% is not real.

This cannot distinguish those two by itself. But a signal that IS reproducible
from prior-close data is proven causal by construction, so a good score settles
the leakage question in your favour permanently.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import Config
from .walkforward import month_folds


def build_label(panel: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    """y = 1 if the signal fired for this symbol on the NEXT trading day.

    The trade log records the entry date. The decision was made at the previous
    close, so the label belongs on the previous trading day -- found per symbol
    through the real calendar, not by subtracting one.
    """
    p = panel.sort_values(["symbol", "date"]).copy()
    p["_next"] = p.groupby("symbol", sort=False)["date"].shift(-1)
    t = trades[["date", "symbol"]].drop_duplicates().assign(_fired=1)
    p = p.merge(t.rename(columns={"date": "_next"}), on=["_next", "symbol"],
                how="left")
    p["y_signal"] = p["_fired"].fillna(0).astype(int)
    return p.drop(columns=["_fired"])


def replicate(panel: pd.DataFrame, features: list[str], trades: pd.DataFrame,
              cfg: Config, refit_every: int = 3, top_n: int | None = None,
              verbose: bool = True) -> dict:
    """Walk-forward attempt to regenerate the signal from prior-close data."""
    from sklearn.ensemble import HistGradientBoostingClassifier

    d = build_label(panel, trades)
    d = d[d["_next"].notna()].copy()
    # what the model's picks would earn: next day's open -> close
    g = d.groupby("symbol", sort=False)
    nxt_open, nxt_close = g["open"].shift(-1), g["close"].shift(-1)
    d["fwd_ret"] = nxt_close / nxt_open - 1.0 - cfg.cost_pct
    d = d[d["fwd_ret"].notna()]

    fired = int(d["y_signal"].sum())
    if fired < 200:
        raise RuntimeError(f"only {fired} signal days matched; check the log")
    if top_n is None:
        per_day = d[d["y_signal"] == 1].groupby("date").size()
        top_n = int(round(per_day.median()))

    if verbose:
        print(f"\n{'=' * 82}\nSIGNAL REPLICATION   can prior-close data regenerate "
              f"your signal?\n{'=' * 82}")
        print(f"  {len(d):,} symbol-days, {fired:,} of them signal days "
              f"({d['y_signal'].mean():.2%})")
        print(f"  the model will pick the top {top_n} names each morning")

    folds = month_folds(d["date"], cfg)
    rows, picks = [], []
    model = None
    for i, (tr_s, tr_e, te_e) in enumerate(folds):
        train = d[(d["date"] >= tr_s) & (d["date"] < tr_e)]
        test = d[(d["date"] >= tr_e) & (d["date"] < te_e)]
        if train["y_signal"].sum() < 100 or test.empty:
            continue
        if model is None or i % refit_every == 0:
            model = HistGradientBoostingClassifier(
                max_iter=150, max_depth=4, learning_rate=0.08,
                random_state=cfg.seed)
            model.fit(train[features].to_numpy(dtype=np.float32),
                      train["y_signal"].to_numpy())
        prob = model.predict_proba(
            test[features].to_numpy(dtype=np.float32))[:, 1]
        te = test.assign(p=prob)
        sel = (te.sort_values(["date", "p"], ascending=[True, False])
                 .groupby("date").head(top_n))
        picks.append(sel[["date", "symbol", "p", "y_signal", "fwd_ret"]])
        rows.append({
            "fold": i, "n_test": len(te), "n_signal": int(te["y_signal"].sum()),
            "precision": float(sel["y_signal"].mean()),
            "base_rate": float(te["y_signal"].mean()),
            "model_ret": float(sel["fwd_ret"].mean()),
            "actual_signal_ret": float(te.loc[te["y_signal"] == 1, "fwd_ret"].mean())
            if te["y_signal"].any() else np.nan,
            "universe_ret": float(te["fwd_ret"].mean()),
        })

    f = pd.DataFrame(rows)
    allpicks = pd.concat(picks, ignore_index=True) if picks else pd.DataFrame()

    if verbose and not f.empty:
        prec, base = f["precision"].mean(), f["base_rate"].mean()
        print(f"\n  OUT OF SAMPLE, {len(f)} folds")
        print(f"    precision at top {top_n}   : {prec:.1%}")
        print(f"    base rate (random pick)  : {base:.1%}")
        print(f"    lift                     : {prec/base:.1f}x")
        print(f"\n    model's own basket       : {f['model_ret'].mean()*100:+.3f}%/day")
        print(f"    your actual signal       : {f['actual_signal_ret'].mean()*100:+.3f}%/day")
        print(f"    whole universe           : {f['universe_ret'].mean()*100:+.3f}%/day")
        beat = int((f["model_ret"] > f["universe_ret"]).sum())
        print(f"    folds beating the universe: {beat}/{len(f)}")

        print("\n  READING THIS")
        if prec / max(base, 1e-9) < 2:
            print("    The model barely beats random. Your signal is NOT")
            print("    reconstructible from daily OHLCV. Either it uses information")
            print("    this panel does not contain, or it used data that did not")
            print("    exist at the previous close. Those are very different")
            print("    diagnoses and this test cannot separate them -- but you know")
            print("    how the signal was built, so you can.")
        elif f["model_ret"].mean() > f["universe_ret"].mean():
            print("    The model regenerates the signal from prior-close data ALONE")
            print("    and its own picks beat the universe. That proves your signal")
            print("    is causal -- a leaked signal cannot be reproduced from data")
            print("    that predates it -- and it means the engine can generate")
            print("    entries without you.")
        else:
            print("    The model finds your signal days but its picks do not beat")
            print("    the universe. It has learned WHICH days you trade, not what")
            print("    makes them work.")
    return {"folds": f, "picks": allpicks, "top_n": top_n}
