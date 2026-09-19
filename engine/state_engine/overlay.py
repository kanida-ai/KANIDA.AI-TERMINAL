"""Reverse-engineer an existing signal, then try to improve it.

You already have a signal: generated at the close of day T, entered at 09:15 on
day T+1. That is causal by construction, and the engine can work with it in two
ways that are far more tractable than mining 10,000 rows for a needle.

  1. CHARACTERISE.  Which of the engine's features separate the days your signal
     fired from the days it did not? That is your signal's DNA, expressed in the
     engine's vocabulary. It tells you what the signal is actually keying on --
     which is often not what you designed it to key on.

  2. FILTER.  Your signal produces N trades at some win rate. Instead of hunting
     for new signals, mine states WITHIN those trades that separate the winners
     from the losers. Going from 57% to 70% on an existing set is worth more than
     any new state found from scratch, and the sample problem is far smaller: you
     are explaining hundreds of labelled trades, not tens of thousands of days.

The filter is validated walk-forward like everything else. A filter fitted on all
your trades and then applied to those same trades will always look brilliant.

TRADE LOG FORMAT
----------------
CSV with at least:  trade_date, symbol, stock_ret_pct
Optional:           exit_reason, entry_time, exit_time
`stock_ret_pct` is in percent (4.07 means +4.07%).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import Config
from .states import QuantileBinner, score_features, select_features, GridStateModel, wilson_lower
from .walkforward import month_folds

DEFAULT_COST = 0.0011


# --------------------------------------------------------------------------- #
# loading                                                                       #
# --------------------------------------------------------------------------- #
def load_trade_log(path: str, cost: float = DEFAULT_COST) -> pd.DataFrame:
    d = pd.read_csv(path)
    cols = {c.lower().strip(): c for c in d.columns}

    def pick(*names):
        for n in names:
            if n in cols:
                return cols[n]
        raise KeyError(f"trade log needs one of {names}; found {list(d.columns)}")

    out = pd.DataFrame({
        "date": pd.to_datetime(d[pick("trade_date", "date", "entry_date")]).dt.normalize(),
        "symbol": d[pick("symbol", "ticker", "tradingsymbol")].astype(str).str.strip(),
        "ret": pd.to_numeric(d[pick("stock_ret_pct", "ret_pct", "return_pct", "pnl_pct")],
                             errors="coerce") / 100.0,
    })
    for opt in ("exit_reason", "exit"):
        if opt in cols:
            out["exit_reason"] = d[cols[opt]].astype(str)
            break
    out["net"] = out["ret"] - cost
    out["win"] = (out["net"] > 0).astype(float)
    return out.dropna(subset=["ret"]).sort_values(["date", "symbol"]).reset_index(drop=True)


def summarise_log(t: pd.DataFrame) -> None:
    print(f"\n{'=' * 68}\nTRADE LOG   {len(t)} trades, "
          f"{t['date'].min().date()} -> {t['date'].max().date()}\n{'=' * 68}")
    print(f"  win rate          : {t['win'].mean():.1%}")
    print(f"  mean net return   : {t['net'].mean() * 100:+.3f}%")
    print(f"  median            : {t['net'].median() * 100:+.3f}%")
    w, l = t.loc[t.net > 0, "net"], t.loc[t.net <= 0, "net"]
    print(f"  avg win / avg loss: {w.mean() * 100:+.2f}% / {l.mean() * 100:+.2f}%  "
          f"(ratio {abs(w.mean() / l.mean()):.2f})")
    print(f"  top 10 trades     : {t['net'].nlargest(10).sum() / t['net'].sum():.0%} of P&L")
    if "exit_reason" in t:
        print("\n  by exit reason")
        g = t.groupby("exit_reason")["net"].agg(
            n="size", win=lambda s: (s > 0).mean(), mean=lambda s: s.mean() * 100)
        print(g.round(3).to_string())


# --------------------------------------------------------------------------- #
# 1. characterise: what is the signal keying on?                                #
# --------------------------------------------------------------------------- #
def attach_signal_day(panel: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    """Map each trade back to the day its SIGNAL was generated.

    The trade log records the ENTRY date -- you buy at 09:15. But the signal was
    produced at the previous close, so the features that explain it live on the
    PREVIOUS trading day. Joining on the entry date would use that morning's own
    bar to explain a trade already entered: lookahead, and it would make any
    signal look brilliant.

    merge_asof with direction='backward' and allow_exact_matches=False finds, per
    symbol, the last panel date strictly before the entry date.
    """
    p = panel[["date", "symbol"]].copy()
    p["_signal_date"] = p["date"]
    out = []
    for sym, t in trades.groupby("symbol", sort=False):
        cal = p[p["symbol"] == sym].sort_values("date")
        if cal.empty:
            continue
        m = pd.merge_asof(t.sort_values("date"), cal[["date", "_signal_date"]],
                          on="date", direction="backward",
                          allow_exact_matches=False)
        out.append(m)
    if not out:
        raise RuntimeError("no trade symbols matched the panel")
    res = pd.concat(out, ignore_index=True).dropna(subset=["_signal_date"])
    res["entry_date"] = res["date"]
    res["date"] = res["_signal_date"]      # join key is now the signal day
    return res.drop(columns=["_signal_date"])


def characterise(panel: pd.DataFrame, features: list[str], trades: pd.DataFrame,
                 top: int = 20, verbose: bool = True,
                 within_symbols: bool = True) -> pd.DataFrame:
    """Standardised difference between signal days and other days.

    Cohen's d, so it is comparable across features on different scales. A |d|
    above ~0.3 means the signal is clearly selecting on that dimension, whether
    or not you intended it to.
    """
    t = attach_signal_day(panel, trades)
    p = panel.merge(t[["date", "symbol"]].assign(_sig=1),
                    on=["date", "symbol"], how="left")
    p["_sig"] = p["_sig"].fillna(0).astype(bool)
    if p["_sig"].sum() < 20:
        raise RuntimeError(f"only {int(p['_sig'].sum())} trades matched the panel; "
                           "check symbol spelling and date alignment")

    # Restrict the comparison group to the symbols you actually trade.
    #
    # This matters more than it sounds. Compare your signal days against a
    # 633-symbol universe and most of the measured effect is simply that you
    # trade unusual stocks -- high-momentum names sit far above their 60-day
    # lows whatever day you pick. That is SYMBOL selection, and it tells you
    # nothing about timing. Comparing against other days of the SAME symbols
    # isolates the only thing your signal actually decides: which day to enter.
    traded = set(t["symbol"].unique())
    if within_symbols:
        p = p[p["symbol"].isin(traded)]

    rows = []
    a, b = p[p["_sig"]], p[~p["_sig"]]
    for f in features:
        x, y = a[f].dropna(), b[f].dropna()
        if len(x) < 20 or len(y) < 100:
            continue
        sd = np.sqrt((x.var(ddof=1) + y.var(ddof=1)) / 2.0)
        if not np.isfinite(sd) or sd == 0:
            continue
        rows.append({"feature": f, "n_signal": len(x),
                     "signal_mean": float(x.mean()), "other_mean": float(y.mean()),
                     "cohens_d": float((x.mean() - y.mean()) / sd),
                     "signal_pctile": float((y < x.mean()).mean())})
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["abs_d"] = out["cohens_d"].abs()
    out = out.sort_values("abs_d", ascending=False).reset_index(drop=True)

    if verbose:
        scope = (f"other days of the same {len(traded)} symbols" if within_symbols
                 else f"all other symbol-days in the panel")
        print(f"\n{'=' * 78}\nSIGNAL DNA   signal days vs {scope}\n{'=' * 78}")
        print(f"  matched {int(p['_sig'].sum())} of {len(trades)} trades to the panel")
        v = out.head(top).copy()
        v["feature"] = v["feature"].str.slice(0, 44)
        print(v[["feature", "signal_mean", "other_mean", "cohens_d",
                 "signal_pctile"]].round(4).to_string(index=False))
        print("\n  |d| > 0.5 strong, 0.3-0.5 moderate, < 0.2 incidental.")
        if within_symbols:
            print("  Scoped to your traded symbols, so this measures WHICH DAY the")
            print("  signal picks, not which stock. Run with within_symbols=False")
            print("  to see the stock-selection effect instead.")
        print("  signal_pctile is where the signal's average day sits in the")
        print("  overall distribution: 0.90 means it fires near the top decile.")
    return out


# --------------------------------------------------------------------------- #
# 2. filter: separate the winners from the losers, walk-forward                 #
# --------------------------------------------------------------------------- #
def rank_separators(panel: pd.DataFrame, features: list[str], trades: pd.DataFrame,
                    top: int = 20, n_bins: int = 4, verbose: bool = True) -> pd.DataFrame:
    """Which SINGLE feature best separates winning trades from losing ones?

    With a few hundred trades this is about all the sample supports. A two-feature
    grid at 4 bins each is 16 cells; 300 trades gives 19 per cell, which is noise.
    One feature at 4 bins gives 75 per cell, which is measurable.

    In-sample by construction -- this is a hypothesis generator, not evidence.
    Anything promising here still has to clear the walk-forward filter below.
    """
    t = attach_signal_day(panel, trades)
    p = panel.merge(t[["date", "symbol", "net", "win"]], on=["date", "symbol"],
                    how="inner").dropna(subset=["net"])
    if len(p) < 50:
        raise RuntimeError(f"only {len(p)} trades matched")

    base_win, base_ret = p["win"].mean(), p["net"].mean()
    rows = []
    for f in features:
        v = p[f]
        if v.notna().sum() < len(p) * 0.8 or v.nunique() < n_bins:
            continue
        try:
            b = pd.qcut(v, n_bins, labels=False, duplicates="drop")
        except (ValueError, IndexError):
            continue
        g = p.assign(_b=b).dropna(subset=["_b"]).groupby("_b")
        wins, rets, ns = g["win"].mean(), g["net"].mean(), g.size()
        if (ns < 20).any() or len(ns) < 2:
            continue
        best = rets.idxmax()
        rows.append({
            "feature": f, "best_bin": int(best), "n_bins": len(ns),
            "n_in_best": int(ns[best]),
            "win_best": float(wins[best]), "win_worst": float(wins[rets.idxmin()]),
            "ret_best": float(rets[best]), "ret_worst": float(rets.min()),
            "ret_spread": float(rets.max() - rets.min()),
            "monotone": float(np.corrcoef(np.arange(len(rets)), rets.to_numpy())[0, 1]),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values("ret_spread", ascending=False).reset_index(drop=True)

    if verbose:
        print(f"\n{'=' * 82}\nSEPARATORS   which feature splits your winners from your losers"
              f"\n{'=' * 82}")
        print(f"  all {len(p)} trades: win {base_win:.1%}, mean return {base_ret*100:+.3f}%")
        v = out.head(top).copy()
        v["feature"] = v["feature"].str.slice(0, 40)
        print(v[["feature", "best_bin", "n_in_best", "win_best", "ret_best",
                 "ret_worst", "ret_spread", "monotone"]].round(4).to_string(index=False))
        print("\n  ret_spread = best bin minus worst bin, in decimal return.")
        print("  |monotone| near 1 means the effect is ordered across bins, which")
        print("  is far more believable than one bin happening to look good.")
        print("  IN-SAMPLE. Nothing here is evidence until it clears walk-forward.")
    return out


def monotone_null(panel: pd.DataFrame, features: list[str], trades: pd.DataFrame,
                  n_bins: int = 4, n_perm: int = 300, seed: int = 0,
                  verbose: bool = True) -> dict:
    """How monotone would the BEST feature look if the returns were random?

    A four-bin monotonicity is four points. Getting |r| > 0.95 from four random
    points happens roughly 5% of the time, and with ~390 features you therefore
    expect around twenty near-perfect looking relationships from noise alone.
    Reading the monotone column without this correction is self-deception.

    The bins do not move when you permute the labels, so they are computed once
    and every permutation is just a re-average. Cheap, and it gives the only
    threshold that accounts for having searched 390 features.
    """
    t = attach_signal_day(panel, trades)
    p = panel.merge(t[["date", "symbol", "net"]], on=["date", "symbol"],
                    how="inner").dropna(subset=["net"])
    y = p["net"].to_numpy(dtype=float)
    n = len(y)

    cols, mats = [], []
    for f in features:
        v = p[f]
        if v.notna().sum() < n * 0.9 or v.nunique() < n_bins:
            continue
        try:
            b = pd.qcut(v, n_bins, labels=False, duplicates="drop")
        except (ValueError, IndexError):
            continue
        if b.isna().any() or b.nunique() < n_bins:
            continue
        cols.append(f)
        mats.append(b.to_numpy(dtype=np.int64))
    if not cols:
        return {}
    B = np.vstack(mats)
    idx = np.arange(n_bins, dtype=float)
    idx = (idx - idx.mean()) / idx.std()

    def max_abs_mono(vals):
        best = 0.0
        for row in B:
            cnt = np.bincount(row, minlength=n_bins).astype(float)
            tot = np.bincount(row, weights=vals, minlength=n_bins)
            m = tot / np.maximum(cnt, 1)
            sd = m.std()
            if sd > 0:
                best = max(best, abs(float((m - m.mean()) @ idx / (n_bins * sd))))
        return best

    observed = max_abs_mono(y)
    rng = np.random.default_rng(seed)
    null = np.array([max_abs_mono(rng.permutation(y)) for _ in range(n_perm)])
    thresh = float(np.quantile(null, 0.95))
    pval = float((null >= observed).mean())

    if verbose:
        print(f"\n{'=' * 78}\nMONOTONICITY, CORRECTED FOR SEARCHING {len(cols)} FEATURES"
              f"\n{'=' * 78}")
        print(f"  best observed |monotone| : {observed:.6f}")
        print(f"  95th pct under random    : {thresh:.6f}")
        print(f"  p-value                  : {pval:.3f}")
        if pval > 0.05:
            print("\n  Your best monotone relationship is no stronger than the best")
            print("  one random returns would produce. Ignore the monotone column.")
        else:
            print("\n  The best relationship exceeds what the search alone explains.")
        print(f"  Only treat |monotone| above {thresh:.6f} as meaningful.")
        print("  Note: with 4 bins that threshold sits against the ceiling, so")
        print("  monotonicity can barely distinguish anything. Use more bins if")
        print("  you want this column to carry weight.")
    return {"observed": observed, "threshold": thresh, "pvalue": pval,
            "n_features": len(cols)}


def winner_loser_dna(panel: pd.DataFrame, features: list[str], trades: pd.DataFrame,
                     quantile: float = 0.2, top: int = 15,
                     verbose: bool = True) -> pd.DataFrame:
    """The full fingerprint of your best trades versus your worst.

    `characterise` asks what separates signal days from all other days.
    This asks a different and more useful question: given the signal fired,
    what distinguishes the trades that worked from the ones that did not?

    Reported as the standardised gap between the top and bottom quintile, so a
    feature only appears if it genuinely ordered the outcome -- not merely
    because your signal happened to fire in an unusual place.
    """
    t = attach_signal_day(panel, trades)
    p = panel.merge(t[["date", "symbol", "net"]], on=["date", "symbol"],
                    how="inner").dropna(subset=["net"])
    hi_cut, lo_cut = p["net"].quantile(1 - quantile), p["net"].quantile(quantile)
    W, L = p[p["net"] >= hi_cut], p[p["net"] <= lo_cut]

    rows = []
    for f in features:
        a, b = W[f].dropna(), L[f].dropna()
        if len(a) < 15 or len(b) < 15:
            continue
        sd = np.sqrt((a.var(ddof=1) + b.var(ddof=1)) / 2.0)
        if not np.isfinite(sd) or sd == 0:
            continue
        rows.append({"feature": f, "winner_mean": float(a.mean()),
                     "loser_mean": float(b.mean()),
                     "d": float((a.mean() - b.mean()) / sd),
                     "all_mean": float(p[f].mean())})
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["abs_d"] = out["d"].abs()
    out = out.sort_values("abs_d", ascending=False).reset_index(drop=True)

    if verbose:
        print(f"\n{'=' * 82}\nWINNER DNA vs LOSER DNA   "
              f"top {quantile:.0%} ({len(W)} trades) vs bottom {quantile:.0%} ({len(L)})"
              f"\n{'=' * 82}")
        print(f"  winners avg {W['net'].mean()*100:+.2f}%   "
              f"losers avg {L['net'].mean()*100:+.2f}%")
        v = out.head(top).copy()
        v["feature"] = v["feature"].str.slice(0, 42)
        print(v[["feature", "winner_mean", "loser_mean", "all_mean", "d"]]
              .round(4).to_string(index=False))
        print("\n  d > 0 means winners score HIGHER on that feature.")
        print("  Compare against SIGNAL DNA: a feature your signal selects for")
        print("  but which does not separate winners is a filter doing no work.")
    return out


def greedy_pair(panel: pd.DataFrame, features: list[str], trades: pd.DataFrame,
                n_bins: int = 3, min_support: int = 35, top_seeds: int = 8,
                verbose: bool = True) -> pd.DataFrame:
    """Two-feature rules, built greedily so the sample never runs out.

    A blind three-feature grid at four bins is 64 cells; 361 trades gives under
    six per cell, which is not a measurement. Instead: take the best single
    separators, then inside the best bin of each, search for a second feature.
    Three bins keeps a two-feature rule at nine cells, ~40 trades each.

    Still in-sample. Two-feature rules overfit faster than one, so treat these
    strictly as candidates for the walk-forward filter.
    """
    t = attach_signal_day(panel, trades)
    p = panel.merge(t[["date", "symbol", "net", "win"]], on=["date", "symbol"],
                    how="inner").dropna(subset=["net"])
    base = p["net"].mean()

    singles = rank_separators(panel, features, trades, n_bins=n_bins, verbose=False)
    if singles.empty:
        return pd.DataFrame()

    rows = []
    for _, seed in singles.head(top_seeds).iterrows():
        f1 = seed["feature"]
        try:
            b1 = pd.qcut(p[f1], n_bins, labels=False, duplicates="drop")
        except (ValueError, IndexError):
            continue
        sub = p[b1 == seed["best_bin"]]
        if len(sub) < min_support * 2:
            continue
        for f2 in features:
            if f2 == f1:
                continue
            v = sub[f2]
            if v.notna().sum() < len(sub) * 0.9 or v.nunique() < n_bins:
                continue
            try:
                b2 = pd.qcut(v, n_bins, labels=False, duplicates="drop")
            except (ValueError, IndexError):
                continue
            g = sub.assign(_b=b2).dropna(subset=["_b"]).groupby("_b")
            ns, rets, wins = g.size(), g["net"].mean(), g["win"].mean()
            ok = ns >= min_support
            if not ok.any():
                continue
            bb = rets[ok].idxmax()
            rows.append({
                "rule": f"{f1}=bin{int(seed['best_bin'])} AND {f2}=bin{int(bb)}",
                "f1": f1, "f2": f2, "n": int(ns[bb]),
                "win": float(wins[bb]), "ret": float(rets[bb]),
                "base_ret": float(base), "gain": float(rets[bb] - base),
                "seed_ret": float(seed["ret_best"]),
                "gain_over_seed": float(rets[bb] - seed["ret_best"]),
            })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values("gain", ascending=False).reset_index(drop=True)

    if verbose:
        print(f"\n{'=' * 92}\nTWO-FEATURE RULES   greedy, {n_bins} bins, "
              f"min {min_support} trades per cell\n{'=' * 92}")
        print(f"  base return across all {len(p)} trades: {base*100:+.3f}%")
        v = out.head(12).copy()
        v["rule"] = v["rule"].str.slice(0, 62)
        print(v[["rule", "n", "win", "ret", "gain", "gain_over_seed"]]
              .round(4).to_string(index=False))
        print("\n  gain_over_seed is the column that matters: how much the SECOND")
        print("  feature added beyond the first. Near zero means the pair is just")
        print("  the single separator wearing a hat.")
    return out


def filter_states(panel: pd.DataFrame, features: list[str], trades: pd.DataFrame,
                  cfg: Config, min_support: int = 20, n_bins: int = 4,
                  state_features: int = 1, train_months: int | None = None,
                  verbose: bool = True) -> dict:
    """Mine states inside the trade set that predict which trades win.

    Every fold: fit on trades before the boundary, apply to trades after it.
    The headline is not the win rate of the filter -- it is how the win rate and
    the mean return of the KEPT trades compare with taking every trade.
    """
    t = attach_signal_day(panel, trades)
    p = panel.merge(t[["date", "symbol", "net", "win"]],
                    on=["date", "symbol"], how="inner")
    p = p.dropna(subset=["net"]).sort_values("date").reset_index(drop=True)
    if len(p) < 100:
        raise RuntimeError(f"only {len(p)} trades matched the panel")
    p["y_hit"] = p["win"]

    if verbose:
        print(f"\n{'=' * 78}\nFILTER SEARCH   {len(p)} trades matched, "
              f"base win rate {p['win'].mean():.1%}, "
              f"base return {p['net'].mean() * 100:+.3f}%")
        print(f"  {state_features} feature(s), {n_bins} bins, min {min_support} per "
              f"state, {(train_months or cfg.train_months_min)}-month train\n{'=' * 78}")

    fcfg = Config(**{**cfg.__dict__})
    if train_months:
        fcfg.train_months_min = train_months

    kept_rows, fold_rows = [], []
    for fi, (tr_s, tr_e, te_e) in enumerate(month_folds(p["date"], fcfg), 1):
        train = p[(p["date"] >= tr_s) & (p["date"] < tr_e)]
        test = p[(p["date"] >= tr_e) & (p["date"] < te_e)]
        if len(train) < min_support * 3 or test.empty:
            continue
        base = float(train["win"].mean())

        binner = QuantileBinner(n_bins).fit(train, features)
        ranked = score_features(train, features, binner, min_support=min_support,
                                min_coverage=cfg.min_feature_coverage)
        if ranked.empty:
            continue
        chosen = select_features(train, ranked, state_features,
                                 cfg.max_corr_between_selected)
        if not chosen:
            continue
        model = GridStateModel(chosen, binner)

        tr_sig = model.assign(train)
        good = set()
        # build a 3-column frame rather than appending to the ~390-column panel,
        # which pandas has to defragment on every fold
        tr_small = pd.DataFrame({"_s": tr_sig.to_numpy(),
                                 "win": train["win"].to_numpy(),
                                 "net": train["net"].to_numpy()})
        for st, g in tr_small.dropna(subset=["_s"]).groupby("_s"):
            n, h = len(g), int(g["win"].sum())
            if n < min_support:
                continue
            if (h / n - base) >= 0.03 and g["net"].mean() > train["net"].mean() * 1.2:
                good.add(st)
        if not good:
            continue

        te_sig = model.assign(test)
        keep = test[te_sig.isin(good)]
        if keep.empty:
            continue
        kept_rows.append(keep)
        fold_rows.append({
            "fold": fi, "n_test": len(test), "n_kept": len(keep),
            "win_all": float(test["win"].mean()), "win_kept": float(keep["win"].mean()),
            "ret_all": float(test["net"].mean()), "ret_kept": float(keep["net"].mean()),
            "features": ",".join(chosen),
        })

    folds = pd.DataFrame(fold_rows)
    kept = pd.concat(kept_rows) if kept_rows else pd.DataFrame()

    if verbose and not folds.empty:
        n_k, n_t = int(folds["n_kept"].sum()), int(folds["n_test"].sum())
        print(f"  folds with a filter : {len(folds)}")
        print(f"  trades kept         : {n_k} of {n_t}  ({n_k / n_t:.0%})")
        print(f"\n  {'':16} {'take all':>10} {'filtered':>10} {'delta':>10}")
        wa, wk = float(kept.shape[0] and folds['win_all'].mean()), float(kept["win"].mean())
        ra, rk = folds["ret_all"].mean(), float(kept["net"].mean())
        print(f"  {'win rate':16} {wa:>10.1%} {wk:>10.1%} {wk - wa:>+10.1%}")
        print(f"  {'mean return':16} {ra * 100:>9.3f}% {rk * 100:>9.3f}% "
              f"{(rk - ra) * 100:>+9.3f}%")
        print(f"  {'total P&L':16} {folds['ret_all'].sum() * n_t / len(folds) * 100:>9.1f}% "
              f"{kept['net'].sum() * 100:>9.1f}%")
        better = int((folds["ret_kept"] > folds["ret_all"]).sum())
        print(f"\n  folds where filtering beat taking everything: {better}/{len(folds)}")
        if rk <= ra:
            print("\n  The filter does not help. Taking every signal is better.")
        elif better < len(folds) * 0.6:
            print("\n  Improvement is not consistent across folds. Treat with caution.")
    elif verbose:
        print("  no fold produced a usable filter")

    return {"folds": folds, "kept": kept, "matched": p}


# --------------------------------------------------------------------------- #
# testing ONE hypothesis you already have, rather than searching for new ones   #
# --------------------------------------------------------------------------- #
def test_hypothesis(panel: pd.DataFrame, trades: pd.DataFrame, feature: str,
                    keep: str = "low", n_bins: int = 4, cfg: Config | None = None,
                    train_months: int = 8, split_date: str | None = None,
                    verbose: bool = True) -> dict:
    """Walk-forward test of a SINGLE, pre-specified rule.

    The searches above try hundreds of features and report the best, so their
    winners are inflated by selection. This tests one rule you decided on in
    advance. Bin edges come from TRAIN only, so the threshold itself is honest.

    `keep` is 'low' (bottom bin), 'high' (top bin), or 'not_high' (drop the top
    bin). 'not_high' is usually the right shape for an exclusion rule: you are
    removing a subset you believe is bad, not betting everything on one bin.

    If `split_date` is given, the rule is ALSO evaluated once on data after that
    date only. That is the closest thing to a clean out-of-sample read when the
    hypothesis itself came from looking at the whole sample -- as anything drawn
    from the Winner DNA table did.
    """
    cfg = cfg or Config()
    t = attach_signal_day(panel, trades)
    p = panel.merge(t[["date", "symbol", "net", "win"]], on=["date", "symbol"],
                    how="inner").dropna(subset=["net", feature]).sort_values("date")
    if len(p) < 60:
        raise RuntimeError(f"only {len(p)} trades usable for {feature}")

    fcfg = Config(**{**cfg.__dict__})
    fcfg.train_months_min = train_months

    rows = []
    for fi, (tr_s, tr_e, te_e) in enumerate(month_folds(p["date"], fcfg), 1):
        train = p[(p["date"] >= tr_s) & (p["date"] < tr_e)]
        test = p[(p["date"] >= tr_e) & (p["date"] < te_e)]
        if len(train) < 40 or test.empty:
            continue
        edges = np.nanquantile(train[feature].to_numpy(dtype=float),
                               np.linspace(0, 1, n_bins + 1)[1:-1])
        b = np.searchsorted(edges, test[feature].to_numpy(dtype=float))
        if keep == "low":
            m = b == 0
        elif keep == "high":
            m = b == n_bins - 1
        elif keep == "not_high":
            m = b < n_bins - 1
        else:
            raise ValueError("keep must be low | high | not_high")
        kept = test[m]
        if kept.empty:
            continue
        rows.append({"fold": fi, "n_all": len(test), "n_kept": len(kept),
                     "win_all": float(test["win"].mean()),
                     "win_kept": float(kept["win"].mean()),
                     "ret_all": float(test["net"].mean()),
                     "ret_kept": float(kept["net"].mean())})

    f = pd.DataFrame(rows)
    if f.empty:
        if verbose:
            print(f"  {feature}: no usable folds")
        return {"folds": f}

    better = int((f["ret_kept"] > f["ret_all"]).sum())
    res = {"folds": f, "feature": feature, "keep": keep,
           "n_folds": len(f), "folds_better": better,
           "kept_share": float(f["n_kept"].sum() / f["n_all"].sum()),
           "ret_all": float((f["ret_all"] * f["n_all"]).sum() / f["n_all"].sum()),
           "ret_kept": float((f["ret_kept"] * f["n_kept"]).sum() / f["n_kept"].sum()),
           "win_all": float((f["win_all"] * f["n_all"]).sum() / f["n_all"].sum()),
           "win_kept": float((f["win_kept"] * f["n_kept"]).sum() / f["n_kept"].sum())}

    if split_date:
        cut = pd.Timestamp(split_date)
        late = f[f["fold"] >= 1]
        pl = p[p["date"] >= cut]
        if len(pl) > 30:
            tr = p[p["date"] < cut]
            edges = np.nanquantile(tr[feature].to_numpy(dtype=float),
                                   np.linspace(0, 1, n_bins + 1)[1:-1])
            b = np.searchsorted(edges, pl[feature].to_numpy(dtype=float))
            m = (b == 0) if keep == "low" else (b == n_bins - 1) if keep == "high" \
                else (b < n_bins - 1)
            res["holdout_n"] = int(m.sum())
            res["holdout_ret_all"] = float(pl["net"].mean())
            res["holdout_ret_kept"] = float(pl[m]["net"].mean()) if m.any() else np.nan

    if verbose:
        print(f"\n  {feature}  [keep {keep}]")
        print(f"    folds {res['n_folds']}, better {better}/{res['n_folds']}, "
              f"kept {res['kept_share']:.0%} of trades")
        print(f"    win  {res['win_all']:.1%} -> {res['win_kept']:.1%}   "
              f"ret {res['ret_all']*100:+.3f}% -> {res['ret_kept']*100:+.3f}%   "
              f"({(res['ret_kept']-res['ret_all'])*100:+.3f}%)")
        if "holdout_ret_kept" in res and np.isfinite(res["holdout_ret_kept"]):
            print(f"    holdout after {split_date}: "
                  f"{res['holdout_ret_all']*100:+.3f}% -> "
                  f"{res['holdout_ret_kept']*100:+.3f}%  (n={res['holdout_n']})")
    return res


def test_signal_contradictions(panel: pd.DataFrame, trades: pd.DataFrame,
                               cfg: Config | None = None,
                               split_date: str | None = None) -> pd.DataFrame:
    """Test the specific rules implied by Signal DNA disagreeing with Winner DNA.

    When your signal selects HIGH on a feature but your winners score LOW on it,
    the signal is choosing against itself on that dimension. Each such feature
    becomes one pre-specified exclusion rule, tested walk-forward.
    """
    checks = [
        ("f_close__accel5", "not_high"),
        ("f_dollar_vol__accel5", "not_high"),
        ("f_volume__accel5", "not_high"),
        ("f_dollar_vol__ratio20", "not_high"),
        ("f_volume__ratio20", "not_high"),
        ("f_volume__pctrank20", "not_high"),
        ("f_dollar_vol__pctrank60", "not_high"),
        ("f_close__distmin5", "low"),
        ("f_close__distmin3", "not_high"),
    ]
    print(f"\n{'=' * 78}\nPRE-SPECIFIED EXCLUSION RULES")
    print("  Each rule was chosen from the Winner-DNA disagreement, then tested")
    print("  walk-forward. One rule at a time, so no search inflation -- but the")
    print(f"  hypothesis did come from this sample, so treat it as suggestive.\n{'=' * 78}")
    out = []
    for f, k in checks:
        if f not in panel.columns:
            continue
        try:
            r = test_hypothesis(panel, trades, f, keep=k, cfg=cfg,
                                split_date=split_date)
        except Exception as e:
            print(f"  {f}: {e}")
            continue
        if "ret_kept" in r:
            out.append({k2: v for k2, v in r.items() if k2 != "folds"})
    df = pd.DataFrame(out)
    if not df.empty:
        df["gain"] = df["ret_kept"] - df["ret_all"]
        df = df.sort_values("gain", ascending=False)
        print(f"\n  best: {df.iloc[0]['feature']} "
              f"({df.iloc[0]['gain']*100:+.3f}%, "
              f"{int(df.iloc[0]['folds_better'])}/{int(df.iloc[0]['n_folds'])} folds)")
        print("  A rule is only worth adopting if it gains AND wins most folds.")
    return df


# --------------------------------------------------------------------------- #
# are your EXITS helping or hurting?                                            #
# --------------------------------------------------------------------------- #
def exit_audit(panel: pd.DataFrame, trades: pd.DataFrame, cost: float = DEFAULT_COST,
               verbose: bool = True) -> pd.DataFrame:
    """Compare what each exit rule ACTUALLY earned against holding to the close.

    This asks a question no amount of state mining can answer: given the trade
    was taken, did the exit rule add or destroy value?

    The counterfactual is exact and needs no intraday data -- the daily bar gives
    open and close, and the trade entered at the open. So for every trade we know
    both the realised return and the return from simply holding to 15:29.

    Two things in your log make this urgent. A stop that fires on many symbols at
    the same timestamp is a portfolio kill switch, not a per-trade stop: it exits
    positions because of what the MARKET did, which is a different decision and
    deserves separate accounting. And a trailing stop that averages far less than
    the untrailed exits is cutting winners short, which costs more than it saves.
    """
    t = trades.copy()
    d = panel[["date", "symbol", "open", "close"]].copy()
    m = t.merge(d, on=["date", "symbol"], how="inner")
    if m.empty:
        raise RuntimeError("no trades matched the panel on their ENTRY date")

    m["eod_ret"] = m["close"] / m["open"] - 1.0 - cost
    m["actual"] = m["net"]
    m["exit_cost"] = m["eod_ret"] - m["actual"]        # >0 means the exit cost you

    if verbose:
        print(f"\n{'=' * 84}\nEXIT AUDIT   what each exit rule earned vs simply "
              f"holding to the close\n{'=' * 84}")
        print(f"  {len(m):,} of {len(t):,} trades matched on entry date\n")
        col = "exit_reason" if "exit_reason" in m.columns else None
        if col:
            g = m.groupby(col)
            tab = pd.DataFrame({
                "n": g.size(),
                "actual_%": g["actual"].mean() * 100,
                "hold_to_close_%": g["eod_ret"].mean() * 100,
                "cost_of_exit_%": g["exit_cost"].mean() * 100,
                "total_cost_%": g["exit_cost"].sum() * 100,
                "would_be_better": g["exit_cost"].apply(lambda s: float((s > 0).mean())),
            })
            print(tab.round(3).to_string())
            print("\n  cost_of_exit_% > 0 means holding to the close would have earned")
            print("  MORE. total_cost_% is that summed across every trade, in units")
            print("  of one position's return -- the aggregate price of the rule.")

        overall = m["exit_cost"].mean() * 100
        print(f"\n  ALL TRADES: actual {m['actual'].mean()*100:+.3f}%  vs  "
              f"hold-to-close {m['eod_ret'].mean()*100:+.3f}%   "
              f"({overall:+.3f}%)")
        if overall > 0.05:
            print("\n  Your exit rules are costing you. Holding every position to the")
            print("  close beats the current stop-and-trail logic on average.")
        elif overall < -0.05:
            print("\n  Your exit rules are earning their keep.")
        else:
            print("\n  Exits are roughly neutral versus holding to the close.")

        # same-timestamp clusters = portfolio kill switch
        if "exit_time" in m.columns:
            cl = m.groupby(["date", "exit_time"]).size()
            big = cl[cl >= 5]
            if len(big):
                print(f"\n  {len(big)} occasions where 5+ positions exited at the SAME")
                print("  timestamp: that is a portfolio kill switch. Those losses are")
                print("  about the market, not the stock, and should be modelled as a")
                print("  regime decision rather than mined as a per-trade state.")
    return m


def stop_sweep(panel: pd.DataFrame, trades: pd.DataFrame, cost: float = DEFAULT_COST,
               verbose: bool = True) -> pd.DataFrame:
    """What stop width SHOULD you use? Swept on your own trades.

    The exit audit says the current stop destroys value on average. The wrong
    conclusion is 'remove the stop'. A mean is not a risk profile: a stop earns
    its keep in the tail, and the average trade never sees the tail.

    So this reports both. For each candidate width: mean return, and the 1st and
    5th percentile of the trade distribution plus the worst single day summed
    across simultaneous positions. If the widest stop has the best mean and an
    unacceptable tail, the right answer is somewhere in the middle, and you can
    see exactly where.

    Resolution uses the daily bar: the trade entered at the open, so a stop at
    -X% was hit if low/open-1 <= -X. Exact for a single barrier -- there is no
    ordering ambiguity when only one level can be touched.
    """
    d = panel[["date", "symbol", "open", "high", "low", "close"]]
    m = trades.merge(d, on=["date", "symbol"], how="inner").copy()
    if m.empty:
        raise RuntimeError("no trades matched on entry date")

    m["adverse"] = m["low"] / m["open"] - 1.0
    m["to_close"] = m["close"] / m["open"] - 1.0

    widths = [0.005, 0.0075, 0.010, 0.015, 0.020, 0.025, 0.030, 0.040, 0.050, None]
    rows = []
    for w in widths:
        if w is None:
            pnl = m["to_close"] - cost
            label, hit = "no stop", 0.0
        else:
            stopped = m["adverse"] <= -w
            pnl = np.where(stopped, -w, m["to_close"]) - cost
            label, hit = f"{w:.2%}", float(stopped.mean())
        pnl = pd.Series(pnl, index=m.index)
        # Equal-weight the book each day, so a 15-position day is not counted as
        # fifteen times the risk of a 1-position day. This is the number that
        # maps onto an account.
        day = pnl.groupby(m["date"]).mean()
        curve = (1.0 + day).cumprod()
        dd = float((curve / curve.cummax() - 1.0).min() * 100)
        rows.append({
            "stop": label, "pct_stopped": hit,
            "mean_%": float(pnl.mean() * 100),
            "win_rate": float((pnl > 0).mean()),
            "p1_trade_%": float(pnl.quantile(0.01) * 100),
            "worst_trade_%": float(pnl.min() * 100),
            "worst_day_%": float(day.min() * 100),
            "max_dd_%": dd,
            "total_%": float(pnl.sum() * 100),
        })
    out = pd.DataFrame(rows)

    if verbose:
        print(f"\n{'=' * 92}\nSTOP WIDTH SWEEP   on your own {len(m):,} trades"
              f"\n{'=' * 92}")
        print(out.round(3).to_string(index=False))
        print("\n  worst_day_% and max_dd_% assume the book is equal-weighted each")
        print("  day, so they map onto an account. worst_trade_% is the single")
        print("  position tail -- the thing a stop actually insures against.")
        print("\n  Compare what each width COSTS (mean_% below the no-stop row,")
        print("  times the trade count) against what it BUYS (the improvement in")
        print("  worst_trade_% and max_dd_%). A stop is insurance: judge it on the")
        print("  claim, not the premium.")
        ns_row = out[out["stop"] == "no stop"].iloc[0]
        out["cost_vs_nostop_pts"] = (ns_row["mean_%"] - out["mean_%"]) * len(m)
        print("\n  total cost of each width, in percentage points across all "
              f"{len(m):,} trades:")
        for _, r in out.iterrows():
            if r["stop"] == "no stop":
                continue
            print(f"    {r['stop']:>7}  costs {r['cost_vs_nostop_pts']:>8.0f} pts   "
                  f"caps worst trade at {r['worst_trade_%']:>6.2f}%   "
                  f"max DD {r['max_dd_%']:>6.2f}%")
        # cheapest width that still caps the single-trade tail meaningfully
        ns = out[out["stop"] == "no stop"].iloc[0]
        cand = out[(out["stop"] != "no stop")
                   & (out["worst_trade_%"] > ns["worst_trade_%"] * 0.55)]
        if len(cand):
            pick = cand.loc[cand["mean_%"].idxmax()]
            print(f"\n  Cheapest width that still halves the single-trade tail: "
                  f"{pick['stop']}")
            print(f"    costs {pick['cost_vs_nostop_pts']:.0f} pts, caps the worst "
                  f"trade at {pick['worst_trade_%']:.2f}% instead of "
                  f"{ns['worst_trade_%']:.2f}%")
        print("\n  If tightening the stop barely improves max_dd_% while costing")
        print("  thousands of points, your bad trades are IDIOSYNCRATIC, not")
        print("  clustered on the same days -- and a portfolio-level kill switch is")
        print("  then the wrong instrument entirely.")
    return out
