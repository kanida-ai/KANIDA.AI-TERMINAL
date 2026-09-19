"""Basket-level paths and exit policies.

WHY THIS IS A DIFFERENT PROBLEM
-------------------------------
Everything before this optimised the average TRADE. Your objective is +1% on the
BASKET, and those are not the same problem -- because diversification changes
the arithmetic in your favour.

Twenty positions with an average pairwise correlation around 0.3 have roughly
55-60% of a single stock's volatility, while keeping the full mean. So a basket
whose average constituent is a 4% daily-range name behaves like a 2% one. The
mean stays at +1.3%; the noise around it halves. Reaching +1% reliably on the
basket is therefore a far more tractable target than reaching +1% reliably on any
one stock -- and it is the target your execution system can actually manage,
because you said it trails at basket level.

WHAT THIS MODULE MEASURES
-------------------------
For each trading day, the equal-weighted intraday path of that day's signals.
Then, against that path:

  P(basket touches +1%)      does the objective even occur?
  when it touches            is there time to act?
  terminal vs peak           how much of the peak survives to the close?
  entry timing               would entering at 09:30 or 10:00 beat 09:15?
  exit policies              hold, hard target, arm-and-trail, floor lock

Only the traded symbol-days are pulled from the intraday table -- a few thousand
rows, not a hundred million -- so this is cheap despite using 5-minute bars.

The policies simulated here map onto capabilities you already have. Nothing is
proposed that your execution system cannot do.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .sqlite_io import DBConfig, connect


# --------------------------------------------------------------------------- #
def load_trade_paths(db: DBConfig, trades: pd.DataFrame, bar_minutes: int = 5,
                     verbose: bool = True) -> pd.DataFrame:
    """Intraday return path from the open, for the traded symbol-days only."""
    cm = db.intraday_cols
    want = trades[["date", "symbol"]].drop_duplicates()
    syms = sorted(want["symbol"].unique())
    dates = {s: set(g["date"].dt.strftime("%Y-%m-%d"))
             for s, g in want.groupby("symbol")}

    con = connect(db.db_path)
    q = (f'SELECT {cm.select_clause()} FROM "{db.intraday_table}" '
         f'WHERE "{cm.symbol}" = ? ORDER BY "{cm.date}"')
    out = []
    for i, s in enumerate(syms, 1):
        df = pd.read_sql_query(q, con, params=[s])
        if df.empty:
            continue
        df["ts"] = pd.to_datetime(df["date"])
        df["session"] = df["ts"].dt.normalize()
        keep = df["ts"].dt.strftime("%Y-%m-%d").isin(dates[s])
        df = df[keep]
        if df.empty:
            continue
        if bar_minutes > 1:
            df = (df.set_index("ts")
                    .groupby("session")
                    .resample(f"{bar_minutes}min", label="left", closed="left")
                    .agg({"open": "first", "high": "max", "low": "min",
                          "close": "last"})
                    .dropna(subset=["close"])
                    .reset_index())
        df["bar"] = df.groupby("session").cumcount()
        first_open = df.groupby("session")["open"].transform("first")
        df["entry_px"] = first_open
        df["px"] = df["close"]
        df["r"] = df["close"] / first_open - 1.0
        df["r_hi"] = df["high"] / first_open - 1.0
        df["r_lo"] = df["low"] / first_open - 1.0
        df["symbol"] = s
        if "ts" not in df.columns:
            df["ts"] = df["session"]
        out.append(df[["session", "symbol", "bar", "ts", "entry_px", "px",
                       "r", "r_hi", "r_lo"]])
        if verbose and i % 50 == 0:
            print(f"    {i}/{len(syms)} symbols")
    con.close()
    if not out:
        raise RuntimeError("no intraday data found for the traded symbol-days")
    p = pd.concat(out, ignore_index=True).rename(columns={"session": "date"})
    if verbose:
        print(f"  loaded {len(p):,} bars across {p['date'].nunique()} sessions")
    return p


def build_basket(paths: pd.DataFrame, min_names: int = 2) -> pd.DataFrame:
    """Equal-weight the day's signals bar by bar."""
    g = paths.groupby(["date", "bar"])
    b = g.agg(ret=("r", "mean"), n=("symbol", "size")).reset_index()
    keep = b.groupby("date")["n"].transform("max") >= min_names
    b = b[keep].sort_values(["date", "bar"]).reset_index(drop=True)
    b["peak"] = b.groupby("date")["ret"].cummax()
    b["trough"] = b.groupby("date")["ret"].cummin()
    return b


# --------------------------------------------------------------------------- #
def basket_profile(b: pd.DataFrame, target: float = 0.01,
                   bar_minutes: int = 5, verbose: bool = True) -> pd.DataFrame:
    """Does the +1% you are aiming for actually happen, and when?"""
    rows = []
    for d, g in b.groupby("date"):
        g = g.sort_values("bar")
        hit = g[g["ret"] >= target]
        rows.append({
            "date": d, "n_names": int(g["n"].max()),
            "peak": float(g["ret"].max()), "trough": float(g["ret"].min()),
            "close": float(g["ret"].iloc[-1]),
            "touched": bool(len(hit)),
            "bar_touched": int(hit["bar"].iloc[0]) if len(hit) else -1,
            "give_back": float(g["ret"].max() - g["ret"].iloc[-1]),
        })
    prof = pd.DataFrame(rows)

    if verbose:
        n = len(prof)
        t = prof["touched"]
        print(f"\n{'=' * 78}\nBASKET PROFILE   {n} sessions, "
              f"target +{target:.1%}\n{'=' * 78}")
        print(f"  median names per day  : {prof['n_names'].median():.0f}")
        print(f"  P(touch +{target:.1%})        : {t.mean():.1%}")
        print(f"  P(close above target) : {(prof['close'] >= target).mean():.1%}")
        print(f"  mean close            : {prof['close'].mean()*100:+.3f}%")
        print(f"  mean peak             : {prof['peak'].mean()*100:+.3f}%")
        print(f"  mean give-back        : {prof['give_back'].mean()*100:.3f}%")
        if t.any():
            mins = prof.loc[t, "bar_touched"] * bar_minutes
            print(f"\n  when it touches, minutes after the open:")
            print(f"    25th {mins.quantile(.25):.0f}   median {mins.median():.0f}"
                  f"   75th {mins.quantile(.75):.0f}")
            held = prof.loc[t, "close"] >= target
            print(f"  of the days that touched, {held.mean():.1%} still closed above")
            print(f"  target -- the gap is what a floor lock is for.")
    return prof


def entry_timing(paths: pd.DataFrame, bar_minutes: int = 5,
                 verbose: bool = True) -> pd.DataFrame:
    """Would entering later than 09:15 have been better?

    Re-bases every path to a later bar and rebuilds the basket. Your execution
    system can enter at any time, so this is a free parameter you have never
    tested.
    """
    rows = []
    for k in [0, 1, 2, 3, 6, 12]:
        sub = paths[paths["bar"] >= k].copy()
        base = sub.groupby(["date", "symbol"])["r"].transform("first")
        sub["r"] = (1 + sub["r"]) / (1 + base) - 1.0
        bb = build_basket(sub)
        prof = basket_profile(bb, verbose=False)
        rows.append({
            "entry": f"+{k * bar_minutes}min",
            "sessions": len(prof),
            "mean_close_%": prof["close"].mean() * 100,
            "P_touch_1%": float((prof["peak"] >= 0.01).mean()),
            "mean_peak_%": prof["peak"].mean() * 100,
            "mean_trough_%": prof["trough"].mean() * 100,
        })
    out = pd.DataFrame(rows)
    if verbose:
        print(f"\n{'=' * 78}\nENTRY TIMING   basket outcome by entry delay"
              f"\n{'=' * 78}")
        print(out.round(3).to_string(index=False))
        print("\n  09:15 is a choice, not a constraint. If a later entry has a")
        print("  higher mean and a shallower trough, you were paying for the")
        print("  opening auction's noise.")
    return out


# --------------------------------------------------------------------------- #
def simulate_policies(b: pd.DataFrame, cost: float = 0.0011,
                      verbose: bool = True) -> pd.DataFrame:
    """Exit policies your system can already execute, scored on the basket path.

    hold           exit at the close
    target X       exit the moment the basket touches +X
    arm A give G   once +A is touched, exit if the basket falls G below its peak
    floor F        once +A is touched, exit if it falls back to +F
    trail T        exit whenever the basket is T below its running peak
    """
    policies = []
    policies.append(("hold to close", lambda g: g["ret"].iloc[-1]))
    for x in (0.005, 0.0075, 0.010, 0.015):
        policies.append((f"target +{x:.2%}",
                         lambda g, x=x: _first_touch_exit(g, x)))
    for a, gv in [(0.005, 0.002), (0.0075, 0.003), (0.010, 0.003), (0.010, 0.005)]:
        policies.append((f"arm +{a:.2%} give {gv:.2%}",
                         lambda g, a=a, gv=gv: _arm_giveback(g, a, gv)))
    for a, f in [(0.0075, 0.005), (0.010, 0.005), (0.010, 0.0075)]:
        policies.append((f"arm +{a:.2%} floor +{f:.2%}",
                         lambda g, a=a, f=f: _arm_floor(g, a, f)))
    for t in (0.003, 0.005, 0.0075):
        policies.append((f"trail {t:.2%}", lambda g, t=t: _trail(g, t)))

    rows = []
    for name, fn in policies:
        pnl = np.array([fn(g.sort_values("bar")) for _, g in b.groupby("date")])
        pnl = pnl - cost
        curve = np.cumprod(1 + pnl)
        dd = float((curve / np.maximum.accumulate(curve) - 1).min() * 100)
        rows.append({
            "policy": name, "days": len(pnl),
            "mean_%": pnl.mean() * 100, "median_%": float(np.median(pnl)) * 100,
            "win_rate": float((pnl > 0).mean()),
            "P_over_1%": float((pnl >= 0.01).mean()),
            "worst_day_%": pnl.min() * 100, "max_dd_%": dd,
            "total_%": pnl.sum() * 100,
        })
    out = pd.DataFrame(rows).sort_values("mean_%", ascending=False)

    if verbose:
        print(f"\n{'=' * 96}\nEXIT POLICIES ON THE BASKET   "
              f"(cost {cost:.2%} per day)\n{'=' * 96}")
        print(out.round(3).to_string(index=False))
        print("\n  P_over_1% is the column that matches your objective: the share")
        print("  of days the basket actually delivered +1% or better. A policy with")
        print("  a lower mean but a much higher P_over_1% may be the right one if")
        print("  consistency is what you are buying.")
    return out


def _first_touch_exit(g, x):
    h = g[g["r_hi_b"] >= x] if "r_hi_b" in g else g[g["ret"] >= x]
    return x if len(h) else g["ret"].iloc[-1]


def _arm_giveback(g, arm, give):
    r = g["ret"].to_numpy()
    peak = np.maximum.accumulate(r)
    armed = peak >= arm
    trig = np.where(armed & (r <= peak - give))[0]
    return float(r[trig[0]]) if len(trig) else float(r[-1])


def _arm_floor(g, arm, floor):
    r = g["ret"].to_numpy()
    peak = np.maximum.accumulate(r)
    armed = peak >= arm
    trig = np.where(armed & (r <= floor))[0]
    return float(floor) if len(trig) else float(r[-1])


def _trail(g, t):
    r = g["ret"].to_numpy()
    peak = np.maximum.accumulate(r)
    trig = np.where(r <= peak - t)[0]
    return float(r[trig[0]]) if len(trig) else float(r[-1])


# --------------------------------------------------------------------------- #
# choosing the exit policy HONESTLY, then reporting the book                    #
# --------------------------------------------------------------------------- #
POLICIES = {
    "hold": lambda r: len(r) - 1,
    "arm1.0_floor0.50": lambda r: _ix_arm_floor(r, 0.010, 0.005),
    "arm1.0_floor0.75": lambda r: _ix_arm_floor(r, 0.010, 0.0075),
    "arm0.75_floor0.50": lambda r: _ix_arm_floor(r, 0.0075, 0.005),
    "arm1.0_give0.50": lambda r: _ix_arm_give(r, 0.010, 0.005),
    "trail0.75": lambda r: _ix_trail(r, 0.0075),
    "trail0.50": lambda r: _ix_trail(r, 0.005),
}


def _ix_arm_floor(r, arm, floor):
    peak = np.maximum.accumulate(r)
    t = np.where((peak >= arm) & (r <= floor))[0]
    return int(t[0]) if len(t) else len(r) - 1


def _ix_arm_give(r, arm, give):
    peak = np.maximum.accumulate(r)
    t = np.where((peak >= arm) & (r <= peak - give))[0]
    return int(t[0]) if len(t) else len(r) - 1


def _ix_trail(r, tr):
    peak = np.maximum.accumulate(r)
    t = np.where(r <= peak - tr)[0]
    return int(t[0]) if len(t) else len(r) - 1


def walk_forward_policy(b: pd.DataFrame, cost: float = 0.0011,
                        min_train_months: int = 6,
                        verbose: bool = True) -> pd.DataFrame:
    """Pick the exit policy the way you would have had to pick it: blind.

    Choosing the best of fifteen policies by looking at all 526 sessions and
    then quoting that policy's return is exactly the overfitting this engine
    exists to prevent. With fifteen candidates the winner is inflated whether or
    not any of them is genuinely better.

    So: at the start of each month, pick whichever policy led on the months
    BEFORE it, and apply that choice to the month ahead. The result is what the
    procedure would actually have earned, including the months it picked wrong.
    """
    b = b.copy()
    b["month"] = pd.to_datetime(b["date"]).dt.to_period("M")
    months = sorted(b["month"].unique())
    daily = {name: {} for name in POLICIES}
    for d, g in b.groupby("date"):
        r = g.sort_values("bar")["ret"].to_numpy()
        for name, fn in POLICIES.items():
            daily[name][d] = float(r[fn(r)]) - cost

    rows = []
    for i, m in enumerate(months):
        if i < min_train_months:
            continue
        past = b[b["month"] < m]["date"].unique()
        scores = {n: np.mean([daily[n][d] for d in past]) for n in POLICIES}
        pick = max(scores, key=scores.get)
        for d in b[b["month"] == m]["date"].unique():
            rows.append({"date": d, "month": str(m), "policy": pick,
                         "ret": daily[pick][d],
                         "ret_hold": daily["hold"][d]})
    out = pd.DataFrame(rows)

    if verbose and not out.empty:
        print(f"\n{'=' * 78}\nWALK-FORWARD POLICY CHOICE   "
              f"{out['date'].nunique()} sessions\n{'=' * 78}")
        print(f"  policies chosen: {out['policy'].value_counts().to_dict()}")
        print(f"  chosen-blind mean : {out['ret'].mean()*100:+.3f}%")
        print(f"  always-hold mean  : {out['ret_hold'].mean()*100:+.3f}%")
        d = (out['ret'].mean() - out['ret_hold'].mean()) * 100
        print(f"  difference        : {d:+.3f}%")
        if d <= 0:
            print("\n  Choosing a policy from history did NOT beat simply holding.")
            print("  The in-sample ranking was selection, not skill.")
    return out


def simulate_book(paths: pd.DataFrame, b: pd.DataFrame, policy: str = "hold",
                  capital_per_stock: float = 50000.0, cost: float = 0.0011,
                  verbose: bool = True):
    """Per-trade log and monthly P&L at a fixed rupee allocation per position."""
    fn = POLICIES[policy]
    exit_bar = {}
    for d, g in b.groupby("date"):
        r = g.sort_values("bar")["ret"].to_numpy()
        exit_bar[d] = int(g.sort_values("bar")["bar"].to_numpy()[fn(r)])

    p = paths.copy()
    p["exit_bar"] = p["date"].map(exit_bar)
    ent = (p[p["bar"] == 0][["date", "symbol", "ts", "entry_px"]]
           .rename(columns={"ts": "entry_time"}))
    ex = p[p["bar"] == p["exit_bar"]][["date", "symbol", "ts", "px"]] \
        .rename(columns={"ts": "exit_time", "px": "exit_px"})
    t = ent.merge(ex, on=["date", "symbol"], how="inner")
    t["stock_ret_pct"] = (t["exit_px"] / t["entry_px"] - 1.0) * 100 - cost * 100
    t["pnl_rs"] = capital_per_stock * (t["stock_ret_pct"] / 100.0)
    t["exit_reason"] = policy
    t["Positive"] = np.where(t["stock_ret_pct"] > 0.05, "Yes",
                      np.where(t["stock_ret_pct"] < -0.05, "No", "Neutral"))
    t["Year"] = pd.to_datetime(t["date"]).dt.year
    t = t.rename(columns={"date": "trade_date"}).sort_values(
        ["trade_date", "symbol"]).reset_index(drop=True)
    t["entry_time"] = pd.to_datetime(t["entry_time"]).dt.strftime("%H:%M")
    t["exit_time"] = pd.to_datetime(t["exit_time"]).dt.strftime("%H:%M")

    day = t.groupby("trade_date").agg(n=("symbol", "size"),
                                      ret=("stock_ret_pct", "mean"),
                                      pnl=("pnl_rs", "sum")).reset_index()
    day["month"] = pd.to_datetime(day["trade_date"]).dt.to_period("M")
    mth = day.groupby("month").agg(
        days=("trade_date", "size"),
        days_pos=("ret", lambda s: int((s > 0).sum())),
        ret1x_pct=("ret", "sum"),
        pnl_rs=("pnl", "sum")).reset_index()
    mth["month"] = mth["month"].astype(str)

    if verbose:
        print(f"\n{'=' * 78}\nMONTHLY P&L   policy '{policy}', "
              f"Rs {capital_per_stock:,.0f} per position\n{'=' * 78}")
        print(mth.round(2).to_string(index=False))
        print(f"\n  total {len(t):,} trades, "
              f"Rs {t['pnl_rs'].sum():,.0f}   "
              f"positive months {int((mth['pnl_rs'] > 0).sum())}/{len(mth)}")
        print(f"  median positions per day: {day['n'].median():.0f}")
        print(f"  peak capital deployed   : "
              f"Rs {capital_per_stock * day['n'].max():,.0f}")
    return t, mth
