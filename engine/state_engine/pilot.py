"""Two-stock pilot: the unconditional baseline, measured before any mining.

THE ORDER OF OPERATIONS THIS ENFORCES
-------------------------------------
Every earlier module started from a signal and asked whether it could be
improved. This starts from nothing and asks the prior question:

    if I enter this stock at a fixed time and exit at a fixed time,
    what normally happens?

That number is the benchmark. A conditional rule is only worth anything if it
beats the unconditional baseline for the same stock, side and horizon -- and
until the baseline exists there is nothing to beat.

WHY THRESHOLD SEQUENCING IS THE POINT
-------------------------------------
Two trades can close at the same price and require opposite execution logic. One
reaches +1% and then falls to -1%; the other falls to -1% first and recovers.
The closing return hides the difference entirely. Any stop, target, trail or
arm-and-floor rule lives or dies on which came first, so this records the ORDER
in which each threshold was touched, not merely whether it was.

Resolution is at bar level. Within a single bar the order is genuinely unknown,
and those cases are counted separately rather than guessed.

WHAT IS AND IS NOT MODELLED
---------------------------
Costs are charged per round trip and configurable. Leverage is reported as a
separate view of the same trades, never baked in. Short results assume intraday
MIS shorting, which is legal in India; positional shorts would need futures with
contract sizes, margins and rollover, and are deliberately NOT modelled here
rather than approximated with cash equity.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

import numpy as np
import pandas as pd

from .sqlite_io import DBConfig, connect


@dataclass
class PilotConfig:
    symbols: Sequence[str] = ("ADANIENT", "CARTRADE")
    start: str = "2022-01-01"
    end: str = "2025-12-31"          # 2026 held back untouched
    notional: float = 25000.0
    cost_pct: float = 0.0011         # round trip, retail intraday India
    bar_minutes: int = 5
    exit_times: Sequence[str] = ("09:30", "10:00", "10:30", "11:00",
                                 "12:00", "13:00", "14:00", "15:00", "close")
    thresholds: Sequence[float] = (0.0025, 0.005, 0.0075, 0.010, 0.015, 0.020)
    horizons: Sequence[int] = (0, 1, 2, 3, 5, 7)
    leverage: Sequence[float] = (1.0, 5.0)
    entry_time: str = "09:15"
    overnight_entry: str = "15:00"
    slippage_pct: float = 0.0005   # charged on top of cost_pct, per round trip

    def convention(self) -> str:
        return (
            "EXECUTION CONVENTION\n"
            f"  signal      : known before the session opens\n"
            f"  order       : placed pre-open\n"
            f"  fill        : the {cfg_open()} of the {self.entry_time} bar\n"
            f"  exit fill   : the CLOSE of the bar at or after the stated time\n"
            f"  costs       : {self.cost_pct*100:.3f}% round trip "
            f"(brokerage + STT 0.025% sell side + exchange + GST + stamp)\n"
            f"  slippage    : {self.slippage_pct*100:.3f}% round trip\n"
            f"  ALL RETURNS BELOW ARE NET of "
            f"{(self.cost_pct + self.slippage_pct)*100:.3f}%\n"
            "  Filling at the opening print assumes the order existed before the\n"
            "  auction. If you decide after seeing the open, this is optimistic.")


def cfg_open() -> str:
    return "OPEN"


# --------------------------------------------------------------------------- #
def load_bars(db: DBConfig, cfg: PilotConfig, verbose: bool = True) -> pd.DataFrame:
    cm = db.intraday_cols
    con = connect(db.db_path)
    q = (f'SELECT {cm.select_clause()} FROM "{db.intraday_table}" '
         f'WHERE "{cm.symbol}" = ? AND "{cm.date}" >= ? AND "{cm.date}" <= ? '
         f'ORDER BY "{cm.date}"')
    out = []
    for s in cfg.symbols:
        d = pd.read_sql_query(q, con, params=[s, cfg.start, cfg.end + " 23:59"])
        if d.empty:
            print(f"  WARNING no intraday rows for {s}")
            continue
        d["ts"] = pd.to_datetime(d["date"])
        d["session"] = d["ts"].dt.normalize()
        if cfg.bar_minutes > 1:
            d = (d.set_index("ts").groupby("session")
                   .resample(f"{cfg.bar_minutes}min", label="left", closed="left")
                   .agg({"open": "first", "high": "max", "low": "min",
                         "close": "last", "volume": "sum"})
                   .dropna(subset=["close"]).reset_index())
        d["symbol"] = s
        d["hm"] = d["ts"].dt.strftime("%H:%M")
        out.append(d)
    con.close()
    if not out:
        raise RuntimeError("no intraday data for the pilot symbols")
    b = pd.concat(out, ignore_index=True)
    per_session = len(b) / max(b["session"].nunique() * b["symbol"].nunique(), 1)
    native = 375.0 / max(per_session, 1)
    if verbose:
        print(f"  {len(b):,} bars, {b['session'].nunique()} sessions, "
              f"{b['symbol'].nunique()} symbols "
              f"({b['session'].min().date()} -> {b['session'].max().date()})")
        print(f"  ~{per_session:.0f} bars/session -> native resolution "
              f"~{native:.0f} minutes, from table '{db.intraday_table}'")
        if abs(native - cfg.bar_minutes) > 0.6:
            print(f"\n  !! bar_minutes={cfg.bar_minutes} but the data is "
                  f"~{native:.0f}-minute.")
            print(f"  !! Asking for a FINER resolution than the source table has")
            print(f"  !! cannot create it. Every 'minutes to threshold' figure")
            print(f"  !! below is wrong by a factor of {native/cfg.bar_minutes:.0f}.")
            print(f"  !! Point intraday_table at your 1-minute table to fix it.")
    return b


# --------------------------------------------------------------------------- #
def _path_row(g: pd.DataFrame, entry: float, side: str,
              cfg: PilotConfig) -> dict:
    """Full path description of one intraday trade, including touch ORDER."""
    s = 1.0 if side == "long" else -1.0
    fav = (g["high"] / entry - 1.0) * s if side == "long" else (1.0 - g["low"] / entry)
    adv = (g["low"] / entry - 1.0) * s if side == "long" else (1.0 - g["high"] / entry)
    fav, adv = fav.to_numpy(), adv.to_numpy()
    mins = np.arange(len(g)) * cfg.bar_minutes

    i_mfe, i_mae = int(np.argmax(fav)), int(np.argmin(adv))
    peak_run = np.maximum.accumulate(fav)
    close_ret = ((g["close"].iloc[-1] / entry - 1.0) * s)

    r = {
        "mfe": float(fav.max()), "mae": float(adv.min()),
        "min_to_mfe": int(mins[i_mfe]), "min_to_mae": int(mins[i_mae]),
        "give_back": float(fav.max() - close_ret),
        "close_ret": float(close_ret),
    }
    for t in cfg.thresholds:
        up = np.where(fav >= t)[0]
        dn = np.where(adv <= -t)[0]
        tag = f"{t*100:g}"
        r[f"touch_up_{tag}"] = int(len(up) > 0)
        r[f"touch_dn_{tag}"] = int(len(dn) > 0)
        if len(up) and len(dn):
            # same bar: the order is genuinely unknown, so say so
            r[f"first_{tag}"] = ("up" if up[0] < dn[0] else
                                 "dn" if dn[0] < up[0] else "same_bar")
        elif len(up):
            r[f"first_{tag}"] = "up"
        elif len(dn):
            r[f"first_{tag}"] = "dn"
        else:
            r[f"first_{tag}"] = "neither"
        r[f"min_to_up_{tag}"] = int(mins[up[0]]) if len(up) else -1
        r[f"min_to_dn_{tag}"] = int(mins[dn[0]]) if len(dn) else -1
    return r


def intraday_baseline(bars: pd.DataFrame, cfg: PilotConfig,
                      verbose: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Enter 09:15, exit at each fixed time. Long and short, independently."""
    trades = []
    for (sym, sess), g in bars.groupby(["symbol", "session"], sort=True):
        g = g.sort_values("ts").reset_index(drop=True)
        if len(g) < 10:
            continue
        entry = float(g["open"].iloc[0])
        if not np.isfinite(entry) or entry <= 0:
            continue
        for side in ("long", "short"):
            s = 1.0 if side == "long" else -1.0
            for et in cfg.exit_times:
                if et == "close":
                    j = len(g) - 1
                else:
                    m = np.where(g["hm"].to_numpy() >= et)[0]
                    if not len(m):
                        continue
                    j = int(m[0])
                px, when = float(g["close"].iloc[j]), g["hm"].iloc[j]
                # BUG FIXED: the path must stop at the exit bar. Measuring MFE
                # and MAE over the whole session and attaching them to a 09:30
                # exit uses prices from after the trade closed -- lookahead, and
                # it made every exit time show identical path stats.
                path = _path_row(g.iloc[:j + 1], entry, side, cfg)
                gross = (px / entry - 1.0) * s
                trades.append({
                    "symbol": sym, "date": sess, "side": side,
                    "exit_time": et, "actual_exit": when,
                    "entry_px": entry, "exit_px": px,
                    "ret_pct": (gross - cfg.cost_pct - cfg.slippage_pct) * 100,
                    "pnl_rs": cfg.notional * (gross - cfg.cost_pct - cfg.slippage_pct),
                    **path,
                })
    t = pd.DataFrame(trades)
    if t.empty:
        raise RuntimeError("no intraday trades simulated")

    g = t.groupby(["symbol", "side", "exit_time"])
    summ = g.agg(
        n=("ret_pct", "size"),
        mean_pct=("ret_pct", "mean"), median_pct=("ret_pct", "median"),
        win_rate=("ret_pct", lambda s: float((s > 0).mean())),
        p_over_1pct=("ret_pct", lambda s: float((s >= 1.0).mean())),
        mean_mfe=("mfe", lambda s: float(s.mean() * 100)),
        mean_mae=("mae", lambda s: float(s.mean() * 100)),
        worst_pct=("ret_pct", "min"), best_pct=("ret_pct", "max"),
        total_rs=("pnl_rs", "sum"),
    ).reset_index()
    summ["payoff"] = summ["mean_mfe"] / summ["mean_mae"].abs().replace(0, np.nan)
    order = {t: i for i, t in enumerate(cfg.exit_times)}
    summ = summ.sort_values(["symbol", "side", "exit_time"],
                            key=lambda c: c.map(order) if c.name == "exit_time" else c)

    if verbose:
        print(f"\n{'=' * 104}\nPHASE 1  INTRADAY BASELINE   enter 09:15, "
              f"exit at a fixed time, Rs {cfg.notional:,.0f} notional\n{'=' * 104}")
        print(cfg.convention())
        print()
        print(summ.round(3).to_string(index=False))
        print("\n  This is UNCONDITIONAL -- every single session, no filter.")
        print("  Any state or signal must beat the row for its own stock, side")
        print("  and exit time. Nothing else counts as an improvement.")
    return t, summ


# --------------------------------------------------------------------------- #
def threshold_profile(trades: pd.DataFrame, cfg: PilotConfig,
                      verbose: bool = True) -> pd.DataFrame:
    """Which threshold was touched first -- the number execution logic needs."""
    t = trades[trades["exit_time"] == "close"]
    rows = []
    for (sym, side), g in t.groupby(["symbol", "side"]):
        for th in cfg.thresholds:
            tag = f"{th*100:g}"
            fc = g[f"first_{tag}"]
            rows.append({
                "symbol": sym, "side": side, "threshold_%": th * 100,
                "n": len(g),
                "P_touch_up": float(g[f"touch_up_{tag}"].mean()),
                "P_touch_dn": float(g[f"touch_dn_{tag}"].mean()),
                "P_up_first": float((fc == "up").mean()),
                "P_dn_first": float((fc == "dn").mean()),
                "P_same_bar": float((fc == "same_bar").mean()),
                "P_neither": float((fc == "neither").mean()),
                "med_min_to_up": float(
                    g.loc[g[f"min_to_up_{tag}"] >= 0, f"min_to_up_{tag}"].median()),
            })
    out = pd.DataFrame(rows)
    if verbose:
        print(f"\n{'=' * 104}\nPHASE 2  THRESHOLD SEQUENCING   which came first, "
              f"the gain or the loss?\n{'=' * 104}")
        print(out.round(3).to_string(index=False))
        print("\n  P_up_first is the number a target-and-stop pair depends on.")
        print("  P_same_bar is honest ignorance: both touched inside one bar, so")
        print("  the order cannot be known at this resolution. If it is large,")
        print("  re-run at 1-minute before trusting any bracket.")
        worst = out["P_same_bar"].max()
        if worst > 0.15:
            bad = out.loc[out["P_same_bar"] > 0.15, "threshold_%"].max()
            print(f"\n  VERDICT: same-bar ambiguity reaches {worst:.0%}. Do NOT")
            print(f"  optimise any bracket at or below {bad:g}% on these bars.")
    return out


# --------------------------------------------------------------------------- #
def multi_session(daily: pd.DataFrame, cfg: PilotConfig,
                  verbose: bool = True) -> pd.DataFrame:
    """Enter at the open, exit H sessions later. Both capital models.

    overlapping   a new position every day, so H positions run at once and the
                  capital requirement is H x notional
    rollover      one position at a time, entered only after the last exits

    These are different businesses and are never averaged together.
    """
    d = daily[daily["symbol"].isin(cfg.symbols)].sort_values(["symbol", "date"])
    rows = []
    bh = {}
    for sym, g in d.groupby("symbol"):
        n_yr = len(g) / 250.0
        tot = float(g["close"].iloc[-1] / g["open"].iloc[0] - 1.0)
        bh[sym] = {"total_%": tot * 100,
                   "cagr_%": ((1 + tot) ** (1 / max(n_yr, 0.1)) - 1) * 100}
    for sym, g in d.groupby("symbol"):
        g = g.reset_index(drop=True)
        entry = g["open"].shift(-1)
        for H in cfg.horizons:
            exit_c = g["close"].shift(-1) if H == 0 else g["close"].shift(-(H + 1))
            for side in ("long", "short"):
                s = 1.0 if side == "long" else -1.0
                r = ((exit_c / entry - 1.0) * s
                     - cfg.cost_pct - cfg.slippage_pct).dropna()
                if len(r) < 50:
                    continue
                hold = max(H + 1, 1)
                # OVERLAPPING: hold positions run CONCURRENTLY, so compounding
                # them one after another is meaningless -- it is what produced
                # the -99% drawdowns. The book earns 1/hold of each position's
                # return per day; that daily book return is what compounds.
                daily_book = r.rolling(hold, min_periods=1).mean() / hold * hold
                daily_book = r / hold
                n_yr = len(r) / 250.0
                a = _ms_row(sym, side, H, "overlapping", r, cfg,
                            capital=cfg.notional * hold, book_daily=daily_book)
                b_ = _ms_row(sym, side, H, "rollover", r.iloc[::hold], cfg,
                             capital=cfg.notional, book_daily=r.iloc[::hold])
                for row in (a, b_):
                    row["cagr_%"] = ((1 + row["total_rs"] / row["capital_rs"])
                                     ** (1 / max(n_yr, 0.1)) - 1) * 100
                    row["buyhold_cagr_%"] = bh[sym]["cagr_%"]
                    row["cagr_%"] = float("nan") if isinstance(row["cagr_%"], complex) else row["cagr_%"]
                    row["beats_buyhold"] = bool(row["cagr_%"] == row["cagr_%"] and row["cagr_%"] > bh[sym]["cagr_%"])
                rows.append(a)
                rows.append(b_)
    out = pd.DataFrame(rows)
    if verbose:
        print(f"\n{'=' * 110}\nPHASE 1b  MULTI-SESSION   enter at the open, "
              f"exit H sessions later\n{'=' * 110}")
        print(out.round(3).to_string(index=False))
        print("\n  ret_on_capital_% divides by the capital the schedule actually")
        print("  ties up. Overlapping looks better per trade and needs H times the")
        print("  money; rollover needs one unit and trades far less often.")
        print("\n  buyhold_cagr_% is the benchmark that matters for multi-day LONGS.")
        print("  A stock that tripled makes any long-biased schedule look brilliant.")
        print("  If beats_buyhold is False you have found the stock's drift, not an")
        print("  edge -- and you took drawdown risk to underperform simply holding.")
    return out


def _ms_row(sym, side, H, mode, r, cfg, capital, book_daily=None):
    bd = r if book_daily is None else book_daily
    curve = (1 + bd).cumprod()
    dd = float((curve / curve.cummax() - 1).min() * 100)
    total_rs = float((r * cfg.notional).sum())
    return {
        "symbol": sym, "side": side, "hold_sessions": H + 1, "schedule": mode,
        "n": len(r), "mean_pct": float(r.mean() * 100),
        "median_pct": float(r.median() * 100),
        "win_rate": float((r > 0).mean()),
        "p_over_1pct": float((r >= 0.01).mean()),
        "worst_pct": float(r.min() * 100), "max_dd_%": dd,
        "note": ("THEORETICAL short - cash equity cannot hold overnight; "
                 "needs futures" if (side == "short" and H > 0) else ""),
        "capital_rs": capital, "total_rs": total_rs,
        "ret_on_capital_%": total_rs / capital * 100,
    }


# --------------------------------------------------------------------------- #
def directional_scorecard(summ: pd.DataFrame, trades: pd.DataFrame,
                          cfg: PilotConfig, verbose: bool = True) -> pd.DataFrame:
    """Long vs short, per stock, on economically meaningful measures -- not win rate.

    A side wins only if it leads on several dimensions at once. A higher win
    rate with a worse payoff ratio is not an edge, it is a different shape of
    the same nothing.
    """
    t = trades[trades["exit_time"] == "close"].copy()
    t["year"] = pd.to_datetime(t["date"]).dt.year
    t["month"] = pd.to_datetime(t["date"]).dt.to_period("M").astype(str)

    rows = []
    for (sym, side), g in t.groupby(["symbol", "side"]):
        r = g["ret_pct"] / 100
        curve = (1 + r).cumprod()
        by_m = g.groupby("month")["ret_pct"].sum()
        by_y = g.groupby("year")["ret_pct"].sum()
        rows.append({
            "symbol": sym, "side": side, "n": len(g),
            "mean_pct": float(r.mean() * 100),
            "median_pct": float(r.median() * 100),
            "win_rate": float((r > 0).mean()),
            "P_plus1pct": float(g["mfe"].ge(0.01).mean()),
            "mean_mfe_%": float(g["mfe"].mean() * 100),
            "mean_mae_%": float(g["mae"].mean() * 100),
            "reward_to_adverse": float(g["mfe"].mean() / abs(g["mae"].mean()))
            if g["mae"].mean() else np.nan,
            "worst_trade_%": float(r.min() * 100),
            "max_dd_%": float((curve / curve.cummax() - 1).min() * 100),
            "pos_months": f"{int((by_m > 0).sum())}/{len(by_m)}",
            "pos_years": f"{int((by_y > 0).sum())}/{len(by_y)}",
            "total_rs": float(g["pnl_rs"].sum()),
        })
    sc = pd.DataFrame(rows)

    verdicts = []
    for sym, g in sc.groupby("symbol"):
        if len(g) != 2:
            continue
        L = g[g["side"] == "long"].iloc[0]
        S = g[g["side"] == "short"].iloc[0]
        wins = sum([L["mean_pct"] > S["mean_pct"],
                    L["median_pct"] > S["median_pct"],
                    L["win_rate"] > S["win_rate"],
                    L["reward_to_adverse"] > S["reward_to_adverse"],
                    L["max_dd_%"] > S["max_dd_%"]])
        best = max(L["mean_pct"], S["mean_pct"])
        verdicts.append({
            "symbol": sym, "long_wins_on": f"{wins}/5",
            "verdict": ("long" if wins >= 4 else "short" if wins <= 1 else "neither"),
            "best_side_mean_%": best,
            "tradable": "yes" if best > 0 else "NO - both sides lose",
        })
    v = pd.DataFrame(verdicts)

    if verbose:
        print(f"\n{'=' * 112}\nPHASE 3  DIRECTIONAL SCORECARD   "
              f"09:15 to close, unconditional\n{'=' * 112}")
        print(sc.round(3).to_string(index=False))
        print("\n" + v.round(3).to_string(index=False))
        print("\n  'tradable = NO' means neither direction has an unconditional")
        print("  edge on this stock. That is a legitimate and useful result: it")
        print("  says any profit must come from CONDITIONAL selection, and sets")
        print("  the bar that conditioning has to clear.")
    return sc


# --------------------------------------------------------------------------- #
def leverage_view(summ: pd.DataFrame, cfg: PilotConfig,
                  verbose: bool = True) -> pd.DataFrame:
    """The same trades, expressed on margin rather than notional."""
    rows = []
    for lev in cfg.leverage:
        s = summ.copy()
        s["leverage"] = lev
        s["margin_rs"] = cfg.notional / lev
        s["ret_on_margin_%"] = s["mean_pct"] * lev
        s["worst_on_margin_%"] = s["worst_pct"] * lev
        rows.append(s[["symbol", "side", "exit_time", "leverage", "margin_rs",
                       "mean_pct", "ret_on_margin_%", "worst_pct",
                       "worst_on_margin_%"]])
    out = pd.concat(rows, ignore_index=True)
    if verbose:
        print(f"\n{'=' * 96}\nCAPITAL AND LEVERAGE   same trades, "
              f"different capital base\n{'=' * 96}")
        best = out[out["exit_time"] == "close"]
        print(best.round(3).to_string(index=False))
        print("\n  Leverage multiplies the loss identically. worst_on_margin_% is")
        print("  the number that decides whether 5x is survivable, not the return.")
    return out
