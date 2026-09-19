"""
SPS_V1 — Baseline Intraday Profitability Engine
================================================
Question it answers (per stock, per side):
  "If I mechanically ENTER at 09:15 every day and hold, what normally happens
   minute-by-minute until EOD?"  -> then: When to enter? How long to stay? How to manage?

Design / leak-free guarantees
-----------------------------
* Entry price   = OPEN of the 09:15 one-minute candle (first tradable print; no
                  same-bar close look-ahead).
* Marks         = CLOSE at each forward minute offset (decision at end of minute t).
* MAE / MFE     = intrabar HIGH/LOW over the held window (realistic excursions).
* Forward-only  = every statistic is measured AFTER entry; no future info selects
                  days or entries.
* Costs         = round-trip pct of notional (STT + exch + brokerage + slippage),
                  applied to every measured round trip. Gross AND net reported.
* Sizing        = fixed CAPITAL per (stock, side); qty = floor(CAPITAL / entry).
* In-sample     = 2022-2025 (the requested study window).
* Out-of-sample = Jan-2026 -> latest (held out) to validate recommendations.

Usage:  python engine.py
Outputs: ./outputs/<STOCK>_<SIDE>_drift.csv, _summary.json, report.md, *.png
"""
from __future__ import annotations
import json, sqlite3
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DB = HERE.parents[1] / "db" / "kanida.db"          # Kanida_Falcon/db/kanida.db
OUT = HERE / "outputs"; OUT.mkdir(exist_ok=True)

# ---- config ----------------------------------------------------------------
STOCKS = ["ADANIENT", "CARTRADE"]
SIDES = ["long", "short"]
CAPITAL = 30_000.0
RT_COST_PCT = 0.0010            # 0.10% round-trip (STT+exch+brokerage+slippage)
IS_START, IS_END = "2022-01-01", "2025-12-31"      # in-sample (requested)
OOS_START, OOS_END = "2026-01-01", "2026-12-31"     # held-out validation
SESSION_MIN = 375               # 09:15..15:29 inclusive
ENTRY_SCAN = [0, 5, 10, 15, 30, 45, 60, 90, 120, 180, 240]   # minute offsets to test as entry
HOLD_MARKS = [5, 15, 30, 45, 60, 90, 120, 180, 240, 374]     # minute offsets to report


# ---- data ------------------------------------------------------------------
def load(symbol: str, start: str, end: str) -> pd.DataFrame:
    con = sqlite3.connect(str(DB))
    df = pd.read_sql(
        "SELECT bar_time,open,high,low,close FROM ohlc_1min "
        "WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
        con, params=[symbol, start, end + " 23:59:59"])
    con.close()
    dt = pd.to_datetime(df["bar_time"])
    df["date"] = dt.dt.date
    df["off"] = (dt.dt.hour * 60 + dt.dt.minute) - (9 * 60 + 15)
    return df[(df["off"] >= 0) & (df["off"] < SESSION_MIN)]


def day_matrices(df: pd.DataFrame):
    """Return per-day aligned arrays on a 0..374 minute grid.
       entry[d]         = open of the 09:15 bar
       close_mat[d,t]   = close at offset t (ffilled within day; leading NaN->entry)
       runmax/runmin[d,t] = running intrabar high/low from entry..t  (for MAE/MFE)
    """
    dates = sorted(df["date"].unique())
    N = SESSION_MIN
    close_mat = np.full((len(dates), N), np.nan)
    high_mat = np.full((len(dates), N), np.nan)
    low_mat = np.full((len(dates), N), np.nan)
    entry = np.full(len(dates), np.nan)
    keep = []
    g = df.groupby("date")
    for d in dates:
        sub = g.get_group(d)
        row0 = sub[sub["off"] == 0]
        if row0.empty:               # no 09:15 candle -> skip (special/half session start)
            continue
        i = len(keep)
        entry[i] = float(row0["open"].iloc[0])
        offs = sub["off"].to_numpy()
        close_mat[i, offs] = sub["close"].to_numpy()
        high_mat[i, offs] = sub["high"].to_numpy()
        low_mat[i, offs] = sub["low"].to_numpy()
        keep.append(d)
    n = len(keep)
    close_mat, high_mat, low_mat, entry = close_mat[:n], high_mat[:n], low_mat[:n], entry[:n]
    # forward-fill close along time; leading NaN -> entry (position held at entry price)
    close_df = pd.DataFrame(close_mat).ffill(axis=1)
    for i in range(n):
        close_df.iloc[i] = close_df.iloc[i].fillna(entry[i])
    close_mat = close_df.to_numpy()
    # running max/min of intrabar extremes from entry..t
    high_run = pd.DataFrame(high_mat).ffill(axis=1).cummax(axis=1).to_numpy()
    low_run = pd.DataFrame(low_mat).ffill(axis=1).cummin(axis=1).to_numpy()
    # seed extremes with entry so t=0 excursion is ~0
    high_run = np.fmax(high_run, entry[:, None])
    low_run = np.fmin(low_run, entry[:, None])
    return np.array(keep), entry, close_mat, high_run, low_run


def side_ret(gross_close_over_entry, side):
    """gross return of the position given close/entry ratio."""
    r = gross_close_over_entry - 1.0
    return r if side == "long" else -r


# ---- analysis --------------------------------------------------------------
def analyze(symbol, df, side, write=True):
    dates, entry, close_mat, high_run, low_run = day_matrices(df)
    n = len(dates)
    qty = np.floor(CAPITAL / entry)
    ratio = close_mat / entry[:, None]                     # close/entry per (day,offset)
    gross = side_ret(ratio, side)                           # gross return per (day,offset)
    net = gross - RT_COST_PCT                               # net after round-trip cost
    # drift curves (mean/median across days per offset)
    drift = pd.DataFrame({
        "offset_min": np.arange(SESSION_MIN),
        "gross_mean": np.nanmean(gross, axis=0),
        "net_mean": np.nanmean(net, axis=0),
        "net_median": np.nanmedian(net, axis=0),
        "pct_positive": np.nanmean(net > 0, axis=0),
    })
    # EOD (offset 374) outcome
    eod_net = net[:, -1]
    eod_pnl = qty * entry * gross[:, -1] - RT_COST_PCT * qty * entry   # ₹ P&L net
    # MAE / MFE over full day, in return terms for THIS side
    if side == "long":
        mfe = high_run[:, -1] / entry - 1.0
        mae = low_run[:, -1] / entry - 1.0
    else:
        mfe = entry / low_run[:, -1] * 0 + (entry - low_run[:, -1]) / entry   # price falls => profit
        mae = (entry - high_run[:, -1]) / entry
    # hold-duration: mean net by offset, find peak (expectancy) and risk-adj peak
    m = drift["net_mean"].to_numpy()
    sd = np.nanstd(net, axis=0); sd[sd == 0] = np.nan
    sharpe_t = m / sd
    t_star_exp = int(np.nanargmax(m))
    t_star_ra = int(np.nanargmax(np.where(np.arange(SESSION_MIN) >= 5, sharpe_t, -np.inf)))
    # entry-time scan: enter at offset E (open≈close[E-1]->use close_mat at E as entry proxy), hold to EOD
    scan = []
    for E in ENTRY_SCAN:
        ent = close_mat[:, E]                              # price at entry offset E
        q = np.floor(CAPITAL / ent)
        r = side_ret(close_mat[:, -1] / ent, side) - RT_COST_PCT
        scan.append({"entry_off": E, "net_mean_ret": float(np.nanmean(r)),
                     "win_rate": float(np.nanmean(r > 0)),
                     "net_mean_pnl": float(np.nanmean(q * ent * (side_ret(close_mat[:, -1] / ent, side)) - RT_COST_PCT * q * ent))})
    def pctl(a, ps): return {f"p{p}": float(np.nanpercentile(a, p)) for p in ps}
    summary = {
        "symbol": symbol, "side": side, "days": int(n),
        "capital": CAPITAL, "rt_cost_pct": RT_COST_PCT,
        "avg_qty": float(np.nanmean(qty)),
        "baseline_0915_to_eod": {
            "gross_mean_ret": float(np.nanmean(gross[:, -1])),
            "net_mean_ret": float(np.nanmean(eod_net)),
            "net_median_ret": float(np.nanmedian(eod_net)),
            "win_rate": float(np.nanmean(eod_net > 0)),
            "net_mean_pnl": float(np.nanmean(eod_pnl)),
            "net_total_pnl": float(np.nansum(eod_pnl)),
            "net_ret_std": float(np.nanstd(eod_net)),
            "sharpe_daily": float(np.nanmean(eod_net) / (np.nanstd(eod_net) + 1e-12)),
        },
        "mae_ret_pctiles": pctl(mae, [5, 25, 50, 75]),
        "mfe_ret_pctiles": pctl(mfe, [25, 50, 75, 95]),
        "hold_curve": {str(t): float(drift["net_mean"].iloc[t]) for t in HOLD_MARKS},
        "t_star_expectancy_min": t_star_exp,
        "t_star_risk_adj_min": t_star_ra,
        "entry_scan": scan,
    }
    if write:
        drift.to_csv(OUT / f"{symbol}_{side}_drift.csv", index=False)
    # equity curve of daily EOD pnl
    eq = np.nancumsum(eod_pnl)
    dd = eq - np.maximum.accumulate(eq)
    summary["baseline_0915_to_eod"]["max_drawdown_pnl"] = float(dd.min())
    return summary, drift, (mae, mfe, eod_pnl, eq)


def oos_check(symbol, side):
    df = load(symbol, OOS_START, OOS_END)
    if df.empty:
        return None
    s, _, _ = analyze(symbol, df, side, write=False)
    b = s["baseline_0915_to_eod"]
    return {"days": s["days"], "net_mean_ret": b["net_mean_ret"],
            "win_rate": b["win_rate"], "net_mean_pnl": b["net_mean_pnl"],
            "net_total_pnl": b["net_total_pnl"]}


def plot(symbol, side, drift, extras):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return
    mae, mfe, eod_pnl, eq = extras
    fig, ax = plt.subplots(1, 2, figsize=(12, 4))
    ax[0].plot(drift["offset_min"], drift["net_mean"] * 100, label="net mean")
    ax[0].plot(drift["offset_min"], drift["net_median"] * 100, alpha=.6, label="net median")
    ax[0].axhline(0, color="k", lw=.6); ax[0].set_title(f"{symbol} {side}: mean return vs minutes held (09:15 entry)")
    ax[0].set_xlabel("minutes after 09:15"); ax[0].set_ylabel("return %"); ax[0].legend()
    ax[1].plot(eq); ax[1].set_title(f"{symbol} {side}: cumulative net ₹ (hold to EOD daily)")
    ax[1].set_xlabel("trading day"); ax[1].set_ylabel("cum ₹")
    fig.tight_layout(); fig.savefig(OUT / f"{symbol}_{side}.png", dpi=110); plt.close(fig)


def main():
    report = ["# SPS_V1 — Baseline Intraday Profitability Profile",
              f"_generated {datetime.now():%Y-%m-%d %H:%M} · capital ₹{CAPITAL:,.0f}/side · "
              f"round-trip cost {RT_COST_PCT*100:.2f}% · in-sample {IS_START}..{IS_END} · OOS {OOS_START}.._latest_",
              ""]
    allsum = {}
    for symbol in STOCKS:
        df = load(symbol, IS_START, IS_END)
        report.append(f"## {symbol}")
        for side in SIDES:
            s, drift, extras = analyze(symbol, df, side)
            plot(symbol, side, drift, extras)
            oos = oos_check(symbol, side)
            s["oos_2026"] = oos
            allsum[f"{symbol}_{side}"] = s
            json.dump(s, open(OUT / f"{symbol}_{side}_summary.json", "w"), indent=2)
            b = s["baseline_0915_to_eod"]
            report.append(f"### {side.upper()}  (n={s['days']} days, avg qty {s['avg_qty']:.0f})")
            report.append(f"- **Baseline 09:15→EOD**: net mean **{b['net_mean_ret']*100:+.3f}%/day** "
                          f"(gross {s['baseline_0915_to_eod']['gross_mean_ret']*100:+.3f}%), "
                          f"win rate {b['win_rate']*100:.1f}%, mean ₹{b['net_mean_pnl']:+.1f}/day, "
                          f"total ₹{b['net_total_pnl']:+,.0f}, Sharpe {b['sharpe_daily']:.3f}, maxDD ₹{b['max_drawdown_pnl']:,.0f}")
            report.append(f"- **Hold curve** (net mean ret by minutes held): " +
                          ", ".join(f"{t}m={s['hold_curve'][str(t)]*100:+.2f}%" for t in [15, 30, 60, 120, 374]))
            report.append(f"- **Best hold** ≈ {s['t_star_expectancy_min']}m (expectancy), "
                          f"{s['t_star_risk_adj_min']}m (risk-adj)")
            report.append(f"- **MAE** (adverse) p25/p50/p75 = "
                          f"{s['mae_ret_pctiles']['p25']*100:.2f}/{s['mae_ret_pctiles']['p50']*100:.2f}/{s['mae_ret_pctiles']['p75']*100:.2f}% · "
                          f"**MFE** (favorable) p50/p75/p95 = "
                          f"{s['mfe_ret_pctiles']['p50']*100:.2f}/{s['mfe_ret_pctiles']['p75']*100:.2f}/{s['mfe_ret_pctiles']['p95']*100:.2f}%")
            best = max(s["entry_scan"], key=lambda x: x["net_mean_ret"])
            report.append(f"- **Entry-time scan** best entry ≈ +{best['entry_off']}m "
                          f"(net {best['net_mean_ret']*100:+.3f}%/day, win {best['win_rate']*100:.1f}%)")
            if oos:
                report.append(f"- **OOS 2026** ({oos['days']}d): net mean {oos['net_mean_ret']*100:+.3f}%/day, "
                              f"win {oos['win_rate']*100:.1f}%, total ₹{oos['net_total_pnl']:+,.0f}  "
                              f"→ edge {'HOLDS' if np.sign(oos['net_mean_ret'])==np.sign(b['net_mean_ret']) and abs(b['net_mean_ret'])>1e-5 else 'does NOT hold / no edge'}")
            report.append("")
    (OUT / "report.md").write_text("\n".join(report), encoding="utf-8")
    print("\n".join(report))
    print(f"[written: {OUT}]")


if __name__ == "__main__":
    main()
