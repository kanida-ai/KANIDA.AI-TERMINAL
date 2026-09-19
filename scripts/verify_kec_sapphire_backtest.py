"""
Independent KANIDA backtest verifier for KEC and SAPPHIRE.

This is the saved version of the one-off verifier used to compute the
read-only numbers reported from:
  - reports/verify_KEC_patterns.csv
  - reports/verify_SAPPHIRE_patterns.csv
  - db/kanida.db

It opens SQLite in read-only/query-only mode and prints results to stdout.
It does not write reports, caches, or database rows.
"""

from __future__ import annotations

import math
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning
import warnings


warnings.filterwarnings("ignore", category=PerformanceWarning)

ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
DB = ROOT / "db" / "kanida.db"
REPORTS = ROOT / "reports"

START = pd.Timestamp("2025-01-01")
END = pd.Timestamp("2026-07-31")

TGT = {
    "dn_1pct_1d": (1, 1),
    "dn_2pct_2d": (2, 2),
    "dn_5pct_5d": (5, 5),
}

TRAIL = {
    "KEC": (10.0, 4.0, 6.0, 3.0),
    "SAPPHIRE": (5.0, 2.0, 3.0, 1.5),
}

LEV = 5.0
COST_SLIP = 0.80
COND_RE = re.compile(r"^\s*([A-Za-z0-9_]+)\s*(<=|>=|<|>)\s*(-?(?:\d+(?:\.\d*)?|\.\d+))\s*$")


def ro_connect() -> sqlite3.Connection:
    uri = "file:" + DB.as_posix() + "?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.execute("PRAGMA query_only=ON")
    return con


def parse_conditions(text: str) -> list[tuple[str, str, float]]:
    out: list[tuple[str, str, float]] = []
    for part in str(text).split(" AND "):
        m = COND_RE.match(part)
        if not m:
            raise ValueError(f"cannot parse condition: {part}")
        out.append((m.group(1), m.group(2), float(m.group(3))))
    return out


def mask_for(frame: pd.DataFrame, conds: list[tuple[str, str, float]]) -> np.ndarray:
    mask = np.ones(len(frame), dtype=bool)
    for feat, op, threshold in conds:
        if feat not in frame.columns:
            raise KeyError(f"missing feature {feat}")
        x = frame[feat].to_numpy(dtype=float)
        if op == ">":
            cm = x > threshold
        elif op == "<":
            cm = x < threshold
        elif op == ">=":
            cm = x >= threshold
        else:
            cm = x <= threshold
        mask &= cm & np.isfinite(x)
    return mask


def load_market(con: sqlite3.Connection) -> pd.Series:
    market = pd.read_sql_query(
        "SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50' ORDER BY bar_time",
        con,
    )
    market["date"] = pd.to_datetime(market["bar_time"].str[:10])
    return market.groupby("date", sort=True)["close"].last().astype(float)


def build_frame(
    con: sqlite3.Connection,
    symbol: str,
    market_close: pd.Series,
    max_t: int = 5,
) -> tuple[pd.DataFrame, dict[str, dict[str, np.ndarray]]]:
    df = pd.read_sql_query(
        """
        SELECT bar_time,open,high,low,close,volume
        FROM ohlc_1min
        WHERE symbol=?
        ORDER BY bar_time
        """,
        con,
        params=(symbol,),
    )
    df["date"] = df["bar_time"].str[:10]
    df["hm"] = df["bar_time"].str[11:16]
    df = df[(df["hm"] >= "09:15") & (df["hm"] <= "15:29")].copy()

    vol_bl = None
    rng_bl = None
    rows: list[dict[str, float | pd.Timestamp]] = []
    day_data: dict[str, dict[str, np.ndarray]] = {}

    for day, g in df.groupby("date", sort=True):
        hm = g["hm"].to_numpy(str)
        o = g["open"].to_numpy(float)
        h = g["high"].to_numpy(float)
        l = g["low"].to_numpy(float)
        c = g["close"].to_numpy(float)
        v = g["volume"].to_numpy(float)
        if len(o) == 0:
            continue

        day_data[day] = {"hm": hm, "o": o, "h": h, "l": l, "c": c}
        day_open = float(o[0])
        day_high = float(np.nanmax(h))
        day_low = float(np.nanmin(l))
        day_close = float(c[-1])
        day_vol = float(np.nansum(v))

        n_hivol = n_iceberg = n_absorb = 0
        up_vol = dn_vol = 0.0
        max_vol_z = 0.0
        atp_cross = 0
        prev_side = 0
        cvd = 0.0
        fast = slow = 0.0
        vc_init = False
        prev_c = None
        run_hi = -np.inf
        deepest = 0.0
        cum_tv = cum_v = 0.0
        buckets: dict[int, float] = defaultdict(float)
        poc_ref = None
        tick = None

        for bo, bh, bl, bc, bv in zip(o, h, l, c, v):
            if not np.isfinite(bo + bh + bl + bc) or not np.isfinite(bv):
                continue

            typ = (bh + bl + bc) / 3.0
            rng_pct = (bh - bl) / bc * 100.0 if bc > 0 else 0.0
            if vol_bl is None:
                vol_bl = bv if bv > 0 else 1.0
                rng_bl = rng_pct if rng_pct > 0 else 0.1

            vol_base = vol_bl if vol_bl and vol_bl > 0 else 1.0
            rng_base = rng_bl if rng_bl and rng_bl > 0 else 0.1
            vol_z = bv / vol_base if vol_base > 0 else 0.0
            max_vol_z = max(max_vol_z, vol_z)

            hivol = bv >= 2.0 * vol_base
            if hivol:
                n_hivol += 1
                if rng_pct <= 0.5 * rng_base:
                    n_iceberg += 1
                close_loc = (bc - bl) / (bh - bl) if bh > bl else 0.5
                if bc < bo and close_loc >= 0.5:
                    n_absorb += 1

            if bc >= bo:
                up_vol += bv
            else:
                dn_vol += bv

            cum_tv += typ * bv
            cum_v += bv
            atp = cum_tv / cum_v if cum_v > 0 else typ
            side = 1 if bc >= atp else -1
            if prev_side != 0 and side != prev_side:
                atp_cross += 1
            prev_side = side

            close_loc = (bc - bl) / (bh - bl) if bh > bl else 0.5
            cvd += bv * (2.0 * close_loc - 1.0)

            if prev_c is not None and prev_c > 0:
                r2 = (bc / prev_c - 1.0) ** 2
                if not vc_init:
                    fast = slow = r2
                    vc_init = True
                else:
                    fast = (1.0 / 30.0) * r2 + (29.0 / 30.0) * fast
                    slow = (1.0 / 180.0) * r2 + (179.0 / 180.0) * slow
            prev_c = bc

            if bh > run_hi:
                run_hi = bh
            if run_hi > 0:
                dd = (bc - run_hi) / run_hi * 100.0
                deepest = min(deepest, dd)

            if poc_ref is None:
                poc_ref = typ
                tick = max(typ * 0.0005, 0.01)
            bucket = int((typ - poc_ref) / tick) if tick and tick > 0 else 0
            buckets[bucket] += bv

            vol_bl = 0.02 * bv + 0.98 * vol_bl
            rng_bl = 0.02 * rng_pct + 0.98 * rng_bl

        total_side_vol = up_vol + dn_vol
        poc_bucket = max(buckets.items(), key=lambda kv: kv[1])[0] if buckets else 0
        poc = (poc_ref + poc_bucket * tick) if (poc_ref is not None and tick) else day_close

        rows.append(
            {
                "date": pd.Timestamp(day),
                "o": day_open,
                "h": day_high,
                "l": day_low,
                "c": day_close,
                "v": day_vol,
                "eod_ret": (day_close - day_open) / day_open * 100.0 if day_open > 0 else np.nan,
                "eod_range_pct": (day_high - day_low) / day_close * 100.0 if day_close > 0 else np.nan,
                "eod_n_hivol": float(n_hivol),
                "eod_n_iceberg": float(n_iceberg),
                "eod_n_absorb": float(n_absorb),
                "eod_buy_sell_imbalance": (
                    (up_vol - dn_vol) / total_side_vol if total_side_vol > 0 else 0.0
                ),
                "eod_max_vol_z": float(max_vol_z),
                "eod_atp_crossings": float(atp_cross),
                "eod_cvd_norm": cvd / day_vol if day_vol > 0 else 0.0,
                "eod_vol_compression": fast / slow if slow > 0 else 1.0,
                "eod_deepest_pullback": float(deepest),
                "eod_dist_poc": (day_close / poc - 1.0) * 100.0 if poc and poc > 0 else np.nan,
            }
        )

    daily = pd.DataFrame(rows).set_index("date").sort_index()
    frame = add_daily_features(daily, market_close)
    base_cols = list(frame.columns)

    lagged = {}
    for n in range(1, max_t + 1):
        for col in base_cols:
            lagged[f"{col}_T{n}"] = frame[col].shift(n)

    frame = pd.concat([frame, pd.DataFrame(lagged, index=frame.index)], axis=1)
    frame["_o"] = daily["o"]
    frame["_h"] = daily["h"]
    frame["_l"] = daily["l"]
    frame["_c"] = daily["c"]
    frame["_v"] = daily["v"]
    frame["year"] = frame.index.year
    return frame, day_data


def add_daily_features(daily: pd.DataFrame, market_close: pd.Series) -> pd.DataFrame:
    h = daily["h"]
    l = daily["l"]
    c = daily["c"]
    v = daily["v"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)

    frame = pd.DataFrame(index=daily.index)
    frame["m_roc_5"] = (c / c.shift(5) - 1.0) * 100.0
    frame["m_roc_20"] = (c / c.shift(20) - 1.0) * 100.0
    frame["m_atr20_pct"] = tr.rolling(20).mean() / c * 100.0
    frame["m_dist_high_20"] = (c / h.rolling(20).max() - 1.0) * 100.0
    frame["m_dist_high_60"] = (c / h.rolling(60).max() - 1.0) * 100.0

    sma20 = c.rolling(20).mean()
    frame["m_dist_sma_20"] = (c / sma20 - 1.0) * 100.0
    frame["m_slope_sma20"] = (sma20 / sma20.shift(5) - 1.0) * 100.0

    delta = c.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    frame["m_rsi14"] = 100.0 - 100.0 / (1.0 + gain / loss)

    frame["m_vol_vs_20"] = v / v.rolling(20).mean()
    mfm = ((c - l) - (h - c)) / (h - l).replace(0, np.nan)
    frame["m_cmf20"] = (mfm * v).rolling(20).sum() / v.rolling(20).sum()

    typ = (h + l + c) / 3.0
    raw_money_flow = typ * v
    pos = raw_money_flow.where(typ > typ.shift(1), 0.0)
    neg = raw_money_flow.where(typ < typ.shift(1), 0.0)
    frame["m_mfi14"] = 100.0 - 100.0 / (1.0 + pos.rolling(14).sum() / neg.rolling(14).sum())

    obv = (np.sign(c.diff()).fillna(0.0) * v).cumsum()
    frame["m_obv_slope20"] = (obv - obv.shift(20)) / v.rolling(20).mean()

    iso = pd.MultiIndex.from_arrays(
        [daily.index.isocalendar().year.to_numpy(), daily.index.isocalendar().week.to_numpy()]
    )
    frame["m_wtd_ret"] = (c / c.groupby(iso).transform("first") - 1.0) * 100.0

    month_key = pd.MultiIndex.from_arrays([daily.index.year, daily.index.month])
    frame["m_mtd_ret"] = (c / c.groupby(month_key).transform("first") - 1.0) * 100.0
    frame["m_wtd_range"] = (h.groupby(iso).cummax() - l.groupby(iso).cummin()) / c * 100.0

    market = market_close.reindex(c.index).ffill()
    frame["m_rs_20d"] = ((c / c.shift(20) - 1.0) - (market / market.shift(20) - 1.0)) * 100.0

    micro_cols = [col for col in daily.columns if col.startswith("eod_")]
    return pd.concat([frame, daily[micro_cols]], axis=1)


def sim_short_day(day: dict[str, np.ndarray], params: tuple[float, float, float, float]) -> tuple[float, str]:
    hm = day["hm"]
    o = day["o"]
    h = day["h"]
    l = day["l"]
    c = day["c"]

    if len(o) < 2:
        return np.nan, "no-data"

    entry = float(o[0])
    if not np.isfinite(entry) or entry <= 0:
        return np.nan, "bad-entry"

    exact = np.where(hm == "15:20")[0]
    if len(exact):
        end = int(exact[0])
    else:
        before = np.where(hm <= "15:20")[0]
        end = int(before[-1]) if len(before) else len(hm) - 1

    exit_price = float(c[end])
    reason = "square-off"
    arm, floor, giveback, hard_stop = params
    armed = False
    peak = -1e18

    def capital_profit(price: float) -> float:
        return LEV * (entry - float(price)) / entry * 100.0

    for i in range(1, end + 1):
        cap_hi = capital_profit(h[i])
        if not armed:
            if cap_hi <= -hard_stop:
                exit_price = entry * (1.0 + hard_stop / (LEV * 100.0))
                reason = "hard-stop"
                break
        else:
            threshold = max(floor, peak - giveback)
            if cap_hi <= threshold:
                exit_price = entry * (1.0 - threshold / (LEV * 100.0))
                reason = "trail"
                break

        cap_lo = capital_profit(l[i])
        if cap_lo >= arm:
            armed = True
        peak = max(peak, cap_lo)

    gross_capital = LEV * (entry - exit_price) / entry * 100.0
    return gross_capital - COST_SLIP, reason


def load_patterns(symbol: str, pattern_file: str | None = None) -> pd.DataFrame:
    path = REPORTS / (pattern_file or f"verify_{symbol}_patterns.csv")
    patterns = pd.read_csv(path)
    patterns["conds"] = patterns["conditions(feature op threshold, AND-joined)"].map(parse_conditions)
    patterns["hold"] = patterns["target"].map(lambda target: TGT[target][1])
    return patterns


def max_lag(patterns: pd.DataFrame) -> int:
    max_t = 0
    for conds in patterns["conds"]:
        for feat, _, _ in conds:
            m = re.search(r"_T(\d+)$", feat)
            if m:
                max_t = max(max_t, int(m.group(1)))
    return max(max_t, 5)


def prepare_symbol(
    con: sqlite3.Connection,
    symbol: str,
    market: pd.Series,
    trail_params: tuple[float, float, float, float] | None = None,
    pattern_file: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[int, dict[str, float | int | bool]], pd.DataFrame]:
    patterns = load_patterns(symbol, pattern_file)
    frame, day_data = build_frame(con, symbol, market, max_t=max_lag(patterns))
    trail = trail_params or TRAIL[symbol]

    dates = frame.index.to_list()
    dstr = np.array([d.strftime("%Y-%m-%d") for d in dates])
    exec_ret = np.full(len(frame), np.nan)
    reasons = np.full(len(frame), "", dtype=object)

    for i, ds in enumerate(dstr):
        if ds in day_data:
            exec_ret[i], reasons[i] = sim_short_day(day_data[ds], trail)

    years = frame["year"].to_numpy()
    segment_masks: dict[int, np.ndarray] = {}
    pattern_masks: list[np.ndarray] = []

    for _, row in patterns.iterrows():
        mask = mask_for(frame, row["conds"])
        pattern_masks.append(mask)
        hold = int(row["hold"])
        if hold not in segment_masks:
            segment_masks[hold] = mask.copy()
        else:
            segment_masks[hold] |= mask

    patterns["train_fires"] = [
        int(np.sum(mask & (years <= 2024))) for mask in pattern_masks
    ]

    lifts: dict[int, float] = {}
    segment_info: dict[int, dict[str, float | int | bool]] = {}

    for hold, mask in sorted(segment_masks.items()):
        vals = []
        train_signal_days = 0

        for i in np.where(mask & (years <= 2024))[0]:
            idxs = list(range(i + 1, min(i + 1 + hold, len(frame))))
            if len(idxs) < hold:
                continue
            if any(years[j] > 2024 for j in idxs):
                continue
            daily_returns = exec_ret[idxs]
            daily_returns = daily_returns[np.isfinite(daily_returns)]
            if len(daily_returns) == hold:
                vals.extend(daily_returns.tolist())
                train_signal_days += 1

        lift = float(np.mean(vals)) if vals else np.nan
        lifts[hold] = lift
        segment_info[hold] = {
            "kept": bool(np.isfinite(lift) and lift > 0),
            "lift": lift,
            "train_signal_days": train_signal_days,
            "train_daily_obs": len(vals),
            "report_signal_days_raw": int(np.sum(mask & (frame.index >= START) & (frame.index <= END))),
        }

    kept = {hold: info for hold, info in segment_info.items() if info["kept"]}
    trades = []
    report_idxs = np.where((frame.index >= START) & (frame.index <= END))[0]
    pos = 0
    campaign_id = 0

    while pos < len(report_idxs):
        i = int(report_idxs[pos])
        firing = [hold for hold in kept if segment_masks[hold][i]]

        if not firing:
            pos += 1
            continue

        chosen = max(firing, key=lambda hold: (lifts[hold], -hold))
        campaign_id += 1

        for x in range(i + 1, min(i + 1 + chosen, len(frame))):
            if frame.index[x] < START or frame.index[x] > END:
                continue
            ret = exec_ret[x]
            if np.isfinite(ret):
                trades.append(
                    {
                        "date": frame.index[x],
                        "return": float(ret),
                        "signal_date": frame.index[i],
                        "hold": chosen,
                        "lift": lifts[chosen],
                        "reason": reasons[x],
                        "campaign_id": campaign_id,
                    }
                )

        pos += chosen

    return frame, patterns, segment_info, pd.DataFrame(trades)


def dd_fixed(values: np.ndarray) -> float:
    curve = np.concatenate([[0.0], np.cumsum(values)])
    peak = np.maximum.accumulate(curve)
    return float(np.max(peak - curve)) if len(curve) else 0.0


def dd_account(values: np.ndarray) -> float:
    cumulative = np.concatenate([[0.0], np.cumsum(values)])
    equity = 100000.0 * (1.0 + cumulative / 100.0)
    peak = np.maximum.accumulate(equity)
    dd = np.where(peak > 0, (peak - equity) / peak * 100.0, 0.0)
    return float(np.max(dd)) if len(dd) else 0.0


def profit_factor(values: np.ndarray) -> float:
    wins = values[values > 0].sum()
    losses = values[values <= 0].sum()
    if losses == 0:
        return math.inf if wins > 0 else 0.0
    return float(wins / abs(losses))


def metrics(trades: pd.DataFrame) -> dict[str, float | int]:
    if trades.empty:
        return {
            "n": 0,
            "total": 0.0,
            "ret2025": 0.0,
            "ret2026": 0.0,
            "acct_mdd": 0.0,
            "fixed_dd": 0.0,
            "calmar": 0.0,
            "win_rate": 0.0,
            "pf": 0.0,
        }

    returns = trades["return"].to_numpy(float)
    years = trades["date"].dt.year
    acct_mdd = dd_account(returns)

    return {
        "n": int(len(returns)),
        "total": float(returns.sum()),
        "ret2025": float(trades.loc[years == 2025, "return"].sum()),
        "ret2026": float(trades.loc[years == 2026, "return"].sum()),
        "acct_mdd": acct_mdd,
        "fixed_dd": dd_fixed(returns),
        "calmar": float(returns.sum() / acct_mdd) if acct_mdd > 0 else math.inf,
        "win_rate": float((returns > 0).mean() * 100.0),
        "pf": profit_factor(returns),
    }


def monthly(trades: pd.DataFrame) -> list[dict[str, float | int | str]]:
    if trades.empty:
        return []

    out = []
    t = trades.copy()
    t["month"] = t["date"].dt.strftime("%Y-%m")

    for month, group in t.groupby("month", sort=True):
        returns = group["return"].to_numpy(float)
        out.append(
            {
                "month": month,
                "days": int(len(returns)),
                "win_rate": float((returns > 0).mean() * 100.0),
                "return": float(returns.sum()),
                "worst_dd": dd_fixed(returns),
            }
        )
    return out


def fnum(value: float | int, ndigits: int = 2) -> str:
    if isinstance(value, float) and math.isinf(value):
        return "inf"
    return f"{value:.{ndigits}f}"


def main() -> None:
    with ro_connect() as con:
        market = load_market(con)
        results = {}
        for symbol in ["KEC", "SAPPHIRE"]:
            results[symbol] = (*prepare_symbol(con, symbol, market),)

    print("READ_ONLY_BACKTEST_COMPLETE")
    print(
        "Assumptions: signal on D enters next w trading days; kept segment lift is "
        "unique segment-fired daily mean through 2024 only; DDs below are positive magnitudes."
    )

    for symbol, (frame, _patterns, segment_info, trades) in results.items():
        met = metrics(trades)
        months = monthly(trades)

        report_start = trades["date"].min().date() if not trades.empty else "none"
        report_end = trades["date"].max().date() if not trades.empty else "none"

        print("\n" + symbol)
        print(
            "data_days",
            frame.index.min().date(),
            frame.index.max().date(),
            "report_trade_span",
            report_start,
            report_end,
        )
        print("segments hold kept lift train_signal_days train_daily_obs raw_report_signal_days")
        for hold, info in sorted(segment_info.items()):
            print(
                hold,
                int(info["kept"]),
                fnum(float(info["lift"])),
                info["train_signal_days"],
                info["train_daily_obs"],
                info["report_signal_days_raw"],
            )

        print(
            "overall total_pct ret2025_pct ret2026_pct account_mdd_pct "
            "fixed_cap_dd_pp calmar win_rate_pct trading_days profit_factor"
        )
        print(
            fnum(float(met["total"])),
            fnum(float(met["ret2025"])),
            fnum(float(met["ret2026"])),
            fnum(float(met["acct_mdd"])),
            fnum(float(met["fixed_dd"])),
            fnum(float(met["calmar"])),
            fnum(float(met["win_rate"])),
            met["n"],
            fnum(float(met["pf"])),
        )

        if not trades.empty:
            print("exit_reasons", trades["reason"].value_counts().to_dict())

        print("monthly month days win_rate_pct return_pct worst_intramonth_dd_pp")
        for row in months:
            print(
                row["month"],
                row["days"],
                fnum(float(row["win_rate"])),
                fnum(float(row["return"])),
                fnum(float(row["worst_dd"])),
            )


if __name__ == "__main__":
    main()
