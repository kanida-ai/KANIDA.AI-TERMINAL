"""
KANIDA - Intraday Accumulation Causation Engine
================================================
Read-only causation study for the Intraday Accumulation Intelligence layer.

It tests whether prior intraday accumulation behavior adds predictive lift for
next-day and multi-day upside beyond simple correlation.

Input DB:
  C:/Users/SPS/Desktop/Kanida Intraday Lab/data/intraday.db

Outputs:
  outputs/intraday_causation/causation_stock_days.csv
  outputs/intraday_causation/causation_base_rates.csv
  outputs/intraday_causation/causation_dose_response.csv
  outputs/intraday_causation/causation_forward_returns.csv
  outputs/intraday_causation/causation_granger.csv
  outputs/intraday_causation/causation_report.txt

Run:
  python scripts/intraday_accumulation_causation_engine.py
"""
from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(r"C:\Users\SPS\Desktop\Kanida Intraday Lab\data\intraday.db")
OUT_DIR = ROOT / "outputs" / "intraday_causation"

SESSION_START = "09:15"
SESSION_END = "15:30"
ACCUM_START = "14:00"
ACCUM_END = "15:30"


@dataclass
class TestResult:
    metric: str
    treated_n: int
    control_n: int
    treated_mean: float
    control_mean: float
    diff: float
    ci_low: float
    ci_high: float
    t_stat: float
    p_value: float


def load_intraday() -> pd.DataFrame:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Missing DB: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as con:
        df = pd.read_sql_query(
            """
            SELECT ticker, ts, open, high, low, close, volume
            FROM ohlc_1min
            WHERE market='NSE'
            ORDER BY ticker, ts
            """,
            con,
        )
    df["dt"] = pd.to_datetime(df["ts"])
    df["date"] = df["dt"].dt.strftime("%Y-%m-%d")
    df["time"] = df["dt"].dt.strftime("%H:%M")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[(df["time"] >= SESSION_START) & (df["time"] <= SESSION_END) & (df["close"] > 0)].copy()


def build_daily(df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df.groupby(["ticker", "date"])
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            bars=("close", "size"),
        )
        .reset_index()
        .sort_values(["ticker", "date"])
    )

    g = daily.groupby("ticker", group_keys=False)
    daily["prev_close"] = g["close"].shift(1)
    daily["ret_1d_prev"] = g["close"].pct_change(1) * 100
    daily["ret_3d_prev"] = g["close"].pct_change(3) * 100
    daily["range_pct_day"] = (daily["high"] - daily["low"]) / daily["close"] * 100
    daily["vol20_daily"] = g["volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    daily["daily_vol_ratio"] = daily["volume"] / daily["vol20_daily"]

    for n in range(1, 6):
        daily[f"close_d{n}"] = g["close"].shift(-n)
        daily[f"ret_d{n}_pct"] = (daily[f"close_d{n}"] / daily["close"] - 1) * 100

    daily["next_open"] = g["open"].shift(-1)
    daily["next_high"] = g["high"].shift(-1)
    daily["gap_d1_pct"] = (daily["next_open"] / daily["close"] - 1) * 100
    daily["next_high_d1_pct"] = (daily["next_high"] / daily["close"] - 1) * 100

    first = []
    for (ticker, day), bars in df.groupby(["ticker", "date"], sort=False):
        bars = bars.sort_values("time")
        open_ = float(bars.iloc[0]["open"])
        f15 = bars[bars["time"] <= "09:30"]
        f60 = bars[bars["time"] <= "10:15"]
        first.append(
            {
                "ticker": ticker,
                "date": day,
                "first15_same_pct": (float(f15.iloc[-1]["close"]) / open_ - 1) * 100 if not f15.empty else np.nan,
                "first60_same_pct": (float(f60.iloc[-1]["close"]) / open_ - 1) * 100 if not f60.empty else np.nan,
            }
        )
    daily = daily.merge(pd.DataFrame(first), on=["ticker", "date"], how="left")
    daily["first15_d1_pct"] = daily.groupby("ticker")["first15_same_pct"].shift(-1)
    daily["first60_d1_pct"] = daily.groupby("ticker")["first60_same_pct"].shift(-1)
    return daily


def build_ias_features(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for ticker, bars in df.groupby("ticker", sort=False):
        pivot = bars.pivot_table(index="date", columns="time", values="volume", aggfunc="sum").sort_index()
        expected = pivot.shift(1).rolling(20, min_periods=5).mean()

        for day, day_bars in bars.groupby("date", sort=False):
            win = day_bars[(day_bars["time"] >= ACCUM_START) & (day_bars["time"] <= ACCUM_END)].sort_values("time")
            if len(win) < 60 or day not in expected.index:
                continue

            exp_row = expected.loc[day]
            exp_vol = float(exp_row.reindex(list(win["time"])).fillna(0).sum())
            if exp_vol <= 0:
                continue

            vol_ratio = float(win["volume"].sum()) / exp_vol
            last30 = win.tail(30)
            exp_last30 = float(exp_row.reindex(list(last30["time"])).fillna(0).sum())
            last30_ratio = float(last30["volume"].sum()) / exp_last30 if exp_last30 > 0 else 0.0

            half = len(win) // 2
            vol_accel = float(win.iloc[half:]["volume"].sum()) / max(float(win.iloc[:half]["volume"].sum()), 1.0)

            first_open = float(win.iloc[0]["open"])
            last_close = float(win.iloc[-1]["close"])
            high = float(win["high"].max())
            low = float(win["low"].min())
            range_pct = (high - low) / last_close * 100 if last_close else np.nan
            net_pct = (last_close / first_open - 1) * 100 if first_open else np.nan

            bodies = (win["close"] - win["open"]).abs()
            ranges = (win["high"] - win["low"]).replace(0, np.nan)
            body_comp = float((bodies / ranges).dropna().mean()) if not (bodies / ranges).dropna().empty else 1.0

            vwap = float((win["close"] * win["volume"]).sum() / max(win["volume"].sum(), 1))
            pct_above_vwap = float((win["close"] >= vwap).mean()) if vwap else 0.0
            avg_dist_vwap_pct = float(((win["close"] - vwap).abs() / vwap * 100).mean()) if vwap else np.nan

            absorption = vol_ratio >= 1.20 and range_pct <= 1.10 and abs(net_pct) <= 0.45 and body_comp <= 0.70
            compressed = range_pct <= 0.90 and abs(net_pct) <= 0.45
            vwap_support = pct_above_vwap >= 0.48 and avg_dist_vwap_pct <= 0.35

            ias = score_ias(vol_ratio, last30_ratio, vol_accel, range_pct, net_pct, body_comp, vwap_support, absorption)
            rows.append(
                {
                    "ticker": ticker,
                    "date": day,
                    "ias": ias,
                    "vol_ratio_20d": round(vol_ratio, 4),
                    "last30_vol_ratio_20d": round(last30_ratio, 4),
                    "vol_accel": round(vol_accel, 4),
                    "range90_pct": round(range_pct, 4),
                    "net90_pct": round(net_pct, 4),
                    "body_compression": round(body_comp, 4),
                    "pct_above_vwap": round(pct_above_vwap, 4),
                    "avg_dist_vwap_pct": round(avg_dist_vwap_pct, 4),
                    "absorption": int(absorption),
                    "compressed": int(compressed),
                    "vwap_support": int(vwap_support),
                }
            )
    return pd.DataFrame(rows)


def score_ias(
    vol_ratio: float,
    last30_ratio: float,
    vol_accel: float,
    range_pct: float,
    net_pct: float,
    body_comp: float,
    vwap_support: bool,
    absorption: bool,
) -> float:
    score = 0.0
    score += min(max((vol_ratio - 1.0) / 1.2, 0.0), 1.0) * 2.5
    score += min(max((last30_ratio - 1.0) / 1.3, 0.0), 1.0) * 1.5
    score += min(max((vol_accel - 1.0) / 0.8, 0.0), 1.0) * 1.0
    score += max(0.0, min(1.0, (1.25 - range_pct) / 1.25)) * 1.8
    score += max(0.0, min(1.0, (0.60 - abs(net_pct)) / 0.60)) * 1.2
    score += max(0.0, min(1.0, (0.75 - body_comp) / 0.75)) * 0.8
    score += 1.0 if vwap_support else 0.0
    score += 1.2 if absorption else 0.0
    return round(min(10.0, score), 3)


def make_stock_days(daily: pd.DataFrame, ias: pd.DataFrame) -> pd.DataFrame:
    data = daily.merge(ias, on=["ticker", "date"], how="inner")
    data = data[(data["bars"] >= 300) & data["gap_d1_pct"].notna()].copy()
    data["accumulation_signal"] = ((data["ias"] >= 6.0) & (data["absorption"] == 1) & (data["compressed"] == 1)).astype(int)
    return data


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def two_sided_p_from_t(t_stat: float) -> float:
    return max(0.0, min(1.0, 2.0 * (1.0 - normal_cdf(abs(t_stat)))))


def welch_row(data: pd.DataFrame, metric: str, test_type: str) -> dict:
    treated = data.loc[data["accumulation_signal"] == 1, metric].dropna().astype(float).to_numpy()
    control = data.loc[data["accumulation_signal"] == 0, metric].dropna().astype(float).to_numpy()
    n1, n0 = len(treated), len(control)
    m1 = float(np.mean(treated)) if n1 else np.nan
    m0 = float(np.mean(control)) if n0 else np.nan
    v1 = float(np.var(treated, ddof=1)) if n1 > 1 else 0.0
    v0 = float(np.var(control, ddof=1)) if n0 > 1 else 0.0
    se = math.sqrt(v1 / n1 + v0 / n0) if n1 and n0 else np.nan
    diff = m1 - m0
    t_stat = diff / se if se and se > 0 else 0.0
    p = two_sided_p_from_t(t_stat)
    return {
        "test_type": test_type,
        "metric": metric,
        "treated_n": n1,
        "control_n": n0,
        "treated_mean": round(m1, 6),
        "control_mean": round(m0, 6),
        "difference": round(diff, 6),
        "ci95_low": round(diff - 1.96 * se, 6) if se == se else np.nan,
        "ci95_high": round(diff + 1.96 * se, 6) if se == se else np.nan,
        "t_stat": round(t_stat, 4),
        "p_value_normal_approx": round(p, 6),
        "significant_5pct": int(p < 0.05),
    }


def base_rate_tests(data: pd.DataFrame) -> pd.DataFrame:
    tmp = data.copy()
    specs = {
        "gap_up_0_8": (tmp["gap_d1_pct"] >= 0.8).astype(int),
        "first15_up_0_75": (tmp["first15_d1_pct"] >= 0.75).astype(int),
        "first60_up_1_2": (tmp["first60_d1_pct"] >= 1.2).astype(int),
        "next_high_up_2": (tmp["next_high_d1_pct"] >= 2.0).astype(int),
        "day1_close_positive": (tmp["ret_d1_pct"] > 0).astype(int),
        "day3_close_positive": (tmp["ret_d3_pct"] > 0).astype(int),
        "day5_close_positive": (tmp["ret_d5_pct"] > 0).astype(int),
        "day3_up_2": (tmp["ret_d3_pct"] >= 2.0).astype(int),
        "day5_up_3": (tmp["ret_d5_pct"] >= 3.0).astype(int),
    }
    rows = []
    for metric, values in specs.items():
        tmp[metric] = values
        rows.append(welch_row(tmp, metric, "base_rate"))
    return pd.DataFrame(rows)


def forward_return_tests(data: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        "gap_d1_pct",
        "first15_d1_pct",
        "first60_d1_pct",
        "next_high_d1_pct",
        "ret_d1_pct",
        "ret_d2_pct",
        "ret_d3_pct",
        "ret_d4_pct",
        "ret_d5_pct",
    ]
    return pd.DataFrame([welch_row(data, metric, "forward_return") for metric in metrics])


def dose_response(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    out["ias_quintile"] = pd.qcut(out["ias"], 5, labels=["Q1_low", "Q2", "Q3", "Q4", "Q5_high"], duplicates="drop")
    rows = []
    for q, g in out.groupby("ias_quintile", observed=False):
        rows.append(
            {
                "ias_quintile": str(q),
                "n": len(g),
                "ias_min": round(float(g["ias"].min()), 3),
                "ias_max": round(float(g["ias"].max()), 3),
                "ias_mean": round(float(g["ias"].mean()), 3),
                "p_gap_up_0_8": round(float((g["gap_d1_pct"] >= 0.8).mean()), 6),
                "p_first60_up_1_2": round(float((g["first60_d1_pct"] >= 1.2).mean()), 6),
                "p_next_high_up_2": round(float((g["next_high_d1_pct"] >= 2.0).mean()), 6),
                "p_day3_positive": round(float((g["ret_d3_pct"] > 0).mean()), 6),
                "p_day5_positive": round(float((g["ret_d5_pct"] > 0).mean()), 6),
                "avg_ret_d1_pct": round(float(g["ret_d1_pct"].mean()), 6),
                "avg_ret_d3_pct": round(float(g["ret_d3_pct"].mean()), 6),
                "avg_ret_d5_pct": round(float(g["ret_d5_pct"].mean()), 6),
            }
        )
    return pd.DataFrame(rows)


def ols_sse(y: np.ndarray, x: np.ndarray) -> tuple[float, int, int]:
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    resid = y - x @ beta
    return float(np.sum(resid**2)), len(y), x.shape[1]


def granger_tests(data: pd.DataFrame) -> pd.DataFrame:
    rows = []
    targets = [
        "gap_d1_pct",
        "first60_d1_pct",
        "ret_d1_pct",
        "ret_d3_pct",
        "ret_d5_pct",
    ]
    for target in targets:
        frame = data[
            [target, "ias", "ret_1d_prev", "ret_3d_prev", "range_pct_day", "daily_vol_ratio"]
        ].replace([np.inf, -np.inf], np.nan).dropna()
        if len(frame) < 50:
            continue
        y = frame[target].to_numpy(float)
        base_x = np.column_stack(
            [
                np.ones(len(frame)),
                frame["ret_1d_prev"].to_numpy(float),
                frame["ret_3d_prev"].to_numpy(float),
                frame["range_pct_day"].to_numpy(float),
                frame["daily_vol_ratio"].to_numpy(float),
            ]
        )
        full_x = np.column_stack([base_x, frame["ias"].to_numpy(float)])
        sse_r, n, k_r = ols_sse(y, base_x)
        sse_f, _, k_f = ols_sse(y, full_x)
        q = k_f - k_r
        f_stat = ((sse_r - sse_f) / q) / (sse_f / (n - k_f)) if sse_f > 0 and n > k_f else 0.0
        p_value = two_sided_p_from_t(math.sqrt(max(0.0, f_stat)))
        r2_gain = (sse_r - sse_f) / sse_r if sse_r > 0 else 0.0
        rows.append(
            {
                "target": target,
                "n": n,
                "restricted_vars": "ret_1d_prev, ret_3d_prev, range_pct_day, daily_vol_ratio",
                "added_var": "ias",
                "f_stat": round(f_stat, 6),
                "p_value_normal_approx": round(p_value, 6),
                "r2_gain_from_ias": round(r2_gain, 8),
                "significant_5pct": int(p_value < 0.05),
            }
        )
    return pd.DataFrame(rows)


def write_report(data: pd.DataFrame, base: pd.DataFrame, dose: pd.DataFrame, forward: pd.DataFrame, granger: pd.DataFrame) -> None:
    positive_core = base[
        (base["metric"].isin(["day3_close_positive", "day5_close_positive", "day3_up_2", "day5_up_3"]))
        & (base["difference"] > 0)
        & (base["p_value_normal_approx"] < 0.05)
    ]
    significant_granger = granger[(granger["significant_5pct"] == 1) & (granger["r2_gain_from_ias"] > 0)]

    if len(positive_core) >= 2 and len(significant_granger) >= 1:
        verdict = "CAUSAL_EVIDENCE_PRESENT"
    elif len(positive_core) or len(significant_granger):
        verdict = "WEAK_CAUSAL_EVIDENCE"
    else:
        verdict = "NO_STRONG_CAUSAL_EVIDENCE_YET"

    lines = [
        "KANIDA Intraday Accumulation Causation Report",
        "=" * 58,
        "",
        f"DB: {DB_PATH}",
        f"Stock-days tested: {len(data):,}",
        f"Tickers: {data['ticker'].nunique():,}",
        f"Date range: {data['date'].min()} to {data['date'].max()}",
        f"Accumulation signal days: {int(data['accumulation_signal'].sum()):,}",
        "",
        "Interpretation:",
        "- Causation is not claimed from one test.",
        "- Evidence is stronger when base-rate lift, dose-response, multi-day returns,",
        "  and Granger-style lag tests all point in the same positive direction.",
        "- P-values use a normal approximation to keep the engine dependency-light.",
        "",
        f"Verdict: {verdict}",
        "",
        "Base Rate Tests",
        base.to_string(index=False),
        "",
        "IAS Quintile Dose Response",
        dose.to_string(index=False),
        "",
        "Forward Return Tests",
        forward.to_string(index=False),
        "",
        "Granger-Style Lag Tests",
        granger.to_string(index=False),
        "",
    ]
    (OUT_DIR / "causation_report.txt").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 70)
    print("KANIDA - Intraday Accumulation Causation Engine")
    print("=" * 70)
    print(f"Loading: {DB_PATH}")
    intraday = load_intraday()
    print(f"  Intraday rows: {len(intraday):,}")

    print("Building daily outcomes...")
    daily = build_daily(intraday)

    print("Computing IAS for every stock-day...")
    ias = build_ias_features(intraday)
    stock_days = make_stock_days(daily, ias)
    print(f"  Stock-days: {len(stock_days):,}")
    print(f"  Accumulation signals: {int(stock_days['accumulation_signal'].sum()):,}")
    print(f"  Tickers: {stock_days['ticker'].nunique():,}")

    print("Running causation tests...")
    base = base_rate_tests(stock_days)
    dose = dose_response(stock_days)
    forward = forward_return_tests(stock_days)
    granger = granger_tests(stock_days)

    stock_days.to_csv(OUT_DIR / "causation_stock_days.csv", index=False)
    base.to_csv(OUT_DIR / "causation_base_rates.csv", index=False)
    dose.to_csv(OUT_DIR / "causation_dose_response.csv", index=False)
    forward.to_csv(OUT_DIR / "causation_forward_returns.csv", index=False)
    granger.to_csv(OUT_DIR / "causation_granger.csv", index=False)
    write_report(stock_days, base, dose, forward, granger)

    print("")
    print("Base rate summary:")
    print(base.to_string(index=False))
    print("")
    print("Dose response:")
    print(dose.to_string(index=False))
    print("")
    print("Granger-style tests:")
    print(granger.to_string(index=False))
    print("")
    print(f"Wrote outputs to: {OUT_DIR}")


if __name__ == "__main__":
    main()
