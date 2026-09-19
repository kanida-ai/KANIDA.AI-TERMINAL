"""
Cohort verifier for the KANIDA sellable-worker backtest.

Inputs:
  - reports/verify_worker_config.csv
  - reports/verify_*_patterns.csv named by the config
  - db/kanida.db opened read-only

Outputs:
  - reports/verify_results_ALL.csv
  - reports/verify_monthly_ALL.csv

By default this script refuses to overwrite existing output files. Pass
--overwrite if you intentionally want to regenerate them.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd

from verify_kec_sapphire_backtest import (
    REPORTS,
    dd_fixed,
    load_market,
    metrics,
    monthly,
    prepare_symbol,
    ro_connect,
)


RESULT_COLUMNS = [
    "symbol",
    "total_return",
    "ret2025",
    "ret2026",
    "account_mdd",
    "fixed_capital_dd",
    "calmar",
    "win_rate",
    "trading_days",
    "profit_factor",
]

MONTHLY_COLUMNS = [
    "symbol",
    "month",
    "trading_days",
    "win_rate",
    "monthly_return",
    "worst_intramonth_dd",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite verify_results_ALL.csv and verify_monthly_ALL.csv if they already exist",
    )
    return parser.parse_args()


def finite_or_inf(value: float | int) -> float:
    if isinstance(value, float) and math.isinf(value):
        return float("inf")
    return float(value)


def load_config() -> pd.DataFrame:
    cfg_path = REPORTS / "verify_worker_config.csv"
    cfg = pd.read_csv(cfg_path)
    required = {
        "symbol",
        "is_fno",
        "arm",
        "floor",
        "giveback",
        "hard_stop",
        "pattern_file",
    }
    missing = sorted(required.difference(cfg.columns))
    if missing:
        raise ValueError(f"missing config columns: {missing}")
    bad = cfg[cfg["is_fno"].astype(int) != 0]
    if not bad.empty:
        names = ", ".join(bad["symbol"].astype(str))
        raise ValueError(f"config contains F&O workers, which are out of scope: {names}")
    return cfg


def refuse_overwrite(paths: list[Path], overwrite: bool) -> None:
    existing = [p for p in paths if p.exists()]
    if existing and not overwrite:
        names = ", ".join(str(p) for p in existing)
        raise FileExistsError(f"refusing to overwrite existing output file(s): {names}")


def main() -> None:
    args = parse_args()
    result_path = REPORTS / "verify_results_ALL.csv"
    monthly_path = REPORTS / "verify_monthly_ALL.csv"
    refuse_overwrite([result_path, monthly_path], args.overwrite)

    cfg = load_config()
    summary_rows = []
    monthly_rows = []

    with ro_connect() as con:
        market = load_market(con)

        for row in cfg.itertuples(index=False):
            symbol = str(row.symbol)
            trail_params = (
                float(row.arm),
                float(row.floor),
                float(row.giveback),
                float(row.hard_stop),
            )

            print(f"running {symbol} ...", flush=True)
            _frame, _patterns, _segments, trades = prepare_symbol(
                con,
                symbol,
                market,
                trail_params=trail_params,
                pattern_file=str(row.pattern_file),
            )
            met = metrics(trades)

            summary_rows.append(
                {
                    "symbol": symbol,
                    "total_return": finite_or_inf(met["total"]),
                    "ret2025": finite_or_inf(met["ret2025"]),
                    "ret2026": finite_or_inf(met["ret2026"]),
                    "account_mdd": finite_or_inf(met["acct_mdd"]),
                    "fixed_capital_dd": finite_or_inf(met["fixed_dd"]),
                    "calmar": finite_or_inf(met["calmar"]),
                    "win_rate": finite_or_inf(met["win_rate"]),
                    "trading_days": int(met["n"]),
                    "profit_factor": finite_or_inf(met["pf"]),
                }
            )

            for item in monthly(trades):
                monthly_rows.append(
                    {
                        "symbol": symbol,
                        "month": item["month"],
                        "trading_days": int(item["days"]),
                        "win_rate": finite_or_inf(item["win_rate"]),
                        "monthly_return": finite_or_inf(item["return"]),
                        "worst_intramonth_dd": finite_or_inf(item["worst_dd"]),
                    }
                )

            # Keep a tiny progress checksum visible during long cohort runs.
            if not trades.empty:
                total = trades["return"].sum()
                dd = dd_fixed(trades["return"].to_numpy(float))
                print(f"  {symbol}: days={len(trades)} total={total:.2f}% fixed_dd={dd:.2f}", flush=True)
            else:
                print(f"  {symbol}: no report-window trades", flush=True)

    results = pd.DataFrame(summary_rows, columns=RESULT_COLUMNS)
    monthly_df = pd.DataFrame(monthly_rows, columns=MONTHLY_COLUMNS)
    results.to_csv(result_path, index=False)
    monthly_df.to_csv(monthly_path, index=False)

    print(f"written {result_path}")
    print(f"written {monthly_path}")
    print("\nSUMMARY")
    print(results.to_string(index=False, formatters={col: "{:.2f}".format for col in RESULT_COLUMNS if col not in {"symbol", "trading_days"}}))


if __name__ == "__main__":
    main()
