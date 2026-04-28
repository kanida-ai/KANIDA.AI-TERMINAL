"""
Data freshness service.
Answers: "Is the data up to date?" for both OHLCV and signals.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).parent
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)

ROOT = _HERE.parent.parent

PIPELINE_LOGS = {
    "ohlcv_fetch":        ROOT / "outputs" / "scheduler_1_fetch.log",
    "pattern_learning":   ROOT / "outputs" / "scheduler_2_learning.log",
    "backtest":           ROOT / "outputs" / "scheduler_3_backtest.log",
    "execution_analysis": ROOT / "outputs" / "scheduler_4_execution.log",
}

# NSE trading calendar: Mon–Fri (approximate — no holiday list)
def _last_trading_day() -> date:
    d = date.today()
    # If today is a weekend, step back to Friday
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def get_freshness() -> dict:
    last_trading = _last_trading_day()
    result: dict = {"last_trading_day": str(last_trading)}

    with _conn() as conn:
        # Latest OHLCV date per market
        ohlcv_rows = conn.execute(
            "SELECT market, MAX(trade_date) as latest FROM ohlc_daily GROUP BY market"
        ).fetchall()
        ohlcv = {r["market"]: r["latest"] for r in ohlcv_rows}

        # Stale tickers: in roster but no data for latest date
        nse_latest = ohlcv.get("NSE")
        nse_total  = conn.execute(
            "SELECT COUNT(DISTINCT ticker) FROM ohlc_daily WHERE market='NSE'"
        ).fetchone()[0]
        nse_fresh  = conn.execute(
            "SELECT COUNT(DISTINCT ticker) FROM ohlc_daily WHERE market='NSE' AND trade_date=?",
            (nse_latest,),
        ).fetchone()[0] if nse_latest else 0

        # Latest snapshot run
        snap = conn.execute(
            "SELECT run_date, signal_count FROM snapshot_runs ORDER BY run_date DESC LIMIT 1"
        ).fetchone()

        # Live opportunities count and created_at
        opp = conn.execute(
            """
            SELECT COUNT(*) as cnt, MAX(created_at) as latest
            FROM live_opportunities lo
            JOIN snapshot_runs sr ON sr.id = lo.snapshot_run_id
            WHERE sr.run_date = (SELECT MAX(run_date) FROM snapshot_runs)
            """
        ).fetchone()

    result["ohlcv"] = {
        "latest_date":  ohlcv,
        "nse_total_tickers":  nse_total,
        "nse_fresh_tickers":  nse_fresh,
        "nse_stale_tickers":  nse_total - nse_fresh,
        "is_fresh":     nse_latest == str(last_trading) if nse_latest else False,
    }

    result["signals"] = {
        "latest_snapshot_date": snap["run_date"] if snap else None,
        "signal_count":         snap["signal_count"] if snap else 0,
        "live_opportunities":   opp["cnt"] if opp else 0,
        "last_generated":       opp["latest"] if opp else None,
        "is_fresh": (snap["run_date"] == str(last_trading)) if snap else False,
    }

    # Pipeline log file status
    logs = {}
    for step, path in PIPELINE_LOGS.items():
        if path.exists():
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            last_line = ""
            try:
                with open(path, encoding="utf-8", errors="replace") as f:
                    lines = f.read().splitlines()
                    last_line = lines[-1].strip() if lines else ""
            except Exception:
                pass
            logs[step] = {
                "last_run":  mtime.strftime("%Y-%m-%d %H:%M:%S"),
                "last_line": last_line,
            }
        else:
            logs[step] = {"last_run": None, "last_line": None}

    result["pipeline_logs"] = logs
    result["overall_healthy"] = result["ohlcv"]["is_fresh"] and result["signals"]["is_fresh"]

    return result
