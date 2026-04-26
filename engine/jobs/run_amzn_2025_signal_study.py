from __future__ import annotations

import csv
import json
import math
import sqlite3
import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any

from engine.config import load_config
from engine.outcome_first.features import build_behavior_rows


TICKER = "AMZN"
MARKET = "US"
DEFAULT_YEARS = ["2024", "2025", "2026"]
TARGET_PCT = 0.10
STOP_PCT = 0.05
MAX_HOLD_BARS = 20
POST_EVENT_BARS = 10


@dataclass
class TradeResult:
    exit_date: str
    exit_price: float
    exit_reason: str
    pnl_pct: float
    mfe_pct: float
    mae_pct: float
    bars_held: int
    target_hit_date: str
    stop_hit_date: str
    post_target_behavior: str
    post_stop_behavior: str
    missed_profit_index: float


def main() -> None:
    parser = argparse.ArgumentParser(description="AMZN multi-year signal performance study")
    parser.add_argument("--years", nargs="+", default=DEFAULT_YEARS, help="Years to analyze, e.g. 2024 2025 2026")
    args = parser.parse_args()

    config = load_config()
    root_out = config["outputs_path"] / "reports" / "amzn_signal_study"
    root_out.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(config["signals_db_path"])
    conn.row_factory = sqlite3.Row
    ohlc = load_ohlc(conn)
    fitness = load_fitness(conn)
    roster = load_roster(conn)
    behavior_by_date = {
        row["trade_date"]: row
        for row in build_behavior_rows(ohlc)
    }
    date_index = {row["trade_date"]: i for i, row in enumerate(ohlc)}

    combined_rows: list[dict[str, Any]] = []
    combined_overall = []
    for year in args.years:
        out_dir = root_out / year
        out_dir.mkdir(parents=True, exist_ok=True)
        events = load_events(conn, year)
        rows = run_year_study(year, events, ohlc, date_index, behavior_by_date, fitness, roster, out_dir)
        combined_rows.extend(rows)
        overall = overall_summary(rows)
        overall["year"] = year
        combined_overall.append(overall)

    conn.close()

    if combined_rows:
        combined_dir = root_out / "combined"
        combined_dir.mkdir(parents=True, exist_ok=True)
        for row in combined_rows:
            row["entry_month"] = row["entry_date"][:7]
        combined_summaries = {
            "overall": overall_summary(combined_rows),
            "by_year": combined_overall,
            "by_strategy": summarize_group(combined_rows, ["strategy_name", "bias", "timeframe_used"]),
            "by_pattern_atom_signal": summarize_group(combined_rows, ["behavior_pattern", "flow", "volatility", "ma_position", "breakout_state", "strategy_name"]),
            "by_timeframe": summarize_group(combined_rows, ["timeframe_used"]),
            "by_month": summarize_group(combined_rows, ["entry_month"]),
            "by_market_condition": summarize_group(combined_rows, ["engine_trend_state", "volatility", "ma_position"]),
            "by_quality_bucket": summarize_group(combined_rows, ["quality_bucket"]),
            "by_signal_type": summarize_group(combined_rows, ["signal_type"]),
        }
        write_csv(combined_dir / "amzn_all_years_complete_trade_log.csv", combined_rows)
        write_csv(combined_dir / "summary_by_year.csv", combined_overall)
        write_csv(combined_dir / "summary_by_strategy.csv", combined_summaries["by_strategy"])
        write_csv(combined_dir / "summary_by_pattern_atom_signal.csv", combined_summaries["by_pattern_atom_signal"])
        write_csv(combined_dir / "summary_by_timeframe.csv", combined_summaries["by_timeframe"])
        write_csv(combined_dir / "summary_by_month.csv", combined_summaries["by_month"])
        write_csv(combined_dir / "summary_by_market_condition.csv", combined_summaries["by_market_condition"])
        write_csv(combined_dir / "summary_by_quality_bucket.csv", combined_summaries["by_quality_bucket"])
        write_csv(combined_dir / "summary_by_signal_type.csv", combined_summaries["by_signal_type"])
        (combined_dir / "AMZN_MULTI_YEAR_SIGNAL_PERFORMANCE_STUDY.md").write_text(
            build_combined_markdown_report(combined_rows, combined_summaries),
            encoding="utf-8",
        )
        print(f"Combined AMZN study complete: {combined_dir}")
        print(json.dumps(combined_overall, indent=2))


def run_year_study(
    year: str,
    events: list[dict[str, Any]],
    ohlc: list[dict[str, Any]],
    date_index: dict[str, int],
    behavior_by_date: dict[str, dict[str, Any]],
    fitness: dict[tuple[str, str, str], dict[str, Any]],
    roster: dict[tuple[str, str, str], dict[str, Any]],
    out_dir: Path,
) -> list[dict[str, Any]]:
    clusters = same_day_clusters(events)

    trade_rows: list[dict[str, Any]] = []
    for idx, event in enumerate(events, 1):
        side = trade_side(event["bias"])
        behavior = behavior_by_date.get(event["signal_date"], {})
        same_cluster = clusters[(event["signal_date"], event["timeframe"], event["bias"])]
        fit = fitness.get((event["timeframe"], event["strategy_name"], event["bias"]), {})
        ros = roster.get((event["timeframe"], event["strategy_name"], event["bias"]), {})
        result = simulate_trade(event, side, ohlc, date_index)
        signal_type = classify_signal_type(event["strategy_name"], behavior)
        quality = quality_bucket(result, fit, side)
        strength = signal_strength_score(result, fit, quality)
        trade_rows.append(
            {
                "trade_id": f"AMZN-{year}-{idx:04d}",
                "year": year,
                "stock": TICKER,
                "market": MARKET,
                "entry_date": event["signal_date"],
                "entry_time": event.get("detected_at") or "daily_close_proxy",
                "exit_date": result.exit_date,
                "exit_time": "daily_ohlc_proxy",
                "timeframe_used": event["timeframe"],
                "signal_type": signal_type,
                "strategy_name": event["strategy_name"],
                "bias": event["bias"],
                "trade_side": side,
                "pattern_combination": pattern_combination(behavior, event, same_cluster),
                "behavior_pattern": behavior.get("coarse_behavior", ""),
                "behavior_atoms": " + ".join(behavior.get("behavior_atoms", [])),
                "signal_cluster": " + ".join(same_cluster[:8]),
                "same_day_cluster_count": len(same_cluster),
                "entry_price": round(float(event["entry_price"]), 4),
                "stop_loss": stop_price(float(event["entry_price"]), side),
                "target": target_price(float(event["entry_price"]), side),
                "risk_reward": "1:2",
                "exit_reason": result.exit_reason,
                "pnl_pct": round(result.pnl_pct * 100, 4),
                "max_favorable_excursion_pct": round(result.mfe_pct * 100, 4),
                "max_adverse_excursion_pct": round(result.mae_pct * 100, 4),
                "bars_held": result.bars_held,
                "target_hit_date": result.target_hit_date,
                "stop_hit_date": result.stop_hit_date,
                "post_target_behavior": result.post_target_behavior,
                "post_stop_behavior": result.post_stop_behavior,
                "missed_profit_index_pct": round(result.missed_profit_index * 100, 4),
                "trend_state": event.get("trend_state") or "",
                "engine_trend_state": behavior.get("trend_20", ""),
                "flow": behavior.get("flow", ""),
                "volatility": behavior.get("volatility", ""),
                "ma_position": behavior.get("ma_position", ""),
                "breakout_state": behavior.get("breakout_state", ""),
                "roster_tier": ros.get("tier", ""),
                "fitness_score": fit.get("fitness_score", ""),
                "historical_win_rate_15d": fit.get("win_rate_15d", ""),
                "historical_avg_ret_15d": fit.get("avg_ret_15d", ""),
                "quality_bucket": quality,
                "signal_strength_score": strength,
            }
        )

    for row in trade_rows:
        row["entry_month"] = row["entry_date"][:7]

    write_csv(out_dir / f"amzn_{year}_complete_trade_log.csv", trade_rows)

    summaries = {
        "overall": overall_summary(trade_rows),
        "by_signal_type": summarize_group(trade_rows, ["signal_type"]),
        "by_strategy": summarize_group(trade_rows, ["strategy_name", "bias", "timeframe_used"]),
        "by_pattern_atom_signal": summarize_group(trade_rows, ["behavior_pattern", "flow", "volatility", "ma_position", "breakout_state", "strategy_name"]),
        "by_timeframe": summarize_group(trade_rows, ["timeframe_used"]),
        "by_month": summarize_group(trade_rows, ["entry_month"]),
        "by_market_condition": summarize_group(trade_rows, ["engine_trend_state", "volatility", "ma_position"]),
        "by_quality_bucket": summarize_group(trade_rows, ["quality_bucket"]),
    }
    write_csv(out_dir / "summary_by_signal_type.csv", summaries["by_signal_type"])
    write_csv(out_dir / "summary_by_strategy.csv", summaries["by_strategy"])
    write_csv(out_dir / "summary_by_pattern_atom_signal.csv", summaries["by_pattern_atom_signal"])
    write_csv(out_dir / "summary_by_timeframe.csv", summaries["by_timeframe"])
    write_csv(out_dir / "summary_by_month.csv", summaries["by_month"])
    write_csv(out_dir / "summary_by_market_condition.csv", summaries["by_market_condition"])
    write_csv(out_dir / "summary_by_quality_bucket.csv", summaries["by_quality_bucket"])

    report = build_markdown_report(trade_rows, summaries, year)
    (out_dir / f"AMZN_{year}_SIGNAL_PERFORMANCE_STUDY.md").write_text(report, encoding="utf-8")
    (out_dir / "metadata.json").write_text(
        json.dumps(
            {
                "ticker": TICKER,
                "market": MARKET,
                "year": year,
                "target_pct": TARGET_PCT,
                "stop_pct": STOP_PCT,
                "max_hold_bars": MAX_HOLD_BARS,
                "post_event_bars": POST_EVENT_BARS,
                "trade_rows": len(trade_rows),
                "assumption": "Daily/weekly signal study using daily OHLC. AMZN intraday 5m/15m/1H fills were not present in kanida_signals.db.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"AMZN {year} study complete: {out_dir}")
    print(json.dumps(summaries["overall"], indent=2))
    return trade_rows


def load_events(conn: sqlite3.Connection, year: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM signal_events
        WHERE market=? AND ticker=?
          AND signal_date BETWEEN ? AND ?
        ORDER BY signal_date, timeframe, bias, strategy_name, id
        """,
        (MARKET, TICKER, f"{year}-01-01", f"{year}-12-31"),
    ).fetchall()
    return [dict(r) for r in rows]


def load_ohlc(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT market, ticker, trade_date, open, high, low, close, volume
        FROM ohlc_daily
        WHERE market=? AND ticker=?
        ORDER BY trade_date
        """,
        (MARKET, TICKER),
    ).fetchall()
    return [dict(r) for r in rows]


def load_fitness(conn: sqlite3.Connection) -> dict[tuple[str, str, str], dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM stock_signal_fitness
        WHERE market=? AND ticker=?
        """,
        (MARKET, TICKER),
    ).fetchall()
    return {
        (r["timeframe"], r["strategy_name"], r["bias"]): dict(r)
        for r in rows
    }


def load_roster(conn: sqlite3.Connection) -> dict[tuple[str, str, str], dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM signal_roster
        WHERE market=? AND ticker=?
        """,
        (MARKET, TICKER),
    ).fetchall()
    return {
        (r["timeframe"], r["strategy_name"], r["bias"]): dict(r)
        for r in rows
    }


def same_day_clusters(events: list[dict[str, Any]]) -> dict[tuple[str, str, str], list[str]]:
    grouped: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for event in events:
        key = (event["signal_date"], event["timeframe"], event["bias"])
        grouped[key].append(event["strategy_name"])
    return {k: sorted(set(v)) for k, v in grouped.items()}


def trade_side(bias: str) -> str:
    if bias == "bullish":
        return "long"
    if bias == "bearish":
        return "short"
    return "neutral_observation"


def target_price(entry: float, side: str) -> float | str:
    if side == "long":
        return round(entry * (1 + TARGET_PCT), 4)
    if side == "short":
        return round(entry * (1 - TARGET_PCT), 4)
    return ""


def stop_price(entry: float, side: str) -> float | str:
    if side == "long":
        return round(entry * (1 - STOP_PCT), 4)
    if side == "short":
        return round(entry * (1 + STOP_PCT), 4)
    return ""


def simulate_trade(event: dict[str, Any], side: str, ohlc: list[dict[str, Any]], date_index: dict[str, int]) -> TradeResult:
    entry = float(event["entry_price"])
    idx = date_index.get(event["signal_date"])
    if idx is None or side == "neutral_observation":
        return TradeResult("", 0.0, "not_tradeable_neutral", 0.0, 0.0, 0.0, 0, "", "", "", "", 0.0)

    future = ohlc[idx + 1 : idx + MAX_HOLD_BARS + 1]
    if not future:
        return TradeResult("", 0.0, "no_future_data", 0.0, 0.0, 0.0, 0, "", "", "", "", 0.0)

    tp = float(target_price(entry, side))
    sl = float(stop_price(entry, side))
    exit_date = future[-1]["trade_date"]
    exit_price = float(future[-1]["close"])
    exit_reason = "time_exit"
    target_hit_date = ""
    stop_hit_date = ""

    highs = [float(r["high"]) for r in future]
    lows = [float(r["low"]) for r in future]
    if side == "long":
        mfe = (max(highs) - entry) / entry
        mae = max(0.0, (entry - min(lows)) / entry)
    else:
        mfe = (entry - min(lows)) / entry
        mae = max(0.0, (max(highs) - entry) / entry)

    bars_held = len(future)
    for i, row in enumerate(future, 1):
        high = float(row["high"])
        low = float(row["low"])
        if side == "long":
            hit_stop = low <= sl
            hit_target = high >= tp
        else:
            hit_stop = high >= sl
            hit_target = low <= tp
        if hit_stop and hit_target:
            exit_date = row["trade_date"]
            exit_price = sl
            exit_reason = "stop_loss_same_bar_conservative"
            stop_hit_date = row["trade_date"]
            bars_held = i
            break
        if hit_stop:
            exit_date = row["trade_date"]
            exit_price = sl
            exit_reason = "stop_loss"
            stop_hit_date = row["trade_date"]
            bars_held = i
            break
        if hit_target:
            exit_date = row["trade_date"]
            exit_price = tp
            exit_reason = "target"
            target_hit_date = row["trade_date"]
            bars_held = i
            break

    pnl = directional_return(entry, exit_price, side)
    post_target_behavior = ""
    post_stop_behavior = ""
    missed_profit = 0.0
    if target_hit_date:
        post_target_behavior, missed_profit = analyze_after_target(target_hit_date, tp, side, ohlc, date_index)
    if stop_hit_date:
        post_stop_behavior = analyze_after_stop(stop_hit_date, entry, tp, side, ohlc, date_index)

    return TradeResult(
        exit_date=exit_date,
        exit_price=round(exit_price, 4),
        exit_reason=exit_reason,
        pnl_pct=pnl,
        mfe_pct=mfe,
        mae_pct=mae,
        bars_held=bars_held,
        target_hit_date=target_hit_date,
        stop_hit_date=stop_hit_date,
        post_target_behavior=post_target_behavior,
        post_stop_behavior=post_stop_behavior,
        missed_profit_index=missed_profit,
    )


def directional_return(entry: float, exit_price: float, side: str) -> float:
    if side == "long":
        return (exit_price - entry) / entry
    if side == "short":
        return (entry - exit_price) / entry
    return 0.0


def analyze_after_target(date: str, target: float, side: str, ohlc: list[dict[str, Any]], date_index: dict[str, int]) -> tuple[str, float]:
    idx = date_index.get(date)
    if idx is None:
        return "", 0.0
    future = ohlc[idx + 1 : idx + POST_EVENT_BARS + 1]
    if not future:
        return "no_post_target_data", 0.0
    if side == "long":
        best = max(float(r["high"]) for r in future)
        worst = min(float(r["low"]) for r in future)
        extra = (best - target) / target
        reversed_now = worst < target * 0.98
    else:
        best = min(float(r["low"]) for r in future)
        worst = max(float(r["high"]) for r in future)
        extra = (target - best) / target
        reversed_now = worst > target * 1.02
    if extra >= 0.10:
        label = "continued_10pct_more"
    elif extra >= 0.05:
        label = "continued_5pct_more"
    elif reversed_now:
        label = "reversed_immediately"
    else:
        label = "consolidated_sideways"
    return label, max(0.0, extra)


def analyze_after_stop(date: str, entry: float, target: float, side: str, ohlc: list[dict[str, Any]], date_index: dict[str, int]) -> str:
    idx = date_index.get(date)
    if idx is None:
        return ""
    future = ohlc[idx + 1 : idx + POST_EVENT_BARS + 1]
    if not future:
        return "no_post_stop_data"
    if side == "long":
        recovered_entry = max(float(r["high"]) for r in future) >= entry
        recovered_target = max(float(r["high"]) for r in future) >= target
        continued_down = min(float(r["low"]) for r in future) <= entry * (1 - STOP_PCT * 2)
    else:
        recovered_entry = min(float(r["low"]) for r in future) <= entry
        recovered_target = min(float(r["low"]) for r in future) <= target
        continued_down = max(float(r["high"]) for r in future) >= entry * (1 + STOP_PCT * 2)
    if recovered_target:
        return "recovered_and_hit_target_later"
    if recovered_entry:
        return "bounced_back_above_entry"
    if continued_down:
        return "continued_further_against_trade"
    return "flat_noise_after_stop"


def classify_signal_type(strategy: str, behavior: dict[str, Any]) -> str:
    s = strategy.lower()
    if any(k in s for k in ["breakout", "break", "52w", "or", "pivot", "range expansion"]):
        return "Breakout"
    if any(k in s for k in ["pullback", "support", "reclaim", "wick", "mean", "gap fill"]):
        return "Mean Reversion"
    if any(k in s for k in ["ema", "sma", "trend", "slope", "ribbon", "stack", "higher", "momentum", "rs bull"]):
        return "Trend Continuation"
    if behavior.get("coarse_behavior"):
        return "AI Pattern"
    return "Other"


def pattern_combination(behavior: dict[str, Any], event: dict[str, Any], cluster: list[str]) -> str:
    atoms = [
        behavior.get("flow", ""),
        behavior.get("volatility", ""),
        behavior.get("ma_position", ""),
        behavior.get("breakout_state", ""),
    ]
    atoms = [a for a in atoms if a]
    companion = [s for s in cluster if s != event["strategy_name"]][:3]
    companion_txt = f" | companions: {', '.join(companion)}" if companion else ""
    return f"{behavior.get('coarse_behavior', 'unknown')} | atoms: {', '.join(atoms)} | signal: {event['strategy_name']}{companion_txt}"


def quality_bucket(result: TradeResult, fit: dict[str, Any], side: str) -> str:
    if side == "neutral_observation":
        return "Observation"
    if result.exit_reason.startswith("target") and result.bars_held <= 3 and result.post_target_behavior in {"continued_5pct_more", "continued_10pct_more"} and result.mae_pct <= 0.025:
        return "Turbo"
    if result.exit_reason.startswith("target") and result.post_target_behavior in {"continued_5pct_more", "continued_10pct_more", "consolidated_sideways"}:
        return "Super"
    if result.exit_reason.startswith("target") or result.pnl_pct > 0.02:
        return "Standard"
    if result.exit_reason.startswith("stop") and result.post_stop_behavior in {"recovered_and_hit_target_later", "bounced_back_above_entry"}:
        return "Trap"
    if result.exit_reason.startswith("stop"):
        return "Trap"
    return "Standard"


def signal_strength_score(result: TradeResult, fit: dict[str, Any], quality: str) -> int:
    wr = safe_float(fit.get("win_rate_15d"), 0.5)
    avg_ret = safe_float(fit.get("avg_ret_15d"), 0.0) / 10.0
    speed = 1.0 - min(result.bars_held, MAX_HOLD_BARS) / MAX_HOLD_BARS if result.bars_held else 0.0
    continuation = min(result.missed_profit_index / 0.10, 1.0)
    drawdown = 1.0 - min(result.mae_pct / STOP_PCT, 1.0)
    false_stop_penalty = 0.35 if quality == "Trap" else 0.0
    score = (
        32 * wr
        + 18 * max(0.0, min(avg_ret, 1.0))
        + 18 * speed
        + 16 * continuation
        + 16 * drawdown
        - 100 * false_stop_penalty
    )
    if quality == "Turbo":
        score += 12
    elif quality == "Super":
        score += 7
    elif quality == "Trap":
        score -= 10
    return int(max(0, min(100, round(score))))


def summarize_group(rows: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if "entry_month" not in row:
            row["entry_month"] = row["entry_date"][:7]
        grouped[tuple(row.get(k, "") for k in keys)].append(row)
    out = []
    for key, items in grouped.items():
        tradeable = [r for r in items if r["trade_side"] != "neutral_observation" and r["exit_reason"] != "no_future_data"]
        if not tradeable:
            continue
        wins = [r for r in tradeable if float(r["pnl_pct"]) > 0]
        targets = [r for r in tradeable if str(r["exit_reason"]).startswith("target")]
        stops = [r for r in tradeable if str(r["exit_reason"]).startswith("stop")]
        false_stops = [r for r in stops if r["post_stop_behavior"] in {"recovered_and_hit_target_later", "bounced_back_above_entry"}]
        d = {keys[i]: key[i] for i in range(len(keys))}
        d.update(
            {
                "trades": len(tradeable),
                "win_rate": pct(len(wins), len(tradeable)),
                "target_rate": pct(len(targets), len(tradeable)),
                "stop_rate": pct(len(stops), len(tradeable)),
                "false_stop_rate": pct(len(false_stops), len(stops)) if stops else 0,
                "avg_return_pct": round(mean(float(r["pnl_pct"]) for r in tradeable), 4),
                "median_return_pct": round(median(float(r["pnl_pct"]) for r in tradeable), 4),
                "avg_mfe_pct": round(mean(float(r["max_favorable_excursion_pct"]) for r in tradeable), 4),
                "avg_mae_pct": round(mean(float(r["max_adverse_excursion_pct"]) for r in tradeable), 4),
                "avg_strength_score": round(mean(int(r["signal_strength_score"]) for r in tradeable), 2),
                "missed_profit_index_pct": round(mean(float(r["missed_profit_index_pct"]) for r in tradeable), 4),
                "turbo_count": sum(1 for r in tradeable if r["quality_bucket"] == "Turbo"),
                "super_count": sum(1 for r in tradeable if r["quality_bucket"] == "Super"),
                "standard_count": sum(1 for r in tradeable if r["quality_bucket"] == "Standard"),
                "trap_count": sum(1 for r in tradeable if r["quality_bucket"] == "Trap"),
            }
        )
        out.append(d)
    out.sort(key=lambda r: (-r["avg_strength_score"], -r["trades"], -r["avg_return_pct"]))
    return out


def overall_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tradeable = [r for r in rows if r["trade_side"] != "neutral_observation" and r["exit_reason"] != "no_future_data"]
    wins = [r for r in tradeable if float(r["pnl_pct"]) > 0]
    targets = [r for r in tradeable if str(r["exit_reason"]).startswith("target")]
    stops = [r for r in tradeable if str(r["exit_reason"]).startswith("stop")]
    buckets = Counter(r["quality_bucket"] for r in tradeable)
    return {
        "signal_events": len(rows),
        "tradeable_events": len(tradeable),
        "neutral_observations": sum(1 for r in rows if r["trade_side"] == "neutral_observation"),
        "win_rate": pct(len(wins), len(tradeable)),
        "target_rate": pct(len(targets), len(tradeable)),
        "stop_rate": pct(len(stops), len(tradeable)),
        "avg_return_pct": round(mean(float(r["pnl_pct"]) for r in tradeable), 4) if tradeable else 0,
        "median_return_pct": round(median(float(r["pnl_pct"]) for r in tradeable), 4) if tradeable else 0,
        "avg_mfe_pct": round(mean(float(r["max_favorable_excursion_pct"]) for r in tradeable), 4) if tradeable else 0,
        "avg_mae_pct": round(mean(float(r["max_adverse_excursion_pct"]) for r in tradeable), 4) if tradeable else 0,
        "avg_strength_score": round(mean(int(r["signal_strength_score"]) for r in tradeable), 2) if tradeable else 0,
        "bucket_counts": dict(buckets),
    }


def build_markdown_report(rows: list[dict[str, Any]], summaries: dict[str, Any], year: str) -> str:
    overall = summaries["overall"]
    best_strategies = filter_min_trades(summaries["by_strategy"], 20)[:12]
    worst_traps = sorted(filter_min_trades(summaries["by_strategy"], 10), key=lambda r: (-r["trap_count"], r["avg_return_pct"]))[:12]
    best_combos = filter_min_trades(summaries["by_pattern_atom_signal"], 8)[:12]
    best_timeframes = summaries["by_timeframe"]
    best_months = filter_min_trades(summaries["by_month"], 20)[:12]
    best_conditions = filter_min_trades(summaries["by_market_condition"], 20)[:12]
    buckets = summaries["by_quality_bucket"]

    lines = [
        f"# AMZN {year} Signal Performance Study",
        "",
        f"Scope: AMZN, US market, signal events dated {year}-01-01 through {year}-12-31.",
        "",
        "Backtest model: recorded signal entry price, +10% target, -5% stop, 20-trading-day max hold, daily OHLC execution. If stop and target occur in the same daily bar, stop is assumed first.",
        "",
        "Important limitation: the available DB contains 1D and 1W AMZN signals here. It does not contain AMZN 5m, 15m, or 1H intraday trigger/fill data for this study.",
        "",
        "## Executive Summary",
        "",
        f"- Signal events analyzed: {overall['signal_events']:,}",
        f"- Tradeable bullish/bearish events: {overall['tradeable_events']:,}",
        f"- Neutral observations excluded from trade ranking: {overall['neutral_observations']:,}",
        f"- Win rate: {overall['win_rate']:.2f}%",
        f"- Target-hit rate: {overall['target_rate']:.2f}%",
        f"- Stop-hit rate: {overall['stop_rate']:.2f}%",
        f"- Average return per simulated trade: {overall['avg_return_pct']:.2f}%",
        f"- Median return per simulated trade: {overall['median_return_pct']:.2f}%",
        f"- Average MFE / MAE: {overall['avg_mfe_pct']:.2f}% / {overall['avg_mae_pct']:.2f}%",
        f"- Average signal strength score: {overall['avg_strength_score']:.1f}/100",
        "",
        "## Quality Bucket Counts",
        "",
        table(["Bucket", "Trades"], [[k, v] for k, v in sorted(overall["bucket_counts"].items())]),
        "",
        "## Best Performing Signal Types",
        "",
        table_rows(summaries["by_signal_type"], ["signal_type", "trades", "win_rate", "avg_return_pct", "target_rate", "avg_strength_score", "missed_profit_index_pct"]),
        "",
        "## Best Strategies",
        "",
        table_rows(best_strategies, ["strategy_name", "bias", "timeframe_used", "trades", "win_rate", "avg_return_pct", "target_rate", "avg_strength_score", "turbo_count", "super_count", "trap_count"]),
        "",
        "## Worst Trap Signals",
        "",
        table_rows(worst_traps, ["strategy_name", "bias", "timeframe_used", "trades", "stop_rate", "false_stop_rate", "avg_return_pct", "trap_count"]),
        "",
        "## Best Pattern + Atom + Signal Combinations",
        "",
        table_rows(best_combos, ["behavior_pattern", "flow", "volatility", "ma_position", "breakout_state", "strategy_name", "trades", "win_rate", "avg_return_pct", "avg_strength_score"]),
        "",
        "## Timeframe Intelligence",
        "",
        table_rows(best_timeframes, ["timeframe_used", "trades", "win_rate", "avg_return_pct", "target_rate", "avg_strength_score"]),
        "",
        "## Best Months",
        "",
        table_rows(best_months, ["entry_month", "trades", "win_rate", "avg_return_pct", "target_rate", "avg_strength_score"]),
        "",
        "## Best Market Conditions",
        "",
        table_rows(best_conditions, ["engine_trend_state", "volatility", "ma_position", "trades", "win_rate", "avg_return_pct", "avg_strength_score"]),
        "",
        "## Signals To Auto-Trade Candidate List",
        "",
        "Candidate rule used here: at least 20 trades, average strength score >= 65, positive average return, trap count below 25%.",
        "",
        table_rows(auto_trade_candidates(summaries["by_strategy"]), ["strategy_name", "bias", "timeframe_used", "trades", "win_rate", "avg_return_pct", "avg_strength_score", "trap_count"]),
        "",
        "## Signals To Avoid",
        "",
        "Avoid rule used here: at least 10 trades and either negative average return or trap rate above 45%.",
        "",
        table_rows(avoid_candidates(summaries["by_strategy"]), ["strategy_name", "bias", "timeframe_used", "trades", "stop_rate", "avg_return_pct", "avg_strength_score", "trap_count"]),
        "",
        "## Missed Profit Index",
        "",
        "Missed Profit Index measures extra move after a +10% target was hit. Example: target booked at +10%, then the stock continued another +5% within the post-target window = 5% missed profit index.",
        "",
        table_rows(sorted(filter_min_trades(summaries["by_strategy"], 10), key=lambda r: -r["missed_profit_index_pct"])[:12], ["strategy_name", "bias", "timeframe_used", "trades", "target_rate", "missed_profit_index_pct", "avg_return_pct"]),
        "",
    ]
    return "\n".join(lines)


def build_combined_markdown_report(rows: list[dict[str, Any]], summaries: dict[str, Any]) -> str:
    overall = summaries["overall"]
    by_year = sorted(summaries["by_year"], key=lambda r: r["year"])
    best_strategies = filter_min_trades(summaries["by_strategy"], 40)[:15]
    worst_traps = sorted(filter_min_trades(summaries["by_strategy"], 20), key=lambda r: (-r["trap_count"], r["avg_return_pct"]))[:15]
    best_conditions = filter_min_trades(summaries["by_market_condition"], 40)[:15]
    best_combos = filter_min_trades(summaries["by_pattern_atom_signal"], 15)[:15]
    lines = [
        "# AMZN Multi-Year Signal Performance Study",
        "",
        "Scope: AMZN, US market, combined signal events across requested years.",
        "",
        "Backtest model: recorded signal entry price, +10% target, -5% stop, 20-trading-day max hold, daily OHLC execution. If stop and target occur in the same daily bar, stop is assumed first.",
        "",
        "Important limitation: the available DB contains 1D and 1W AMZN signals for this study. It does not contain AMZN 5m, 15m, or 1H intraday trigger/fill data.",
        "",
        "## Combined Executive Summary",
        "",
        f"- Signal events analyzed: {overall['signal_events']:,}",
        f"- Tradeable bullish/bearish events: {overall['tradeable_events']:,}",
        f"- Win rate: {overall['win_rate']:.2f}%",
        f"- Target-hit rate: {overall['target_rate']:.2f}%",
        f"- Stop-hit rate: {overall['stop_rate']:.2f}%",
        f"- Average return per simulated trade: {overall['avg_return_pct']:.2f}%",
        f"- Average signal strength score: {overall['avg_strength_score']:.1f}/100",
        "",
        "## Year-by-Year Comparison",
        "",
        table_rows(by_year, ["year", "signal_events", "tradeable_events", "win_rate", "target_rate", "stop_rate", "avg_return_pct", "avg_strength_score"]),
        "",
        "## Combined Best Signal Types",
        "",
        table_rows(summaries["by_signal_type"], ["signal_type", "trades", "win_rate", "avg_return_pct", "target_rate", "avg_strength_score", "missed_profit_index_pct"]),
        "",
        "## Combined Best Strategies",
        "",
        table_rows(best_strategies, ["strategy_name", "bias", "timeframe_used", "trades", "win_rate", "avg_return_pct", "target_rate", "avg_strength_score", "super_count", "trap_count"]),
        "",
        "## Combined Best Market Conditions",
        "",
        table_rows(best_conditions, ["engine_trend_state", "volatility", "ma_position", "trades", "win_rate", "avg_return_pct", "target_rate", "avg_strength_score", "trap_count"]),
        "",
        "## Combined Best Pattern + Atom + Signal Combinations",
        "",
        table_rows(best_combos, ["behavior_pattern", "flow", "volatility", "ma_position", "breakout_state", "strategy_name", "trades", "win_rate", "avg_return_pct", "avg_strength_score"]),
        "",
        "## Combined Trap Signals",
        "",
        table_rows(worst_traps, ["strategy_name", "bias", "timeframe_used", "trades", "stop_rate", "false_stop_rate", "avg_return_pct", "trap_count"]),
        "",
        "## Combined Signals To Auto-Trade Candidate List",
        "",
        table_rows(auto_trade_candidates(summaries["by_strategy"]), ["strategy_name", "bias", "timeframe_used", "trades", "win_rate", "avg_return_pct", "avg_strength_score", "trap_count"]),
        "",
        "## Combined Signals To Avoid",
        "",
        table_rows(avoid_candidates(summaries["by_strategy"]), ["strategy_name", "bias", "timeframe_used", "trades", "stop_rate", "avg_return_pct", "avg_strength_score", "trap_count"]),
        "",
    ]
    return "\n".join(lines)


def auto_trade_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        trap_rate = r["trap_count"] / r["trades"] if r["trades"] else 1
        if r["trades"] >= 20 and r["avg_strength_score"] >= 65 and r["avg_return_pct"] > 0 and trap_rate < 0.25:
            out.append(r)
    return out[:20]


def avoid_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        trap_rate = r["trap_count"] / r["trades"] if r["trades"] else 0
        if r["trades"] >= 10 and (r["avg_return_pct"] < 0 or trap_rate > 0.45):
            out.append(r)
    return sorted(out, key=lambda r: (-r["trap_count"], r["avg_return_pct"]))[:20]


def filter_min_trades(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    return [r for r in rows if int(r.get("trades", 0)) >= n]


def table_rows(rows: list[dict[str, Any]], columns: list[str]) -> str:
    return table(columns, [[format_cell(row.get(c, "")) for c in columns] for row in rows])


def table(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return "_No rows matched this rule._"
    out = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(out)


def format_cell(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def pct(num: int, den: int) -> float:
    return round((num / den) * 100, 4) if den else 0.0


def safe_float(value: Any, default: float) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


if __name__ == "__main__":
    main()
