from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import re
from typing import Any

from engine.patterns.signal_pattern_miner import SignalPatternMiner


def current_matches(
    learned_rows: list[dict[str, Any]],
    live_rows: list[dict[str, Any]],
    miner: SignalPatternMiner,
    config: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    live_stats = miner.mine(live_rows)
    live_keys = set(live_stats.keys())
    learned_rows = [r for r in learned_rows if r["roster_state"] in {"keep", "watch"}]
    live_index = _build_live_index(live_rows, miner)
    exact_keys = {
        (
            key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7], key[8], key[9], key[10], key[11], key[12]
        )
        for key in live_keys
    }
    matches = []
    per_stock_count: dict[tuple[str, str, str, str], int] = defaultdict(int)
    max_per_stock = int(config["opportunity"]["max_per_stock"])

    scored = []
    for row in learned_rows:
        stock_key = (row["market"], row["ticker"], row["timeframe"], row["bias"])
        live_state = live_index.get(stock_key)
        if not live_state:
            continue
        candidate = _score_candidate(row, live_state, exact_keys, config)
        if candidate is None:
            continue
        scored.append(candidate)

    scored.sort(
        key=lambda r: (
            -float(r["decision_score"]),
            -float(r["evidence_score"]),
            -float(r["weighted_directional_return"]),
            -int(r["occurrences"]),
        )
    )

    target_per_market = int(config["opportunity"]["target_per_market"])
    per_market_count: dict[str, int] = defaultdict(int)
    for row in scored:
        stock_key = (row["market"], row["ticker"], row["timeframe"], row["bias"])
        if per_stock_count[stock_key] >= max_per_stock:
            continue
        if per_market_count[row["market"]] >= target_per_market:
            continue
        row["match_status"] = "approved_condition" if row["roster_state"] == "keep" else "watch_condition"
        matches.append(row)
        per_stock_count[stock_key] += 1
        per_market_count[row["market"]] += 1

    if not matches:
        matches = _fallback_candidates(scored, config)

    clusters = portfolio_clusters(matches, int(config["reports"]["portfolio_cluster_min_matches"]))
    return matches, clusters


def _build_live_index(
    live_rows: list[dict[str, Any]],
    miner: SignalPatternMiner,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in live_rows:
        grouped[(row["market"], row["ticker"], row["timeframe"], row["bias"])].append(row)

    out = {}
    for key, rows in grouped.items():
        market, ticker, timeframe, bias = key
        latest_date = max(row["signal_date"] for row in rows)
        latest_rows = [r for r in rows if r["signal_date"] == latest_date]
        recent_dates = sorted({r["signal_date"] for r in rows}, reverse=True)[:5]
        recent_rows = [r for r in rows if r["signal_date"] in recent_dates]
        current_signals = set(miner._top_signals(latest_rows, 12))
        recent_signals = set(miner._top_signals(recent_rows, 20))
        trend = miner.trend_engine.get(market, ticker, latest_date)
        out[key] = {
            "market": market,
            "ticker": ticker,
            "timeframe": timeframe,
            "bias": bias,
            "latest_date": latest_date,
            "current_signals": current_signals,
            "recent_signals": recent_signals,
            "trend_state": trend.get("composite_trend_state", "trend_missing"),
            "weekly_context": trend.get("weekly_proxy_state", "weekly_proxy_unknown"),
            "market_context": trend.get("market_context_state", "market_unknown"),
            "sector_context": trend.get("sector_context_state", "sector_unknown"),
            "volatility_regime": trend.get("volatility_regime", "vol_unknown"),
            "sector": trend.get("sector", "UNKNOWN"),
        }
    return out


def _score_candidate(
    row: dict[str, Any],
    live_state: dict[str, Any],
    exact_keys: set[tuple[Any, ...]],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    exact_key = (
        row["market"],
        row["ticker"],
        row["timeframe"],
        row["bias"],
        row["pattern_type"],
        row["pattern_size"],
        row["pattern"],
        row["trend_state"],
        row["weekly_context"],
        row["market_context"],
        row["sector_context"],
        row["volatility_regime"],
        row["sector"],
    )
    pattern_signals = _pattern_signals(row["pattern"], row["pattern_type"])
    signal_overlap = _overlap_ratio(pattern_signals, live_state["recent_signals"] or live_state["current_signals"])
    current_overlap = _overlap_ratio(pattern_signals, live_state["current_signals"])
    exact = exact_key in exact_keys
    trend_match = (
        row["trend_state"] == live_state["trend_state"]
        and row["weekly_context"] == live_state["weekly_context"]
    )
    coarse_match = _coarse_state(row["trend_state"]) == _coarse_state(live_state["trend_state"])
    any_signal_match = signal_overlap >= float(config["opportunity"]["min_signal_overlap_ratio"])
    if not (exact or trend_match or coarse_match or any_signal_match):
        return None

    precision = "exact_context" if exact else "trend_context" if trend_match else "coarse_context" if coarse_match else "signal_context"
    evidence = float(row["evidence_score"])
    win_rate = float(row["win_rate"])
    avg_ret = float(row["avg_directional_return"])
    recent_wr = float(row.get("recent_win_rate", win_rate))
    recent_ret = float(row.get("recent_avg_directional_return", avg_ret))
    recent_component = max(0.0, min(1.0, ((recent_wr - 0.45) / 0.25 + (recent_ret / 0.05)) / 2))
    decision_score = (
        0.34 * evidence
        + 0.20 * win_rate
        + 0.18 * min(1.0, avg_ret / 0.05)
        + 0.08 * min(1.0, recent_component)
        + float(config["opportunity"]["signal_overlap_bonus"]) * signal_overlap
        + (float(config["opportunity"]["exact_context_bonus"]) if exact else 0.0)
        + (float(config["opportunity"]["trend_context_bonus"]) if trend_match else 0.0)
        + (float(config["opportunity"]["coarse_context_bonus"]) if coarse_match and not trend_match else 0.0)
    )
    tier = _tier(decision_score, evidence, signal_overlap, row["roster_state"])
    setup_type = _setup_type(row["trend_state"], row["bias"])
    fired = sorted(live_state["current_signals"])
    summary = (
        f"This stock is in {live_state['trend_state']}. "
        f"Signals firing now: {', '.join(fired[:6]) if fired else 'recent signal activity'}. "
        f"Historically {row['pattern']} led to {win_rate:.1%} win rate and "
        f"{avg_ret:.2%} average directional return over 15 days. "
        f"This is a {tier.replace('_', ' ')} {setup_type} setup."
    )
    out = dict(row)
    out.update(
        {
            "latest_signal_date": live_state["latest_date"],
            "current_trend_state": live_state["trend_state"],
            "current_signals": ", ".join(fired[:10]),
            "current_signal_count": len(live_state["current_signals"]),
            "signal_overlap_ratio": signal_overlap,
            "current_signal_overlap_ratio": current_overlap,
            "decision_score": round(decision_score, 6),
            "conviction_tier": tier,
            "setup_type": setup_type,
            "match_precision": precision,
            "setup_summary": summary,
        }
    )
    return out


def _fallback_candidates(scored: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    fallback = []
    per_market_count: dict[str, int] = defaultdict(int)
    limit = int(config["opportunity"]["fallback_top_per_market"])
    for row in scored:
        if per_market_count[row["market"]] >= limit:
            continue
        out = dict(row)
        out["match_status"] = "watch_condition"
        out["match_precision"] = out.get("match_precision", "fallback")
        fallback.append(out)
        per_market_count[row["market"]] += 1
    return fallback


def _pattern_signals(pattern: str, pattern_type: str) -> set[str]:
    if pattern_type == "same_bar":
        return {p.strip() for p in pattern.split(" + ") if p.strip()}
    if pattern_type == "cross_timeframe":
        pattern = re.sub(r"\b1W:[^+]+ \+ ", "", pattern)
        pattern = re.sub(r"\b1D:", "", pattern)
        return {p.strip() for p in pattern.split(" + ") if p.strip()}
    if pattern_type == "sequence":
        items = re.findall(r"\[([^\]]+)\]", pattern)
        parts: set[str] = set()
        for item in items:
            parts.update({p.strip() for p in item.split(" & ") if p.strip()})
        return parts
    return {pattern}


def _overlap_ratio(pattern_signals: set[str], live_signals: set[str]) -> float:
    if not pattern_signals:
        return 0.0
    return len(pattern_signals & live_signals) / len(pattern_signals)


def _coarse_state(state: str) -> str:
    parts = state.split("|")
    if len(parts) < 3:
        return state
    return "|".join([_coarse_part(parts[0]), _coarse_part(parts[1]), _coarse_part(parts[2])])


def _coarse_part(part: str) -> str:
    if "strong_up" in part or part.endswith("_up"):
        return "up"
    if "strong_down" in part or part.endswith("_down"):
        return "down"
    if "sideways" in part or "range" in part:
        return "sideways"
    if "continuation" in part:
        return "continuation"
    if "reversal" in part or "pullback" in part:
        return "reversal"
    if "compressed" in part:
        return "compressed"
    if "expanded" in part or "break" in part:
        return "expanded"
    if "normal_vol" in part:
        return "normal_vol"
    return part


def _setup_type(trend_state: str, bias: str) -> str:
    coarse = _coarse_state(trend_state)
    if "continuation" in coarse:
        return "continuation"
    if "reversal" in coarse:
        return "reversal"
    if "sideways" in coarse and bias == "bullish":
        return "breakout"
    if "sideways" in coarse and bias == "bearish":
        return "breakdown"
    return "continuation" if bias == "bullish" else "breakdown"


def _tier(decision_score: float, evidence: float, overlap: float, roster_state: str) -> str:
    if decision_score >= 0.88 and evidence >= 0.70 and overlap >= 0.5 and roster_state == "keep":
        return "high_conviction"
    if decision_score >= 0.72 and evidence >= 0.55:
        return "medium"
    return "exploratory"


def portfolio_clusters(matches: list[dict[str, Any]], min_matches: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in matches:
        key = (
            row["market"],
            row.get("sector") or "UNKNOWN",
            row["bias"],
            row["pattern_type"],
            row["trend_state"],
        )
        grouped[key].append(row)

    clusters = []
    for (market, sector, bias, pattern_type, trend_state), rows in grouped.items():
        tickers = sorted({r["ticker"] for r in rows})
        if len(tickers) < min_matches:
            continue
        clusters.append(
            {
                "market": market,
                "sector": sector,
                "bias": bias,
                "pattern_type": pattern_type,
                "trend_state": trend_state,
                "stock_count": len(tickers),
                "tickers": ", ".join(tickers[:20]),
                "avg_evidence_score": sum(float(r["evidence_score"]) for r in rows) / len(rows),
                "avg_directional_return": sum(float(r["avg_directional_return"]) for r in rows) / len(rows),
                "avg_win_rate": sum(float(r["win_rate"]) for r in rows) / len(rows),
            }
        )
    clusters.sort(key=lambda r: (-r["stock_count"], -r["avg_evidence_score"]))
    return clusters
