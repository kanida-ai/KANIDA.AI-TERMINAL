"""Falcon Top 20 — 3-bucket explainability builder.

Reads real data from PROD (falcon_signals_live, falcon_pattern_taxonomy,
falcon_sectors, universe_master, ohlc_daily) and RND (falcon_outcomes).
Returns a list of up-to-20 picks with full 3-bucket institutional-grade
explainability for the /power/today page.

Three buckets per pick:
    bucket1 — WHY  : the patterns that fired, in plain English, with lift
    bucket2 — EVIDENCE : historical track record of these patterns on THIS stock
    bucket3 — SECTOR   : sector rank, peer firings, sector momentum

Architecture decision (2026-05-23): compute inline at request time. No
pre-computed table for v1 — the query budget is ~20 picks × 4 small
indexed queries each = ~80 queries, comfortably under 500ms. If load
requires it, swap to a nightly `falcon_top20_daily` materialiser later;
the response shape stays identical so no frontend churn.

The endpoint that calls this is /api/power/today/falcon-top-20 and the
response shape matches lib/falcon-top20-types.ts (frontend).
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .pattern_narrator import narrate as _narrate_pattern
from .pattern_narrator import narrate_synthesis as _narrate_synthesis


# Strategy-level per-trade win rate from the LOCKED falcon-top-10 persona.
# This is the headline metric every card shows — what % of all Falcon Top 10
# trades have closed profitable historically under the 7-day-hold / -7%-stop
# rules. Source: persona_simulator's `falcon-top-10` backtest. Same value
# on every card by design. Cached for 1 hour.
_STRAT_WR_CACHE: Dict[str, Any] = {"win_rate_pct": None, "n_trades": 0, "ts": 0.0}
_STRAT_WR_TTL_SECONDS = 3600
_FALLBACK_WIN_RATE_PCT = 69.25   # from falcon_top10_5yr_walkforward_CORRECTED.xlsx
_FALLBACK_TRADES_N     = 2843    # 6-yr backtest total (approximate)


def _per_stock_persona_stats() -> Dict[str, Dict[str, Any]]:
    """Returns {symbol: per-stock backtest stats} from the locked falcon-top-10
    persona's actual closed trades. This is what the UX 'Historical track
    record' section reads — per-stock, NOT strategy-wide.

    Each stock's dict carries:
      n_trades, n_wins, n_losses, win_rate_pct,
      avg_win_pct, avg_loss_pct, best_win_pct, worst_loss_pct,
      first_trade_date, last_trade_date

    All numbers come from net_return_pct in the persona's closed-trade rows,
    which means the -7% stop and 7-day hold are ALREADY applied. The 'worst
    loss' here is the worst real trade outcome — typically near -7% (the
    stop), never the misleading -30% 20-day MAE.

    Cached for the same 1-hour TTL as the persona simulator's own cache.
    """
    import time as _t
    if (_STOCK_STATS_CACHE["data"] is not None
            and (_t.time() - _STOCK_STATS_CACHE["ts"]) < _STRAT_WR_TTL_SECONDS):
        return _STOCK_STATS_CACHE["data"]   # type: ignore
    try:
        from .persona_simulator import simulate_persona
        trades = (simulate_persona("falcon-top-10").get("trades") or [])
    except Exception as e:
        log.warning("_per_stock_persona_stats: persona unavailable (%s)", e)
        return {}

    from collections import defaultdict
    by_sym: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for t in trades:
        sym = t.get("symbol")
        if sym:
            by_sym[sym].append(t)

    out: Dict[str, Dict[str, Any]] = {}
    for sym, ts in by_sym.items():
        n = len(ts)
        wins   = [t for t in ts if (t.get("net_pnl_rs") or 0) > 0]
        losses = [t for t in ts if (t.get("net_pnl_rs") or 0) < 0]
        rets   = [float(t.get("net_return_pct") or 0) for t in ts]
        win_rets  = [float(t.get("net_return_pct") or 0) for t in wins]
        loss_rets = [float(t.get("net_return_pct") or 0) for t in losses]
        out[sym] = {
            "n_trades":         n,
            "n_wins":           len(wins),
            "n_losses":         len(losses),
            "win_rate_pct":     round(len(wins) / n * 100, 1) if n else None,
            "avg_win_pct":      round(sum(win_rets) / len(win_rets), 2) if win_rets else None,
            "avg_loss_pct":     round(sum(loss_rets) / len(loss_rets), 2) if loss_rets else None,
            "best_win_pct":     round(max(rets), 2) if rets else None,
            "worst_loss_pct":   round(min(rets), 2) if rets else None,
            "first_trade_date": min((t.get("entry_date") for t in ts if t.get("entry_date")), default=None),
            "last_trade_date":  max((t.get("entry_date") for t in ts if t.get("entry_date")), default=None),
        }
    _STOCK_STATS_CACHE.update({"data": out, "ts": _t.time()})
    log.info("_per_stock_persona_stats: cached %d symbols", len(out))
    return out


_STOCK_STATS_CACHE: Dict[str, Any] = {"data": None, "ts": 0.0}


def _strategy_win_rate() -> Tuple[float, int]:
    """Returns (win_rate_pct, n_trades) for the locked falcon-top-10 persona.

    Computed from the actual closed-trade outcomes (winners / total) so it
    matches what the operator's parity tests validate. Cached 1 hour; falls
    back to the locked spec value if the persona simulator is unavailable.
    """
    import time as _t
    if (_STRAT_WR_CACHE["win_rate_pct"] is not None
            and (_t.time() - _STRAT_WR_CACHE["ts"]) < _STRAT_WR_TTL_SECONDS):
        return float(_STRAT_WR_CACHE["win_rate_pct"]), int(_STRAT_WR_CACHE["n_trades"])
    try:
        from .persona_simulator import simulate_persona
        result = simulate_persona("falcon-top-10")
        trades = result.get("trades") or []
        # Persona trade row keys: net_pnl_rs, net_return_pct (NOT pnl_rs).
        n_total, n_wins = 0, 0
        for t in trades:
            pnl = t.get("net_pnl_rs")
            if pnl is None:
                pnl = t.get("net_return_pct")
            if pnl is None:
                pnl = t.get("pnl_rs")          # legacy fallback
            if pnl is None:
                continue
            n_total += 1
            if float(pnl) > 0:
                n_wins += 1
        if n_total > 0:
            wr = n_wins / n_total * 100.0
            _STRAT_WR_CACHE.update({"win_rate_pct": wr, "n_trades": n_total, "ts": _t.time()})
            log.info("strategy_win_rate computed: %.2f%% over %d closed trades", wr, n_total)
            return wr, n_total
        # No trade rows had usable keys — use the summary stat the persona
        # simulator already aggregates.
        summary = result.get("summary") or {}
        if "avg_win_rate_pct" in summary:
            wr = float(summary["avg_win_rate_pct"])
            n  = int(summary.get("total_trades", 0))
            _STRAT_WR_CACHE.update({"win_rate_pct": wr, "n_trades": n, "ts": _t.time()})
            log.info("strategy_win_rate from summary: %.2f%% over %d trades", wr, n)
            return wr, n
    except Exception as e:
        log.warning("strategy_win_rate: persona unavailable (%s) — using locked fallback", e)
    return _FALLBACK_WIN_RATE_PCT, _FALLBACK_TRADES_N

# Bucket 2 v2 lookback floor — matches research_2_signal_store.py and the
# falcon_top10_explainability_today_CORRECTED.xlsx evidence window.
_BUCKET2_LOOKBACK_START = "2021-01-01"

# Feature columns expected in falcon_features (subset of the 39 columns).
# Patterns reference any of these via rule_json; we accept ANY column the
# patterns name, but only these are guaranteed numeric-castable.
_FEATURE_COLS_NUMERIC: set[str] = {
    "range_pct","close_loc","gap_pct","body_pct","upper_wick_pct","lower_wick_pct",
    "dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20","slope_sma_50",
    "rsi_14","roc_5","roc_20","roc_60",
    "vol_vs_20d","vol_5d_vs_20d","n_sub_75v_7d","n_sub_75v_20d",
    "atr_20_pct","atr_5_vs_20","n_sub_2_5_range_7d","n_sub_3_range_7d",
    "n_higher_lows_5d","n_higher_highs_5d",
    "dist_high_10","dist_high_20","dist_high_60","dist_high_120","dist_high_252",
    "weekly_close_vs_sma20","weekly_breakout_20w","weekly_range_pct","weekly_close_loc",
    "rs_sector_20d","rs_sector_60d","rs_market_20d","rs_market_60d",
}

log = logging.getLogger("kanida.power_user.falcon_top20")


# ════════════════════════════════════════════════════════════════════════
# Public entry point
# ════════════════════════════════════════════════════════════════════════

def build_falcon_top20(
    prod_con: sqlite3.Connection,
    rnd_con:  sqlite3.Connection,
    signal_date: Optional[str] = None,
    universe:    str = "all500",
    sector:      Optional[str] = None,
    top_n:       int = 10,        # Falcon Top 10 — locked spec
) -> Dict[str, Any]:
    """Build the Falcon Top 20 payload for a given signal date.

    Args:
        prod_con    PROD DB (data/db/kanida_universe.db)
        rnd_con     RND DB  (universe_engine/data/db/kanida_universe.db)
        signal_date YYYY-MM-DD or None → latest available
        universe    'all500' | 'nifty50' | 'nifty100' | 'nifty200' | 'sector' | 'custom'
        sector      Only honoured when universe='sector' (e.g. 'Healthcare')
        top_n       defaults to 20

    Returns dict matching frontend Top20Response shape.
    """
    # Distinct signal dates available — embedded in the response so the
    # /power/today date picker stays data-driven (no separate endpoint
    # request, no client-side guessing). Cheap query: <100 distinct rows
    # for the foreseeable future.
    avail_rows = prod_con.execute(
        "SELECT DISTINCT signal_date FROM falcon_signals_live "
        "WHERE signal_date IS NOT NULL ORDER BY signal_date DESC LIMIT 365"
    ).fetchall()
    available_signal_dates = [r[0] for r in avail_rows]

    if signal_date is None:
        signal_date = available_signal_dates[0] if available_signal_dates else None
        if signal_date is None:
            return _empty_response(None, None, universe, sector, available_signal_dates)

    # ── 1. Get top N raw picks for this universe, ranked by avg_lift ────
    raw_picks = _fetch_raw_picks(
        prod_con, signal_date, universe, sector, top_n
    )

    # Authoritative entry_date is the one the engine wrote when it emitted
    # the signal. Falling back to MIN(trade_date) in ohlc_daily is fragile
    # because ohlc_daily can lag signal_date during off-hours (the daily
    # OHLC fetcher runs separately from the signal engine).
    entry_date: Optional[str] = None
    if raw_picks:
        for p in raw_picks:
            if p.get("entry_date"):
                entry_date = p["entry_date"]
                break
    if entry_date is None:
        # Last-resort fallback: query ohlc_daily for the next trade_date
        row = prod_con.execute(
            "SELECT MIN(trade_date) FROM ohlc_daily WHERE trade_date > ?", (signal_date,)
        ).fetchone()
        entry_date = row[0] if row else None

    if not raw_picks:
        return _empty_response(signal_date, entry_date, universe, sector, available_signal_dates)

    # Signal-table maturity: used to gate "thin historical sample" warnings.
    # While the engine is still in bootstrap (first few weeks of nightly
    # emits), the WHOLE table is thin — flagging every stock as "thin
    # sample, High risk" is noise, not signal. Once we have >= 30 distinct
    # signal_dates of history, the warning becomes meaningful.
    age_row = prod_con.execute(
        "SELECT COUNT(DISTINCT signal_date) FROM falcon_signals_live"
    ).fetchone()
    signal_history_days = int(age_row[0]) if age_row and age_row[0] else 0

    # ── 2. Load FULL taxonomy (all 865 patterns with rule_json) ────────
    # We need this for two reasons:
    #   (a) the 5 cap in falcon_signals_live.fired_pattern_ids means we
    #       can't trust the persisted list for a regime breakdown — a stock
    #       can have n_fires=94 but only 5 pattern IDs persisted. We
    #       re-evaluate all 865 patterns against the stock's feature row
    #       below to get the TRUE firing set.
    #   (b) the synthesis narrator needs rule conditions from EVERY firing
    #       pattern, not just the top 5, so the "approaching one-year high"
    #       type observations don't get dropped.
    # Cost: one query × 865 rows, ~50ms; cached for the request.
    taxonomy = _load_full_taxonomy(prod_con)

    # ── 3. Pre-compute sector stats (once, shared across picks) ─────────
    sector_stats = _compute_sector_stats(prod_con, signal_date)

    # ── 3b. Pre-compute the prior-5-days top-50 symbol set (for fresh_entry) ─
    prior5_top50 = _prior5_top50_symbols(prod_con, signal_date)

    # ── 3c. Pre-compute per-stock backtest stats (one persona call, cached).
    #         Lookups in _build_pick are O(1) dict access.
    per_stock_stats = _per_stock_persona_stats()

    # ── 4. Per-pick: compute 3 buckets ──────────────────────────────────
    picks_out: List[Dict[str, Any]] = []
    for rank_pos, p in enumerate(raw_picks, start=1):
        try:
            pick = _build_pick(
                prod_con, rnd_con,
                rank=rank_pos, raw=p,
                taxonomy=taxonomy,
                sector_stats=sector_stats,
                signal_date=signal_date,
                signal_history_days=signal_history_days,
                prior5_top50=prior5_top50,
                per_stock_stats=per_stock_stats,
            )
            picks_out.append(pick)
        except Exception as e:
            log.exception("build_pick failed for %s: %s", p.get("symbol"), e)
            continue

    # Latest available date — useful for the frontend to show "viewing
    # historical date" badges when the user has navigated to a past day.
    latest_signal_date = available_signal_dates[0] if available_signal_dates else None
    is_latest = (signal_date == latest_signal_date)

    # Strategy-level per-trade win rate (the new headline number, same on
    # every card) — pulled once from the locked falcon-top-10 persona.
    strategy_wr, strategy_n_trades = _strategy_win_rate()

    # Locked rules — one source of truth, read from PERSONA_CONFIGS via _locked_rules().
    lr = _locked_rules()

    return {
        "signal_date":      signal_date,
        "entry_date":       entry_date,
        "next_trading_day": entry_date,
        "universe":         universe,
        "sector":           sector,
        "validation_status": "pending",  # 09:30 IST gate not yet wired (Phase 2c)
        "validated_at":     None,
        "picks":            picks_out,
        "available_signal_dates": available_signal_dates,
        "latest_signal_date":     latest_signal_date,
        "is_latest":              is_latest,
        # NEW: strategy-level metrics (same on every card)
        "strategy_win_rate_pct":  round(strategy_wr, 1),
        "strategy_n_trades":      strategy_n_trades,
        "hold_days":              lr["time_horizon_d"],
        "standard_position_rs":   lr["position_rs"],
        "smaller_position_rs":    lr["smaller_rs"],
    }


# ════════════════════════════════════════════════════════════════════════
# Step 1: fetch raw picks for the chosen universe
# ════════════════════════════════════════════════════════════════════════

_UNIVERSE_WHERE_CLAUSE: Dict[str, str] = {
    "all500":    "",
    "nifty50":   "AND s.symbol IN (SELECT symbol FROM universe_master WHERE in_nifty50  = 1 AND is_active = 1)",
    "nifty100":  "AND s.symbol IN (SELECT symbol FROM universe_master WHERE in_nifty100 = 1 AND is_active = 1)",
    "nifty200":  "AND s.symbol IN (SELECT symbol FROM universe_master WHERE in_nifty200 = 1 AND is_active = 1)",
    # F&O — using nifty200+active as the effective F&O set today (per notes column).
    # Replaced with a real in_fno flag when one is added.
    "fno":       "AND s.symbol IN (SELECT symbol FROM universe_master WHERE in_nifty200 = 1 AND is_active = 1)",
}


def _fetch_raw_picks(
    prod_con: sqlite3.Connection,
    signal_date: str,
    universe: str,
    sector: Optional[str],
    top_n: int,
) -> List[Dict[str, Any]]:
    """Top-N picks for the day, ranked by avg_lift = score / n_fires.

    Falcon Top 20 ranker is avg_lift (quality-per-fire), NOT raw sum_lift.
    Filters by universe membership + sector name when given, AND by the
    persona's min_fires threshold (F1, 2026-05-24).
    """
    where_universe = _UNIVERSE_WHERE_CLAUSE.get(universe, "")
    where_sector = ""
    # F1 — read min_fires from PERSONA_CONFIGS so the live endpoint enforces
    # the same gate the backtest does. Previously only n_fires > 0 was checked,
    # which silently allowed weak-confluence picks on thin-signal days where
    # the live endpoint diverged from the persona simulator's parity tests.
    # Lazy import to match the pattern used by _locked_rules() / _per_stock_persona_stats.
    from .persona_simulator import PERSONA_CONFIGS
    min_fires = int(PERSONA_CONFIGS["falcon-top-10"]["run_cfg"].min_fires)

    params: List[Any] = [signal_date, min_fires]
    if sector:
        where_sector = "AND s.sector = ?"
        params.append(sector)
    params.append(top_n)

    sql = f"""
        SELECT s.id, s.signal_date, s.entry_date, s.symbol, s.sector,
               s.n_fires, s.score, s.close_at_signal, s.avg_value_60d,
               s.fired_pattern_ids, s.sample_rules
        FROM falcon_signals_live s
        WHERE s.signal_date = ?
          AND s.n_fires >= ?
          {where_universe}
          {where_sector}
        ORDER BY (s.score * 1.0 / s.n_fires) DESC
        LIMIT ?
    """
    rows = prod_con.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


# ════════════════════════════════════════════════════════════════════════
# Step 2: load taxonomy for all firing patterns (one batch query)
# ════════════════════════════════════════════════════════════════════════

def _load_full_taxonomy(
    prod_con: sqlite3.Connection,
) -> Dict[int, Dict[str, Any]]:
    """Load ALL 865 promoted patterns (with rule_json) in one query.

    Used by the request-time pattern-replay path that gives Bucket 1 the
    true full firing distribution (vs. the 5-cap in falcon_signals_live).
    Cheap (~50ms, one round-trip).
    """
    sql = """
        SELECT pattern_id, regime, classification, target, oos_hit_rate,
               lift_pp, base_rate_pct, blurb, english, rule_text, rule_json
        FROM falcon_pattern_taxonomy
    """
    return {r["pattern_id"]: dict(r) for r in prod_con.execute(sql).fetchall()}


def _evaluate_all_patterns_for_stock(
    prod_con: sqlite3.Connection,
    symbol: str,
    signal_date: str,
    taxonomy: Dict[int, Dict[str, Any]],
) -> List[int]:
    """Evaluate all 865 patterns against this stock's feature row for
    signal_date. Returns the full list of pattern IDs that fire (NOT
    capped at 5 like falcon_signals_live.fired_pattern_ids).
    """
    # Pull this stock's single feature row for signal_date
    feat_cols = sorted(_FEATURE_COLS_NUMERIC)
    cols_select = ", ".join(feat_cols)
    row = prod_con.execute(
        f"SELECT {cols_select} FROM falcon_features "
        f"WHERE symbol = ? AND trade_date = ?",
        (symbol, signal_date),
    ).fetchone()
    if not row:
        return []

    # Convert to a dict {col: value} with NaN for None
    feat_vals: Dict[str, float] = {}
    for c in feat_cols:
        v = row[c]
        feat_vals[c] = float(v) if v is not None else float("nan")

    firing: List[int] = []
    for pid, tax in taxonomy.items():
        rule_json = tax.get("rule_json")
        if not rule_json:
            continue
        try:
            conds = json.loads(rule_json)
        except Exception:
            continue
        ok = True
        for cond in conds:
            if not isinstance(cond, list) or len(cond) < 3:
                ok = False; break
            col, op, val = cond[0], cond[1], cond[2]
            v = feat_vals.get(col)
            if v is None or math.isnan(v):
                ok = False; break
            try:
                thr = float(val)
            except Exception:
                ok = False; break
            if   op == "<=": passes = (v <= thr)
            elif op == "<":  passes = (v <  thr)
            elif op == ">=": passes = (v >= thr)
            elif op == ">":  passes = (v >  thr)
            elif op == "==": passes = (v == thr)
            else:            ok = False; break
            if not passes:
                ok = False; break
        if ok:
            firing.append(pid)
    return firing


def _load_taxonomy(
    prod_con: sqlite3.Connection,
    pattern_ids: set[int],
) -> Dict[int, Dict[str, Any]]:
    """Return {pattern_id: row_dict} for the given pattern IDs.

    Includes rule_json so the pattern_narrator can rewrite each pattern's
    machine `english` (which still reads like a formula) into prose. See
    2026-05-23 operator feedback: "explanation should always be user
    friendly — show me the why, the evidence, the context, not the rule".
    """
    pids = list(pattern_ids)
    placeholders = ",".join("?" * len(pids))
    sql = f"""
        SELECT pattern_id, regime, classification, target, oos_hit_rate,
               lift_pp, base_rate_pct, blurb, english, rule_text, rule_json
        FROM falcon_pattern_taxonomy
        WHERE pattern_id IN ({placeholders})
    """
    return {r["pattern_id"]: dict(r) for r in prod_con.execute(sql, pids).fetchall()}


# ════════════════════════════════════════════════════════════════════════
# Step 3: pre-compute sector stats for today (shared across all picks)
# ════════════════════════════════════════════════════════════════════════

def _compute_sector_stats(
    prod_con: sqlite3.Connection,
    signal_date: str,
) -> Dict[str, Dict[str, Any]]:
    """Per-sector aggregates for today + 5 trading days ago, plus the
    'in top 50' peer-concentration count per sector.

    Returns: {sector_name: {
        rank, rank_5d_ago, rotation, n_firing, peer_count_in_top50,
        avg_score, sum_score
    }}

    Rank uses sector-wise SUM(score) of firing stocks (sector with the
    deepest concentration ranks #1). FIX 3 from corrected XLSX: when
    rs_market_20d is null on signal_date (pipeline lag), fall back to the
    most recent date with valid signal data when ranking — done by simply
    using sum(score) on signals_live which exists for any date we have
    a signal row for.

    rank_5d_ago: same query on the signal_date 5 trading days earlier
    (MAX(signal_date) WHERE signal_date < date(signal_date, '-5 days')).
    """
    # Helper: rank sectors by sum_score on a given signal_date
    def _ranks_on(d: str) -> Dict[str, Dict[str, Any]]:
        rows = prod_con.execute("""
            SELECT sector, COUNT(*) n_firing,
                   SUM(score) sum_score, AVG(score) avg_score
            FROM falcon_signals_live
            WHERE signal_date = ?
              AND sector IS NOT NULL
            GROUP BY sector
            ORDER BY sum_score DESC
        """, (d,)).fetchall()
        out: Dict[str, Dict[str, Any]] = {}
        for rank_pos, r in enumerate(rows, start=1):
            out[r["sector"]] = {
                "rank":      rank_pos,
                "n_firing":  int(r["n_firing"]),
                "avg_score": float(r["avg_score"] or 0),
                "sum_score": float(r["sum_score"] or 0),
            }
        return out

    today = _ranks_on(signal_date)

    # Find a comparable date ~5 trading days back. We use 5 distinct
    # signal_dates strictly before today; the engine emits Mon-Fri so
    # 5 distinct signal_dates ≈ one calendar week.
    prior_row = prod_con.execute("""
        SELECT signal_date FROM (
            SELECT DISTINCT signal_date FROM falcon_signals_live
             WHERE signal_date < ?
             ORDER BY signal_date DESC LIMIT 5
        ) ORDER BY signal_date ASC LIMIT 1
    """, (signal_date,)).fetchone()
    prior_date = prior_row[0] if prior_row else None
    prior = _ranks_on(prior_date) if prior_date else {}

    # Peer count in today's top 50 per sector
    peer_rows = prod_con.execute("""
        SELECT sector, COUNT(*) n
        FROM (
            SELECT sector
            FROM falcon_signals_live
            WHERE signal_date = ? AND n_fires > 0
            ORDER BY (score * 1.0 / n_fires) DESC
            LIMIT 50
        )
        GROUP BY sector
    """, (signal_date,)).fetchall()
    peers_top50 = {r["sector"]: int(r["n"]) for r in peer_rows}

    total_sectors_today = len(today) or 1
    for sec, d in today.items():
        rank_now = d["rank"]
        rank_prior = prior.get(sec, {}).get("rank", 0)
        # rotation = how many positions the sector improved over 5d
        # (positive = moved up = rotating in, negative = rotating out)
        if rank_prior:
            rotation = rank_prior - rank_now
        else:
            rotation = 0
        d["rank_5d_ago"]          = rank_prior or None
        d["rotation_positions"]   = rotation
        d["peer_count_in_top50"]  = peers_top50.get(sec, 0)
        d["total_sectors"]        = total_sectors_today
    return today


def _prior5_top50_symbols(
    prod_con: sqlite3.Connection,
    signal_date: str,
) -> set[str]:
    """Symbols that appeared in the top-50 of ANY of the prior 5 distinct
    signal dates. Used to compute the fresh_entry flag — a stock NOT in this
    set is "fresh" (first time in the top 50 in a week).
    """
    rows = prod_con.execute("""
        SELECT DISTINCT s.symbol
        FROM falcon_signals_live s
        WHERE s.signal_date IN (
            SELECT signal_date FROM (
                SELECT DISTINCT signal_date FROM falcon_signals_live
                 WHERE signal_date < ? ORDER BY signal_date DESC LIMIT 5
            )
        )
        AND s.n_fires > 0
        AND ROWID IN (
            SELECT s2.ROWID FROM falcon_signals_live s2
             WHERE s2.signal_date = s.signal_date AND s2.n_fires > 0
             ORDER BY (s2.score * 1.0 / s2.n_fires) DESC LIMIT 50
        )
    """, (signal_date,)).fetchall()
    # Above SQL is sub-optimal under SQLite (the correlated subquery), fall
    # back to a simpler approach: pull top-50 per prior date in Python.
    if rows:
        return {r["symbol"] for r in rows}
    # Fallback path (more reliable)
    prior_dates = [r["signal_date"] for r in prod_con.execute("""
        SELECT DISTINCT signal_date FROM falcon_signals_live
         WHERE signal_date < ? ORDER BY signal_date DESC LIMIT 5
    """, (signal_date,)).fetchall()]
    out: set[str] = set()
    for d in prior_dates:
        for r in prod_con.execute("""
            SELECT symbol FROM falcon_signals_live
             WHERE signal_date = ? AND n_fires > 0
             ORDER BY (score * 1.0 / n_fires) DESC LIMIT 50
        """, (d,)):
            out.add(r["symbol"])
    return out


def _sector_20d_return_pct(
    prod_con: sqlite3.Connection,
    sector: str,
    signal_date: str,
) -> Optional[float]:
    """Average 20-day price return of all stocks in a sector, as of signal_date.

    Computed from ohlc_daily. Returns None if data is missing.
    """
    try:
        row = prod_con.execute("""
            WITH sector_syms AS (
                SELECT DISTINCT symbol FROM falcon_sectors WHERE sector = ?
                UNION
                SELECT DISTINCT symbol FROM universe_master WHERE sector = ?
            ),
            today_close AS (
                SELECT o.symbol, o.close AS close_today
                FROM ohlc_daily o
                INNER JOIN sector_syms s ON o.symbol = s.symbol
                WHERE o.trade_date = ?
            ),
            prior_close AS (
                SELECT o.symbol, o.close AS close_prior
                FROM ohlc_daily o
                INNER JOIN sector_syms s ON o.symbol = s.symbol
                WHERE o.trade_date = (
                    SELECT MAX(trade_date) FROM ohlc_daily
                    WHERE trade_date <= date(?, '-30 days')
                )
            )
            SELECT AVG((tc.close_today / pc.close_prior - 1) * 100.0) AS ret_pct
            FROM today_close tc
            JOIN prior_close pc ON tc.symbol = pc.symbol
            WHERE pc.close_prior > 0
        """, (sector, sector, signal_date, signal_date)).fetchone()
        return float(row[0]) if row and row[0] is not None else None
    except Exception as e:
        log.debug("sector_20d_return failed for %s: %s", sector, e)
        return None


# ════════════════════════════════════════════════════════════════════════
# Step 4: per-pick — assemble all 3 buckets
# ════════════════════════════════════════════════════════════════════════

def _build_pick(
    prod_con: sqlite3.Connection,
    rnd_con:  sqlite3.Connection,
    rank: int,
    raw: Dict[str, Any],
    taxonomy: Dict[int, Dict[str, Any]],
    sector_stats: Dict[str, Dict[str, Any]],
    signal_date: str,
    signal_history_days: int = 0,
    prior5_top50: Optional[set[str]] = None,
    per_stock_stats: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Compose ONE pick with all 3 buckets + action + risk derivation +
    cross-bucket warning flags (capitulation_warning, falling_knife,
    fresh_entry) per the locked Point 8 spec."""
    sym = raw["symbol"]
    sec = raw["sector"] or "Other"

    try:
        sample_rules: List[Dict[str, Any]] = json.loads(raw["sample_rules"]) or []
    except Exception:
        sample_rules = []

    # FULL pid_list — re-evaluate ALL 865 patterns against this stock's
    # feature row for signal_date. The persisted fired_pattern_ids in
    # falcon_signals_live is capped at 5, so we can't trust it for the
    # regime breakdown / synthesis. Cost ~5-10ms per stock.
    pid_list = _evaluate_all_patterns_for_stock(prod_con, sym, signal_date, taxonomy)

    # Sanity: if the re-evaluation comes back empty (e.g. missing features),
    # fall back to whatever IDs were persisted so the card still renders.
    if not pid_list:
        try:
            pid_list = json.loads(raw["fired_pattern_ids"]) or []
        except Exception:
            pid_list = []

    # Bucket 2 first so Bucket 1 can use the lifetime baseline for its
    # synthesised "X pp above lifetime" framing.
    bucket2 = _build_bucket2(prod_con, rnd_con, sym, pid_list, signal_date, taxonomy)
    bucket1 = _build_bucket1(pid_list, sample_rules, taxonomy, raw["n_fires"], raw["score"],
                              stock_baseline_pct=bucket2.get("lifetime_baseline_pct"))
    bucket3 = _build_bucket3(prod_con, sym, sec, signal_date, sector_stats)
    # Bucket 1 v3 returns dominant_regime instead of a patterns array.
    signal_type = _derive_signal_type_from_regime(bucket1.get("dominant_regime"))

    # ── Cross-bucket signals + flags ───────────────────────────────────
    cap_share = _cap_share(pid_list, taxonomy)
    day_ret   = _day_return_pct(prod_con, sym, signal_date)
    last_similar = bucket2.get("last_similar")

    # New rating + position-sizing replaces the LOW/MEDIUM/HIGH risk label.
    rp = _derive_rating_and_position(
        bucket1, bucket2,
        cap_share_pct=cap_share,
        last_similar=last_similar,
        entry_price=raw.get("close_at_signal"),
    )

    # Action block uses the persona-locked 7-day hold + position size from rp.
    # hold_days omitted → _derive_action reads it from _locked_rules() itself.
    action = _derive_action(
        close_at_signal=raw.get("close_at_signal"),
        position_size_rs=rp["position_size_rs"],
        shares_recommended=rp["shares_recommended"],
        smaller_position=rp["smaller_position"],
    )

    # Flags retained (consumed by some downstream surfaces + the Excel export).
    # Do NOT render risk_level in the UX — it's gone per spec.
    flags: Dict[str, Any] = {
        "capitulation_warning": cap_share > 30.0,
        "falling_knife":        (cap_share > 50.0 and (day_ret or 0) < 0 and bucket3.get("sector_rank", 0) > 10),
        "fresh_entry":          (prior5_top50 is not None and sym not in prior5_top50),
        "cap_share_pct":        round(cap_share, 1),
        "day_return_pct":       round(day_ret, 2) if day_ret is not None else None,
    }

    # Per-stock historical track record from the locked backtest — the
    # NEW headline for the "Historical track record" section. Per-stock,
    # not strategy-wide. With the -7% stop and 7-day hold already applied,
    # which means worst_loss_pct is near -7% (not the misleading 20-day MAE).
    per_stock_bt = (per_stock_stats or {}).get(sym) or {
        "n_trades": 0, "n_wins": 0, "n_losses": 0,
        "win_rate_pct": None, "avg_win_pct": None, "avg_loss_pct": None,
        "best_win_pct": None, "worst_loss_pct": None,
        "first_trade_date": None, "last_trade_date": None,
    }

    return {
        "rank":                     rank,
        "symbol":                   sym,
        "sector":                   sec,
        "signal_type":              signal_type,
        # Plain-English UX fields (replace risk_level/warning)
        "rating":                   rp["rating"],
        "position_size_rs":         rp["position_size_rs"],
        "shares_recommended":       rp["shares_recommended"],
        "smaller_position":         rp["smaller_position"],
        "smaller_position_reasons": rp["smaller_position_reasons"],
        "weaker_history_message":   rp["weaker_history_message"],
        # NEW: per-stock historical track record from the persona backtest
        "per_stock_backtest":       per_stock_bt,
        # Back-compat fields (UI doesn't read these but other consumers might)
        "risk_level":               "—",
        "warning":                  rp["weaker_history_message"],
        "bucket1":                  bucket1,
        "bucket2":                  bucket2,
        "bucket3":                  bucket3,
        "action":                   action,
        "flags":                    flags,
    }


def _cap_share(pid_list: List[int], taxonomy: Dict[int, Dict[str, Any]]) -> float:
    """Share (0-100) of firing patterns whose regime is 'capitulation'."""
    if not pid_list:
        return 0.0
    cap = 0
    seen = 0
    for pid in pid_list:
        tax = taxonomy.get(pid)
        if not tax:
            continue
        seen += 1
        if str(tax.get("regime") or "").lower() == "capitulation":
            cap += 1
    if seen == 0:
        return 0.0
    return cap / seen * 100.0


def _day_return_pct(
    prod_con: sqlite3.Connection, symbol: str, signal_date: str,
) -> Optional[float]:
    """(close/prev_close - 1) * 100 on signal_date. None when not available."""
    try:
        row = prod_con.execute("""
            WITH today AS (
                SELECT close FROM ohlc_daily WHERE symbol = ? AND trade_date = ?
            ),
            prior AS (
                SELECT close FROM ohlc_daily WHERE symbol = ?
                  AND trade_date < ? ORDER BY trade_date DESC LIMIT 1
            )
            SELECT (today.close * 1.0 / prior.close - 1) * 100 AS day_ret
              FROM today CROSS JOIN prior
        """, (symbol, signal_date, symbol, signal_date)).fetchone()
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def _narrative_for(tax: Dict[str, Any], pid: int) -> str:
    """Translate one taxonomy row into 2-3 sentences of institutional prose.

    Falls back to the DB's `english` ONLY if narration fails. The DB column
    contains machine-translated rules ("5-day return <= 4.11 AND ...") which
    are NOT what the user should read — the moat is the explanation, so the
    narrator is the canonical path.
    """
    try:
        text = _narrate_pattern(
            rule_json     = tax.get("rule_json"),
            regime        = tax.get("regime"),
            lift_pp       = tax.get("lift_pp"),
            oos_hit_rate  = tax.get("oos_hit_rate"),
            base_rate_pct = tax.get("base_rate_pct"),
            target        = tax.get("target"),
            fallback_blurb= tax.get("blurb"),
        )
        if text and len(text) > 20:
            return text
    except Exception as e:
        log.warning("narrator failed for pid=%s: %s", pid, e)
    # Last-ditch fallback so the UI never breaks
    return tax.get("english") or tax.get("rule_text") or "Rule details unavailable."


def _today_weighted_hit_rate(
    pid_list: List[int],
    taxonomy: Dict[int, Dict[str, Any]],
) -> Optional[float]:
    """Weighted average of each firing pattern's oos_hit_rate, weighted by lift_pp.

    FIX 4 (from Falcon Top 10 corrected XLSX): return None — NOT 0.0 — when
    sum_lift_for_hr <= 0. Low-fire defensive stocks (MARICO, NESTLE) used to
    produce today_hr=0 which artificially inflated kaynes_delta vs lifetime.
    Frontend must render None as "N/A".
    """
    sum_lift = 0.0
    sum_lhr  = 0.0
    for pid in pid_list:
        tax = taxonomy.get(pid)
        if not tax:
            continue
        lift = tax.get("lift_pp")
        hr   = tax.get("oos_hit_rate")
        if lift is None or hr is None or float(lift) <= 0:
            continue
        sum_lift += float(lift)
        sum_lhr  += float(hr) * float(lift)
    if sum_lift <= 0:
        return None
    return round(sum_lhr / sum_lift, 2)


def _build_bucket1(
    pid_list: List[int],
    sample_rules: List[Dict[str, Any]],
    taxonomy: Dict[int, Dict[str, Any]],
    n_fires_total: int,
    score_total: float,
    stock_baseline_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """Bucket 1 v3 — ONE synthesised paragraph + regime breakdown.

    Replaces the v2 "top-3 patterns with per-pattern narratives" approach,
    which surfaced three lookalike paragraphs for stocks whose top patterns
    were variants of the same rule (the "Closing near top of weekly range
    × 3" complaint, 2026-05-23).

    The new shape answers "Why is the AI picking this stock?" with one
    research-note paragraph and a confluence breakdown by regime, the way
    an institutional analyst would write it.
    """
    today_hr = _today_weighted_hit_rate(pid_list, taxonomy)

    # ── 1. Dominant regime — by total positive-lift contribution ─────────
    # F5 (2026-05-24): previously computed a full per-regime breakdown
    # (count, sum_lift_pp, avg_lift_pp, weighted_hit_rate, headline blurb)
    # and shipped it on the response. The 2026-05-24 UX rewrite stopped
    # rendering it but the ~50ms/stock compute kept running. The synthesis
    # narrator only needs dominant_regime, so collapse to that one number.
    regime_lift: Dict[str, float] = {}
    for pid in pid_list:
        tax = taxonomy.get(pid)
        if not tax:
            continue
        regime = (tax.get("regime") or "other").lower()
        regime_lift[regime] = regime_lift.get(regime, 0.0) + max(float(tax.get("lift_pp") or 0), 0)
    dominant_regime = max(regime_lift.items(), key=lambda x: x[1])[0] if regime_lift else "other"

    # ── 3. Deduplicate conditions across ALL firing patterns ─────────────
    # For each column, keep the strongest threshold the stock cleared.
    # `>`/`>=` ops: take the MAX threshold (tighter pass).
    # `<=`/`<` ops: take the MIN threshold (tighter cap).
    by_col_gt: Dict[str, float] = {}
    by_col_le: Dict[str, float] = {}
    for pid in pid_list:
        tax = taxonomy.get(pid)
        if not tax:
            continue
        rule_json = tax.get("rule_json")
        if not rule_json:
            continue
        try:
            conds = json.loads(rule_json)
        except Exception:
            continue
        for cond in conds:
            if not isinstance(cond, list) or len(cond) < 3:
                continue
            col, op, val = cond[0], cond[1], cond[2]
            try:
                v = float(val)
            except Exception:
                continue
            if op in (">", ">="):
                cur = by_col_gt.get(col)
                if cur is None or v > cur:
                    by_col_gt[col] = v
            elif op in ("<=", "<"):
                cur = by_col_le.get(col)
                if cur is None or v < cur:
                    by_col_le[col] = v

    deduped_conditions: List[List[Any]] = []
    for col, v in by_col_gt.items():
        deduped_conditions.append([col, ">", v])
    for col, v in by_col_le.items():
        deduped_conditions.append([col, "<=", v])

    # ── 4. Single synthesised narrative ──────────────────────────────────
    synthesis = _narrate_synthesis(
        deduped_conditions=deduped_conditions,
        dominant_regime=dominant_regime,
        weighted_hit_rate=today_hr,
        stock_baseline_pct=stock_baseline_pct,
        n_patterns=int(n_fires_total),
        target="hit_10pc_20d",
    )

    # Bucket 1 payload — only fields the new UX renders. The per-regime
    # `breakdown` array and `combined_lift_pp` aggregate that the older
    # "confluence breakdown" UI consumed have been removed entirely (F5,
    # 2026-05-24). `dominant_regime` stays — it's used internally by
    # _derive_signal_type_from_regime + the synthesis closer.
    return {
        "total_fires_today":       int(n_fires_total),
        "pattern_count":           int(n_fires_total),
        "today_weighted_hit_rate": today_hr,
        "dominant_regime":         dominant_regime,
        "synthesis":               synthesis,
    }


def _build_bucket2(
    prod_con: sqlite3.Connection,
    rnd_con:  sqlite3.Connection,
    symbol: str,
    pid_list: List[int],
    signal_date: str,
    taxonomy: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    """Bucket 2 v2 — REAL historical evidence via pattern replay.

    Locked spec (2026-05-23, Point 8): do NOT use falcon_signals_live for
    history — that table is only ~13 days deep and produces the +0.0%/0.0%
    bug. Instead:
      1. For each fired pattern (pid_list), load rule_json from taxonomy
      2. Replay each rule against falcon_features[symbol, >= 2021-01-01]
      3. Collect (pattern_id, fire_date) tuples where rule fires
      4. JOIN to falcon_outcomes (RND) for realized ret_20d / hit_10pc_20d / mae_20d
      5. Aggregate: n_historical_fires, historical_hit_rate, avg_win_pct,
         avg_loss_pct, worst_drawdown_pct, first/last fire dates

    Reference impl: universe_engine .../research_2_signal_store.py
    (already does this for all 865 patterns × 478 stocks → 16.6M fires).

    Performance: ~10 patterns × ~800-1200 feature rows per stock = sub-100ms
    of numpy work per stock. 10 stocks per request = under 1s total.
    """
    # ── Step 1 — load this stock's features back to 2021 in one query ─────
    feat_cols_to_select = "trade_date, " + ", ".join(sorted(_FEATURE_COLS_NUMERIC))
    feat_rows = prod_con.execute(
        f"SELECT {feat_cols_to_select} FROM falcon_features "
        f"WHERE symbol = ? AND trade_date >= ? AND trade_date <= ? "
        f"ORDER BY trade_date",
        (symbol, _BUCKET2_LOOKBACK_START, signal_date),
    ).fetchall()

    if not feat_rows:
        # No feature history at all — return the lifetime baseline as the
        # honest single-stat answer
        baseline = _stock_lifetime_baseline(rnd_con, symbol)
        return _bucket2_empty(baseline)

    # Build numpy column arrays for vectorized rule eval
    n_dates = len(feat_rows)
    dates_arr   = np.array([r["trade_date"] for r in feat_rows], dtype=object)
    col_arrays: Dict[str, np.ndarray] = {}
    for col in _FEATURE_COLS_NUMERIC:
        col_arrays[col] = np.array(
            [(r[col] if r[col] is not None else np.nan) for r in feat_rows],
            dtype=np.float64,
        )

    # ── Step 2 — for each fired pattern, evaluate its rule_json ──────────
    fire_dates_per_pattern: Dict[int, List[str]] = {}
    all_fire_dates_set: set[str] = set()
    for pid in pid_list:
        tax = taxonomy.get(pid)
        if not tax:
            continue
        rule_json = tax.get("rule_json")
        if not rule_json:
            continue
        try:
            conds = json.loads(rule_json)
        except Exception:
            continue

        mask = np.ones(n_dates, dtype=bool)
        ok = True
        for cond in conds:
            if not isinstance(cond, list) or len(cond) < 3:
                ok = False; break
            col, op, val = cond[0], cond[1], cond[2]
            if col not in col_arrays:
                ok = False; break
            arr = col_arrays[col]
            try:
                v = float(val)
            except Exception:
                ok = False; break
            if   op == "<=": m = (arr <= v)
            elif op == "<":  m = (arr <  v)
            elif op == ">=": m = (arr >= v)
            elif op == ">":  m = (arr >  v)
            elif op == "==": m = (arr == v)
            else:
                ok = False; break
            mask &= m & ~np.isnan(arr)
        if not ok:
            continue

        fire_idxs = np.where(mask)[0]
        if len(fire_idxs) == 0:
            continue
        fires_for_p = [dates_arr[i] for i in fire_idxs]
        fire_dates_per_pattern[pid] = fires_for_p
        all_fire_dates_set.update(fires_for_p)

    # ── Step 3 — load outcomes for the union of fire dates ───────────────
    n_historical_fires = sum(len(v) for v in fire_dates_per_pattern.values())
    baseline = _stock_lifetime_baseline(rnd_con, symbol)

    if not all_fire_dates_set:
        # Patterns never fired historically on this stock — surface the
        # lifetime baseline as the only honest answer.
        return _bucket2_baseline_only(baseline, n_historical_fires=0)

    all_fire_dates = sorted(all_fire_dates_set)
    placeholders = ",".join("?" * len(all_fire_dates))
    out_rows = rnd_con.execute(
        f"SELECT trade_date, ret_20d, hit_10pc_20d, mae_20d, mfe_20d "
        f"FROM falcon_outcomes "
        f"WHERE symbol = ? AND trade_date IN ({placeholders})",
        [symbol, *all_fire_dates],
    ).fetchall()
    outcomes_by_date: Dict[str, Dict[str, Any]] = {
        r["trade_date"]: dict(r) for r in out_rows
    }

    # ── Step 4 — aggregate across ALL fires (one row per (pattern, date)) ─
    n_with_outcome = 0
    n_hits         = 0
    wins:   List[float] = []
    losses: List[float] = []
    mae_obs: List[float] = []
    all_pairs: List[Tuple[int, str]] = []
    for pid, dates in fire_dates_per_pattern.items():
        for d in dates:
            all_pairs.append((pid, d))
            oc = outcomes_by_date.get(d)
            if not oc:
                continue
            ret_20d = oc.get("ret_20d")
            hit_10  = oc.get("hit_10pc_20d")
            mae_20  = oc.get("mae_20d")
            if ret_20d is None:
                continue
            n_with_outcome += 1
            if hit_10:
                n_hits += 1
            r = float(ret_20d)
            if r > 0:    wins.append(r)
            elif r < 0:  losses.append(r)
            if mae_20 is not None:
                mae_obs.append(float(mae_20))

    if n_with_outcome == 0:
        # Patterns fired but every fire is too recent for ret_20d to have
        # realised. Fall back to the lifetime baseline; do NOT print 0%.
        return _bucket2_baseline_only(baseline, n_historical_fires=n_historical_fires)

    historical_hit_rate = (n_hits / n_with_outcome * 100.0)
    avg_win_pct  = sum(wins) / len(wins) if wins else None
    avg_loss_pct = sum(losses) / len(losses) if losses else None
    worst_dd_pct = min(mae_obs) if mae_obs else None
    kaynes_delta = historical_hit_rate - baseline

    # Last similar setup: pick the most recent (pattern_id, fire_date) with an outcome
    last_similar = None
    for pid, d in sorted(all_pairs, key=lambda x: x[1], reverse=True):
        oc = outcomes_by_date.get(d)
        if oc and oc.get("ret_20d") is not None:
            last_similar = {
                "date":            d,
                "return_pct":      round(float(oc["ret_20d"]), 2),
                "days_to_outcome": 20,
                "pattern_id":      int(pid),
            }
            break

    return {
        "n_historical_fires":    n_historical_fires,
        "n_with_outcome":        n_with_outcome,
        "hit_rate_pct":          round(historical_hit_rate, 2),
        "avg_win_pct":           round(avg_win_pct, 2)  if avg_win_pct  is not None else None,
        "avg_loss_pct":          round(avg_loss_pct, 2) if avg_loss_pct is not None else None,
        "worst_drawdown_pct":    round(worst_dd_pct, 2) if worst_dd_pct is not None else None,
        "lifetime_baseline_pct": round(baseline, 2),
        "kaynes_delta_pp":       round(kaynes_delta, 2),
        "last_similar":          last_similar,
        "first_fire_date":       min(all_fire_dates),
        "last_fire_date":        max(all_fire_dates),
        "lookback_start":        _BUCKET2_LOOKBACK_START,
    }


def _bucket2_empty(baseline: float) -> Dict[str, Any]:
    """No feature history for this stock at all (very rare)."""
    b = round(baseline, 2)
    return {
        "n_historical_fires":    0,
        "n_with_outcome":        0,
        "hit_rate_pct":          b,
        "avg_win_pct":           None,
        "avg_loss_pct":          None,
        "worst_drawdown_pct":    None,
        "lifetime_baseline_pct": b,
        "kaynes_delta_pp":       0.0,
        "last_similar":          None,
        "first_fire_date":       None,
        "last_fire_date":        None,
        "lookback_start":        _BUCKET2_LOOKBACK_START,
    }


def _bucket2_baseline_only(baseline: float, n_historical_fires: int) -> Dict[str, Any]:
    """Patterns either never fired, or fires too recent for ret_20d to have
    realised. Surface lifetime baseline as the honest answer; do NOT print
    avg_win/avg_loss as 0% (frontend renders null as em-dash)."""
    b = round(baseline, 2)
    return {
        "n_historical_fires":    n_historical_fires,
        "n_with_outcome":        0,
        "hit_rate_pct":          b,
        "avg_win_pct":           None,
        "avg_loss_pct":          None,
        "worst_drawdown_pct":    None,
        "lifetime_baseline_pct": b,
        "kaynes_delta_pp":       0.0,
        "last_similar":          None,
        "first_fire_date":       None,
        "last_fire_date":        None,
        "lookback_start":        _BUCKET2_LOOKBACK_START,
    }


def _stock_lifetime_baseline(rnd_con: sqlite3.Connection, symbol: str) -> float:
    """% of all-time falcon_outcomes rows for this symbol that hit_10pc_20d."""
    try:
        r = rnd_con.execute(
            "SELECT AVG(hit_10pc_20d) * 100 FROM falcon_outcomes WHERE symbol = ? AND hit_10pc_20d IS NOT NULL",
            (symbol,)
        ).fetchone()
        return float(r[0]) if r and r[0] is not None else 0.0
    except Exception:
        return 0.0


def _load_outcomes(
    rnd_con: sqlite3.Connection,
    symbol: str,
    dates: List[str],
) -> Dict[str, Dict[str, Any]]:
    """Bulk-load outcomes for a symbol across many dates → {date: row}."""
    if not dates:
        return {}
    placeholders = ",".join("?" * len(dates))
    sql = f"""
        SELECT trade_date, ret_5d, ret_10d, ret_20d, ret_30d,
               hit_10pc_20d, hit_15pc_20d, failure_10d
        FROM falcon_outcomes
        WHERE symbol = ? AND trade_date IN ({placeholders})
    """
    rows = rnd_con.execute(sql, [symbol, *dates]).fetchall()
    return {r["trade_date"]: dict(r) for r in rows}


def _build_bucket3(
    prod_con: sqlite3.Connection,
    symbol: str,
    sector: str,
    signal_date: str,
    sector_stats: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Bucket 3 v2 — sector context with 5d-ago rank, rotation magnitude,
    peer concentration in top 50, and tailwind/neutral/headwind verdict.

    Locked spec (Point 8): rotation_direction = sector_rank_5d_ago -
    sector_rank_today (positive = rotating in). Verdict: tailwind <=7,
    neutral 8-13, headwind >=14. peer_count_in_top50 includes THIS stock
    (the spec wording says "5 other Capital Goods stocks in top 50" but
    the underlying count is the full sector slice; UI subtracts 1 if needed).
    """
    st = sector_stats.get(sector, {})
    sector_rank        = int(st.get("rank", 0))
    sector_rank_5d_ago = st.get("rank_5d_ago")
    rotation_positions = int(st.get("rotation_positions", 0))
    n_firing           = int(st.get("n_firing", 0))
    n_peers            = max(0, n_firing - 1)        # exclude this stock itself
    total_sectors      = int(st.get("total_sectors", len(sector_stats) or 1))
    peer_count_top50   = int(st.get("peer_count_in_top50", 0))

    sector_20d = _sector_20d_return_pct(prod_con, sector, signal_date)
    if sector_20d is None:
        sector_20d = 0.0

    # Rotation direction from the actual 5d rank delta (NOT a 20d-return proxy)
    if rotation_positions >= 2:
        rotation_dir = "inflow"
    elif rotation_positions <= -2:
        rotation_dir = "outflow"
    else:
        rotation_dir = "neutral"

    # Plain-English narrative — same string the UX renders verbatim under
    # "What the sector is doing". No "rotation", "verdict", "tailwind" jargon.
    # F6 (2026-05-24): removed the tailwind/neutral/headwind `verdict` tag —
    # frontend never rendered it after the 2026-05-24 UX rewrite, and the
    # narrative paragraph already conveys the same information in plain
    # English. No downstream consumers (grep'd backend + frontend clean).
    narrative = _plain_sector_narrative(
        sector_name=sector,
        rank_today=sector_rank,
        rank_5d_ago=sector_rank_5d_ago,
        total_sectors=total_sectors,
        rotation_positions=rotation_positions,
        peer_count_top50=peer_count_top50,
        sector_20d_pct=sector_20d,
    )

    return {
        "sector_name":             sector,
        "sector_rank":             sector_rank,
        "sector_rank_5d_ago":      sector_rank_5d_ago,
        "rotation_positions":      rotation_positions,  # +N = improved N ranks
        "total_sectors":           total_sectors,
        "n_peers_firing":          n_peers,
        "peer_count_in_top50":     peer_count_top50,
        "sector_20d_return_pct":   round(sector_20d, 2),
        "rotation_direction":      rotation_dir,
        "rotation_sessions_of_10": 7 if rotation_dir == "inflow" else (3 if rotation_dir == "outflow" else 5),
        # Plain-English narrative the UX renders directly
        "narrative":               narrative,
    }


def _plain_sector_narrative(
    sector_name: str,
    rank_today:  int,
    rank_5d_ago: Optional[int],
    total_sectors: int,
    rotation_positions: int,
    peer_count_top50: int,
    sector_20d_pct: float,
) -> str:
    """Compose the plain-English sector paragraph. Example output:
       "Financial Services is one of the hottest sectors right now —
        moved from rank 7 to rank 3 in the last week. 4 other Financial
        Services names are also in today's picks. Money is flowing into
        this sector."
    """
    name = sector_name or "This sector"
    # Sentence 1 — sector strength + 5d rotation
    if rank_today == 0:
        s1 = f"{name} has no clear ranking in today's picks."
    elif rank_today <= 3:
        if rank_5d_ago and rotation_positions >= 2:
            s1 = (f"{name} is one of the hottest sectors right now — "
                  f"moved from rank {rank_5d_ago} to rank {rank_today} in the last week.")
        elif rank_5d_ago and rotation_positions <= -2:
            s1 = (f"{name} is still leading today (rank {rank_today}), "
                  f"but slipped from rank {rank_5d_ago} over the last week.")
        else:
            s1 = f"{name} is one of the strongest sectors today (rank {rank_today})."
    elif rank_today <= 7:
        if rank_5d_ago and rotation_positions >= 2:
            s1 = (f"{name} has been climbing the rankings — up from "
                  f"rank {rank_5d_ago} to {rank_today} this week.")
        else:
            s1 = f"{name} is mid-pack today (rank {rank_today} of {total_sectors})."
    elif rank_today <= 13:
        s1 = f"{name} is mid-pack today (rank {rank_today} of {total_sectors})."
    else:
        s1 = f"{name} is lagging today (rank {rank_today} of {total_sectors})."

    # Sentence 2 — peer concentration
    n_other = max(0, peer_count_top50 - 1)   # exclude the stock itself
    if n_other >= 3:
        s2 = f"{n_other} other {name} names are also in today's picks."
    elif n_other >= 1:
        s2 = (f"{n_other} other {name} name{'s' if n_other > 1 else ''} "
              f"{'are' if n_other > 1 else 'is'} also in today's picks.")
    else:
        s2 = f"This is the only {name} name in today's picks — it's flying solo."

    # Sentence 3 — money flow (only when there's a real signal)
    if rotation_positions >= 2:
        s3 = "Money is flowing into this sector."
    elif rotation_positions <= -2:
        s3 = "Money has been leaving this sector."
    elif sector_20d_pct >= 3:
        s3 = "The sector has had a strong run over the past month."
    elif sector_20d_pct <= -3:
        s3 = "The sector has been under pressure over the past month."
    else:
        s3 = ""

    return " ".join([p for p in (s1, s2, s3) if p])


# ════════════════════════════════════════════════════════════════════════
# Helpers: signal type + risk + action
# ════════════════════════════════════════════════════════════════════════

# Falcon Top 10 LOCKED exit rules — read from persona_simulator.PERSONA_CONFIGS
# at request time so the explainer never has its own copy of init_stop /
# hold_days / position size that could silently diverge from the backtest.
#
# F2 (2026-05-24): eliminates the duplicate source-of-truth that previously
# lived as _STOP_LOSS_PCT, _TIME_HORIZON_D, _STANDARD_POSITION_RS, and
# _SMALLER_POSITION_RS module constants. Edit PERSONA_CONFIGS["falcon-top-10"]
# ["run_cfg"] and the UX picks up new values on the next request — no second
# place to remember to update.
def _locked_rules() -> Dict[str, Any]:
    """Lazy read of locked Falcon Top 10 rules. Lookup is one dict access
    well under 1µs; not worth caching at this scale."""
    from .persona_simulator import PERSONA_CONFIGS
    cfg = PERSONA_CONFIGS["falcon-top-10"]["run_cfg"]
    return {
        "stop_loss_pct":   round(cfg.init_stop * 100),     # -0.07 → -7
        "time_horizon_d":  int(cfg.hold_days),              # 7
        "position_rs":     int(cfg.fixed_per_trade),        # 50000
        # Smaller-position is a UX-side rule (half-size), NOT a persona knob.
        # Derived here so it stays in lockstep with position_rs.
        "smaller_rs":      int(cfg.fixed_per_trade / 2),    # 25000
    }


def _derive_signal_type(patterns: List[Dict[str, Any]]) -> str:
    """Legacy: map dominant pattern regime → human signal type."""
    if not patterns:
        return "Compression"
    regimes = [p.get("regime", "other") for p in patterns]
    dominant = max(set(regimes), key=regimes.count) if regimes else "other"
    return _derive_signal_type_from_regime(dominant)


def _derive_signal_type_from_regime(regime: Optional[str]) -> str:
    """Bucket 1 v3 path — regime is already derived in _build_bucket1."""
    if not regime:
        return "Compression"
    return {
        "breakout":       "Breakout",
        "momentum":       "Momentum",
        "compression":    "Compression",
        "reversal":       "Reversal",
        "mean_reversion": "Mean-Reversion",
        "capitulation":   "Capitulation",
    }.get(regime.lower(), "Compression")


# Thresholds for the smaller-position flag (NOT a filter — the validated
# top 10 is never re-ranked; weaker picks stay at their engine rank with
# an honest plain-English note). Tuned 2026-05-24 after smoke test:
#   - DD threshold raised from -30 → -45 (most quality stocks have hit -30
#     somewhere in 5y; -45 is the real "this can hurt" threshold)
#   - recent_loss requires the stock's OVERALL pattern track record to
#     ALSO be weak (kaynes <= 0); otherwise a single negative recent
#     example in a generally-down month would flag every pick.
_KAYNES_WEAK_THRESHOLD     = -10.0  # today HR ≥10pp below lifetime baseline → weak
_KAYNES_NEUTRAL_THRESHOLD  =   0.0  # used to gate the single-recent-loss trigger
_DD_SEVERE_THRESHOLD       = -45.0  # worst dip during a trade ≤ -45% → severe
_RECENT_LOSS_THRESHOLD     = -5.0   # last_similar must lose >5% to count
_OVERSOLD_SHARE_THRESHOLD  = 30.0   # ≥30% of firing patterns are oversold-bounce


def _derive_rating_and_position(
    bucket1: Dict[str, Any],
    bucket2: Dict[str, Any],
    cap_share_pct: float,
    last_similar:  Optional[Dict[str, Any]],
    entry_price:   Optional[float],
) -> Dict[str, Any]:
    """Replaces _derive_risk. Output is a 'go' framing, never a 'no':

      rating: "Strong Buy" | "Good Setup"   — both mean BUY, conviction differs
      position_size_rs: 50000 (standard) | 25000 (smaller, when qualified)
      shares_recommended: floor(position_size_rs / entry_price)
      smaller_position_reasons: ['weak_history','severe_dd','recent_loss','oversold']
      weaker_history_message: one plain-English line to render above 'What to do'

    Per the locked UX spec (2026-05-24): never filter out of top 10. Flag
    with smaller position. Recent losing example MUST drive the smaller
    flag — not sit decoratively next to a 'Strong Buy' badge.
    """
    n_with_outcome = int(bucket2.get("n_with_outcome", 0) or 0)
    kaynes         = float(bucket2.get("kaynes_delta_pp")   or 0)
    worst_dd       = bucket2.get("worst_drawdown_pct")
    has_oversold   = cap_share_pct >= _OVERSOLD_SHARE_THRESHOLD

    reasons: List[str] = []

    # Trigger 1 — weaker track record on this stock
    if n_with_outcome > 0 and kaynes <= _KAYNES_WEAK_THRESHOLD:
        reasons.append("weak_history")

    # Trigger 2 — large historical drawdown during the trade window
    if worst_dd is not None and float(worst_dd) <= _DD_SEVERE_THRESHOLD:
        reasons.append("severe_dd")

    # Trigger 3 — most recent similar setup lost MEANINGFUL money (>5%),
    # AND the stock's overall pattern track record is at-or-below baseline.
    # Two conditions because a strong-track-record stock having a single
    # negative recent example in a down month shouldn't demote the whole
    # pick; the spec says "the recent real EXAMPLES lost money" — plural,
    # implying a pattern, not one data point. We approximate plural with
    # "recent loss AND overall track record is weak".
    if last_similar and last_similar.get("return_pct") is not None:
        rp_val = float(last_similar["return_pct"])
        if rp_val <= _RECENT_LOSS_THRESHOLD and kaynes <= _KAYNES_NEUTRAL_THRESHOLD:
            reasons.append("recent_loss")

    # Trigger 4 — oversold-bounce setup (heavy capitulation regime)
    if has_oversold:
        reasons.append("oversold")

    lr = _locked_rules()
    smaller = bool(reasons)
    pos_rs = lr["smaller_rs"] if smaller else lr["position_rs"]
    shares = int(pos_rs // entry_price) if entry_price and entry_price > 0 else 0

    # Edge case: high-priced stock (e.g. POWERINDIA at ₹35,555) where the
    # smaller-position ₹25,000 can't buy a single share. Bump to standard
    # size in that case — better to have 1 share at full size than zero.
    # The heads-up message still renders so the user sees the caution.
    if smaller and shares == 0 and entry_price and entry_price > 0:
        pos_rs = lr["position_rs"]
        shares = int(pos_rs // entry_price)

    # Compose the user-facing weaker-history line. Recent-loss takes priority
    # (the spec is explicit that loss must DRIVE the flag).
    msg: Optional[str] = None
    if "recent_loss" in reasons:
        rp = float(last_similar["return_pct"])
        ld = last_similar.get("date", "")
        msg = (f"Heads up: the most recent similar setup on this stock "
               f"({ld}) lost {abs(rp):.1f}% — using a smaller position.")
    elif "weak_history" in reasons:
        msg = ("Heads up: weaker track record on this stock lately — "
               "using a smaller position.")
    elif "severe_dd" in reasons and worst_dd is not None:
        msg = (f"Heads up: this stock has dipped as much as {abs(float(worst_dd)):.0f}% "
               f"during similar trades historically — using a smaller position.")
    elif "oversold" in reasons:
        msg = ("Note: this one's bouncing off a sharp fall — it can snap back "
               "fast or keep falling. Smaller position.")

    # Rating: "Strong Buy" only when no smaller-position triggers AND track
    # record looks at-or-above lifetime baseline. Otherwise "Good Setup" —
    # still a buy (we're in the top 10), just less conviction.
    is_strong_buy = (not smaller) and (
        n_with_outcome == 0    # bootstrap case — assume Strong Buy until outcomes land
        or kaynes >= 0
    )
    rating = "Strong Buy" if is_strong_buy else "Good Setup"

    return {
        "rating":                   rating,
        "position_size_rs":         pos_rs,
        "shares_recommended":       shares,
        "smaller_position":         smaller,
        "smaller_position_reasons": reasons,
        "weaker_history_message":   msg,
    }


def _derive_action(
    close_at_signal: Optional[float],
    position_size_rs: int,
    shares_recommended: int,
    smaller_position: bool,
    hold_days: Optional[int] = None,
) -> Dict[str, Any]:
    """Plain-English action block. All locked Falcon Top 10 rules
    (-7% stop, 7-day hold) come from PERSONA_CONFIGS via _locked_rules().

    Output strings are user-ready — no jargon, no "watch above", no "MAE".
    """
    lr = _locked_rules()
    stop_pct = lr["stop_loss_pct"]
    if hold_days is None:
        hold_days = lr["time_horizon_d"]

    px = float(close_at_signal) if close_at_signal else 0.0
    stop = round(px * (1 + stop_pct / 100), 2) if px else 0.0

    if px:
        entry_text = f"Buy at tomorrow's open (~₹{px:,.2f})"
    else:
        entry_text = "Buy at tomorrow's open (price levels pending)"

    if shares_recommended > 0:
        smaller_tag = "smaller — see note above" if smaller_position else ""
        position_text = (
            f"Position size: ₹{position_size_rs:,}  (~{shares_recommended:,} shares)"
            + (f"  ·  {smaller_tag}" if smaller_tag else "")
        )
    else:
        position_text = f"Position size: ₹{position_size_rs:,}"

    stop_text = f"Exit if it drops to ₹{stop:,.2f} ({stop_pct}% stop)" if px \
                 else f"{stop_pct}% stop from entry"

    hold_text = f"We'll hold for up to {hold_days} trading days"

    return {
        # New plain-English fields (preferred by frontend)
        "entry_text":         entry_text,
        "position_text":      position_text,
        "stop_text":          stop_text,
        "hold_text":          hold_text,
        # Raw fields kept for downstream consumers + the per-trade calculator
        "entry_price_rs":     px if px else None,
        "stop_loss_rs":       stop,
        "stop_loss_pct":      stop_pct,
        "time_horizon_days":  hold_days,
        # Back-compat — old field that some legacy renderers may still read
        "entry_label":        entry_text,
    }


# ════════════════════════════════════════════════════════════════════════
# Empty response shape (for "no data yet" path)
# ════════════════════════════════════════════════════════════════════════

def _empty_response(
    signal_date: Optional[str],
    entry_date: Optional[str],
    universe: str,
    sector: Optional[str],
    available_signal_dates: Optional[List[str]] = None,
) -> Dict[str, Any]:
    dates = available_signal_dates or []
    return {
        "signal_date":      signal_date,
        "entry_date":       entry_date,
        "next_trading_day": entry_date,
        "universe":         universe,
        "sector":           sector,
        "validation_status": "pre_market",
        "validated_at":     None,
        "picks":            [],
        "available_signal_dates": dates,
        "latest_signal_date":     dates[0] if dates else None,
        "is_latest":              True if not signal_date else (signal_date == (dates[0] if dates else None)),
    }
