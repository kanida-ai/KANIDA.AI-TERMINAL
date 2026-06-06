"""
KANIDA.AI Quant — Trade Signal Performance Backtest 2024-2026

Dedup strategy: ONE best trade per (stock, signal_date, direction).
When multiple patterns fire on the same day for the same stock+direction,
we keep the highest opportunity_score match and record how many patterns
competed in the notes (multi_pattern_count).

Datetime convention (NSE 1D data):
  - Signal fires at market close:  signal_date  15:30:00 IST (+05:30)
  - Entry at next market open:     entry_date   09:15:00 IST (+05:30)
  - Signal-to-entry delay is computed as actual calendar minutes.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from engine.outcome_first.features import build_behavior_rows

DB_PATH        = Path(__file__).parent.parent.parent / "data" / "db" / "kanida_quant.db"
BACKTEST_YEARS = {"2024", "2025", "2026"}
MIN_OVERLAP    = 0.65
RR_RATIO       = 2.0

NSE_SIGNAL_TIME = "15:30:00"   # IST — pattern detected at daily close
NSE_ENTRY_TIME  = "09:15:00"   # IST — entry at next day open
TIMEFRAME       = "1D"         # all current data is daily


# ── helpers ───────────────────────────────────────────────────────────────────

def signal_to_entry_mins(signal_date_str: str, entry_date_str: str) -> int:
    """Calendar minutes from signal close to entry open (overnight / weekend gap)."""
    sig_dt = datetime.strptime(f"{signal_date_str[:10]} {NSE_SIGNAL_TIME}", "%Y-%m-%d %H:%M:%S")
    ent_dt = datetime.strptime(f"{entry_date_str[:10]} {NSE_ENTRY_TIME}", "%Y-%m-%d %H:%M:%S")
    delta  = ent_dt - sig_dt
    return max(0, int(delta.total_seconds() // 60))


def classify_signal_type(pattern: str) -> str:
    p = pattern.lower()
    if "breakout_state:breakout" in p or "range_expansion" in p:
        return "Breakout"
    if "up_pullback" in p or "up_continuation" in p:
        return "Trend Continuation"
    if "down_reversal_attempt" in p or "lower_wick_rejection" in p:
        return "Mean Reversion"
    if "volume_dryup" in p or "compressed" in p:
        return "Compression Setup"
    return "AI Pattern"


def classify_bucket(exit_reason, days_to_exit, forward_window, mfe, mae, mpi, post_5d) -> str:
    if exit_reason == "tp":
        speed = days_to_exit / max(1, forward_window)
        cont  = mpi or 0.0
        if speed <= 0.30 and cont >= 0.03 and mae >= -0.02:
            return "turbo"
        if speed <= 0.55 and cont >= 0.01:
            return "super"
        return "standard"
    else:
        if (post_5d or 0.0) > 0.025:
            return "trap"
        return "standard"


def simulate_trade(entry_price: float, forward_bars: list[dict], direction: str, target_move: float) -> dict:
    tp = entry_price * (1 + target_move) if direction == "rally" else entry_price * (1 - target_move)
    sl = entry_price * (1 - target_move / RR_RATIO) if direction == "rally" else entry_price * (1 + target_move / RR_RATIO)

    exit_price  = float(forward_bars[-1]["close"]) if forward_bars else entry_price
    exit_date   = forward_bars[-1]["trade_date"]   if forward_bars else ""
    exit_reason = "timeout"
    days        = len(forward_bars)
    mfe = mae   = 0.0

    for j, bar in enumerate(forward_bars):
        bh = float(bar["high"])
        bl = float(bar["low"])
        if direction == "rally":
            mfe = max(mfe, (bh - entry_price) / entry_price)
            mae = min(mae, (bl - entry_price) / entry_price)
            if bh >= tp:
                exit_price = tp;  exit_date = bar["trade_date"];  exit_reason = "tp";  days = j + 1; break
            if bl <= sl:
                exit_price = sl;  exit_date = bar["trade_date"];  exit_reason = "sl";  days = j + 1; break
        else:
            mfe = max(mfe, (entry_price - bl) / entry_price)
            mae = min(mae, -(bh - entry_price) / entry_price)
            if bl <= tp:
                exit_price = tp;  exit_date = bar["trade_date"];  exit_reason = "tp";  days = j + 1; break
            if bh >= sl:
                exit_price = sl;  exit_date = bar["trade_date"];  exit_reason = "sl";  days = j + 1; break

    return dict(exit_price=exit_price, exit_date=exit_date,
                exit_reason=exit_reason, days=days,
                tp=tp, sl=sl, mfe=mfe, mae=mae)


def post_exit_stats(raw_rows: list[dict], exit_bar_idx: int,
                    direction: str, exit_price: float) -> tuple:
    post = raw_rows[exit_bar_idx + 1 : exit_bar_idx + 6]
    if not post or exit_price <= 0:
        return None, None
    if direction == "rally":
        mpi    = (max(float(b["high"]) for b in post) - exit_price) / exit_price
        post5d = (float(post[-1]["close"]) - exit_price) / exit_price
    else:
        mpi    = (exit_price - min(float(b["low"]) for b in post)) / exit_price
        post5d = (exit_price - float(post[-1]["close"])) / exit_price
    return mpi, post5d


# ── Candidate collector ───────────────────────────────────────────────────────

def collect_candidates(
    market: str, ticker: str,
    ohlcv_rows: list[dict],
    behavior_rows: list[dict],
    raw_date_idx: dict[str, int],
    stock_patterns: list[dict],
) -> list[dict]:
    """
    For every (signal_date, direction) pair, collect all pattern matches
    as candidates. Returns raw candidate list with opportunity_score for dedup.
    """
    candidates: list[dict] = []

    for pat in stock_patterns:
        pat_id   = str(pat.get("id") or pat.get("behavior_pattern", ""))
        pat_str  = str(pat.get("behavior_pattern", ""))
        pat_atoms = set(s.strip() for s in pat_str.split("+") if s.strip())
        if not pat_atoms:
            continue
        try:
            target_move    = float(pat["target_move"])
            direction      = str(pat["direction"])
            forward_window = int(pat["forward_window"])
            opp_score      = float(pat.get("opportunity_score") or 0)
        except (KeyError, TypeError, ValueError):
            continue

        for brow in behavior_rows:
            sig_date = str(brow["trade_date"])
            if sig_date[:4] not in BACKTEST_YEARS:
                continue
            live_atoms = set(brow.get("behavior_atoms") or [])
            if not live_atoms:
                continue
            overlap = len(pat_atoms & live_atoms) / len(pat_atoms)
            if overlap < MIN_OVERLAP:
                continue

            raw_sig_idx = raw_date_idx.get(sig_date)
            if raw_sig_idx is None or raw_sig_idx + 1 >= len(ohlcv_rows):
                continue

            candidates.append({
                "market":        market,
                "ticker":        ticker,
                "sig_date":      sig_date,
                "direction":     direction,
                "pat_id":        pat_id,
                "pat_str":       pat_str,
                "target_move":   target_move,
                "forward_window": forward_window,
                "opp_score":     opp_score,
                "overlap":       overlap,
                "raw_sig_idx":   raw_sig_idx,
                "tier":          str(pat.get("tier", "")),
                "credibility":   str(pat.get("credibility", "")),
            })

    return candidates


# ── Dedup: best per (stock, date, direction) ──────────────────────────────────

def dedup_candidates(candidates: list[dict]) -> list[tuple[dict, int]]:
    """
    Group by (ticker, sig_date, direction).
    Keep the highest opportunity_score match. Return (best_candidate, group_size).
    group_size > 1 means MULTI_PATTERN situation.
    """
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for c in candidates:
        key = (c["ticker"], c["sig_date"], c["direction"])
        groups[key].append(c)

    result = []
    for group in groups.values():
        group.sort(key=lambda x: -x["opp_score"])
        result.append((group[0], len(group)))
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def run_backtest() -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    print("Loading OHLCV data...")
    raw = con.execute("""
        SELECT market, ticker, trade_date, open, high, low, close, volume
        FROM ohlc_daily WHERE quality_flag != 'rejected'
        ORDER BY market, ticker, trade_date
    """).fetchall()

    ohlcv_by_stock: dict[tuple, list[dict]] = defaultdict(list)
    for r in raw:
        ohlcv_by_stock[(r["market"], r["ticker"])].append(dict(r))

    print("Loading pattern library...")
    all_pats = [dict(r) for r in con.execute("SELECT * FROM pattern_library").fetchall()]
    pats_by_stock: dict[tuple, list[dict]] = defaultdict(list)
    for p in all_pats:
        pats_by_stock[(p["market"], p["ticker"])].append(p)
    print(f"  {len(all_pats)} patterns across {len(pats_by_stock)} stocks")

    print("Clearing previous backtest trades...")
    con.execute("DELETE FROM trade_log WHERE trade_type='backtest'")
    con.commit()

    all_trades: list[dict] = []

    for (market, ticker), ohlcv_rows in ohlcv_by_stock.items():
        stock_pats = pats_by_stock.get((market, ticker), [])
        if not stock_pats:
            continue

        behavior_rows = build_behavior_rows(ohlcv_rows)
        if not behavior_rows:
            continue

        raw_date_idx = {str(r["trade_date"]): i for i, r in enumerate(ohlcv_rows)}

        # ── Step 1: collect all candidates ───────────────────────────────────
        candidates = collect_candidates(
            market, ticker, ohlcv_rows, behavior_rows, raw_date_idx, stock_pats
        )

        # ── Step 2: deduplicate — 1 best trade per (stock, date, direction) ──
        deduped = dedup_candidates(candidates)

        # ── Step 3: enforce per-direction cooldown after dedup ────────────────
        last_entry_by_dir: dict[str, date | None] = {}
        deduped.sort(key=lambda x: x[0]["sig_date"])

        stock_trades = 0
        for cand, multi_count in deduped:
            direction = cand["direction"]
            cooldown  = max(5, cand["forward_window"] // 2)
            last_ent  = last_entry_by_dir.get(direction)

            try:
                sig_date_obj = date.fromisoformat(cand["sig_date"][:10])
            except ValueError:
                continue

            if last_ent and (sig_date_obj - last_ent).days < cooldown:
                continue

            raw_sig_idx = cand["raw_sig_idx"]
            entry_raw   = ohlcv_rows[raw_sig_idx + 1]
            entry_price = float(entry_raw["open"] or 0)
            entry_date  = str(entry_raw["trade_date"])
            if entry_price <= 0:
                continue

            fwd_bars = ohlcv_rows[raw_sig_idx + 2 : raw_sig_idx + 2 + cand["forward_window"]]
            if not fwd_bars:
                continue

            sim = simulate_trade(entry_price, fwd_bars, direction, cand["target_move"])
            if sim["exit_price"] <= 0:
                continue

            exit_raw_idx = raw_date_idx.get(sim["exit_date"],
                                            raw_sig_idx + 1 + sim["days"])
            mpi, post5d = post_exit_stats(ohlcv_rows, exit_raw_idx, direction, sim["exit_price"])

            pnl = ((sim["exit_price"] - entry_price) / entry_price * 100
                   if direction == "rally"
                   else (entry_price - sim["exit_price"]) / entry_price * 100)

            bucket      = classify_bucket(sim["exit_reason"], sim["days"],
                                          cand["forward_window"], sim["mfe"],
                                          sim["mae"], mpi, post5d)
            signal_type = classify_signal_type(cand["pat_str"])

            # ── Reason code ──────────────────────────────────────────────────
            if multi_count == 1:
                reason_code = "FRESH_SIGNAL"
            elif multi_count <= 3:
                reason_code = f"MULTI_PATTERN_{multi_count}"
            else:
                reason_code = f"MULTI_PATTERN_{multi_count}_BEST_SELECTED"

            # ── DateTime metadata ────────────────────────────────────────────
            sig_mins = signal_to_entry_mins(cand["sig_date"], entry_date)
            # Weekend/holiday gap: normal overnight = 1065 min; Mon entry after Fri signal = 3975 min
            if sig_mins > 2000:
                delay_label = f"{sig_mins // 60}h {sig_mins % 60}m (weekend/holiday gap)"
            else:
                delay_label = f"{sig_mins // 60}h {sig_mins % 60}m (overnight)"

            notes = json.dumps({
                "bucket":               bucket,
                "signal_type":          signal_type,
                "pattern":              cand["pat_str"],
                "overlap":              round(cand["overlap"], 3),
                "year":                 cand["sig_date"][:4],
                "mfe_pct":              round(sim["mfe"] * 100, 2),
                "mae_pct":              round(sim["mae"] * 100, 2),
                "mpi_pct":              round((mpi or 0) * 100, 2),
                "post_5d_pct":          round((post5d or 0) * 100, 2),
                "tier":                 cand["tier"],
                "credibility":          cand["credibility"],
                "timeframe":            TIMEFRAME,
                "signal_time":          f"{cand['sig_date']} {NSE_SIGNAL_TIME} IST",
                "entry_time":           f"{entry_date} {NSE_ENTRY_TIME} IST",
                "signal_to_entry_mins": sig_mins,
                "delay_label":          delay_label,
                "reason_code":          reason_code,
                "multi_pattern_count":  multi_count,
                "opportunity_score":    round(cand["opp_score"], 4),
            })

            all_trades.append({
                "market":            market,
                "ticker":            ticker,
                "trade_type":        "backtest",
                "direction":         "long" if direction == "rally" else "short",
                "signal_date":       cand["sig_date"],
                "entry_date":        entry_date,
                "entry_price":       round(entry_price, 2),
                "target_price":      round(sim["tp"], 2),
                "stop_price":        round(sim["sl"], 2),
                "exit_date":         sim["exit_date"],
                "exit_price":        round(sim["exit_price"], 2),
                "exit_reason":       sim["exit_reason"],
                "days_held":         sim["days"],
                "pnl_pct":           round(pnl, 3),
                "risk_reward_ratio": RR_RATIO,
                "notes":             notes,
            })

            last_entry_by_dir[direction] = date.fromisoformat(entry_date[:10])
            stock_trades += 1

        print(f"  {market}:{ticker:<12} {stock_trades} trades")

    print(f"\nInserting {len(all_trades)} backtest trades into trade_log...")
    sql = """
        INSERT INTO trade_log (
            market, ticker, trade_type, direction, signal_date, entry_date,
            entry_price, target_price, stop_price, exit_date, exit_price,
            exit_reason, days_held, pnl_pct, risk_reward_ratio, notes
        ) VALUES (
            :market, :ticker, :trade_type, :direction, :signal_date, :entry_date,
            :entry_price, :target_price, :stop_price, :exit_date, :exit_price,
            :exit_reason, :days_held, :pnl_pct, :risk_reward_ratio, :notes
        )
    """
    con.executemany(sql, all_trades)
    con.commit()
    con.close()

    # ── summary ───────────────────────────────────────────────────────────────
    total = len(all_trades)
    wins  = sum(1 for t in all_trades if t["exit_reason"] == "tp")
    buckets: dict[str, int] = defaultdict(int)
    by_stock: dict[str, dict] = defaultdict(lambda: {"total": 0, "wins": 0})
    multi_count_total = 0

    for t in all_trades:
        n = json.loads(t["notes"])
        buckets[n["bucket"]] += 1
        by_stock[t["ticker"]]["total"] += 1
        multi_count_total += n.get("multi_pattern_count", 1)
        if t["exit_reason"] == "tp":
            by_stock[t["ticker"]]["wins"] += 1

    avg_multi = multi_count_total / total if total else 0

    print(f"\n{'='*55}")
    print("BACKTEST COMPLETE  (1 best trade per stock/date/direction)")
    print(f"{'='*55}")
    print(f"  Total trades     : {total}")
    if total:
        print(f"  Win rate (TP)    : {wins/total*100:.1f}%  ({wins}/{total})")
        print(f"  Avg patterns/day : {avg_multi:.1f}  (competing patterns per signal)")
    print(f"\n  Bucket breakdown:")
    for b, c in sorted(buckets.items()):
        pct_str = f"({c/total*100:.0f}%)" if total else ""
        print(f"    {b:<12} {c:>5}  {pct_str}")
    print(f"\n  Per stock:")
    for tkr, s in sorted(by_stock.items()):
        wr = s["wins"] / s["total"] * 100 if s["total"] else 0
        print(f"    {tkr:<12} {s['total']:>4} trades  WR={wr:.0f}%")

    return all_trades


if __name__ == "__main__":
    run_backtest()
