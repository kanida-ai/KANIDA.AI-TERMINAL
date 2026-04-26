"""
Run outcome-first pattern learning on kanida_quant.db.
Reads ohlc_daily, learns patterns, writes to pattern_library + snapshot_runs.

Usage:
    python engine/jobs/run_learning.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from engine.outcome_first.learner import learn_outcome_patterns
from engine.outcome_first.live import rank_live_outcome_opportunities as find_live_opportunities

DB_PATH     = ROOT / "data" / "db" / "kanida_quant.db"
CONFIG_PATH = ROOT / "config" / "prototype_config.json"

N_WORKERS = min(os.cpu_count() or 4, 8)


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_ohlcv(con: sqlite3.Connection) -> dict[tuple[str, str], list[dict]]:
    rows = con.execute("""
        SELECT market, ticker, trade_date, open, high, low, close, volume
        FROM ohlc_daily
        WHERE quality_flag != 'rejected'
        ORDER BY market, ticker, trade_date
    """).fetchall()
    data: dict[tuple[str, str], list[dict]] = {}
    for market, ticker, trade_date, open_, high, low, close, volume in rows:
        key = (market, ticker)
        if key not in data:
            data[key] = []
        data[key].append({
            "market": market,
            "ticker": ticker,
            "trade_date": trade_date,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        })
    return data


def _learn_chunk(args: tuple[dict, dict]) -> list[dict]:
    """Worker function — runs in a separate process."""
    ohlcv_chunk, config = args
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from engine.outcome_first.learner import learn_outcome_patterns as _learn
    return _learn(ohlcv_chunk, config)


def learn_parallel(ohlcv: dict, config: dict) -> list[dict]:
    items = list(ohlcv.items())
    chunk_size = max(1, (len(items) + N_WORKERS - 1) // N_WORKERS)
    chunks = [
        dict(items[i : i + chunk_size])
        for i in range(0, len(items), chunk_size)
    ]
    print(f"  Parallel learning: {len(items)} stocks, {N_WORKERS} workers, {len(chunks)} chunks", flush=True)

    all_patterns: list[dict] = []
    with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
        futures = {executor.submit(_learn_chunk, (chunk, config)): idx
                   for idx, chunk in enumerate(chunks)}
        for future in as_completed(futures):
            idx = futures[future]
            chunk_patterns = future.result()
            all_patterns.extend(chunk_patterns)
            print(f"  Chunk {idx+1}/{len(chunks)} done — {len(chunk_patterns)} patterns "
                  f"(total so far: {len(all_patterns)})", flush=True)

    return all_patterns


def write_patterns(con: sqlite3.Connection, run_id: int, patterns: list[dict]) -> int:
    if not patterns:
        return 0
    sql = """
        INSERT INTO pattern_library (
            market, ticker, direction, target_move, forward_window,
            pattern_size, behavior_pattern, occurrences, hits,
            baseline_probability, raw_probability, trusted_probability,
            display_probability, probability_ci_low, probability_ci_high,
            credibility, lift, avg_forward_return, recent_probability,
            stability, opportunity_score, decay_flag, tier,
            first_seen_date, last_seen_date, updated_at
        ) VALUES (
            :market, :ticker, :direction, :target_move, :forward_window,
            :pattern_size, :behavior_pattern, :occurrences, :hits,
            :baseline_probability, :raw_probability, :trusted_probability,
            :display_probability, :probability_ci_low, :probability_ci_high,
            :credibility, :lift, :avg_forward_return, :recent_probability,
            :stability, :opportunity_score, :decay_flag, :tier,
            :first_seen_date, :last_seen_date, datetime('now')
        )
        ON CONFLICT(market, ticker, direction, target_move, forward_window, behavior_pattern)
        DO UPDATE SET
            occurrences=excluded.occurrences, hits=excluded.hits,
            raw_probability=excluded.raw_probability,
            trusted_probability=excluded.trusted_probability,
            display_probability=excluded.display_probability,
            probability_ci_low=excluded.probability_ci_low,
            probability_ci_high=excluded.probability_ci_high,
            credibility=excluded.credibility, lift=excluded.lift,
            avg_forward_return=excluded.avg_forward_return,
            recent_probability=excluded.recent_probability,
            stability=excluded.stability,
            opportunity_score=excluded.opportunity_score,
            decay_flag=excluded.decay_flag, tier=excluded.tier,
            last_seen_date=excluded.last_seen_date,
            updated_at=datetime('now')
    """
    now = datetime.now().strftime("%Y-%m-%d")
    payload = []
    for p in patterns:
        row = dict(p)
        row.setdefault("first_seen_date", now)
        row.setdefault("last_seen_date", now)
        payload.append(row)
    con.executemany(sql, payload)
    return len(payload)


def write_live_opportunities(con: sqlite3.Connection, run_id: int, live: list[dict]) -> int:
    if not live:
        return 0
    con.execute("DELETE FROM live_opportunities WHERE snapshot_run_id = (SELECT MAX(id) FROM snapshot_runs WHERE status='success')")
    sql = """
        INSERT INTO live_opportunities (
            snapshot_run_id, market, ticker, direction, target_move, forward_window,
            behavior_pattern, occurrences, hits, display_probability, credibility,
            lift, opportunity_score, decision_score, tier, latest_date,
            current_close, current_behavior, current_atoms, similarity,
            setup_summary, decay_flag
        ) VALUES (
            :snapshot_run_id, :market, :ticker, :direction, :target_move, :forward_window,
            :behavior_pattern, :occurrences, :hits, :display_probability, :credibility,
            :lift, :opportunity_score, :decision_score, :tier, :latest_date,
            :current_close, :current_behavior, :current_atoms, :similarity,
            :setup_summary, :decay_flag
        )
    """
    payload = [{**row, "snapshot_run_id": run_id} for row in live]
    con.executemany(sql, payload)
    return len(payload)


def main() -> None:
    config = load_config()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    print("=" * 55, flush=True)
    print("KANIDA.AI — Outcome-First Pattern Learning", flush=True)
    print(f"DB: {DB_PATH}", flush=True)
    print(f"Workers: {N_WORKERS}", flush=True)
    print("=" * 55, flush=True)

    cur = con.execute(
        "INSERT INTO snapshot_runs(run_type, status, started_at) VALUES (?, ?, ?)",
        ["outcome_first", "running", datetime.now().isoformat(timespec="seconds")]
    )
    run_id = cur.lastrowid
    con.commit()

    try:
        print("\nLoading OHLCV data...", flush=True)
        ohlcv = load_ohlcv(con)
        print(f"  {len(ohlcv)} stocks loaded", flush=True)

        print("\nLearning patterns (parallel)...", flush=True)
        t0 = datetime.now()
        patterns = learn_parallel(ohlcv, config)
        elapsed = (datetime.now() - t0).total_seconds()
        print(f"  {len(patterns)} patterns learned in {elapsed:.0f}s", flush=True)

        print("\nFinding live opportunities...", flush=True)
        live = find_live_opportunities(patterns, ohlcv, config)
        print(f"  {len(live)} live opportunities", flush=True)

        print("\nWriting to DB...", flush=True)
        n_patterns = write_patterns(con, run_id, patterns)
        n_live     = write_live_opportunities(con, run_id, live)
        con.execute("""
            UPDATE snapshot_runs
            SET status='success', finished_at=?, learned_patterns=?, live_opportunities=?, tickers_processed=?
            WHERE id=?
        """, [datetime.now().isoformat(timespec="seconds"), n_patterns, n_live, len(ohlcv), run_id])
        con.commit()

        print("\n" + "=" * 55, flush=True)
        print("RESULTS", flush=True)
        print("=" * 55, flush=True)
        by_stock: dict[str, dict] = {}
        for p in patterns:
            k = f"{p['market']}:{p['ticker']}"
            if k not in by_stock:
                by_stock[k] = {"total": 0, "high_conviction": 0, "medium": 0}
            by_stock[k]["total"] += 1
            if p["tier"] == "high_conviction":
                by_stock[k]["high_conviction"] += 1
            elif p["tier"] == "medium":
                by_stock[k]["medium"] += 1

        for stock, s in sorted(by_stock.items()):
            print(f"  {stock:<20}  {s['total']:>4} patterns  "
                  f"high_conviction={s['high_conviction']}  medium={s['medium']}", flush=True)

        print(f"\n  Total patterns : {n_patterns}", flush=True)
        print(f"  Live signals   : {n_live}", flush=True)
        if live:
            print("\n  Top 5 live opportunities:", flush=True)
            live_sorted = sorted(live, key=lambda x: -x.get("decision_score", 0))
            for opp in live_sorted[:5]:
                print(f"    {opp['market']}:{opp['ticker']:<12} "
                      f"{opp['direction']:<6} {opp['target_move']*100:.0f}% "
                      f"prob={opp['display_probability']:.0%} "
                      f"score={opp['decision_score']:.3f} "
                      f"[{opp['tier']}]", flush=True)
        print("\nDone.", flush=True)

    except Exception as e:
        con.execute(
            "UPDATE snapshot_runs SET status='failed', finished_at=?, message=? WHERE id=?",
            [datetime.now().isoformat(timespec="seconds"), str(e), run_id]
        )
        con.commit()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
