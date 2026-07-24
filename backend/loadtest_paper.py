"""
Paper-fire LOAD TEST — fire N paper sessions at once, measure the real order path.

Answers "can 100-200 accounts fire at 09:15 without the system choking?" by
exercising the ACTUAL fire path (session build → 8-parallel placement → DB
writes → tick-driver arm), in PAPER mode (dry_run → no real orders), with a
STUBBED price source.

WHY STUBBED PRICES: every paper session here uses the operator's single Kite key,
so real quote calls would just measure Kite's per-key throttle — the opposite of
what we want. In production each user has their OWN key, so quotes are naturally
distributed. Stubbing isolates OUR concurrency: DB write contention (the SQLite
single-writer wall) + the thread-per-session model. Gated by
KANIDA_LOADTEST_STUB_PRICE, which the broker adapter honours.

SAFETY
  * PAPER only (mode='paper' → dry_run → NO real broker orders).
  * All sessions owned by user_id 'loadtest-<i>' (no real user uses that prefix),
    so cleanup is exact and can never touch real data.
  * Cleanup runs in a finally: it deletes every loadtest session + its positions/
    events/claims, and reports what it removed. Re-runnable.
  * Admin-gated route only.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

log = logging.getLogger(__name__)

USER_PREFIX = "loadtest-"
_CLEANUP_TABLES = [
    ("autotrade_positions", "session_id"),
    ("autotrade_order_events", "session_id"),
    ("autotrade_alerts", "session_id"),
    ("autotrade_slippage", "session_id"),
    # NOTE: portfolio_positions is a CO-TRADING table (no session_id column) and
    # is NOT written by the autotrade fire path — deliberately excluded so cleanup
    # doesn't error on a missing column.
    ("falcon_position_state", "session_id"),
    ("autotrade_session_account_allocations", "session_id"),
    ("autotrade_sessions", "session_id"),
]


def _cfg_dict(capital: float, top_n: int, product: str) -> Dict[str, Any]:
    # Known-valid minimal config (mirrors the autotrade tests). symbols=None →
    # the session sizes the current Falcon top_n picks from falcon_signals_live.
    return {
        "total_allocated_capital": float(capital),
        "top_n_stocks": int(top_n),
        "sizing_mode": "equal",
        "kill_switch_enabled": False,
        "kill_switch_pct": 0.02,
        "order_product": product,
    }


def _cleanup(session_ids: List[str]) -> Dict[str, int]:
    """Delete everything the load test created. Best-effort, reports counts."""
    from falcon.db import falcon_conn
    deleted: Dict[str, int] = {}
    if not session_ids:
        # Also sweep any stragglers from a previous crashed run.
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT session_id FROM autotrade_sessions WHERE user_id LIKE ?",
                (USER_PREFIX + "%",)).fetchall()
            session_ids = [r[0] for r in rows]
    if not session_ids:
        return deleted
    qmarks = ",".join("?" * len(session_ids))
    with falcon_conn() as con:
        for tbl, col in _CLEANUP_TABLES:
            try:
                cur = con.execute(
                    f"DELETE FROM {tbl} WHERE {col} IN ({qmarks})", session_ids)
                deleted[tbl] = cur.rowcount
            except Exception as e:
                deleted[tbl] = -1
                log.warning("loadtest cleanup %s failed: %s", tbl, e)
        # entry-claims are keyed 'entry:<session_id>'
        try:
            keys = [f"entry:{s}" for s in session_ids] + [f"driver:{s}" for s in session_ids]
            qm2 = ",".join("?" * len(keys))
            cur = con.execute(
                f"DELETE FROM autotrade_claims WHERE claim_key IN ({qm2})", keys)
            deleted["autotrade_claims"] = cur.rowcount
        except Exception as e:
            log.warning("loadtest cleanup claims failed: %s", e)
        con.commit()
    return deleted


def run(n: int = 100, capital: float = 1e7, top_n: int = 10,
        product: str = "CNC", stub_price: float = 100.0) -> Dict[str, Any]:
    """Create N paper sessions and FIRE them concurrently. Measures + cleans up."""
    n = max(1, min(500, int(n)))
    os.environ["KANIDA_LOADTEST_STUB_PRICE"] = str(stub_price)
    from autotrade.config import TradingSessionConfig
    from autotrade.session import TradingSession

    created: List[Any] = []
    session_ids: List[str] = []
    result: Dict[str, Any] = {"n_requested": n, "capital": capital,
                              "top_n": top_n, "product": product}
    try:
        # 1. create sessions (sequential — this is the setup, not the measured part)
        t_create0 = time.time()
        for i in range(n):
            cfg = TradingSessionConfig.from_dict(_cfg_dict(capital, top_n, product))
            cfg.validate()
            s = TradingSession.create(cfg, mode="paper", user_id=f"{USER_PREFIX}{i}")
            created.append(s)
            session_ids.append(s.session_id)
        result["create_sec"] = round(time.time() - t_create0, 2)

        # 2. FIRE all N concurrently (the measured part — thread per session,
        #    exactly the production model).
        latencies: List[float] = [0.0] * n
        statuses: List[str] = [""] * n
        errors: List[str] = [None] * n

        def _fire(idx: int, sess: Any) -> None:
            t0 = time.time()
            try:
                res = asyncio.run(sess._fire_entries())
                statuses[idx] = str(res.get("status", "?")) if isinstance(res, dict) else "ok"
                latencies[idx] = time.time() - t0
            except Exception as e:
                statuses[idx] = "EXC"
                errors[idx] = f"{type(e).__name__}: {str(e)[:150]}"
                latencies[idx] = time.time() - t0

        wall0 = time.time()
        with ThreadPoolExecutor(max_workers=n) as ex:
            futs = [ex.submit(_fire, i, s) for i, s in enumerate(created)]
            for f in futs:
                f.result()
        wall = time.time() - wall0

        # 3. measure
        oks = sum(1 for s in statuses if s not in ("EXC",))
        excs = [e for e in errors if e]
        lat_sorted = sorted(latencies)
        def pct(p): return round(lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * p))], 3)
        # positions actually written (proof orders were placed)
        from falcon.db import falcon_conn
        with falcon_conn() as con:
            qm = ",".join("?" * len(session_ids))
            npos = con.execute(
                f"SELECT COUNT(*) FROM autotrade_positions WHERE session_id IN ({qm})",
                session_ids).fetchone()[0]

        result.update({
            "fired": n,
            "ok": oks,
            "exceptions": len(excs),
            "wall_sec": round(wall, 2),
            "fires_per_sec": round(n / wall, 1) if wall > 0 else 0,
            "latency_p50_sec": pct(0.50),
            "latency_p95_sec": pct(0.95),
            "latency_max_sec": round(max(latencies), 3),
            "positions_written": npos,
            "sample_errors": excs[:5],
            "status_breakdown": {s: statuses.count(s) for s in set(statuses)},
        })
    finally:
        result["cleanup_deleted"] = _cleanup(session_ids)
        os.environ.pop("KANIDA_LOADTEST_STUB_PRICE", None)
    return result
