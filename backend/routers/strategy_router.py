"""
Strategy Lab router — create, iterate, backtest, and promote trading strategies.

GET  /api/strategies                   list all strategies
POST /api/strategies                   create a new draft strategy
GET  /api/strategies/{id}              get one strategy (with computed backtest summary)
PUT  /api/strategies/{id}              update name / description / params / notes
POST /api/strategies/{id}/compute      compute backtest stats from trade_log → saves to DB
POST /api/strategies/{id}/promote      advance status: draft→sandbox→staging→prod
DELETE /api/strategies/{id}            delete a draft (other statuses cannot be deleted)

Status lifecycle:
    draft  →  sandbox  →  staging  →  prod
                                        ↓
                                    archived   (any status can move to archived)

Only one strategy can be 'prod' at a time. Promoting a strategy to prod
auto-archives the current prod strategy.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))
from db import get_conn

router = APIRouter()

_ADMIN_SECRET = lambda: os.getenv("ADMIN_SECRET", "")

_STATUS_PROGRESSION = {
    "draft":   "sandbox",
    "sandbox": "staging",
    "staging": "prod",
    "prod":    "archived",
}

_DEFAULT_PARAMS = {
    "rr_ratio":        2.0,
    "min_overlap":     0.65,
    "max_hold_days":   21,
    "entry_type":      "smart",
    "directions":      ["rally"],
    "backtest_years":  ["2024", "2025", "2026"],
}

_DEFAULT_UNIVERSE_FILTER = {"sets": ["FNO"], "exchange": "NSE"}


# ── DB bootstrap ───────────────────────────────────────────────────────────────

def _ensure_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            id               TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(8)))),
            name             TEXT NOT NULL,
            description      TEXT,
            version          INTEGER NOT NULL DEFAULT 1,
            status           TEXT NOT NULL DEFAULT 'draft',
            universe_filter  TEXT NOT NULL DEFAULT '{"sets":["FNO"],"exchange":"NSE"}',
            params           TEXT NOT NULL DEFAULT '{}',
            backtest_result  TEXT,
            last_backtest_at TEXT,
            promoted_by      TEXT,
            promoted_at      TEXT,
            created_at       TEXT DEFAULT (datetime('now')),
            notes            TEXT
        )
    """)
    conn.commit()


# ── Serialisation helpers ──────────────────────────────────────────────────────

def _row_to_dict(row) -> dict:
    d = dict(row)
    for field in ("params", "universe_filter", "backtest_result"):
        raw = d.get(field)
        if raw and isinstance(raw, str):
            try:
                d[field] = json.loads(raw)
            except Exception:
                pass
        elif raw is None:
            d[field] = None
    return d


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Pydantic models ────────────────────────────────────────────────────────────

class CreateStrategyRequest(BaseModel):
    name:             str
    description:      Optional[str]  = None
    universe_filter:  Optional[dict] = None   # defaults to FNO/NSE
    params:           Optional[dict] = None   # defaults to _DEFAULT_PARAMS
    notes:            Optional[str]  = None


class UpdateStrategyRequest(BaseModel):
    name:            Optional[str]  = None
    description:     Optional[str]  = None
    universe_filter: Optional[dict] = None
    params:          Optional[dict] = None
    notes:           Optional[str]  = None


class PromoteRequest(BaseModel):
    secret:     str
    promoted_by: str = "admin"


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/strategies")
def list_strategies():
    """List all strategies newest-first."""
    with get_conn() as conn:
        _ensure_table(conn)
        rows = conn.execute(
            "SELECT * FROM strategies ORDER BY created_at DESC"
        ).fetchall()
    return {"strategies": [_row_to_dict(r) for r in rows]}


@router.post("/strategies")
def create_strategy(body: CreateStrategyRequest):
    """Create a new strategy in draft status."""
    with get_conn() as conn:
        _ensure_table(conn)

        existing = conn.execute(
            "SELECT id FROM strategies WHERE name=? AND status != 'archived'",
            (body.name,)
        ).fetchone()
        if existing:
            raise HTTPException(
                status_code=409,
                detail=f"Active strategy named '{body.name}' already exists. Choose a different name or archive the existing one.",
            )

        params_json   = json.dumps(body.params or _DEFAULT_PARAMS)
        filter_json   = json.dumps(body.universe_filter or _DEFAULT_UNIVERSE_FILTER)
        created_at    = _now_iso()

        cur = conn.execute("""
            INSERT INTO strategies
                (name, description, version, status, universe_filter, params, created_at, notes)
            VALUES (?, ?, 1, 'draft', ?, ?, ?, ?)
        """, (body.name, body.description, filter_json, params_json, created_at, body.notes))

        # Fetch the generated id
        new_id = conn.execute(
            "SELECT id FROM strategies WHERE name=? AND created_at=? ORDER BY rowid DESC LIMIT 1",
            (body.name, created_at),
        ).fetchone()
        conn.commit()

    strategy_id = new_id[0] if new_id else None
    return {"status": "created", "id": strategy_id, "name": body.name}


@router.get("/strategies/{strategy_id}")
def get_strategy(strategy_id: str):
    """Get a single strategy by id."""
    with get_conn() as conn:
        _ensure_table(conn)
        row = conn.execute(
            "SELECT * FROM strategies WHERE id=?", (strategy_id,)
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Strategy {strategy_id} not found.")
    return _row_to_dict(row)


@router.put("/strategies/{strategy_id}")
def update_strategy(strategy_id: str, body: UpdateStrategyRequest):
    """Update mutable fields on a strategy."""
    with get_conn() as conn:
        _ensure_table(conn)

        row = conn.execute(
            "SELECT status FROM strategies WHERE id=?", (strategy_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Strategy {strategy_id} not found.")

        if dict(row)["status"] == "prod":
            raise HTTPException(
                status_code=400,
                detail="Cannot edit a prod strategy. Promote it to archived first, then create a new version.",
            )

        updates: list[str] = []
        params: list[Any]  = []

        if body.name is not None:
            updates.append("name=?"); params.append(body.name)
        if body.description is not None:
            updates.append("description=?"); params.append(body.description)
        if body.universe_filter is not None:
            updates.append("universe_filter=?"); params.append(json.dumps(body.universe_filter))
        if body.params is not None:
            updates.append("params=?"); params.append(json.dumps(body.params))
        if body.notes is not None:
            updates.append("notes=?"); params.append(body.notes)

        if not updates:
            return {"status": "no_changes"}

        params.append(strategy_id)
        conn.execute(
            f"UPDATE strategies SET {', '.join(updates)} WHERE id=?", params
        )
        conn.commit()

    return {"status": "updated", "id": strategy_id}


@router.post("/strategies/{strategy_id}/compute")
def compute_strategy_results(strategy_id: str):
    """
    Read trade_log rows where strategy_id matches and compute a backtest summary.
    Saves the result as JSON into backtest_result and updates last_backtest_at.
    """
    with get_conn() as conn:
        _ensure_table(conn)

        row = conn.execute(
            "SELECT * FROM strategies WHERE id=?", (strategy_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Strategy {strategy_id} not found.")

        # Check if trade_log table exists
        try:
            trades = conn.execute(
                """
                SELECT pnl_pct, exit_reason, direction, days_held, entry_date
                FROM trade_log
                WHERE strategy_id=?
                ORDER BY entry_date DESC
                """,
                (strategy_id,),
            ).fetchall()
        except Exception:
            trades = []

        if not trades:
            result = {
                "trades":         0,
                "message":        "No trades found for this strategy. Run the backtest pipeline first.",
            }
        else:
            pnls     = [dict(t)["pnl_pct"] for t in trades if dict(t)["pnl_pct"] is not None]
            wins     = [p for p in pnls if p and p > 0]
            losses   = [p for p in pnls if p and p <= 0]
            by_dir: dict[str, list] = {}
            for t in trades:
                td = dict(t)
                by_dir.setdefault(td["direction"], []).append(td["pnl_pct"] or 0)

            result = {
                "trades":        len(trades),
                "win_rate":      round(len(wins) / len(pnls) * 100, 2) if pnls else None,
                "avg_return":    round(sum(pnls) / len(pnls), 4) if pnls else None,
                "avg_win":       round(sum(wins) / len(wins), 4) if wins else None,
                "avg_loss":      round(sum(losses) / len(losses), 4) if losses else None,
                "total_pnl_pct": round(sum(pnls), 4) if pnls else None,
                "max_win":       round(max(pnls), 4) if pnls else None,
                "max_loss":      round(min(pnls), 4) if pnls else None,
                "by_direction":  {
                    d: {
                        "trades":   len(v),
                        "avg_pnl":  round(sum(v) / len(v), 4) if v else None,
                    }
                    for d, v in by_dir.items()
                },
                "computed_at": _now_iso(),
            }

        conn.execute(
            "UPDATE strategies SET backtest_result=?, last_backtest_at=? WHERE id=?",
            (json.dumps(result), _now_iso(), strategy_id),
        )
        conn.commit()

    return {"status": "computed", "id": strategy_id, "result": result}


@router.post("/strategies/{strategy_id}/promote")
def promote_strategy(strategy_id: str, body: PromoteRequest):
    """
    Advance the strategy's status by one step in the lifecycle.
    Promoting to 'prod' auto-archives any existing prod strategy.
    Requires ADMIN_SECRET.
    """
    secret = _ADMIN_SECRET()
    if not secret or body.secret != secret:
        raise HTTPException(status_code=403, detail="Invalid admin secret.")

    with get_conn() as conn:
        _ensure_table(conn)

        row = conn.execute(
            "SELECT * FROM strategies WHERE id=?", (strategy_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Strategy {strategy_id} not found.")

        strategy = dict(row)
        current_status = strategy["status"]

        if current_status == "archived":
            raise HTTPException(status_code=400, detail="Archived strategies cannot be promoted.")

        new_status = _STATUS_PROGRESSION.get(current_status)
        if not new_status:
            raise HTTPException(status_code=400, detail=f"No promotion path from status '{current_status}'.")

        now = _now_iso()

        # If promoting to prod, archive the existing prod strategy
        if new_status == "prod":
            conn.execute(
                "UPDATE strategies SET status='archived' WHERE status='prod' AND id != ?",
                (strategy_id,),
            )

        conn.execute(
            "UPDATE strategies SET status=?, version=version+1, promoted_by=?, promoted_at=? WHERE id=?",
            (new_status, body.promoted_by, now, strategy_id),
        )
        conn.commit()

    return {
        "status":      "promoted",
        "id":          strategy_id,
        "old_status":  current_status,
        "new_status":  new_status,
        "promoted_at": now,
    }


@router.delete("/strategies/{strategy_id}")
def delete_strategy(strategy_id: str, secret: str = ""):
    """Delete a strategy — only allowed when status is 'draft'."""
    admin_secret = _ADMIN_SECRET()
    if not admin_secret or secret != admin_secret:
        raise HTTPException(status_code=403, detail="Invalid admin secret.")

    with get_conn() as conn:
        _ensure_table(conn)

        row = conn.execute(
            "SELECT status FROM strategies WHERE id=?", (strategy_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Strategy {strategy_id} not found.")

        if dict(row)["status"] != "draft":
            raise HTTPException(
                status_code=400,
                detail="Only draft strategies can be deleted. Archive non-draft strategies instead.",
            )

        conn.execute("DELETE FROM strategies WHERE id=?", (strategy_id,))
        conn.commit()

    return {"status": "deleted", "id": strategy_id}
