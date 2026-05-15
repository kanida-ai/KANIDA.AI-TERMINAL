"""Auth status reader — used by the admin widget AND the live-tier degradation
banner. Single source of truth for "is Zerodha auth healthy right now."

Computed from:
  kanida_quant.db.kite_tokens        (current token validity)
  kanida_universe.db.falcon_auth_log (today's attempts + most recent)

Layer 3 (graceful degradation) decision is here too — when token_valid=False
and we're past 09:30 IST, /api/power/picks/live should respond with a
'degraded' flag so the UI renders the yellow banner per operator spec.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

IST = timezone(timedelta(hours=5, minutes=30))


def _kite_token_status() -> Dict[str, Any]:
    """Reuse the existing services.kite_auth.get_token_status."""
    try:
        from services.kite_auth import get_token_status    # noqa: WPS433
        return get_token_status()
    except Exception as e:
        return {"valid": False, "reason": f"status_check_failed: {e}"}


def _next_expiry_iso() -> str:
    """Zerodha tokens expire at 06:00 IST the day after they're issued.
    If now is before 06:00 IST today, expiry = 06:00 IST today.
    Else expiry = 06:00 IST tomorrow."""
    now = datetime.now(IST)
    six_am_today = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now < six_am_today:
        return six_am_today.isoformat()
    return (six_am_today + timedelta(days=1)).isoformat()


def _today_iso() -> str:
    return datetime.now(IST).date().isoformat()


def get_status(db_path: str) -> Dict[str, Any]:
    """Full status dict for the admin widget.

    Keys:
      token_valid:        bool
      token_user:         str | None
      token_set_by:       str | None    ('auto_auth_bot' / 'admin_api' / 'magic_link' / None)
      active_until:       ISO timestamp (when token expires)
      last_auto_attempt:  {at, status, attempt_of_day, error_code} | None
      next_scheduled_at:  ISO timestamp
      today_attempts:     count of attempts logged for today
      degraded:           bool   — True iff token invalid AND past 09:30 IST
      fallbacks_pending:  bool   — True iff token invalid AND no recent push sent
    """
    token = _kite_token_status()
    valid = bool(token.get("valid"))

    # Most recent attempt + today's count
    last_attempt = None
    today_count  = 0
    try:
        con = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute("""
                SELECT attempt_at, status, attempt_of_day, error_code, error_detail,
                       trigger_kind, token_preview
                  FROM falcon_auth_log
                 ORDER BY id DESC LIMIT 1
            """).fetchone()
            if row:
                last_attempt = dict(row)
            today = _today_iso()
            cnt_row = con.execute("""
                SELECT COUNT(*) FROM falcon_auth_log WHERE date(attempt_at) = ?
            """, (today,)).fetchone()
            today_count = cnt_row[0] if cnt_row else 0
        finally:
            con.close()
    except sqlite3.Error:
        pass

    # Next scheduled attempt (read live state from auth_scheduler)
    try:
        from services.auth_scheduler import status as sched_status    # noqa: WPS433
        next_at = sched_status().get("next_attempt_at")
    except Exception:
        next_at = None

    # Degradation: invalid + past 09:30 IST on a weekday = degraded UX
    now = datetime.now(IST)
    nine_thirty = now.replace(hour=9, minute=30, second=0, microsecond=0)
    is_weekday  = now.weekday() < 5
    degraded    = (not valid) and is_weekday and now >= nine_thirty

    return {
        "computed_at":         now.isoformat(),
        "token_valid":         valid,
        "token_user":          token.get("user"),
        "token_set_by":        token.get("set_by"),
        "token_date":          token.get("token_date"),
        "active_until":        _next_expiry_iso() if valid else None,
        "last_auto_attempt":   last_attempt,
        "next_scheduled_at":   next_at,
        "today_attempts":      today_count,
        "degraded":            degraded,
        "fallbacks_pending":   _has_pending_push_fallback(db_path) if not valid else False,
    }


def _has_pending_push_fallback(db_path: str) -> bool:
    """Was a push notification sent today? If yes, no further fallbacks pending."""
    try:
        con = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
        try:
            row = con.execute("""
                SELECT 1 FROM power_user_magic_links
                 WHERE kind = 'admin_auth_refresh'
                   AND date(created_at) = ?
                 LIMIT 1
            """, (_today_iso(),)).fetchone()
            return row is None
        finally:
            con.close()
    except sqlite3.Error:
        return False


def recent_log(db_path: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Return the most recent N falcon_auth_log rows for the admin widget timeline."""
    try:
        con = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute("""
                SELECT id, attempt_at, attempt_of_day, trigger_kind, status,
                       stage, error_code, error_detail, token_preview, elapsed_ms
                  FROM falcon_auth_log
                 ORDER BY id DESC LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()
    except sqlite3.Error:
        return []
