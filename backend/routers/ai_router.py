"""
AI chat router — Anthropic-backed assistant for the Kanida terminal.

Surfaces:
  POST /api/ai/chat — context-aware chat. Pulls live engine + signal data
                      automatically and injects it as system context so the
                      assistant can reason about what the user is currently
                      looking at.

Auth: Open by default (matches other read endpoints). Add an admin secret
if cost containment is needed later.

Cost control:
  - Hard-cap conversation history at MAX_HISTORY messages.
  - Hard-cap user message length at MAX_MSG_CHARS.
  - Use Haiku 4.5 by default (fast, cheap). Switch to opus via ?model=opus.
"""
from __future__ import annotations

import logging
import os
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()
log = logging.getLogger("kanida.ai")

MAX_HISTORY  = 20      # last 20 messages preserved
MAX_MSG_CHARS = 4000   # incoming user message hard cap
DEFAULT_MODEL = "claude-haiku-4-5-20251001"
OPUS_MODEL    = "claude-opus-4-7"

SYSTEM_PROMPT = """You are Kanida AI, an embedded analyst inside the Kanida.AI Quant Trading Terminal.

You help discretionary traders and quants understand:
  - Engine performance (Turbo, Super, Standard) and what each means
  - Active signals (live opportunities the engine has flagged today)
  - Why a specific stock is firing — its pattern, statistics, smart-entry plan
  - How to act: position sizing intuition, risk levels, exit logic

Voice and behavior:
  - Be concise. Traders are busy. 2-4 sentences for casual questions, lists for setups.
  - Never invent numbers. If you don't have data, say so and offer how to get it.
  - Reference specific tickers and engine names when discussing positions.
  - When asked "what should I do?", give a clear actionable answer (watch / enter / wait)
    grounded in the data you've been given. Add caveats only when truly relevant.
  - Do NOT give legal or financial advice — frame opinions as analysis.
  - Use minimal markdown. Bullet lists are great. No headers.
"""


class ChatMessage(BaseModel):
    role:    Literal["user", "assistant"]
    content: str = Field(..., max_length=MAX_MSG_CHARS)


class ChatRequest(BaseModel):
    message:      str = Field(..., max_length=MAX_MSG_CHARS)
    history:      list[ChatMessage] = []
    # Free-form context the frontend wants the model to know about.
    # Examples: { "ticker": "DRREDDY", "year": "2025", "mode": "MODERN" }
    context:      dict = {}
    model:        Optional[Literal["haiku", "opus"]] = "haiku"


def _live_data_summary() -> str:
    """Pull a tight snapshot of current engine + signal state to inject as context."""
    try:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from db import get_conn
    except Exception:
        return ""

    try:
        with get_conn() as con:
            # Engine summary (HC scope)
            hc = con.execute("""
                SELECT COUNT(*) AS n,
                       ROUND(AVG(CASE WHEN e.trade_taken=1 AND e.smart_pnl_pct IS NOT NULL
                                      THEN e.smart_pnl_pct
                                      ELSE COALESCE(e.blind_pnl_pct, t.pnl_pct) END), 2) AS avg_pnl,
                       SUM(CASE WHEN (CASE WHEN e.trade_taken=1 AND e.smart_pnl_pct IS NOT NULL
                                           THEN e.smart_pnl_pct
                                           ELSE COALESCE(e.blind_pnl_pct, t.pnl_pct) END) > 0 THEN 1 ELSE 0 END) AS wins
                FROM trade_log t LEFT JOIN execution_log e ON e.trade_log_id = t.id
                WHERE t.direction IN ('rally','long')
                  AND json_extract(t.notes,'$.bucket') IN ('turbo','super')
            """).fetchone()
            n   = hc["n"] or 1
            wr  = round((hc["wins"] or 0) / n * 100, 1)

            sigs = con.execute("""
                SELECT ticker, opportunity_score, tier, setup_summary
                FROM live_opportunities
                WHERE direction = 'rally'
                ORDER BY opportunity_score DESC
                LIMIT 8
            """).fetchall()
    except Exception as exc:
        log.warning("live_data_summary failed: %s", exc)
        return ""

    parts = [
        f"<engine_performance>",
        f"  scope: Turbo + Super (high-conviction)",
        f"  trades: {hc['n']}",
        f"  win_rate: {wr}%",
        f"  avg_pnl: {hc['avg_pnl']}%",
        f"</engine_performance>",
        f"<active_signals count={len(sigs)}>",
    ]
    for s in sigs:
        parts.append(
            f"  {s['ticker']} score={s['opportunity_score']:.3f} tier={s['tier']} "
            f"setup={(s['setup_summary'] or '')[:160]}"
        )
    parts.append("</active_signals>")
    return "\n".join(parts)


@router.post("/ai/chat")
def ai_chat(body: ChatRequest):
    """Returns assistant message. Falls back to 503 if API key not configured."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail=(
                "AI chat is not configured. Set ANTHROPIC_API_KEY in the Railway "
                "environment to enable this feature."
            ),
        )

    try:
        import anthropic
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"Anthropic SDK missing: {exc}")

    client = anthropic.Anthropic(api_key=api_key)
    model_id = OPUS_MODEL if body.model == "opus" else DEFAULT_MODEL

    # Build system prompt with live data + frontend context
    sys_parts = [SYSTEM_PROMPT]
    live = _live_data_summary()
    if live:
        sys_parts.append("Current terminal state (auto-injected):\n" + live)
    if body.context:
        ctx_str = "\n".join(f"  {k}: {v}" for k, v in body.context.items() if v not in (None, "", "ALL"))
        if ctx_str:
            sys_parts.append("User's current view filters:\n" + ctx_str)
    system = "\n\n".join(sys_parts)

    # Conversation history (cap to last MAX_HISTORY)
    history = body.history[-MAX_HISTORY:] if body.history else []
    messages = [{"role": m.role, "content": m.content} for m in history]
    messages.append({"role": "user", "content": body.message})

    try:
        resp = client.messages.create(
            model=model_id,
            max_tokens=1024,
            system=system,
            messages=messages,
        )
    except anthropic.APIError as exc:
        log.error("Anthropic API error: %s", exc)
        raise HTTPException(status_code=502, detail=f"AI provider error: {exc}")
    except Exception as exc:
        log.exception("AI chat unexpected error")
        raise HTTPException(status_code=500, detail=str(exc))

    text = ""
    for block in resp.content:
        if block.type == "text":
            text += block.text

    return {
        "message":     text,
        "model":       model_id,
        "input_tokens":  resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
    }


@router.get("/ai/health")
def ai_health():
    """Quick check used by the frontend to know whether to render chat as available."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    return {
        "configured": bool(api_key),
        "model":      DEFAULT_MODEL,
    }
