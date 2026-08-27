"""
FastAPI surface for the Agent Platform. Mount in main.py exactly like agent_builder:

    from agents.router import router as agents_router
    app.include_router(agents_router, prefix="/api", tags=["Agents"])

Read-only + paper by default. NO execution happens here — an agent's intents flow to
backend/autotrade/ downstream (paper-default, cert-gated, operator-armed).
"""
from __future__ import annotations
from fastapi import APIRouter, HTTPException
from . import registry

router = APIRouter()
registry.load_builtin()


@router.get("/agents/health")
def agents_health():
    return {"ok": True, "agents": [a.manifest.agent_id for a in registry.all_agents()]}


@router.get("/agents")
def list_agents():
    return {"agents": [a.manifest.to_dict() for a in registry.all_agents()]}


@router.get("/agents/{agent_id}")
def get_agent(agent_id: str):
    a = registry.get(agent_id)
    if not a:
        raise HTTPException(404, f"no agent '{agent_id}'")
    return a.manifest.to_dict()
