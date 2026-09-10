"""
Pathfinder MOCK SERVER — the artifact Session P2 (frontend) builds against.

Run it:
    cd backend
    uvicorn pathfinder.mock_app:app --port 8010 --reload

Then:
    http://127.0.0.1:8010/docs                        interactive contract
    http://127.0.0.1:8010/api/pathfinder/loop
    http://127.0.0.1:8010/api/pathfinder/experiments?status=died
    http://127.0.0.1:8010/api/pathfinder/experiment/exp_0007
    http://127.0.0.1:8010/api/pathfinder/learnings

The frontend points `EXPO_PUBLIC_API_BASE_URL` at this server. When P1 lands the
real engine, the base URL changes and NOTHING else does — same paths, same
schemas, same guarded errors.

This app is deliberately separate from `backend/main.py`: the mock must never be
mistakable for the product backend. `main.py` mounts the SAME router, gated OFF
by default (`KANIDA_PATHFINDER_ENABLED`).
"""
from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .router import router as pathfinder_router
from .schemas import RESEARCH_DISCLOSURE
from .store import get_store

log = logging.getLogger("kanida.pathfinder.mock")

app = FastAPI(
    title="KANIDA.AI — Pathfinder (mock)",
    version="0.1.0",
    description=(
        "Contract-first mock for the Pathfinder research loop.\n\n"
        "**The data served here is hand-authored, honest sample data — not engine "
        "output, not real results, nothing was traded.** It exists so the frontend and "
        "engine tracks can be built in parallel against one contract."
    ),
)

# The mock is a local dev tool; the real backend keeps its own CORS policy.
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"]
)


@app.middleware("http")
async def _request_id(request: Request, call_next):
    request.state.request_id = uuid.uuid4().hex[:12]
    return await call_next(request)


@app.exception_handler(Exception)
async def _guarded_500(request: Request, exc: Exception) -> JSONResponse:
    """Guarded 500: log the detail server-side, disclose nothing to the client."""
    rid = getattr(request.state, "request_id", None) or uuid.uuid4().hex[:12]
    log.exception("pathfinder mock error rid=%s", rid)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": "Something went wrong on our side.",
                "request_id": rid,
            }
        },
    )


app.include_router(pathfinder_router, prefix="/api")


def _source() -> tuple[str, str, list[str]]:
    """(source name, warning, endpoints) — never raises, never says 'fixtures' for the research source."""
    from .router import RESEARCH_SOURCE_PATHS, _research_source
    if _research_source():
        return (
            "research (S1 feed + S2 experiments) — engine output from the point-in-time stores; never fixtures",
            "RESEARCH SOURCE. Editions and experiments computed by the engine on the sealed warehouse; "
            "a backfilled edition is labelled so on every payload. Nothing was traded.",
            RESEARCH_SOURCE_PATHS,
        )
    return (
        get_store().source_name,
        "MOCK DATA. Hand-authored honest fixtures — not engine output, not real results, nothing was traded.",
        [
            "/api/pathfinder/loop",
            "/api/pathfinder/experiments?status=",
            "/api/pathfinder/experiment/{experiment_id}",
            "/api/pathfinder/learnings",
        ],
    )


@app.get("/", include_in_schema=False)
def root() -> dict:
    name, warning, endpoints = _source()
    return {
        "service": "kanida-pathfinder-mock",
        "data_source": name,
        "warning": warning,
        "disclosure": RESEARCH_DISCLOSURE,
        "endpoints": endpoints,
        "docs": "/docs",
    }


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"ok": True, "data_source": _source()[0]}
