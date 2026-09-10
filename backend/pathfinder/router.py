"""
Pathfinder API — `/api/pathfinder/*`.

Four reads, matching docs/sessions/PATHFINDER.md Session P0:

    GET /api/pathfinder/loop                  the live loop, as a story
    GET /api/pathfinder/experiments?status=   the edge-discovery pipeline
    GET /api/pathfinder/experiment/{id}       the full journey incl. the change-log
    GET /api/pathfinder/learnings             what was learned + what is next

Read-only by construction. Pathfinder emits research, never an order; there is
no write surface here and there will not be one on the customer path.

Errors are GUARDED: every failure is mapped to a stable `{error:{code,message}}`
shape. No stack trace, SQL, path or internal id ever reaches a client.
"""
from __future__ import annotations

import logging
import re
import uuid
from typing import Optional

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import JSONResponse

from .schemas import (
    ErrorBody,
    ErrorResponse,
    ExperimentDetail,
    ExperimentListResponse,
    ExperimentStatus,
    LearningsResponse,
    LoopResponse,
)
from .store import get_store

log = logging.getLogger("kanida.pathfinder")

router = APIRouter(prefix="/pathfinder", tags=["Pathfinder"])

_EXPERIMENT_ID_RE = re.compile(r"^exp_[a-z0-9_]+$")

_ERRORS = {
    400: {"model": ErrorResponse, "description": "Malformed request."},
    404: {"model": ErrorResponse, "description": "No such resource."},
    500: {"model": ErrorResponse, "description": "Unexpected server error (details are not disclosed)."},
}


def _error(status: int, code: str, message: str, request: Request) -> JSONResponse:
    """The only way this module returns a failure. Never leaks internals."""
    rid = getattr(request.state, "request_id", None) or uuid.uuid4().hex[:12]
    return JSONResponse(
        status_code=status,
        content=ErrorResponse(error=ErrorBody(code=code, message=message, request_id=rid)).model_dump(),
    )


@router.get(
    "/loop",
    response_model=LoopResponse,
    responses=_ERRORS,
    summary="The live research loop, as a story",
    description=(
        "One cycle of the loop: what the deterministic observer noticed, the hypothesis "
        "it justified waking the LLM for, the virtual experiment, what happened, what was "
        "learned and changed, and what is next — plus the supporting `facts` every "
        "narrative sentence references.\n\n"
        "**Every number in this payload originates in `facts[]`, computed by a "
        "deterministic engine.** LLM-authored story text contains no literal numerals; it "
        "carries `{{fact:<id>}}` references the client resolves against `facts[]`."
    ),
)
def get_loop(request: Request) -> LoopResponse:
    return get_store().loop()


@router.get(
    "/experiments",
    response_model=ExperimentListResponse,
    responses=_ERRORS,
    summary="The edge-discovery pipeline",
    description=(
        "Every experiment across the lifecycle. Dead experiments are listed, not hidden.\n\n"
        "Ordering is **losers first**: struggling and dead experiments lead, promoted ones "
        "come last. `historical_return` and `virtual_return` are each a performance block "
        "that structurally pairs expectancy with drawdown; `sample_flag` is derived from "
        "`n` (`greyed` below 20, `flagged` below 50)."
    ),
)
def get_experiments(
    request: Request,
    status: Optional[ExperimentStatus] = Query(
        None, description="Filter by lifecycle status. Omit for all."
    ),
) -> ExperimentListResponse:
    return get_store().experiments(status)


@router.get(
    "/experiment/{experiment_id}",
    response_model=ExperimentDetail,
    responses=_ERRORS,
    summary="One experiment's full journey",
    description=(
        "The whole story for a single experiment: the question, the deterministic "
        "rulebook, the six story beats, every supporting fact, the evidence bundles, the "
        "virtual book with its **losers-first** ledger, the append-only L1–L3 change-log "
        "(what changed → why → evidence → previous version → new version → did it "
        "improve), and — when the experiment died — its published post-mortem."
    ),
)
def get_experiment(experiment_id: str, request: Request, response: Response):
    if not _EXPERIMENT_ID_RE.match(experiment_id or ""):
        return _error(
            400, "invalid_experiment_id",
            "experiment_id must look like 'exp_0007'.", request,
        )
    detail = get_store().experiment(experiment_id)
    if detail is None:
        return _error(404, "not_found", "No experiment with that id.", request)
    return detail


@router.get(
    "/learnings",
    response_model=LearningsResponse,
    responses=_ERRORS,
    summary="What Pathfinder has learned, and what it is testing next",
    description=(
        "`learned[]` is what the agent now believes, each tied to the experiments and "
        "evidence that produced it and tagged with its learning level (L1 evidence, L2 "
        "parameter, L3 strategy — L4 Constitution changes are human-only and never "
        "authored here). `testing_next[]` is what it decided to test next and why now, "
        "including the ones that are blocked and why."
    ),
)
def get_learnings(request: Request) -> LearningsResponse:
    return get_store().learnings()
