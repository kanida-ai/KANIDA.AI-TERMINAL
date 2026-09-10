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

import os
from typing import Union

from .schemas import (
    ErrorBody,
    ErrorResponse,
    ExperimentDetail,
    ExperimentListResponse,
    ExperimentRecord,
    ExperimentStatus,
    ExperimentsResponse,
    FeedResponse,
    LearningsResponse,
    LoopResponse,
)
from .store import get_store
from .research.store import get_research_store
from .experiments.store import get_experiment_store
from .sessions import BASIS_PROJECTED, BASIS_SESSION_CALENDAR, project_session_after


def _research_source() -> bool:
    """S2: `KANIDA_PATHFINDER_SOURCE=research` serves the experiment registry on /experiments."""
    return os.environ.get("KANIDA_PATHFINDER_SOURCE", "mock").strip().lower() == "research"


#: The paths the research source serves. `/loop` and `/learnings` are the P0/P1 loop story and
#: are NOT served by it (S3 §4.1) — they answer with a guarded 404 naming these, never a 500.
RESEARCH_SOURCE_PATHS = ["/api/pathfinder/feed", "/api/pathfinder/experiments", "/api/pathfinder/experiment/{experiment_id}"]


def _not_served(request: Request) -> JSONResponse:
    rid = getattr(request.state, "request_id", None) or uuid.uuid4().hex[:12]
    return JSONResponse(
        status_code=404,
        content=ErrorResponse(error=ErrorBody(
            code="not_served_by_source",
            message="Not served by the research source (KANIDA_PATHFINDER_SOURCE=research). "
                    "Use /api/pathfinder/feed for the edition and /api/pathfinder/experiments for the registry.",
            request_id=rid, use=RESEARCH_SOURCE_PATHS)).model_dump(),
    )


def schema_version_string() -> str:
    from .research.store import SCHEMA_VERSION as RESEARCH_SCHEMA
    from .experiments.store import SCHEMA_VERSION as EXPERIMENTS_SCHEMA
    from .schemas import FEED_SCHEMA_SEMVER
    return f"pathfinder_feed@{FEED_SCHEMA_SEMVER}+research_store.{RESEARCH_SCHEMA}+experiments_store.{EXPERIMENTS_SCHEMA}"


def serve_feed(feed: FeedResponse, *, research_engine: Optional[str], experiment_cards: Optional[list] = None,
               experiments_scoreboard: Optional[dict] = None, experiments_engine: Optional[str] = None) -> FeedResponse:
    """
    The served form of an edition (integration polish, S3 §4.4 / §4.5): the S2 cards and scoreboard
    attached, `engine_version` / `schema_version` stamped, and every PENDING card given a `due_session` —
    the engine's own when it stamped one (`session_calendar`), else a LABELLED projection over the
    exchange calendar (`projected`). The grade never reads the projected date. A continuation keeps
    whatever its root's horizon gave it (its base date is the root's edition, not this card's).
    Validated once, as a whole, so the contract's own laws are re-checked on the served bytes.
    """
    body = feed.model_dump(mode="json")
    if experiment_cards is not None:
        body["experiment_cards"] = experiment_cards
        body["experiments_scoreboard"] = experiments_scoreboard
    parts = [v for v in (research_engine, experiments_engine if experiment_cards is not None else None) if v]
    body["engine_version"] = "; ".join(parts) if parts else None
    body["schema_version"] = schema_version_string()
    for tier in ("what_matters_now", "discoveries"):
        for f in body[tier]:
            g = f["grading"]
            if g.get("due_session"):
                g["due_session_basis"] = g.get("due_session_basis") or BASIS_SESSION_CALENDAR
            elif g["status"] == "pending":
                # only a PENDING root: a continuation is graded through its root's horizon, whose
                # base date is the root's edition, not this card's — never projected from here
                g["due_session"] = project_session_after(
                    feed.edition_date, int(f["grading_rule"]["horizon_sessions"])).isoformat()
                g["due_session_basis"] = BASIS_PROJECTED
    return FeedResponse.model_validate(body)

log = logging.getLogger("kanida.pathfinder")

router = APIRouter(prefix="/pathfinder", tags=["Pathfinder"])

_EXPERIMENT_ID_RE = re.compile(r"^exp_[a-z0-9_]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

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
def get_loop(request: Request):
    if _research_source():
        return _not_served(request)
    return get_store().loop()


@router.get(
    "/experiments",
    response_model=Union[ExperimentsResponse, ExperimentListResponse],
    responses=_ERRORS,
    summary="The edge-discovery pipeline",
    description=(
        "Every experiment across the lifecycle. Dead experiments are listed, not hidden.\n\n"
        "**S2 (`KANIDA_PATHFINDER_SOURCE=research`)** serves the experiment REGISTRY as "
        "`ExperimentsResponse`: every experiment's PUBLIC card (theme + evidence + the seven-line "
        "story — never a constituent, an entry, a target or a stop), the S1 findings the gate "
        "declined with their trial counts (`not_opened`), and the running experiment scoreboard "
        "(Right · Wrong · Inconclusive · n over graded periods, void apart, forward / backfilled "
        "split). Losers first: buried, then testing, proposed last.\n\n"
        "Other sources serve the P0/P1 `ExperimentListResponse`."
    ),
)
def get_experiments(
    request: Request,
    status: Optional[ExperimentStatus] = Query(
        None, description="P0/P1 only: filter by lifecycle status. Omit for all."
    ),
):
    if _research_source():
        from .experiments.config import ENGINE_VERSION
        from .experiments.views import experiments_response
        xs = get_experiment_store()
        body = experiments_response(xs, engine_version=ENGINE_VERSION) if xs is not None else None
        if body is None:
            return _error(404, "no_experiments", "The experiment loop has not run yet.", request)
        return body
    return get_store().experiments(status)


@router.get(
    "/experiment/{experiment_id}",
    response_model=Union[ExperimentRecord, ExperimentDetail],
    responses=_ERRORS,
    summary="One experiment's full journey",
    description=(
        "**S2 (`KANIDA_PATHFINDER_SOURCE=research`)**: the in-app `ExperimentRecord` — versions "
        "(v1, v2… with what changed, why, the trial count and the expectation FROZEN when each "
        "opened), every counted trial, every period with its frozen grading rule, forward result, "
        "expected-vs-actual and learning, the change-log, the post-mortem when buried, and the "
        "graduation PROPOSAL when one exists (human-gated). Constituent names are withheld "
        "pending RA review.\n\n"
        "Other sources serve the P0/P1 `ExperimentDetail`."
    ),
)
def get_experiment(experiment_id: str, request: Request, response: Response):
    if not _EXPERIMENT_ID_RE.match(experiment_id or ""):
        return _error(
            400, "invalid_experiment_id",
            "experiment_id must look like 'exp_0007'.", request,
        )
    if _research_source():
        from .experiments.views import record
        xs = get_experiment_store()
        rec = record(xs, experiment_id) if xs is not None else None
        if rec is None:
            return _error(404, "not_found", "No experiment with that id.", request)
        return rec
    detail = get_store().experiment(experiment_id)
    if detail is None:
        return _error(404, "not_found", "No experiment with that id.", request)
    return detail


@router.get(
    "/feed",
    response_model=FeedResponse,
    responses=_ERRORS,
    summary="The clarity-first research feed for one close (S1)",
    description=(
        "The after-close edition: findings ranked by usefulness, **clarity first** — "
        "`what_matters_now` holds the first two or three, `discoveries` the rest. There is "
        "**no minimum count and no padding**: an edition with four findings is a valid edition.\n\n"
        "Every finding was **computed before it was written** by a template from the question "
        "library and carries: a digit-free `narrative` whose numbers are `{{fact:<id>}}` "
        "references into `facts[]`; full `provenance` (evidence level, n, period, regime, "
        "comparison group, cost hurdle); a `grading_rule` **frozen at publication**; and its "
        "`grading` state (pending, or graded Right / Wrong / Inconclusive with the realised "
        "facts). `scoreboard` is the running public record with n.\n\n"
        "`date` selects an edition (YYYY-MM-DD); omit it for the latest. Research items only — "
        "no entry, target, stop or execution is ever implied."
    ),
)
def get_feed(
    request: Request,
    date: Optional[str] = Query(None, description="Edition close date, YYYY-MM-DD. Omit for the latest."),
):
    if date is not None and not _DATE_RE.match(date):
        return _error(400, "invalid_date", "date must look like 2026-07-29.", request)
    store = get_research_store()
    if store is None:
        return _error(404, "no_edition", "No research edition has been published yet.", request)
    feed = store.feed(date)
    if feed is None:
        return _error(404, "no_edition", "No research edition for that date.", request)
    ed = feed.edition_date.isoformat()
    row = store.edition(ed)
    research_engine = row["engine_version"] if row is not None else None
    # S2: experiment cards with news on this edition, and the experiment scoreboard as of it —
    # only when the process serves the research source (S1's own contract is untouched otherwise).
    xs = get_experiment_store() if _research_source() else None
    if xs is not None and xs.latest_edition() is not None:
        from .experiments.config import ENGINE_VERSION as XENGINE
        from .experiments.views import cards_on
        xrow = xs.one("SELECT engine_version FROM pfx_editions WHERE edition_date = ?", [ed])
        return serve_feed(
            feed, research_engine=research_engine,
            experiment_cards=[c.model_dump(mode="json") for c in cards_on(xs, ed)],
            experiments_scoreboard=xs.scoreboard(ed).model_dump(mode="json"),
            experiments_engine=(xrow["engine_version"] if xrow is not None else XENGINE),
        )
    return serve_feed(feed, research_engine=research_engine)


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
def get_learnings(request: Request):
    if _research_source():
        return _not_served(request)
    return get_store().learnings()
