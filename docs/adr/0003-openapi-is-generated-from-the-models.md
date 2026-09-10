# ADR 0003 — `docs/openapi.yaml` is generated from the Pydantic models and merged, never hand-edited

- **Status:** Accepted
- **Date:** 2026-09-08 (IST)
- **Session:** Pathfinder P0

## Context

`docs/openapi.yaml` is the seam of the whole build: the engine track builds to *produce* it and the
app track builds to *consume* it, in parallel. If the spec and the server can disagree, the parallel
build produces rework — exactly the thing contract-first was meant to prevent.

A second problem: several sessions write into the same file. Pathfinder owns `/api/pathfinder/*`;
session `01-api-contract` owns `/api/trader/*`; compliance and creator surfaces come later. A
whole-file overwrite by any one session destroys another's work.

## Decision

**Generate, then merge.**

- `backend/pathfinder/schemas.py` is the source of truth. The product laws live there as Pydantic
  validators (expectancy required, drawdown paired, losers-first ordering, derived `n` flags,
  no numerals in LLM prose, L4 human-only), so they are enforced at runtime *and* exported into the
  spec's `required` lists.
- `scripts/gen_openapi.py` builds the spec from a FastAPI app carrying only the Pathfinder router,
  then **merges** it into the existing `docs/openapi.yaml`: it replaces only paths under
  `/api/pathfinder/`, adds its component schemas, and preserves everything else.
- `scripts/gen_openapi.py --check` fails if the committed spec is stale, and a test row (P0-36)
  runs it in CI. Drift is therefore a build failure, not a discovery three weeks later.
- `scripts/validate_openapi.py` runs `openapi-spec-validator` when it is installed, and always runs
  KANIDA's own structural + product-law checks (all `$ref`s resolve, no orphan schemas, every
  operation carries 200/400/404/500, no banned promise/target fields, `PerformanceBlock` requires
  expectancy and both drawdowns and does *not* require win rate).

## Consequences

- The spec, the mock and the server stubs cannot disagree, because there is only one artefact.
- Sessions can write the same file concurrently as long as each owns a path prefix. Any future
  session generating its own slice must follow the same merge discipline — an overwrite is a
  regression.
- Hand-editing `docs/openapi.yaml` is now a mistake, not an option: the next generation run reverts
  it. Contract changes are made in the models.
- YAML formatting is machine-chosen, so diffs are mechanical rather than stylistic.
