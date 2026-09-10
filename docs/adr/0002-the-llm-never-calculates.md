# ADR 0002 — "The LLM never calculates" is a data structure, not a prompt instruction

- **Status:** Accepted
- **Date:** 2026-09-08 (IST)
- **Session:** Pathfinder P0

## Context

The core principle of Pathfinder is that the LLM is a research brain around a deterministic quant
engine, and that it never computes a number. Stating this in a system prompt is not an enforcement
mechanism — a model that drifts, or a prompt that regresses, produces a plausible number and nothing
in the system notices. For a product whose non-negotiables include *"never fabricate results"*, that
failure mode is unacceptable.

The question was how to make the principle **checkable** rather than merely intended.

## Decision

Numbers and narrative are separated at the type level.

- Every number lives in a **`Fact`**: an id, a label, a value, a unit, an `n`, and a `Provenance`
  (data source, date range, `as_of`, cost convention, and the *deterministic component* that computed
  it).
- LLM-authored prose may not contain a literal numeral. It references a number as `{{fact:fct_…}}`,
  which the client resolves against `facts[]` at render time.
- Therefore *"did a number originate in the LLM?"* becomes a regular expression, and it is checked in
  three independent places:
  1. **Database** — `pathfinder.story_lines` has a CHECK that LLM-authored text, with `{{…}}` tokens
     stripped, contains no digit; `facts`/`outcomes`/`evidence` have a CHECK that `computed_by` does
     not match a model name.
  2. **API contract** — the same rule is a Pydantic validator on `StoryLine`, so a violating payload
     cannot be serialised.
  3. **Gateway** — `pathfinder.narrate.v1` gives `headline` the JSON-Schema pattern `^[^0-9]*$`,
     making a bare numeral structurally impossible in the headline; a provider must raise
     `OutputContractViolation` rather than return a violating `body`.

**Carve-out:** a *rule parameter* is not a computed number. "A 3-session pullback below 0.75× median
volume" is a definition, stored in `parameters` inside a Constitution-approved range. Rule text may
carry digits; narrative claims may not.

## Consequences

- `dev-quant-auditor`'s "no number originated in the LLM" check becomes mechanical rather than a
  judgement call.
- The frontend must resolve fact tokens. That is a small cost with a large upside: every rendered
  number can carry its own n, window and source on hover, which is exactly what the non-negotiables
  demand anyway.
- Narrative prose is slightly stiffer — the model writes "the sample is too small" rather than
  "only 11 trades". Acceptable, and arguably better copy.
- P0's own fixtures caught two violations at import time before any test ran. The mechanism works.
