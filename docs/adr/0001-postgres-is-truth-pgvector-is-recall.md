# ADR 0001 — Postgres is truth; pgvector is recall

- **Status:** Accepted (locked in `docs/sessions/PATHFINDER.md`, implemented in P0)
- **Date:** 2026-09-08 (IST)
- **Session:** Pathfinder P0

## Context

Pathfinder needs two different kinds of memory. It has to *know* things exactly — what a rule was,
what a trade returned, what changed and why — and it has to *recall* things fuzzily: "have I explored
something like this before?", "what past experiments resemble this one?"

Vector stores are good at the second and terrible at the first. A retrieved-by-similarity row is
approximate by construction: it can be the wrong row, a stale row, or a row whose numbers have since
been superseded. A product whose entire claim is honesty cannot serve a number that arrived via
cosine similarity.

## Decision

**Postgres is the single source of truth.** Nine authoritative tables (`experiments`, `hypotheses`,
`parameters`, `trades`, `outcomes`, `evidence`, `strategy_versions`, `learning_events`, `decisions`)
plus the supporting tables the API contract requires. Immutable and append-only: a `deny_mutation()`
trigger makes `UPDATE`/`DELETE` raise on every history table, and the published-record tables carry a
`prev_hash`/`row_hash` chain. Status is not a column — it is an append-only transition log with a
`experiments_current` view on top.

**pgvector is associative recall only.** `recall_embeddings` rows are *pointers*: each carries the
authoritative table and id it came from. The engine may use recall to decide **where to look**; it
must then read the evidence from the authoritative row. Nothing is served to a customer, or handed to
the LLM as fact, out of the vector store.

## Consequences

- Every number the product renders is traceable to an immutable row with its own `as_of`, window,
  data source and cost convention.
- Recall degrading (a bad embedding model, a stale index, a missing extension) costs the agent
  *inspiration*, never *correctness*. The migration explicitly skips the recall schema when pgvector
  is unavailable and says so, leaving the truth schema untouched.
- Corrections cost more: they are new rows, and readers must go through views. That is the intended
  price of an auditable record.
- Storage grows monotonically. Accepted — the volumes here are tiny next to the market data.
