# ADR 0004 — All model access goes through the `pathfinder_llm` gateway; the job picks the model

- **Status:** Accepted (routing locked in `docs/sessions/PATHFINDER.md`)
- **Date:** 2026-09-08 (IST)
- **Session:** Pathfinder P0

## Context

Pathfinder makes six kinds of model call with wildly different cost and quality profiles — proposing
a hypothesis is worth real reasoning; turning computed results into a sentence is not. Left to the
call site, model choice drifts: whoever writes the code picks whatever is at hand, cost becomes
unpredictable, and swapping providers means touching every call site.

There is also a compliance dimension. Which model wrote which customer-facing sentence, under which
version of the Constitution, is an audit question — not a logging nicety.

## Decision

Product code **never** calls a model SDK. It calls three verbs on a provider-independent gateway:
`reason()`, `narrate()`, `classify()` (`backend/pathfinder/llm/gateway.py`; full contract in
`docs/STRATEGY_METHODOLOGY.md` §3).

- **The job picks the model, not the caller.** `hypothesis` / `critique` / `decide_next` →
  `claude-sonnet-5`; `narrate` / `classify` → `claude-haiku-4-5`; `hard_research` →
  `claude-opus-5` (rare). `pathfinder.llm_calls` has a CHECK pinning that set; changing a row is an
  **L4 (Constitution) decision**.
- **Structured output on every call** (`output_config.format`, `strict: true`), by versioned
  `schema_id`. `pathfinder.reason.v1` cannot express `learning_level: "L4"` — a Constitution change
  is unrepresentable in the model's output type.
- **Caching on the stable prefix** (Constitution + methodology + rulebook + deterministically
  serialised fact table), volatile content after the last breakpoint, with
  `usage.cache_read_input_tokens` logged so a silent invalidator is an alert rather than a mystery.
- **Batch where non-urgent** — `narrate` and `classify` default to the Batches API (~50% cost);
  `reason` does not, because a hypothesis or a kill decision is part of a loop turn.
- **Hard daily cap, checked before the call**, enforced against `budget_day_ist` (IST, not the host
  timezone). No headroom → `BudgetExceeded`, and the loop pauses. It does not degrade to a cheaper
  model, reuse a stale answer, or write text without a model.
- **Every call is metered** into `pathfinder.llm_calls` from real provider token counts, never
  estimates, and surfaced on `GET /api/pathfinder/loop` as `llm_usage`.
- **Nothing outside a provider module** may import a vendor SDK or name a vendor type. Claude → GPT →
  Gemini is a config swap.

## Consequences

- Cost is a property of the workload rather than of whoever wrote the call site, and it is visible
  from day one instead of arriving as a bill.
- Provider-specific quirks (adaptive thinking on Sonnet 5 / Opus 5 vs `budget_tokens` on Haiku 4.5;
  no assistant prefill; no mid-conversation system messages on Sonnet 5) stay inside one module.
- A degraded gateway degrades the *narrative*, never the numbers: the deterministic observer keeps
  running, the queue keeps filling, and the story resumes when budget or the provider does.
- P0 ships the contract only. There is no provider implementation and no SDK import anywhere in the
  package — P1 lands the Claude provider behind this Protocol.
