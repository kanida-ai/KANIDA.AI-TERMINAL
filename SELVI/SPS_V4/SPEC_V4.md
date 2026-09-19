# SPS_V4 — Self-Improving Quant Agent · Spec (extends SPS_V3)

> Adds recursive self-improvement to the SELVI agents **without ever weakening the
> leak-free guarantees**. Paper/research only; live execution stays human-authorized.

## The central design: two layers
| Layer | Who may change it | Contents |
|---|---|---|
| **Mutable capability layer** (`capability_layer.py`) | **the agent may author & evolve code here** | fact-extractors, triggers, **filters/predicates**, features, search policy |
| **Immutable integrity core** (`immutable_core.py`) | **LOCKED — the agent may NOT touch it** | point-in-time features, **vault seal**, simulator, cost model, the **admission gauntlet** (validator) |

Self-modification *feeds into* the core; it can never *bypass* it. The core carries a
content hash (`SEAL`); the engine refuses to run if the core was altered unexpectedly.

## The four capabilities — delivered & bounded
1. **Recursive self-improvement.** The engine authors new components, admits the ones
   that survive the gauntlet, then **composes admitted survivors into stronger ones**
   (generation N+1 builds on generation N). A meta-learner biases search toward the
   feature families that keep getting admitted. *Bounded by real market edge — it finds
   more/faster, it does not manufacture alpha.*
2. **Self-modifying code, training-only, leak-free.** New components are authored **only
   during research**; the deployed strategy is a **frozen artifact** (admitted specs),
   never live-mutating code. Every self-authored component runs against point-in-time
   data it cannot alter and is admitted only after clearing **train t≥2.5 AND val t≥2.5**,
   then **vault-confirmed** on sealed 2026. New component *families* get a human review gate.
3. **Autonomous action.** Full autonomy in research/paper (generate, test, admit, recurse,
   self-heal). **Real-money order placement remains human-authorized — always.**
4. **Resource accrual.** The agent autonomously uses an **allowlist** of resources — the
   data in `kanida.db` (+ fetch more when tokens live), libraries (numpy/pandas/scipy/
   sklearn), the skills it builds, and parallel compute — and **accrues knowledge capital**
   in a growing **skill library**. It does **NOT** acquire credentials, external accounts,
   or money autonomously (hard security wall).

## The admission gauntlet (immutable)
A candidate component is admitted **iff**: ≥100 train trades, ≥40 val trades, net>0 on
both, **train t-stat ≥ 2.5 AND val t-stat ≥ 2.5**, and it **beats the current baseline's
val t-stat**. Champions are then **vault-confirmed** on sealed 2026 (the honest final test —
which will *reject* self-improvements that merely overfit validation).

## The self-improvement loop
`author candidates (capability layer) → immutable gauntlet → admit survivors to skill
library → RECURSE: compose admitted survivors → meta-learn admitted feature families →
repeat`. Thresholds are sampled from **TRAIN quantiles only** (no val/vault peeking).

## Skill library (`skill_library.db`)
Persistent record of every admitted component: description, target/stop, train/val t-stats,
generation, and **lineage** (what it was composed from) — the agent's compounding capital.

## Guardrails (non-negotiable)
- Immutable-core `SEAL` hash-check; core is read-only to the agent.
- Code-gen sandboxed to the capability layer; no filesystem/network/credential access.
- Human review gate before a **new component family** enters production.
- Live execution human-authorized; kill-switch on drawdown / paper-vs-backtest divergence.

## Honest limit
Self-improvement raises how much *real* edge we extract and how fast, and how well we avoid
overfitting. It cannot exceed the market's extractable alpha net of costs/capacity. The
highest-value target for the loop is **discovering new *uncorrelated* setup families** and
**learning the cost/regime structure** — that is where progress toward +1%/day lives.
