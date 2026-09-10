# DATA_MODEL — KANIDA.AI

Owner session: `data` (see `docs/DOC_MAP.md`). Authority order: PRD → Strategy Methodology /
Security → Architecture → contracts (this file, `openapi.yaml`, Frontend Spec).

DDL + migrations live in `migrations/`. This document explains the *rules the DDL encodes*;
the SQL is the executable truth.

| Slice | Tables | Status | Written by |
|---|---|---|---|
| **Pathfinder** (research loop) | `pathfinder.*` — below | **Defined, P0** | `docs/sessions/PATHFINDER.md` P0 |
| Trader / Investor managers | `managers.*` | *not yet written* | session `01-api-contract` / S1.1 |
| Book engine (shared) | `book.*` | *not yet written* | BUILD_PROGRAM S0.3 |
| Compliance (RA queue, disclosures, corrections) | `compliance.*` | *not yet written* | BUILD_PROGRAM S3.x |
| Creator / referral / billing | `creator.*`, `billing.*` | *not yet written* | BUILD_PROGRAM S6/S7 |

Existing production tables (`autotrade_*` on Postgres, the SQLite research DBs) are **not**
re-specified here — they are Phase-2 / R&D and are documented in `docs/architecture.md` and the
AutoTrade docs. Do not duplicate them.

---

# Pathfinder — the authoritative schema

**Migration:** `migrations/0001_pathfinder.sql` · **Schema:** `pathfinder` · **Target:** PostgreSQL 15+
**Traces to:** PRD **R5** (Pathfinder runner: hypothesis = its own virtual book; status lifecycle;
post-mortem required on death), **R17** (digest + graveyard), **R4** (book conventions, costs,
expectancy, drawdown), **R3** (append-only, hash-chained, reproducible), **R19** (methodology).

## The locked storage decision

> **Postgres is TRUTH. pgvector is RECALL. Recall is never evidence.**

- **Postgres (authoritative, relational, immutable/append-only):** everything a number, a decision
  or a claim rests on.
- **pgvector (`recall_embeddings`):** answers *"have I explored something similar?"*, *"what past
  experiments resemble this?"*, *"retrieve related journal entries."* Every row is a **pointer back**
  into an authoritative table. The engine may use recall to decide *where to look*; it must then read
  the actual evidence from the authoritative row. Nothing is ever served to a customer, or fed to the
  LLM as fact, straight out of the vector store.

## The nine authoritative tables (from the locked P0 decision)

| # | Table | Holds |
|---|---|---|
| 1 | `experiments` | Immutable identity + header of one experiment (universe, direction, horizon, cost convention, constitution version). |
| 2 | `hypotheses` | What the LLM proposed: the question, the falsifiable statement, the rationale, which model and prompt produced it, and what the novelty search returned. |
| 3 | `parameters` | Every threshold of a strategy version, with the Constitution-approved range it must sit inside. This is where **L2** learning lands. |
| 4 | `trades` | The virtual ledger. Entry = **next open after the signal bar** (a CHECK enforces it), costs and slippage on every row. |
| 5 | `outcomes` | The computed book, marked point-in-time. Serialises to the API's `PerformanceBlock`. |
| 6 | `evidence` | A named deterministic evidence bundle (replay, forward book, regime split, cost sensitivity, placebo, novelty check) the LLM is allowed to *interpret*. |
| 7 | `strategy_versions` | **L3**. A versioned rulebook; a replacement cannot be inserted without stating its validation. |
| 8 | `learning_events` | **The L1–L4 change-log.** what changed → why → evidence → previous version → new version → did performance improve. Also the customer-facing story. |
| 9 | `decisions` | The audit of autonomy: every decision, the trigger that caused it, who/what decided, under which Constitution. |

## Supporting tables the API contract requires

Added in P0 because `docs/openapi.yaml` cannot be served without them. Flagged for the founder as an
extension of the nine.

| Table | Why it must exist |
|---|---|
| `constitution_versions` | **L4.** Human-only (a CHECK forbids a non-human author). Versioned, hash-chained. |
| `facts` | **The only place a number may originate.** Every fact carries n + date range + data source + cost convention + `as_of` + the deterministic component that computed it. Narrative references facts by id. |
| `story_lines` | The six story beats. Carries the `produced_by` / `model` provenance of every sentence. |
| `triggers` | What woke the LLM (autonomy = intelligent activation, not a clock). |
| `llm_calls` | Real token + cost metering from day one; the hard daily cap is enforced against `budget_day_ist`. |
| `post_mortems` | Mandatory on death. Published. |
| `experiment_state` | Append-only status transitions (status is **not** a column on `experiments`). |
| `research_cycles` | One immutable row per turn of the loop. |
| `recall_embeddings` | pgvector recall. Never truth. |

## Rules the database itself enforces

Application code can be wrong. These are in the DDL so a bug cannot produce a dishonest row.

| Law (CLAUDE.md) | How the schema enforces it |
|---|---|
| **Append-only; nothing edited in place** | `deny_mutation()` trigger on all 17 history tables — `UPDATE`/`DELETE` raises. A correction is a new row. |
| **Hash chain on the published record** | `prev_hash` / `row_hash` on `constitution_versions`, `experiment_state`, `outcomes`, `decisions`, `learning_events`, `post_mortems`. |
| **Point-in-time is law; no look-ahead** | `facts`, `outcomes`, `evidence`: `CHECK (range_end <= as_of)`. |
| **Entry = next open after the signal bar** | `trades`: `CHECK (entry_date > signal_date)`. |
| **Costs on every simulated trade** | `trades`: a closed trade without `pnl_pct_net` **and** `costs_pct` is rejected; `slippage_bps` is `NOT NULL`. |
| **Expectancy is the hero; every return paired with its drawdown** | `outcomes`: expectancy, the 2× slippage expectancy, `max_drawdown_pct` and `current_drawdown_pct` are all `NOT NULL`; `win_rate_pct` is nullable. |
| **Backtests are labelled** | `outcomes`: `CHECK` ties `basis` to the honesty label (`Simulated …` / `Virtual money …`). |
| **No target prices, no promises** | `strategy_versions`: `CHECK (NOT (rulebook ?| ARRAY['target','target_price','price_target','expected_return']))`; a rulebook must carry an `invalidation` instead. |
| **Losers first** | The `ledger_losers_first` view and the ascending `trades_by_experiment` index are the only shipped ordering. |
| **The LLM never calculates** | `story_lines`: `CHECK` that LLM-authored `headline`/`body`, with `{{fact:…}}` tokens stripped, contains **no digit**. `facts`/`outcomes`/`evidence`: `CHECK (computed_by !~* '(claude\|gpt\|gemini\|sonnet\|haiku\|opus\|llm)')`. |
| **L4 is human-only** | `learning_events`: `CHECK (level <> 'L4' OR (decided_by = 'human' AND approved_by IS NOT NULL))`; `constitution_versions`: `CHECK (authored_by = 'human')`. |
| **L3 must validate before it replaces** | `learning_events`: `CHECK (level <> 'L3' OR validation IS NOT NULL)`; `strategy_versions`: a row with a `previous_version` needs `validation`. |
| **L2 stays inside approved ranges** | `parameters`: `CHECK` that `value_num` sits within `approved_min`/`approved_max` (both sourced from the Constitution). |
| **Every change cites evidence** | `learning_events`: `CHECK (cardinality(evidence_ids) >= 1)`. |
| **Death publishes a post-mortem** | `post_mortems` keyed by `experiment_id`, with non-empty `what_we_kept` and at least one evidence id. |

## Two modelling decisions worth knowing

**Status is history, not a column.** `experiments` is immutable. Every transition is a row in
`experiment_state`; `experiments_current` is the view that derives "where is it now". This is what
makes *"queued → testing → validating → promising → promoted → died"* auditable rather than
overwritable.

**A research cycle is immutable.** `research_cycles` holds one row per turn of the loop, at the
stage it was at. When the loop advances, a new `cycle_id` is minted (`cyc_YYYY_MM_DD_NN`);
`GET /api/pathfinder/loop` reads the latest. Nothing is updated in place.

## Serialisation map (schema → API)

| API model (`docs/openapi.yaml`) | Source |
|---|---|
| `Fact` | `facts` (one row) |
| `Provenance` | the provenance columns of `facts` / `outcomes` / `evidence` |
| `PerformanceBlock` | `outcomes` |
| `VirtualTrade` | `trades` |
| `VirtualBook.ledger_losers_first` | `ledger_losers_first` view, filtered by experiment |
| `Evidence` | `evidence` (+ its `outcome_id`, + `story_lines` for the interpretation) |
| `StoryLine` | `story_lines` |
| `ChangeLogEntry` | `learning_events` |
| `PostMortem` | `post_mortems` |
| `Trigger` | `triggers` |
| `LlmUsage` | aggregate over `llm_calls` |
| `ExperimentSummary` / `ExperimentDetail` | `experiments_current` + the above |
| `LoopResponse` | `research_cycles` + `story_lines` + `facts` + counts over `experiments_current` |

## Running the migration

```bash
psql "$KANIDA_PG_URL" -v ON_ERROR_STOP=1 -f migrations/0001_pathfinder.sql
```

Idempotent — safe to re-run. `pgvector` is optional: if the extension is unavailable, the recall
schema is skipped with a notice and the authoritative schema is unaffected.

> **Still not executed against a live Postgres.** P1 had no reachable database either: no
> `psycopg` in the environment, and the local PostgreSQL 18 instance needs a password this session
> would not supply. `0001_pathfinder.sql` remains **unverified**.

## The SQLite mirror — how P1 actually ran

`migrations/0002_pathfinder_sqlite.sql` is a faithful port of 0001: the **same table names, the same
column names, and the same laws**. The engine ran against it, and rows P1-35…P1-45 of
`docs/TEST_PLAN.md` prove that each product-law CHECK rejects the row it exists to reject — which is
exactly the gap P0 flagged, closed against a database that could actually be executed.

`backend/pathfinder/engine/repository.py` writes identical SQL against either backend; a `_Dialect`
holds the only three differences (placeholder style, array encoding, and how the two regex laws are
enforced). Porting to Postgres is implementing one class, not rewriting the writer.

Dialect differences, listed so none of them is a surprise:

| Postgres (0001) | SQLite (0002) |
|---|---|
| `text[]` | JSON array in `TEXT`; `cardinality(x) >= 1` becomes a `json_array_length` CHECK |
| `jsonb` + `?|` operator | `TEXT` holding JSON; the no-target-price CHECK becomes `NOT LIKE` |
| regex CHECKs (`computed_by !~* '(claude\|gpt\|…)'`, the no-bare-numeral rule) | `BEFORE INSERT` triggers calling `pf_names_a_model()` / `pf_has_bare_numeral()`, registered by `repository.py` on every connection. An unregistered connection cannot insert at all — the correct direction to fail in. |
| one `deny_mutation()` trigger function | one `BEFORE UPDATE` + one `BEFORE DELETE` trigger per history table |
| `bigserial` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| pgvector + HNSW | pointer columns + a JSON embedding, linear scan. Still never a source of truth. |

## One schema change P1 made, and why

`llm_calls` gains **`provider`** (`live:anthropic` | `recorded` | `none`). Without it the table
cannot answer the question that matters most about a narrative sentence: *did a model write this,
just now?* A recorded replay and a live call are different facts about the world, and the record now
says which. Whoever runs `0001_pathfinder.sql` should add the same column and CHECK.

## The S1 research store (`backend/pathfinder/research/store.py`)

Session S1 rebuilt the engine to the locked spec and added a second, self-contained SQLite store
(`KANIDA_PATHFINDER_RESEARCH_DB`, default `var/pathfinder_research.db`) for the clarity-first feed.
Every table is **append-only** (UPDATE and DELETE are rejected by triggers):

| Table | One row per | What it holds |
|---|---|---|
| `pf_editions` | after-close edition (date) | data seal, regime label, universe scanned, candidates considered, the usefulness threshold and every template parameter used (`params_json`), and `engine_version` = `pathfinder_research@<semver>+code.<sha256[:12] of research/*.py>` — the edition is attributable to the exact code that computed it (audit C1); `pf_grades.rule_version` likewise carries the hash of `grading.py` (P2) |
| `pf_findings` | **published** finding | rank, tier, template, subject, decision, novelty key, usefulness, **`grading_rule_json` + `frozen_at`** (the rule as frozen at publication), and `card_json` — the full `Finding` (facts with provenance, digit-free narrative, provenance block) |
| `pf_candidates` | every computed card, published or not | its score components and the reason (`below usefulness threshold` / `held by llm` / `published`) — what the engine chose *not* to publish is on the record |
| `pf_grades` | graded finding (**at most one, ever** — PK) | verdict, the data seal it was graded on, the due session, the rule version, the realised facts |
| `pf_scoreboard` | grading pass | a snapshot of `Right · Wrong · Inconclusive · n · pending` and the per-template split |

The published count of an edition is **derived** from `pf_findings` at read time; the edition row
is never updated. A date that already has an edition is refused, not recomputed. The scoreboard
served by `/feed` is computed from `pf_grades` (the snapshots answer "what did it say on date X");
its `pending` is "published on or before X and not graded on a seal ≤ X" (P4).

A store written by superseded code cannot be corrected in place (append-only): it is **deleted and
rebuilt** (`run_pathfinder_scan.py --fresh`). `engine_version` is how a reader tells them apart.

`Fact.sample_flag` gained `not_applicable` (C8): a parameter (a configured threshold) or a single
observation (today's move, today's z-score) carries no `n` and says so; the flag is still never
derived by hand for a statistic.

## Founder inputs still stubbed

- The **Constitution document** itself (`constitution_versions.document`): risk limits, honesty
  rules, compliance boundaries, permitted actions, and the approved parameter ranges that
  `parameters.approved_min/max` are checked against. P0 assumes `constitution@1.3.0` exists; nobody
  has written its contents.
- The **cost values** behind `costs_v3` (brokerage, STT, exchange, GST, stamp, the slippage
  assumption). P0 uses a placeholder string.
- Whether the Pathfinder schema lives in the **same** Postgres instance as `autotrade_*` or its own.
