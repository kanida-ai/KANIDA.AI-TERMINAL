# Pathfinder — Build Session S1: ENGINE CORE

> Paste into a fresh agent-mode session in this repo. First read `CLAUDE.md`, then
> **`docs/sessions/PATHFINDER.md`** (the LOCKED spec — its top-line governing sentence is law), and
> `docs/DOC_MAP.md`. Then read the **reference implementations** in
> `C:\Users\SPS\Documents\Kanida_Falcon`: **`pathfinder_theme.py`** and **`pathfinder_demo.py`** — the
> engine's math is already proven there on real data. **Port it; do not re-derive it.**

## Governing law (non-negotiable)
*"Pathfinder computes market evidence first and uses GenAI only to decide what is worth investigating and
to explain verified results."* **No number ever originates in the LLM.**

## Goal
Build the deterministic Pathfinder research engine core: after-close scan → **question library** → computed
**evidence with provenance** → **rank by usefulness** → publish only what clears the usefulness threshold →
**grade** published findings → serve a **clarity-first feed API**. The LLM (via the existing
`pathfinder_llm` gateway) only *selects, prioritizes, narrates* — every number is deterministic.

## Scope — IN
1. **Question library** (spec addendum 2) — template objects: `question → parameters → computation →
   evidence card`. **Seed with the 5 templates already proven in the prototypes** (port their math):
   THEME/CYCLE (sector in play + proof + watchlist), GROUP base rates DIP and SURGE, VOLUME ANOMALY,
   RELATIONSHIP (pair spread break + convergence), MARKET REGIME. New templates are added deliberately
   (founder-reviewed); the model never fabricates a test.
2. **After-close scan** — run the relevant templates on the latest close; the gateway **selects** which
   questions are relevant today and **ranks** findings by usefulness / novelty / evidence-strength /
   trader-relevance. **Usefulness threshold gates publication — NO minimum count, NO padding** (addendum 1;
   "four findings today" is a valid edition).
3. **Evidence + provenance on every card** (addendum 7) — sample size, period, market regime, comparison
   group, transaction-cost hurdle, and the provenance level: **same stock / peer group / sector / whole
   market.** Reuse `state_engine/baseline.py` + `regime.py`.
4. **Grading** (addendum 4) — per-finding-type **Right / Wrong / Inconclusive** rules **frozen before
   publication** (e.g. NO TRADE is Right if the trade would have lost after costs; a theme call is Right if
   the sector beat Nifty over the stated horizon; Inconclusive if inside the cost hurdle). A persisted,
   **append-only scoreboard** (`Right · Wrong · Inconclusive · n`). Grade prior findings when their horizon
   completes. Reuse the prototype's verdict logic.
5. **Feed contract** — extend `docs/openapi.yaml`: `GET /api/pathfinder/feed?date=` returns the ranked
   findings **clarity-first** (first 2–3 = "what matters now"), each with: narrative (LLM, **digit-free**) +
   `facts[]` (computed) + provenance + grading state. Reuse the existing `{{fact:…}}` token pattern.
6. **Honesty enforced in code** — the gateway contract (AI prose carries no digits); every fact carries
   provenance; the cost hurdle is applied; samples < thresholds are labelled.

## Scope — OUT (later sessions)
- The virtual-capital **experiment loop**: registry, versions (v1/v2…), trial counts, retirement, forward
  virtual tracking, expected-vs-actual (S2).
- The **graduation / champion-challenger promotion gate** to Trader/Investor + NDP null-calibration / arena
  gate integration (S2).
- The **swipeable clarity-first frontend feed** (S3).

## Reuse (do not re-derive)
`pathfinder_theme.py`, `pathfinder_demo.py` (reference math on real data) · `engine/state_engine/baseline.py`,
`scripts/regime.py` (R&D) · `db/kanida.db` (data) · the existing `backend/pathfinder/` gateway + contract
scaffolding (the AI-narrates/engine-computes seam already exists).

## Done when
- The scan produces **real, ranked, clarity-first findings** from real data for the latest date, honoring
  the usefulness threshold (it can legitimately publish "4 findings today").
- Every card carries full **provenance** + a **frozen grading rule**; the **scoreboard persists** and grades
  prior findings when their horizon closes.
- `GET /api/pathfinder/feed` serves them; **narrative is digit-free** (numbers are computed `{{fact}}` refs).
- **`dev-quant-auditor`** confirms: no number originates in the LLM; cost-hurdle / grading / provenance are
  real; the question library is template-based (no fabricated tests).
- Hand-back `docs/handbacks/PF-S1.md`.

## Founder inputs (stub + flag if missing)
Starter question-template set (5 seeded; add yours) · usefulness-threshold parameters · cost-hurdle value ·
per-type grading rules (defaults from the prototype — confirm).
