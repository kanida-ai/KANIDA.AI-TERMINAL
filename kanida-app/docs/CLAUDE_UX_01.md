# KANIDA derivatives UX review

## Objective

Review the existing derivatives experience and propose a concrete UX revision before implementation. The owner has chosen UX first, followed by backend contracts, end-to-end integration, and a gated AWS launch. Codex owns product direction and acceptance review; Claude Code is the implementation partner. This first task is a bounded review, not a redesign implementation.

## Workspace and existing work

- Repository: `C:\Users\SPS\Documents\Kanida_Falcon`
- Local product: `http://127.0.0.1:8082/derivative`
- Current branch at handoff: `codex/market-intelligence-engine`. Check current status before working; do not change branches automatically.
- There are pre-existing uncommitted changes plus Codex's preliminary registration-mode changes and an unfinished, unintegrated `server/kanida_pilot/market_intelligence.py`. Preserve all of them. Backend work is paused while UX is aligned.
- Do not reset, stash, clean, commit, push, deploy, install packages, change credentials, or alter market databases for this review.
- Do not read or include secrets, broker tokens, private invitation files, or customer data in the report.

## Focused inputs

Read `kanida-app/AGENTS.md`, then:

1. `kanida-app/docs/Derivative detail questions and answers.docx` for the owner's UX requirements. Extract its text locally if needed.
2. `kanida-app/src/derivative/index.tsx`, `frame.tsx`, `SignalTable.tsx`, `ScreenerSection.tsx`, and relevant components they directly use.
3. `kanida-app/docs/DERIVATIVE_TAB_AUDIT.md` and `kanida-app/docs/derivative-audit-2026-09-19/README.md` as prior findings, not proof of current behavior.

Inspect the authenticated running screen if your browser tools have an authorized session. Do not bypass authentication or create an account. If blocked, complete the source review and identify exactly which visual observations remain unverified. Do not reread the entire repository or explore unrelated research engines.

## Product direction

KANIDA should explain what matters now and how it evolved, reducing manual interpretation of option-chain numbers. Structured intelligence comes before narrative generation. The UX must make room for:

- Instrument, expiry, session, and selected reading as one clear context, with freshness and replay status.
- A primary explanation of what matters now: headline, plain-language explanation, and location/persistence context.
- Calls and puts interpreted independently before any combined conclusion.
- An evolving session timeline: appearing, building, continuing, broadening, concentrating, shifting, slowing, fading, unwinding, covering, reversing, or mixed/unconfirmed.
- Expandable evidence identifying the relevant strikes, comparison baseline, observed metrics, and uncertainty.
- Clear differences between isolated activity and wider participation, and between observed facts and inferred behavior.
- Honest loading, missing-data, stale, partial-capture, and no-material-change states.

Keep useful existing charts, selection interactions, and evidence tools. Evaluate where they belong in the hierarchy. Do not add widgets merely to fill space. Identify unnecessary fixed heights, oversized gaps, scroll traps, repeated explanations, and sections that require the trader to do the interpretation.

NSE is the launch market. Access stays gated initially, with public registration configurable later. Kite is already integrated; the owner is negotiating with a market-data vendor. AWS is already the intended cloud platform. Do not ask the owner to repeat these decisions.

## Deliverable

Write only `kanida-app/docs/CLAUDE_UX_01_RESULT.md`. Keep it focused, ideally under 1,200 words:

1. What you actually inspected: source versus live-screen observations; relevant viewport sizes and authentication limitations.
2. A prioritized table of up to eight UX issues, including code locations and the practical impact on a trader.
3. A proposed desktop and mobile content order or concise text wireframe.
4. A precise first implementation slice: affected components, preserved interactions, and observable acceptance criteria.
5. Which proposed UI elements can use current API outputs and which require clearly labelled illustrative fixtures until backend work resumes.

No application code changes in this first review. Finish with a short handoff naming the report and any blockers. Codex will review it and issue the implementation brief; the owner should not have to copy your full response between tools.
