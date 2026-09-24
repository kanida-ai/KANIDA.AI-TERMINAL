# Derivative product context

Updated: 2026-09-20

## Role and source of truth

Act as KANIDA.AI's CTO and product architecture council: connect product clarity, market intelligence, engineering correctness and scalability. Authoritative user brief: `Derivative detail questions and answers.docx` in this directory, read in full on 2026-09-20. The user's pasted path had an extra folder separator; the document was found in the current Kanida_Falcon workspace.

Maintain this context as discussions proceed. Record accepted decisions separately from proposals, along with limitations and outstanding work. Preserve the original brief.

## User direction

Show only what helps a trader understand the market. Premium and cinematic means focused hierarchy, readable explanations, restrained motion and clear evidence. More metrics is not a requirement. Current request is review and recommendations for Block 1, including columns, rows, checks and combinations; application implementation has not been requested in this turn.

## Architecture and intelligence principles

Build a Market Intelligence State Engine, not raw-data commentary. Pipeline: market data, normalization, state engine, event detection, session memory, cross-market reasoning, historical context, structured intelligence, language, experience. Deterministic evidence precedes language generation. Architecture supports indices, stock options, futures, all expiries and dynamic strike expansion. Launch focuses on liquid index options. User clarification on 2026-09-20: 15-minute snapshots are the core refresh mechanism, not merely a presentation choice. Interval and session intelligence must be constructed from those snapshots. More frequent upstream observations, if ever available, must not silently change this product contract.

Maintain previous scan, open, previous close, session extremes and historical baselines. Preserve the session story and original strike history as ATM moves. Analyze calls and puts independently before correlating with spot, futures and volatility. Track appearance, persistence, broadening, concentration, leadership shifts, acceleration, fading, unwinding and reversals. Missing data and one noisy scan must not falsely establish or break continuity.

Language first, numbers as evidence. Use a specific headline, 2–3 short explanatory sentences and natural persistence context. Explain where, what changed, duration, breadth and supporting evidence. Keep uncertainty and conflicting evidence explicit. Buying/writing are evidence-supported inferences, not proven from OI alone. Avoid default simplistic bullish/bearish labels, opaque scores and trade instructions. Context-aware leadership cannot be inferred from one metric alone. Timeline and end-of-day narrative remain required; unchanged scans update state without adding repetitive visible events.

## Review evidence

Read the brief, current ScreenerSection, UnusualWidget and SignalTable components and earlier CODEX_BLOCK_01_REVIEW.md. Viewed the authenticated Chrome derivative page and a screenshot on 2026-09-20. The separate in-app browser redirected to welcome. No reference image arrived with the message; Block 1 is interpreted as screener + signal-by-reading table + What is happening panel, consistent with the implementation and live view.

Live layout: three cramped panels; screener requires horizontal scrolling, signal table repeats readings, story is smallest with internal scrolling and many controls. Screener reading was 18 Sep 15:45; signal and narrative were 11:30. UI showed Nothing flagged alongside very high volume ratios; eligibility and unavailable inputs need explanation before claiming a quiet market. Existing review records additional unresolved continuity, leadership and baseline issues; these were not re-tested in this turn.

## Proposals awaiting user decision

- Two primary panes: compact instrument activity list approximately one-third width and market story approximately two-thirds. Session changes beneath the story. Keep detailed signal table available as an optional evidence/history view rather than deleting its capability.
- Default screener: Instrument and What changed; add concise location and duration within the activity row once the engine supports them. At most three columns: Instrument, Activity, Since. No fabricated duration or directional narrative from current aggregates.
- Move Contracts, Premium INR cr, aggregate daily OI change, Vol/OI over 1 and highest volume-vs-median to evidence. Spot belongs in selected instrument header. Expiry and DTE combine into one selected-context control; event rows must identify another expiry when applicable. Replace Busiest contract with an evidence-supported activity location; until supported label its actual ranking metric precisely.
- Start with about 6–8 relevant rows, Show all and search. Default to meaningful measured activity; keep selected/watchlisted names accessible. Do not equate missing data with quiet activity, silently exclude unavailable instruments or lose active fading/reversal stories.
- Merge repeated OI interpretation and market signal into concise event language. Call/put evidence remains independently inspectable. Show meaningful changes in the timeline; compress unchanged intervals, retain data gaps and the complete scan history on demand.
- One shared context strip: instrument, expiry, reading/session mode and freshness. One Filters entry point, one refresh, one evidence access path. Move replay/motion preferences and technical definitions to secondary controls. Remove repeated headings, explanatory subtitles, rule IDs and long coverage/floor footers from primary view.
- Preserve backend liquidity, baseline, synchronization, missing-data, uncertainty and expiry checks. Summarize material limitations visibly in one short status with details on demand. Simplifying presentation must not remove validation or make incomplete readings appear current.
- Cinematic treatment: legible text, generous spacing, restrained color, subtle event transitions, no delayed access to the conclusion. Respect reduced motion.

## Decision log

- Follow-up wireframe response reviewed from attachment 39c77999-ae38-463a-a910-6395c3d40daf/Pasted text.txt. CTO recommendations, not user-approved implementation: keep scoped cross-market rows instead of requiring prose; dots must describe calibrated support for a specific claim or be omitted, never generic instrument confidence. Previous live inspection supports 9 of 26 IV values for one NIFTY session, not a global failure rate. Immediate semantic fix must qualify or replace unsupported buying/writing labels throughout screener, tiles and stories; adding spot movement alone is insufficient. Model residuals may support inference but do not prove participant intent; account for time, nonlinear spot response, quote quality and dependence of price-derived IV. Put track expands when a qualified event changes the selected explanation or an existing put episode changes; missing put coverage remains visible. Max Pain available in advanced evidence, no permanent row and no automatic directional interpretation. Same-context fixed-strike interval ladder, with session history separate; label largest OI addition by interval rather than carry that label across history without evidence. Keep baseline/date/expiry placeholders explicitly illustrative. Structure can proceed with conservative copy; quant attribution is a separate validated milestone.

- User supplied a new text wireframe for review (attachment b3f77d05-ca08-4155-a0d1-997f9c7bbecb/Pasted text.txt). No implementation approval given. Proposed assessment: keep compact market list, latest interval, separate session story, strike ladder, expandable evidence and mobile list-to-detail navigation. Required corrections: label ladder metric/window rather than equate largest delta OI with busiest; put spot 23,302 between 23,300 and 23,350; distinguish flat/unflagged/missing strikes; do not use check/cross icons to treat contextual metrics as directional votes; remove default Max Pain row; resolve puts unchanged versus puts thinning contradiction; use matched windows and explain PCR denominator effects; classify position reductions as unwinding rather than automatic reversal; don't present flat IV as an automatic conflict with falling premium; avoid future same-session timestamps (TCS since 14:45 at 11:30); preserve expiry on mobile. Attachment claims about IV failure frequency and backend wiring are unverified, and approving a visual would not automatically approve backend scope.

- User clarification, 2026-09-20: preserve three distinct intelligence layers: current market/screener (why this name matters), latest 15-minute interval (what changed), and persistent full-session intelligence (how the story evolved). Evidence/details support all three. This supersedes the earlier suggestion to merge interval intelligence into a single session narrative.
- Full-session relevance is not restricted to a last-45-minute or other arbitrary display window. Do not expose arbitrary persistence scores or repeatedly explain refresh mechanics.
- Scope expanded to end-to-end product/architecture design across data, quant inference, AI, interaction, desktop/tablet/mobile and millions-of-users scale. Recommendations must challenge weak assumptions and remain proposals until accepted. Do not ask the user to restate established concepts.
- Detailed proposed design is saved in DERIVATIVE_INTELLIGENCE_EXPERIENCE_SPEC.md. The proposal favors a compact market selector alongside separately visible latest-interval and session narratives, with shared evidence and selection state.

- Accepted by user: source document establishes role and product principles; keep context updated; essential information and premium cinematic presentation take priority over metric quantity.
- Proposed only: all specific layout, column and row changes above.
- No application code changed during this review.

## User correction and rollback — 2026-09-20

The user rejected the implemented BlockOne workspace and requested rollback. Restored the pre-implementation ScreenerSection, reversed only this implementation's test changes, removed its three new source/test files, and rebuilt dist-pilot. Pre-existing work remains intact.

The user's pasted two-pane wireframe is now the explicit design target, superseding generic layout proposals above: compact What's Happening list with activity, since/location and evidence strength; selected instrument/expiry/time header; concise latest change; inline OI-added strike ladder with new/since/leading annotations and spot marker; compact put comparison; separate Since narrative; scoped cross-market rows; evidence and downstream links at the bottom. Preserve this exact composition and density when implementing; do not substitute an enlarged vertically stacked workspace, permanent Calls/Puts tabs, or generic net-position screener copy. Example numbers and claims require supporting data, not hardcoding. Rollback does not constitute delivery of this target.
