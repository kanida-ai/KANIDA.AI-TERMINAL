# Implement Block 1 in the existing derivatives app

User authorized implementation in the running app, not another standalone prototype. Work in this repository. Preserve all pre-existing edits. No deployment, commits, resets, data deletion or unrelated redesign. Read AGENTS.md and the full `docs/Derivative detail questions and answers.docx` before coding. The earlier standalone UX02 design was rejected and is NOT the target.

## Product decision

Keep the topmost two-pane block: existing instrument activity screener LEFT, selected instrument's evolving plain-language explanation RIGHT. All charts below stay connected. Modify the existing ScreenerRail/SignalTable integration; do not add duplicate screeners, searches or context panels. Keep original signal table available through a secondary history/details view if useful. Preserve filters, sort, expand, refresh, selected instrument/expiry/reading, and all lower blocks including the 2x5 strike charts.

The approved writing direction is demonstrated in `C:/Users/SPS/.codex/visualizations/2026/09/20/01a0bf96-ae4a-7a82-9ba8-4c3ab9ed7e93/summary-revised.html`. Read it as a CONTENT/STRUCTURE reference only. Never copy its illustrative market statements into production outputs.

## Writing contract — trader clarity

Headline roughly 6–10 words: specific activity and location. Body 2–3 short connected sentences: what happened, supporting evidence, what changed since the prior reading. Context line: first appearance, elapsed duration, breadth and leading strike when supported. Calls and puts analyzed independently then related; futures/spot/IV context only when relevant. Avoid metric-by-metric compulsory paragraphs, blended scores, simplistic strong bullish/bearish summaries, and vague 'activity spreading' without type and location. Avoid 'participation', 'confirmation', 'cluster' where ordinary words explain the same fact more clearly. Use 'consistent with' for inferred buying/writing; don't claim known participant intent.

Example STYLE ONLY: 'Call buying broadens across 23,350–23,450 CE. 23,450 CE has joined the earlier buying at 23,350 and 23,400 CE: new positions, option prices and IV have risen together. 23,350 CE remains the busiest, while buying has spread to a third nearby strike. First seen at 09:30 · continuing for 45 minutes.' Every clause must be backed by actual stored data, otherwise omit/qualify it. Three scans at 09:30/09:45/10:00 are 30 elapsed minutes, not 45.

Timeline advances the story: appeared, continued, broadened, slowed, faded, unwound. A quiet scan can say 'No material change; the earlier buildup remains intact' ONLY when evidence supports continuity. Missing data is not quiet activity or an automatic break/reversal.

## Actual data and wiring

1. Trace the existing capture/store -> API -> shared page reads -> OI grid/signal pipeline before modifying it. Start with src/derivative/{index,ScreenerSection,SignalTable,logic,useDerivatives,types}.tsx/.ts and server/kanida_pilot/derivatives.py plus underlying metrics modules. Inspect existing AI/event marks and reuse only compatible verified behavior. Do not assume the unintegrated market_intelligence.py is an approved engine.
2. Reuse stored historical readings. No synthetic production statements, made-up history, static narrative strings unrelated to payloads, external LLM call per user, or new market-data provider. Add only bounded additive APIs/adapters necessary for this block. Preserve existing analytics/ranking/liquidity thresholds; do not silently redefine them.
3. Structured observations first, language second. Each statement needs instrument, expiry, selected timestamp, compared timestamps, strikes/side, observed fields, inferred behavior, uncertainty and source coverage. Build the deterministic explanation from those observations, shared/reusable by context, not rediscovered independently for each user. No blended OI/IV/PCR/max-pain score. Each metric retains its meaning and baseline.
4. Read only data AT OR BEFORE the selected timestamp. Consistent instrument and expiry. Cancel or reject stale asynchronous responses on selection changes. If a panel has only an earlier reading, explicitly label it and never combine mismatched timestamps into a fresh claim. Don't extend persistence across unobserved intervals as though measured. Don't infer fresh buying from OI alone. Don't equate missing average traded price/turnover with missing option LTP.
5. Preserve the activity list's existing search semantics and say when a searched stock isn't in the filtered results. Do not relabel absence as 'nothing happening'. Avoid adding a second search. If safe explicit instrument lookup can reuse existing selection infrastructure, retain filters and explain excluded/no-data states rather than bypassing them silently.
6. Summary selection and history use the page's shared reading/target controls; all lower views follow the same selection. Evidence control names actual supporting readings and metrics; relevant strike links use existing shared highlight/selection wiring. Keep 2x5 charts intact.

## Motion — implement, don't merely describe

On explicit replay and initial completed selection, reveal explanation in short readable phrases over ~2–4 seconds, not character-by-character typing. Reserve final text height so cards don't jump. Replay and Skip always available; no loop or repeated animation on refresh. Cancel old animation immediately on instrument/reading change. Quiet-motion preference plus OS reduced-motion presents complete text immediately. Screen readers receive the complete summary once, not every phrase.

Use a restrained hand-drawn-style underline/bracket on the cited strike/context, synchronized with the relevant phrase. Where supported by existing chart hooks, trace ONLY the referenced historical interval and highlight the cited strikes, preserving gaps and respecting reduced motion. Do not invent new chart data or disturb existing chart behavior. Report any chart synchronization not implemented honestly.

## Validation and result

Inspect current git diff before edits. Run relevant existing backend tests, frontend type/build checks and focused tests for as-of isolation, instrument/expiry switching, missing baselines, persistence elapsed time, conflicts and stale responses. Do not claim regression-free based on compilation alone.

Use the actual authenticated Chrome app at http://127.0.0.1:8082/derivative. Verify search/clear, filters, row selection, history selection and lower chart synchronization, refresh, expand, loading/error/no-data, desktop/mobile, Replay/Skip, cancel-on-change and reduced motion. Capture concrete actual-data examples: one index and one stock if available, with source timestamps. If the available data cannot support buying/writing, show understandable measured facts and the limitation instead of forcing the example narrative.

Write `docs/CLAUDE_BLOCK_01_RESULT.md` only when finished or blocked. Include changed files, data path, actual output examples and underlying evidence, tests and failures, visual verification, any unimplemented motion/wiring and preview URL. Do not claim completion while checks remain unverified. Notify in the Claude conversation when done. Scope ends after this block; no other redesign or deployment.
