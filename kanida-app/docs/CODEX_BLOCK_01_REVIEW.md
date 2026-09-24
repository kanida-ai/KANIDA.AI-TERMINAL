# Block 1 review — 20 September 2026

Status: implemented first pass, NOT accepted as complete.
Preview: http://127.0.0.1:8082/derivative (reload to load rebuilt bundle).

## Independently verified

- Read implementation brief, result report, summary.ts and SummaryPanel.tsx.
- Reloaded authenticated Chrome app: right-hand summary exists beside the original screener.
- Selected NIFTY 10:30 through the summary history control; loaded narrative changed to that reading. Initial page was 15:45 with the summary at 11:30.
- Replay then Skip restored the complete-state controls; Quiet motion made Skip disabled on replay.
- Re-ran scripts/check-derivative.cjs: 416 checks passed.
- Narrow viewport test rendered a 433 CSS-pixel layout (requested 390, browser zoom affects measurement). Screener and summary stack. Viewport override reset. No claim of full mobile acceptance.

## Must fix

1. Shared reading wiring remains incomplete. Builder explicitly reports PCR, max pain, IV and futures ignore the selected reading. This is within the required wiring contract, regardless of whether the defect predates the change. Prominent summary context must distinguish its actual reading from page time.
2. Gap persistence: observe() keeps run.first through missing readings, then sets run.last to the resumed timestamp and subtracts first from last. This includes the gap on resumption, despite the comment claiming otherwise. Test missing AND resumed data, not only the missing endpoint. Distinguish elapsed wall time from confirmed continuity.
3. Busiest strike is selected solely by largest absolute OI change. Either label it precisely as 'largest position change' or implement the evidence-backed behavior-specific leadership required by the vision. Do not call this full busiest-strike reasoning.
4. First comparison is misrepresented as a new market event. At 10:30 NIFTY says 'Nothing comparable was showing at the reading before this one', but the preceding scan lacks the 60-minute comparison baseline. Say when KANIDA first had enough evidence; do not assert the earlier market lacked activity.
5. summary.ts adds PACE_UP=1.25 and PACE_DOWN=0.75 despite the report claiming no new thresholds. Pace is rolling-window absolute OI change, which does not necessarily describe latest-scan acceleration. Separate existing baseline behavior from actual interval change; document and validate any new analytics.
6. Body remains five technical sentences with raw deltas and repeated range/time references. Follow the approved short headline + 2–3 connected sentences + persistence line. Put numeric details behind evidence. 'Put strikes are not moving open interest and price together in the same way' is unclear trader language.
7. Summary currently uses only OI and option price from the ten-strike grid. It correctly avoids invented IV, but has not integrated available IV/futures or other relevant contextual evidence. Do not present this as the complete requested contextual intelligence.
8. Chart interval tracing is unimplemented (builder admission). Existing strike links are not synchronized interval replay. Skip is disabled once phrases finish, while the underline may still be animating; the full animation should remain skippable.
9. At desktop size the current header content consumes most of the first screen and the summary is a small nested scrolling panel. The important context is below its fold. Reading-table mode nests another header. Improve the right pane without duplicating controls.

## Verification limits

Full backend suite/typecheck results are builder-reported, not independently rerun. Full search/filter/expiry/strike regression, OS reduced-motion, screen-reader announcement and cancellation during a pending fetch remain unverified. Builder used synthetic dispatched events for its interaction checks; this review successfully used browser semantic clicks for history, Replay, Skip and Quiet. The source has no aria-live on the summary announcement, so a static accessible label alone does not prove automatic announcement on updates.

No application/backend changes made during this review. Monitor paused after delivering review as requested. Next step is a bounded correction pass on these findings before acceptance.
