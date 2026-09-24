# UX02 acceptance review — 20 September 2026

Status: ready for design feedback, not accepted for implementation yet.
Preview: http://127.0.0.1:8086/ (local illustrative prototype only).

Read the brief, Claude's completion report, live-app walkthrough report, HTML and relevant JS/CSS assets. Independently inspected desktop at 1440x900 and mobile at 390x844 using the approved browser. No production application or backend changes made.

## Keep

- Dark ink/mint hierarchy, prominent market headline, supporting chart, independent calls/puts and progressive evidence disclosure are a useful direction.
- Illustrative-data labeling is explicit in the header and footer.
- Mobile stacks content and switches calls/puts into tabs. No horizontal document overflow in the measured default state.
- Replay exposes Skip; Skip restores Replay. Quiet motion prevents replay. Option-chain drawer opens with the selected reading and Escape closes it. These were independently exercised.

## Corrections before acceptance

1. **Earlier readings must not know the future.** In Capture gap at 09:45, Combined says this is the last confirmed reading today. Verified in the rendered UI. The capture header also uses scenario-level status/detail including later missing readings. Make all context strictly as-of the selected reading; separately label any whole-session summary.
2. **Mobile chart is not readable.** Full-page screenshot at 390px shows tiny axis, time and annotation text. Reflow the SVG for mobile rather than shrinking its desktop coordinate system. Keep readable text and prioritize the relevant prices/time points.
3. **Missing evidence is not absent activity.** Capture gap at 10:15 shows 'nothing participating' beside 'Unconfirmed', although OI was captured and confirmation inputs were missing. Use 'participation unconfirmed'. The drawer also equates a missing evidence row with 'no material change'; use explicit observed-neutral versus unavailable states.
4. **Correct missing-price semantics.** At 10:15 the headline explanation says traded-price average is absent while the call card says premium is missing. Those are not interchangeable. Define the actual missing input consistently across fixture, explanation and evidence.
5. **Replay must preserve chart gaps.** Source inspection shows the trace filters null spots then joins the remaining points into a continuous path. Split into separate segments at missing readings just as the static chart does. This is a source finding; the transient bridge was not visually captured.
6. **Correct historical fixtures.** Capture-gap first-detection claims need a genuine earlier comparison baseline, not a delta against the first reading itself. Weekday labels for 22–24 September 2026 are one day early. Keep synthetic sessions clearly labeled.
7. **Remove review scaffolding from the product surface.** 'Combined — read last', implementation explanations, disabled inspection tiles and repetitive caution copy dominate useful space. Put prototype limitations in a separate design note. Keep concise uncertainty tied to the actual observation. Remove the empty warning-icon row under the headline.
8. **Keep accessibility announcements current.** After replaying Capture gap 09:45 then switching to Buildup broadens, the rendered status region retained the old explanation. Clear/update it on selection changes.

## Verification limits

OS reduced-motion support and cancel-on-change handlers exist in source; OS preference emulation and every intermediate animation frame were not independently exercised. Quiet motion and Skip were exercised. No physical mobile-device testing. Claude reports broader scenario testing and a signed-in Chrome walkthrough; those are builder-reported, not independently repeated here. Desktop side-card summaries extend below the 900px fold, so the report's first-screen completeness claim needs qualification.

Next bounded step: correct this prototype and repeat focused acceptance checks. User can review the visual direction now. No backend integration or deployment until UX alignment.
