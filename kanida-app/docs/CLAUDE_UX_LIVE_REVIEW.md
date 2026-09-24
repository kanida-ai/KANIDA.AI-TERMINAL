# Required authenticated Chrome walkthrough

Owner clarification: the existing app is already open and signed in in Chrome at `http://127.0.0.1:8082/derivative`. Use that existing Chrome tab and profile for the end-to-end review. A fresh built-in browser session returning SIGN_IN_REQUIRED is not evidence that the user's actual app is inaccessible.

Codex directly verified the existing Chrome tab on 20 September 2026. It shows the authenticated Derivative page, NIFTY, session 2026-09-18, reading 15:45 IST, partial capture, a populated screener and the signal table. The page accessibility tree exposes all remaining sections. The initial viewport screenshot confirms the app's dark/mint visual language and dense table hierarchy. Codex has not yet tested all interactions; perform and record that work yourself.

Before finalizing the UX02 prototype, use your available Chrome integration to locate the existing tab. Discover your own tab identifiers; do not assume Codex's tool handles work in Claude. If your Chrome connector genuinely cannot see the tab, inspect its available connection/status tools and report the exact connector limitation. Do not bypass authentication, extract cookies, reset accounts, or invent a completed visual walkthrough.

## Walkthrough checklist

Record actual observations and relevant screenshots in a concise `kanida-app/docs/CLAUDE_UX_LIVE_RESULT.md`. Preserve the current app and restore changed selection/filter settings afterward.

- Scroll from top to bottom through screener, signal table, chain, OI by strike, delta OI tiles, PCR, max pain, IV, futures, and index sections. Record spacing, hierarchy, legibility, clipping, and empty areas in the actual rendered UI.
- Select NIFTY and one stock such as TCS. Verify which linked charts and panels follow the selection.
- Select an earlier available reading and return to the latest. Record mismatched timestamps and stale/missing observations exactly; do not infer successful synchronization from source alone.
- Exercise strike selection, chart interaction, expand/collapse, filter dialog, and evidence/help disclosures. Close dialogs and restore the starting state.
- Check scrolling at table boundaries and whether nested panels trap the page.
- Inspect a narrow/mobile layout if your browser tools support a temporary viewport, and restore the original viewport afterward. Report desktop versus mobile coverage honestly.
- Distinguish working features that should be preserved from layout issues and data gaps that require future backend work.

Use these findings to refine the approval prototype. Do not change production application code in this task. The existing app walkthrough is an acceptance requirement alongside the standalone prototype checks, not a reason to discard or indefinitely postpone the prototype.
