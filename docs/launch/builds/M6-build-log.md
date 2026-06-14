# M6 — Legal — Build Log

**Module:** M6 (Legal)
**Agent:** BuildAgent-M6
**Date:** 2026-06-13 (IST)
**Contract refs:** CONTRACT §6 (file ownership), §7 (invariants) · MASTER_SPEC "M6 — Legal"

---

## 1. Files written / changed

| File | Change |
|---|---|
| `frontend/app/legal/layout.tsx` | **New.** Chrome for `/legal/*` (brand bar + footer cross-linking all four docs). |
| `frontend/app/legal/_components.tsx` | **New.** Shared server components: `DraftBanner`, `LegalHeader`, `Section`, `LegalArticle`. Underscore = private, not a route. |
| `frontend/app/legal/terms/page.tsx` | **New.** Terms of Service. |
| `frontend/app/legal/privacy/page.tsx` | **New.** Privacy Policy. |
| `frontend/app/legal/refund/page.tsx` | **New.** Refund & Cancellation Policy (₹999/mo monthly India). |
| `frontend/app/legal/risk/page.tsx` | **New.** Risk Disclosure (SEBI-aware). |
| `docs/launch/legal/terms.md` | **New.** Prose source of truth + lawyer-review HTML comment block. |
| `docs/launch/legal/privacy.md` | **New.** Prose source of truth + lawyer-review block. |
| `docs/launch/legal/refund.md` | **New.** Prose source of truth + lawyer-review block. |
| `docs/launch/legal/risk.md` | **New.** Prose source of truth + lawyer-review block. |
| `frontend/app/power/layout.tsx` | **Edited (footer only).** Added Terms/Privacy/Refund/Risk nav to the Power-surface `Footer()`. |
| `frontend/middleware.ts` | **Edited (1 line + comment).** Added `/legal` to `POWER_PORTAL_PATHS` so the public legal pages bypass the site-wide HTTP Basic Auth dialog. |

All in M6's CONTRACT §6 ownership (`frontend/app/legal/*`, `docs/launch/legal/*`) except two
deliberate, minimal cross-file edits flagged under **Deviations** below. No `backend/falcon/*`,
no operator routes, no backend code touched.

---

## 2. What was built

### Four legal pages (`/legal/{terms,privacy,refund,risk}`)
- Server components, `export const dynamic = 'force-static'` — static content, no client JS,
  no data fetching (matches the `credibility` page convention).
- Match the site's black / `mint-*` / `neutral-*` Tailwind language. Built on Next.js 16 /
  React 19 app-router (read `node_modules/next/dist/docs/` layouts-and-pages guide first;
  conventions matched against existing `power/credibility/page.tsx`).
- Each page renders the **mandatory visible DRAFT banner** at the top
  ("DRAFT — pending legal review. Not yet legally binding.") via the shared `DraftBanner`
  inside `LegalArticle`.
- TSX prose mirrors the markdown source; markdown is the editable source of truth.

### Source markdown (`docs/launch/legal/*.md`)
- Full prose so legal can edit/review without touching TSX.
- Each file opens with an **HTML comment block** listing the exact claims/sentences a
  SEBI-savvy lawyer must review (consolidated in §4 below).

### Content (India + SEBI-aware)
- **Risk Disclosure** explicitly states: informational/educational only, NOT investment advice;
  not a SEBI-registered intermediary; past performance ≠ future results; and that win rates,
  backtests, walk-forward results, and the **"Live" persona portfolios are MODEL / simulated /
  hypothetical track records, not real customer capital.**
- **Refund** reflects the **₹999/mo, monthly, India, auto-renewing** plan, Razorpay processing,
  no pro-rata refund, and access-until-`current_end` cancel behaviour (aligned with CONTRACT
  M3 cancel semantics).
- **Terms** carries the core "not an investment adviser" disclaimer, the ₹999/mo subscription
  clause, and a "Live"/performance clause that points to the Risk Disclosure.
- **Privacy** is DPDP Act 2023 + IT Act 2000 aware (Data Fiduciary, Grievance Officer,
  data-principal rights, Razorpay = no card storage).

### Footer integration
- Added a 4-link nav (Terms / Privacy / Refund / Risk) to the **Power surface footer only**
  (`app/power/layout.tsx`'s inline `Footer()`).
- The `/legal/*` pages also carry their own footer (in `app/legal/layout.tsx`) cross-linking
  the four docs.

---

## 3. Footer integration approach + INV2 safety

The Power footer is an **inline `Footer()` function inside `app/power/layout.tsx`** — it is NOT
a shared component. The operator surface `/falcon/*` has a **separate** `app/falcon/layout.tsx`
and does not render this footer. Therefore adding the legal links touches the public Power
surface only and leaves operator-only nav untouched (INV2 / INV6 spirit honoured). No shared
component was modified.

---

## 4. LAWYER-REVIEW CHECKLIST (the important part)

> Authored by an AI build agent as review-ready templates — **NOT legal advice.** Every item
> below is a claim/sentence a SEBI-savvy Indian lawyer must confirm or rewrite before these
> pages go public or are relied upon. Same list is embedded as an HTML comment at the top of
> each markdown file.

### Risk Disclosure — `risk.md` (HIGHEST PRIORITY)
- **K1.** "informational and educational only … does NOT provide investment advice" — core SEBI
  disclaimer; confirm it keeps us outside SEBI (Investment Advisers) Regs 2013 / (Research
  Analysts) Regs 2014; confirm we never make personalised recommendations.
- **K2.** "not registered with SEBI as an investment adviser, research analyst…" — confirm true
  at launch and that no registration is required for what the product does.
- **K3.** "Past performance is not indicative of future results." — match SEBI's prescribed
  disclaimer phrasing for performance claims.
- **K4.** "All performance figures, including the 'Live' persona portfolios, backtests, and
  walk-forward results, are MODEL / SIMULATED / HYPOTHETICAL track records, not returns earned
  on real client capital." — confirm this accurately describes the product (Live tier =
  model ENTER/WAIT/SKIP decisions + simulated portfolios, NOT actual customer trades). Highest-
  risk claim to get wrong.
- **K5.** The specific headline marketing numbers (e.g. "₹30L → ₹1.05Cr in 3.3 years", "+250%",
  win/hit rates) — confirm each is substantiated, sourced, and shown with the simulated/
  hypothetical caveat; confirm SEBI rules on advertising returns.
- **K6.** "No assurance or guarantee of returns." / "You may lose some or all of your invested
  capital." — confirm risk-warning sufficiency.
- **K7.** Language about the engine "noticing"/"ranking"/"picking" stocks — confirm it cannot be
  construed as a personalised buy/sell recommendation.
- **K8.** Confirm whether stronger signup-time consent text is required and whether the checkbox
  consent log must be retained as evidence (signup checkbox is built in M7).

### Terms of Service — `terms.md`
- **T1.** "not an investment adviser, research analyst, stock broker, or portfolio manager" —
  confirm we perform no SEBI-regulated activity (the load-bearing disclaimer).
- **T2.** "Nothing … is a personal recommendation to buy, sell, or hold any security." —
  sufficiency to stay outside "investment advice".
- **T3.** "₹999 per month, billed monthly, auto-renewing" — confirm vs Refund Policy and
  Razorpay mandate / e-mandate (UPI Autopay / recurring card) disclosure requirements.
- **T4.** Limitation-of-liability + indemnity (§9–10) — enforceability under Indian Contract Act
  1872; defensibility of the 3-month-fees liability cap.
- **T5.** "Live" / model-portfolio language (§4) — must align exactly with the Risk Disclosure;
  scrub anything readable as a performance promise.
- **T6.** Governing law / jurisdiction (§13) — confirm seat and courts-vs-arbitration.
- **T7.** Eligibility "18+ and resident in India" (§2) — confirm intent to exclude NRIs /
  non-residents.
- **T8.** Account/data references — consistency with Privacy Policy + IT Act 2000 + DPDP 2023.
- **T9.** Fill `[LEGAL ENTITY NAME]`, `[CIN]`, `[REGISTERED ADDRESS]` before publication.

### Privacy Policy — `privacy.md`
- **P1.** DPDP Act 2023 alignment — "Data Fiduciary" framing, lawful basis, required notice
  elements; whether a Consent Manager / grievance mechanism is mandated.
- **P2.** "We do not store your full card/bank details; payments handled by Razorpay." — confirm
  accurate vs M3 billing and that we are out of PCI scope.
- **P3.** Data-sharing list (§4) — confirm actual processors (Razorpay, email provider, hosting/
  Supabase, analytics if any) and that DPAs exist.
- **P4.** Cross-border transfer language (§5) — confirm where data actually resides (Supabase
  region) and DPDP transfer rules.
- **P5.** Retention periods (§6) — replace placeholders with real, defensible periods.
- **P6.** Data-principal rights (§7) — access/correction/erasure/grievance wording vs DPDP; name
  a Grievance Officer with contact (DPDP requirement).
- **P7.** Children's data (§8) — DPDP restrictions; confirm 18+ gate is sufficient.
- **P8.** Fill `[LEGAL ENTITY NAME]`, `[GRIEVANCE OFFICER NAME/EMAIL]`, `[REGISTERED ADDRESS]`.

### Refund Policy — `refund.md`
- **R1.** "₹999/mo, billed monthly" + auto-renewal mechanics — confirm vs actual Razorpay
  recurring/e-mandate setup and Terms §5.
- **R2.** "no pro-rata refunds for the unused portion of a cycle" — enforceability under Consumer
  Protection Act 2019; whether a cooling-off / first-time refund window is advisable.
- **R3.** "Cancel any time; access until end of current paid period; no further charges." —
  confirm matches M3 cancel endpoint (status→cancelled, access valid until `current_end`).
- **R4.** "Free accounts have no charges and therefore no refunds." — confirm wording.
- **R5.** Chargeback/dispute clause (§6) — align with Razorpay dispute process.
- **R6.** Failed-payment / involuntary-cancellation handling (§5) — confirm grace-period vs
  billing implementation.
- **R7.** Fill `[LEGAL ENTITY NAME]` + `[SUPPORT EMAIL]`.

### Global placeholders to fill before publication
`[LEGAL ENTITY NAME]`, `[CIN]`, `[REGISTERED ADDRESS]`, `[DATE]` / effective dates,
`[SUPPORT EMAIL]`, `[GRIEVANCE OFFICER NAME/EMAIL]`, `[REGION]`, `[RETENTION PERIOD]`,
`[CITY, STATE]`, `[PROCESSORS PAGE / on request]`.

---

## 5. Verification
- `npx tsc --noEmit` — **clean** (no type errors).
- `npx eslint app/legal middleware.ts app/power/layout.tsx` — **clean** (no warnings/errors).
- Conventions matched against existing `app/power/credibility/page.tsx` and
  `app/power/layout.tsx`; Next.js 16 app-router guide read before writing.

---

## 6. Deviations (flagged for audit)

1. **Edited `frontend/middleware.ts` (1 line + comment) — outside M6's literal §6 ownership.**
   Added `/legal` to `POWER_PORTAL_PATHS`. **Why:** the site-wide HTTP Basic Auth gate fails
   closed and challenges every non-`/power` path. Without this, footer/signup links to
   `/legal/*` would hit a browser auth dialog and the pages would be unreachable for prospects —
   defeating the launch-blocker requirement. The change only widens the existing public-bypass
   list to a static, no-operator-data surface; it does not weaken operator-route protection
   (`/falcon`, `/admin`, `/analysis`, `/` still gated). Acceptable per INV6 spirit ("keep this
   to the legal pages + power footer"); flagged here for the orchestrator to confirm.

2. **Edited `frontend/app/power/layout.tsx` — footer only.** Explicitly mandated by the task
   ("add Terms/Privacy/Refund/Risk links to the POWER surface footer"). Footer is inline (not a
   shared component); operator surface unaffected. Logged for completeness.

3. **Pages live at `/legal/*`, not `/power/legal/*`.** Per CONTRACT §6 (`frontend/app/legal/*`)
   and MASTER_SPEC ("Frontend route group `/legal/*`"). Consequence: they do NOT inherit the
   Power portal layout, so M6 added its own `app/legal/layout.tsx` chrome (in-scope).

---

## 7. Risks for audit
- **Content is NOT legally reviewed.** Every page is a DRAFT template carrying a visible banner
  + an embedded lawyer-review comment block. Must NOT be treated as binding or as legal advice.
  The §4 checklist is the gate before public launch.
- **Placeholders unfilled** (`[LEGAL ENTITY NAME]`, dates, emails, Grievance Officer, etc.) —
  intentional; must be filled before publication. Listed in §4.
- **Signup checkbox consent** (acceptance criterion: "risk disclosure shown at signup with
  checkbox consent") is **M7's** scope, not M6. M6 provides the linkable `/legal/risk` page;
  M7 must wire the checkbox + persist consent.
- **TSX/markdown drift risk:** prose exists in two places (TSX + markdown). Markdown is the
  declared source of truth; reviewers should edit markdown and re-mirror into TSX. They are in
  sync as of this build.
- Could not run a full `next build` in this environment within scope; relied on `tsc --noEmit`
  + `eslint` (both clean) and convention-matching against existing static pages.
