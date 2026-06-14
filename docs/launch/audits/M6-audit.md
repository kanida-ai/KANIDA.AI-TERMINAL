# M6 — Legal — Audit

**Module:** M6 (Legal)
**Agent:** AuditAgent-M6
**Date:** 2026-06-13 (IST)
**Scope refs:** CONTRACT §6 (file ownership), §7 (invariants) · MASTER_SPEC "M6 — Legal" · M6-build-log.md
**Disposition:** READ-ONLY on all code. Only file written: this audit.

---

## VERDICT: GREEN

All four legal pages render the required content with the mandatory DRAFT banner; the
Risk Disclosure carries the three SEBI-critical claims conservatively worded; the
lawyer-review checklist is complete and surfaces the real risky claims; the middleware
change adds ONLY `/legal` to the public bypass and exposes NO operator route; footer
integration touches the Power surface only; no invariant breached; no secrets.

Content being unreviewed draft + unfilled placeholders is EXPECTED per spec, not a defect.

---

## Checklist results

### 1. Four pages exist + render required content + DRAFT banner — PASS
- `frontend/app/legal/terms/page.tsx`, `privacy/page.tsx`, `refund/page.tsx`, `risk/page.tsx`
  all exist; each is a server component (`export const dynamic = 'force-static'`) returning a
  `<LegalArticle>`.
- The mandatory visible banner renders for every page via `DraftBanner` inside `LegalArticle`:
  `frontend/app/legal/_components.tsx:15-26` ("**DRAFT — pending legal review.** Not yet
  legally binding.") — `LegalArticle` calls `<DraftBanner />` unconditionally at
  `_components.tsx:76`. No page can omit it.
- Content coverage: Terms (14 sections, incl. not-an-adviser §1, ₹999/mo §5, Live/perf §4),
  Privacy (DPDP Data Fiduciary, Grievance Officer, no card storage), Refund (₹999/mo monthly
  India, no pro-rata, access-until-cycle-end), Risk (8 sections, below).

### 2. Risk Disclosure SEBI claims — PASS (conservatively worded)
All three required claims are present and strong:
- **Informational / not advice:** `risk/page.tsx:27-42` (§1) — "NOT investment advice, NOT a
  research report prepared for you, and NOT a personal recommendation … you are solely
  responsible for your trades." Reinforced by the not-a-SEBI-intermediary statement at
  `risk/page.tsx:44-53` (§2).
- **Past performance ≠ future:** `risk/page.tsx:55-62` (§3) — "Past performance is not
  indicative of, and provides no guarantee of, future results."
- **"Live" personas are model/simulated track records:** `risk/page.tsx:64-84` (§4) — win
  rates, hit rates, backtests, walk-forward sims, and the "Live" persona portfolios are
  "model, simulated, or hypothetical track records … NOT returns earned on real customer
  capital and NOT a record of trades placed for you," with the hindsight/costs/slippage
  limitation language and "no representation is made that any account will or is likely to
  achieve results similar to those shown." Headline credibility-page numbers explicitly framed
  as illustrations, not a forecast or promise.
- Plus capital-loss warning (§5), market/data/tech risk (§6), seek-SEBI-adviser (§7), and an
  acknowledgement clause (§8). Wording is conservative throughout. TSX mirrors
  `docs/launch/legal/risk.md` faithfully.

### 3. Lawyer-review checklist — PASS (complete, lists real risky claims)
- Embedded as an HTML comment at the top of each markdown source
  (`docs/launch/legal/risk.md:1-41` is the highest-priority block) AND consolidated in
  `M6-build-log.md` §4.
- Risk items K1–K8 correctly flag the load-bearing claims: K4 explicitly names the
  "Live"/performance claim as "the highest-risk claim to get wrong"; K5 flags the specific
  headline marketing numbers (₹30L→₹1.05Cr, +250%, win/hit rates) for substantiation + SEBI
  advertising rules. Terms (T1–T9), Privacy (P1–P8), Refund (R1–R7) checklists present, plus a
  global placeholder list. Nothing material omitted.

### 4. middleware.ts (SECURITY) — PASS · OPERATOR ROUTES NOT EXPOSED
Read the full file (`frontend/middleware.ts:1-121`) and the exact diff vs HEAD.
- **The change is exactly one array element + a comment.** `POWER_PORTAL_PATHS` went from
  `['/power', '/api/power', '/api/power-auth']` to
  `['/power', '/api/power', '/api/power-auth', '/legal']` (`middleware.ts:34`). Nothing else in
  the auth logic changed.
- **Match semantics are safe:** the bypass test is
  `POWER_PORTAL_PATHS.some(p => pathname === p || pathname.startsWith(p + '/'))`
  (`middleware.ts:73`). For `/legal` this matches only `/legal` and `/legal/...`; it does NOT
  match `/legalfoo` (the `+ '/'` guard prevents prefix-bleed). No regex, no wildcard.
- **Operator routes remain gated:** `/falcon`, `/admin`, `/analysis`, and `/` are NOT in
  `POWER_PORTAL_PATHS`, so they fall through to the Basic-Auth path
  (`middleware.ts:80-115`), which still fails closed (503) if `SITE_USER`/`SITE_PASS` are
  unset and 401s on bad/absent credentials. The fail-closed and noindex behaviour is unchanged.
- `/legal` is a static-content surface (no data fetching, no operator data), so widening the
  public bypass to it leaks nothing.

### 5. Footer integration (INV2) — PASS
- `frontend/app/power/layout.tsx` diff is footer-only: the inline `Footer()` gains a 4-link
  `<nav>` (Terms/Privacy/Refund/Risk) above the existing tagline; the original copy + version
  string are preserved. The `Link` import already existed (`power/layout.tsx:7`), so no new
  import and no build break.
- The Power `Footer()` is an inline function, NOT a shared component; the operator `/falcon/*`
  surface uses a separate `app/falcon/layout.tsx` and does not render this footer. Operator nav
  untouched. The change carries an in-code INV2 comment.
- `/legal/*` pages also self-cross-link via their own `app/legal/layout.tsx` footer
  (in M6 scope).

### 6. Invariants — PASS
- **INV2 (Auto-Trade never touched):** no `backend/falcon/*` file changed; verified via
  `git status` — M6's footprint is `frontend/app/legal/*`, `frontend/middleware.ts`,
  `frontend/app/power/layout.tsx`, `docs/launch/legal/*` only.
- **INV5 (no secrets):** secret-pattern scan over `frontend/app/legal/` and
  `docs/launch/legal/` returned nothing. No keys, passwords, or tokens committed.
- **INV6 (power_user code stays in power_user/):** N/A for frontend; the two cross-file edits
  (middleware, power footer) are minimal, justified, and flagged in the build log §6.

---

## Notes (non-blocking, expected)
- Unfilled placeholders (`[LEGAL ENTITY NAME]`, `[CIN]`, `[REGISTERED ADDRESS]`, `[DATE]`,
  `[SUPPORT EMAIL]`, `[GRIEVANCE OFFICER ...]`, `[REGION]`, `[RETENTION PERIOD]`,
  `[CITY, STATE]`, `[PROCESSORS PAGE]`) — intentional, listed in build log §4; must be filled
  before public launch.
- Content is unreviewed draft by design (DRAFT banner + per-file lawyer-review block). NOT a
  RED condition per audit rubric.
- Signup checkbox consent for the Risk Disclosure is M7 scope, not M6 — correctly deferred.
- TSX/markdown dual-source drift risk noted by builder; verified in sync as of this audit
  (risk page spot-checked line-for-line against `risk.md`).

---

## Required confirmation
**The middleware change does NOT expose any operator route.** It adds only `/legal` to the
public Basic-Auth bypass list. `/falcon`, `/admin`, `/analysis`, and `/` are absent from
`POWER_PORTAL_PATHS` and remain behind the fail-closed HTTP Basic Auth gate. Path-matching is
exact-or-`/`-prefixed, so no operator path is incidentally matched.
