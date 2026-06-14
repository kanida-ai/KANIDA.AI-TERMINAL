# M7 — Commercial Frontend — Audit

**Auditor:** AuditAgent-M7
**Date:** 2026-06-13 (IST)
**Scope:** `frontend/app/power/{pricing,signup,billing}/*`, `billing/actions.ts` + `BillingActions.tsx`, `lib/power-api.ts` (additions), `lib/power-auth-client.ts`, `components/power/PaywallNotice.tsx`, wiring into `today`/`live`/`portfolios`.
**Mode:** READ-ONLY on code (only this file written).

---

## VERDICT: GREEN

No card data on our frontend. JWT stays server-side. Pay flow contract matches. Consent links resolve. No invariant breach.

---

## Checklist findings (cited)

### 1. Contract fidelity — PASS
- **Signup §3.1:** `signupWithEmail` (`power-auth-client.ts:28-36`) omits `invite_code` when blank so M5's `Optional[str]=None` default applies. Routing on response (`signup/page.tsx:54`): `access==='full' → /power/today`, else `→ /power/billing`. Token stored both outcomes (`signup/page.tsx:51`). 409 handled (`signup/page.tsx:59` — checks both `code==='EMAIL_EXISTS'` and `status===409`).
- **Billing §3.2:** all four calls present and correctly shaped (`power-api.ts:755-767`): `billingCreateSubscription` (POST), `billingStatus` (GET), `billingCancel` (POST). Status page reads `current_end`/`billing_plan`/`subscription_status` (`billing/page.tsx:38-49`). Subscribe consumes `short_url` (`actions.ts:30-34`).
- **402 §3.3:** `isPaymentRequired()` triggers on status 402 OR `code==='PAYMENT_REQUIRED'`; `checkoutUrl()` reads `detail.checkout_url` (`power-api.ts:218-226`). `apiFetch` error branch (`power-api.ts:281-294`) hardened to accept either FastAPI `detail`-nested OR flat `{code,message,checkout_url}` body, preserving `code`/`checkout_url` for the paywall. Sound.

### 2. PCI / card safety — PASS (no card data on frontend)
- Grep across `frontend/app/power` for `card/cvv/cvc/expiry/new Razorpay/checkout.js`: **zero input fields**. Matches are component names (`PortfolioCard`, `PillarCard`), copy ("No card needed", "card details never touch KANIDA.AI"), and doc comments only.
- Payment is exclusively a redirect: `subscribeAction` → `short_url` → `window.location.href` (`BillingActions.tsx:24`); 402 `checkout_url` → plain `<a href>` (`PaywallNotice.tsx:42-49`). No Razorpay JS SDK embed, no checkout iframe, no card form. PCI scope correctly offloaded to Razorpay hosted checkout.

### 3. Auth / session security — PASS (JWT stays server-side)
- `power_jwt` cookie is `httpOnly:true`, `secure` in prod, `SameSite=Lax` (`app/api/power/session/route.ts:37-50`). Client JS never reads it.
- All authed billing calls run server-side: billing status fetched in the server component via `requireSession()` JWT (`billing/page.tsx:27,32`); Subscribe/Cancel are Server Actions (`'use server'` in `actions.ts:1`) that read the cookie via `getSessionJWT()` (`actions.ts:27,46`). Client buttons (`BillingActions.tsx`) only invoke the actions — token never crosses to the browser.
- `power-auth-client.ts` correctly never reads the JWT (only triggers the Route Handler to set the cookie).

### 4. Consent checkbox — PASS
- Signup has a required Risk Disclosure/Terms checkbox (`signup/page.tsx:106-122`), enforced two ways: submit `disabled || !consent` (`:126`) AND re-validated in `onSubmit` (`:43-46`).
- Links target `/legal/risk` + `/legal/terms` (`signup/page.tsx:116,119`). **Both routes now exist** (`frontend/app/legal/risk/page.tsx` 125 lines, `frontend/app/legal/terms/page.tsx` 188 lines — real content, not stubs) via M6. The build-log "broken links" blocker is **resolved**. Pricing and billing pages link to the same routes and also resolve.

### 5. Paywall UX — PASS
- `PaywallNotice` renders a "Subscribe to continue" card on 402 (`PaywallNotice.tsx`). Wired into all three gated product pages: `today` (`:51-61`), `live` (`:38-47`), `portfolios` (`:38-48`). Each uses the `undefined`-sentinel pattern to distinguish "not paywalled" from "paywalled with null checkout_url", returns early on 402, and leaves the existing error banner path intact for non-402 errors. No hard crash.

### 6. Conventions — PASS
- Routing/data-fetch/cookie all match existing power pages: `requireSession()` gate + server-side `PowerAPI.x(jwt)` (same as `today`/`live`); `force-dynamic` + `revalidate=0`; `apiFetch` reused (not bypassed); `storeSessionJWT`/`PowerAPIError` reused; `Field` helper + neutral-900/mint styling mirror `/power/login`. No new design system.
- **INV2:** no `falcon/*` / operator / backend / DB file touched — confirmed (all edits are power-user frontend). **INV6 spirit** honoured (power surface stays in power pages + power-api client). M6's `legal/*` only linked, not modified.

### 7. Build quality — PASS
- `npx tsc --noEmit` on the frontend: **clean (exit 0, no output)** — independently re-run, confirms the build-log claim.
- No obvious import/type errors in changed files. New types (`BillingPlan`, `SignupResponse`, etc.) mirror contract shapes. `today` page's `is_latest`/`latest_signal_date`/`_built_in_ms` references resolve against `Top20Response` (`falcon-top20-types.ts:227-243`) — pre-existing, not M7-introduced.

---

## Non-blocking notes (for orchestrator, not must-fix)

1. **402 body-shape dependency.** Paywall card always renders on status 402 (robust), but the *direct* `checkout_url` link in `PaywallNotice` only appears if M4 emits it under the parsed `detail`/flat body the way §3.3 specifies. Confirm M4's actual emitter shape against `apiFetch`'s parser (`power-api.ts:287-294`). Worst case: card still shows, direct link absent, primary `/power/billing` CTA still works. Cosmetic only.
2. **Paywall coverage scope.** Wired on the 3 primary server-rendered pages. `portfolios/[slug]` and `/power/sizing` are unwired by design (only reachable through a now-paywalled listing / different client fetch path). Belt-and-suspenders follow-up, not a launch blocker — the funnel can't reach them without first hitting a wired page.
3. **Frontend allow-predicate is cosmetic.** `billing/page.tsx:42-49` recomputes the §2 gate for display only; real enforcement is M4's server-side 402. A tampered client cannot grant access. Correct posture.

---

## Explicit confirmations
- **No card data is collected on our frontend.** Payment is Razorpay hosted-checkout via `short_url`/`checkout_url` redirect only.
- **The `power_jwt` JWT never reaches client JS.** It is HTTPOnly; all authed billing/product calls execute in server components or Server Actions.
