# M7 — Commercial Frontend (pricing / signup / billing / paywall) — Build Log

**Module:** M7 (Frontend)
**Agent:** BuildAgent-M7
**Date:** 2026-06-13 (IST)
**Contract refs:** CONTRACT §2, §3.1, §3.2, §3.3, §6, §7 · MASTER_SPEC "M7 — Frontend"
**Next.js:** 16.2.3 (App Router). Verified `redirect`, route-handler, server-action conventions against `node_modules/next/dist/docs/` before writing.

**Files owned + created/edited (CONTRACT §6 — `frontend/app/power/{pricing,signup,billing}/page.tsx`, `frontend/lib/power-api.ts`):**

Created:
- `frontend/app/power/pricing/page.tsx`
- `frontend/app/power/signup/page.tsx`
- `frontend/app/power/billing/page.tsx`
- `frontend/app/power/billing/actions.ts` (billing Server Actions)
- `frontend/app/power/billing/BillingActions.tsx` (client buttons)
- `frontend/components/power/PaywallNotice.tsx` (shared 402 state)

Edited:
- `frontend/lib/power-api.ts` (extend client — types + endpoints + 402 helpers)
- `frontend/lib/power-auth-client.ts` (add `signupWithEmail` helper, matching existing client-auth pattern)
- `frontend/app/power/today/page.tsx` (paywall-on-402)
- `frontend/app/power/live/page.tsx` (paywall-on-402)
- `frontend/app/power/portfolios/page.tsx` (paywall-on-402)

**Not touched:** `/falcon/*`, operator surfaces, backend, DB, M6's `frontend/app/legal/*` (only LINKED to `/legal/risk` + `/legal/terms`). INV2 + INV6 honoured — power pages + power-api client only.

---

## What was built

### 1. `power-api.ts` — client extension (CONTRACT §3.1/§3.2/§3.3)
**Types added:** `BillingPlan`, `SignupAccess`, `SignupRequest`, `SignupResponse`, `CreateSubscriptionResponse`, `BillingStatusResponse`, `CancelSubscriptionResponse` — mirroring the contract response shapes exactly.

**Endpoints added to `PowerAPI`:**
- `signup(req)` → `POST /api/power/auth/signup` (public, no JWT).
- `billingCreateSubscription(jwt)` → `POST /api/power/billing/create-subscription`.
- `billingStatus(jwt, signal?)` → `GET /api/power/billing/status`.
- `billingCancel(jwt)` → `POST /api/power/billing/cancel`.

All authed billing calls take `jwt` as first arg — matches the existing `me(jwt)` / `liveDecisions(jwt)` / `todayFull(jwt)` convention. They go through the same `apiFetch` helper (so retries/interceptors/telemetry land in one place, per the file's own doc).

**Shared 402 handler (CONTRACT §3.3):** added to `PowerAPIError`:
- `isPaymentRequired()` → true on HTTP 402 OR `code==='PAYMENT_REQUIRED'`.
- `checkoutUrl()` → returns `detail.checkout_url` if present, else null.

**Error-body parsing hardened:** `apiFetch`'s `!r.ok` branch previously only read `body.detail`. The 402 contract body is FLAT (`{code, message, checkout_url}`), so I made it accept either: prefer `detail` when it's an object, else fall back to the top-level body. This preserves `code`/`checkout_url` for the paywall while leaving FastAPI's `detail`-nested errors (the existing 401/403/409 paths) working unchanged.

### 2. `power-auth-client.ts` — `signupWithEmail(email, code)`
Thin wrapper mirroring the existing `loginWithInviteCode`. Builds `SignupRequest`, omitting `invite_code` entirely when blank (so the backend's `Optional[str]=None` default applies — coordinated with M5's request model). Returns `SignupResponse`; the page stores the token via the existing `storeSessionJWT`.

### 3. `/power/pricing` — single plan card (public, server component)
₹999/mo card, feature list, two CTAs both → `/power/signup` (the invite-code path lives on the signup form, avoiding a duplicate form). Links to `/legal/risk` + `/legal/terms`. Styling matches `/power/login` + `/power/waitlist` (neutral-900 cards, mint CTA) — no new design system.

### 4. `/power/signup` — open funnel (client component)
Mirrors `/power/login`'s structure (same `Field` helper, same error handling, same `PowerAPIError` branch). Collects email + optional invite code + **required Risk Disclosure / Terms consent checkbox** (links to `/legal/risk` + `/legal/terms`, `target="_blank"`). Submit disabled until consent is checked AND re-validated in `onSubmit`.

On success (`signupWithEmail` → `SignupResponse`):
- `storeSessionJWT(res.token)` (both outcomes are authenticated).
- `access==='full'` → `router.push('/power/today')`; `access==='payment_required'` → `router.push('/power/billing')`.
- `router.refresh()` after push so the layout re-reads the new cookie.
- 409 / `EMAIL_EXISTS` → "account already exists, sign in instead".

### 5. `/power/billing` — status + manage (server component + server actions)
- `requireSession()` (same gate as `/power/today`, `/power/live`) → `{jwt, user}`.
- Reads `GET /billing/status` server-side with the JWT.
- Computes the CONTRACT §2 allow predicate locally so the page agrees with M4's backend gate: `admin OR plan∈{founding,comp} OR (paid & active)`.
- Three rendered states:
  - **free-forever** (admin / founding / comp) → "You have full access, no payment required" + link to today.
  - **paid & active** → status card + renewal date (`current_end`, formatted in IST per INV4) + Cancel.
  - **blocked / lapsed / not active** → paywall card + "Subscribe ₹999/mo".
- Status grid shows Plan / Access / Subscription / Valid-until.

**Subscribe / Cancel are Server Actions** (`billing/actions.ts`), not client fetches, because `power_jwt` is HTTPOnly and unreadable from client JS. The actions run server-side where `getSessionJWT()` works — same server-side `PowerAPI.x(jwt)` convention used everywhere else.
- `subscribeAction()` → `billingCreateSubscription` → returns `short_url`; the client `SubscribeButton` does `window.location.href = short_url` (Razorpay **hosted checkout** — no card data on our site, PCI out of scope).
- `cancelAction()` → `billingCancel`; client `CancelButton` (with a confirm step) → `router.refresh()` to re-read status. (Plan flips to `blocked` on the Razorpay `subscription.cancelled` webhook — M3.)

### 6. Paywall UX on product pages (CONTRACT §3.3 + §4)
New shared `components/power/PaywallNotice.tsx` — a server component "Subscribe to continue" card pointing at `/power/billing` (plus an optional direct `checkout_url` link from the 402 body).

Wired into the three gated product pages whose routers M4 gates (`falcon_top20_router`, `portfolios_router`, live-tier):
- `/power/today`, `/power/live`, `/power/portfolios`: in each fetch `catch`, if `err.isPaymentRequired()` → render `<PaywallNotice/>` and return early; otherwise the existing error banner is unchanged. No hard crash.

---

## Auth / session convention matched
- **Cookie:** backend issues JWT → client POSTs it to `/api/power/session` (Route Handler) → sets HTTPOnly `power_jwt` cookie (`path:'/'`, `SameSite=Lax`, `Secure` in prod). Client JS never reads the token.
- **Server reads:** `getSessionJWT()` / `requireSession()` from `next/headers` cookies. Billing page + actions use these.
- **Client triggers:** `storeSessionJWT()` (signup) is the same helper `/power/login` uses.
- Authed product/billing data is fetched server-side as `PowerAPI.x(jwt)`. The only client→server-with-JWT path (billing buttons) goes through Server Actions so the token stays server-side.

---

## Reuse summary (no duplicated logic)
| Need | Reused from |
|---|---|
| HTTP + auth header + error envelope | `apiFetch` in `power-api.ts` (extended, not bypassed) |
| Store JWT → HTTPOnly cookie | `storeSessionJWT` (`power-auth-client.ts`) |
| Server-side JWT read + auth gate | `getSessionJWT` / `requireSession` (`power-auth.ts`) |
| Form / field / error-banner styling | `/power/login`, `/power/waitlist` patterns + `Field` helper |
| Error typing + 402 branch | `PowerAPIError` (extended with `isPaymentRequired`/`checkoutUrl`) |

---

## Verification done (cannot run dev server — no backend creds)
- `npx tsc --noEmit` → **clean** (0 errors) across all changed files.
- `npx eslint` on the 8 changed/created files → **0 errors**; the single warning (`lastYear` unused) is pre-existing code I did not touch.

---

## Dependencies / blockers for the orchestrator
- **M6 legal pages required.** Signup, pricing, and billing LINK to `/legal/risk` and `/legal/terms`. Those routes are M6-owned and did not exist at build time (`frontend/app/legal/*` absent). The signup consent checkbox is required-to-submit and points at them — **broken links until M6 ships.** I did NOT create them (file ownership).
- **Backend contract assumptions** (already built + audited per task brief): signup returns `{token,user_id,billing_plan,access,checkout_url}`; `create-subscription` returns `{razorpay_subscription_id, short_url}`; gated routes 402 with `{code:'PAYMENT_REQUIRED', message, checkout_url}`.

---

## Deviations
1. **Invite-code entry is on `/signup`, not `/pricing`.** The spec says pricing has a "have a code?" path; I implemented that path as a link to the signup form (which collects the code) rather than a second code field on pricing. One funnel, no duplicate form. Pricing → signup → (full | billing) matches the M7 acceptance flow.
2. **402 body parsed flat OR `detail`-nested.** CONTRACT §3.3 shows a flat 402 body, but FastAPI's `HTTPException(detail=...)` nests under `detail`. I made `apiFetch` accept both so the paywall works regardless of how M4 raised the 402. Auditor: confirm which shape M4 actually emits; both are handled.
3. **Billing actions via Server Actions (not Route Handlers).** Chosen because the JWT is HTTPOnly; Server Actions are the idiomatic Next 16 server-side path and keep the token off the client. No new `/api/power/*` Route Handler invented.

## Risks for audit
- **402 detection depends on M4's actual body shape + status code.** `isPaymentRequired()` triggers on status 402 OR `code==='PAYMENT_REQUIRED'`; `checkoutUrl()` reads `detail.checkout_url`. If M4 emits a different status (e.g. 403) or nests `checkout_url` differently than the §3.3 flat shape, the paywall card still renders (via the code check) but the direct checkout link may be absent — verify against M4's emitter.
- **`subscription_status` string values are passed through, not enum-validated** on the frontend. The "paid & active" branch checks `=== 'active'` exactly; any other Razorpay status (`halted`/`cancelled`/etc.) falls to the paywall state — matches CONTRACT §2 intent, but auditor should confirm M3 writes lower-case `active`.
- **Founding/comp/admin gating is recomputed on the frontend** for the billing page's display only — the real gate is M4 server-side (402). The frontend predicate is cosmetic; a tampered client can't grant product access because the backend 402s independently.
- **No automated UI test** — dev server needs backend creds (per task brief). Verified by typecheck + lint + contract/pattern matching only. Later manual run + AuditAgent gate to confirm.
- **Paywall coverage scope.** Wired on the three primary server-rendered product pages (today, live, portfolios listing). Two further gated surfaces were left unwired by design: `portfolios/[slug]` (reachable only via the listing, which now paywalls and exposes no clickable cards on 402) and `/power/sizing` (client-side `Promise`-based fetch with a different error path). If the orchestrator wants belt-and-suspenders, these two are the follow-up — but the funnel can't reach them without first hitting a wired page.
- **`current_end` formatted with `Intl` in `Asia/Kolkata`** (INV4). Relies on the backend sending a parseable ISO/TIMESTAMPTZ string; falls back to the raw string if `Date` can't parse it.
