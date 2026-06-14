'use server'

/**
 * Billing Server Actions (M7).
 *
 * Why server actions: the `power_jwt` cookie is HTTPOnly, so a 'use client'
 * component can NOT read the token to call PowerAPI.billing* directly. These
 * run on the server, where getSessionJWT() can read the cookie — matching the
 * existing server-side `requireSession()` → `PowerAPI.x(jwt)` convention used
 * by /power/today and /power/live. The client billing buttons just invoke
 * these actions; the JWT never reaches the browser.
 *
 * `subscribeAction` returns the Razorpay hosted-checkout `short_url` to the
 * client, which then sets `window.location.href` to it. No card data ever
 * touches our frontend (PCI out of scope — CONTRACT / MASTER_SPEC M7).
 */
import { getSessionJWT } from '@/lib/power-auth'
import { PowerAPI, PowerAPIError } from '@/lib/power-api'

export type ActionResult =
  | { ok: true;  redirectUrl: string }   // subscribe → go to Razorpay short_url
  | { ok: true;  redirectUrl: null }     // cancel succeeded → caller refreshes
  | { ok: false; error: string }

/** Create a Razorpay subscription and hand back the hosted-checkout URL. */
export async function subscribeAction(): Promise<ActionResult> {
  const jwt = await getSessionJWT()
  if (!jwt) return { ok: false, error: 'Your session expired. Sign in again.' }
  try {
    const res = await PowerAPI.billingCreateSubscription(jwt)
    if (!res.short_url) {
      return { ok: false, error: 'Checkout link unavailable. Please try again.' }
    }
    return { ok: true, redirectUrl: res.short_url }
  } catch (e) {
    if (e instanceof PowerAPIError) {
      return { ok: false, error: e.message || 'Could not start checkout. Try again.' }
    }
    return { ok: false, error: 'Network error. Please try again.' }
  }
}

/** Cancel the active subscription. Backend flips plan → blocked on the
 *  Razorpay cancellation webhook; the page re-reads status after this. */
export async function cancelAction(): Promise<ActionResult> {
  const jwt = await getSessionJWT()
  if (!jwt) return { ok: false, error: 'Your session expired. Sign in again.' }
  try {
    await PowerAPI.billingCancel(jwt)
    return { ok: true, redirectUrl: null }
  } catch (e) {
    if (e instanceof PowerAPIError) {
      return { ok: false, error: e.message || 'Could not cancel. Try again.' }
    }
    return { ok: false, error: 'Network error. Please try again.' }
  }
}
