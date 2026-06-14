/**
 * Client-side auth helpers — used in 'use client' components.
 *
 * Never reads the JWT directly (it's HTTPOnly). Only triggers state changes
 * via Route Handlers and API calls.
 */
import { PowerAPI, type GoogleSignInResponse, type GoogleSignInOK, type InviteLoginOK,
          type SignupRequest, type SignupResponse } from './power-api'


/** Send Google id_token to backend, return {status:'ok',jwt,user} or {status:'needs_invite',...} */
export async function exchangeGoogleIdToken(idToken: string): Promise<GoogleSignInResponse> {
  return await PowerAPI.signInWithGoogle(idToken)
}


/** Phase 1b: email + invite-code login. Throws PowerAPIError(400, 'LOGIN_INVALID')
 *  on any failure mode (intentionally indistinguishable to block enumeration). */
export async function loginWithInviteCode(email: string, code: string): Promise<InviteLoginOK> {
  return await PowerAPI.signInWithInviteCode({ email, code })
}


/** M7 open signup: email + optional invite code → backend creates the account
 *  and returns {token, access, billing_plan, checkout_url}. Throws
 *  PowerAPIError(409,'EMAIL_EXISTS') on a duplicate email. The caller stores
 *  the token via storeSessionJWT and routes on `access`. */
export async function signupWithEmail(
  email: string,
  inviteCode: string,
): Promise<SignupResponse> {
  const req: SignupRequest = { email }
  const code = inviteCode.trim()
  if (code) req.invite_code = code
  return await PowerAPI.signup(req)
}


/** Store the backend-issued JWT in HTTPOnly cookie via our Route Handler. */
export async function storeSessionJWT(jwt: string): Promise<void> {
  const r = await fetch('/api/power/session', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ jwt }),
  })
  if (!r.ok) {
    throw new Error(`Failed to store session: ${r.status}`)
  }
}


/** Redeem an invite code with an already-fetched Google id_token. */
export async function redeemInviteCode(idToken: string, code: string): Promise<GoogleSignInOK> {
  return await PowerAPI.redeemInvite(idToken, code)
}


/** Clear the session cookie. */
export async function logout(): Promise<void> {
  await fetch('/api/power/session', { method: 'DELETE' }).catch(() => {})
  // Hard refresh to clear any cached server-component data
  window.location.href = '/power'
}


/** Join the waitlist (called from /power/waitlist + /power/login). */
export async function joinWaitlist(email: string, source = 'landing_cta'): Promise<{ joined: boolean }> {
  const r = await PowerAPI.joinWaitlist(email, source)
  return { joined: r.joined }
}
