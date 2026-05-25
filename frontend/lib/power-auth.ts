/**
 * Power-user auth helpers — split by runtime.
 *
 * Server (Server Components, Route Handlers):
 *   await getSessionJWT()   — reads HTTPOnly cookie, returns string | null
 *   await requireSession()  — same but redirects to /power/login if absent
 *   await getCurrentUser()  — full user object via /auth/me, null if absent/invalid
 *
 * Client (use client):
 *   exchangeGoogleId(idToken)   — POST /api/power/auth/google → JWT or NEEDS_INVITE
 *   storeJWT(jwt)               — POST /api/power/session sets HTTPOnly cookie
 *   logout()                    — DELETE /api/power/session clears cookie
 *
 * Why split: HTTPOnly cookies can't be read by client JS at all. Server reads
 * the cookie via `next/headers`; client never touches the token. The client
 * has helpers to TRIGGER the cookie set/clear via our Route Handler.
 */
import 'server-only'
import { cookies } from 'next/headers'
import { redirect } from 'next/navigation'
import { PowerAPI, PowerAPIError, type GoogleSignInOK } from './power-api'

export const COOKIE_NAME = 'power_jwt'


// ──────────────────────────────────────────────────────────────────────────
// Server-side: read the cookie + look up user
// ──────────────────────────────────────────────────────────────────────────

export async function getSessionJWT(): Promise<string | null> {
  const c = await cookies()
  const t = c.get(COOKIE_NAME)?.value
  return t && t.length > 16 ? t : null
}


export type SessionUser = {
  id:           number
  email:        string
  display_name: string | null
  picture_url:  string | null
  role:         string
}

/**
 * Fix #3 (2026-05-25): represent the three distinct outcomes of a session
 * lookup so the caller can react differently to "JWT expired" vs "backend
 * temporarily unreachable". Previously `getCurrentUser()` collapsed all
 * errors to `null`, which made `requireSession()` redirect the user to
 * `/power/login?expired=1` even on a 10-second backend restart blip —
 * effectively mass-logging-out every active user during our 14:30 PDT
 * scheduled restart. Now: only HTTP 401 from the backend is treated as a
 * real session-expired event; 5xx / network / timeout errors throw a
 * BackendUnavailableError up the call stack so the page can show a
 * "service unavailable, please retry" state without destroying the cookie.
 */
export class BackendUnavailableError extends Error {
  constructor(public readonly cause: unknown) {
    super(`BACKEND_UNAVAILABLE: ${cause instanceof Error ? cause.message : String(cause)}`)
    this.name = 'BackendUnavailableError'
  }
}

/**
 * Fetch the current user from /auth/me using the session JWT.
 * Returns null ONLY when the backend says HTTP 401 (real auth failure)
 * or the JWT is absent / malformed.
 * Throws BackendUnavailableError when the backend itself is unreachable
 * or returns 5xx — caller must handle this without destroying the session.
 */
export async function getCurrentUser(): Promise<SessionUser | null> {
  const jwt = await getSessionJWT()
  if (!jwt) return null
  try {
    const u = await PowerAPI.me(jwt)
    return {
      id:           u.id,
      email:        u.email,
      display_name: u.display_name,
      picture_url:  u.picture_url,
      role:         u.role,
    }
  } catch (e) {
    // HTTP 401 = real auth failure (expired/revoked JWT) → null → caller redirects
    if (e instanceof PowerAPIError && e.status === 401) return null
    // 5xx, network error, timeout = backend transiently down → throw
    throw new BackendUnavailableError(e)
  }
}


/**
 * Server Component helper: if not authed, redirect to /power/login.
 * Returns { jwt, user } on success.
 *
 * Fix #3 (2026-05-25): on BackendUnavailableError, re-throw instead of
 * redirecting. Next.js's error boundary will render the nearest error.tsx
 * (or the default 500 page) while leaving the session cookie intact —
 * the user's next refresh after the backend recovers logs them back in
 * without ever having to re-enter credentials.
 */
export async function requireSession(): Promise<{ jwt: string; user: SessionUser }> {
  const jwt = await getSessionJWT()
  if (!jwt) redirect('/power/login')
  let user: SessionUser | null
  try {
    user = await getCurrentUser()
  } catch (e) {
    if (e instanceof BackendUnavailableError) throw e   // page error.tsx handles it
    throw e
  }
  if (!user) {
    // JWT present but invalid (expired or backend rotated POWER_JWT_SECRET).
    // ONLY redirects on a real 401 from the backend now — 5xx no longer
    // mass-logs-out the active user base.
    redirect('/power/login?expired=1')
  }
  return { jwt, user }
}
