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
import { PowerAPI, type GoogleSignInOK } from './power-api'

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
 * Fetch the current user from /auth/me using the session JWT.
 * Returns null if no session OR if the JWT is invalid (e.g. expired).
 * Use in server components on authed pages.
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
  } catch {
    return null
  }
}


/**
 * Server Component helper: if not authed, redirect to /power/login.
 * Returns { jwt, user } on success.
 */
export async function requireSession(): Promise<{ jwt: string; user: SessionUser }> {
  const jwt = await getSessionJWT()
  if (!jwt) redirect('/power/login')
  const user = await getCurrentUser()
  if (!user) {
    // JWT present but invalid (expired or backend rotated POWER_JWT_SECRET)
    redirect('/power/login?expired=1')
  }
  return { jwt, user }
}
