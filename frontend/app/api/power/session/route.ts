/**
 * Session cookie management — HTTPOnly cookie set by Next.js Route Handler.
 *
 * Why this exists: the backend issues a JWT, but storing it in localStorage
 * exposes it to any XSS attack. Setting via document.cookie can't make it
 * HTTPOnly. This Route Handler sets/clears the cookie server-side so JS
 * never touches the token directly.
 *
 * Flow:
 *   1. Frontend gets Google id_token via GIS
 *   2. POST /api/power/auth/google { id_token } → backend returns { jwt }
 *   3. Frontend POSTs that jwt here: POST /api/power/session { jwt }
 *   4. This handler sets `power_jwt` HTTPOnly cookie + returns 200
 *   5. Server Components read the cookie via `cookies()` from next/headers
 *
 * Logout: DELETE here clears the cookie.
 */
import { cookies } from 'next/headers'
import { NextRequest, NextResponse } from 'next/server'

export const COOKIE_NAME = 'power_jwt'
const COOKIE_MAX_AGE = 60 * 60 * 24       // 24h (matches backend JWT TTL)

export async function POST(req: NextRequest) {
  let body: { jwt?: unknown }
  try {
    body = await req.json()
  } catch {
    return NextResponse.json({ error: 'invalid_json' }, { status: 400 })
  }
  const jwt = body.jwt
  if (typeof jwt !== 'string' || jwt.length < 16) {
    return NextResponse.json({ error: 'jwt_missing' }, { status: 400 })
  }

  const cookieStore = await cookies()
  cookieStore.set({
    name:     COOKIE_NAME,
    value:    jwt,
    httpOnly: true,
    secure:   process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    path:     '/power',
    maxAge:   COOKIE_MAX_AGE,
  })
  return NextResponse.json({ ok: true })
}

export async function DELETE() {
  const cookieStore = await cookies()
  cookieStore.set({
    name:     COOKIE_NAME,
    value:    '',
    httpOnly: true,
    secure:   process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    path:     '/power',
    maxAge:   0,        // expire immediately
  })
  return NextResponse.json({ ok: true })
}
