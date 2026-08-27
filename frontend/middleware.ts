import { NextRequest, NextResponse } from 'next/server'

/**
 * HTTP Basic Auth — fully private gate before any HTML or branding is served.
 *
 * Visitors see only their browser's native auth dialog (no Kanida.ai branding,
 * no login page, no info about what the site is). The dialog reads
 * "Sign in — kanida.ai requires authentication".
 *
 * Credentials are stored in Vercel env vars:
 *   SITE_USER  — your chosen username (any string)
 *   SITE_PASS  — your chosen strong password
 *
 * SECURITY: if either env var is missing, the middleware FAILS CLOSED — every
 * request gets 503. This prevents accidentally exposing the site if the env
 * vars are deleted or renamed.
 *
 * Browsers cache Basic Auth credentials for the session, so you only see the
 * dialog once per browser session.
 */

const BASIC_REALM = 'Restricted'

// Power User Portal paths — public-facing surface with its own invite-code
// auth (backend/power_user/services/invites.py + JWT cookie at
// /api/power/session). These MUST bypass the site-wide HTTP Basic Auth
// gate so invitees can reach /power/login with just their invite code.
// Operator surfaces (/falcon, /admin, /analysis, /) stay behind Basic Auth.
// `/legal` (M6) is the public Terms/Privacy/Refund/Risk surface. It is linked
// from the Power footer + the signup flow and must be reachable WITHOUT the
// site-wide Basic Auth dialog (otherwise prospects hit a browser auth prompt
// before they can read the legal docs). It exposes no operator data — static
// content only.
// '/api/builder' = Agent Builder v0 API — a power-portal surface, so it bypasses
// the operator site-Basic-Auth like /api/power (else invited power users, who
// don't have SITE_PASS, couldn't use it). NOTE: v0 uses an X-User-Id stub, so this
// endpoint is currently unauthenticated — add power-JWT before public launch.
const POWER_PORTAL_PATHS = ['/power', '/api/power', '/api/power-auth', '/api/builder', '/api/agents', '/legal']

function unauthorized(): NextResponse {
  return new NextResponse(null, {
    status: 401,
    headers: {
      'WWW-Authenticate': `Basic realm="${BASIC_REALM}", charset="UTF-8"`,
      // Block search engines from indexing the 401 page itself
      'X-Robots-Tag': 'noindex, nofollow, noarchive',
      // Prevent any caching of the auth challenge
      'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
    },
  })
}

function notConfigured(): NextResponse {
  return new NextResponse(null, {
    status: 503,
    headers: {
      'X-Robots-Tag': 'noindex, nofollow, noarchive',
      'Cache-Control': 'no-store',
    },
  })
}

export function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl

  // Skip static asset paths that Next.js needs internally. The `matcher`
  // below already excludes most of these, but being defensive here in case
  // the matcher pattern changes.
  if (pathname.startsWith('/_next') || pathname === '/favicon.ico') {
    return NextResponse.next()
  }

  // Power User Portal bypass — handled by its own invite-code auth layer.
  // Without this, invitees would see a browser Basic Auth dialog before
  // ever reaching the Kanida.AI login page (defeats the purpose of the
  // public-facing portal).
  if (POWER_PORTAL_PATHS.some(p => pathname === p || pathname.startsWith(p + '/'))) {
    const res = NextResponse.next()
    // Still keep search engines out — invite-only beta.
    res.headers.set('X-Robots-Tag', 'noindex, nofollow, noarchive')
    // Expose the resolved pathname to Server Components (Falcon AI shell uses
    // it to decide whether to render the public TopBar chrome or hand the page
    // straight to the new left-nav AppShell). Additive — read-only, no auth
    // behavior change.
    res.headers.set('x-pathname', pathname)
    return res
  }

  // Accept SITE_* (canonical) or APP_* (the operator's existing Vercel vars).
  // The password is the shared secret; the username is OPTIONAL — if no user
  // var is set, any username is accepted and only the password is validated.
  const expectedUser = process.env.SITE_USER || process.env.APP_USER || ''
  const expectedPass = process.env.SITE_PASS || process.env.APP_PASSWORD || ''

  // Fail closed: if the password secret isn't configured, deny every request.
  if (!expectedPass) {
    return notConfigured()
  }

  const authHeader = req.headers.get('authorization') || ''
  if (!authHeader.startsWith('Basic ')) {
    return unauthorized()
  }

  let decoded = ''
  try {
    // atob is available in Next.js Edge Runtime
    decoded = atob(authHeader.slice(6))
  } catch {
    return unauthorized()
  }

  // RFC 7617 — Basic Auth uses "user:pass" with the FIRST colon as separator
  // (a password may itself contain colons).
  const idx = decoded.indexOf(':')
  if (idx < 0) return unauthorized()
  const user = decoded.slice(0, idx)
  const pass = decoded.slice(idx + 1)

  // Username enforced only when configured; password is always required.
  if ((expectedUser && user !== expectedUser) || pass !== expectedPass) {
    return unauthorized()
  }

  // Authenticated — let the request through. Add a noindex header just in case.
  const res = NextResponse.next()
  res.headers.set('X-Robots-Tag', 'noindex, nofollow, noarchive')
  return res
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
}
