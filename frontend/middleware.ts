import { NextRequest, NextResponse } from 'next/server'

// Routes that bypass the operator-side APP_PASSWORD gate.
// Landing/marketing pages on hold pending design — / currently redirects to
// /falcon (auth-gated) per pre-landing behavior.
//
// /power/* — the Power User Portal (Phase 1) — is PUBLIC-FACING (Design.md §10).
// It runs its own auth layer: Google OAuth + backend JWT for authed routes,
// invite gate for sign-up. Bypass the password gate for the whole namespace.
const PUBLIC_PATHS = ['/login', '/api/auth', '/power', '/api/power']
// Static assets that must be reachable without the operator gate. The push
// service worker (sw-push.js) is loaded by /power/admin and cannot tolerate
// a redirect — browsers refuse to register a service worker that 302s.
const PUBLIC_EXACT: string[] = ['/sw-push.js']

export function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl

  // Allow public paths and static assets
  if (
    PUBLIC_EXACT.includes(pathname) ||
    PUBLIC_PATHS.some(p => pathname.startsWith(p)) ||
    pathname.startsWith('/_next') ||
    pathname.startsWith('/favicon')
  ) {
    return NextResponse.next()
  }

  const token = req.cookies.get('kanida_auth')?.value
  const expected = process.env.APP_PASSWORD

  if (!expected || token !== expected) {
    const loginUrl = req.nextUrl.clone()
    loginUrl.pathname = '/login'
    loginUrl.searchParams.set('from', pathname)
    return NextResponse.redirect(loginUrl)
  }

  return NextResponse.next()
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
}
