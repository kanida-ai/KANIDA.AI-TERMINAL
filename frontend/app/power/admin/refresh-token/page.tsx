/**
 * /power/admin/refresh-token?t=<token> — magic-link landing page (Sprint 5c-1).
 *
 * Lands here when the admin taps the push notification CTA fired after 4
 * failed morning auth attempts. Calls POST /api/power/auth-refresh/consume
 * which atomically marks the token used + triggers an immediate Playwright
 * auth attempt. Whole flow: tap-and-done, no typing.
 *
 * The magic-link IS the auth — there is no JWT requirement on /consume.
 * The token is one-shot (DB-enforced) and expires in 15 minutes.
 */
'use client'

import { useEffect, useState } from 'react'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'

type Result =
  | { kind: 'idle' }
  | { kind: 'consuming' }
  | { kind: 'ok'; message: string }
  | { kind: 'err'; message: string }


export default function RefreshTokenPage() {
  const [result, setResult] = useState<Result>({ kind: 'idle' })
  const [token,  setToken]  = useState<string | null>(null)

  useEffect(() => {
    if (typeof window === 'undefined') return
    const t = new URLSearchParams(window.location.search).get('t')
    if (!t) {
      setResult({ kind: 'err', message: 'Missing refresh token in URL. Open the link from the notification.' })
      return
    }
    setToken(t)
    setResult({ kind: 'consuming' })
    void (async () => {
      try {
        const r = await fetch(`${API}/api/power/auth-refresh/consume`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ token: t }),
        })
        if (!r.ok) {
          const body = await r.json().catch(() => ({}))
          const msg  = body.detail?.message
                      ?? `Refresh failed (HTTP ${r.status}). Try again from /power/admin.`
          setResult({ kind: 'err', message: msg })
          return
        }
        const body = await r.json()
        setResult({ kind: 'ok', message: body.message || 'Refresh triggered.' })
      } catch (e) {
        setResult({ kind: 'err', message: e instanceof Error ? e.message : 'Network error.' })
      }
    })()
  }, [])

  return (
    <div className="max-w-md mx-auto py-20 px-4">
      <h1 className="text-2xl font-bold text-neutral-100 mb-2">
        Zerodha token refresh
      </h1>
      <p className="text-sm text-neutral-500 mb-8">
        One-time link from the morning auto-auth fallback notification.
      </p>

      {result.kind === 'consuming' && (
        <div className="flex items-center gap-3 text-amber-200">
          <span className="inline-block w-4 h-4 border-2 border-amber-300 border-t-transparent rounded-full animate-spin" />
          <span>Validating link and triggering refresh…</span>
        </div>
      )}

      {result.kind === 'ok' && (
        <div className="space-y-4">
          <div role="status" className="px-4 py-3 rounded border border-green-500/40 bg-green-500/10 text-green-200">
            ✓ {result.message}
          </div>
          <p className="text-sm text-neutral-400">
            The bot is running a fresh Playwright login attempt now. Refresh{' '}
            <a className="underline hover:text-neutral-200" href="/power/admin">/power/admin</a>{' '}
            in ~30 seconds to confirm the token is VALID.
          </p>
        </div>
      )}

      {result.kind === 'err' && (
        <div className="space-y-4">
          <div role="alert" className="px-4 py-3 rounded border border-red-500/40 bg-red-500/10 text-red-200">
            {result.message}
          </div>
          <p className="text-sm text-neutral-400">
            Sign in at{' '}
            <a className="underline hover:text-neutral-200" href="/power/admin">/power/admin</a>{' '}
            and click <span className="font-semibold">Refresh token now</span> to recover manually.
          </p>
        </div>
      )}

      {token && (
        <p className="mt-12 text-[10px] text-neutral-700 font-mono break-all">
          {token.slice(0, 8)}…{token.slice(-4)}
        </p>
      )}
    </div>
  )
}
