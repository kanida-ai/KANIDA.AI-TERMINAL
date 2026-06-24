'use client'

/**
 * /power/login — Phase 1b email + invite-code sign-in.
 *
 * Three accepted shapes (server distinguishes; UI just collects both fields):
 *   1. NEW USER       email + valid unused invite code (kn-2026-xxxxxx)
 *   2. RETURNING USER email only (code field can be left blank)
 *   3. ADMIN          admin email + POWER_ADMIN_SECRET
 *
 * The backend's GENERIC_LOGIN_FAIL contract intentionally collapses every
 * failure mode (email unknown, code unused, code expired, wrong admin secret)
 * into the same response body. The UI mirrors that — never tell the user
 * "email unknown" vs "code wrong"; both look like one generic error message.
 */
import { useRouter, useSearchParams } from 'next/navigation'
import { useState } from 'react'
import Link from 'next/link'
import { PowerAPIError } from '@/lib/power-api'
import { loginWithInviteCode, storeSessionJWT } from '@/lib/power-auth-client'

export default function LoginPage() {
  const router       = useRouter()
  const search       = useSearchParams()
  const [email, setEmail] = useState('')
  const [code,  setCode]  = useState('')
  const [busy,  setBusy]  = useState(false)
  const [error, setError] = useState<string | null>(null)
  const expired = search.get('expired') === '1'

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    const cleanEmail = email.trim().toLowerCase()
    if (!cleanEmail || !cleanEmail.includes('@')) {
      setError('Enter a valid email address.')
      return
    }
    setBusy(true)
    try {
      const result = await loginWithInviteCode(cleanEmail, code.trim())
      await storeSessionJWT(result.jwt)
      // After-login destination: admins go to /power/admin (their landing),
      // everyone else to /power/ask (the new AI-native shell home). The
      // router.push will hit the server and the layout will re-render with the
      // new auth cookie. (Legacy /power/today stays reachable from the shell.)
      const dest = result.user.role === 'admin' ? '/power/admin' : '/power/ask'
      router.push(dest)
      router.refresh()
    } catch (e) {
      if (e instanceof PowerAPIError) {
        setError(e.message || 'Sign-in failed. Try again.')
      } else {
        setError('Network error. Check your connection.')
      }
      setBusy(false)
    }
  }

  return (
    <div className="max-w-md mx-auto py-12 md:py-20">
      <h1 className="text-2xl md:text-3xl font-bold mb-2">Sign in</h1>
      <p className="text-sm text-neutral-400 mb-6">
        Power User beta {'—'} invite only. Enter your email and the invite code your admin shared.
        Returning users can leave the code field blank.
      </p>

      {expired && (
        <div role="alert"
              className="mb-4 px-3 py-2 rounded bg-yellow-500/10 text-yellow-200 border border-yellow-500/40 text-sm">
          Your session expired. Sign in again.
        </div>
      )}

      <form onSubmit={onSubmit} className="bg-neutral-900 border border-neutral-800 rounded-lg p-6 space-y-4">
        <Field label="Email">
          <input
            type="email"
            inputMode="email"
            autoComplete="email"
            autoFocus
            required
            value={email}
            onChange={e => setEmail(e.target.value)}
            placeholder="you@example.com"
            className="w-full bg-neutral-950 border border-neutral-700 rounded px-3 py-2 text-neutral-100 focus:outline-none focus:border-mint-500/60"
          />
        </Field>

        <Field label="Invite code" hint="kn-2026-xxxxxx (leave blank if you're a returning user)">
          <input
            type="text"
            inputMode="text"
            autoComplete="off"
            value={code}
            onChange={e => setCode(e.target.value)}
            placeholder="kn-2026-xxxxxx"
            className="w-full bg-neutral-950 border border-neutral-700 rounded px-3 py-2 text-neutral-100 font-mono focus:outline-none focus:border-mint-500/60"
          />
        </Field>

        <button
          type="submit"
          disabled={busy}
          className="w-full px-4 py-2 bg-mint-400 text-neutral-950 rounded font-semibold hover:bg-mint-300 disabled:opacity-50"
        >
          {busy ? 'Signing in…' : 'Sign in'}
        </button>

        {error && (
          <p role="alert" className="px-3 py-2 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
            {error}
          </p>
        )}
      </form>

      <div className="mt-6 text-center text-sm text-neutral-500 space-y-2">
        <p>No invite yet?</p>
        <Link href="/power/waitlist"
              className="inline-block px-4 py-2 text-mint-400 hover:text-mint-300 underline">
          Join the waitlist {'→'}
        </Link>
      </div>
    </div>
  )
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="block text-xs uppercase tracking-wider text-neutral-400 mb-1">{label}</span>
      {children}
      {hint && <span className="block text-[11px] text-neutral-500 mt-1">{hint}</span>}
    </label>
  )
}
