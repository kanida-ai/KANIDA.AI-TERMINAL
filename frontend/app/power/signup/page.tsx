'use client'

/**
 * /power/signup — open funnel (M7). Email + optional invite code.
 *
 * Flow (CONTRACT §3.1):
 *   POST /api/power/auth/signup {email, invite_code?}
 *     → access:'full'             (valid code → free `comp`)  → store token → /power/today
 *     → access:'payment_required' (no/invalid code → `blocked`) → store token → /power/billing
 *
 * Auth/session convention matched from /power/login:
 *   storeSessionJWT(token) POSTs to /api/power/session, which sets the HTTPOnly
 *   `power_jwt` cookie. We never touch the token in client JS beyond handing it
 *   to that Route Handler. router.refresh() forces the layout to re-read the
 *   new cookie so the user is recognised as signed-in on the next page.
 *
 * Legal gate: a Risk Disclosure + Terms consent checkbox is REQUIRED to submit
 * (links to /legal/risk + /legal/terms, owned by M6).
 */
import { useRouter } from 'next/navigation'
import { useState } from 'react'
import Link from 'next/link'
import { PowerAPIError } from '@/lib/power-api'
import { signupWithEmail, storeSessionJWT } from '@/lib/power-auth-client'

export default function SignupPage() {
  const router = useRouter()
  const [email,   setEmail]   = useState('')
  const [code,    setCode]    = useState('')
  const [consent, setConsent] = useState(false)
  const [busy,    setBusy]    = useState(false)
  const [error,   setError]   = useState<string | null>(null)

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)

    const cleanEmail = email.trim().toLowerCase()
    if (!cleanEmail || !cleanEmail.includes('@')) {
      setError('Enter a valid email address.')
      return
    }
    if (!consent) {
      setError('Please accept the Risk Disclosure and Terms to continue.')
      return
    }

    setBusy(true)
    try {
      const res = await signupWithEmail(cleanEmail, code)
      // Both outcomes are authenticated — store the token either way.
      await storeSessionJWT(res.token)
      // Route on access: full → product; payment_required → billing.
      const dest = res.access === 'full' ? '/power/today' : '/power/billing'
      router.push(dest)
      router.refresh()
    } catch (err) {
      if (err instanceof PowerAPIError) {
        if (err.code === 'EMAIL_EXISTS' || err.status === 409) {
          setError('An account with this email already exists. Sign in instead.')
        } else {
          setError(err.message || 'Could not create your account. Try again.')
        }
      } else {
        setError('Network error. Check your connection.')
      }
      setBusy(false)
    }
  }

  return (
    <div className="max-w-md mx-auto py-12 md:py-20">
      <h1 className="text-2xl md:text-3xl font-bold mb-2">Create your account</h1>
      <p className="text-sm text-neutral-400 mb-6">
        Start with ₹999/month, or redeem an invite code for free access.
        No card needed to create the account.
      </p>

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

        <Field label="Invite code" hint="Optional — kn-2026-xxxxxx. With a valid code, your account is free.">
          <input
            type="text"
            inputMode="text"
            autoComplete="off"
            value={code}
            onChange={e => setCode(e.target.value)}
            placeholder="kn-2026-xxxxxx (optional)"
            className="w-full bg-neutral-950 border border-neutral-700 rounded px-3 py-2 text-neutral-100 font-mono focus:outline-none focus:border-mint-500/60"
          />
        </Field>

        <label className="flex items-start gap-2.5 cursor-pointer">
          <input
            type="checkbox"
            checked={consent}
            onChange={e => setConsent(e.target.checked)}
            className="mt-0.5 h-4 w-4 shrink-0 accent-mint-400"
          />
          <span className="text-xs text-neutral-400 leading-relaxed">
            I understand KANIDA.AI surfaces engineered patterns, not financial advice,
            and I accept the{' '}
            <Link href="/legal/risk" target="_blank"
                  className="text-mint-400 underline hover:text-mint-300">Risk Disclosure</Link>{' '}
            and{' '}
            <Link href="/legal/terms" target="_blank"
                  className="text-mint-400 underline hover:text-mint-300">Terms of Service</Link>.
          </span>
        </label>

        <button
          type="submit"
          disabled={busy || !consent}
          className="w-full px-4 py-2 bg-mint-400 text-neutral-950 rounded font-semibold hover:bg-mint-300 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {busy ? 'Creating account…' : 'Create account'}
        </button>

        {error && (
          <p role="alert" className="px-3 py-2 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
            {error}
          </p>
        )}
      </form>

      <div className="mt-6 text-center text-sm text-neutral-500 space-y-1">
        <p>
          Already have an account?{' '}
          <Link href="/power/login" className="text-mint-400 underline hover:text-mint-300">Sign in →</Link>
        </p>
        <p className="text-xs text-neutral-600">
          See{' '}
          <Link href="/power/pricing" className="underline hover:text-neutral-400">full pricing</Link>.
        </p>
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
