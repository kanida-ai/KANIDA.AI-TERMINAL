'use client'

/**
 * /power/redeem — invite code redemption after Google sign-in returned NEEDS_INVITE.
 *
 * Sequence:
 *   1. Read stashed Google id_token + email from sessionStorage (set by /login)
 *   2. User pastes invite code → POST /api/power/invites/redeem { id_token, code }
 *   3. On success: store JWT cookie, redirect to /power/today, clear stash
 *   4. On failure: UNIFORM error message (backend returns INVITE_INVALID for
 *      any failure mode — bad format / expired / used / not found), offer
 *      "join waitlist" link
 *
 * Important: we never reveal which failure mode (operator policy — prevents
 * code enumeration). UI just says "code not valid".
 */
import { useRouter } from 'next/navigation'
import { useEffect, useState } from 'react'
import Link from 'next/link'
import { PowerAPIError } from '@/lib/power-api'
import {
  joinWaitlist,
  redeemInviteCode,
  storeSessionJWT,
} from '@/lib/power-auth-client'

const STASH_KEY    = 'power_pending_id_token'
const STASH_EMAIL  = 'power_pending_email'
const STASH_NAME   = 'power_pending_name'

export default function RedeemPage() {
  const router = useRouter()
  const [idToken, setIdToken]   = useState<string | null>(null)
  const [email, setEmail]       = useState<string | null>(null)
  const [code, setCode]         = useState('')
  const [busy, setBusy]         = useState(false)
  const [error, setError]       = useState<string | null>(null)
  const [waitlistMsg, setWLMsg] = useState<string | null>(null)

  // Pull stashed id_token from sessionStorage on mount
  useEffect(() => {
    const t = sessionStorage.getItem(STASH_KEY)
    const e = sessionStorage.getItem(STASH_EMAIL)
    if (t && e) {
      setIdToken(t)
      setEmail(e)
    } else {
      // No stash — user got here directly. Send them back to login.
      router.replace('/power/login')
    }
  }, [router])

  const onRedeem = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!idToken) return
    const trimmed = code.trim().toLowerCase()
    if (!trimmed) return
    setBusy(true)
    setError(null)
    try {
      const result = await redeemInviteCode(idToken, trimmed)
      // Success — store JWT cookie + clear stash + go
      await storeSessionJWT(result.jwt)
      sessionStorage.removeItem(STASH_KEY)
      sessionStorage.removeItem(STASH_EMAIL)
      sessionStorage.removeItem(STASH_NAME)
      router.push('/power/today')
      router.refresh()
    } catch (err) {
      // Uniform error message regardless of internal cause
      const fallback = 'This invite code is not valid. It may have been used, '
                      + 'expired, or entered incorrectly.'
      const msg = err instanceof PowerAPIError ? (err.message || fallback) : fallback
      setError(msg)
      setBusy(false)
    }
  }

  const onJoinWaitlist = async () => {
    if (!email) return
    setBusy(true)
    try {
      const r = await joinWaitlist(email, 'login_no_invite')
      setWLMsg(r.joined
        ? `You're on the waitlist as ${email}. We'll email when an invite opens up.`
        : `${email} is already on the waitlist — we'll be in touch.`)
    } catch {
      setWLMsg(`Could not add ${email} to the waitlist. Try /power/waitlist directly.`)
    } finally {
      setBusy(false)
    }
  }

  if (!idToken) {
    return (
      <div className="max-w-md mx-auto py-20 text-center text-neutral-400">
        Redirecting to sign-in…
      </div>
    )
  }

  return (
    <div className="max-w-md mx-auto py-12 md:py-20">
      <h1 className="text-2xl md:text-3xl font-bold mb-2">Enter your invite</h1>
      <p className="text-sm text-neutral-400 mb-6">
        Signed in as <span className="text-neutral-200 font-mono">{email}</span>.{' '}
        Beta access requires an invite code (12-18 issued for the Phase 1 cohort).
      </p>

      <form onSubmit={onRedeem} className="space-y-3">
        <input
          type="text"
          value={code}
          onChange={e => setCode(e.target.value)}
          placeholder="kn-2026-xxxxxx"
          autoFocus
          autoComplete="off"
          className="w-full bg-neutral-950 border border-neutral-700 rounded px-3 py-2
                      font-mono text-neutral-100 placeholder-neutral-600
                      focus:outline-none focus:border-amber-500/60"
        />
        <button
          type="submit"
          disabled={busy || code.trim().length < 4}
          className="w-full px-4 py-2.5 bg-amber-500 text-neutral-950 rounded-md
                      font-semibold hover:bg-amber-400 transition-colors
                      disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {busy ? 'Redeeming…' : 'Redeem code'}
        </button>
      </form>

      {error && (
        <p role="alert" className="mt-3 px-3 py-2 rounded bg-red-500/10 text-red-200
                                     border border-red-500/40 text-sm">
          {error}
        </p>
      )}

      <div className="mt-8 pt-6 border-t border-neutral-800 space-y-3">
        <p className="text-sm text-neutral-400">No invite yet?</p>
        {waitlistMsg ? (
          <p className="text-sm text-green-300 px-3 py-2 bg-green-500/10
                          border border-green-500/30 rounded">
            ✓ {waitlistMsg}
          </p>
        ) : (
          <button
            type="button"
            onClick={onJoinWaitlist}
            disabled={busy || !email}
            className="text-sm text-amber-400 hover:text-amber-300 underline disabled:opacity-50"
          >
            Add {email ?? 'this email'} to the waitlist →
          </button>
        )}
        <p className="text-xs text-neutral-600">
          Or go back to{' '}
          <Link href="/power" className="underline hover:text-neutral-400">
            the home page
          </Link>
          .
        </p>
      </div>
    </div>
  )
}
