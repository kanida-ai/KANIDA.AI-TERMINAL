'use client'

import Link from 'next/link'
import { useState } from 'react'
import { PowerAPIError } from '@/lib/power-api'
import { joinWaitlist } from '@/lib/power-auth-client'

export default function WaitlistPage() {
  const [email, setEmail]   = useState('')
  const [busy, setBusy]     = useState(false)
  const [done, setDone]     = useState(false)
  const [error, setError]   = useState<string | null>(null)

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    const trimmed = email.trim()
    if (!trimmed || !trimmed.includes('@')) return
    setBusy(true)
    setError(null)
    try {
      await joinWaitlist(trimmed, 'landing_cta')
      setDone(true)
    } catch (err) {
      if (err instanceof PowerAPIError && err.isRateLimited()) {
        setError("Too many waitlist signups from this network in the last hour. Try again later.")
      } else {
        setError("Couldn't add you to the waitlist right now. Try refreshing.")
      }
      setBusy(false)
    }
  }

  return (
    <div className="max-w-md mx-auto py-12 md:py-20">
      <h1 className="text-2xl md:text-3xl font-bold mb-2">Join the waitlist</h1>
      <p className="text-sm text-neutral-400 mb-6">
        Power User beta is invite-only — 12-18 traders for Phase 1. Drop your
        email and we'll reach out when an invite opens.
      </p>

      {done ? (
        <div className="bg-green-500/10 border border-green-500/30 rounded p-4 space-y-2">
          <p className="text-green-200 font-semibold">✓ You're on the list.</p>
          <p className="text-sm text-green-300/80">
            We'll email <span className="font-mono">{email}</span> when an invite opens up.
            In the meantime, browse{' '}
            <Link href="/power" className="underline">the public preview</Link>.
          </p>
        </div>
      ) : (
        <form onSubmit={onSubmit} className="space-y-3">
          <input
            type="email"
            value={email}
            onChange={e => setEmail(e.target.value)}
            placeholder="you@example.com"
            required
            autoFocus
            className="w-full bg-neutral-950 border border-neutral-700 rounded
                        px-3 py-2 text-neutral-100 placeholder-neutral-600
                        focus:outline-none focus:border-mint-500/60"
          />
          <button
            type="submit"
            disabled={busy || !email.includes('@')}
            className="w-full px-4 py-2.5 bg-mint-500 text-neutral-950 rounded-md
                        font-semibold hover:bg-mint-400 transition-colors
                        disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {busy ? 'Adding…' : 'Add me to the waitlist'}
          </button>
          {error && (
            <p role="alert" className="text-sm px-3 py-2 rounded bg-red-500/10
                                         text-red-200 border border-red-500/40">
              {error}
            </p>
          )}
        </form>
      )}

      <p className="text-xs text-neutral-600 mt-6">
        Already have a code?{' '}
        <Link href="/power/login" className="underline hover:text-neutral-400">Sign in →</Link>
      </p>
    </div>
  )
}
