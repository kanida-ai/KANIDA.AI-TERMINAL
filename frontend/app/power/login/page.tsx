'use client'

/**
 * /power/login — Google sign-in entry.
 *
 * Two outcomes after Google authenticates:
 *   - status='ok'             → store JWT cookie, redirect to /power/today
 *   - status='needs_invite'   → stash Google id_token in sessionStorage,
 *                                redirect to /power/redeem
 *
 * The id_token is short-lived (Google: 1h). sessionStorage is acceptable —
 * it persists only for the tab/session and is cleared on tab close. The
 * redeem flow uses it once then deletes it.
 */
import { useRouter, useSearchParams } from 'next/navigation'
import { useState } from 'react'
import Link from 'next/link'
import { GoogleSignInButton } from '@/components/power/GoogleSignInButton'
import { PowerAPIError } from '@/lib/power-api'
import { exchangeGoogleIdToken, storeSessionJWT } from '@/lib/power-auth-client'

const STASH_KEY = 'power_pending_id_token'

export default function LoginPage() {
  const router       = useRouter()
  const search       = useSearchParams()
  const [busy, setBusy]     = useState(false)
  const [error, setError]   = useState<string | null>(null)
  const expired      = search.get('expired') === '1'

  const onCredential = async (idToken: string) => {
    setError(null)
    setBusy(true)
    try {
      const result = await exchangeGoogleIdToken(idToken)
      if (result.status === 'ok') {
        await storeSessionJWT(result.jwt)
        router.push('/power/today')
        router.refresh()
      } else {
        // NEEDS_INVITE — stash the id_token + email so redeem page can finish the flow
        sessionStorage.setItem(STASH_KEY, idToken)
        sessionStorage.setItem('power_pending_email', result.email)
        if (result.display_name) {
          sessionStorage.setItem('power_pending_name', result.display_name)
        }
        router.push('/power/redeem')
      }
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
        Power User beta — invite only. Sign in with Google to enter your invite code,
        or join the waitlist below.
      </p>

      {expired && (
        <div role="alert"
              className="mb-4 px-3 py-2 rounded bg-yellow-500/10 text-yellow-200
                          border border-yellow-500/40 text-sm">
          Your session expired. Sign in again.
        </div>
      )}

      <div className="bg-neutral-900 border border-neutral-800 rounded-lg p-6 flex justify-center">
        {busy
          ? <p className="text-sm text-neutral-400">Verifying…</p>
          : <GoogleSignInButton
              onCredential={onCredential}
              onError={msg => setError(msg)}
            />}
      </div>

      {error && (
        <p role="alert" className="mt-3 px-3 py-2 rounded bg-red-500/10
                                     text-red-200 border border-red-500/40 text-sm">
          {error}
        </p>
      )}

      <div className="mt-6 text-center text-sm text-neutral-500 space-y-2">
        <p>No invite yet?</p>
        <Link href="/power/waitlist"
              className="inline-block px-4 py-2 text-amber-400 hover:text-amber-300 underline">
          Join the waitlist →
        </Link>
      </div>
    </div>
  )
}
