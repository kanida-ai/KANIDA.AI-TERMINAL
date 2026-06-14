'use client'

/**
 * Billing action buttons (M7) — Subscribe / Cancel.
 *
 * Calls the billing Server Actions (which read the HTTPOnly JWT server-side).
 * - Subscribe: action returns the Razorpay hosted-checkout `short_url`; we send
 *   the browser there with window.location.href. Card data never touches us.
 * - Cancel: action acks; we router.refresh() so the server re-reads status.
 */
import { useState, useTransition } from 'react'
import { useRouter } from 'next/navigation'
import { subscribeAction, cancelAction } from './actions'

export function SubscribeButton({ label }: { label: string }) {
  const [pending, start] = useTransition()
  const [error, setError] = useState<string | null>(null)

  const onClick = () => {
    setError(null)
    start(async () => {
      const res = await subscribeAction()
      if (res.ok && res.redirectUrl) {
        // Razorpay hosted checkout — full-page navigation off our site.
        window.location.href = res.redirectUrl
        return
      }
      setError(res.ok ? 'Checkout link unavailable.' : res.error)
    })
  }

  return (
    <div className="space-y-2">
      <button
        onClick={onClick}
        disabled={pending}
        className="w-full px-4 py-3 bg-mint-400 text-neutral-950 rounded-md font-semibold
                    hover:bg-mint-300 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {pending ? 'Starting checkout…' : label}
      </button>
      <p className="text-[11px] text-neutral-500 text-center">
        Secure checkout hosted by Razorpay. Your card details never touch KANIDA.AI.
      </p>
      {error && (
        <p role="alert" className="px-3 py-2 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
          {error}
        </p>
      )}
    </div>
  )
}

export function CancelButton() {
  const router = useRouter()
  const [pending, start] = useTransition()
  const [error, setError] = useState<string | null>(null)
  const [confirming, setConfirming] = useState(false)

  const onConfirm = () => {
    setError(null)
    start(async () => {
      const res = await cancelAction()
      if (res.ok) {
        setConfirming(false)
        router.refresh()
        return
      }
      setError(res.error)
    })
  }

  if (!confirming) {
    return (
      <button
        onClick={() => setConfirming(true)}
        className="text-sm text-neutral-400 underline hover:text-neutral-200"
      >
        Cancel subscription
      </button>
    )
  }

  return (
    <div className="space-y-2 rounded-lg border border-red-500/30 bg-red-500/[0.06] p-4">
      <p className="text-sm text-red-200">
        Cancel your subscription? You&apos;ll keep access until the end of your current
        billing period, then your account is paused.
      </p>
      <div className="flex items-center gap-3">
        <button
          onClick={onConfirm}
          disabled={pending}
          className="px-3 py-1.5 bg-red-500 text-white rounded font-semibold text-sm
                      hover:bg-red-400 disabled:opacity-50"
        >
          {pending ? 'Cancelling…' : 'Yes, cancel'}
        </button>
        <button
          onClick={() => { setConfirming(false); setError(null) }}
          disabled={pending}
          className="text-sm text-neutral-400 hover:text-neutral-200"
        >
          Keep my subscription
        </button>
      </div>
      {error && (
        <p role="alert" className="px-3 py-2 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
          {error}
        </p>
      )}
    </div>
  )
}
