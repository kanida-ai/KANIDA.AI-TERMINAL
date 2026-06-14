/**
 * PaywallNotice (M7) — rendered by a product page when a gated backend call
 * returns HTTP 402 (CONTRACT §3.3 `{code:'PAYMENT_REQUIRED', checkout_url}`).
 *
 * Instead of hard-crashing or showing a generic red error banner, the page
 * shows this "Subscribe to continue" state. The primary CTA points at
 * /power/billing (our own page, which then drives the Razorpay hosted
 * checkout). If the 402 carried a `checkoutUrl` we also surface a direct link.
 *
 * Server component — no client JS needed; it's just a styled link.
 */
import Link from 'next/link'

export function PaywallNotice({
  feature = 'this',
  checkoutUrl = null,
}: {
  /** Short label for the gated surface, e.g. "today's picks", "Co-Trading". */
  feature?: string
  /** Optional direct Razorpay/billing link from the 402 body. */
  checkoutUrl?: string | null
}) {
  return (
    <div className="max-w-lg mx-auto my-10 rounded-xl border border-amber-400/30 bg-amber-400/[0.06] p-6 md:p-8 text-center">
      <p className="text-lg md:text-xl font-bold text-white">
        Subscribe to continue
      </p>
      <p className="text-sm text-neutral-300 mt-2">
        Access to {feature === 'this' ? 'this feature' : feature} needs an active
        KANIDA.AI subscription — ₹999/month. Founding members and invited traders
        keep full access for free.
      </p>

      <div className="mt-6 flex flex-col gap-2">
        <Link
          href="/power/billing"
          className="w-full px-4 py-3 bg-mint-400 text-neutral-950 rounded-md font-semibold
                      hover:bg-mint-300 transition-colors"
        >
          Subscribe ₹999/mo
        </Link>
        {checkoutUrl && (
          <a
            href={checkoutUrl}
            className="text-xs text-neutral-400 underline hover:text-neutral-200"
          >
            Continue to checkout directly →
          </a>
        )}
      </div>

      <p className="text-[11px] text-neutral-500 mt-5">
        Have an invite code?{' '}
        <Link href="/power/billing" className="underline hover:text-neutral-300">
          Manage your account
        </Link>
        . Secure checkout is hosted by Razorpay — card details never touch KANIDA.AI.
      </p>
    </div>
  )
}
