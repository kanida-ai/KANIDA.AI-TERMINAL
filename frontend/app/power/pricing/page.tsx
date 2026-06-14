/**
 * /power/pricing — single commercial plan + "have an invite code?" path.
 *
 * Public marketing page (no auth gate). One plan card: ₹999/month. Both CTAs
 * route to /power/signup — the signup form itself collects the optional invite
 * code (valid code → free `comp` account; no code → Razorpay checkout). Keeping
 * the code entry on /signup avoids a second form here and matches the M7
 * funnel: pricing → signup → (full | billing).
 *
 * Styling mirrors /power/login + /power/waitlist (neutral-900 cards, mint CTA).
 */
import Link from 'next/link'
import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'Pricing — KANIDA.AI Power User',
  description: 'One plan, ₹999/month. Daily ranked picks with the reason, '
             + 'walk-forward-validated. Have an invite code? It is free.',
}

const FEATURES = [
  'Daily Falcon Top 10 — ranked picks with the engineered reason behind each',
  'Co-Trading personas — locked, audit-ready backtests (₹30L → ₹1.05Cr walk-forward)',
  'Live intraday ENTER / WAIT / SKIP overlay',
  'Full historical replay — see how every past pick actually played out',
  'Per-position SL / target / trail guidance',
]

export default function PricingPage() {
  return (
    <div className="max-w-2xl mx-auto py-12 md:py-16">
      <div className="text-center mb-10">
        <h1 className="text-3xl md:text-4xl font-bold mb-3">Simple pricing</h1>
        <p className="text-sm md:text-base text-neutral-400 max-w-lg mx-auto">
          One plan. No tiers, no upsells. Cancel anytime. Founding members and
          invited traders keep full access for free.
        </p>
      </div>

      <div className="bg-neutral-900 border border-neutral-800 rounded-xl p-6 md:p-8">
        <div className="flex items-baseline gap-2">
          <span className="text-4xl md:text-5xl font-bold text-white">₹999</span>
          <span className="text-neutral-400 text-sm">/ month</span>
        </div>
        <p className="text-xs text-neutral-500 mt-1">
          Billed monthly via Razorpay. GST as applicable. Cancel anytime.
        </p>

        <ul className="mt-6 space-y-3">
          {FEATURES.map(f => (
            <li key={f} className="flex items-start gap-3 text-sm text-neutral-200">
              <span aria-hidden className="text-mint-400 mt-0.5 shrink-0">✓</span>
              <span>{f}</span>
            </li>
          ))}
        </ul>

        <Link
          href="/power/signup"
          className="mt-8 block w-full text-center px-4 py-3 bg-mint-400 text-neutral-950
                      rounded-md font-semibold hover:bg-mint-300 transition-colors"
        >
          Get started — ₹999/mo
        </Link>

        <p className="mt-4 text-center text-sm text-neutral-400">
          Have an invite code?{' '}
          <Link href="/power/signup" className="text-mint-400 underline hover:text-mint-300">
            Redeem it on signup — it&apos;s free
          </Link>
          .
        </p>
      </div>

      <p className="text-center text-xs text-neutral-600 mt-8">
        Already have an account?{' '}
        <Link href="/power/login" className="underline hover:text-neutral-400">Sign in →</Link>
      </p>

      <p className="text-center text-[11px] text-neutral-600 mt-6 max-w-lg mx-auto">
        KANIDA.AI surfaces engineered, historically-validated patterns — not financial
        advice. You own your trades. See our{' '}
        <Link href="/legal/risk" className="underline hover:text-neutral-400">Risk Disclosure</Link>{' '}
        and{' '}
        <Link href="/legal/terms" className="underline hover:text-neutral-400">Terms</Link>.
      </p>
    </div>
  )
}
