/**
 * /power/billing — subscription status + manage (M7).
 *
 * Server component. Auth-gated via requireSession() (same convention as
 * /power/today, /power/live). Reads GET /api/power/billing/status with the
 * server-side JWT, then renders one of three states:
 *
 *   • full access, no paid sub (founding / comp / admin)
 *       → "You have full access" — no billing needed.
 *   • paid + active
 *       → status card + renewal date + Cancel button.
 *   • blocked / unpaid / lapsed
 *       → paywall card with "Subscribe ₹999/mo" (Razorpay hosted checkout).
 *
 * Gate semantics mirror CONTRACT §2 exactly so this page agrees with the
 * backend paywall (M4): admin OR plan in {founding,comp} OR (paid & active).
 */
import Link from 'next/link'
import { requireSession } from '@/lib/power-auth'
import { PowerAPI, type BillingStatusResponse } from '@/lib/power-api'
import { SubscribeButton, CancelButton } from './BillingActions'

export const dynamic = 'force-dynamic'
export const revalidate = 0

export default async function BillingPage() {
  const { jwt, user } = await requireSession()

  let status: BillingStatusResponse | null = null
  let fetchError: string | null = null
  try {
    status = await PowerAPI.billingStatus(jwt)
  } catch (e) {
    fetchError = e instanceof Error ? e.message : 'Failed to load billing status.'
  }

  const isAdmin   = user.role === 'admin'
  const plan      = status?.billing_plan
  const subStatus = status?.subscription_status

  // CONTRACT §2 allow predicate.
  const hasAccess =
    isAdmin ||
    plan === 'founding' || plan === 'comp' ||
    (plan === 'paid' && subStatus === 'active')

  // Comp/founding/admin keep access without ever paying.
  const freeForever = isAdmin || plan === 'founding' || plan === 'comp'
  const paidActive  = plan === 'paid' && subStatus === 'active'

  return (
    <div className="max-w-xl mx-auto py-10 md:py-14">
      <h1 className="text-2xl md:text-3xl font-bold mb-2">Billing</h1>
      <p className="text-sm text-neutral-400 mb-6">
        Manage your KANIDA.AI subscription.
      </p>

      {fetchError && (
        <div role="alert" className="mb-4 px-4 py-3 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
          {fetchError} <span className="text-red-300/70">Refresh to retry.</span>
        </div>
      )}

      {status && (
        <div className="bg-neutral-900 border border-neutral-800 rounded-xl p-6 space-y-5">
          <StatusRow
            plan={plan ?? '—'}
            subStatus={subStatus ?? '—'}
            currentEnd={status.current_end}
            hasAccess={hasAccess}
          />

          {freeForever && (
            <div className="rounded-lg bg-mint-400/[0.06] border border-mint-400/30 p-4">
              <p className="text-sm text-mint-200 font-semibold">
                You have full access — no payment required.
              </p>
              <p className="text-xs text-neutral-400 mt-1">
                {isAdmin
                  ? 'Admin account.'
                  : plan === 'founding'
                    ? 'Founding member — free for life.'
                    : 'Comped via invite code.'}
              </p>
              <Link
                href="/power/today"
                className="inline-block mt-3 text-sm text-mint-300 underline hover:text-mint-200"
              >
                Go to today&apos;s picks →
              </Link>
            </div>
          )}

          {paidActive && (
            <div className="space-y-4">
              <div className="rounded-lg bg-neutral-950 border border-neutral-800 p-4">
                <p className="text-sm text-neutral-200">
                  Your <span className="font-semibold text-white">₹999/month</span> subscription is{' '}
                  <span className="text-mint-300 font-semibold">active</span>.
                </p>
                {status.current_end && (
                  <p className="text-xs text-neutral-500 mt-1">
                    Renews on <span className="font-mono text-neutral-300">{fmtDate(status.current_end)}</span>.
                  </p>
                )}
              </div>
              <CancelButton />
            </div>
          )}

          {!freeForever && !paidActive && (
            <div className="space-y-4">
              <div className="rounded-lg bg-amber-400/[0.06] border border-amber-400/30 p-4">
                <p className="text-sm text-amber-200 font-semibold">
                  Your access is paused.
                </p>
                <p className="text-xs text-neutral-400 mt-1">
                  {subStatus && subStatus !== 'active'
                    ? `Subscription status: ${subStatus}. `
                    : ''}
                  Subscribe to unlock daily picks, Co-Trading, and the live overlay.
                </p>
              </div>
              <div className="rounded-lg bg-neutral-950 border border-neutral-800 p-5">
                <div className="flex items-baseline gap-2 mb-3">
                  <span className="text-3xl font-bold text-white">₹999</span>
                  <span className="text-neutral-400 text-sm">/ month</span>
                </div>
                <SubscribeButton label="Subscribe ₹999/mo" />
              </div>
            </div>
          )}
        </div>
      )}

      <p className="text-center text-[11px] text-neutral-600 mt-6">
        By subscribing you agree to our{' '}
        <Link href="/legal/terms" className="underline hover:text-neutral-400">Terms</Link> and{' '}
        <Link href="/legal/risk" className="underline hover:text-neutral-400">Risk Disclosure</Link>.
      </p>
    </div>
  )
}

function StatusRow({
  plan, subStatus, currentEnd, hasAccess,
}: {
  plan: string; subStatus: string; currentEnd: string | null; hasAccess: boolean
}) {
  return (
    <div className="grid grid-cols-2 gap-3 text-sm">
      <Cell label="Plan" value={titleCase(plan)} />
      <Cell
        label="Access"
        value={hasAccess ? 'Active' : 'Paused'}
        accent={hasAccess ? 'mint' : 'amber'}
      />
      <Cell label="Subscription" value={titleCase(subStatus)} />
      <Cell label="Valid until" value={currentEnd ? fmtDate(currentEnd) : '—'} />
    </div>
  )
}

function Cell({
  label, value, accent,
}: { label: string; value: string; accent?: 'mint' | 'amber' }) {
  const color =
    accent === 'mint'  ? 'text-mint-300' :
    accent === 'amber' ? 'text-amber-300' :
    'text-neutral-200'
  return (
    <div className="bg-neutral-950 border border-neutral-800 rounded px-3 py-2">
      <span className="block text-[11px] uppercase tracking-wider text-neutral-500">{label}</span>
      <span className={`block font-semibold ${color}`}>{value}</span>
    </div>
  )
}

function titleCase(s: string): string {
  if (!s || s === '—') return s
  return s.charAt(0).toUpperCase() + s.slice(1)
}

/** Render an ISO/TIMESTAMPTZ string as a date in IST (INV4). */
function fmtDate(iso: string): string {
  try {
    return new Date(iso).toLocaleDateString('en-IN', {
      timeZone: 'Asia/Kolkata',
      year: 'numeric', month: 'short', day: 'numeric',
    })
  } catch {
    return iso
  }
}
