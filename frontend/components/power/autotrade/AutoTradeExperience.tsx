'use client'

/**
 * AutoTradeExperience — the Kanida.AI AutoTrade page for POWER USERS (non-admin).
 *
 * A complete, guided, self-serve flow on the user's OWN broker account:
 *   intro → connect broker → create + Start/Schedule a live session → live status.
 *
 * SCOPE (real money — strict honesty):
 *   • Connecting a broker places NO order. The real creator (PortfolioAutoTrade)
 *     defaults to PAPER; a LIVE session additionally requires the server flag
 *     FALCON_AUTOTRADE_ENABLED — so nothing fires until the operator enables it.
 *   • A power user is NEVER pointed at the operator's global account: the creator
 *     runs with isAdmin={false}, which drops the "Global account" option and makes
 *     a LIVE start REQUIRE the user's own ACTIVE connected account.
 *
 * Flow (no dead ends):
 *   STEP 1 — Connect your broker (BrokerAccountsPanel, the live per-user connect).
 *   STEP 2 — Create your campaign (PortfolioAutoTrade, scoped to this user).
 *            • ≥1 ACTIVE account → a prominent "Create your campaign ↓" CTA that
 *              scrolls to the creator.
 *            • no ACTIVE account → the creator area is framed with "Connect a
 *              broker account above to start a live session" (paper still works).
 *
 * The old standalone "choose style / set capital" steps were REMOVED — the actual
 * strategy + capital are chosen inside PortfolioAutoTrade's config form, so there
 * is exactly ONE capital input (no two competing forms). A compact read-only "what
 * Falcon does" strip keeps the framing.
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { CompassLogo } from '@/components/power/CompassLogo'
import { C, ICON, MechanismStrip } from '@/components/power/shared/cotrade-kit'
import { BrokerAccountsPanel } from '@/components/power/autotrade/BrokerAccountsPanel'
import { PortfolioAutoTrade } from '@/components/power/autotrade/PortfolioAutoTrade'
import { AutoTradeAPI, type BrokerAccount } from '@/lib/autotrade-api'

type Props = { firstName: string; userId: number | string }

export function AutoTradeExperience({ firstName, userId }: Props) {
  const router = useRouter()

  // The user's own broker accounts — fetched here so this shell can drive the
  // guided handoff (connect → create) without modifying BrokerAccountsPanel or
  // PortfolioAutoTrade. Refetched on demand ("I've connected — refresh") and on a
  // light poll while nothing is ACTIVE yet, so a fresh connection advances the
  // flow on its own. Best-effort: a failure leaves the flow usable in paper.
  const [accounts, setAccounts] = useState<BrokerAccount[]>([])
  const [accountsLoaded, setAccountsLoaded] = useState(false)
  const creatorRef = useRef<HTMLDivElement | null>(null)

  const loadAccounts = useCallback(async () => {
    try {
      const res = await AutoTradeAPI.brokerAccounts(userId)
      setAccounts(res.accounts ?? [])
    } catch {
      setAccounts([])
    } finally {
      setAccountsLoaded(true)
    }
  }, [userId])

  useEffect(() => { loadAccounts() }, [loadAccounts])

  const hasActiveAccount = accounts.some((a) => (a.status ?? '').toUpperCase() === 'ACTIVE')
  const hasAnyAccount = accounts.length > 0

  // While the user has no ACTIVE account yet, poll gently so a just-completed
  // connect (in the embedded panel) flips the CTA on without a manual refresh.
  useEffect(() => {
    if (hasActiveAccount) return
    const t = setInterval(loadAccounts, 6_000)
    return () => clearInterval(t)
  }, [hasActiveAccount, loadAccounts])

  const scrollToCreator = useCallback(() => {
    creatorRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }, [])

  return (
    <div className="relative flex flex-col min-h-screen md:min-h-0 md:h-full md:overflow-hidden"
         style={{ background: C.canvas, color: C.ink }}>
      <div className="flex-1 min-h-0 md:overflow-y-auto [scrollbar-width:thin]">
        <div className="mx-auto w-full max-w-[1120px] px-5 md:px-8 py-5 md:py-6 flex flex-col gap-4 md:gap-5">

          {/* heading */}
          <div className="flex flex-col items-center text-center gap-1.5">
            <CompassLogo size={28} />
            <h1 className="text-[21px] md:text-[24px] font-semibold tracking-[-0.02em]" style={{ color: C.ink }}>
              AutoTrade with Falcon, {firstName}
            </h1>
            <p className="text-[12.5px] md:text-[13px] leading-snug max-w-[600px]" style={{ color: C.muted }}>
              Falcon trades for you, <b style={{ color: C.ink }}>automatically</b>, on <b style={{ color: C.ink }}>your own
              broker account</b> — picks, entries, stops, trailing and exits, hands-off.
            </p>
            {/* honesty banner — connecting is safe; paper is the default */}
            <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2.5 py-1 mt-0.5"
                  style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.42)' }}>
              <span className="w-1.5 h-1.5 rounded-full" style={{ background: C.mint }} />
              Connecting places no order · sessions run in Paper by default
            </span>
          </div>

          {/* mechanism strip — AutoTrade variant (no "launching" dead-end) */}
          <MechanismStrip variant="autotrade" onBridge={() => router.push('/power/co-trading')} />

          {/* STEP 1 — connect your broker (REAL, per-user connect flow) */}
          <Step n={1} title="Connect your broker"
                caption="This connects your account only — it never places a real order here. Your API secret is sent once and stored encrypted; it's never shown again.">
            <div className="rounded-2xl border p-3 sm:p-4 mb-3 flex items-start gap-2.5"
                 style={{ borderColor: 'rgba(63,227,164,0.3)', background: 'rgba(63,227,164,0.05)' }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>{ICON.shield(16)}</span>
              <p className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                Falcon runs your campaign on <b style={{ color: C.ink }}>your own connected account</b> — never a shared one.
                Automated execution is <b style={{ color: C.mint }}>paper by default</b> and live trading stays gated
                server-side until enabled.
              </p>
            </div>
            <BrokerAccountsPanel userId={userId} />

            {/* Handoff CTA — appears once the user has a live (ACTIVE) account. */}
            <div className="mt-4 flex flex-wrap items-center gap-2.5">
              {hasActiveAccount ? (
                <button type="button" onClick={scrollToCreator}
                        className="inline-flex items-center gap-2 px-4 py-2.5 rounded-xl text-[13px] font-semibold transition-opacity hover:opacity-90"
                        style={{ color: '#06130c', background: C.mint }}>
                  {ICON.bolt(14)} Create your campaign ↓
                </button>
              ) : (
                <div className="inline-flex items-center gap-2 text-[11.5px]" style={{ color: C.muted }}>
                  <span className="shrink-0" style={{ color: C.amber }}>{ICON.info(13)}</span>
                  {hasAnyAccount
                    ? 'Finish connecting (log in to your broker) to enable a live campaign — Paper works meanwhile.'
                    : 'Connect an account above to run a live campaign on your own account — Paper works meanwhile.'}
                  <button type="button" onClick={loadAccounts}
                          className="underline underline-offset-2" style={{ color: C.mint }}>
                    Refresh
                  </button>
                </div>
              )}
            </div>
          </Step>

          {/* STEP 2 — create + start/schedule the real campaign, scoped to this user. */}
          <div ref={creatorRef}>
            <Step n={2} title="Create your campaign"
                  caption="Choose the strategy and your capital, then Start now or Schedule it. Live runs on your connected account; Paper is a safe simulation.">
              {/* When no ACTIVE account exists, frame the creator with a clear
                  prompt — a power user cannot start a LIVE session with no account
                  (the creator enforces this too; Paper is still allowed). */}
              {accountsLoaded && !hasActiveAccount && (
                <div className="rounded-2xl border p-3 sm:p-4 mb-3 flex items-start gap-2.5"
                     style={{ borderColor: 'rgba(230,180,80,0.42)', background: 'rgba(230,180,80,0.08)' }}>
                  <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(15)}</span>
                  <p className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                    <b style={{ color: C.ink }}>Connect a broker account above</b> to start a <b style={{ color: C.ink }}>live</b> session
                    on your own account. You can still create and run a <b style={{ color: C.mint }}>Paper</b> session now.
                  </p>
                </div>
              )}
              {/* The REAL creator — Sessions/Campaign flow, this user's own list,
                  scoped + isolated. isAdmin={false} drops the global account and
                  requires the user's own ACTIVE account for a live start. */}
              <PortfolioAutoTrade userId={userId} isAdmin={false} />
            </Step>
          </div>

        </div>
      </div>
    </div>
  )
}

function Step({ n, title, caption, children }: { n: number; title: string; caption?: string; children: React.ReactNode }) {
  return (
    <section>
      <div className="flex items-start gap-2 mb-2.5">
        <span className="grid place-items-center w-5 h-5 rounded-full text-[11px] font-mono font-semibold shrink-0 mt-0.5"
              style={{ background: C.mintDim, color: C.mint }}>{n}</span>
        <div className="min-w-0">
          <h2 className="text-[14px] font-semibold" style={{ color: C.ink }}>{title}</h2>
          {caption && <p className="text-[11px] leading-snug mt-0.5" style={{ color: C.faint }}>{caption}</p>}
        </div>
      </div>
      {children}
    </section>
  )
}
