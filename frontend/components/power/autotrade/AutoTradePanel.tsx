'use client'

/**
 * AutoTradePanel — the SINGLE unified operator AutoTrade surface.
 *
 * Everything AutoTrade lives here, sub-tabbed — there is NO separate legacy
 * panel and NO "Operator Console" launcher anymore. The five tabs:
 *
 *   1. Sessions  (HOME)  — the NEW multi-broker /api/autotrade/* system
 *                          (PortfolioAutoTrade): list your sessions → resume a
 *                          live session OR create a new one → status / kill.
 *                          The Falcon Positional Auto-Ladder ("monthly campaign")
 *                          is a STRATEGY option inside this tab's create form —
 *                          NOT a separate surface. Its running campaigns show as a
 *                          summary card atop the same sessions list; the campaign's
 *                          child baskets appear as ordinary tagged rows below.
 *   2. Pre-Market        — the LIVE legacy screen (app/falcon/premarket): stage /
 *                          confirm / 9:15 deploy queue. Reused verbatim.
 *   3. Positions         — the LIVE legacy screen (app/falcon/positions): held
 *                          positions, SL, adopt. Reused verbatim.
 *   4. Config            — the LIVE legacy screen (app/falcon/config): trail
 *                          config + engine playbook. Reused verbatim.
 *   5. Engine            — the LIVE legacy screens (app/falcon/admin = preflight /
 *                          jobs / Kite token / inbox) + an embedded link into the
 *                          manual Trade preview→smoke→place (app/falcon/trade).
 *
 * HOW THE LEGACY SCREENS WORK IN HERE: each legacy page is a plain default-export
 * client component that calls FalconAPI, which already routes through the
 * same-origin /api/falcon-proxy (operator token injected server-side). So they
 * function unchanged inside /power. We import the page components directly and
 * render them in a tab — no logic/behaviour change, and the /falcon/* routes are
 * left fully working as a fallback.
 *
 * SAFETY: UI only. No backend / execution code is touched; no order is placed by
 * this shell. Mint/F2 theme + viewport-lock (shrink-0 header + flex-1 min-h-0
 * scroll body) preserved. Honest paper/live labelling carried from the children.
 */
import { useState } from 'react'
import { C, ICON, Gear, MECHANISM_CSS } from '@/components/power/shared/cotrade-kit'
import { PortfolioAutoTrade } from '@/components/power/autotrade/PortfolioAutoTrade'
import { BrokerAccountsPanel } from '@/components/power/autotrade/BrokerAccountsPanel'
import { TradeJournalPanel } from '@/components/power/autotrade/TradeJournalPanel'

// Legacy operator screens — reused verbatim (default exports rendered as tabs).
import PremarketPage from '@/app/falcon/premarket/page'
import FalconPositionsPage from '@/app/falcon/positions/page'
import TrailConfigPage from '@/app/falcon/config/page'
import FalconAdminPage from '@/app/falcon/admin/page'
import FalconTradePage from '@/app/falcon/trade/page'

type Tab = 'sessions' | 'journal' | 'brokers' | 'trade' | 'premarket' | 'positions' | 'config' | 'engine'

const TABS: { id: Tab; label: string; icon: (n: number) => React.ReactNode }[] = [
  { id: 'sessions',  label: 'Sessions',       icon: ICON.bolt },
  { id: 'journal',   label: 'Journal',        icon: ICON.book },
  { id: 'brokers',   label: 'Broker accounts', icon: ICON.link },
  { id: 'trade',     label: 'Trade',          icon: ICON.arrow },
  { id: 'premarket', label: 'Pre-Market',     icon: ICON.clock },
  { id: 'positions', label: 'Positions',      icon: ICON.shield },
  { id: 'config',    label: 'Config',         icon: ICON.trend },
  { id: 'engine',    label: 'Engine',         icon: ICON.bot },
]

// Read the initial tab from ?attab= (deep links point at ?attab=sessions&…).
// Falls back to 'sessions'. Auto-Ladder is now a strategy INSIDE Sessions — it
// is no longer a tab of its own.
const VALID_TABS: Tab[] = ['sessions', 'journal', 'brokers', 'trade', 'premarket', 'positions', 'config', 'engine']
function initialTab(): Tab {
  if (typeof window === 'undefined') return 'sessions'
  const t = new URLSearchParams(window.location.search).get('attab')
  return (t && (VALID_TABS as string[]).includes(t)) ? (t as Tab) : 'sessions'
}

export function AutoTradePanel({ firstName, userId }: { firstName: string; userId: number | string }) {
  const [tab, setTab] = useState<Tab>(initialTab)
  // activeSessionId is lifted here so the Journal tab can receive it without
  // requiring PortfolioAutoTrade to be rewritten. PortfolioAutoTrade notifies
  // us via onSessionChange whenever the operator resumes or creates a session.
  const [activeSessionId, setActiveSessionId] = useState<string>('')

  return (
    <div className="h-full w-full flex flex-col" style={{ background: C.canvas, color: C.ink }}>
      <style>{MECHANISM_CSS}</style>

      {/* ── Header + framing + tabs (shrink-0) ───────────────────────────────── */}
      <div className="shrink-0 px-5 pt-7 pb-3 sm:px-8">
        <div className="mx-auto w-full max-w-5xl">
          <div className="flex items-start gap-3">
            <div className="flex items-center -space-x-1 shrink-0 mt-0.5">
              <Gear size={26} dir={1} />
              <Gear size={18} dir={-1} dim />
            </div>
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <h1 className="text-[20px] sm:text-[22px] font-semibold leading-tight" style={{ color: C.ink }}>
                  AutoTrade
                </h1>
                <span className="inline-flex items-center gap-1.5 text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2.5 py-1"
                  style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.42)' }}>
                  <span className="w-1.5 h-1.5 rounded-full" style={{ background: C.mint }} />
                  Operator
                </span>
              </div>
              <p className="text-[12.5px] leading-snug mt-1" style={{ color: C.muted }}>
                {firstName ? `${firstName}, everything` : 'Everything'} AutoTrade in one place.{' '}
                <b style={{ color: C.ink2 }}>Sessions run in PAPER by default</b>; live needs the
                server flag <code style={{ color: C.ink }}>FALCON_AUTOTRADE_ENABLED</code>.{' '}
                Pre-Market, Positions, Config &amp; Engine are your live Falcon operator controls.
              </p>
            </div>
          </div>

          {/* Tabs */}
          <div className="mt-5 inline-flex flex-wrap gap-0.5 rounded-xl border p-0.5"
            style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
            {TABS.map((t) => (
              <TabBtn key={t.id} active={tab === t.id} onClick={() => setTab(t.id)} icon={t.icon}>
                {t.label}
              </TabBtn>
            ))}
          </div>
        </div>
      </div>

      {/* ── Body (flex-1, scrolls) ───────────────────────────────────────────── */}
      <div className="flex-1 min-h-0 overflow-y-auto">
        {tab === 'sessions' && (
          <div className="mx-auto w-full max-w-4xl px-5 pb-10 sm:px-8">
            <PortfolioAutoTrade userId={userId} onSessionChange={setActiveSessionId} />
          </div>
        )}

        {tab === 'journal' && (
          <div className="mx-auto w-full max-w-4xl px-5 pb-10 sm:px-8 pt-4">
            {activeSessionId ? (
              <TradeJournalPanel sessionId={activeSessionId} />
            ) : (
              <div className="flex flex-col items-center justify-center gap-3 py-20 text-center">
                <span style={{ color: C.faint }}>{ICON.book(28)}</span>
                <p className="text-[13px]" style={{ color: C.muted }}>
                  Select a session to view its trade journal.
                </p>
                <p className="text-[11.5px]" style={{ color: C.faint }}>
                  Resume a session from the Sessions tab to see its journal here.
                </p>
              </div>
            )}
          </div>
        )}

        {tab === 'brokers' && (
          <div className="mx-auto w-full max-w-4xl px-5 pb-10 sm:px-8">
            <BrokerAccountsPanel userId={userId} />
          </div>
        )}

        {/* The legacy screens own their own internal layout/scroll; render them
            inside the scroll body unchanged. */}
        {tab === 'premarket' && <LegacyMount><PremarketPage /></LegacyMount>}
        {tab === 'positions' && <LegacyMount><FalconPositionsPage /></LegacyMount>}
        {tab === 'config'    && <LegacyMount><TrailConfigPage /></LegacyMount>}

        {/* Trade — the live manual entry window (preview → smoke → place), now its own tab. */}
        {tab === 'trade'     && <LegacyMount><FalconTradePage /></LegacyMount>}

        {tab === 'engine' && (
          <div className="mx-auto w-full max-w-5xl px-5 pb-10 sm:px-8">
            {/* Manual Trade (preview → smoke → place) is now its own top-level "Trade" tab. */}
            {/* Preflight / jobs / Kite token / inbox. */}
            <div className="mt-4">
              <LegacyMount><FalconAdminPage /></LegacyMount>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

/**
 * LegacyMount — wraps a reused legacy /falcon screen. The legacy pages were
 * authored as standalone routes (some assume a light/page-level background); we
 * give them a stable padded container so they sit cleanly inside the tabbed
 * body. No props/logic are passed in — behaviour is 1:1 with the route.
 */
function LegacyMount({ children }: { children: React.ReactNode }) {
  return <div className="legacy-mount">{children}</div>
}

function TabBtn({
  active, onClick, icon, children,
}: {
  active: boolean; onClick: () => void; icon: (n: number) => React.ReactNode; children: React.ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex items-center gap-2 px-4 py-2 rounded-lg text-[12.5px] font-semibold transition-colors"
      style={{
        color: active ? '#06130c' : C.ink2,
        background: active ? C.mint : 'transparent',
      }}
    >
      {icon(14)}
      {children}
    </button>
  )
}
