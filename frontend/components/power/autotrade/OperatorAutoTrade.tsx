'use client'

/**
 * OperatorAutoTrade — the OPERATOR AutoTrade surface, as a two-tab shell:
 *
 *   • "Portfolio Sessions" [NEW] — PortfolioAutoTrade: the LIVE multi-broker
 *     /api/autotrade/* console (create → start → status → kill), called via the
 *     same-origin Falcon proxy. Ships disabled (paper default, kill off, server
 *     flag for live).
 *   • "Operator Console" — the existing AutoTradeConsoleHub: pure-navigation
 *     launcher into the live Falcon console at /falcon/* (unchanged).
 *
 * The existing hub is NOT removed — it is kept as the second tab. Viewport-lock
 * preserved: shrink-0 header + flex-1 min-h-0 overflow-y-auto body. Mint/F2.
 */
import { useState } from 'react'
import { C, ICON, Gear, MECHANISM_CSS } from '@/components/power/shared/cotrade-kit'
import { AutoTradeConsoleHub } from '@/components/power/autotrade/AutoTradeConsoleHub'
import { PortfolioAutoTrade } from '@/components/power/autotrade/PortfolioAutoTrade'

type Tab = 'sessions' | 'console'

export function OperatorAutoTrade({ firstName }: { firstName: string }) {
  const [tab, setTab] = useState<Tab>('sessions')

  return (
    <div className="h-full w-full flex flex-col" style={{ background: C.canvas, color: C.ink }}>
      <style>{MECHANISM_CSS}</style>

      {/* ── Header + tabs (shrink-0) ─────────────────────────────────────────── */}
      <div className="shrink-0 px-5 pt-7 pb-3 sm:px-8">
        <div className="mx-auto w-full max-w-4xl">
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
                {firstName ? `${firstName}, the` : 'The'} multi-broker portfolio engine plus the live Falcon operator console.
              </p>
            </div>
          </div>

          {/* Tabs */}
          <div className="mt-5 inline-flex rounded-xl border p-0.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
            <TabBtn active={tab === 'sessions'} onClick={() => setTab('sessions')} icon={ICON.bolt}>
              Portfolio Sessions
            </TabBtn>
            <TabBtn active={tab === 'console'} onClick={() => setTab('console')} icon={ICON.bot}>
              Operator Console
            </TabBtn>
          </div>
        </div>
      </div>

      {/* ── Body (flex-1, scrolls) ───────────────────────────────────────────── */}
      <div className="flex-1 min-h-0 overflow-y-auto">
        {tab === 'sessions' ? (
          <div className="mx-auto w-full max-w-4xl px-5 pb-10 sm:px-8">
            <PortfolioAutoTrade />
          </div>
        ) : (
          <AutoTradeConsoleHub firstName={firstName} />
        )}
      </div>
    </div>
  )
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
