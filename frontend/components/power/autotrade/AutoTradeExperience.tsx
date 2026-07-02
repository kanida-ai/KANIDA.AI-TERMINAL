'use client'

/**
 * AutoTradeExperience — the Kanida.AI AutoTrade page for power users.
 *
 * SCOPE (money is involved — strict honesty):
 *   • The page lets a user CONNECT their real broker (the live, per-user
 *     broker-connect panel). Connecting places NO order.
 *   • Automated hands-off execution on the user's OWN account is still gated
 *     server-side (paper by default, FALCON_AUTOTRADE_ENABLED) — the honesty
 *     banner says so. This page never places or simulates a real order.
 *
 * Flow: choose style → set capital → connect broker. The page ends at the
 * connected-accounts list (the old readiness / waitlist / preview scaffolding
 * was removed to keep the page coherent now that broker connect is live).
 */
import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { CompassLogo } from '@/components/power/CompassLogo'
import { C, ICON, MechanismStrip, fmtINR, fmtCapital } from '@/components/power/shared/cotrade-kit'
import { BrokerAccountsPanel } from '@/components/power/autotrade/BrokerAccountsPanel'

// ── Fixed sizing model (REAL math, no fabrication) — informational on this page. ─
const BASE_CAPITAL = 500_000
const BASE_PER_TRADE = 50_000
const CAPITAL_PRESETS = [100_000, 500_000, 1_000_000]

// ── Trading styles — only Swing is LIVE as an engine (rest Launch-Pending) ───
type StyleId = 'swing' | 'btst' | 'intraday' | 'weekly' | 'longterm'
type Style = { id: StyleId; name: string; hold: string; live: boolean }
const STYLES: Style[] = [
  { id: 'swing',    name: 'Falcon Top 10 Swing', hold: '~7 trading days', live: true  },
  { id: 'btst',     name: 'BTST',                hold: '1–2 days',        live: false },
  { id: 'intraday', name: 'Intraday',           hold: 'Same day',        live: false },
  { id: 'weekly',   name: 'Weekly Swing',       hold: '1–4 weeks',       live: false },
  { id: 'longterm', name: 'Long-Term',          hold: '3+ months',       live: false },
]

type Props = { firstName: string; userId: number | string }

export function AutoTradeExperience({ firstName, userId }: Props) {
  const router = useRouter()
  const [styleId, setStyleId] = useState<StyleId>('swing')
  const [capital, setCapital] = useState<number>(500_000)
  const perTrade = Math.max(0, (capital / BASE_CAPITAL) * BASE_PER_TRADE)

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
              Falcon trades for you, <b style={{ color: C.ink }}>automatically</b>, with your real broker — picks,
              entries, stops, trailing and exits, hands-off.
            </p>
            {/* honesty banner — connecting is live; auto-execution is still gated */}
            <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2.5 py-1 mt-0.5"
                  style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.42)' }}>
              <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: C.amber }} />
              Connecting places no order — automated trading on your account isn’t live yet
            </span>
          </div>

          {/* mechanism strip — AutoTrade variant + launch badge */}
          <MechanismStrip variant="autotrade" launching onBridge={() => router.push('/power/co-trading')} />

          {/* STEP 1 — choose style */}
          <Step n={1} title="Choose your trading style">
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2.5">
              {STYLES.map(s => {
                const active = s.id === styleId
                return (
                  <button key={s.id} type="button" disabled={!s.live}
                          onClick={() => { if (s.live) setStyleId(s.id) }}
                          className="relative text-left rounded-2xl border p-3 transition-colors disabled:cursor-not-allowed"
                          style={{
                            borderColor: active ? 'rgba(63,227,164,0.55)' : C.line2,
                            background: active ? 'rgba(63,227,164,0.07)' : 'rgba(255,255,255,0.02)',
                            opacity: s.live ? 1 : 0.6,
                          }}>
                    <div className="flex items-center gap-2 mb-1.5">
                      <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0" style={{ background: C.mintDim, color: C.mint }}>{ICON.flame(15)}</span>
                      {s.live
                        ? (active && <span className="ml-auto w-2 h-2 rounded-full shrink-0" style={{ background: C.mint, boxShadow: `0 0 8px ${C.mint}` }} />)
                        : <span className="ml-auto text-[8.5px] font-mono uppercase tracking-[0.08em] shrink-0" style={{ color: C.faint }}>soon</span>}
                    </div>
                    <div className="text-[12.5px] font-semibold leading-tight" style={{ color: s.live ? C.ink : C.faint }}>{s.name}</div>
                    <div className="text-[10px] mt-0.5" style={{ color: C.faint }}>{s.hold}</div>
                  </button>
                )
              })}
            </div>
            <p className="text-[10.5px] mt-2" style={{ color: C.faint }}>
              Only <b style={{ color: C.muted }}>Falcon Top 10 Swing</b> is live today — the other engines are Launch-Pending.
            </p>
          </Step>

          {/* STEP 2 — capital */}
          <Step n={2} title="Set your capital">
            <div className="max-w-[520px]">
              <div className="flex items-center gap-2 rounded-xl px-3 py-2 border"
                   style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.03)' }}>
                <span className="text-[16px] font-semibold" style={{ color: C.muted }}>₹</span>
                <input type="number" min={10_000} step={10_000} value={capital}
                       onChange={e => setCapital(Math.max(0, Number(e.target.value) || 0))}
                       className="w-full bg-transparent outline-none text-[17px] font-mono font-semibold tabular-nums" style={{ color: C.ink }} />
                <span className="text-[11px] font-mono shrink-0" style={{ color: C.faint }}>{fmtCapital(capital)}</span>
              </div>
              <div className="flex items-center gap-1.5 mt-2 flex-wrap">
                {CAPITAL_PRESETS.map(v => (
                  <button key={v} type="button" onClick={() => setCapital(v)}
                          className="text-[12px] rounded-full px-3 py-1 border transition-colors"
                          style={capital === v
                            ? { borderColor: 'rgba(63,227,164,0.5)', color: C.mint, background: 'rgba(63,227,164,0.08)' }
                            : { borderColor: C.line2, color: C.muted }}>{fmtCapital(v)}</button>
                ))}
              </div>
              <p className="text-[10.5px] mt-2" style={{ color: C.faint }}>
                Falcon would size <b style={{ color: C.muted }}>{fmtINR(perTrade)}</b> per pick (₹50k at ₹5 L, scaled). When live,
                this uses your real broker margin/cash.
              </p>
            </div>
          </Step>

          {/* STEP 3 — connect your broker (REAL, per-user connect flow) */}
          <Step n={3} title="Connect your broker">
            <div className="rounded-2xl border p-3 sm:p-4 mb-3 flex items-start gap-2.5"
                 style={{ borderColor: 'rgba(63,227,164,0.3)', background: 'rgba(63,227,164,0.05)' }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>{ICON.shield(16)}</span>
              <p className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                This connects your broker account only — it <b style={{ color: C.ink }}>never places a real order here</b>.
                Automated execution is <b style={{ color: C.mint }}>paper by default</b> and live trading stays gated
                server-side. Your API secret is sent once and stored encrypted; it&apos;s never shown again.
              </p>
            </div>
            <BrokerAccountsPanel userId={userId} />
          </Step>

        </div>
      </div>
    </div>
  )
}

function Step({ n, title, children }: { n: number; title: string; children: React.ReactNode }) {
  return (
    <section>
      <div className="flex items-center gap-2 mb-2.5">
        <span className="grid place-items-center w-5 h-5 rounded-full text-[11px] font-mono font-semibold shrink-0"
              style={{ background: C.mintDim, color: C.mint }}>{n}</span>
        <h2 className="text-[14px] font-semibold" style={{ color: C.ink }}>{title}</h2>
      </div>
      {children}
    </section>
  )
}
