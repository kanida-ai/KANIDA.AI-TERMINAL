'use client'

/**
 * AutoTradeConsoleHub — OPERATOR-ONLY launcher for the LIVE Falcon AutoTrade
 * operator console.
 *
 * The real, live console (Trade / Pre-Market / Positions / Config / Admin + the
 * /falcon home overview) already exists at app/falcon/* and is protected by the
 * site-wide HTTP Basic Auth (it lives OUTSIDE the /power invite-auth zone, by
 * design). We are NOT moving it. This hub is PURE NAVIGATION: a clean mint/F2
 * card grid of plain <a href> links into that console.
 *
 * SAFETY: this component fetches NOTHING. It calls no /api/falcon/* endpoint and
 * touches no execution path — links only. The targets open the operator console,
 * which is signed in separately (Basic Auth), hence plain anchors (not Next
 * <Link>) so the browser does a full navigation out of the /power (app) group.
 *
 * Reuses the cotrade-kit F2 palette (C) + icon set (ICON) so it reads as one
 * product family with Co-Trading / AutoTrade.
 */
import { C, ICON, Gear, MECHANISM_CSS } from '@/components/power/shared/cotrade-kit'

type ConsoleLink = {
  href:    string
  title:   string
  purpose: string
  Icon:    (n: number) => React.ReactNode
}

/** The 6 operator console destinations under app/falcon/* (Basic-Auth zone). */
const LINKS: ConsoleLink[] = [
  {
    href: '/falcon',
    title: 'Console overview',
    purpose: 'Token, signals, positions & pre-market at a glance.',
    Icon: ICON.bot,
  },
  {
    href: '/falcon/trade',
    title: 'Trade',
    purpose: 'Preview → smoke-test → place the day’s orders.',
    Icon: ICON.bolt,
  },
  {
    href: '/falcon/premarket',
    title: 'Pre-Market',
    purpose: 'Stage, confirm & queue the 9:15 deploy.',
    Icon: ICON.clock,
  },
  {
    href: '/falcon/positions',
    title: 'Positions',
    purpose: 'Held positions, stop-losses & adopt externals.',
    Icon: ICON.shield,
  },
  {
    href: '/falcon/config',
    title: 'Config',
    purpose: 'Trail config + the engine playbook knobs.',
    Icon: ICON.trend,
  },
  {
    href: '/falcon/admin',
    title: 'Admin',
    purpose: 'Preflight, jobs, Kite token & the inbox.',
    Icon: ICON.book,
  },
]

export function AutoTradeConsoleHub({ firstName }: { firstName: string }) {
  return (
    <div className="h-full w-full overflow-y-auto" style={{ background: C.canvas, color: C.ink }}>
      <style>{MECHANISM_CSS}</style>

      <div className="mx-auto w-full max-w-4xl px-5 py-8 sm:px-8 sm:py-10">
        {/* ── Header ─────────────────────────────────────────────────────── */}
        <div className="flex items-start gap-3">
          <div className="flex items-center -space-x-1 shrink-0 mt-0.5">
            <Gear size={26} dir={1} />
            <Gear size={18} dir={-1} dim />
          </div>
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <h1 className="text-[20px] sm:text-[22px] font-semibold leading-tight" style={{ color: C.ink }}>
                AutoTrade Console
              </h1>
              <span
                className="inline-flex items-center gap-1.5 text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2.5 py-1"
                style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.42)' }}
              >
                <span className="w-1.5 h-1.5 rounded-full" style={{ background: C.mint }} />
                Live · operator
              </span>
            </div>
            <p className="text-[12.5px] leading-snug mt-1" style={{ color: C.muted }}>
              {firstName ? `${firstName}, your` : 'Your'} live AutoTrade engine. These open the operator
              console (separately signed in).
            </p>
          </div>
        </div>

        {/* ── Card grid ──────────────────────────────────────────────────── */}
        <div className="mt-7 grid grid-cols-1 sm:grid-cols-2 gap-3">
          {LINKS.map((l) => (
            <a
              key={l.href}
              href={l.href}
              className="group flex flex-col rounded-2xl border px-4 py-4 transition-colors"
              style={{
                borderColor: C.line2,
                background: 'linear-gradient(180deg, rgba(63,227,164,0.04), rgba(255,255,255,0.012))',
              }}
            >
              <div className="flex items-center gap-2.5">
                <span
                  className="grid place-items-center w-8 h-8 rounded-xl shrink-0"
                  style={{ color: C.mint, background: 'rgba(63,227,164,0.10)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.30)' }}
                >
                  {l.Icon(16)}
                </span>
                <span className="text-[14px] font-semibold leading-tight" style={{ color: C.ink }}>
                  {l.title}
                </span>
              </div>
              <p className="text-[11.5px] leading-snug mt-2.5" style={{ color: C.muted }}>
                {l.purpose}
              </p>
              <span
                className="mt-3 inline-flex items-center gap-1.5 text-[11px] font-medium transition-transform group-hover:translate-x-0.5"
                style={{ color: C.mint }}
              >
                Open console
                <span>{ICON.arrow(12)}</span>
              </span>
            </a>
          ))}
        </div>

        {/* ── Honest footnote ────────────────────────────────────────────── */}
        <div
          className="mt-5 flex items-start gap-2 rounded-xl border px-3.5 py-3 text-[11px] leading-snug"
          style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)', color: C.muted }}
        >
          <span className="shrink-0 mt-0.5" style={{ color: C.faint }}>{ICON.info(14)}</span>
          <span>
            The operator console lives outside this portal and is protected by its own sign-in.
            Opening a card navigates there in this tab — these links place no orders and read no
            live data here.
          </span>
        </div>
      </div>
    </div>
  )
}
