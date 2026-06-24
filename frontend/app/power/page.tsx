/**
 * KANIDA.AI Power User Portal — landing page.
 *
 * Operator design change 2026-05-16 (Sprint 5c re-test): gate ALL picks
 * behind sign-in. The value of the daily call is daily — seeing today's
 * picks for free dilutes the offer.
 *
 * What stays PUBLIC:
 *   - Hero + credibility line + Random Replay button + "see the proof" link
 *   - 3 featured replays (the audited historical proof points)
 *   - General copy
 *
 * What now requires SIGN-IN:
 *   - Today's top N picks (every rank, no preview)
 *   - Live decisions
 *   - Custom-date replay (featured + random remain public)
 *   - Watchlist
 *
 * Composition:
 *   <Hero>             — credibility + Random + "see the proof"
 *   <TodaysGateCard>   — single sign-in CTA where the public top-5 used to be
 *   <FeaturedReplays>  — 3 audited replays. Public.
 *   <ConversionCTA>    — bottom CTA + waitlist
 */
import { Suspense } from 'react'
import { redirect } from 'next/navigation'
import Link from 'next/link'
import { getCurrentUser } from '@/lib/power-auth'
import {
  PowerAPI,
  type FeaturedReplaySummary,
} from '@/lib/power-api'
import { FeaturedReplayCard, FeaturedReplayCardSkeleton } from '@/components/power/FeaturedReplayCard'
import { RandomReplayButton } from '@/components/power/RandomReplayButton'
import { SelectDateButton } from '@/components/power/SelectDateButton'
// CompassPersonaOrbit deliberately not imported — v3 (2026-05-17) dropped the
// compass + 5-card orbit from the hero. The 5 persona cards already live on
// /power/portfolios (the page the primary CTA links to); duplicating them was
// confusing. The component file itself is kept in components/power/ so the
// operator can revive it as a smaller decorative element on the co-trading
// page later. If you reinstate it here, also bring back the HeroOrbit wrapper.
import { RevealOnScroll } from '@/components/power/RevealOnScroll'

// Force dynamic — landing is personalised by date + cache state
export const dynamic = 'force-dynamic'
export const revalidate = 0

const IST = 'Asia/Kolkata'

export default async function LandingPage() {
  // Signed-in users go straight to the new AI-native shell — /power IS the
  // portal. Only logged-out visitors see the public marketing landing below.
  const user = await getCurrentUser()
  if (user) redirect('/power/ask')

  return (
    <div className="space-y-10 md:space-y-14">
      <Hero />
      <TodaysGateCard />
      <Suspense fallback={<FeaturedReplaysSkeleton />}>
        <FeaturedReplays />
      </Suspense>
      <ConversionCTA />
    </div>
  )
}

// ═════════════════════════════════════════════════════════════════════════
// HERO v3 (Sprint 5d, 2026-05-17)
//
// Two changes vs v2 — everything else stays.
//   • Drop the compass + 5 orbiting persona cards (already on /power/portfolios)
//   • Swap accent from gold #F5A524 → mint #3FE3A4 (handled at the Tailwind
//     theme level: see globals.css `--color-mint-*`)
//
// Plus: small mint underline marks (80×3px, rounded) under two highlighted
// phrases — "AI co-trading partner" and "structured clarity in under 60
// seconds" — for that "crafted brand" feel from the operator's reference.
//
// Composition (top → bottom):
//   1. Brand statement   — H1 128/64, H2 56/32, credibility 24/18, brand intro 20/16
//   2. Pain → Promise    — pain 28/20, solution 40/28
//   3. Transition        — 20/16 italic between two horizontal rules
//   4. Three pillars     — 14/12 mint caps header, then 3 cards (40px padding)
//   5. Brand close       — intro 20/16, finale "You're not trading…" 64/36 BOLD
//   6. Primary CTA       — 64px tall solid-mint button, "→ KANIDA.AI CO-TRADING"
//   7. Legacy CTAs       — unchanged: Random Replay + Select Date + nav links
//
// Spacing — same v2 table EXCEPT the orbit row is gone, so we collapse
// "intro → orbit (96/56) + orbit → pain (128/72)" into a single
// "intro → pain (128/72)" gap that keeps the visual rhythm.
//
// Accent (mint-400 #3FE3A4) used ONLY on:
//   • "AI co-trading partner" (H2 phrase + underline mark)
//   • "500+ AI agents. One per stock." (credibility line)
//   • "structured clarity in under 60 seconds" (solution phrase + underline)
//   • 3-pillar section header + card headers
//   • Primary CTA fill (bg-mint-400 → hover bg-mint-300)
//
// EVERYTHING BELOW the legacy-CTA divider stays untouched.
// ═════════════════════════════════════════════════════════════════════════

function Hero() {
  return (
    <section className="pt-12 md:pt-24">
      <HeroBrandStatement />
      <HeroPainPromise />
      <HeroTransitionLine />
      <HeroThreePillars />
      <HeroBrandClose />
      <HeroPrimaryCTA />
      <HeroLegacyCTAs />
    </section>
  )
}


/** Small mint underline mark, 80×3px, sits 8px below the highlighted phrase
 *  it wraps. Positioned absolutely so it doesn't affect inline text flow.
 *  Drop on very narrow viewports — hidden under `sm:` to keep mobile clean.
 *  Operator-locked detail (v3 spec). */
function AccentUnderline() {
  return (
    <span
      aria-hidden
      className="hidden sm:block absolute left-0 w-[80px] h-[3px] bg-mint-400 rounded-[2px]"
      style={{ bottom: '-11px' }}
    />
  )
}


// ─────────────────────────────────────────────────────────────────────────
// 1. Brand statement — H1 + H2 + credibility tagline + brand intro
// All centered. Hero block max-w-[1280px] (governed by layout).
// ─────────────────────────────────────────────────────────────────────────
function HeroBrandStatement() {
  return (
    <RevealOnScroll className="text-center">
      {/* Brand-mark rule: `KANIDA` neutral (white on dark), `.AI` mint accent.
          Mirrors the TopBar wordmark in layout.tsx so the brand reads the
          same wherever it appears as a wordmark display. (CTA button labels
          are different — dark text on mint bg, treated as a single label.) */}
      <h1
        className="font-extrabold leading-[1.0] text-[64px] md:text-[128px]"
        style={{ letterSpacing: '-2px' }}
      >
        <span className="text-white">KANIDA</span><span className="text-mint-400">.AI</span>
      </h1>

      <h2 className="mt-4 md:mt-6 font-bold text-white text-[32px] md:text-[56px] leading-[1.1]">
        Your{' '}
        <span className="relative inline-block text-mint-400">
          AI co-trading partner
          <AccentUnderline />
        </span>
        .
      </h2>

      <p
        className="mt-5 md:mt-8 font-semibold text-mint-400 text-[18px] md:text-[24px]"
        style={{ letterSpacing: '1px' }}
      >
        500+ AI agents. One per stock.
      </p>

      <p className="mt-4 md:mt-6 text-white/65 text-[16px] md:text-[20px] leading-relaxed max-w-[640px] mx-auto">
        The next-generation AI co-trading system, built for retail stock traders.
      </p>
    </RevealOnScroll>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// 2. Pain → Promise — the emotional contrast moment
//
// v3 (2026-05-17): top margin increased from mt-18 to mt-24/mt-40 to fill the
// vertical gap left by the removed compass orbit section, keeping the page
// rhythm balanced. Operator spec didn't change the spacing rules — we just
// fold the (intro → orbit) + (orbit → pain) gaps into one.
// ─────────────────────────────────────────────────────────────────────────
function HeroPainPromise() {
  return (
    <RevealOnScroll
      delayMs={100}
      className="mt-24 md:mt-40 max-w-[880px] mx-auto text-center"
    >
      <p className="font-medium text-white/75 text-[20px] md:text-[28px] leading-snug">
        Most traders scan 4 timeframes, test endless strategies, guess probabilities —
        and still don&apos;t know what to trust.
      </p>
      <p className="mt-10 md:mt-16 font-bold text-white text-[28px] md:text-[40px] leading-tight">
        KANIDA turns that into{' '}
        <span className="relative inline-block text-mint-400">
          structured clarity in under 60 seconds
          <AccentUnderline />
        </span>
        .
      </p>
    </RevealOnScroll>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// 4. Transition line — italic, flanked by horizontal rules
// ─────────────────────────────────────────────────────────────────────────
function HeroTransitionLine() {
  return (
    <RevealOnScroll delayMs={100} className="mt-14 md:mt-24">
      <div className="flex items-center gap-4 md:gap-8 max-w-[720px] mx-auto">
        <span aria-hidden className="flex-1 h-px bg-white/15" />
        <p className="font-medium italic text-white/50 text-[16px] md:text-[20px] whitespace-nowrap">
          Hours of work. One clear call.
        </p>
        <span aria-hidden className="flex-1 h-px bg-white/15" />
      </div>
    </RevealOnScroll>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// 5. Three pillars — "EVERY PICK ANSWERS 3 THINGS" + 3 cards
// ─────────────────────────────────────────────────────────────────────────
function HeroThreePillars() {
  return (
    <RevealOnScroll delayMs={100} className="mt-10 md:mt-16">
      <h2
        className="text-center font-bold text-mint-400 uppercase text-[12px] md:text-[14px]"
        style={{ letterSpacing: '2px' }}
      >
        Every pick answers 3 things
      </h2>
      <div className="mt-6 md:mt-8 grid grid-cols-1 md:grid-cols-3 gap-6 max-w-[1200px] mx-auto">
        <PillarCard
          header="Why this stock?"
          body="The setup matched a pattern that worked historically."
          icon={<TargetIcon />}
        />
        <PillarCard
          header="What happened before?"
          body="KANIDA checks similar past setups using years of stock-specific data."
          icon={<HistoryIcon />}
        />
        <PillarCard
          header="Why now?"
          body="The stock is showing the same behaviour again today."
          icon={<ClockIcon />}
        />
      </div>
    </RevealOnScroll>
  )
}


function PillarCard({ header, body, icon }: {
  header: string; body: string; icon: React.ReactNode
}) {
  return (
    <div
      className="group relative bg-neutral-900 border border-neutral-800 rounded-2xl
                 p-6 md:p-10
                 hover:border-mint-500/40
                 hover:shadow-[0_0_32px_-6px_rgba(63, 227, 164,0.28)]
                 transition-all duration-300"
    >
      <div className="text-mint-400 mb-5 md:mb-6">{icon}</div>
      <h3
        className="font-bold uppercase text-mint-400 text-[16px] md:text-[18px] mb-3 md:mb-4"
        style={{ letterSpacing: '1px' }}
      >
        {header}
      </h3>
      <p className="text-white/85 text-[16px] md:text-[18px] leading-relaxed font-normal">
        {body}
      </p>
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// 6. Brand close — emotional landing
// ─────────────────────────────────────────────────────────────────────────
function HeroBrandClose() {
  return (
    <RevealOnScroll delayMs={100} className="mt-18 md:mt-32 text-center">
      <p className="text-white/70 text-[16px] md:text-[20px] leading-relaxed max-w-[640px] mx-auto">
        No generic strategy. No blind tips.<br />
        Just stock-specific intelligence — before you trade.
      </p>
      <p
        className="mt-16 md:mt-24 font-bold text-white text-[36px] md:text-[64px] leading-[1.05]"
        style={{ letterSpacing: '-1px' }}
      >
        You&apos;re not trading alone anymore.
      </p>
    </RevealOnScroll>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// 7. Primary CTA — 64px tall solid-gold button
// ─────────────────────────────────────────────────────────────────────────
function HeroPrimaryCTA() {
  return (
    <RevealOnScroll delayMs={150} className="mt-10 md:mt-16 flex justify-center">
      <Link
        href="/power/portfolios"
        className="group inline-flex items-center justify-center gap-3
                   bg-mint-500 text-[#0A0A0A] rounded-xl font-bold uppercase
                   h-[56px] md:h-[64px]
                   px-8 md:px-12
                   text-[18px] md:text-[20px]
                   tracking-wide
                   hover:bg-mint-400 hover:-translate-y-0.5
                   hover:shadow-[0_8px_32px_rgba(63, 227, 164,0.40)]
                   transition-all duration-150 ease-out"
        style={{ letterSpacing: '0.04em' }}
      >
        <span aria-hidden className="text-[22px] md:text-[24px] transition-transform duration-150 group-hover:translate-x-0.5">→</span>
        KANIDA.AI Co-Trading
      </Link>
    </RevealOnScroll>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// 8. Legacy CTAs — Random Replay + Select Date + nav links.
// UNCHANGED from earlier ship. Operator: "Everything below 'Show me a
// random day' button stays exactly as it is on the live page."
// ─────────────────────────────────────────────────────────────────────────
function HeroLegacyCTAs() {
  return (
    <div className="mt-16 md:mt-24 pt-8 md:pt-10 border-t border-neutral-900 space-y-6">
      <div className="flex flex-wrap items-start gap-4 md:gap-6">
        <div className="space-y-1.5">
          <RandomReplayButton />
          <span className="block text-xs text-neutral-500 max-w-[18rem]">
            Pick any random day — see real outcomes. Anti-cherry-pick proof.
          </span>
        </div>
        <div className="space-y-1.5">
          <SelectDateButton />
          <span className="block text-xs text-neutral-500 max-w-[18rem]">
            Or audit a specific date — any trading day from 2021 onward.
          </span>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-neutral-500">
        <Link href="/power/portfolios" className="text-mint-400 hover:text-mint-300 underline">
          Live AI Portfolios →
        </Link>
        <span className="text-neutral-700">·</span>
        <Link href="/power/sizing" className="text-mint-400 hover:text-mint-300 underline">
          Position sizing assistant
        </Link>
        <span className="text-neutral-700">·</span>
        <Link href="/power/credibility" className="text-mint-400 hover:text-mint-300 underline">
          See the proof
        </Link>
      </div>
    </div>
  )
}


// ── Pillar-card icons — 32px gold line-icon style (operator spec) ────────

function TargetIcon() {
  return (
    <svg width="32" height="32" viewBox="0 0 32 32" fill="none" aria-hidden>
      <circle cx="16" cy="16" r="12.5" stroke="currentColor" strokeWidth="1.6" />
      <circle cx="16" cy="16" r="7.5"  stroke="currentColor" strokeWidth="1.6" />
      <circle cx="16" cy="16" r="2.2"  fill="currentColor" />
    </svg>
  )
}

function HistoryIcon() {
  return (
    <svg width="32" height="32" viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M4 16a12 12 0 1 0 4-9"  stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      <path d="M4 6v6h6"               stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M16 9v7l4.5 3"          stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function ClockIcon() {
  return (
    <svg width="32" height="32" viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M16 4l3.5 6h-7L16 4z"           fill="currentColor" />
      <path d="M16 14v8m0 0l-4-3m4 3l4-3"      stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="16" cy="22" r="6"            stroke="currentColor" strokeWidth="1.6" />
    </svg>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// FEATURED REPLAYS — async, fetched at request time
// ─────────────────────────────────────────────────────────────────────────

async function FeaturedReplays() {
  let featured: FeaturedReplaySummary[] = []
  try {
    const r = await PowerAPI.featuredReplays()
    featured = r.featured
  } catch (e) {
    console.error('[landing] FeaturedReplays fetch failed:', e)
    return (
      <SectionShell
        title="Featured replays"
        subtitle="Verified outcomes from real historical dates"
      >
        <p className="text-sm text-neutral-500">
          Featured replays temporarily unavailable. Refresh in a moment.
        </p>
      </SectionShell>
    )
  }

  return (
    <SectionShell
      title="Three days the engine called"
      subtitle="Hand-picked from history — verified outcomes, anti-cherry-pick proof one click away above."
    >
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {featured.map((f, idx) => (
          <FeaturedReplayCard
            key={f.replay_date}
            replay={f}
            emphasis={idx === 0 ? 'showcase' : 'standard'}
          />
        ))}
      </div>
    </SectionShell>
  )
}

function FeaturedReplaysSkeleton() {
  return (
    <SectionShell
      title="Three days the engine called"
      subtitle="Hand-picked from history — verified outcomes, anti-cherry-pick proof one click away above."
    >
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[0, 1, 2].map(i => <FeaturedReplayCardSkeleton key={i} />)}
      </div>
    </SectionShell>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// TODAY'S GATE CARD — replaces the old public top-5 preview
// (Sprint 5c re-test design change 3)
//
// We deliberately do NOT call /api/power/picks/today/preview here. The
// preview endpoint still exists (operator may flip the gate back), but the
// landing page no longer reads from it. Signal date below is computed from
// "next trading day in IST" so the CTA stays accurate without a backend hit.
// ─────────────────────────────────────────────────────────────────────────

function TodaysGateCard() {
  // Friendly IST date for the CTA — picks land for "tomorrow's open"
  const tomorrow = nextWeekdayIST()
  const dateStr = tomorrow.toLocaleDateString('en-IN', {
    timeZone: IST, weekday: 'long', day: 'numeric', month: 'long',
  })

  return (
    <section
      className="rounded-xl border border-mint-500/30 bg-gradient-to-br from-mint-500/[0.06] via-neutral-900 to-neutral-900 p-6 md:p-8"
      aria-label="Today's picks — sign-in required"
    >
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-5">
        <div className="space-y-2">
          <p className="text-[11px] tracking-[0.2em] uppercase text-mint-300 font-semibold">
            Today&apos;s picks · sign-in required
          </p>
          <h2 className="text-xl md:text-2xl font-bold text-neutral-100 leading-tight">
            See today&apos;s top 14 picks for{' '}
            <span className="text-mint-300 whitespace-nowrap">{dateStr}</span>
          </h2>
          <p className="text-sm text-neutral-400 max-w-xl">
            Each pick comes with its full story, the 2-year hit rate of the pattern,
            and what happened the last time the engine made this call on this stock.
            Beta access by invite — Google sign-in.
          </p>
        </div>
        <div className="flex flex-col items-stretch md:items-end gap-2 shrink-0">
          <Link
            href="/power/login"
            className="px-5 py-2.5 bg-mint-500 text-neutral-950 rounded-md font-semibold text-center
                        hover:bg-mint-400 transition-colors whitespace-nowrap"
          >
            Sign in with Google →
          </Link>
          <Link
            href="/power/waitlist"
            className="text-xs text-neutral-400 underline hover:text-neutral-200 text-center md:text-right"
          >
            No invite? Join the waitlist
          </Link>
        </div>
      </div>
    </section>
  )
}


/** Next NSE-trading weekday (Mon-Fri) in IST. Used purely for the CTA label. */
function nextWeekdayIST(): Date {
  const now = new Date()
  // Compute IST day-of-week without converting to a "wall clock" string
  const istDow = Number(
    new Intl.DateTimeFormat('en-US', { timeZone: IST, weekday: 'short' })
      .format(now)
      .replace(/Mon|Tue|Wed|Thu|Fri|Sat|Sun/, (m) =>
        ({ Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 } as Record<string, number>)[m].toString()
      )
  )
  // Skip to next weekday: today+1, but if that lands on Sat/Sun skip ahead
  let addDays = 1
  let dow = (istDow + addDays) % 7
  while (dow === 6 || dow === 0) {
    addDays += 1
    dow = (istDow + addDays) % 7
  }
  const out = new Date(now.getTime() + addDays * 24 * 60 * 60 * 1000)
  return out
}

// ─────────────────────────────────────────────────────────────────────────
// CONVERSION CTA — bottom of page
// ─────────────────────────────────────────────────────────────────────────

function ConversionCTA() {
  return (
    <section className="rounded-lg border border-mint-500/30 bg-gradient-to-br from-mint-500/5 to-transparent p-6 md:p-8 text-center">
      <h2 className="text-xl md:text-2xl font-bold">
        Want all 100 picks every day, with custom backtest?
      </h2>
      <p className="mt-2 text-sm md:text-base text-neutral-400 max-w-2xl mx-auto">
        Power User beta — invite only. 12-18 traders, no public sign-up.{' '}
        Already have a code?
      </p>
      <div className="mt-5 flex flex-wrap items-center justify-center gap-3">
        <Link
          href="/power/login"
          className="px-5 py-2.5 bg-mint-400 text-neutral-950 rounded-md font-semibold hover:bg-mint-300 transition-colors"
        >
          Sign in with Google
        </Link>
        <Link
          href="/power/waitlist"
          className="px-5 py-2.5 border border-neutral-700 text-neutral-300 rounded-md hover:bg-neutral-900 hover:text-neutral-100 transition-colors"
        >
          Join the waitlist
        </Link>
      </div>
    </section>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// Section primitive
// ─────────────────────────────────────────────────────────────────────────

function SectionShell({ title, subtitle, children }: {
  title: React.ReactNode; subtitle: React.ReactNode; children: React.ReactNode
}) {
  return (
    <section>
      <header className="mb-4 md:mb-6">
        <h2 className="text-xl md:text-2xl font-bold text-neutral-100">{title}</h2>
        <p className="mt-1 text-sm text-neutral-400">{subtitle}</p>
      </header>
      {children}
    </section>
  )
}
