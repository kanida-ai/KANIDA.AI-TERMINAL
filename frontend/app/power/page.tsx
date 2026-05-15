/**
 * KANIDA.AI Power User Portal — landing page.
 *
 * SSR via async server component. Fetches featured replays + today's top-5
 * preview at request time. Cache hit on featured = ~1.8ms backend; total
 * first-paint budget < 500ms (operator-locked).
 *
 * Composition:
 *   <Hero>                — credibility statement + Random + Sign-in CTA
 *   <FeaturedReplays>     — 3 cards (showcase + 2 standards). Server-rendered.
 *   <TodaysTopFive>       — public preview, full PickCard.expanded × 5
 *   <ConversionCTA>       — "see all 100 +"
 */
import { Suspense } from 'react'
import Link from 'next/link'
import {
  PowerAPI,
  assertPickVersion,
  type FeaturedReplaySummary,
  type Pick,
} from '@/lib/power-api'
import { FeaturedReplayCard, FeaturedReplayCardSkeleton } from '@/components/power/FeaturedReplayCard'
import { PickCard } from '@/components/power/PickCard'
import { PickCardSkeleton } from '@/components/power/PickCardSkeleton'
import { RandomReplayButton } from '@/components/power/RandomReplayButton'

// Force dynamic — landing is personalised by date + cache state
export const dynamic = 'force-dynamic'
export const revalidate = 0

export default function LandingPage() {
  return (
    <div className="space-y-10 md:space-y-14">
      <Hero />
      <Suspense fallback={<FeaturedReplaysSkeleton />}>
        <FeaturedReplays />
      </Suspense>
      <Suspense fallback={<TodaysTopFiveSkeleton />}>
        <TodaysTopFive />
      </Suspense>
      <ConversionCTA />
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// HERO
// ─────────────────────────────────────────────────────────────────────────

function Hero() {
  return (
    <section className="text-center md:text-left">
      <h1 className="text-3xl md:text-5xl font-bold tracking-tight leading-tight">
        Know the <span className="text-amber-300">reason</span> before you buy.
      </h1>
      <p className="mt-3 md:mt-4 text-base md:text-lg text-neutral-400 max-w-2xl">
        Daily ranked picks from <span className="text-neutral-200 font-semibold">865 patterns</span>{' '}
        validated over 9 years of out-of-sample data. Each pick comes with a story,
        a hit rate, and what actually happened the last time the engine called this
        setup on this stock.
      </p>

      {/* Credibility line */}
      <div className="mt-4 md:mt-5 inline-flex flex-wrap items-center gap-x-3 gap-y-1.5 text-sm text-neutral-300">
        <span className="font-mono">
          ₹30 L → <span className="text-amber-300 font-semibold">₹1.05 Cr</span>
        </span>
        <span className="text-neutral-600">·</span>
        <span>3.3-year walk-forward</span>
        <span className="text-neutral-600">·</span>
        <span>4 of 4 years positive</span>
        <span className="text-neutral-600">·</span>
        <Link href="/power/credibility" className="text-amber-400 hover:text-amber-300 underline underline-offset-2">
          see the proof
        </Link>
      </div>

      <div className="mt-6 md:mt-7 flex flex-wrap items-center gap-3 justify-center md:justify-start">
        <RandomReplayButton />
        <span className="text-xs text-neutral-500">
          Pick any random day — see real outcomes. Anti-cherry-pick proof.
        </span>
      </div>
    </section>
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
// TODAY'S TOP 5 — public preview
// ─────────────────────────────────────────────────────────────────────────

async function TodaysTopFive() {
  let picks: Pick[] = []
  let signalDate: string | null = null
  let entryDate: string | null = null
  let totalAvailable = 0

  try {
    const r = await PowerAPI.todayPreview()
    picks = r.picks
    signalDate = r.signal_date
    entryDate = r.entry_date
    totalAvailable = r.total_available
    // Schema-version assertion on every fetched pick
    picks.forEach(assertPickVersion)
  } catch (e) {
    console.error('[landing] TodaysTopFive fetch failed:', e)
    return (
      <SectionShell
        title="Today's picks"
        subtitle="The 5 highest-conviction setups for tomorrow's open"
      >
        <p className="text-sm text-neutral-500">
          Today's picks temporarily unavailable. Refresh in a moment.
        </p>
      </SectionShell>
    )
  }

  if (picks.length === 0) {
    return (
      <SectionShell
        title="Today's picks"
        subtitle="The highest-conviction setups for tomorrow's open"
      >
        <p className="text-sm text-neutral-500">
          The engine hasn't emitted today's signals yet. Picks land 16:35 IST on every trading day.
        </p>
      </SectionShell>
    )
  }

  return (
    <SectionShell
      title="Today's top 5"
      subtitle={
        <>
          Highest-conviction picks emitted at <span className="font-mono text-neutral-300">{signalDate ?? '—'}</span>
          {entryDate && (
            <>
              {' '}for entry on <span className="font-mono text-neutral-300">{entryDate}</span>
            </>
          )}
          .
        </>
      }
    >
      <div className="space-y-4">
        {picks.map(p => <PickCard key={`${p.symbol}-${p.rank}`} pick={p} />)}
      </div>
      {totalAvailable > picks.length && (
        <p className="text-center text-sm text-neutral-500 mt-6">
          Showing top {picks.length} of {totalAvailable}.{' '}
          <Link href="/power/login" className="text-amber-400 hover:text-amber-300 underline">
            Sign in to see all {totalAvailable} →
          </Link>
        </p>
      )}
    </SectionShell>
  )
}

function TodaysTopFiveSkeleton() {
  return (
    <SectionShell title="Today's top 5" subtitle="The highest-conviction setups for tomorrow's open">
      <div className="space-y-4">
        {[0, 1, 2, 3, 4].map(i => <PickCardSkeleton key={i} />)}
      </div>
    </SectionShell>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// CONVERSION CTA — bottom of page
// ─────────────────────────────────────────────────────────────────────────

function ConversionCTA() {
  return (
    <section className="rounded-lg border border-amber-500/30 bg-gradient-to-br from-amber-500/5 to-transparent p-6 md:p-8 text-center">
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
          className="px-5 py-2.5 bg-amber-500 text-neutral-950 rounded-md font-semibold hover:bg-amber-400 transition-colors"
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
