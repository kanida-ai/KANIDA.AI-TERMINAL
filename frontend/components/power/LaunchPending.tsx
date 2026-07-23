'use client'

/**
 * LaunchPending — premium placeholder for a mode/feature that is built in the
 * IA but not yet wired to a real engine. Honesty is the product's credibility:
 * every not-ready nav item renders this instead of a 404 or a faked screen.
 *
 * Four honest sections:
 *   • title + one-line summary  — what the mode is
 *   • "What it will do"         — concrete capabilities (bullets)
 *   • "What to expect"          — the shape of the output when live
 *   • "Why not yet"             — the honest reason it isn't live today
 * plus a "Notify me" CTA (stub — POSTs nowhere yet; logs intent locally).
 *
 * Only the Falcon Top 10 Swing (+ Weekly variant) engine is LIVE. This
 * component is how BTST / Intraday / Long-Term / per-user AutoTrade / etc.
 * stay visible and complete without lying.
 */
import { useState } from 'react'
import { IconBell } from './shell/icons'

type Props = {
  /** e.g. "AutoTrade" */
  title:       string
  /** one-line plain-English summary */
  summary:     string
  /** concrete capabilities this mode will offer */
  willDo:      string[]
  /** what the live output will look like */
  expect:      string[]
  /** the honest reason it isn't live yet */
  whyNotYet:   string
  /** label for the badge (default "Launch Pending") */
  badge?:      string
}

export function LaunchPending({
  title, summary, willDo, expect, whyNotYet, badge = 'Launch Pending',
}: Props) {
  const [notified, setNotified] = useState(false)

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="space-y-3">
        <span className="inline-flex items-center gap-1.5 text-[11px] font-mono uppercase tracking-wider
                         text-mint-300 bg-mint-400/10 border border-mint-400/30 rounded-full px-2.5 py-1">
          <span className="w-1.5 h-1.5 rounded-full bg-mint-400 animate-pulse" aria-hidden />
          {badge}
        </span>
        <h1 className="text-2xl md:text-3xl font-semibold text-neutral-100">{title}</h1>
        <p className="text-neutral-400 max-w-2xl leading-relaxed">{summary}</p>
      </div>

      {/* Two-column: will do / expect */}
      <div className="grid md:grid-cols-2 gap-4">
        <Panel heading="What it will do">
          <ul className="space-y-2.5">
            {willDo.map((t, i) => (
              <li key={i} className="flex gap-2.5 text-sm text-neutral-300">
                <Dot /> <span>{t}</span>
              </li>
            ))}
          </ul>
        </Panel>
        <Panel heading="What to expect">
          <ul className="space-y-2.5">
            {expect.map((t, i) => (
              <li key={i} className="flex gap-2.5 text-sm text-neutral-300">
                <Dot /> <span>{t}</span>
              </li>
            ))}
          </ul>
        </Panel>
      </div>

      {/* Why not yet — the honest bit */}
      <Panel heading="Why it isn't live yet">
        <p className="text-sm text-neutral-400 leading-relaxed">{whyNotYet}</p>
      </Panel>

      {/* Notify CTA */}
      <div className="flex flex-col sm:flex-row sm:items-center gap-3 pt-1">
        <button
          type="button"
          disabled={notified}
          onClick={() => setNotified(true)}
          className={[
            'inline-flex items-center justify-center gap-2 rounded-lg px-4 py-2.5 text-sm font-semibold transition-colors',
            notified
              ? 'bg-mint-400/10 text-mint-300 border border-mint-400/30 cursor-default'
              : 'bg-mint-400 text-neutral-950 hover:bg-mint-300',
          ].join(' ')}
        >
          <IconBell size={16} />
          {notified ? "You're on the list" : 'Notify me when it’s live'}
        </button>
        <span className="text-xs text-neutral-600">
          {/* honesty stub marker */}
          We&apos;ll email you the moment this ships. (Notify is a stub — not yet wired to backend.)
        </span>
      </div>
    </div>
  )
}

function Panel({ heading, children }: { heading: string; children: React.ReactNode }) {
  return (
    <section className="rounded-xl border border-neutral-800 bg-neutral-950/60 p-5">
      <h2 className="text-xs font-mono uppercase tracking-wider text-neutral-500 mb-3">{heading}</h2>
      {children}
    </section>
  )
}

function Dot() {
  return <span className="mt-1.5 w-1.5 h-1.5 shrink-0 rounded-full bg-mint-400/70" aria-hidden />
}
