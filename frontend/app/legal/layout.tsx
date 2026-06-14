/**
 * Chrome for the public /legal/* pages (Terms, Privacy, Refund, Risk).
 *
 * These pages live OUTSIDE /power/* so they do not inherit the Power portal
 * layout. They get their own thin brand bar + a footer that cross-links the
 * four legal documents, matching the black / mint visual language of the rest
 * of the site (see app/power/layout.tsx).
 *
 * Public surface: the site-wide HTTP Basic Auth in middleware.ts is bypassed
 * for /legal (see POWER_PORTAL_PATHS / LEGAL handling there) so invitees and
 * prospects reaching these from the Power footer or the signup flow are not
 * blocked by a browser auth dialog.
 *
 * M6 (Legal). Static content only — no client JS, no data fetching.
 */
import Link from 'next/link'
import type { Metadata } from 'next'
import type { ReactNode } from 'react'
import { CompassLogo } from '@/components/power/CompassLogo'

export const metadata: Metadata = {
  title: 'Legal — KANIDA.AI',
  description: 'Terms of Service, Privacy Policy, Refund Policy, and Risk Disclosure for KANIDA.AI.',
  robots: { index: false, follow: false },
}

const LEGAL_LINKS: Array<{ href: string; label: string }> = [
  { href: '/legal/terms',   label: 'Terms of Service' },
  { href: '/legal/privacy', label: 'Privacy Policy' },
  { href: '/legal/refund',  label: 'Refund Policy' },
  { href: '/legal/risk',    label: 'Risk Disclosure' },
]

export default function LegalLayout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-black text-neutral-100">
      <header className="border-b border-neutral-900 bg-black/95 backdrop-blur sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 md:px-6 h-14 flex items-center justify-between">
          <Link href="/power" className="flex items-center gap-2.5 font-bold text-lg group">
            <CompassLogo size={24} />
            <span className="flex items-baseline tracking-wide">
              <span className="text-neutral-100">KANIDA</span>
              <span className="text-mint-400">.AI</span>
            </span>
          </Link>
          <Link href="/power" className="text-sm text-neutral-400 hover:text-neutral-100">
            ← Back to home
          </Link>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-10">
        {children}
      </main>

      <footer className="border-t border-neutral-900 mt-16 py-6">
        <div className="max-w-7xl mx-auto px-4 md:px-6 text-xs text-neutral-500 space-y-3">
          <nav className="flex flex-wrap gap-x-5 gap-y-2">
            {LEGAL_LINKS.map(l => (
              <Link key={l.href} href={l.href} className="hover:text-neutral-200">
                {l.label}
              </Link>
            ))}
          </nav>
          <div className="flex flex-col md:flex-row gap-2 md:gap-6 justify-between">
            <span>
              These documents are drafts pending legal review. Not financial advice.
              You own your trades.
            </span>
            <span className="font-mono">v1.0 · 2026</span>
          </div>
        </div>
      </footer>
    </div>
  )
}
