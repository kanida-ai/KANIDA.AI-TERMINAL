/**
 * Public chrome for the Power User portal.
 *
 * Lives at /power/* — strictly separate from the operator-only /falcon/* surface.
 * Renders a thin top bar with brand + sign-in CTA on the right. Mobile-first.
 */
import Link from 'next/link'
import type { Metadata } from 'next'
import type { ReactNode } from 'react'
import { getCurrentUser } from '@/lib/power-auth'
import { UserMenu } from '@/components/power/UserMenu'

export const metadata: Metadata = {
  title: 'KANIDA.AI — Engineered Edge for Indian Equities',
  description: '865 historically-validated patterns. Daily ranked picks with the reason. ' +
                '₹30L → ₹1.05Cr walk-forward proof.',
}

export default async function PowerLayout({ children }: { children: ReactNode }) {
  const user = await getCurrentUser()
  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-100">
      <TopBar
        signedIn={user !== null}
        email={user?.email}
        displayName={user?.display_name ?? null}
        pictureUrl={user?.picture_url ?? null}
        isAdmin={user?.role === 'admin'}
      />
      <main className="max-w-6xl mx-auto px-4 md:px-6 py-6 md:py-10">
        {children}
      </main>
      <Footer />
    </div>
  )
}

function TopBar({ signedIn, email, displayName, pictureUrl, isAdmin }: {
  signedIn:    boolean
  email?:      string
  displayName: string | null
  pictureUrl:  string | null
  isAdmin?:    boolean
}) {
  return (
    <header className="border-b border-neutral-900 bg-neutral-950/95 backdrop-blur sticky top-0 z-10">
      <div className="max-w-6xl mx-auto px-4 md:px-6 h-14 flex items-center justify-between">
        <Link href="/power" className="flex items-center gap-2 font-bold text-lg">
          <span className="text-neutral-100">KANIDA</span>
          <span className="text-amber-400">.AI</span>
        </Link>
        <nav className="flex items-center gap-2 md:gap-4 text-sm">
          {signedIn ? (
            <>
              <Link href="/power/today" className="text-neutral-300 hover:text-neutral-100 hidden md:block">
                Today
              </Link>
              <Link href="/power/live" className="text-neutral-300 hover:text-neutral-100 hidden md:block">
                Live
              </Link>
              <UserMenu
                email={email ?? ''}
                displayName={displayName}
                pictureUrl={pictureUrl}
                isAdmin={isAdmin}
              />
            </>
          ) : (
            <>
              <Link href="/power/credibility" className="text-neutral-400 hover:text-neutral-100 hidden md:block">
                How it works
              </Link>
              <Link
                href="/power/login"
                className="px-3.5 py-1.5 bg-amber-500 text-neutral-950 rounded font-semibold hover:bg-amber-400 transition-colors"
              >
                Sign in
              </Link>
            </>
          )}
        </nav>
      </div>
    </header>
  )
}

function Footer() {
  return (
    <footer className="border-t border-neutral-900 mt-16 py-6">
      <div className="max-w-6xl mx-auto px-4 md:px-6 text-xs text-neutral-500 flex flex-col md:flex-row gap-2 md:gap-6 justify-between">
        <span>
          Engineered patterns from a 9-year mining + walk-forward validation pipeline.
          Not financial advice. You own your trades.
        </span>
        <span className="font-mono">v1.0 · 2026</span>
      </div>
    </footer>
  )
}
