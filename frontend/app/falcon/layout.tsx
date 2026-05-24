import Link from 'next/link'
import { ReactNode } from 'react'
import { InboxBell } from './InboxBell'

export default function FalconLayout({ children }: { children: ReactNode }) {
  const NAV = [
    { href: '/falcon',           label: 'Today' },
    { href: '/falcon/premarket', label: 'Pre-Market' },
    { href: '/falcon/trade',     label: 'Trade' },
    { href: '/falcon/positions', label: 'Positions' },
    { href: '/falcon/config',    label: 'Trail Config' },
    { href: '/falcon/signals',   label: 'Signals' },
    { href: '/falcon/backtest',  label: 'Backtest' },
    { href: '/falcon/patterns',  label: 'Patterns' },
    { href: '/falcon/admin',     label: 'Admin' },
  ]
  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-100">
      <header className="border-b border-neutral-800 px-6 py-3 flex items-center gap-6">
        <Link href="/" className="font-semibold">KANIDA.AI</Link>
        <span className="text-amber-400 font-medium">Falcon V7.1</span>
        <nav className="flex gap-4 text-sm flex-1">
          {NAV.map(n => (
            <Link key={n.href} href={n.href} className="text-neutral-400 hover:text-neutral-100">
              {n.label}
            </Link>
          ))}
        </nav>
        <InboxBell />
      </header>
      <main className="px-6 py-6 max-w-7xl mx-auto">{children}</main>
    </div>
  )
}
