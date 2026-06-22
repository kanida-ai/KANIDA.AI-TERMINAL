'use client'

/**
 * AppShell — the persistent left-nav shell for the Falcon AI-native portal.
 *
 * Benchmark = Claude.ai: a fixed left rail switches *modes*; the main work area
 * swaps on click (the rail + brand persist, so there is no full-page-hop feel).
 * Dark + mint theme only (active item = mint text on a mint-tinted pill).
 *
 * Responsive: on md+ the rail is a fixed 240px column; on small screens it
 * collapses behind a hamburger and slides over the content as an overlay.
 *
 * The shell is a Client Component (needs usePathname for the active state and
 * local state for the mobile drawer). It receives the resolved user as plain
 * props from the server layout — it never reads cookies itself.
 */
import { useState } from 'react'
import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { CompassLogo } from '@/components/power/CompassLogo'
import { PRIMARY_NAV, FOOTER_NAV, type NavItem } from './nav-config'

type Props = {
  children:    React.ReactNode
  displayName: string | null
  email:       string | null
  isAdmin:     boolean
}

export function AppShell({ children, displayName, email, isAdmin }: Props) {
  const [open, setOpen] = useState(false)
  const pathname = usePathname()

  const isActive = (href: string) =>
    pathname === href || pathname.startsWith(href + '/')

  const footer = FOOTER_NAV.filter(i => !i.adminOnly || isAdmin)

  // The Ask-Falcon home is a full-bleed F2 master-detail (its own middle +
  // right panels fill the canvas). Every other mode keeps the centered,
  // padded reading column.
  const fullBleed = pathname === '/power/ask' || pathname === '/power/co-trading' || pathname === '/power/autotrade'

  return (
    <div className="min-h-screen bg-black text-neutral-100 flex">
      {/* ── Mobile top bar (md:hidden) ── */}
      <header className="md:hidden fixed top-0 inset-x-0 z-30 h-14 flex items-center
                         justify-between px-4 border-b border-neutral-900 bg-black/95 backdrop-blur">
        <Link href="/power/ask" className="flex items-center gap-2 font-bold">
          <CompassLogo size={22} />
          <span className="tracking-wide"><span className="text-neutral-100">KANIDA</span><span className="text-mint-400">.AI</span></span>
        </Link>
        <button
          type="button"
          aria-label="Toggle navigation"
          onClick={() => setOpen(v => !v)}
          className="p-2 -mr-2 text-neutral-300 hover:text-mint-400"
        >
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round">
            <path d="M4 7h16M4 12h16M4 17h16" />
          </svg>
        </button>
      </header>

      {/* ── Mobile drawer overlay ── */}
      {open && (
        <div className="md:hidden fixed inset-0 z-40 bg-black/60" onClick={() => setOpen(false)} />
      )}

      {/* ── Left rail ── */}
      <aside
        className={[
          'fixed z-40 md:z-0 inset-y-0 left-0 w-60 shrink-0',
          'border-r border-neutral-900 bg-neutral-950/80 backdrop-blur',
          'flex flex-col md:translate-x-0 transition-transform duration-200',
          open ? 'translate-x-0' : '-translate-x-full',
          'md:sticky md:top-0 md:h-screen',
        ].join(' ')}
      >
        {/* Brand */}
        <Link
          href="/power/ask"
          onClick={() => setOpen(false)}
          className="h-16 px-5 flex items-center gap-2.5 font-bold text-lg border-b border-neutral-900 group"
        >
          <CompassLogo size={26} />
          <span className="flex items-baseline tracking-wide">
            <span className="text-neutral-100">KANIDA</span>
            <span className="text-mint-400">.AI</span>
          </span>
        </Link>

        {/* Primary modes */}
        <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-1">
          {PRIMARY_NAV.map(item => (
            <NavLink key={item.key} item={item} active={isActive(item.href)} onNavigate={() => setOpen(false)} />
          ))}
        </nav>

        {/* Footer / account */}
        <div className="px-3 py-3 border-t border-neutral-900 space-y-1">
          {footer.map(item => (
            <NavLink key={item.key} item={item} active={isActive(item.href)} onNavigate={() => setOpen(false)} small />
          ))}
          <div className="px-3 pt-3 pb-1">
            <div className="text-sm text-neutral-300 truncate">{displayName || 'Account'}</div>
            <div className="text-[11px] text-neutral-600 truncate">{email || 'signed in'}</div>
          </div>
        </div>
      </aside>

      {/* ── Main work area ── */}
      <div className="flex-1 min-w-0 flex flex-col">
        {fullBleed ? (
          // Full-bleed: the F2 home owns the entire canvas (it renders its own
          // middle persona/list panel + right detail). On md+ the area is
          // viewport-locked (md:h-[100dvh] + overflow-hidden) so the shell never
          // page-scrolls — each column scrolls independently inside it. Only the
          // mobile top-bar offset is applied (mobile stays scrollable). No extra
          // padding here: every region's top/padding is owned by AskFalconHome so
          // the three columns share the same usable height with no white gaps.
          <main className="flex-1 min-h-0 pt-14 md:pt-0 md:h-[100dvh] md:overflow-hidden">
            {children}
          </main>
        ) : (
          <main className="flex-1 px-4 md:px-8 lg:px-10 pt-20 md:pt-8 pb-16 max-w-5xl mx-auto w-full">
            {children}
          </main>
        )}
      </div>
    </div>
  )
}

function NavLink({
  item, active, onNavigate, small = false,
}: {
  item:       NavItem
  active:     boolean
  onNavigate: () => void
  small?:     boolean
}) {
  const { Icon, label, href, live } = item
  return (
    <Link
      href={href}
      onClick={onNavigate}
      aria-current={active ? 'page' : undefined}
      className={[
        'group relative flex items-center gap-3 rounded-lg px-3 transition-colors',
        small ? 'py-2 text-[13px]' : 'py-2.5 text-sm',
        active
          ? 'bg-mint-400/10 text-mint-300 font-semibold'
          : 'text-neutral-400 hover:text-neutral-100 hover:bg-neutral-900',
      ].join(' ')}
    >
      {/* active accent bar */}
      <span
        className={[
          'absolute left-0 top-1/2 -translate-y-1/2 h-5 w-0.5 rounded-r',
          active ? 'bg-mint-400' : 'bg-transparent',
        ].join(' ')}
        aria-hidden
      />
      <Icon size={small ? 16 : 18} />
      <span className="flex-1 truncate">{label}</span>
      {!live && (
        <span className="text-[9px] font-mono uppercase tracking-wider text-neutral-500
                         border border-neutral-700 rounded px-1 py-0.5">
          Soon
        </span>
      )}
    </Link>
  )
}
