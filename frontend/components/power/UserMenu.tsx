'use client'

/**
 * UserMenu — top-bar avatar + logout for authed pages.
 *
 * Just an avatar (or initials) + dropdown with logout. Keeps the chrome clean.
 *
 * Dropdown close semantics — three triggers, all needed:
 *   1. Click on the overlay (outside-click)  → setOpen(false) inline
 *   2. Click on any link/button inside menu  → onClose prop passed down
 *   3. URL changes (next/prev, programmatic) → useEffect on usePathname()
 * Without (2) the menu stays open after clicking a link because the layout
 * (which hosts this component) persists across route transitions.
 * Without (3) the menu stays open after a same-route click or browser back.
 *
 * z-index — overlay is z-40 and menu is z-50, both above the sticky header
 * (z-10) so the outside-click overlay reliably catches every viewport click.
 */
import { useEffect, useState } from 'react'
import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { logout } from '@/lib/power-auth-client'

type Props = {
  email:        string
  displayName:  string | null
  pictureUrl:   string | null
  isAdmin?:     boolean
}

export function UserMenu({ email, displayName, pictureUrl, isAdmin }: Props) {
  const [open, setOpen] = useState(false)
  const pathname        = usePathname()

  // Trigger 3: any URL change closes the menu (covers back/forward, programmatic
  // navigation, and same-pathname clicks that the click handler missed).
  useEffect(() => { setOpen(false) }, [pathname])

  const close   = () => setOpen(false)
  const initial = (displayName || email).slice(0, 1).toUpperCase()

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen(v => !v)}
        aria-label="Account menu"
        aria-expanded={open}
        className="flex items-center gap-2 text-sm rounded hover:bg-neutral-900 px-2 py-1"
      >
        {pictureUrl ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img src={pictureUrl} alt="" width={28} height={28} className="rounded-full" />
        ) : (
          <span className="w-7 h-7 rounded-full bg-mint-500 text-neutral-950 grid place-items-center font-bold">
            {initial}
          </span>
        )}
        <span className="hidden md:inline text-neutral-300 max-w-[10rem] truncate">
          {displayName || email}
        </span>
        <span className="text-neutral-600 text-[10px]">▾</span>
      </button>

      {open && (
        <>
          {/* Overlay: z-40 places it above the sticky header (z-10) so
              clicks anywhere outside the menu are reliably captured. */}
          <div className="fixed inset-0 z-40" onClick={close} />
          <div className="absolute right-0 mt-1 w-56 bg-neutral-900 border border-neutral-800
                          rounded shadow-lg z-50 py-1 text-sm">
            <div className="px-3 py-2 border-b border-neutral-800">
              <div className="text-neutral-200 truncate">{displayName || 'Account'}</div>
              <div className="text-xs text-neutral-500 truncate">{email}</div>
            </div>
            <MenuLink href="/power/today"  onClose={close}>Today&apos;s picks</MenuLink>
            <MenuLink href="/power/live"   onClose={close}>Live decisions</MenuLink>
            <MenuLink href="/power"        onClose={close}>Replays</MenuLink>
            {isAdmin && <MenuLink href="/power/admin" onClose={close}>Admin</MenuLink>}
            <button
              type="button"
              onClick={() => { close(); logout() }}
              className="block w-full text-left px-3 py-2 text-red-300 hover:bg-neutral-800"
            >
              Sign out
            </button>
          </div>
        </>
      )}
    </div>
  )
}

function MenuLink({ href, onClose, children }: {
  href:     string
  onClose:  () => void
  children: React.ReactNode
}) {
  return (
    <Link
      href={href}
      onClick={onClose}
      className="block px-3 py-2 text-neutral-300 hover:bg-neutral-800 hover:text-neutral-100"
    >
      {children}
    </Link>
  )
}
