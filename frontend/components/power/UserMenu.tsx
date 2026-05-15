'use client'

/**
 * UserMenu — top-bar avatar + logout for authed pages.
 *
 * Just an avatar (or initials) + dropdown with logout. Keeps the chrome clean.
 */
import { useState } from 'react'
import Link from 'next/link'
import { logout } from '@/lib/power-auth-client'

type Props = {
  email:        string
  displayName:  string | null
  pictureUrl:   string | null
  isAdmin?:     boolean
}

export function UserMenu({ email, displayName, pictureUrl, isAdmin }: Props) {
  const [open, setOpen] = useState(false)
  const initial = (displayName || email).slice(0, 1).toUpperCase()

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen(v => !v)}
        aria-label="Account menu"
        className="flex items-center gap-2 text-sm rounded hover:bg-neutral-900 px-2 py-1"
      >
        {pictureUrl ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img src={pictureUrl} alt="" width={28} height={28} className="rounded-full" />
        ) : (
          <span className="w-7 h-7 rounded-full bg-amber-500 text-neutral-950 grid place-items-center font-bold">
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
          <div className="fixed inset-0 z-10" onClick={() => setOpen(false)} />
          <div className="absolute right-0 mt-1 w-56 bg-neutral-900 border border-neutral-800
                          rounded shadow-lg z-20 py-1 text-sm">
            <div className="px-3 py-2 border-b border-neutral-800">
              <div className="text-neutral-200 truncate">{displayName || 'Account'}</div>
              <div className="text-xs text-neutral-500 truncate">{email}</div>
            </div>
            <MenuLink href="/power/today">Today's picks</MenuLink>
            <MenuLink href="/power/live">Live decisions</MenuLink>
            <MenuLink href="/power">Replays</MenuLink>
            {isAdmin && <MenuLink href="/power/admin">Admin</MenuLink>}
            <button
              type="button"
              onClick={() => { setOpen(false); logout() }}
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

function MenuLink({ href, children }: { href: string; children: React.ReactNode }) {
  return (
    <Link
      href={href}
      className="block px-3 py-2 text-neutral-300 hover:bg-neutral-800 hover:text-neutral-100"
    >
      {children}
    </Link>
  )
}
