'use client'

/**
 * FalconNavLinks — admin-only operator nav links revealed by the
 * "Full Kanida.AI mode" toggle in UserMenu.
 *
 * Why client-side + localStorage:
 *   Visibility here is a pure UI preference (which routes the admin wants
 *   to see in the top nav), NOT an access-control decision. Access to
 *   /falcon/* and /analysis is enforced server-side by middleware.ts
 *   HTTP Basic Auth on every request — even if a non-admin flipped the
 *   localStorage flag manually, those pages would still 401. The toggle
 *   exists only so the consumer nav stays clean by default and admin
 *   opts in to seeing operator chrome.
 *
 * Reactivity (three triggers, all needed):
 *   1. mount         → read localStorage once
 *   2. 'storage'     → another tab toggled it (cross-tab sync)
 *   3. custom event  → UserMenu toggled it in this same tab (no reload)
 *
 * Visual cue:
 *   Links are tinted amber to make it instantly clear they're operator-only
 *   and visually distinct from the mint/neutral consumer links next to them.
 */
import Link from 'next/link'
import { useEffect, useState } from 'react'

export const FULL_MODE_KEY   = 'kanida.fullMode'
export const FULL_MODE_EVENT = 'kanida:toggle-full-mode'

export function FalconNavLinks({ isAdmin }: { isAdmin: boolean }) {
  const [enabled, setEnabled] = useState(false)

  useEffect(() => {
    if (!isAdmin) return
    const read = () => {
      try {
        setEnabled(window.localStorage.getItem(FULL_MODE_KEY) === '1')
      } catch {
        // localStorage blocked (private mode, CSP, etc.) — stay disabled.
        setEnabled(false)
      }
    }
    read()
    window.addEventListener('storage',         read)
    window.addEventListener(FULL_MODE_EVENT,   read)
    return () => {
      window.removeEventListener('storage',       read)
      window.removeEventListener(FULL_MODE_EVENT, read)
    }
  }, [isAdmin])

  if (!isAdmin || !enabled) return null

  return (
    <>
      <Link href="/falcon"           className="text-amber-300/90 hover:text-amber-200 hidden md:block">Falcon</Link>
      <Link href="/falcon/trade"     className="text-amber-300/90 hover:text-amber-200 hidden md:block">Trade</Link>
      <Link href="/falcon/premarket" className="text-amber-300/90 hover:text-amber-200 hidden md:block">Premarket</Link>
      <Link href="/analysis"         className="text-amber-300/90 hover:text-amber-200 hidden md:block">Analysis</Link>
    </>
  )
}
