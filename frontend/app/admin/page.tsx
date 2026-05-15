'use client'

// Legacy /admin route — consolidated into /falcon/admin (2026-05-09).
// This component preserves the OAuth callback flow:
// when Kite redirects back with `?request_token=...&status=success`,
// we forward the query string to /falcon/admin which handles the exchange.
// Existing bookmarks to /admin keep working — they land at the new home.
//
// The old 1411-line implementation lives at page.tsx.legacy-backup for reference.

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

export default function AdminRedirect() {
  const router = useRouter()
  useEffect(() => {
    const qs = typeof window !== 'undefined' ? window.location.search : ''
    router.replace(`/falcon/admin${qs}`)
  }, [router])
  return (
    <div className="min-h-screen flex items-center justify-center bg-neutral-950 text-neutral-400">
      <div className="text-center">
        <div className="text-sm">Admin moved to <code className="text-amber-400">/falcon/admin</code></div>
        <div className="text-xs text-neutral-600 mt-1">Redirecting…</div>
      </div>
    </div>
  )
}
