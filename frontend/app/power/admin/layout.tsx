/**
 * /power/admin/* — server-side role gate.
 *
 * Phase 1b: only users with role === 'admin' can reach any admin route.
 * Non-authed users → /power/login. Authed-but-non-admin → /power/today.
 *
 * The existing /power/admin/page.tsx remains a client component with its own
 * SecretGate for backend API auth (X-Admin-Secret header). This layout is
 * the OUTER gate that hides the page from non-admins entirely.
 *
 * Implementation note: getCurrentUser() reads the HTTPOnly session cookie
 * via next/headers, so this runs server-side per request — there is no way
 * for a non-admin user agent to bypass it by hitting the page directly.
 */
import { redirect } from 'next/navigation'
import type { ReactNode } from 'react'
import { getCurrentUser } from '@/lib/power-auth'

export default async function AdminLayout({ children }: { children: ReactNode }) {
  const user = await getCurrentUser()
  if (!user) redirect('/power/login?from=/power/admin')
  if (user.role !== 'admin') redirect('/power/today')
  return <>{children}</>
}
