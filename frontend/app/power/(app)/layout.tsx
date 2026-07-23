/**
 * Falcon AI-native shell layout — wraps the 6-mode portal (Ask Falcon · Signals
 * · Co-Trading · AutoTrade · Performance · Plans) in the persistent left-nav
 * AppShell. Additive: lives in route group app/power/(app)/* so URLs stay
 * /power/ask, /power/signals, … and the legacy /power/today, /power/live,
 * /power/replay routes are untouched.
 *
 * The parent app/power/layout.tsx detects these routes (x-pathname header) and
 * passes children through without its public TopBar/Footer, so the AppShell's
 * full-screen rail isn't double-wrapped.
 *
 * Auth: resolved here once (getCurrentUser). If absent we redirect to login;
 * BackendUnavailableError bubbles to the nearest error boundary (we never wipe
 * the session on a transient backend blip — see power-auth.ts Fix #3).
 */
import { redirect } from 'next/navigation'
import { getCurrentUser } from '@/lib/power-auth'
import { AppShell } from '@/components/power/shell/AppShell'

export const dynamic = 'force-dynamic'

export default async function FalconShellLayout({ children }: { children: React.ReactNode }) {
  // DEV-ONLY design preview: render the shell without auth so the UI/UX can be
  // reviewed locally without a login. Gated on an env flag that is unset in
  // prod (Vercel), so production auth is unchanged.
  if (process.env.NEXT_PUBLIC_PREVIEW_NO_AUTH === '1') {
    return (
      <AppShell displayName="Shyam" email="preview@kanida.ai" isAdmin>
        {children}
      </AppShell>
    )
  }

  const user = await getCurrentUser()
  if (!user) redirect('/power/login')

  const displayName = user.display_name ?? null
  return (
    <AppShell
      displayName={displayName}
      email={user.email}
      isAdmin={user.role === 'admin'}
    >
      {children}
    </AppShell>
  )
}
