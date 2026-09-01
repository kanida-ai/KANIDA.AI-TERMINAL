/**
 * DEV-ONLY preview of the /power/agents Chart-Agent experience — renders the SAME
 * <AgentsSurface/> with real/live data but WITHOUT the power-auth gate, so the UI
 * can be visually validated with `next dev` at http://localhost:3000/dev/agents-preview.
 *
 * Guarded: returns notFound() when NODE_ENV === 'production', so it never ships to
 * prod. It fetches the identical live endpoints (findFreshestScan default date).
 */
import { notFound } from 'next/navigation'
import { AgentsSurface } from '@/components/power/agents/AgentsSurface'

export const dynamic = 'force-dynamic'

export default function AgentsPreviewPage() {
  if (process.env.NODE_ENV === 'production') notFound()
  return (
    <div style={{ position: 'fixed', inset: 0 }}>
      <AgentsSurface />
    </div>
  )
}
