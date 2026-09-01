'use client'

/**
 * /power/agents — Chart Pattern Agent, rebuilt to CHART_AGENT_UX_SPEC.md.
 *
 * LEFT nav (agent roster + pattern categories) · TOP tabs (ALL / QUALIFIED / WATCH
 * / NO TRADE) · MAIN expand-in-place feed. ONE Kanida dark + mint design system,
 * every number/line/chart from the live /api/agents/chart/* endpoints. The whole
 * experience is the shared <AgentsSurface/> (also mounted by the dev preview route
 * at /dev/agents-preview for auth-free visual validation).
 */
import { AgentsSurface } from '@/components/power/agents/AgentsSurface'

export default function AgentsPage() {
  return <AgentsSurface />
}
