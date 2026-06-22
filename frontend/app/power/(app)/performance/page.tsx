/** /power/performance — Launch-Pending shell (plan §3 phase / §6 proof). */
import { LaunchPending } from '@/components/power/LaunchPending'

export const dynamic = 'force-dynamic'

export default function PerformancePage() {
  return (
    <LaunchPending
      title="Performance"
      summary="Track your picks and Falcon’s proof in one place — outcomes by horizon, historical performance by persona, and the past-signal evidence free users can explore before upgrading."
      willDo={[
        'Show what happened to signals 7 / 14 / 20 / 30 days after they fired.',
        'Break performance down by persona and by signal quality tier.',
        'Let free users replay past Falcon signals as proof, with an upgrade path.',
        'Tie your own entries back to Falcon’s original call and outcome.',
      ]}
      expect={[
        'Honest, walk-forward-validated history — wins and losses both shown.',
        'Qualitative tiers framed as quality bands, never guaranteed outcomes.',
        'A calm proof view, not a vanity dashboard.',
      ]}
      whyNotYet="The underlying outcome data exists from the walk-forward pipeline, but the unified Performance + proof view is the next build after the shell and signals. We’re bringing it up properly rather than scattering numbers across pages."
    />
  )
}
