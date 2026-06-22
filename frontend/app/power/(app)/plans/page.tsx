/** /power/plans — Launch-Pending shell (plan §7; paywall re-wire is Phase 1b). */
import { LaunchPending } from '@/components/power/LaunchPending'

export const dynamic = 'force-dynamic'

export default function PlansPage() {
  return (
    <LaunchPending
      title="Plans"
      summary="Choose what to unlock: free history and proof, or live signals with Gold and Enterprise quality tiers, deeper analysis and (when it’s live) AutoTrade."
      willDo={[
        'Free: explore Falcon’s past signals and proof to build confidence.',
        'Paid tiers: live Standard signals, then Gold, then Enterprise quality bands.',
        'Higher tiers add entry insights, full rulebook evidence and advanced analytics.',
        'Manage billing and your subscription in one place.',
      ]}
      expect={[
        'Tiers are qualitative quality bands — not promises of a fixed win rate.',
        'Lock states live inside the signal cards, so it’s always clear what you get.',
        'No dark patterns: upgrade and downgrade are equally easy.',
      ]}
      whyNotYet="The billing and paywall layer is built and audited but currently dormant. We re-wire it in the next phase (1b) so checkout and gating turn on together, cleanly."
    />
  )
}
