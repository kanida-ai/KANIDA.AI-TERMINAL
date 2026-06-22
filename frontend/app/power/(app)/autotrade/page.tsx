/** /power/autotrade — Launch-Pending shell (plan §5; per-user is operator-only today). */
import { LaunchPending } from '@/components/power/LaunchPending'

export const dynamic = 'force-dynamic'

export default function AutoTradePage() {
  return (
    <LaunchPending
      title="AutoTrade"
      summary="Fully automated execution of Falcon signals against your own broker — readiness check, persona, risk config, and a clear view of what will happen, before anything is live."
      willDo={[
        'Run a readiness check: broker token, available funds, risk config.',
        'Let you pick a persona and configure per-trade size and risk limits.',
        'Show the eligible signals that would be acted on — before they fire.',
        'Execute, monitor and trail stops automatically once you opt in.',
      ]}
      expect={[
        'A clear LIVE-vs-not split: nothing trades without your explicit go-ahead.',
        'Full visibility into every order and stop the system manages for you.',
        'You can pause or stop automation at any time.',
      ]}
      whyNotYet="Automated execution runs today on the operator’s own account only. Per-user AutoTrade needs your individual broker connect plus the safety preflight extended per account — that connection layer isn’t live yet. We will never place an order on your behalf until it is."
    />
  )
}
